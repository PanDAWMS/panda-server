'''
provide web interface to users

'''

import re
import sys
import time
import json
import types
import datetime
import traceback

import pandaserver.jobdispatcher.Protocol as Protocol
import pandaserver.taskbuffer.ProcessGroups
from pandaserver.taskbuffer.WrappedPickle import WrappedPickle
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.taskbuffer import PrioUtil
from pandaserver.dataservice.DDM import rucioAPI

try:
    long()
except NameError:
    long = int

# logger
_logger = PandaLogger().getLogger('UserIF')


# main class     
class UserIF:
    # constructor
    def __init__(self):
        self.taskBuffer = None
        

    # initialize
    def init(self,taskBuffer):
        self.taskBuffer = taskBuffer


    # submit jobs
    def submitJobs(self,jobsStr,user,host,userFQANs,prodRole=False,toPending=False):
        try:
            # deserialize jobspecs
            jobs = WrappedPickle.loads(jobsStr)
            _logger.debug("submitJobs %s len:%s prodRole=%s FQAN:%s" % (user,len(jobs),prodRole,str(userFQANs)))
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error("submitJobs: too many jobs more than %s" % maxJobs)
                jobs = jobs[:maxJobs]
        except Exception as ex:
            _logger.error("submitJobs : %s %s" % (str(ex), traceback.format_exc()))
            jobs = []
        # check prodSourceLabel
        try:
            goodProdSourceLabel = True
            for tmpJob in jobs:
                # prevent internal jobs from being submitted from outside
                if tmpJob.prodSourceLabel in pandaserver.taskbuffer.ProcessGroups.internalSourceLabels:
                    _logger.error("submitJobs %s wrong prodSourceLabel=%s" % (user,tmpJob.prodSourceLabel))
                    goodProdSourceLabel = False
                    break
                # check production role
                if tmpJob.prodSourceLabel in ['managed']:
                    if not prodRole:
                        _logger.error("submitJobs %s missing prod-role for prodSourceLabel=%s" % (user,tmpJob.prodSourceLabel))
                        goodProdSourceLabel = False
                        break
        except Exception:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("submitJobs : checking goodProdSourceLabel %s %s" % (errType,errValue))
            goodProdSourceLabel = False
        # reject injection for bad prodSourceLabel
        if not goodProdSourceLabel:
            return "ERROR: production role is required for production jobs"
        job0 = None
        # get user VO
        userVO = 'atlas'
        try:
            job0 = jobs[0]
            if not job0.VO in [None,'','NULL']:
                userVO = job0.VO
        except (IndexError, AttributeError) as e:
            _logger.error("submitJobs : checking userVO. userVO not found, defaulting to %s. (Exception %s)" %(userVO, e))
        # atlas jobs require FQANs
        if userVO == 'atlas' and userFQANs == []:
            _logger.error("submitJobs : VOMS FQANs are missing in your proxy. They are required for {0}".format(userVO))
            #return "ERROR: VOMS FQANs are missing. They are required for {0}".format(userVO)
        # get LSST pipeline username
        if userVO.lower() == 'lsst':
            try:
                if job0.prodUserName and job0.prodUserName.lower() != 'none':
                    user = job0.prodUserName
            except AttributeError:
                _logger.error("submitJobs : checking username for userVO[%s]: username not found, defaulting to %s. %s %s" % (userVO, user))
        # store jobs
        ret = self.taskBuffer.storeJobs(jobs,user,forkSetupper=True,fqans=userFQANs,
                                        hostname=host, toPending=toPending, userVO=userVO)
        _logger.debug("submitJobs %s ->:%s" % (user,len(ret)))
        # serialize 
        return WrappedPickle.dumps(ret)


    # run task assignment
    def runTaskAssignment(self,jobsStr):
        try:
            # deserialize jobspecs
            jobs = WrappedPickle.loads(jobsStr)
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error("runTaskAssignment : %s %s" % (type,value))
            jobs = []
        # run
        ret = self.taskBuffer.runTaskAssignment(jobs)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get serial number for group job
    def getSerialNumberForGroupJob(self,name):
        # get
        ret = self.taskBuffer.getSerialNumberForGroupJob(name)
        # serialize 
        return WrappedPickle.dumps(ret)


    # change job priorities
    def changeJobPriorities(self,user,prodRole,newPrioMapStr):
        # check production role
        if not prodRole:
            return False,"production role is required"
        try:
            # deserialize map
            newPrioMap = WrappedPickle.loads(newPrioMapStr)
            _logger.debug("changeJobPriorities %s : %s" % (user,str(newPrioMap)))
            # change
            ret = self.taskBuffer.changeJobPriorities(newPrioMap)
        except Exception:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("changeJobPriorities : %s %s" % (errType,errValue))
            return False,'internal server error' 
        # serialize 
        return ret


    # retry failed subjobs in running job
    def retryFailedJobsInActive(self,dn,jobID):
        returnVal = False
        try:
            _logger.debug("retryFailedJobsInActive %s JobID:%s" % (dn,jobID))
            cUID = self.taskBuffer.cleanUserID(dn)            
            tmpRet = self.taskBuffer.finalizePendingJobs(cUID,jobID)
            returnVal = True
        except Exception:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("retryFailedJobsInActive: %s %s" % (errType,errValue))
            returnVal = "ERROR: server side crash"
        # return
        return returnVal


    # set debug mode
    def setDebugMode(self,dn,pandaID,prodManager,modeOn,workingGroup):
        ret = self.taskBuffer.setDebugMode(dn,pandaID,prodManager,modeOn,workingGroup)
        # return
        return ret


    # insert sandbox file info
    def insertSandboxFileInfo(self,userName,hostName,fileName,fileSize,checkSum):
        ret = self.taskBuffer.insertSandboxFileInfo(userName,hostName,fileName,fileSize,checkSum)
        # return
        return ret


    # check duplicated sandbox file
    def checkSandboxFile(self,userName,fileSize,checkSum):
        ret = self.taskBuffer.checkSandboxFile(userName,fileSize,checkSum)
        # return
        return ret


    # get job status
    def getJobStatus(self,idsStr):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
            _logger.debug("getJobStatus len   : %s" % len(ids))
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobStatus : %s %s" % (type,value))
            ids = []
        _logger.debug("getJobStatus start : %s" % ids)       
        # peek jobs
        ret = self.taskBuffer.peekJobs(ids)
        _logger.debug("getJobStatus end")
        # serialize 
        return WrappedPickle.dumps(ret)


    # get PandaID with jobexeID
    def getPandaIDwithJobExeID(self,idsStr):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
            _logger.debug("getPandaIDwithJobExeID len   : %s" % len(ids))
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getPandaIDwithJobExeID : %s %s" % (errtype,errvalue))
            ids = []
        _logger.debug("getPandaIDwithJobExeID start : %s" % ids)       
        # peek jobs
        ret = self.taskBuffer.getPandaIDwithJobExeID(ids)
        _logger.debug("getPandaIDwithJobExeID end")
        # serialize 
        return WrappedPickle.dumps(ret)



    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self,jediTaskID):
        # get PandaIDs
        ret = self.taskBuffer.getPandaIDsWithTaskID(jediTaskID)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get assigned cloud for tasks
    def seeCloudTask(self,idsStr):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error("seeCloudTask : %s %s" % (type,value))
            ids = []
        _logger.debug("seeCloudTask start : %s" % ids)       
        # peek jobs
        ret = {}
        for id in ids:
            tmpRet = self.taskBuffer.seeCloudTask(id)
            ret[id] = tmpRet
        _logger.debug("seeCloudTask end")
        # serialize 
        return WrappedPickle.dumps(ret)


    # get active datasets
    def getActiveDatasets(self,computingSite,prodSourceLabel):
        # run
        ret = self.taskBuffer.getActiveDatasets(computingSite,prodSourceLabel)
        # return
        return ret


    # get assigning task
    def getAssigningTask(self):
        # run
        ret = self.taskBuffer.getAssigningTask()
        # serialize 
        return WrappedPickle.dumps(ret)


    # set task by user
    def setCloudTaskByUser(self,user,tid,cloud,status):
        # run
        ret = self.taskBuffer.setCloudTaskByUser(user,tid,cloud,status)
        return ret

    
    # get job statistics
    def getJobStatistics(self,sourcetype=None):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForExtIF(sourcetype)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get highest prio jobs
    def getHighestPrioJobStat(self,perPG=False,useMorePG=False):
        # get job statistics
        ret = self.taskBuffer.getHighestPrioJobStat(perPG,useMorePG)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get queued analysis jobs at a site
    def getQueuedAnalJobs(self,site,dn):
        # get job statistics
        ret = self.taskBuffer.getQueuedAnalJobs(site,dn)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self,useMorePG=False):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForBamboo(useMorePG)
        # serialize 
        return WrappedPickle.dumps(ret)
        

    # get job statistics per site
    def getJobStatisticsPerSite(self,predefined=False,workingGroup='',countryGroup='',jobType='',
                                minPriority=None,readArchived=True):
        # get job statistics
        ret = self.taskBuffer.getJobStatistics(readArchived,predefined,workingGroup,countryGroup,jobType,
                                               minPriority=minPriority)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get job statistics per site and resource
    def getJobStatisticsPerSiteResource(self, timeWindow):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsPerSiteResource(timeWindow)
        # serialize 
        return json.dumps(ret)


    # get the number of waiting jobs per site and use
    def getJobStatisticsPerUserSite(self):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsPerUserSite()
        # serialize 
        return WrappedPickle.dumps(ret)


    # get job statistics per site with label
    def getJobStatisticsWithLabel(self,site):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsWithLabel(site)
        # serialize 
        return WrappedPickle.dumps(ret)


    # query PandaIDs
    def queryPandaIDs(self,idsStr):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # query PandaIDs 
        ret = self.taskBuffer.queryPandaIDs(ids)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get number of analysis jobs per user  
    def getNUserJobs(self,siteName):
        # get 
        ret = self.taskBuffer.getNUserJobs(siteName)
        # serialize 
        return WrappedPickle.dumps(ret)


    # query job info per cloud
    def queryJobInfoPerCloud(self,cloud,schedulerID):
        # query PandaIDs 
        ret = self.taskBuffer.queryJobInfoPerCloud(cloud,schedulerID)
        # serialize 
        return WrappedPickle.dumps(ret)

    
    # query PandaIDs at site
    def getPandaIDsSite(self,site,status,limit):
        # query PandaIDs 
        ret = self.taskBuffer.getPandaIDsSite(site,status,limit)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get PandaIDs to be updated in prodDB
    def getJobsToBeUpdated(self,limit,lockedby):
        # query PandaIDs 
        ret = self.taskBuffer.getPandaIDsForProdDB(limit,lockedby)
        # serialize 
        return WrappedPickle.dumps(ret)


    # update prodDBUpdateTimes
    def updateProdDBUpdateTimes(self,paramsStr):
        # deserialize IDs
        params = WrappedPickle.loads(paramsStr)
        # get jobs
        ret = self.taskBuffer.updateProdDBUpdateTimes(params)
        # serialize 
        return WrappedPickle.dumps(True)


    # query last files in datasets
    def queryLastFilesInDataset(self,datasetStr):
        # deserialize names
        datasets = WrappedPickle.loads(datasetStr)
        # get files
        ret = self.taskBuffer.queryLastFilesInDataset(datasets)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get input files currently in used for analysis
    def getFilesInUseForAnal(self,outDataset):
        # get files
        ret = self.taskBuffer.getFilesInUseForAnal(outDataset)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get list of dis dataset to get input files in shadow
    def getDisInUseForAnal(self,outDataset):
        # get files
        ret = self.taskBuffer.getDisInUseForAnal(outDataset)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get input LFNs currently in use for analysis with shadow dis
    def getLFNsInUseForAnal(self,inputDisListStr):
        # deserialize IDs
        inputDisList = WrappedPickle.loads(inputDisListStr)
        # get files
        ret = self.taskBuffer.getLFNsInUseForAnal(inputDisList)
        # serialize 
        return WrappedPickle.dumps(ret)


    # kill jobs
    def killJobs(self,idsStr,user,host,code,prodManager,useMailAsID,fqans,killOpts=[]):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        if not isinstance(ids, list):
            ids = [ids]
        _logger.info("killJob : %s %s %s %s %s" % (user,code,prodManager,fqans,ids))
        try:
            if useMailAsID:
                _logger.debug("killJob : getting mail address for %s" % user)
                realDN = re.sub('/CN=limited proxy','',user)
                realDN = re.sub('(/CN=proxy)+','',realDN)
                nTry = 3
                for iDDMTry in range(nTry):
                    status,userInfo = rucioAPI.finger(realDN)
                    if status:
                        _logger.debug("killJob : %s is converted to %s" % (user,userInfo['email']))
                        user = userInfo['email']
                        break
                    time.sleep(1)
        except Exception:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("killJob : failed to convert email address %s : %s %s" % (user,errType,errValue))
        # get working groups with prod role
        wgProdRole = []
        for fqan in fqans:
            tmpMatch = re.search('/atlas/([^/]+)/Role=production',fqan)
            if tmpMatch is not None:
                # ignore usatlas since it is used as atlas prod role
                tmpWG = tmpMatch.group(1) 
                if not tmpWG in ['','usatlas']+wgProdRole:
                    wgProdRole.append(tmpWG)
                    # group production
                    wgProdRole.append('gr_%s' % tmpWG)
        # kill jobs
        ret = self.taskBuffer.killJobs(ids,user,code,prodManager,wgProdRole,killOpts)
        # serialize 
        return WrappedPickle.dumps(ret)


    # reassign jobs
    def reassignJobs(self,idsStr,user,host,forPending,firstSubmission):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # reassign jobs
        ret = self.taskBuffer.reassignJobs(ids,forkSetupper=True,forPending=forPending,
                                           firstSubmission=firstSubmission)
        # serialize 
        return WrappedPickle.dumps(ret)
        

    # resubmit jobs
    def resubmitJobs(self,idsStr):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # kill jobs
        ret = self.taskBuffer.resubmitJobs(ids)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get list of site spec
    def getSiteSpecs(self,siteType='analysis'):
        # get analysis site list
        specList = {}
        siteMapper = SiteMapper(self.taskBuffer)
        for id in siteMapper.siteSpecList:
            spec = siteMapper.siteSpecList[id]
            if siteType == 'all' or spec.type == siteType:
                # convert to map
                tmpSpec = {}
                for attr in spec._attributes:
                    if attr in ['ddm_endpoints_input', 'ddm_endpoints_output', 'ddm_input', 'ddm_output',
                                'setokens_input', 'num_slots_map']:
                        continue
                    tmpSpec[attr] = getattr(spec,attr)
                specList[id] = tmpSpec
        # serialize
        return WrappedPickle.dumps(specList)


    # get list of cloud spec
    def getCloudSpecs(self):
        # get cloud list
        siteMapper = SiteMapper(self.taskBuffer)
        # serialize
        return WrappedPickle.dumps(siteMapper.cloudSpec)


    # get list of cache prefix
    def getCachePrefixes(self):
        # get
        ret = self.taskBuffer.getCachePrefixes()
        # serialize 
        return WrappedPickle.dumps(ret)


    # get list of cmtConfig
    def getCmtConfigList(self,relaseVer):
        # get
        ret = self.taskBuffer.getCmtConfigList(relaseVer)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get nPilots
    def getNumPilots(self):
        # get nPilots
        ret = self.taskBuffer.getCurrentSiteData()
        numMap = {}
        for siteID in ret:
            siteNumMap = ret[siteID]
            nPilots = 0
            # nPilots = getJob+updateJob
            if 'getJob' in siteNumMap:
                nPilots += siteNumMap['getJob']
            if 'updateJob' in siteNumMap:
                nPilots += siteNumMap['updateJob']
            # append
            numMap[siteID] = {'nPilots':nPilots}
        # serialize
        return WrappedPickle.dumps(numMap)


    # get script for offline running
    def getScriptOfflineRunning(self,pandaID,days=None):
        # register
        ret = self.taskBuffer.getScriptOfflineRunning(pandaID,days)
        # return
        return ret


    # register proxy key
    def registerProxyKey(self,params):
        # register
        ret = self.taskBuffer.registerProxyKey(params)
        # return
        return ret

    
    # get client version
    def getPandaClientVer(self):
        # get
        ret = self.taskBuffer.getPandaClientVer()
        # return
        return ret


    # get proxy key
    def getProxyKey(self,dn):
        # get files
        ret = self.taskBuffer.getProxyKey(dn)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get slimmed file info with PandaIDs
    def getSlimmedFileInfoPandaIDs(self,pandaIDsStr,dn):
        try:
            # deserialize IDs
            pandaIDs = WrappedPickle.loads(pandaIDsStr)
            # truncate
            maxIDs = 5500
            if len(pandaIDs) > maxIDs:
                _logger.error("getSlimmedFileInfoPandaIDs: too long ID list more than %s" % maxIDs)
                pandaIDs = pandaIDs[:maxIDs]
            # get
            _logger.debug("getSlimmedFileInfoPandaIDs start : %s %s" % (dn,len(pandaIDs)))            
            ret = self.taskBuffer.getSlimmedFileInfoPandaIDs(pandaIDs)
            _logger.debug("getSlimmedFileInfoPandaIDs end")            
        except Exception:
            ret = {}
        # serialize 
        return WrappedPickle.dumps(ret)


    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,dn,timeRange):
        # get IDs
        ret = self.taskBuffer.getJobIDsInTimeRange(dn,timeRange)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get active JediTasks in a time range
    def getJediTasksInTimeRange(self, dn, timeRange, fullFlag, minTaskID):
        # get IDs
        ret = self.taskBuffer.getJediTasksInTimeRange(dn, timeRange, fullFlag, minTaskID)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get details of JediTask
    def getJediTaskDetails(self,jediTaskID,fullFlag,withTaskInfo):
        # get IDs
        ret = self.taskBuffer.getJediTaskDetails(jediTaskID,fullFlag,withTaskInfo)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,nJobs):
        # get IDs
        ret = self.taskBuffer.getPandIDsWithJobID(dn,jobID,nJobs)
        # serialize 
        return WrappedPickle.dumps(ret)


    # check merge job generation status
    def checkMergeGenerationStatus(self,dn,jobID):
        # check
        ret = self.taskBuffer.checkMergeGenerationStatus(dn,jobID)
        # serialize 
        return WrappedPickle.dumps(ret)


    # get full job status
    def getFullJobStatus(self,idsStr,dn):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
            # truncate
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error("getFullJobStatus: too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except Exception:
            type, value, traceBack = sys.exc_info()
            _logger.error("getFullJobStatus : %s %s" % (type,value))
            ids = []
        _logger.debug("getFullJobStatus start : %s %s" % (dn,str(ids)))
        # peek jobs
        ret = self.taskBuffer.getFullJobStatus(ids)
        _logger.debug("getFullJobStatus end")
        # serialize 
        return WrappedPickle.dumps(ret)


    # add account to siteaccess
    def addSiteAccess(self,siteID,dn):
        # add
        ret = self.taskBuffer.addSiteAccess(siteID,dn)
        # serialize 
        return WrappedPickle.dumps(ret)


    # list site access
    def listSiteAccess(self,siteID,dn,longFormat=False):
        # list
        ret = self.taskBuffer.listSiteAccess(siteID,dn,longFormat)
        # serialize 
        return WrappedPickle.dumps(ret)


    # update site access
    def updateSiteAccess(self,method,siteid,requesterDN,userName,attrValue):
        # list
        ret = self.taskBuffer.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)
        # serialize 
        return str(ret)


    # insert task params
    def insertTaskParams(self,taskParams,user,prodRole,fqans,properErrorCode):
        # register
        ret = self.taskBuffer.insertTaskParamsPanda(taskParams,user,prodRole,fqans,properErrorCode=properErrorCode)
        # return
        return ret


    # kill task
    def killTask(self,jediTaskID,user,prodRole,properErrorCode):
        # kill
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'kill',properErrorCode=properErrorCode)
        # return
        return ret


    # finish task
    def finishTask(self,jediTaskID,user,prodRole,properErrorCode,qualifier):
        # kill
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'finish',
                                                   properErrorCode=properErrorCode,
                                                   comQualifier=qualifier)
        # return
        return ret


    # reload input
    def reloadInput(self,jediTaskID,user,prodRole):
        # kill
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'incexec',comComment='{}',properErrorCode=True)
        # return
        return ret


    # retry task
    def retryTask(self,jediTaskID,user,prodRole,properErrorCode,newParams,noChildRetry,discardEvents):
        # retry with new params
        if newParams is not None:
            try:
                # convert to dict
                newParams = PrioUtil.decodeJSON(newParams)
                # get original params
                taskParams = self.taskBuffer.getTaskPramsPanda(jediTaskID)
                taskParamsJson = PrioUtil.decodeJSON(taskParams)
                # replace with new values
                for newKey in newParams:
                    newVal = newParams[newKey]
                    taskParamsJson[newKey] = newVal
                taskParams = json.dumps(taskParamsJson)
                # retry with new params
                ret = self.taskBuffer.insertTaskParamsPanda(taskParams,user,prodRole,[],properErrorCode=properErrorCode,
                                                            allowActiveTask=True)
            except Exception:
                errType,errValue = sys.exc_info()[:2]
                ret = 1,'server error with {0}:{1}'.format(errType,errValue)
        else:
            if noChildRetry:
                comQualifier = 'sole'
            else:
                comQualifier = None
            if discardEvents:
                if comQualifier is None:
                    comQualifier = 'discard'
                else:
                    comQualifier += ' discard'
            # normal retry
            ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'retry',properErrorCode=properErrorCode,
                                                       comQualifier=comQualifier)
        if properErrorCode == True and ret[0] == 5:
            # retry failed analysis jobs
            jobdefList = self.taskBuffer.getJobdefIDsForFailedJob(jediTaskID)
            cUID = self.taskBuffer.cleanUserID(user)
            for jobID in jobdefList:
                self.taskBuffer.finalizePendingJobs(cUID, jobID)
            self.taskBuffer.increaseAttemptNrPanda(jediTaskID,5)
            retStr  = 'retry has been triggered for failed jobs '
            retStr += 'while the task is still {0}'.format(ret[1])
            if newParams is None:
                ret = 0,retStr
            else:
                ret = 3,retStr
        # return
        return ret


    # reassign task
    def reassignTask(self,jediTaskID,user,prodRole,comComment):
        # reassign
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'reassign',
                                                   comComment=comComment,properErrorCode=True)
        # return
        return ret


    # pause task
    def pauseTask(self,jediTaskID,user,prodRole):
        # exec
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'pause',properErrorCode=True)
        # return
        return ret


    # resume task
    def resumeTask(self,jediTaskID,user,prodRole):
        # exec
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'resume',properErrorCode=True)
        # return
        return ret


    # force avalanche for task
    def avalancheTask(self,jediTaskID,user,prodRole):
        # exec
        ret = self.taskBuffer.sendCommandTaskPanda(jediTaskID,user,prodRole,'avalanche',properErrorCode=True)
        # return
        return ret


    # get retry history
    def getRetryHistory(self,jediTaskID,user):
        # get
        _logger.debug("getRetryHistory jediTaskID={0} start {1}".format(jediTaskID,user))
        ret = self.taskBuffer.getRetryHistoryJEDI(jediTaskID)
        _logger.debug("getRetryHistory jediTaskID={0} done".format(jediTaskID))
        # return
        return ret


    # change task priority 
    def changeTaskPriority(self,jediTaskID,newPriority):
        # kill
        ret = self.taskBuffer.changeTaskPriorityPanda(jediTaskID,newPriority)
        # return
        return ret


    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self,jediTaskID,increasedNr):
        # exec
        ret = self.taskBuffer.increaseAttemptNrPanda(jediTaskID,increasedNr)
        # return
        return ret


    # change task attribute
    def changeTaskAttributePanda(self,jediTaskID,attrName,attrValue):
        # kill
        ret = self.taskBuffer.changeTaskAttributePanda(jediTaskID,attrName,attrValue)
        # return
        return ret


    # change split rule for task
    def changeTaskSplitRulePanda(self,jediTaskID,attrName,attrValue):
        # exec
        ret = self.taskBuffer.changeTaskSplitRulePanda(jediTaskID,attrName,attrValue)
        # return
        return ret


    # reactivate task
    def reactivateTask(self,jediTaskID):
        # update datasets and task status
        ret = self.taskBuffer.reactivateTask(jediTaskID)
        return ret


    # get task status
    def getTaskStatus(self,jediTaskID):
        # update task status
        ret = self.taskBuffer.getTaskStatus(jediTaskID)
        return ret[0]


    # reassign share
    def reassignShare(self, jedi_task_ids, share_dest, reassign_running):
        return self.taskBuffer.reassignShare(jedi_task_ids, share_dest, reassign_running)

    # get global share status overview of the grid
    def getGShareStatus(self):
        return self.taskBuffer.getGShareStatus()

    # list tasks in share
    def listTasksInShare(self, gshare, status):
        return self.taskBuffer.listTasksInShare(gshare, status)


    # get taskParamsMap
    def getTaskParamsMap(self, jediTaskID):
        # get taskParamsMap
        ret = self.taskBuffer.getTaskParamsMap(jediTaskID)
        return ret

    # update workers
    def updateWorkers(self,user,host,harvesterID,data):
        ret = self.taskBuffer.updateWorkers(harvesterID, data)
        if ret is None:
            retVal = (False,'database error in the panda server')
        else:
            retVal = (True,ret)
        # serialize 
        return json.dumps(retVal)

    # update workers
    def updateServiceMetrics(self, user, host, harvesterID, data):
        ret = self.taskBuffer.updateServiceMetrics(harvesterID, data)
        if ret is None:
            retVal = (False,'database error in the panda server')
        else:
            retVal = (True,ret)
        # serialize
        return json.dumps(retVal)

    # add harvester dialog messages
    def addHarvesterDialogs(self, user, harvesterID, dialogs):
        ret = self.taskBuffer.addHarvesterDialogs(harvesterID, dialogs)
        if not ret:
            retVal = (False,'database error in the panda server')
        else:
            retVal = (True,'')
        # serialize 
        return json.dumps(retVal)


    # heartbeat for harvester
    def harvesterIsAlive(self,user,host,harvesterID,data):
        ret = self.taskBuffer.harvesterIsAlive(user,host,harvesterID,data)
        if ret is None:
            retVal = (False,'database error')
        else:
            retVal = (True,ret)
        # serialize 
        return json.dumps(retVal)

    # report stat of workers
    def reportWorkerStats(self, harvesterID, siteName, paramsList):
        return self.taskBuffer.reportWorkerStats(harvesterID, siteName, paramsList)

    # set num slots for workload provisioning
    def setNumSlotsForWP(self, pandaQueueName, numSlots, gshare, resourceType, validPeriod):
        retVal = self.taskBuffer.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)
        # serialize 
        return json.dumps(retVal)

    # enable jumbo jobs
    def enableJumboJobs(self, jediTaskID, totalJumboJobs, nJumboPerSite):
        retVal = self.taskBuffer.enableJumboJobs(jediTaskID, totalJumboJobs, nJumboPerSite)
        if totalJumboJobs > 0 and retVal[0] == 0:
            self.avalancheTask(jediTaskID, 'panda', True)
        # serialize 
        return json.dumps(retVal)

    # get user job metadata
    def getUserJobMetadata(self, jediTaskID):
        retVal = self.taskBuffer.getUserJobMetadata(jediTaskID)
        # serialize 
        return json.dumps(retVal)

    # get jumbo job datasets
    def getJumboJobDatasets(self, n_days, grace_period):
        retVal = self.taskBuffer.getJumboJobDatasets(n_days, grace_period)
        # serialize 
        return json.dumps(retVal)

    # sweep panda queue
    def sweepPQ(self, panda_queue, status_list, ce_list, submission_host_list):
        # deserialize variables
        try:
            panda_queue_des = json.loads(panda_queue)
            status_list_des = json.loads(status_list)
            ce_list_des = json.loads(ce_list)
            submission_host_list_des = json.loads(submission_host_list)
        except Exception:
            _logger.error('Problem deserializing variables')

        # reassign jobs
        ret = self.taskBuffer.sweepPQ(panda_queue_des, status_list_des, ce_list_des, submission_host_list_des)
        # serialize
        return WrappedPickle.dumps(ret)


# Singleton
userIF = UserIF()
del UserIF


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmpKey in req.subprocess_env:
        tmpVal = req.subprocess_env[tmpKey]
        # compact credentials
        if tmpKey.startswith('GRST_CRED_'):
            # VOMS attribute
            if tmpVal.startswith('VOMS'):
                # FQAN
                fqan = tmpVal.split()[-1]
                # append
                fqans.append(fqan)
        # old style         
        elif tmpKey.startswith('GRST_CONN_'):
            tmpItems = tmpVal.split(':')
            # FQAN
            if len(tmpItems)==2 and tmpItems[0]=='fqan':
                fqans.append(tmpItems[-1])
    # return
    return fqans


# get DN
def _getDN(req):
    realDN = ''
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        realDN = req.subprocess_env['SSL_CLIENT_S_DN']
        # remove redundant CN
        realDN = re.sub('/CN=limited proxy','',realDN)
        realDN = re.sub('/CN=proxy(/CN=proxy)+','/CN=proxy',realDN)
    return realDN
                                        

# check role
def _isProdRoleATLAS(req):
    # check role
    prodManager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
            if fqan.startswith(rolePat):
                return True
    return False


# get primary working group with prod role
def _getWGwithPR(req):
    try:
        fqans = _getFQAN(req)
        for fqan in fqans:
            tmpMatch = re.search('/[^/]+/([^/]+)/Role=production',fqan)
            if tmpMatch is not None:
                # ignore usatlas since it is used as atlas prod role
                tmpWG = tmpMatch.group(1)
                if not tmpWG in ['','usatlas']:
                    return tmpWG.split('-')[-1].lower()
    except Exception:
        pass
    return None


    
"""
web service interface

"""

# security check
def isSecure(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # disable limited proxy
    if '/CN=limited proxy' in req.subprocess_env['SSL_CLIENT_S_DN']:
        _logger.warning("access via limited proxy : %s" % req.subprocess_env['SSL_CLIENT_S_DN'])
        return False
    return True


# submit jobs
def submitJobs(req,jobs,toPending=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)
    # get FQAN
    fqans = _getFQAN(req)
    # hostname
    host = req.get_remote_host()
    # production Role
    prodRole = _isProdRoleATLAS(req)
    # to pending
    if toPending == 'True':
        toPending = True
    else:
        toPending = False
    return userIF.submitJobs(jobs,user,host,fqans,prodRole,toPending)


# run task assignment
def runTaskAssignment(req,jobs):
    # check security
    if not isSecure(req):
        return "False"
    return userIF.runTaskAssignment(jobs)


# get job status
def getJobStatus(req,ids):
    return userIF.getJobStatus(ids)


# get PandaID with jobexeID
def getPandaIDwithJobExeID(req,ids):
    return userIF.getPandaIDwithJobExeID(ids)


# get queued analysis jobs at a site
def getQueuedAnalJobs(req,site):
    # check security
    if not isSecure(req):
        return "ERROR: SSL is required"
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)
    return userIF.getQueuedAnalJobs(site,user)


# get active datasets
def getActiveDatasets(req,computingSite,prodSourceLabel='managed'):
    return userIF.getActiveDatasets(computingSite,prodSourceLabel)


# get assigning task
def getAssigningTask(req):
    return userIF.getAssigningTask()


# get assigned cloud for tasks
def seeCloudTask(req,ids):
    return userIF.seeCloudTask(ids)


# set task by user
def setCloudTaskByUser(req,tid,cloud='',status=''):
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # check role
    if not _isProdRoleATLAS(req):
        return "ERROR: production role is required"
    return userIF.setCloudTaskByUser(user,tid,cloud,status)


# set debug mode
def setDebugMode(req,pandaID,modeOn):
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # check role
    prodManager = _isProdRoleATLAS(req)
    # mode
    if modeOn == 'True':
        modeOn = True
    else:
        modeOn = False
    # get the primary working group with prod role
    workingGroup = _getWGwithPR(req)
    # exec    
    return userIF.setDebugMode(user,pandaID,prodManager,modeOn,workingGroup)


# insert sandbox file info
def insertSandboxFileInfo(req,userName,fileName,fileSize,checkSum):
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # check role
    prodManager = _isProdRoleATLAS(req)
    if not prodManager:
        return "ERROR: missing role"
    # hostname
    hostName = req.get_remote_host()
    # exec    
    return userIF.insertSandboxFileInfo(userName,hostName,fileName,fileSize,checkSum)


# check duplicated sandbox file
def checkSandboxFile(req,fileSize,checkSum):
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # exec    
    return userIF.checkSandboxFile(user,fileSize,checkSum)


# query PandaIDs
def queryPandaIDs(req,ids):
    return userIF.queryPandaIDs(ids)


# query job info per cloud
def queryJobInfoPerCloud(req,cloud,schedulerID=None):
    return userIF.queryJobInfoPerCloud(cloud,schedulerID)


# get PandaIDs at site
def getPandaIDsSite(req,site,status,limit=500):
    return userIF.getPandaIDsSite(site,status,limit)


# get PandaIDs to be updated in prodDB
def getJobsToBeUpdated(req,limit=5000,lockedby=''):
    limit = int(limit)
    return userIF.getJobsToBeUpdated(limit,lockedby)


# update prodDBUpdateTimes
def updateProdDBUpdateTimes(req,params):
    # check security
    if not isSecure(req):
        return False
    return userIF.updateProdDBUpdateTimes(params)


# get job statistics
def getJobStatistics(req,sourcetype=None):
    return userIF.getJobStatistics(sourcetype)


# get highest prio jobs
def getHighestPrioJobStat(req,perPG=None,useMorePG=None):
    if perPG == 'True':
        perPG = True
    else:
        perPG = False
    if useMorePG == 'True':
        useMorePG = pandaserver.taskbuffer.ProcessGroups.extensionLevel_1
    elif useMorePG in ['False',None]:
        useMorePG = False
    else:
        try:
            useMorePG = int(useMorePG)
        except Exception:
            useMorePG = False
    return userIF.getHighestPrioJobStat(perPG,useMorePG)


# get job statistics for Babmoo
def getJobStatisticsForBamboo(req,useMorePG=None):
    if useMorePG == 'True':
        useMorePG = pandaserver.taskbuffer.ProcessGroups.extensionLevel_1
    elif useMorePG in ['False',None]:
        useMorePG = False
    else:
        try:
            useMorePG = int(useMorePG)
        except Exception:
            useMorePG = False
    return userIF.getJobStatisticsForBamboo(useMorePG)


# get the number of waiting jobs per site and user
def getJobStatisticsPerUserSite(req):
    return userIF.getJobStatisticsPerUserSite()


# get job statistics per site and resource
def getJobStatisticsPerSiteResource(req, timeWindow=None):
    return userIF.getJobStatisticsPerSiteResource(timeWindow)


# get job statistics per site
def getJobStatisticsPerSite(req,predefined='False',workingGroup='',countryGroup='',jobType='',
                            minPriority=None,readArchived=None):
    if predefined=='True':
        predefined=True
    else:
        predefined=False
    if minPriority is not None:
        try:
            minPriority = int(minPriority)
        except Exception:
            minPriority = None
    if readArchived=='True':
        readArchived = True
    elif readArchived=='False':
        readArchived = False
    else:
        host = req.get_remote_host()
        # read jobsArchived for panglia
        if re.search('panglia.*\.triumf\.ca$',host) is not None or host in ['gridweb.triumf.ca']:
            readArchived = True
        else:
            readArchived = False
    return userIF.getJobStatisticsPerSite(predefined,workingGroup,countryGroup,jobType,
                                          minPriority,readArchived)


# get job statistics per site with label
def getJobStatisticsWithLabel(req,site=''):
    return userIF.getJobStatisticsWithLabel(site)


# query last files in datasets
def queryLastFilesInDataset(req,datasets):
    return userIF.queryLastFilesInDataset(datasets)


# get input files currently in used for analysis
def getFilesInUseForAnal(req,outDataset):
    return userIF.getFilesInUseForAnal(outDataset)


# get list of dis dataset to get input files in shadow
def getDisInUseForAnal(req,outDataset):
    return userIF.getDisInUseForAnal(outDataset)


# get input LFNs currently in use for analysis with shadow dis
def getLFNsInUseForAnal(req,inputDisList):
    return userIF.getLFNsInUseForAnal(inputDisList)


# kill jobs
def killJobs(req,ids,code=None,useMailAsID=None,killOpts=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodManager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # use email address as ID
    if useMailAsID == 'True':
        useMailAsID = True
    else:
        useMailAsID = False
    # hostname
    host = req.get_remote_host()
    # options
    if killOpts is None:
        killOpts = []
    else:
        killOpts = killOpts.split(',')
    return userIF.killJobs(ids,user,host,code,prodManager,useMailAsID,fqans,killOpts)


# reassign jobs
def reassignJobs(req,ids,forPending=None,firstSubmission=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    # for pending
    if forPending == 'True':
        forPending = True
    else:
        forPending = False
    # first submission
    if firstSubmission == 'False':
        firstSubmission = False
    else:
        firstSubmission = True
    return userIF.reassignJobs(ids,user,host,forPending,firstSubmission)


# resubmit jobs
def resubmitJobs(req,ids):
    # check security
    if not isSecure(req):
        return False
    return userIF.resubmitJobs(ids)


# change job priorities
def changeJobPriorities(req,newPrioMap=None):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    ret = userIF.changeJobPriorities(user,prodRole,newPrioMap)
    return WrappedPickle.dumps(ret)


# get list of site spec
def getSiteSpecs(req,siteType=None):
    if siteType is not None:
        return userIF.getSiteSpecs(siteType)
    else:
        return userIF.getSiteSpecs()

# get list of cloud spec
def getCloudSpecs(req):
    return userIF.getCloudSpecs()

# get list of cache prefix
def getCachePrefixes(req):
    return userIF.getCachePrefixes()

# get list of cmtConfig
def getCmtConfigList(self,relaseVer):
    return userIF.getCmtConfigList(relaseVer)

# get client version
def getPandaClientVer(req):
    return userIF.getPandaClientVer()
    
# get nPilots
def getNumPilots(req):
    return userIF.getNumPilots()


# retry failed subjobs in running job
def retryFailedJobsInActive(req,jobID):
    # check SSL
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    # convert jobID to long
    try:
        jobID = long(jobID)
    except Exception:
        return "ERROR: jobID is not an integer"
    return userIF.retryFailedJobsInActive(dn,jobID)


# get serial number for group job
def getSerialNumberForGroupJob(req):
    # check SSL
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    return userIF.getSerialNumberForGroupJob(dn)


# get script for offline running
def getScriptOfflineRunning(req,pandaID,days=None):
    try:
        if days is not None:
            days = int(days)
    except Exception:
        days=None
    return userIF.getScriptOfflineRunning(pandaID,days)


# register proxy key
def registerProxyKey(req,credname,origin,myproxy):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    # get expiration date
    if 'SSL_CLIENT_V_END' not in req.subprocess_env:
        return False
    params = {}
    params['dn'] = _getDN(req)
    # set parameters
    params['credname'] = credname
    params['origin']   = origin
    params['myproxy']  = myproxy
    # convert SSL_CLIENT_V_END
    try:
        expTime = req.subprocess_env['SSL_CLIENT_V_END']
        # remove redundant white spaces
        expTime = re.sub('\s+',' ',expTime)
        # convert to timestamp
        expTime = time.strptime(expTime,'%b %d %H:%M:%S %Y %Z')
        params['expires']  = time.strftime('%Y-%m-%d %H:%M:%S',expTime)
    except Exception:
        _logger.error("registerProxyKey : failed to convert %s" % \
                      req.subprocess_env['SSL_CLIENT_V_END'])
    # execute
    return userIF.registerProxyKey(params)


# register proxy key
def getProxyKey(req):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    dn = _getDN(req)
    # execute
    return userIF.getProxyKey(dn)


# get JobIDs in a time range
def getJobIDsInTimeRange(req,timeRange,dn=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    if dn is None:
        dn = _getDN(req)
    _logger.debug("getJobIDsInTimeRange %s %s" % (dn,timeRange))
    # execute
    return userIF.getJobIDsInTimeRange(dn,timeRange)


# get active JediTasks in a time range
def getJediTasksInTimeRange(req, timeRange, dn=None, fullFlag=None, minTaskID=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    if dn is None:
        dn = _getDN(req)
    if fullFlag == 'True':
        fullFlag = True
    else:
        fullFlag = False
    try:
        minTaskID = long(minTaskID)
    except Exception:
        minTaskID = None
    _logger.debug("getJediTasksInTimeRange %s %s" % (dn,timeRange))
    # execute
    return userIF.getJediTasksInTimeRange(dn, timeRange, fullFlag, minTaskID)


# get details of JediTask
def getJediTaskDetails(req,jediTaskID,fullFlag,withTaskInfo):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    # option
    if fullFlag == 'True':
        fullFlag = True
    else:
        fullFlag = False
    if withTaskInfo == 'True':
        withTaskInfo = True
    else:
        withTaskInfo = False
    _logger.debug("getJediTaskDetails %s %s %s" % (jediTaskID,fullFlag,withTaskInfo))
    # execute
    return userIF.getJediTaskDetails(jediTaskID,fullFlag,withTaskInfo)


# get PandaIDs for a JobID
def getPandIDsWithJobID(req,jobID,nJobs,dn=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    if dn is None:
        dn = _getDN(req)
    _logger.debug("getPandIDsWithJobID %s JobID=%s nJobs=%s" % (dn,jobID,nJobs))
    # execute
    return userIF.getPandIDsWithJobID(dn,jobID,nJobs)


# check merge job generation status
def checkMergeGenerationStatus(req,jobID,dn=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    if dn is None:
        dn = _getDN(req)
    _logger.debug("checkMergeGenerationStatus %s JobID=%s" % (dn,jobID))
    # execute
    return userIF.checkMergeGenerationStatus(dn,jobID)


# get slimmed file info with PandaIDs
def getSlimmedFileInfoPandaIDs(req,ids):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    dn = _getDN(req)
    return userIF.getSlimmedFileInfoPandaIDs(ids,dn)


# get full job status
def getFullJobStatus(req,ids):
    # check security
    if not isSecure(req):
        return False
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    dn = _getDN(req)
    return userIF.getFullJobStatus(ids,dn)


# get a list of DN/myproxy pass phrase/queued job count at a site
def getNUserJobs(req,siteName):
    # check security
    prodManager = False
    if not isSecure(req):
        return "Failed : HTTPS connection is required"
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production',
                        '/atlas/Role=production',
                        '/atlas/usatlas/Role=pilot',                        
                        '/atlas/Role=pilot',
                        ]:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # only prod managers can use this method
    if not prodManager:
        return "Failed : VOMS authorization failure. production or pilot role required"
    # execute
    return userIF.getNUserJobs(siteName)


# add account to siteaccess
def addSiteAccess(req,siteID):
    # check security
    if not isSecure(req):
        return "False"        
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "False"        
    dn = req.subprocess_env['SSL_CLIENT_S_DN']
    return userIF.addSiteAccess(siteID,dn)


# list site access
def listSiteAccess(req,siteID=None,longFormat=False):
    # check security
    if not isSecure(req):
        return "False"
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "False"
    # set DN if siteID is none
    dn = None
    if siteID==None:
        dn = req.subprocess_env['SSL_CLIENT_S_DN']
    # convert longFormat option
    if longFormat == 'True':
        longFormat = True
    else:
        longFormat = False
    return userIF.listSiteAccess(siteID,dn,longFormat)


# update site access
def updateSiteAccess(req,method,siteid,userName,attrValue=''):
    # check security
    if not isSecure(req):
        return "non HTTPS"
    # get DN
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return "invalid DN"
    # set requester's DN
    requesterDN = req.subprocess_env['SSL_CLIENT_S_DN']
    # update
    return userIF.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)


# insert task params
def insertTaskParams(req,taskParams=None,properErrorCode=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check format
    try:
        json.loads(taskParams)
    except Exception:
        return WrappedPickle.dumps((False,'failed to decode json'))        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # get FQANs
    fqans = _getFQAN(req)
    ret = userIF.insertTaskParams(taskParams,user,prodRole,fqans,properErrorCode)
    return WrappedPickle.dumps(ret)



# kill task
def killTask(req,jediTaskID=None,properErrorCode=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    # check security
    if not isSecure(req):
        if properErrorCode:
            return WrappedPickle.dumps((100,'secure connection is required'))
        else:
            return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        if properErrorCode:
            return WrappedPickle.dumps((101,'jediTaskID must be an integer'))        
        else:
            return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.killTask(jediTaskID,user,prodRole,properErrorCode)
    return WrappedPickle.dumps(ret)



# retry task
def retryTask(req,jediTaskID,properErrorCode=None,newParams=None,noChildRetry=None,
              discardEvents=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    if noChildRetry == 'True':
        noChildRetry = True
    else:
        noChildRetry = False
    if discardEvents == 'True':
        discardEvents = True
    else:
        discardEvents = False
    # check security
    if not isSecure(req):
        if properErrorCode:
            return WrappedPickle.dumps((100,'secure connection is required'))
        else:
            return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        if properErrorCode:
            return WrappedPickle.dumps((101,'jediTaskID must be an integer'))        
        else:
            return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.retryTask(jediTaskID,user,prodRole,properErrorCode,newParams,noChildRetry,
                           discardEvents)
    return WrappedPickle.dumps(ret)



# reassign task to site/cloud
def reassignTask(req,jediTaskID,site=None,cloud=None,nucleus=None,soft=None,mode=None):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((100,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((101,'jediTaskID must be an integer'))        
    # site or cloud
    if site is not None:
        # set 'y' to go back to oldStatus immediately
        comComment = 'site:{0}:y'.format(site)
    elif nucleus is not None:
        comComment = 'nucleus:{0}:n'.format(nucleus)
    else:
        comComment = 'cloud:{0}:n'.format(cloud)
    if mode == 'nokill':
        comComment += ':nokill reassign'
    elif mode == 'soft' or soft == 'True':
        comComment += ':soft reassign'
    ret = userIF.reassignTask(jediTaskID,user,prodRole,comComment)
    return WrappedPickle.dumps(ret)



# finish task
def finishTask(req,jediTaskID=None,properErrorCode=None,soft=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    qualifier = None
    if soft == 'True':
        qualifier = 'soft'
    # check security
    if not isSecure(req):
        if properErrorCode:
            return WrappedPickle.dumps((100,'secure connection is required'))
        else:
            return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        if properErrorCode:
            return WrappedPickle.dumps((101,'jediTaskID must be an integer'))        
        else:
            return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.finishTask(jediTaskID,user,prodRole,properErrorCode,
                            qualifier)
    return WrappedPickle.dumps(ret)


# reload input
def reloadInput(req, jediTaskID, properErrorCode=None):
    if properErrorCode == 'True':
        properErrorCode = True
    else:
        properErrorCode = False
    # check security
    if not isSecure(req):
        if properErrorCode:
            return WrappedPickle.dumps((100,'secure connection is required'))
        else:
            return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        if properErrorCode:
            return WrappedPickle.dumps((101,'jediTaskID must be an integer'))        
        else:
            return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.reloadInput(jediTaskID,user,prodRole)
    return WrappedPickle.dumps(ret)



# get retry history
def getRetryHistory(req,jediTaskID=None):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))        
    ret = userIF.getRetryHistory(jediTaskID,user)
    return WrappedPickle.dumps(ret)



# change task priority
def changeTaskPriority(req,jediTaskID=None,newPriority=None):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    if not prodRole:
        return "Failed : production or pilot role required"
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))        
    # check priority
    try:
        newPriority = long(newPriority)
    except Exception:
        return WrappedPickle.dumps((False,'newPriority must be an integer'))        
    ret = userIF.changeTaskPriority(jediTaskID,newPriority)
    return WrappedPickle.dumps(ret)



# increase attempt number for unprocessed files
def increaseAttemptNrPanda(req,jediTaskID,increasedNr):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    ret = None
    if not prodRole:
        ret = 3,"production or pilot role required"
    # check jediTaskID
    if ret is None:
        try:
            jediTaskID = long(jediTaskID)
        except Exception:
            ret = 4,'jediTaskID must be an integer'
    # check increase
    if ret is None:
        wrongNr = False
        try:
            increasedNr = long(increasedNr)
        except Exception:
            wrongNr = True
        if wrongNr or increasedNr<0:
            ret = 4,'increase must be a positive integer'
    # exec
    if ret is None:
        ret = userIF.increaseAttemptNrPanda(jediTaskID,increasedNr)
    return WrappedPickle.dumps(ret)



# change task attribute
def changeTaskAttributePanda(req,jediTaskID,attrName,attrValue):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    if not prodRole:
        return WrappedPickle.dumps((False,"production or pilot role required"))
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))        
    # check attribute
    if not attrName in ['ramCount','wallTime','cpuTime','coreCount']:
        return WrappedPickle.dumps((2,"disallowed to update {0}".format(attrName)))
    ret = userIF.changeTaskAttributePanda(jediTaskID,attrName,attrValue)
    return WrappedPickle.dumps((ret,None))



# change split rule for task
def changeTaskSplitRulePanda(req,jediTaskID,attrName,attrValue):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    if not prodRole:
        return WrappedPickle.dumps((False,"production or pilot role required"))
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))        
    # check attribute
    if not attrName in ['TW','EC','ES','MF','NG','NI','NF','NJ']:
        return WrappedPickle.dumps((2,"disallowed to update {0}".format(attrName)))
    ret = userIF.changeTaskSplitRulePanda(jediTaskID,attrName,attrValue)
    return WrappedPickle.dumps((ret,None))



# pause task
def pauseTask(req,jediTaskID):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, 'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, 'jediTaskID must be an integer'))
    ret = userIF.pauseTask(jediTaskID,user,prodRole)
    return WrappedPickle.dumps(ret)


# resume task
def resumeTask(req,jediTaskID):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, 'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, 'jediTaskID must be an integer'))
    ret = userIF.resumeTask(jediTaskID,user,prodRole)
    return WrappedPickle.dumps(ret)


# force avalanche for task
def avalancheTask(req,jediTaskID):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False, 'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False, 'jediTaskID must be an integer'))
    ret = userIF.avalancheTask(jediTaskID,user,prodRole)
    return WrappedPickle.dumps(ret)



# kill unfinished jobs
def killUnfinishedJobs(req,jediTaskID,code=None,useMailAsID=None):
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodManager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # use email address as ID
    if useMailAsID == 'True':
        useMailAsID = True
    else:
        useMailAsID = False
    # hostname
    host = req.get_remote_host()
    # get PandaIDs
    ids = userIF.getPandaIDsWithTaskID(jediTaskID)
    # kill
    return userIF.killJobs(ids,user,host,code,prodManager,useMailAsID,fqans)



# change modificationTime for task
def changeTaskModTimePanda(req,jediTaskID,diffValue):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)        
    # check role
    prodRole = _isProdRoleATLAS(req)
    # only prod managers can use this method
    if not prodRole:
        return WrappedPickle.dumps((False,"production or pilot role required"))
    # check jediTaskID
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    try:
        diffValue = int(diffValue)
        attrValue = datetime.datetime.now() + datetime.timedelta(hours=diffValue)
    except Exception:
        return WrappedPickle.dumps((False,'failed to convert {0} to time diff'.format(diffValue)))
    ret = userIF.changeTaskAttributePanda(jediTaskID,'modificationTime',attrValue)
    return WrappedPickle.dumps((ret,None))



# get PandaIDs with TaskID
def getPandaIDsWithTaskID(req,jediTaskID):
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    idsStr = userIF.getPandaIDsWithTaskID(jediTaskID)
    # deserialize
    ids = WrappedPickle.loads(idsStr)

    return WrappedPickle.dumps(ids)



# reactivate Task
def reactivateTask(req,jediTaskID):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))

    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.reactivateTask(jediTaskID)

    return WrappedPickle.dumps(ret)



# get task status
def getTaskStatus(req,jediTaskID):
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.getTaskStatus(jediTaskID)
    return WrappedPickle.dumps(ret)


# reassign share
def reassignShare(req, jedi_task_ids_pickle, share, reassign_running):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)
    # check role
    prod_role = _isProdRoleATLAS(req)
    if not prod_role:
        return WrappedPickle.dumps((False,"production or pilot role required"))

    jedi_task_ids = WrappedPickle.loads(jedi_task_ids_pickle)
    _logger.debug('reassignShare: jedi_task_ids: {0}, share: {1}, reassign_running: {2}'.format(jedi_task_ids,
                                                                                                share,
                                                                                                reassign_running))

    if not ((isinstance(jedi_task_ids, list) or (isinstance(jedi_task_ids, tuple)) and isinstance(share, str))):
        return WrappedPickle.dumps((False, 'jedi_task_ids must be tuple/list and share must be string'))

    ret = userIF.reassignShare(jedi_task_ids, share, reassign_running)
    return WrappedPickle.dumps(ret)


# list tasks in share
def listTasksInShare(req, gshare, status):
    # check security
    if not isSecure(req):
        return WrappedPickle.dumps((False,'secure connection is required'))
    # get DN
    user = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        user = _getDN(req)
    # check role
    prod_role = _isProdRoleATLAS(req)
    if not prod_role:
        return WrappedPickle.dumps((False,"production or pilot role required"))

    _logger.debug('listTasksInShare: gshare: {0}, status: {1}'.format(gshare, status))

    if not ((isinstance(gshare, str) and isinstance(status, str))):
        return WrappedPickle.dumps((False, 'gshare and status must be of type string'))

    ret = userIF.listTasksInShare(gshare, status)
    return WrappedPickle.dumps(ret)

# get taskParamsMap with TaskID
def getTaskParamsMap(req,jediTaskID):
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    ret = userIF.getTaskParamsMap(jediTaskID)
    return WrappedPickle.dumps(ret)


# update workers
def updateWorkers(req,harvesterID,workers):
    # check security
    if not isSecure(req):
        return json.dumps((False,"SSL is required"))
    # get DN
    user = _getDN(req)        
    # hostname
    host = req.get_remote_host()
    retVal = None
    tStart = datetime.datetime.utcnow()
    # convert
    try:
        data = json.loads(workers)
    except Exception:
        retVal = json.dumps((False,"failed to load JSON"))
    # update
    if retVal is None:
        retVal = userIF.updateWorkers(user,host,harvesterID,data)
    tDelta = datetime.datetime.utcnow() - tStart
    _logger.debug("updateWorkers %s took %s.%03d sec" % (harvesterID, tDelta.seconds, tDelta.microseconds/1000))
    return retVal


# update workers
def updateServiceMetrics(req, harvesterID, metrics):
    # check security
    if not isSecure(req):
        return json.dumps((False,"SSL is required"))

    user = _getDN(req)

    host = req.get_remote_host()
    retVal = None
    tStart = datetime.datetime.utcnow()

    # convert
    try:
        data = json.loads(metrics)
    except Exception:
        retVal = json.dumps((False,"failed to load JSON"))

    # update
    if retVal is None:
        retVal = userIF.updateServiceMetrics(user, host, harvesterID, data)

    tDelta = datetime.datetime.utcnow() - tStart

    _logger.debug("updateServiceMetrics %s took %s.%03d sec" % (harvesterID, tDelta.seconds, tDelta.microseconds/1000))
    return retVal

# add harvester dialog messages
def addHarvesterDialogs(req, harvesterID, dialogs):
    # check security
    if not isSecure(req):
        return json.dumps((False,"SSL is required"))
    # get DN
    user = _getDN(req)        
    # convert
    try:
        data = json.loads(dialogs)
    except Exception:
        return json.dumps((False,"failed to load JSON"))
    # update
    return userIF.addHarvesterDialogs(user,harvesterID,data)


# heartbeat for harvester
def harvesterIsAlive(req,harvesterID,data=None):
    # check security
    if not isSecure(req):
        return json.dumps((False,"SSL is required"))
    # get DN
    user = _getDN(req)        
    # hostname
    host = req.get_remote_host()
    # convert
    try:
        if data is not None:
            data = json.loads(data)
        else:
            data = dict()
    except Exception:
        return json.dumps((False,"failed to load JSON"))
    # update
    return userIF.harvesterIsAlive(user,host,harvesterID,data)


# report stat of workers
def reportWorkerStats(req, harvesterID, siteName, paramsList):
    # check security
    if not isSecure(req):
        return json.dumps((False,"SSL is required"))
    # update
    ret = userIF.reportWorkerStats(harvesterID, siteName, paramsList)
    return json.dumps(ret)


# set num slots for workload provisioning
def setNumSlotsForWP(req, pandaQueueName, numSlots, gshare=None, resourceType=None, validPeriod=None):
    # check security
    if not isSecure(req):
        return json.dumps((100, "SSL is required"))
    # check role
    if not _isProdRoleATLAS(req):
        return json.dumps((101, "production role is required in the certificate"))
    # convert
    try:
        numSlots = int(numSlots)
    except Exception:
        return json.dumps((102, "numSlots must be int"))
    # execute
    return userIF.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)


# enable jumbo jobs
def enableJumboJobs(req, jediTaskID, nJumboJobs, nJumboPerSite=None):
    # check security
    if not isSecure(req):
        return json.dumps((100, "SSL is required"))
    # check role
    if not _isProdRoleATLAS(req):
        return json.dumps((101, "production role is required in the certificate"))
    # convert
    try:
        nJumboJobs = int(nJumboJobs)
    except Exception:
        return json.dumps((102, "nJumboJobs must be int"))
    try:
        nJumboPerSite = int(nJumboPerSite)
    except Exception:
        nJumboPerSite = nJumboJobs
    # execute
    return userIF.enableJumboJobs(jediTaskID, nJumboJobs, nJumboPerSite)


# get user job metadata
def getUserJobMetadata(req, jediTaskID):
    try:
        jediTaskID = long(jediTaskID)
    except Exception:
        return WrappedPickle.dumps((False,'jediTaskID must be an integer'))
    return userIF.getUserJobMetadata(jediTaskID)



# get jumbo job datasets
def getJumboJobDatasets(req, n_days, grace_period=0):
    try:
        n_days = long(n_days)
    except Exception:
        return WrappedPickle.dumps((False,'wrong n_days'))
    try:
        grace_period = long(grace_period)
    except Exception:
        return WrappedPickle.dumps((False,'wrong grace_period'))
    return userIF.getJumboJobDatasets(n_days, grace_period)


# get Global Share overview
def getGShareStatus(req):
    # check security
    if not isSecure(req):
        return json.dumps((False,"SSL is required"))
    ret = userIF.getGShareStatus()
    return json.dumps(ret)


# send Harvester the command to clean up the workers for a panda queue
def sweepPQ(req, panda_queue, status_list, ce_list, submission_host_list):
    # check security
    if not isSecure(req):
        return json.dumps((False,"SSL is required"))
    # check role
    prod_role = _isProdRoleATLAS(req)
    if not prod_role:
        return json.dumps((False, "production or pilot role required"))

    return json.dumps((True, userIF.sweepPQ(panda_queue, status_list, ce_list, submission_host_list)))
