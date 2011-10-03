'''
provide web interface to users

'''

import re
import sys
import time
import types
import cPickle as pickle
import jobdispatcher.Protocol as Protocol
import brokerage.broker
import taskbuffer.ProcessGroups
from config import panda_config
from taskbuffer.JobSpec import JobSpec
from taskbuffer.WrappedPickle import WrappedPickle
from brokerage.SiteMapper import SiteMapper
from pandalogger.PandaLogger import PandaLogger
from ReBroker import ReBroker
from taskbuffer import PrioUtil

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
    def submitJobs(self,jobsStr,user,host,userFQANs,prodRole=False):
        try:
            # deserialize jobspecs
            jobs = WrappedPickle.loads(jobsStr)
            _logger.debug("submitJobs %s len:%s FQAN:%s" % (user,len(jobs),str(userFQANs)))
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error("too may jobs more than %s" % maxJobs)
                jobs = jobs[:maxJobs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("submitJobs : %s %s" % (type,value))
            jobs = []
        # check prodSourceLabel
        try:
            goodProdSourceLabel = True
            for tmpJob in jobs:
                # prevent internal jobs from being submitted from outside
                if tmpJob.prodSourceLabel in taskbuffer.ProcessGroups.internalSourceLabels:
                    _logger.error("submitJobs %s wrong prodSourceLabel=%s" % (user,tmpJob.prodSourceLabel))
                    goodProdSourceLabel = False
                    break
                # check production role
                if tmpJob.prodSourceLabel in ['managed']:
                    if not prodRole:
                        _logger.error("submitJobs %s missing prod-role for prodSourceLabel=%s" % (user,tmpJob.prodSourceLabel))
                        goodProdSourceLabel = False
                        break
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("submitJobs : checking goodProdSourceLabel %s %s" % (errType,errValue))
            goodProdSourceLabel = False
        # reject injection for bad prodSourceLabel
        if not goodProdSourceLabel:
            return "ERROR: production role is required for production jobs"
        # store jobs
        ret = self.taskBuffer.storeJobs(jobs,user,forkSetupper=True,fqans=userFQANs,
                                        hostname=host)
        _logger.debug("submitJobs %s ->:%s" % (user,len(ret)))
        # serialize 
        return pickle.dumps(ret)


    # logger interface
    def sendLogInfo(self,user,msgType,msgListStr):
        try:
            # deserialize message
            msgList = WrappedPickle.loads(msgListStr)
            # short user name
            cUID = self.taskBuffer.cleanUserID(user)
            # logging
            iMsg = 0
            for msgBody in msgList:
                # make message
                message = "dn='%s' %s" % (cUID,msgBody)
                # send message to logger
                if msgType in ['analy_brokerage']:
                    brokerage.broker.sendMsgToLogger(message)
                # get logger
                _pandaLogger = PandaLogger()            
                _pandaLogger.lock()
                _pandaLogger.setParams({'Type':msgType})
                logger = _pandaLogger.getHttpLogger(panda_config.loggername)
                # add message
                logger.info(message)
                # release HTTP handler
                _pandaLogger.release()
                # sleep
                iMsg += 1
                if iMsg % 5 == 0:
                    time.sleep(1)
        except:
            pass
        # return
        return True


    # run task assignment
    def runTaskAssignment(self,jobsStr):
        try:
            # deserialize jobspecs
            jobs = WrappedPickle.loads(jobsStr)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("runTaskAssignment : %s %s" % (type,value))
            jobs = []
        # run
        ret = self.taskBuffer.runTaskAssignment(jobs)
        # serialize 
        return pickle.dumps(ret)


    # get serial number for group job
    def getSerialNumberForGroupJob(self,name):
        # get
        ret = self.taskBuffer.getSerialNumberForGroupJob(name)
        # serialize 
        return pickle.dumps(ret)


    # run rebrokerage
    def runReBrokerage(self,dn,jobID,cloud,strExcludedSite,forceRebro):
        returnVal = "True"
        try:
            if strExcludedSite == None:
                # excludedSite is unchanged
                excludedSite = None
            else:
                # convert str to list
                excludedSite = []
                for tmpItem in strExcludedSite.split(','):
                    if tmpItem != '':
                        excludedSite.append(tmpItem)
            _logger.debug("runReBrokerage %s JobID:%s cloud=%s ex=%s forceOpt=%s" % (dn,jobID,cloud,str(excludedSite),forceRebro))
            # instantiate ReBroker
            thr = ReBroker(self.taskBuffer,cloud,excludedSite,forceOpt=forceRebro,userRequest=True)
            # lock
            stLock,retLock = thr.lockJob(dn,jobID)
            # failed
            if not stLock:
                returnVal = "ERROR: "+retLock
            else:
                # start ReBroker
                thr.start()
        except:
            errType,errValue,errTraceBack = sys.exc_info()
            _logger.error("runReBrokerage: %s %s" % (errType,errValue))
            returnVal = "ERROR: runReBrokerage crashed"
        # return
        return returnVal


    # retry failed subjobs in running job
    def retryFailedJobsInActive(self,dn,jobID):
        returnVal = False
        try:
            _logger.debug("retryFailedJobsInActive %s JobID:%s" % (dn,jobID))
            cUID = self.taskBuffer.cleanUserID(dn)            
            # instantiate ReBroker
            tmpRet = self.taskBuffer.retryJobsInActive(cUID,jobID)
            returnVal = True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("retryFailedJobsInActive: %s %s" % (errType,errValue))
            returnVal = "ERROR: server side crash"
        # return
        return returnVal


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
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobStatus : %s %s" % (type,value))
            ids = []
        _logger.debug("getJobStatus start : %s" % ids)       
        # peek jobs
        ret = self.taskBuffer.peekJobs(ids)
        _logger.debug("getJobStatus end")
        # serialize 
        return pickle.dumps(ret)


    # get assigned cloud for tasks
    def seeCloudTask(self,idsStr):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
        except:
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
        return pickle.dumps(ret)


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
        return pickle.dumps(ret)


    # set task by user
    def setCloudTaskByUser(self,user,tid,cloud,status):
        # run
        ret = self.taskBuffer.setCloudTaskByUser(user,tid,cloud,status)
        return ret


    # add files to memcached
    def addFilesToMemcached(self,site,node,files):
        # add
        ret = self.taskBuffer.addFilesToMemcached(site,node,files)
        # return
        return ret


    # delete files from memcached
    def deleteFilesFromMemcached(self,site,node,files):
        # delete
        ret = self.taskBuffer.deleteFilesFromMemcached(site,node,files)
        # return
        return ret


    # flush memcached
    def flushMemcached(self,site,node):
        # flush
        ret = self.taskBuffer.flushMemcached(site,node)
        # return
        return ret

        
    # check files with memcached
    def checkFilesWithMemcached(self,site,node,files):
        # check
        ret = self.taskBuffer.checkFilesWithMemcached(site,node,files)
        # return
        return ret

    
    # get job statistics
    def getJobStatistics(self,sourcetype=None):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForExtIF(sourcetype)
        # serialize 
        return pickle.dumps(ret)


    # get highest prio jobs
    def getHighestPrioJobStat(self):
        # get job statistics
        ret = self.taskBuffer.getHighestPrioJobStat()
        # serialize 
        return pickle.dumps(ret)


    # get queued analysis jobs at a site
    def getQueuedAnalJobs(self,site,dn):
        # get job statistics
        ret = self.taskBuffer.getQueuedAnalJobs(site,dn)
        # serialize 
        return pickle.dumps(ret)


    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForBamboo()
        # serialize 
        return pickle.dumps(ret)
        

    # get job statistics per site
    def getJobStatisticsPerSite(self,predefined=False,workingGroup='',countryGroup='',jobType=''):
        # get job statistics
        ret = self.taskBuffer.getJobStatistics(True,predefined,workingGroup,countryGroup,jobType)
        # serialize 
        return pickle.dumps(ret)


    # query PandaIDs
    def queryPandaIDs(self,idsStr):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # query PandaIDs 
        ret = self.taskBuffer.queryPandaIDs(ids)
        # serialize 
        return pickle.dumps(ret)


    # get number of analysis jobs per user  
    def getNUserJobs(self,siteName,nJobs):
        # get 
        ret = self.taskBuffer.getNUserJobs(siteName,nJobs)
        # serialize 
        return pickle.dumps(ret)


    # query job info per cloud
    def queryJobInfoPerCloud(self,cloud,schedulerID):
        # query PandaIDs 
        ret = self.taskBuffer.queryJobInfoPerCloud(cloud,schedulerID)
        # serialize 
        return pickle.dumps(ret)

    
    # query PandaIDs at site
    def getPandaIDsSite(self,site,status,limit):
        # query PandaIDs 
        ret = self.taskBuffer.getPandaIDsSite(site,status,limit)
        # serialize 
        return pickle.dumps(ret)


    # get PandaIDs to be updated in prodDB
    def getJobsToBeUpdated(self,limit,lockedby):
        # query PandaIDs 
        ret = self.taskBuffer.getPandaIDsForProdDB(limit,lockedby)
        # serialize 
        return pickle.dumps(ret)


    # update prodDBUpdateTimes
    def updateProdDBUpdateTimes(self,paramsStr):
        # deserialize IDs
        params = WrappedPickle.loads(paramsStr)
        # get jobs
        ret = self.taskBuffer.updateProdDBUpdateTimes(params)
        # serialize 
        return pickle.dumps(True)


    # query last files in datasets
    def queryLastFilesInDataset(self,datasetStr):
        # deserialize names
        datasets = WrappedPickle.loads(datasetStr)
        # get files
        ret = self.taskBuffer.queryLastFilesInDataset(datasets)
        # serialize 
        return pickle.dumps(ret)


    # get input files currently in used for analysis
    def getFilesInUseForAnal(self,outDataset):
        # get files
        ret = self.taskBuffer.getFilesInUseForAnal(outDataset)
        # serialize 
        return pickle.dumps(ret)


    # kill jobs
    def killJobs(self,idsStr,user,host,code,prodManager):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        if not isinstance(ids,types.ListType):
            ids = [ids]
        _logger.debug("killJob : %s %s %s %s" % (user,code,prodManager,ids))
        # kill jobs
        ret = self.taskBuffer.killJobs(ids,user,code,prodManager)
        # logging
        try:
            # make message
            message = '%s - PandaID =' % host
            for id in ids:
                message += ' %s' % id
            # get logger
            _pandaLogger = PandaLogger()            
            _pandaLogger.lock()
            _pandaLogger.setParams({'Type':'killJobs','User':user})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            logger.info(message)
            # release HTTP handler
            _pandaLogger.release()
        except:
            pass
        # serialize 
        return pickle.dumps(ret)


    # reassign jobs
    def reassignJobs(self,idsStr,user,host):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # reassign jobs
        ret = self.taskBuffer.reassignJobs(ids,forkSetupper=True)
        # logging
        try:
            # make message
            message = '%s - PandaID =' % host
            for id in ids:
                message += ' %s' % id
            # get logger
            _pandaLogger = PandaLogger()            
            _pandaLogger.lock()
            _pandaLogger.setParams({'Type':'reassignJobs','User':user})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            logger.info(message)
            # release HTTP handler
            _pandaLogger.release()
        except:
            pass
        # serialize 
        return pickle.dumps(ret)
        

    # resubmit jobs
    def resubmitJobs(self,idsStr):
        # deserialize IDs
        ids = WrappedPickle.loads(idsStr)
        # kill jobs
        ret = self.taskBuffer.resubmitJobs(ids)
        # serialize 
        return pickle.dumps(ret)


    # get list of site spec
    def getSiteSpecs(self,siteType='analysis'):
        # get analysis site list
        specList = {}
        siteMapper = SiteMapper(self.taskBuffer)
        for id,spec in siteMapper.siteSpecList.iteritems():
            if siteType == 'all' or spec.type == siteType:
                # convert to map
                tmpSpec = {}
                for attr in spec._attributes:
                    tmpSpec[attr] = getattr(spec,attr)
                specList[id] = tmpSpec
        # serialize
        return pickle.dumps(specList)


    # get list of cloud spec
    def getCloudSpecs(self):
        # get cloud list
        siteMapper = SiteMapper(self.taskBuffer)
        # serialize
        return pickle.dumps(siteMapper.cloudSpec)


    # get list of cache prefix
    def getCachePrefixes(self):
        # get
        ret = self.taskBuffer.getCachePrefixes()
        # serialize 
        return pickle.dumps(ret)


    # get nPilots
    def getNumPilots(self):
        # get nPilots
        ret = self.taskBuffer.getCurrentSiteData()
        numMap = {}
        for siteID,siteNumMap in ret.iteritems():
            nPilots = 0
            # nPilots = getJob+updateJob
            if siteNumMap.has_key('getJob'):
                nPilots += siteNumMap['getJob']
            if siteNumMap.has_key('updateJob'):
                nPilots += siteNumMap['updateJob']
            # append
            numMap[siteID] = {'nPilots':nPilots}
        # serialize
        return pickle.dumps(numMap)


    # run brokerage
    def runBrokerage(self,sitesStr,cmtConfig,atlasRelease,trustIS=False,processingType=None,
                     dn=None,loggingFlag=False,memorySize=None,workingGroup=None,fqans=[],
                     nJobs=None):
        if not loggingFlag:
            ret = 'NULL'
        else:
            ret = {'site':'NULL','logInfo':[]}
        try:
            # deserialize sites
            sites = WrappedPickle.loads(sitesStr)
            # instantiate siteMapper
            siteMapper = SiteMapper(self.taskBuffer)
            # instantiate job
            job = JobSpec()
            job.AtlasRelease = atlasRelease
            job.cmtConfig    = cmtConfig
            if processingType != None:
                job.processingType = processingType
            if memorySize != None:
                job.minRamCount = memorySize
            if workingGroup != None:
                userDefinedWG = True
                validWorkingGroup = True
                job.workingGroup = workingGroup
            else:
                userDefinedWG = False
                validWorkingGroup = False
            # get parameters related to priority
            withProdRole,workingGroup,priorityOffset,serNum,weight = self.taskBuffer.getPrioParameters([job],dn,fqans,
                                                                                                       userDefinedWG,
                                                                                                       validWorkingGroup)
            # get min priority using nJobs
            try:
                nJobs = long(nJobs)
            except:
                # use 200 as a default # of jobs
                nJobs =200
            minPrio = PrioUtil.calculatePriority(priorityOffset,serNum+nJobs,weight)
            # run brokerage
            _logger.debug("runBrokerage for dn=%s FQAN=%s minPrio=%s" % (dn,str(fqans),minPrio)) 
            brokerage.broker.schedule([job],self.taskBuffer,siteMapper,True,sites,trustIS,dn,
                                      reportLog=loggingFlag,minPriority=minPrio)
            # get computingSite
            if not loggingFlag:
                ret = job.computingSite
            else:
                ret = pickle.dumps({'site':job.computingSite,'logInfo':job.brokerageErrorDiag})
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("runBrokerage : %s %s" % (type,value))
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
        return pickle.dumps(ret)


    # get slimmed file info with PandaIDs
    def getSlimmedFileInfoPandaIDs(self,pandaIDsStr,dn):
        try:
            # deserialize IDs
            pandaIDs = WrappedPickle.loads(pandaIDsStr)
            # truncate
            maxIDs = 5500
            if len(pandaIDs) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                pandaIDs = pandaIDs[:maxIDs]
            # get
            _logger.debug("getSlimmedFileInfoPandaIDs start : %s %s" % (dn,len(pandaIDs)))            
            ret = self.taskBuffer.getSlimmedFileInfoPandaIDs(pandaIDs)
            _logger.debug("getSlimmedFileInfoPandaIDs end")            
        except:
            ret = {}
        # serialize 
        return pickle.dumps(ret)


    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,dn,timeRange):
        # get IDs
        ret = self.taskBuffer.getJobIDsInTimeRange(dn,timeRange)
        # serialize 
        return pickle.dumps(ret)


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,nJobs):
        # get IDs
        ret = self.taskBuffer.getPandIDsWithJobID(dn,jobID,nJobs)
        # serialize 
        return pickle.dumps(ret)


    # check merge job generation status
    def checkMergeGenerationStatus(self,dn,jobID):
        # check
        ret = self.taskBuffer.checkMergeGenerationStatus(dn,jobID)
        # serialize 
        return pickle.dumps(ret)


    # get full job status
    def getFullJobStatus(self,idsStr,dn):
        try:
            # deserialize jobspecs
            ids = WrappedPickle.loads(idsStr)
            # truncate
            maxIDs = 5500
            if len(ids) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getFullJobStatus : %s %s" % (type,value))
            ids = []
        _logger.debug("getFullJobStatus start : %s %s" % (dn,str(ids)))
        # peek jobs
        ret = self.taskBuffer.getFullJobStatus(ids)
        _logger.debug("getFullJobStatus end")
        # serialize 
        return pickle.dumps(ret)


    # add account to siteaccess
    def addSiteAccess(self,siteID,dn):
        # add
        ret = self.taskBuffer.addSiteAccess(siteID,dn)
        # serialize 
        return pickle.dumps(ret)


    # list site access
    def listSiteAccess(self,siteID,dn,longFormat=False):
        # list
        ret = self.taskBuffer.listSiteAccess(siteID,dn,longFormat)
        # serialize 
        return pickle.dumps(ret)


    # update site access
    def updateSiteAccess(self,method,siteid,requesterDN,userName,attrValue):
        # list
        ret = self.taskBuffer.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)
        # serialize 
        return str(ret)


# Singleton
userIF = UserIF()
del UserIF


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmpKey,tmpVal in req.subprocess_env.iteritems():
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
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
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



"""
web service interface

"""

# submit jobs
def submitJobs(req,jobs):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # get FQAN
    fqans = _getFQAN(req)
    # hostname
    host = req.get_remote_host()
    # production Role
    prodRole = _isProdRoleATLAS(req)
    return userIF.submitJobs(jobs,user,host,fqans,prodRole)


# run task assignment
def runTaskAssignment(req,jobs):
    # check security
    if not Protocol.isSecure(req):
        return "False"
    return userIF.runTaskAssignment(jobs)


# get job status
def getJobStatus(req,ids):
    return userIF.getJobStatus(ids)


# get queued analysis jobs at a site
def getQueuedAnalJobs(req,site):
    # check security
    if not Protocol.isSecure(req):
        return "ERROR: SSL is required"
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
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
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    user = _getDN(req)
    # check role
    if not _isProdRoleATLAS(req):
        return "ERROR: production role is required"
    return userIF.setCloudTaskByUser(user,tid,cloud,status)


# add files to memcached
def addFilesToCacheDB(req,site,node,guids='',lfns=''):
    # exec
    return userIF.addFilesToMemcached(site,node,lfns)


# delete files from memcached
def deleteFilesFromCacheDB(req,site,node,guids='',lfns=''):
    # exec
    return userIF.deleteFilesFromMemcached(site,node,lfns)


# flush memcached
def flushCacheDB(req,site,node):
    # exec
    return userIF.flushMemcached(site,node)


# check files with memcached
def checkFilesWithCacheDB(req,site,node,guids='',lfns=''):
    # exec
    return userIF.checkFilesWithMemcached(site,node,lfns)


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
    if not Protocol.isSecure(req):
        return False
    return userIF.updateProdDBUpdateTimes(params)


# get job statistics
def getJobStatistics(req,sourcetype=None):
    return userIF.getJobStatistics(sourcetype)


# get highest prio jobs
def getHighestPrioJobStat(req):
    return userIF.getHighestPrioJobStat()


# get job statistics for Babmoo
def getJobStatisticsForBamboo(req):
    return userIF.getJobStatisticsForBamboo()


# get job statistics per site
def getJobStatisticsPerSite(req,predefined='False',workingGroup='',countryGroup='',jobType=''):
    if predefined=='True':
        predefined=True
    else:
        predefined=False
    return userIF.getJobStatisticsPerSite(predefined,workingGroup,countryGroup,jobType)


# query last files in datasets
def queryLastFilesInDataset(req,datasets):
    return userIF.queryLastFilesInDataset(datasets)


# get input files currently in used for analysis
def getFilesInUseForAnal(req,outDataset):
    return userIF.getFilesInUseForAnal(outDataset)


# kill jobs
def killJobs(req,ids,code=None):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
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
    # hostname
    host = req.get_remote_host()
    return userIF.killJobs(ids,user,host,code,prodManager)


# reassign jobs
def reassignJobs(req,ids):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    return userIF.reassignJobs(ids,user,host)


# resubmit jobs
def resubmitJobs(req,ids):
    # check security
    if not Protocol.isSecure(req):
        return False
    return userIF.resubmitJobs(ids)


# get list of site spec
def getSiteSpecs(req,siteType=None):
    if siteType != None:
        return userIF.getSiteSpecs(siteType)
    else:
        return userIF.getSiteSpecs()

# get list of cloud spec
def getCloudSpecs(req):
    return userIF.getCloudSpecs()

# get list of cache prefix
def getCachePrefixes(req):
    return userIF.getCachePrefixes()

# get client version
def getPandaClientVer(req):
    return userIF.getPandaClientVer()
    
# get nPilots
def getNumPilots(req):
    return userIF.getNumPilots()

# run brokerage
def runBrokerage(req,sites,cmtConfig=None,atlasRelease=None,trustIS=False,processingType=None,
                 loggingFlag=False,memorySize=None,workingGroup=None,nJobs=None):
    if trustIS=='True':
        trustIS = True
    else:
        trustIS = False
    if loggingFlag=='True':
        loggingFlag = True
    else:
        loggingFlag = False
    if memorySize != None:   
        try:
            memorySize = long(memorySize)
        except:
            pass
    dn = _getDN(req)
    fqans = _getFQAN(req)
    return userIF.runBrokerage(sites,cmtConfig,atlasRelease,trustIS,processingType,dn,
                               loggingFlag,memorySize,workingGroup,fqans,nJobs)

# run rebrokerage
def runReBrokerage(req,jobID,libDS='',cloud=None,excludedSite=None,forceOpt=None):
    # check SSL
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    # convert jobID to long
    try:
        jobID = long(jobID)
    except:
        return "ERROR: jobID is not an integer"
    # force option
    if forceOpt == 'True':
        forceOpt = True
    else:
        forceOpt = False
    return userIF.runReBrokerage(dn,jobID,cloud,excludedSite,forceOpt)


# retry failed subjobs in running job
def retryFailedJobsInActive(req,jobID):
    # check SSL
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    # convert jobID to long
    try:
        jobID = long(jobID)
    except:
        return "ERROR: jobID is not an integer"
    return userIF.retryFailedJobsInActive(dn,jobID)


# logger interface
def sendLogInfo(req,msgType,msgList):
    # check SSL
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    return userIF.sendLogInfo(dn,msgType,msgList)


# get serial number for group job
def getSerialNumberForGroupJob(req):
    # check SSL
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "ERROR: SSL connection is required"
    # get DN
    dn = _getDN(req)
    if dn == '':
        return "ERROR: could not get DN"
    return userIF.getSerialNumberForGroupJob(dn)


# register proxy key
def registerProxyKey(req,credname,origin,myproxy):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    # get expiration date
    if not req.subprocess_env.has_key('SSL_CLIENT_V_END'):
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
    except:
        _logger.error("registerProxyKey : failed to convert %s" % \
                      req.subprocess_env['SSL_CLIENT_V_END'])
    # execute
    return userIF.registerProxyKey(params)


# register proxy key
def getProxyKey(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    # execute
    return userIF.getProxyKey(dn)


# get JobIDs in a time range
def getJobIDsInTimeRange(req,timeRange,dn=None):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("getJobIDsInTimeRange %s %s" % (dn,timeRange))
    # execute
    return userIF.getJobIDsInTimeRange(dn,timeRange)


# get PandaIDs for a JobID
def getPandIDsWithJobID(req,jobID,nJobs,dn=None):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("getPandIDsWithJobID %s JobID=%s nJobs=%s" % (dn,jobID,nJobs))
    # execute
    return userIF.getPandIDsWithJobID(dn,jobID,nJobs)


# check merge job generation status
def checkMergeGenerationStatus(req,jobID,dn=None):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("checkMergeGenerationStatus %s JobID=%s" % (dn,jobID))
    # execute
    return userIF.checkMergeGenerationStatus(dn,jobID)


# get slimmed file info with PandaIDs
def getSlimmedFileInfoPandaIDs(req,ids):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    return userIF.getSlimmedFileInfoPandaIDs(ids,dn)


# get full job status
def getFullJobStatus(req,ids):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    return userIF.getFullJobStatus(ids,dn)


# get number of analysis jobs per user  
def getNUserJobs(req,siteName,nJobs=100):
    # check security
    prodManager = False
    if not Protocol.isSecure(req):
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
        return "Failed : VOMS authorization failure"
    # convert nJobs to int
    try:
        nJobs = int(nJobs)
    except:
        nJobs = 100
    # execute
    return userIF.getNUserJobs(siteName,nJobs)


# add account to siteaccess
def addSiteAccess(req,siteID):
    # check security
    if not Protocol.isSecure(req):
        return "False"        
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "False"        
    dn = req.subprocess_env['SSL_CLIENT_S_DN']
    return userIF.addSiteAccess(siteID,dn)


# list site access
def listSiteAccess(req,siteID=None,longFormat=False):
    # check security
    if not Protocol.isSecure(req):
        return "False"
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
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
    if not Protocol.isSecure(req):
        return "non HTTPS"
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "invalid DN"
    # set requester's DN
    requesterDN = req.subprocess_env['SSL_CLIENT_S_DN']
    # update
    return userIF.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)
