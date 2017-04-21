import re
import sys
import json
import types
import shlex
import time
import datetime
import ProcessGroups
import EventServiceUtils
from threading import Lock
from DBProxyPool import DBProxyPool
from brokerage.SiteMapper import SiteMapper
from dataservice.Setupper import Setupper
from dataservice.Closer import Closer
from dataservice.ProcessLimiter import ProcessLimiter

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('TaskBuffer')

class TaskBuffer:
    """
    task queue
    
    """

    # constructor 
    def __init__(self):
        self.proxyPool = None
        self.lock = Lock()
        self.processLimiter = None

    def __repr__(self):
        return "TaskBuffer"

    # initialize
    def init(self,dbname,dbpass,nDBConnection=10,useTimeout=False):
        # lock
        self.lock.acquire()
        # create Proxy Pool
        if self.proxyPool == None:
            self.proxyPool = DBProxyPool(dbname,dbpass,nDBConnection,useTimeout)
        # create process limiter
        if self.processLimiter == None:
            self.processLimiter = ProcessLimiter()
        # release
        self.lock.release()


    # check production role
    def checkProdRole(self,fqans):
        for fqan in fqans:
            # check production role
            match = re.search('/([^/]+)/Role=production',fqan)
            if match != None:
                return True,match.group(1)
        return False,None


    # get priority parameters for user
    def getPrioParameters(self,jobs,user,fqans,userDefinedWG,validWorkingGroup):
        withProdRole   = False
        workingGroup   = None
        priorityOffset = 0
        serNum         = 0
        weight         = None
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # check production role
        withProdRole,workingGroup = self.checkProdRole(fqans)
        if withProdRole and jobs != []:
            # check dataset name
            for tmpFile in jobs[-1].Files:
                if tmpFile.type in ['output','log'] and \
                        not (tmpFile.lfn.startswith('group') or tmpFile.lfn.startswith('panda.um.group')):
                    # reset
                    withProdRole,workingGroup = False,None
                    break
        # reset nJob/weight for HC
        if jobs != []:
            if jobs[0].processingType in ['hammercloud','gangarobot','hammercloud-fax'] \
                   or jobs[0].processingType.startswith('gangarobot-'):
                serNum = 0
                weight = 0.0
            if jobs[0].processingType in ['gangarobot','gangarobot-pft']:
                priorityOffset = 3000
            if jobs[0].processingType in ['hammercloud-fax']:    
                priorityOffset = 1001
        # check quota
        if weight == None:
            weight = proxy.checkQuota(user)
            # get nJob
            if jobs == []:
                serNum = proxy.getNumberJobsUser(user,workingGroup=userDefinedWG)
            elif userDefinedWG and validWorkingGroup:
                # check if group privileged
                isSU =  proxy.isSuperUser(jobs[0].workingGroup)
                if not isSU:
                    serNum = proxy.getNumberJobsUser(user,workingGroup=jobs[0].workingGroup)
                else:
                    # set high prioryty for production role
                    serNum = 0
                    weight = 0.0
                    priorityOffset = 2000
            else:
                serNum = proxy.getNumberJobsUser(user,workingGroup=None)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return withProdRole,workingGroup,priorityOffset,serNum,weight

    
    # store Jobs into DB
    def storeJobs(self,jobs,user,joinThr=False,forkSetupper=False,fqans=[],hostname='',resetLocInSetupper=False,
                  checkSpecialHandling=True,toPending=False,oldPandaIDs=None,relationType=None,  userVO='atlas'):
        try:
            _logger.debug("storeJobs : start for %s nJobs=%s" % (user,len(jobs)))
            # check quota for priority calculation
            weight         = 0.0
            userJobID      = -1
            userJobsetID   = -1
            userStatus     = True
            priorityOffset = 0
            userCountry    = None
            useExpress     = False
            nExpressJobs   = 0
            useDebugMode   = False
            # check ban user except internally generated jobs
            if len(jobs) > 0 and not jobs[0].prodSourceLabel in ProcessGroups.internalSourceLabels:
                # get DB proxy
                proxy = self.proxyPool.getProxy()
                # check user status
                tmpStatus = proxy.checkBanUser(user,jobs[0].prodSourceLabel)
                # release proxy
                self.proxyPool.putProxy(proxy)
                # return if DN is blocked
                if not tmpStatus:
                    _logger.debug("storeJobs : end for %s DN is blocked 1" % user)
                    return []
            # set parameters for user jobs
            if len(jobs) > 0 and (jobs[0].prodSourceLabel in ['user','panda','ptest','rc_test','ssc']) \
                   and (not jobs[0].processingType in ['merge','unmerge']):
                # get DB proxy
                proxy = self.proxyPool.getProxy()
                # get JobID and status
                userJobID,userJobsetID,userStatus = proxy.getUserParameter(user,jobs[0].jobDefinitionID,jobs[0].jobsetID)
                # get site access
                userSiteAccess = proxy.checkSiteAccess(jobs[0].computingSite,user)
                # check quota for express jobs
                if 'express' in jobs[0].specialHandling:
                    expressQuota = proxy.getExpressJobs(user)
                    if expressQuota != None and expressQuota['status'] and expressQuota['quota'] > 0:
                        nExpressJobs = expressQuota['quota']
                        if nExpressJobs > 0:
                            useExpress = True
                # debug mode
                if 'debug' in jobs[0].specialHandling:
                    debugJobList = proxy.getActiveDebugJobs(user)
                    if len(debugJobList) < ProcessGroups.maxDebugJobs:
                        useDebugMode = True
                # release proxy
                self.proxyPool.putProxy(proxy)
                # get site spec
                siteMapper  = SiteMapper(self)
                tmpSiteSpec = siteMapper.getSite(jobs[0].computingSite)
                # check allowed groups
                if userStatus and hasattr(tmpSiteSpec,'allowedgroups') and (not tmpSiteSpec.allowedgroups in ['',None]):
                    # set status to False when allowedgroups is defined
                    userStatus = False
                    # loop over all groups
                    for tmpGroup in tmpSiteSpec.allowedgroups.split(','):
                        if tmpGroup == '':
                            continue
                        # loop over all FQANs
                        for tmpFQAN in fqans:
                            if re.search('^%s' % tmpGroup,tmpFQAN) != None:
                                userStatus = True
                                break
                        # escape
                        if userStatus:
                            break
                # get priority offset
                if hasattr(tmpSiteSpec,'priorityoffset') and (not tmpSiteSpec.priorityoffset in ['',None]):
                    # loop over all groups
                    for tmpGP in tmpSiteSpec.priorityoffset.split(','):
                        if tmpGP == '':
                            continue
                        # get group and offset
                        tmpGroup = tmpGP.split(':')[0]
                        try:
                            tmpOffset = int(tmpGP.split(':')[-1])
                        except:
                            tmpOffset = 0
                        # loop over all FQANs
                        for tmpFQAN in fqans:
                            _logger.debug(tmpFQAN)
                            if re.search('^%s/' % tmpGroup,tmpFQAN) != None or \
                                   re.search('%s$' % tmpGroup,tmpFQAN) != None:
                                # use the largest offset
                                if tmpOffset > priorityOffset:
                                    priorityOffset = tmpOffset
                                break
                # check site access
                if hasattr(tmpSiteSpec,'accesscontrol') and tmpSiteSpec.accesscontrol == 'grouplist':
                    if userSiteAccess == {} or userSiteAccess['status'] != 'approved':
                        # user is not allowed
                        userStatus = False
                # set priority offset
                if userStatus:        
                    if userSiteAccess.has_key('poffset') and userSiteAccess['poffset'] > priorityOffset: 
                        priorityOffset = userSiteAccess['poffset']
                # extract country group
                for tmpFQAN in fqans:
                    match = re.search('^/atlas/([^/]+)/',tmpFQAN)
                    if match != None:
                        tmpCountry = match.group(1)
                        # use country code or usatlas
                        if len(tmpCountry) == 2:
                            userCountry = tmpCountry
                            break
                        # usatlas
                        if tmpCountry in ['usatlas']:
                            userCountry = 'us'
                            break
            # return if DN is blocked
            if not userStatus:
                _logger.debug("storeJobs : end for %s DN is blocked 2" % user)                
                return []
            # extract VO
            for tmpFQAN in fqans:
                match = re.search('^/([^/]+)/',tmpFQAN)
                if match != None:
                    userVO = match.group(1)
                    break
            # get number of jobs currently in PandaDB
            serNum = 0
            userDefinedWG = False
            validWorkingGroup = False
            usingBuild = False
            withProdRole = False
            workingGroup = None
            if len(jobs) > 0 and (jobs[0].prodSourceLabel in ['user','panda']) \
                   and (not jobs[0].processingType in ['merge','unmerge']):
                # extract user's working group from FQANs
                userWorkingGroupList = []
                for tmpFQAN in fqans:
                    match = re.search('/([^/]+)/Role=production',tmpFQAN)
                    if match != None:
                        userWorkingGroupList.append(match.group(1))
                # check workingGroup
                if not jobs[0].workingGroup in ['',None,'NULL']:
                    userDefinedWG = True
                    if userSiteAccess != {}:
                        if userSiteAccess['status'] == 'approved' and jobs[0].workingGroup in userSiteAccess['workingGroups']:
                            # valid workingGroup
                            validWorkingGroup = True
                    # check with FQANs
                    if jobs[0].workingGroup in userWorkingGroupList:
                        validWorkingGroup = True
                # using build for analysis
                if jobs[0].prodSourceLabel == 'panda':
                    usingBuild = True
                # get priority parameters for user
                withProdRole,workingGroup,priorityOffset,serNum,weight = self.getPrioParameters(jobs,user,fqans,userDefinedWG,
                                                                                                validWorkingGroup)
                _logger.debug("storeJobs : {0} workingGroup={1} serNum={2} weight={3} pOffset={4}".format(user,jobs[0].workingGroup,
                                                                                                          serNum,weight,priorityOffset))
            # get DB proxy
            proxy = self.proxyPool.getProxy()
            # get group job serial number
            groupJobSerialNum = 0
            if len(jobs) > 0 and (jobs[0].prodSourceLabel in ['user','panda']) \
                   and (not jobs[0].processingType in ['merge','unmerge']):
                for tmpFile in jobs[-1].Files:
                    if tmpFile.type in ['output','log'] and '$GROUPJOBSN' in tmpFile.lfn:
                        tmpSnRet = proxy.getSerialNumberForGroupJob(user)
                        if tmpSnRet['status']: 
                            groupJobSerialNum = tmpSnRet['sn']
                        break
            # get total number of files
            totalNumFiles = 0
            for job in jobs:
                totalNumFiles += len(job.Files)
            # bulk fetch fileIDs
            fileIDPool = []
            if totalNumFiles > 0:
                fileIDPool = proxy.bulkFetchFileIDsPanda(totalNumFiles)
                fileIDPool.sort()
            # loop over all jobs
            ret =[]
            newJobs=[]
            usePandaDDM = False
            firstLiveLog = True
            nRunJob = 0
            esJobsetMap = {}
            for idxJob,job in enumerate(jobs):
                # set JobID. keep original JobID when retry
                if userJobID != -1 and job.prodSourceLabel in ['user','panda'] \
                        and (job.attemptNr in [0,'0','NULL'] or \
                                 (not job.jobExecutionID in [0,'0','NULL']) or \
                                 job.lockedby == 'jedi') \
                        and (not jobs[0].processingType in ['merge','unmerge']):
                    job.jobDefinitionID = userJobID
                # set jobsetID
                if job.prodSourceLabel in ['user','panda','ptest','rc_test']:
                    job.jobsetID = userJobsetID
                # set specialHandling
                if job.prodSourceLabel in ['user','panda']:
                    if checkSpecialHandling:
                        specialHandling = ''
                        # debug mode
                        if useDebugMode and nRunJob == 0 and job.prodSourceLabel == 'user':
                            specialHandling += 'debug,'
                        # express mode    
                        if useExpress and (nRunJob < nExpressJobs or job.prodSourceLabel == 'panda'):
                            specialHandling += 'express,'
                        # get DDM backend
                        ddmBackEnd = job.getDdmBackEnd()
                        # reset specialHandling
                        specialHandling = specialHandling[:-1]
                        job.specialHandling = specialHandling
                        # set DDM backend
                        if ddmBackEnd != None:
                            job.setDdmBackEnd(ddmBackEnd)
                    if job.prodSourceLabel != 'panda':
                        nRunJob += 1
                # set relocation flag
                if job.computingSite != 'NULL' and job.relocationFlag != 2:
                    job.relocationFlag = 1
                # protection agains empty jobParameters
                if job.jobParameters in ['',None,'NULL']:
                    job.jobParameters = ' '
                # set country group and nJobs (=taskID)
                if job.prodSourceLabel in ['user','panda']:
                    if job.lockedby != 'jedi':
                        job.countryGroup = userCountry
                    # set workingGroup
                    if not validWorkingGroup:
                        if withProdRole:
                            # set country group if submitted with production role
                            job.workingGroup = workingGroup
                        else:
                            if userDefinedWG:
                                # reset invalid working group
                                job.workingGroup = None
                    # set nJobs (=taskID)
                    if usingBuild:
                        tmpNumBuild = 1
                        tmpNunRun = len(jobs) - 1 
                    else:
                        tmpNumBuild = 0
                        tmpNunRun = len(jobs)
                    # encode    
                    job.taskID = tmpNumBuild + (tmpNunRun << 1)
                    # change TRF URL just in case
                    if job.transformation.startswith('http://www.usatlas.bnl.gov/svn/panda/pathena/trf'):
                        job.transformation = re.sub('^http://www.usatlas.bnl.gov/svn/panda/pathena/trf/',
                                                    'http://pandaserver.cern.ch:25080/trf/user/',
                                                    job.transformation)
                # set hostname
                if hostname != '':
                    job.creationHost = hostname
                # extract file info, change specialHandling for event service
                eventServiceInfo,job.specialHandling,esIndex = EventServiceUtils.decodeFileInfo(job.specialHandling)
                origEsJob = False
                if eventServiceInfo != {}:
                    # set jobsetID 
                    if esJobsetMap.has_key(esIndex):
                        job.jobsetID = esJobsetMap[esIndex]
                    else:
                        origEsJob = True
                    # sort files since file order is important for positional event number
                    job.sortFiles()
                if oldPandaIDs != None and len(oldPandaIDs) > idxJob:
                    jobOldPandaIDs = oldPandaIDs[idxJob]
                else:
                    jobOldPandaIDs = None
                # insert job to DB
                if not proxy.insertNewJob(job,user,serNum,weight,priorityOffset,userVO,groupJobSerialNum,
                                          toPending,origEsJob,eventServiceInfo,oldPandaIDs=jobOldPandaIDs,
                                          relationType=relationType,fileIDPool=fileIDPool):
                    # reset if failed
                    job.PandaID = None
                else:
                    # live log
                    if job.prodSourceLabel in ['user','panda']:
                        if ' --liveLog ' in job.jobParameters:
                            # enable liveLog only for the first one
                            if firstLiveLog:
                                # set file name
                                repPatt = ' --liveLog stdout.%s ' % job.PandaID
                            else:
                                # remove the option
                                repPatt = ' '
                            job.jobParameters = re.sub(' --liveLog ',repPatt,job.jobParameters)
                            firstLiveLog = False
                    # append
                    newJobs.append(job)
                    # mapping of jobsetID for event service
                    if origEsJob:
                        esJobsetMap[esIndex] = job.jobsetID
                if job.prodSourceLabel in ['user','panda','ptest','rc_test']:                
                    ret.append((job.PandaID,job.jobDefinitionID,{'jobsetID':job.jobsetID}))
                else:
                    ret.append((job.PandaID,job.jobDefinitionID,job.jobName))                
                serNum += 1
                try:
                    fileIDPool = fileIDPool[len(job.Files):]
                except:
                    fileIDPool = []
            # release DB proxy
            self.proxyPool.putProxy(proxy)
            # set up dataset
            if not toPending:
                if joinThr:
                    thr = Setupper(self,newJobs,pandaDDM=usePandaDDM,forkRun=forkSetupper,resetLocation=resetLocInSetupper)
                    thr.start()
                    thr.join()
                else:
                    # cannot use 'thr =' because it may trigger garbage collector
                    Setupper(self,newJobs,pandaDDM=usePandaDDM,forkRun=forkSetupper,resetLocation=resetLocInSetupper).start()
            # return jobIDs
            _logger.debug("storeJobs : end for %s succeeded" % user)            
            return ret
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("storeJobs : %s %s" % (errType,errValue))
            return "ERROR: ServerError with storeJobs"
           

    # lock jobs for reassign
    def lockJobsForReassign(self,tableName,timeLimit,statList,labels,processTypes,sites,clouds,
                            useJEDI=False,onlyReassignable=False,useStateChangeTime=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.lockJobsForReassign(tableName,timeLimit,statList,labels,processTypes,sites,clouds,
                                        useJEDI,onlyReassignable,useStateChangeTime)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # get a DB configuration value
    def getConfigValue(self, component, key, app='pandaserver', vo=None):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getConfigValue(component, key, app, vo)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # lock jobs for finisher
    def lockJobsForFinisher(self,timeNow,rownum,highPrio):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.lockJobsForFinisher(timeNow,rownum,highPrio)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # lock jobs for activator
    def lockJobsForActivator(self,timeLimit,rownum,prio):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.lockJobsForActivator(timeLimit,rownum,prio)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # get number of activated/defined jobs with output datasets
    def getNumWaitingJobsWithOutDS(self,outputDSs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getNumWaitingJobsWithOutDS(outputDSs)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # resubmit jobs
    def resubmitJobs(self,jobIDs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        jobs=[]
        # get jobs
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,True,False,False,False)
            if res:
                jobs.append(res)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # set up dataset
        if len(jobs) > 0:
            Setupper(self,jobs).start()
        # return jobIDs
        return True
    

    # update overall job information
    def updateJobs(self,jobs,inJobsDefined,oldJobStatusList=None,extraInfo=None):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # loop over all jobs
        returns    = []
        ddmIDs     = []
        ddmAttempt = 0
        newMover   = None
        for idxJob,job in enumerate(jobs):
            # update DB
            tmpddmIDs = []
            if oldJobStatusList != None and idxJob < len(oldJobStatusList):
                oldJobStatus = oldJobStatusList[idxJob]
            else:
                oldJobStatus = None
            # check for co-jumbo or jumbo jobs
            if EventServiceUtils.isCoJumboJob(job):
                # check if all events assiciated to the co-jumbo are done
                allDone = proxy.checkAllEventsDone(job,None,True)
                # keep until all events are done
                if not allDone:
                    job.jobStatus = 'holding'
            elif EventServiceUtils.isJumboJob(job):
                # check if there are done events
                hasDone = proxy.hasDoneEvents(job.jediTaskID,job.PandaID)
                if hasDone:
                    job.jobStatus = 'finished'
                else:
                    job.jobStatus = 'failed'
            if job.jobStatus == 'failed' and job.prodSourceLabel == 'user' and not inJobsDefined:
                # keep failed analy jobs in Active4
                ret = proxy.updateJob(job,inJobsDefined,oldJobStatus=oldJobStatus)
            elif job.jobStatus in ['finished','failed','cancelled']:
                ret,tmpddmIDs,ddmAttempt,newMover = proxy.archiveJob(job,inJobsDefined,extraInfo=extraInfo)
            else:
                ret = proxy.updateJob(job,inJobsDefined,oldJobStatus=oldJobStatus,extraInfo=extraInfo)
            returns.append(ret)
            # collect IDs for reassign
            if ret:
                ddmIDs += tmpddmIDs
        # release proxy
        self.proxyPool.putProxy(proxy)
        # retry mover
        if newMover != None:
            self.storeJobs([newMover],None,joinThr=True)
        # reassign jobs when ddm failed
        if ddmIDs != []:
            self.reassignJobs(ddmIDs,ddmAttempt,joinThr=True)
        # return
        return returns


    # update job jobStatus only
    def updateJobStatus(self,jobID,jobStatus,param,updateStateChange=False,attemptNr=None):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # update DB and buffer
        ret = proxy.updateJobStatus(jobID,jobStatus,param,updateStateChange,attemptNr)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret


    # finalize pending analysis jobs
    def finalizePendingJobs(self,prodUserName,jobDefinitionID,waitLock=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB
        ret = proxy.finalizePendingJobs(prodUserName,jobDefinitionID,waitLock)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret


    # retry job
    def retryJob(self,jobID,param,failedInActive=False,changeJobInMem=False,inMemJob=None,
                 getNewPandaID=False,attemptNr=None,recoverableEsMerge=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB
        ret = proxy.retryJob(jobID,param,failedInActive,changeJobInMem,inMemJob,
                             getNewPandaID,attemptNr,recoverableEsMerge)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret
    

    # retry failed analysis jobs in Active4
    def retryJobsInActive(self,prodUserName,jobDefinitionID,isJEDI=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB
        ret = proxy.retryJobsInActive(prodUserName,jobDefinitionID,isJEDI)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret

    
    # activate jobs
    def activateJobs(self,jobs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # loop over all jobs
        returns = []
        for job in jobs:
            # update DB
            ret = proxy.activateJob(job)
            returns.append(ret) 
        # release proxy
        self.proxyPool.putProxy(proxy)
        return returns


    # send jobs to jobsWaiting
    def keepJobs(self,jobs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # loop over all jobs
        returns = []
        for job in jobs:
            # update DB
            ret = proxy.keepJob(job)
            returns.append(ret) 
        # release proxy
        self.proxyPool.putProxy(proxy)
        return returns


    # archive jobs
    def archiveJobs(self,jobs,inJobsDefined):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # loop over all jobs
        returns = []
        for job in jobs:
            # update DB
            ret = proxy.archiveJob(job,inJobsDefined)
            returns.append(ret[0]) 
        # release proxy
        self.proxyPool.putProxy(proxy)
        return returns


    # delete stalled jobs
    def deleteStalledJobs(self,libFileName):
        # get DB proxy
        proxy = self.proxyPool.getProxy()        
        # execute
        ret = proxy.deleteStalledJobs(libFileName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret


    # set debug mode
    def setDebugMode(self,dn,pandaID,prodManager,modeOn,workingGroup):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # check the number of debug jobs
        hitLimit = False
        if modeOn == True:
            limitNum = None
            if prodManager:
                jobList = proxy.getActiveDebugJobs(prodRole=True)
                limitNum = ProcessGroups.maxDebugProdJobs
            elif workingGroup != None:
                jobList = proxy.getActiveDebugJobs(workingGroup=workingGroup)
                limitNum = ProcessGroups.maxDebugWgJobs
            else:
                jobList = proxy.getActiveDebugJobs(dn=dn)
                limitNum = ProcessGroups.maxDebugJobs
            if len(jobList) >= limitNum:
                # exceeded
                retStr  = 'You already hit the limit on the maximum number of debug subjobs '
                retStr += '(%s jobs). ' % limitNum
                retStr += 'Please set the debug mode off for one of the following PandaIDs : '
                for tmpID in jobList:
                    retStr += '%s,' % tmpID
                retStr = retStr[:-1]
                hitLimit = True
        if not hitLimit:
            # execute
            retStr = proxy.setDebugMode(dn,pandaID,prodManager,modeOn,workingGroup)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return retStr


    # get jobs
    def getJobs(self,nJobs,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                atlasRelease,prodUserID,getProxyKey,countryGroup,workingGroup,allowOtherCountry,
                taskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get waiting jobs
        t_before = time.time()
        jobs,nSent = proxy.getJobs(nJobs,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                                   atlasRelease,prodUserID,countryGroup,workingGroup,allowOtherCountry,
                                   taskID)
        t_after = time.time()
        t_total = t_after - t_before
        _logger.debug("getJobs : took {0}s for {1} nJobs={2} prodSourceLabel={3}"
                               .format(t_total, siteName, nJobs, prodSourceLabel))
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get Proxy Key
        proxyKey = {}
        if getProxyKey and len(jobs) > 0:
            # get MetaDB proxy
            proxy = self.proxyPool.getProxy()
            # get Proxy Key
            proxyKey = proxy.getProxyKey(jobs[0].prodUserID)
            # release proxy
            self.proxyPool.putProxy(proxy)
        # return
        return jobs+[nSent,proxyKey]


    # run task assignment
    def runTaskAssignment(self,jobs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # loop over all jobs
        retList =[]
        newJobs =[]
        for job in jobs:
            ret = None
            if not job.taskID in ['NULL',0,'']:
                # get cloud
                cloudTask = proxy.getCloudTask(job.taskID)
                if cloudTask != None and cloudTask.status == 'assigned':
                    ret = cloudTask.cloud
            if ret == None:
                # append for TA
                newJobs.append(job)
            retList.append(ret)    
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # run setupper
        if newJobs != []:
            pass
        # return clouds
        return retList


    # reset modification time of a task to shorten retry interval
    def resetTmodCloudTask(self,tid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # run
        res = proxy.resetTmodCloudTask(tid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # get assigning task
    def getAssigningTask(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # run
        res = proxy.getAssigningTask()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # get fareshare policy
    def getFaresharePolicy(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # run
        res = proxy.getFaresharePolicy(True)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res
        

    # check merge job generation status
    def checkMergeGenerationStatus(self,dn,jobID):
        # return for NA
        retNA = {'status':'NA','mergeIDs':[]}
        try:
            # get at most 2 PandaIDs
            idStatus = self.getPandIDsWithJobID(dn,jobID,2)
            if idStatus == {}:
                return retNA
            # use larger PandaID which corresponds to runXYZ
            tmpKeys = idStatus.keys()
            tmpKeys.sort()
            pandaID = tmpKeys[-1]
            # get job
            tmpJobs = self.getFullJobStatus([pandaID])
            if tmpJobs == [] or tmpJobs[0] == None:
                return retNA
            pandaJob = tmpJobs[0]
            # non-merge job
            if not '--mergeOutput' in pandaJob.jobParameters:
                return retNA
            # loop over all sub datasets
            subDsList = []
            mergeStatus = None
            mergeIDs    = []
            for tmpFile in pandaJob.Files:
                if tmpFile.type in ['output','log']:
                    if not tmpFile.destinationDBlock in subDsList:
                        subDsList.append(tmpFile.destinationDBlock)
                        # get dataset
                        tmpDsSpec = self.queryDatasetWithMap({'name':tmpFile.destinationDBlock})
                        if tmpDsSpec != None:
                            if tmpDsSpec.status in ['tobemerged']:
                                # going to be merged
                                mergeStatus = 'generating'
                                mergeIDs = []
                            elif tmpDsSpec.status in ['tobeclosed','closed','completed']:
                                # another dataset from --individualOutDS is waiting for Merger
                                if mergeStatus == 'generating':
                                    continue
                                # set status
                                mergeStatus = 'generated'
                                # collect JobIDs of merge jobs
                                tmpMergeID = tmpDsSpec.MoverID
                                if not tmpMergeID in [0,None,'NULL']+mergeIDs:
                                    mergeIDs.append(tmpMergeID)
            # no merger most likely because jobs were killed
            if mergeStatus == 'generated' and mergeIDs == []:
                mergeStatus = 'aborted'
            # jobs are still runnign
            if mergeStatus == None:
                mergeStatus = 'standby'
            # return
            return {'status':mergeStatus,'mergeIDs':mergeIDs}
        except:
            return retNA

    
    # get job status
    def getJobStatus(self,jobIDs,fromDefined=True,fromActive=True,fromArchived=True,fromWaiting=True):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retStatus = []
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,fromDefined,fromActive,fromArchived,fromWaiting)
            if res:
                retStatus.append(res.jobStatus)
            else:
                retStatus.append(None)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retStatus


    # peek at jobs
    def peekJobs(self,jobIDs,fromDefined=True,fromActive=True,fromArchived=True,fromWaiting=True,forAnal=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retJobs = []
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,fromDefined,fromActive,fromArchived,fromWaiting,forAnal)
            if res:
                retJobs.append(res)
            else:
                retJobs.append(None)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retJobs


    # get PandaID with jobexeID
    def getPandaIDwithJobExeID(self,jobexeIDs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retJobs = []
        # peek at job
        for jobexeID in jobexeIDs:
            res = proxy.getPandaIDwithJobExeID(jobexeID)
            retJobs.append(res)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retJobs


    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self,jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retJobs = proxy.getPandaIDsWithTaskID(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retJobs


    # get slimmed file info with PandaIDs
    def getSlimmedFileInfoPandaIDs(self,pandaIDs):
        iPandaID = 0
        nPandaID = 100
        retInfo = {}
        while iPandaID < len(pandaIDs):
            # get DBproxy
            proxy = self.proxyPool.getProxy()
            # get
            tmpRetInfo = proxy.getSlimmedFileInfoPandaIDs(pandaIDs[iPandaID:iPandaID+nPandaID])
            # release proxy
            self.proxyPool.putProxy(proxy)
            iPandaID += nPandaID 
            if retInfo == {}:
                retInfo = tmpRetInfo
            else:
                for outKey in tmpRetInfo.keys():
                    if not retInfo.has_key(outKey):
                        retInfo[outKey] = []
                    # append
                    for tmpItemRetInfo in tmpRetInfo[outKey]:
                        if not tmpItemRetInfo in retInfo[outKey]:
                            retInfo[outKey].append(tmpItemRetInfo)
        # return
        return retInfo


    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,dn,timeRangeStr):
        # check DN
        if dn in ['NULL','','None',None]:
            return []
        # check timeRange
        match = re.match('^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$',timeRangeStr)
        if match == None:
            return []
        timeRange = datetime.datetime(year   = int(match.group(1)),
                                      month  = int(match.group(2)),
                                      day    = int(match.group(3)),
                                      hour   = int(match.group(4)),
                                      minute = int(match.group(5)),
                                      second = int(match.group(6)))
        # max range is 3 months
        maxRange = datetime.datetime.utcnow() - datetime.timedelta(days=30)
        if timeRange < maxRange:
            timeRange = maxRange
        retJobIDs = []
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get JobIDs
        retJobIDs = proxy.getJobIDsInTimeRange(dn,timeRange,retJobIDs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # read ARCH when time window is more than 3days (- 3 hours as a margin)
        if timeRange < datetime.datetime.utcnow() - datetime.timedelta(days=2,hours=21) :
            # get ArchiveDBproxy
            proxy = self.proxyPool.getProxy()
            # get JobIDs
            retJobIDs = proxy.getJobIDsInTimeRangeLog(dn,timeRange,retJobIDs)
            # release proxy
            self.proxyPool.putProxy(proxy)
        # return
        return retJobIDs


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,nJobs):
        idStatus = {}
        # check DN
        if dn in ['NULL','','None',None]:
            return idStatus
        # check JobID
        try:
            jobID = long(jobID)
            nJobs = long(nJobs)
        except:
            return idStatus
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        idStatus,buildJobID = proxy.getPandIDsWithJobID(dn,jobID,idStatus,nJobs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get ArchiveDBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        idStatus = proxy.getPandIDsWithJobIDLog(dn,jobID,idStatus,nJobs,buildJobID)        
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return idStatus


    # get PandaIDs for a JobsetID or JobdefID in jobsArchived
    def getPandIDsWithIdInArch(self,prodUserName,id,isJobset):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getPandIDsWithIdInArch(prodUserName,id,isJobset)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get beyond pledge resource ratio
    # ! this method is not thread-safe
    def getPledgeResourceRatio(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getPledgeResourceRatio()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return proxy.beyondPledgeRatio

        
    # get the number of waiting jobs with a dataset
    def getNumWaitingJobsForPD2P(self,datasetName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        nJobs = proxy.getNumWaitingJobsForPD2P(datasetName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return nJobs


    # get the number of waiting jobsets with a dataset
    def getNumWaitingJobsetsForPD2P(self,datasetName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        nJobs = proxy.getNumWaitingJobsetsForPD2P(datasetName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return nJobs


    # lock job for re-brokerage
    def lockJobForReBrokerage(self,dn,jobID,simulation,forceOpt,forFailed=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.lockJobForReBrokerage(dn,jobID,simulation,forceOpt,forFailed)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # reset buildJob for re-brokerage
    def resetBuildJobForReBrokerage(self,pandaID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.resetBuildJobForReBrokerage(pandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get PandaIDs using libDS for re-brokerage
    def getPandaIDsForReBrokerage(self,userName,jobID,fromActive,forFailed=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.getPandaIDsForReBrokerage(userName,jobID,fromActive,forFailed)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get input datasets for rebroerage
    def getInDatasetsForReBrokerage(self,jobID,userName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.getInDatasetsForReBrokerage(jobID,userName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret
    

    # get outDSs with userName/jobID
    def getOutDSsForReBrokerage(self,userName,jobID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get IDs
        ret = proxy.getOutDSsForReBrokerage(userName,jobID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get full job status
    def getFullJobStatus(self,jobIDs,fromDefined=True,fromActive=True,fromArchived=True,fromWaiting=True,forAnal=True,
                         days=30):
        retJobMap = {}
        # peek at job
        for jobID in jobIDs:
            # get DBproxy for each job to avoid occupying connection for long time
            proxy = self.proxyPool.getProxy()
            # peek job
            res = proxy.peekJob(jobID,fromDefined,fromActive,fromArchived,fromWaiting,forAnal)
            retJobMap[jobID] = res
            # release proxy
            self.proxyPool.putProxy(proxy)
        # get IDs
        for jobID in jobIDs:
            if retJobMap[jobID] == None:
                # get ArchiveDBproxy
                proxy = self.proxyPool.getProxy()
                # peek job
                res = proxy.peekJobLog(jobID,days)
                retJobMap[jobID] = res
                # release proxy
                self.proxyPool.putProxy(proxy)
        # sort
        retJobs = []
        for jobID in jobIDs:
            retJobs.append(retJobMap[jobID])
        # return
        return retJobs


    # get script for offline running 
    def getScriptOfflineRunning(self,pandaID,days=None):
        try:
            # get job
            tmpJobs = self.getFullJobStatus([pandaID],days=days)
            if tmpJobs == [] or tmpJobs[0] == None:
                errStr = "ERROR: Cannot get PandaID=%s in DB " % pandaID
                if days == None:
                    errStr += "for the last 30 days. You may add &days=N to the URL"
                else:
                    errStr += "for the last {0} days. You may change &days=N in the URL".format(days)
                return errStr
            tmpJob = tmpJobs[0]
            # check prodSourceLabel
            if not tmpJob.prodSourceLabel in ['managed','test']:
                return "ERROR: Non production job : prodSourceLabel=%s. This method is only for production jobs" % tmpJob.prodSourceLabel
            # release and trf
            tmpRels = tmpJob.homepackage.split("\n")
            tmpPars = tmpJob.jobParameters.split("\n")
            tmpTrfs = tmpJob.transformation.split("\n")
            if not (len(tmpRels) == len(tmpPars) == len(tmpTrfs)):
                return "ERROR: The number of releases or parameters or trfs is inconsitent with others"
            # construct script
            scrStr = "#retrieve inputs\n\n"
            # collect inputs
            dsFileMap = {}
            for tmpFile in tmpJob.Files:
                if tmpFile.type=='input':
                    if not dsFileMap.has_key(tmpFile.dataset):
                        dsFileMap[tmpFile.dataset] = []
                    if not tmpFile.lfn in dsFileMap[tmpFile.dataset]:    
                        dsFileMap[tmpFile.dataset].append(tmpFile.scope+':'+tmpFile.lfn)
            # get
            for tmpDS,tmpFileList in dsFileMap.iteritems():
                for tmpLFN in tmpFileList:
                    scrStr += "rucio download "
                    scrStr += "%s\n" % tmpLFN
                    # ln
                    tmpScope,tmpBareLFN = tmpLFN.split(':')
                    scrStr += "ln -fs %s/%s ./%s\n" % (tmpScope,tmpBareLFN,tmpBareLFN)
            scrStr += "\n#transform commands\n\n"
            for tmpIdx,tmpRel in enumerate(tmpRels):
                # asetup
                scrStr += "asetup --cmtconfig=%s %s,%s\n" % tuple([tmpJob.cmtConfig]+tmpRel.split("/"))
                # athenaMP
                if not tmpJob.coreCount in ['NULL',None] and tmpJob.coreCount > 1:
                    scrStr += "export ATHENA_PROC_NUMBER=%s\n" % tmpJob.coreCount
                # add double quotes for zsh
                tmpParamStr = tmpPars[tmpIdx]
                tmpSplitter = shlex.shlex(tmpParamStr, posix=True)
                tmpSplitter.whitespace = ' '
                tmpSplitter.whitespace_split = True
                # loop for params
                for tmpItem in tmpSplitter:
                    tmpMatch = re.search('^(-[^=]+=)(.+)$',tmpItem)
                    if tmpMatch != None:
                        tmpArgName = tmpMatch.group(1)
                        tmpArgVal  = tmpMatch.group(2)
                        tmpArgIdx = tmpParamStr.find(tmpArgName) + len(tmpArgName)
                        # add " 
                        if tmpParamStr[tmpArgIdx] != '"':
                            tmpParamStr = tmpParamStr.replace(tmpMatch.group(0),
                                                              tmpArgName+'"'+tmpArgVal+'"')
                # run trf
                scrStr += "%s %s\n\n" % (tmpTrfs[tmpIdx],tmpParamStr)
            return scrStr
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getScriptOfflineRunning : %s %s" % (errType,errValue))
            return "ERROR: ServerError in getScriptOfflineRunning with %s %s" % (errType,errValue)
                                
            
    # kill jobs
    def killJobs(self,ids,user,code,prodManager,wgProdRole=[],killOptions=[]):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        rets = []
        # kill jobs
        pandaIDforCloserMap = {}
        for id in ids:
            ret,userInfo = proxy.killJob(id,user,code,prodManager,True,wgProdRole,killOptions)
            rets.append(ret)
            if ret and userInfo['prodSourceLabel'] in ['user','managed','test']:
                jobIDKey = (userInfo['prodUserID'],userInfo['jobDefinitionID'],userInfo['jobsetID'])
                if not pandaIDforCloserMap.has_key(jobIDKey):
                    pandaIDforCloserMap[jobIDKey] = id
        # release proxy
        self.proxyPool.putProxy(proxy)
        # run Closer
        try:
            if pandaIDforCloserMap != {}:
                for pandaIDforCloser in pandaIDforCloserMap.values():
                    tmpJobs = self.peekJobs([pandaIDforCloser])
                    tmpJob = tmpJobs[0]
                    if tmpJob != None:
                        tmpDestDBlocks = []
                        # get destDBlock
                        for tmpFile in tmpJob.Files:
                            if tmpFile.type in ['output','log']:
                                if not tmpFile.destinationDBlock in tmpDestDBlocks:
                                    tmpDestDBlocks.append(tmpFile.destinationDBlock)
                        # run            
                        cThr = Closer(self,tmpDestDBlocks,tmpJob)
                        cThr.start()
                        cThr.join()
        except:
            pass
        # return
        return rets


    # reassign jobs
    def reassignJobs(self,ids,attempt=0,joinThr=False,forkSetupper=False,forPending=False,
                     firstSubmission=True):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        jobs = []
        oldSubMap = {}
        # keep old assignment
        keepSiteFlag = False
        if (attempt % 2) != 0:
            keepSiteFlag = True
        # reset jobs
        for id in ids:
            try:
                # try to reset active job
                if not forPending:
                    tmpRet = proxy.resetJob(id,keepSite=keepSiteFlag,getOldSubs=True)
                    if isinstance(tmpRet,types.TupleType):
                        ret,tmpOldSubList = tmpRet
                    else:
                        ret,tmpOldSubList = tmpRet,[]
                    if ret != None:
                        jobs.append(ret)
                        for tmpOldSub in tmpOldSubList:
                            if not oldSubMap.has_key(tmpOldSub):
                                oldSubMap[tmpOldSub] = ret
                        continue
                # try to reset waiting job
                tmpRet = proxy.resetJob(id,False,keepSite=keepSiteFlag,getOldSubs=False,forPending=forPending)
                if isinstance(tmpRet,types.TupleType):
                    ret,tmpOldSubList = tmpRet
                else:
                    ret,tmpOldSubList = tmpRet,[]
                if ret != None:
                    jobs.append(ret)
                    # waiting jobs don't create sub or dis
                    continue
                # try to reset defined job
                if not forPending:
                    tmpRet = proxy.resetDefinedJob(id,keepSite=keepSiteFlag,getOldSubs=True)
                    if isinstance(tmpRet,types.TupleType):
                        ret,tmpOldSubList = tmpRet
                    else:
                        ret,tmpOldSubList = tmpRet,[]
                    if ret != None:
                        jobs.append(ret)
                        for tmpOldSub in tmpOldSubList:
                            if not oldSubMap.has_key(tmpOldSub):
                                oldSubMap[tmpOldSub] = ret
                        continue
            except:
                pass
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # run Closer for old sub datasets
        if not forPending:
            for tmpOldSub,tmpJob in oldSubMap.iteritems():
                cThr = Closer(self,[tmpOldSub],tmpJob)
                cThr.start()
                cThr.join()
        # setup dataset
        if jobs != []:
            if joinThr:
                thr = Setupper(self,jobs,resubmit=True,ddmAttempt=attempt,forkRun=forkSetupper,
                               firstSubmission=firstSubmission)
                thr.start()
                thr.join()
            else:
                # cannot use 'thr =' because it may trigger garbage collector
                Setupper(self,jobs,resubmit=True,ddmAttempt=attempt,forkRun=forkSetupper,
                         firstSubmission=firstSubmission).start()
        # return
        return True


    # awake jobs in jobsWaiting
    def awakeJobs(self,ids):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        jobs = []
        # reset jobs
        for id in ids:
            # try to reset waiting job
            ret = proxy.resetJob(id,False)
            if ret != None:
                jobs.append(ret)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # setup dataset
        Setupper(self,jobs).start()
        # return
        return True


    # query PandaIDs
    def queryPandaIDs(self,jobDefIDs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        pandaIDs = []
        # query PandaID
        for jobDefID in jobDefIDs:
            id = proxy.queryPandaID(jobDefID)
            pandaIDs.append(id)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return pandaIDs


    # query job info per cloud
    def queryJobInfoPerCloud(self,cloud,schedulerID=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query job info
        ret = proxy.queryJobInfoPerCloud(cloud,schedulerID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get PandaIDs to be updated in prodDB
    def getPandaIDsForProdDB(self,limit,lockedby):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query PandaID
        ret = proxy.getPandaIDsForProdDB(limit,lockedby)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update prodDBUpdateTime 
    def updateProdDBUpdateTimes(self,paramList):
        retList = []
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update
        for param in paramList:
            ret = proxy.updateProdDBUpdateTime(param)
            retList.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # get PandaIDs at Site
    def getPandaIDsSite(self,site,status,limit):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query PandaID
        ids = proxy.getPandaIDsSite(site,status,limit)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ids


    # get input files currently in used for analysis
    def getFilesInUseForAnal(self,outDataset):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query LFNs
        retList = proxy.getFilesInUseForAnal(outDataset)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # get list of dis dataset to get input files in shadow
    def getDisInUseForAnal(self,outDataset):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query dis
        retList = proxy.getDisInUseForAnal(outDataset)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # get input LFNs currently in use for analysis with shadow dis
    def getLFNsInUseForAnal(self,inputDisList):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query dis
        retList = proxy.getLFNsInUseForAnal(inputDisList)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self,dataset,status,fileLFN=''):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query PandaID
        retList = proxy.updateInFilesReturnPandaIDs(dataset,status,fileLFN)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # update file status in dispatch dataset
    def updateFileStatusInDisp(self,dataset,fileStatusMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query PandaID
        retVal = proxy.updateFileStatusInDisp(dataset,fileStatusMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retVal


    # update output files and return corresponding PandaIDs
    def updateOutFilesReturnPandaIDs(self,dataset,fileLFN=''):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query PandaID
        retList = proxy.updateOutFilesReturnPandaIDs(dataset,fileLFN)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # get datasets associated with file
    def getDatasetWithFile(self,lfn,jobPrioity=0):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query PandaID
        retList = proxy.getDatasetWithFile(lfn,jobPrioity)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # get _dis datasets associated to _sub
    def getAssociatedDisDatasets(self,subDsName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query
        retList = proxy.getAssociatedDisDatasets(subDsName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # insert sandbox file info
    def insertSandboxFileInfo(self,userName,hostName,fileName,fileSize,checkSum):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret= proxy.insertSandboxFileInfo(userName,hostName,fileName,fileSize,checkSum)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check duplicated sandbox file
    def checkSandboxFile(self,userName,fileSize,checkSum):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret= proxy.checkSandboxFile(userName,fileSize,checkSum)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # insert datasets
    def insertDatasets(self,datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # insert
        for dataset in datasets:
            ret= proxy.insertDataset(dataset)
            retList.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # query Dataset
    def queryDatasetWithMap(self,map):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.queryDatasetWithMap(map)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # query last files in a dataset
    def queryLastFilesInDataset(self,datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query files
        ret = proxy.queryLastFilesInDataset(datasets)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # set GUIDs
    def setGUIDs(self,files):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # set GUIDs
        ret = proxy.setGUIDs(files)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # query PandaID with dataset
    def queryPandaIDwithDataset(self,datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.queryPandaIDwithDataset(datasets)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # query PandaID with filenames
    def queryPandaIDwithLFN(self,lfns):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.queryPandaIDwithLFN(lfns)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update dataset
    def updateDatasets(self,datasets,withLock=False,withCriteria="",criteriaMap={}):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update Dataset
        retList = proxy.updateDataset(datasets,withLock,withCriteria,criteriaMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList
    

    # delete dataset
    def deleteDatasets(self,datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query Dataset
        for dataset in datasets:
            ret = proxy.deleteDataset(dataset)
            retList.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # query files with map
    def queryFilesWithMap(self,map):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query files
        ret = proxy.queryFilesWithMap(map)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # count the number of files with map
    def countFilesWithMap(self,map):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query files
        ret = proxy.countFilesWithMap(map)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # count the number of pending files
    def countPendingFiles(self,pandaID,forInput=True):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count files
        ret = proxy.countPendingFiles(pandaID,forInput)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get serial number for dataset
    def getSerialNumber(self,datasetname,definedFreshFlag=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getSerialNumber(datasetname,definedFreshFlag)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get serial number for group job
    def getSerialNumberForGroupJob(self,name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getSerialNumberForGroupJob(name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # add metadata
    def addMetadata(self,ids,metadataList,newStatusList):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # add metadata
        index = 0
        retList = []
        for id in ids:
            ret = proxy.addMetadata(id,metadataList[index],newStatusList[index])
            retList.append(ret)
            index += 1
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # add stdout
    def addStdOut(self,id,stdout):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # add 
        ret = proxy.addStdOut(id,stdout)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # extract name from DN
    def cleanUserID(self,id):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get 
        ret = proxy.cleanUserID(id)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # extract scope from dataset name
    def extractScope(self,name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get 
        ret = proxy.extractScope(name)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # change job priorities
    def changeJobPriorities(self,newPrioMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get 
        ret = proxy.changeJobPriorities(newPrioMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get destinationDBlockToken for a dataset
    def getDestTokens(self,dsname):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get token
        ret = proxy.getDestTokens(dsname)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get destinationSE for a dataset
    def getDestSE(self,dsname,fromArch=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get token
        ret = proxy.getDestSE(dsname,fromArch)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get job statistics
    def getJobStatistics(self,archived=False,predefined=False,workingGroup='',countryGroup='',jobType='',forAnal=None,minPriority=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatistics(archived,predefined,workingGroup,countryGroup,jobType,forAnal,minPriority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get job statistics with label
    def getJobStatisticsWithLabel(self,siteStr=''):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatisticsWithLabel(siteStr)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get job statistics for brokerage
    def getJobStatisticsBrokerage(self,minPrio=None,maxPrio=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get stat
        ret = proxy.getJobStatisticsBrokerage(minPrio,maxPrio)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # convert
        conRet = ProcessGroups.countJobsPerGroup(ret)
        # return
        return conRet


    # get job statistics for analysis brokerage
    def getJobStatisticsAnalBrokerage(self,minPriority=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get stat
        ret = proxy.getJobStatisticsAnalBrokerage(minPriority=minPriority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # convert
        conRet = ProcessGroups.countJobsPerGroupForAnal(ret)
        # return
        return conRet


    # get the number of waiting jobs per site and user
    def getJobStatisticsPerUserSite(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get stat
        ret = proxy.getJobStatisticsPerUserSite()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get highest prio jobs
    def getHighestPrioJobStat(self,perPG=False,useMorePG=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get stat
        if not perPG:
            ret = proxy.getHighestPrioJobStat()
        else:
            ret = proxy.getHighestPrioJobStatPerPG(useMorePG)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get queued analysis jobs at a site
    def getQueuedAnalJobs(self,site,dn):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get stat
        ret = proxy.getQueuedAnalJobs(site,dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get job statistics for ExtIF
    def getJobStatisticsForExtIF(self,sourcetype=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatisticsForExtIF(sourcetype)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self,useMorePG=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatisticsPerProcessingType(useMorePG)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get number of analysis jobs per user
    def getNUserJobs(self,siteName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get number of analysis jobs per user
        tmpRet = proxy.getNUserJobs(siteName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # get log proxy
        proxy = self.proxyPool.getProxy()
        # get Proxy Key
        ret = {}
        for userID,nJobs in tmpRet.iteritems():
            proxyKey = proxy.getProxyKey(userID)
            if proxyKey != {}:
                # add nJobs
                proxyKey['nJobs'] = nJobs
                # append
                ret[userID] = proxyKey
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get number of activated analysis jobs
    def getNAnalysisJobs(self,nProcesses):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.getNAnalysisJobs(nProcesses)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update transfer status for a dataset
    def updateTransferStatus(self,datasetname,bitMap):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update
        ret = proxy.updateTransferStatus(datasetname,bitMap)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # get CloudTask
    def getCloudTask(self,tid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.getCloudTask(tid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # set cloud to CloudTask
    def setCloudTask(self,cloudTask):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.setCloudTask(cloudTask)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # see CloudTask
    def seeCloudTask(self,tid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.seeCloudTask(tid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # set cloud to CloudTask by user
    def setCloudTaskByUser(self,user,tid,cloud,status,forceUpdate=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # count
        ret = proxy.setCloudTaskByUser(user,tid,cloud,status,forceUpdate)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update site data
    def updateSiteData(self,hostID,pilotRequests):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.updateSiteData(hostID,pilotRequests)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get current site data
    def getCurrentSiteData(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getCurrentSiteData()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # insert nRunning in site data
    def insertnRunningInSiteData(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.insertnRunningInSiteData()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get nRunning in site data
    def getnRunningInSiteData(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getnRunningInSiteData()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get site list
    def getSiteList(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get site info
        ret = proxy.getSiteList()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get site info
    def getSiteInfo(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get site info
        ret = proxy.getSiteInfo()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get cloud list
    def getCloudList(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get cloud list
        ret = proxy.getCloudList()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check sites with release/cache
    def checkSitesWithRelease(self,sites,releases=None,caches=None,cmtConfig=None,onlyCmtConfig=False,
                              cmtConfigPattern=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # check
        ret = proxy.checkSitesWithRelease(sites,releases,caches,cmtConfig,onlyCmtConfig,cmtConfigPattern)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get sites with release/cache in cloud 
    def getSitesWithReleaseInCloud(self,cloud,releases=None,caches=None,validation=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # check
        ret = proxy.getSitesWithReleaseInCloud(cloud,releases,caches,validation)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get list of cache prefix
    def getCachePrefixes(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # check
        ret = proxy.getCachePrefixes()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get list of cmtConfig
    def getCmtConfigList(self,relaseVer):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # check
        ret = proxy.getCmtConfigList(relaseVer)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get pilot owners
    def getPilotOwners(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get pilot owners
        ret = proxy.getPilotOwners()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get allowed nodes
    def getAllowedNodes(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getAllowedNodes()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get special dipatcher parameters
    def getSpecialDispatchParams(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getSpecialDispatchParams()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get email address
    def getEmailAddr(self,name,withDN=False,withUpTime=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get 
        ret = proxy.getEmailAddr(name,withDN,withUpTime)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # set email address for a user
    def setEmailAddr(self,userName,emailAddr):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # set 
        ret = proxy.setEmailAddr(userName,emailAddr)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get client version
    def getPandaClientVer(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get 
        ret = proxy.getPandaClientVer()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # register proxy key
    def registerProxyKey(self,params):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # register proxy key
        ret = proxy.registerProxyKey(params)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # register proxy key
    def registerProxyKey(self,params):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # register proxy key
        ret = proxy.registerProxyKey(params)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get proxy key
    def getProxyKey(self,dn):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get proxy key
        ret = proxy.getProxyKey(dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # add account to siteaccess  
    def addSiteAccess(self,siteID,dn):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # add account to siteaccess
        ret = proxy.addSiteAccess(siteID,dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # list site access
    def listSiteAccess(self,siteid,dn,longFormat=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # list site access
        ret = proxy.listSiteAccess(siteid,dn,longFormat)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update site access
    def updateSiteAccess(self,method,siteid,requesterDN,userName,attrValue):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update site access
        ret = proxy.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    
    # generate pilot token
    def genPilotToken(self,schedulerhost,scheduleruser,schedulerid):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.genPilotToken(schedulerhost,scheduleruser,schedulerid)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get list of scheduler users
    def getListSchedUsers(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getListSchedUsers()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret
    

    # query an SQL return Status  
    def querySQLS(self,sql,varMap,arraySize=1000):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.querySQLS(sql,varMap,arraySize)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check quota
    def checkQuota(self,dn):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.checkQuota(dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get JobID for user
    def getJobIdUser(self,dn):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getJobIdUser(dn)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get user subscriptions
    def getUserSubscriptions(self,datasetName,timeRange):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getUserSubscriptions(datasetName,timeRange)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get the number of user subscriptions
    def getNumUserSubscriptions(self):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getNumUserSubscriptions()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # add user subscriptions
    def addUserSubscription(self,datasetName,dq2IDs):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.addUserSubscription(datasetName,dq2IDs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # increment counter for subscription
    def incrementUsedCounterSubscription(self,datasetName):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.incrementUsedCounterSubscription(datasetName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get active datasets
    def getActiveDatasets(self,computingSite,prodSourceLabel):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getActiveDatasets(computingSite,prodSourceLabel)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check status of all sub datasets to trigger Notifier
    def checkDatasetStatusForNotifier(self,jobsetID,jobDefinitionID,prodUserName):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.checkDatasetStatusForNotifier(jobsetID,jobDefinitionID,prodUserName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get MoU share for T2 PD2P
    def getMouShareForT2PD2P(self):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getMouShareForT2PD2P()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # insert TaskParams
    def insertTaskParamsPanda(self,taskParams,user,prodRole,fqans=[],parent_tid=None,properErrorCode=False,
                              allowActiveTask=False):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # check user status
        tmpStatus = proxy.checkBanUser(user,None,True)
        if tmpStatus == True:
            # exec
            ret = proxy.insertTaskParamsPanda(taskParams,user,prodRole,fqans,parent_tid,properErrorCode,
                                              allowActiveTask)
        elif tmpStatus == 1:
            ret = False,"Failed to update DN in PandaDB"
        elif tmpStatus == 2:
            ret = False,"Failed to insert user info to PandaDB"
        else:
            ret = False,"The following DN is banned: DN={0}".format(user)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # send command to task
    def sendCommandTaskPanda(self,jediTaskID,dn,prodRole,comStr,comComment=None,useCommit=True,properErrorCode=False,
                             comQualifier=None):
        # query an SQL return Status  
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.sendCommandTaskPanda(jediTaskID,dn,prodRole,comStr,comComment,useCommit,
                                         properErrorCode,comQualifier)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # update unmerged datasets to trigger merging
    def updateUnmergedDatasets(self,job,finalStatusDS,updateCompleted=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateUnmergedDatasets(job,finalStatusDS,updateCompleted)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get active JediTasks in a time range
    def getJediTasksInTimeRange(self,dn,timeRangeStr):
        # check DN
        if dn in ['NULL','','None',None]:
            return {}
        # check timeRange
        match = re.match('^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$',timeRangeStr)
        if match == None:
            return {}
        timeRange = datetime.datetime(year   = int(match.group(1)),
                                      month  = int(match.group(2)),
                                      day    = int(match.group(3)),
                                      hour   = int(match.group(4)),
                                      minute = int(match.group(5)),
                                      second = int(match.group(6)))
        # max range is 3 months
        maxRange = datetime.datetime.utcnow() - datetime.timedelta(days=30)
        if timeRange < maxRange:
            timeRange = maxRange
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJediTasksInTimeRange(dn,timeRange)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get details of JediTask
    def getJediTaskDetails(self,jediTaskID,fullFlag,withTaskInfo):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJediTaskDetails(jediTaskID,fullFlag,withTaskInfo)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get a list of even ranges for a PandaID
    def getEventRanges(self,pandaID,jobsetID,jediTaskID,nRanges,acceptJson):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getEventRanges(pandaID,jobsetID,jediTaskID,nRanges,acceptJson)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # update an even range
    def updateEventRange(self,eventRangeID,eventStatus,cpuCore,cpuConsumptionTime,objstoreID=None):
        eventDict = {}
        eventDict['eventRangeID'] = eventRangeID
        eventDict['eventStatus'] = eventStatus
        eventDict['cpuCore'] = cpuCore
        eventDict['cpuConsumptionTime'] = cpuConsumptionTime
        eventDict['objstoreID'] = objstoreID
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateEventRanges([eventDict])
        # release proxy
        self.proxyPool.putProxy(proxy)
        # extract return
        try:
            retVal = ret[0][0]
        except:
            retVal = False
        # return
        return retVal,json.dumps(ret[1])



    # update even ranges
    def updateEventRanges(self,eventRanges,version=0):
        # decode json
        try:
            eventRanges = json.loads(eventRanges)
        except:
            return json.dumps("ERROR : failed to convert eventRanges with json")
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateEventRanges(eventRanges,version)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        if version != 0:
            return ret
        return json.dumps(ret[0]),json.dumps(ret[1])



    # get retry history
    def getRetryHistoryJEDI(self,jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getRetryHistoryJEDI(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # change task priority
    def changeTaskPriorityPanda(self,jediTaskID,newPriority):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.changeTaskPriorityPanda(jediTaskID,newPriority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get WAN data flow matrix
    def getWanDataFlowMaxtrix(self):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getWanDataFlowMaxtrix()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # throttle job
    def throttleJob(self,pandaID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.throttleJob(pandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # throttle user jobs
    def throttleUserJobs(self,prodUserName):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.throttleUserJobs(prodUserName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # unthrottle job
    def unThrottleJob(self,pandaID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.unThrottleJob(pandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # unthrottle user jobs
    def unThrottleUserJobs(self,prodUserName):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.unThrottleUserJobs(prodUserName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get throttled users
    def getThrottledUsers(self):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getThrottledUsers()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get the list of jobdefIDs for failed jobs in a task
    def getJobdefIDsForFailedJob(self,jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJobdefIDsForFailedJob(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # change task attribute
    def changeTaskAttributePanda(self,jediTaskID,attrName,attrValue):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.changeTaskAttributePanda(jediTaskID,attrName,attrValue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # change split rule for task
    def changeTaskSplitRulePanda(self,jediTaskID,attrName,attrValue):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.changeTaskSplitRulePanda(jediTaskID,attrName,attrValue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self,jediTaskID,increasedNr):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseAttemptNrPanda(jediTaskID,increasedNr)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get jediTaskID from taskName
    def getTaskIDwithTaskNameJEDI(self,userName,taskName):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getTaskIDwithTaskNameJEDI(userName,taskName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # update error dialog for a jediTaskID
    def updateTaskErrorDialogJEDI(self,jediTaskID,msg):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateTaskErrorDialogJEDI(jediTaskID,msg)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # update modificationtime for a jediTaskID to trigger subsequent process
    def updateTaskModTimeJEDI(self,jediTaskID,newStatus=None):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateTaskModTimeJEDI(jediTaskID,newStatus)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # check input file status 
    def checkInputFileStatusInJEDI(self,jobSpec):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.checkInputFileStatusInJEDI(jobSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # increase memory limit
    def increaseRamLimitJEDI(self,jediTaskID,jobRamCount):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseRamLimitJEDI(jediTaskID,jobRamCount)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # increase memory limit
    def increaseRamLimitJobJEDI(self, job, jobRamCount, jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseRamLimitJobJEDI(job, jobRamCount, jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # reset files in JEDI
    def resetFileStatusInJEDI(self,dn,prodManager,datasetName,lostFiles,lostInputDatasets):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.resetFileStatusInJEDI(dn,prodManager,datasetName,lostFiles,lostInputDatasets)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get input datasets for output dataset
    def getInputDatasetsForOutputDatasetJEDI(self,datasetName):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getInputDatasetsForOutputDatasetJEDI(datasetName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # copy file record
    def copyFileRecord(self,newLFN,fileSpec,updateOrig):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.copyFileRecord(newLFN,fileSpec,updateOrig)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # retry module: get the defined rules
    def getRetrialRules(self):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getRetrialRules()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # retry module action: set max number of retries
    def setMaxAttempt(self, jobID, jediTaskID, files, attemptNr):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.setMaxAttempt(jobID, jediTaskID, files, attemptNr)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # retry module action: increase CPU Time
    def increaseCpuTimeTask(self, jobID, taskID, siteid, files, active):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseCpuTimeTask(jobID, taskID, siteid, files, active)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # retry module action: recalculate the Task Parameters
    def requestTaskParameterRecalculation(self, taskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.requestTaskParameterRecalculation(taskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # throttle jobs for resource shares
    def throttleJobsForResourceShare(self,site):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.throttleJobsForResourceShare(site)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # activate jobs for resource shares
    def activateJobsForResourceShare(self,site,nJobsPerQueue):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.activateJobsForResourceShare(site,nJobsPerQueue)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # add associate sub datasets for single consumer job
    def getDestDBlocksWithSingleConsumer(self,jediTaskID,PandaID,ngDatasets):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getDestDBlocksWithSingleConsumer(jediTaskID,PandaID,ngDatasets)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # check validity of merge job
    def isValidMergeJob(self,pandaID,jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.isValidMergeJob(pandaID,jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # Configurator: insert network matrix data
    def insertNetworkMatrixData(self, data):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.insertNetworkMatrixData(data)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # Configurator: delete old network matrix data
    def deleteOldNetworkData(self):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.deleteOldNetworkData()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get dispatch datasets per user
    def getDispatchDatasetsPerUser(self,vo,prodSourceLabel,onlyActive,withSize):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getDispatchDatasetsPerUser(vo,prodSourceLabel,onlyActive,withSize)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get task parameters
    def getTaskPramsPanda(self,jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getTaskPramsPanda(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get task attributes
    def getTaskAttributesPanda(self,jediTaskID,attrs):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getTaskAttributesPanda(jediTaskID,attrs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # check for cloned jobs
    def checkClonedJob(self,jobSpec):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.checkClonedJob(jobSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get co-jumbo jobs to be finished
    def getCoJumboJobsToBeFinished(self,timeLimit,minPriority):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getCoJumboJobsToBeFinished(timeLimit,minPriority)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # check if task is applicable for jumbo jobs
    def isApplicableTaskForJumbo(self,jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.isApplicableTaskForJumbo(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # cleanup jumbo jobs
    def cleanupJumboJobs(self,jediTaskID=None):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.cleanupJumboJobs(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # convert ObjID to endpoint
    def convertObjIDtoEndPoint(self,srcFileName,ObjID):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.convertObjIDtoEndPoint(srcFileName,ObjID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res



    # get OS IDs
    def getObjIDs(self,jediTaskID,pandaID):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getObjIDs(jediTaskID,pandaID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res



    # get task status
    def getTaskStatus(self,jediTaskID):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getTaskStatus(jediTaskID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res



    # reactivate task
    def reactivateTask(self,jediTaskID):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reactivateTask(jediTaskID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # get event statistics
    def getEventStat(self, jediTaskID, PandaID):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getEventStat(jediTaskID, PandaID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # get the HS06 distribution for global shares
    def get_hs_distribution(self):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.get_hs_distribution()
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # reassign share
    def reassignShare(self, jedi_task_ids, share_dest):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reassignShare(jedi_task_ids, share_dest)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # list tasks in share
    def listTasksInShare(self, gshare, status):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.listTasksInShare(gshare, status)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    def is_valid_share(self, share_name):
        """
        Checks whether the share is a valid leave share
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.is_valid_share(share_name)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    def get_share_for_task(self, task):
        """
        Return the share based on a task specification
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.get_share_for_task(task)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    def get_share_for_job(self, job):
        """
        Return the share based on a task specification
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.get_share_for_job(job)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    def getTaskParamsMap(self,jediTaskID):
        """
        Return the taskParamsMap
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getTaskPramsPanda(jediTaskID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    def getCommands(self, harvester_id, n_commands):
        """
        Get n commands for a particular harvester instance
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getCommands(harvester_id, n_commands)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    def ackCommands(self, command_ids):
        """
        Acknowledge a list of command IDs
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.ackCommands(command_ids)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # update workers
    def updateWorkers(self, harvesterID, data):
        """
        Update workers
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.updateWorkers(harvesterID, data)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # heartbeat for harvester
    def harvesterIsAlive(self, user, host, harvesterID, data):
        """
        update harvester instance information
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.harvesterIsAlive(user,host,harvesterID,data)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    def storePilotLog(self, panda_id, pilot_log):
        """
        Store the pilot log in the pandalog table
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.storePilotLog(panda_id, pilot_log)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # read the resource types from the DB
    def load_resource_types(self, task_spec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.load_resource_types(task_spec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret_val


    # get the resource of a task
    def get_resource_type_task(self, task_spec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.get_resource_type_task(task_spec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret_val


    def reset_resource_type_task(self, jedi_task_id):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.reset_resource_type_task(jedi_task_id)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret_val


    # get the resource of a task
    def get_resource_type_job(self, job_spec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.get_resource_type_job(job_spec)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret_val


# Singleton
taskBuffer = TaskBuffer()

