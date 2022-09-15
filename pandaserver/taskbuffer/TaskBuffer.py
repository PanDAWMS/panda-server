import re
import sys
import json
import shlex
import time
import datetime
import traceback
from pandaserver.taskbuffer import ProcessGroups
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.taskbuffer import ErrorCode
from pandaserver.taskbuffer import JobUtils
from threading import Lock
from pandaserver.taskbuffer.DBProxyPool import DBProxyPool
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.dataservice.Setupper import Setupper
from pandaserver.dataservice.Closer import Closer
from pandaserver.dataservice.ProcessLimiter import ProcessLimiter
from pandaserver.srvcore import CoreUtils
from pandaserver.config import panda_config

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
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
        self.nDBConection = None

    def __repr__(self):
        return "TaskBuffer"

    # initialize
    def init(self,dbname,dbpass,nDBConnection=10,useTimeout=False):
        # lock
        self.lock.acquire()
        self.nDBConection = nDBConnection
        # create Proxy Pool
        if self.proxyPool is None:
            self.proxyPool = DBProxyPool(dbname,dbpass,nDBConnection,useTimeout)
        # create process limiter
        if self.processLimiter is None:
            self.processLimiter = ProcessLimiter()
        # release
        self.lock.release()

    # cleanup
    def cleanup(self):
        if self.proxyPool:
            self.proxyPool.cleanup()

    # get number of database connections
    def get_num_connections(self):
        return self.nDBConection

    # check production role
    def checkProdRole(self,fqans):
        for fqan in fqans:
            # check production role
            match = re.search('/([^/]+)/Role=production',fqan)
            if match is not None:
                return True,match.group(1)
        return False,None


    # get priority parameters for user
    def getPrioParameters(self, jobs, user, fqans, userDefinedWG, validWorkingGroup):
        priorityOffset = 0
        serNum         = 0
        weight         = None
        prio_reduction = True
        # get boosted users and groups
        boost_dict = {}
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
            elif jobs[0].processingType in ['gangarobot','gangarobot-pft']:
                priorityOffset = 3000
            elif jobs[0].processingType in ['hammercloud-fax']:
                priorityOffset = 1001
            else:
                # get users and groups to boost job priorities
                boost_dict = proxy.get_dict_to_boost_job_prio(jobs[-1].VO)
                if boost_dict:
                    prodUserName = proxy.cleanUserID(user)
                    # check boost list
                    if userDefinedWG and validWorkingGroup:
                        if 'group' in boost_dict and jobs[-1].workingGroup in boost_dict['group']:
                            priorityOffset = boost_dict['group'][jobs[-1].workingGroup]
                            weight = 0.0
                            prio_reduction = False
                    else:
                        if 'user' in boost_dict and prodUserName in boost_dict['user']:
                            priorityOffset = boost_dict['user'][prodUserName]
                            weight = 0.0
                            prio_reduction = False
        # check quota
        if weight is None:
            weight = proxy.checkQuota(user)
            # get nJob
            if jobs == []:
                serNum = proxy.getNumberJobsUser(user,workingGroup=userDefinedWG)
            elif userDefinedWG and validWorkingGroup:
                # check if group privileged
                isSU, isGU =  proxy.isSuperUser(jobs[0].workingGroup)
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
        return withProdRole, workingGroup, priorityOffset, serNum, weight, prio_reduction


    # store Jobs into DB
    def storeJobs(self, jobs, user, joinThr=False, forkSetupper=False, fqans=[], hostname='', resetLocInSetupper=False,
                  checkSpecialHandling=True, toPending=False, oldPandaIDs=None, relationType=None, userVO='atlas',
                  esJobsetMap=None, getEsJobsetMap=False, unprocessedMap=None):
        try:
            tmpLog = LogWrapper(_logger, 'storeJobs <{}>'.format(CoreUtils.clean_user_id(user)))
            tmpLog.debug("start nJobs={}".format(len(jobs)))
            # check quota for priority calculation
            weight = 0.0
            userJobID = -1
            userJobsetID = -1
            userStatus = True
            priorityOffset = 0
            userCountry = None
            useExpress = False
            nExpressJobs = 0
            useDebugMode = False
            siteMapper = SiteMapper(self)

            # check ban user except internally generated jobs
            if len(jobs) > 0 and not jobs[0].prodSourceLabel in ProcessGroups.internalSourceLabels:
                # get DB proxy
                proxy = self.proxyPool.getProxy()
                # check user status
                tmpStatus = proxy.checkBanUser(user, jobs[0].prodSourceLabel)
                # release proxy
                self.proxyPool.putProxy(proxy)
                # return if DN is blocked
                if not tmpStatus:
                    tmpLog.debug("end 1 since DN %s is blocked" % user)
                    if getEsJobsetMap:
                        return ([], None, unprocessedMap)
                    return []

            # set parameters for user jobs
            if len(jobs) > 0 and (jobs[0].prodSourceLabel in JobUtils.analy_sources + JobUtils.list_ptest_prod_sources) \
                   and (not jobs[0].processingType in ['merge', 'unmerge']):
                # get DB proxy
                proxy = self.proxyPool.getProxy()
                # get JobID and status
                userJobID, userJobsetID, userStatus = proxy.getUserParameter(user,jobs[0].jobDefinitionID,jobs[0].jobsetID)
                # get site access
                userSiteAccess = proxy.checkSiteAccess(jobs[0].computingSite,user)
                # check quota for express jobs
                if 'express' in jobs[0].specialHandling:
                    expressQuota = proxy.getExpressJobs(user)
                    if expressQuota is not None and expressQuota['status'] and expressQuota['quota'] > 0:
                        nExpressJobs = expressQuota['quota']
                        if nExpressJobs > 0:
                            useExpress = True
                # debug mode
                if jobs[0].is_debug_mode() or jobs[-1].is_debug_mode():
                    useDebugMode = True
                # release proxy
                self.proxyPool.putProxy(proxy)

                # get site spec
                tmpSiteSpec = siteMapper.getSite(jobs[0].computingSite)

                # get priority offset
                if hasattr(tmpSiteSpec,'priorityoffset') and (tmpSiteSpec.priorityoffset not in ['',None]):
                    # loop over all groups
                    for tmpGP in tmpSiteSpec.priorityoffset.split(','):
                        if tmpGP == '':
                            continue
                        # get group and offset
                        tmpGroup = tmpGP.split(':')[0]
                        try:
                            tmpOffset = int(tmpGP.split(':')[-1])
                        except Exception:
                            tmpOffset = 0
                        # loop over all FQANs
                        for tmpFQAN in fqans:
                            tmpLog.debug(tmpFQAN)
                            if re.search('^%s/' % tmpGroup,tmpFQAN) is not None or \
                                   re.search('%s$' % tmpGroup,tmpFQAN) is not None:
                                # use the largest offset
                                if tmpOffset > priorityOffset:
                                    priorityOffset = tmpOffset
                                break

                # set priority offset
                if userStatus:
                    if 'poffset' in userSiteAccess and userSiteAccess['poffset'] > priorityOffset:
                        priorityOffset = userSiteAccess['poffset']

                # extract country group
                for tmpFQAN in fqans:
                    match = re.search('^/atlas/([^/]+)/',tmpFQAN)
                    if match is not None:
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
                tmpLog.debug("end 2 since %s DN is blocked" % user)
                if getEsJobsetMap:
                    return ([], None, unprocessedMap)
                return []
            # extract VO
            for tmpFQAN in fqans:
                match = re.search('^/([^/]+)/',tmpFQAN)
                if match is not None:
                    userVO = match.group(1)
                    break

            # get number of jobs currently in PandaDB
            serNum = 0
            userDefinedWG = False
            validWorkingGroup = False
            usingBuild = False
            withProdRole = False
            workingGroup = None
            prio_reduction = True
            if len(jobs) > 0 and (jobs[0].prodSourceLabel in JobUtils.analy_sources) \
                   and (not jobs[0].processingType in ['merge','unmerge']):
                # extract user's working group from FQANs
                userWorkingGroupList = []
                for tmpFQAN in fqans:
                    match = re.search('/([^/]+)/Role=production',tmpFQAN)
                    if match is not None:
                        userWorkingGroupList.append(match.group(1))
                # check workingGroup
                if jobs[0].workingGroup not in ['',None,'NULL']:
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
                withProdRole, workingGroup, priorityOffset, serNum, weight, prio_reduction = \
                    self.getPrioParameters(jobs, user, fqans, userDefinedWG, validWorkingGroup)
                tmpLog.debug("workingGroup={} serNum={} weight={} pOffset={} reduction={}".format(
                    jobs[0].workingGroup, serNum, weight, priorityOffset, prio_reduction))
            # get DB proxy
            proxy = self.proxyPool.getProxy()
            # get group job serial number
            groupJobSerialNum = 0
            if len(jobs) > 0 and (jobs[0].prodSourceLabel in JobUtils.analy_sources) \
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
            if esJobsetMap is None:
                esJobsetMap = {}
            try:
                tmpLog.debug("jediTaskID={} len(esJobsetMap)={} nJobs={}".format(jobs[0].jediTaskID,
                                                                                 len(esJobsetMap), len(jobs)))
            except Exception:
                pass
            for idxJob, job in enumerate(jobs):
                # set JobID. keep original JobID when retry
                if userJobID != -1 and job.prodSourceLabel in JobUtils.analy_sources \
                        and (job.attemptNr in [0,'0','NULL'] or \
                                 (job.jobExecutionID not in [0,'0','NULL']) or \
                                 job.lockedby == 'jedi') \
                        and (not jobs[0].processingType in ['merge','unmerge']):
                    job.jobDefinitionID = userJobID
                # set jobsetID
                if job.prodSourceLabel in JobUtils.analy_sources + JobUtils.list_ptest_prod_sources:
                    job.jobsetID = userJobsetID
                # set specialHandling
                if job.prodSourceLabel in JobUtils.analy_sources:
                    if checkSpecialHandling:
                        specialHandling = ''
                        # debug mode
                        if useDebugMode and job.prodSourceLabel == 'user':
                            specialHandling += 'debug,'
                        # express mode
                        if useExpress and (nRunJob < nExpressJobs or job.prodSourceLabel == 'panda'):
                            specialHandling += 'express,'
                        # keep original attributes
                        ddmBackEnd = job.getDdmBackEnd()
                        isHPO = job.is_hpo_workflow()
                        isScout = job.isScoutJob()
                        no_looping_check = job.is_no_looping_check()
                        use_secrets = job.use_secrets()
                        push_changes = job.push_status_changes()
                        is_push_job = job.is_push_job()
                        # reset specialHandling
                        specialHandling = specialHandling[:-1]
                        job.specialHandling = specialHandling
                        if isScout:
                            job.setScoutJobFlag()
                        if isHPO:
                            job.set_hpo_workflow()
                        if no_looping_check:
                            job.disable_looping_check()
                        if use_secrets:
                            job.set_use_secrets()
                        if push_changes:
                            job.set_push_status_changes()
                        if is_push_job:
                            job.set_push_job()
                        # set DDM backend
                        if ddmBackEnd is not None:
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
                if job.prodSourceLabel in JobUtils.analy_sources:
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
                # set hostname
                if hostname != '':
                    job.creationHost = hostname

                # process and set the job_label
                if not job.job_label or job.job_label not in (JobUtils.PROD_PS, JobUtils.ANALY_PS):
                    tmpSiteSpec = siteMapper.getSite(job.computingSite)
                    queue_type = tmpSiteSpec.type
                    if queue_type == 'analysis':
                        job.job_label = JobUtils.ANALY_PS
                    elif queue_type == 'production':
                        job.job_label = JobUtils.PROD_PS
                    elif queue_type == 'unified':
                        if job.prodSourceLabel in JobUtils.analy_sources:
                            job.job_label = JobUtils.ANALY_PS
                        else:
                            # set production as default if not specified and neutral prodsourcelabel
                            job.job_label = JobUtils.PROD_PS
                    else:  # e.g. type = special
                        job.job_label = JobUtils.PROD_PS

                # extract file info, change specialHandling for event service
                origSH = job.specialHandling
                eventServiceInfo, job.specialHandling, esIndex = EventServiceUtils.decodeFileInfo(job.specialHandling)
                origEsJob = False
                if eventServiceInfo != {}:
                    # set jobsetID
                    if esIndex in esJobsetMap:
                        job.jobsetID = esJobsetMap[esIndex]
                    else:
                        origEsJob = True
                    # sort files since file order is important for positional event number
                    job.sortFiles()
                if oldPandaIDs is not None and len(oldPandaIDs) > idxJob:
                    jobOldPandaIDs = oldPandaIDs[idxJob]
                else:
                    jobOldPandaIDs = None
                # check events for jumbo jobs
                isOK = True
                if EventServiceUtils.isJumboJob(job):
                    hasReadyEvents = proxy.hasReadyEvents(job.jediTaskID)
                    if hasReadyEvents is False:
                        isOK = False
                # insert job to DB
                if not isOK:
                    # skip since there is no ready event
                    job.PandaID = None
                tmpRetI = proxy.insertNewJob(job, user, serNum, weight, priorityOffset, userVO, groupJobSerialNum,
                                             toPending, origEsJob, eventServiceInfo, oldPandaIDs=jobOldPandaIDs,
                                             relationType=relationType, fileIDPool=fileIDPool,
                                             origSpecialHandling=origSH, unprocessedMap=unprocessedMap,
                                             prio_reduction=prio_reduction)
                if unprocessedMap is not None:
                    tmpRetI, unprocessedMap = tmpRetI
                if not tmpRetI:
                    # reset if failed
                    job.PandaID = None
                else:
                    # live log
                    if job.prodSourceLabel in JobUtils.analy_sources:
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
                if job.prodSourceLabel in JobUtils.analy_sources + JobUtils.list_ptest_prod_sources:
                    ret.append((job.PandaID,job.jobDefinitionID,{'jobsetID':job.jobsetID}))
                else:
                    ret.append((job.PandaID,job.jobDefinitionID,job.jobName))
                serNum += 1
                try:
                    fileIDPool = fileIDPool[len(job.Files):]
                except Exception:
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
            tmpLog.debug("end successfully")
            if getEsJobsetMap:
                return (ret, esJobsetMap, unprocessedMap)
            return ret
        except Exception as e:
            tmpLog.error("{} {}".format(str(e), traceback.format_exc()))
            errStr = "ERROR: ServerError with storeJobs"
            if getEsJobsetMap:
                return (errStr, None, unprocessedMap)
            return errStr


    # lock jobs for reassign
    def lockJobsForReassign(self,tableName,timeLimit,statList,labels,processTypes,sites,clouds,
                            useJEDI=False,onlyReassignable=False,useStateChangeTime=False,
                            getEventService=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.lockJobsForReassign(tableName,timeLimit,statList,labels,processTypes,sites,clouds,
                                        useJEDI,onlyReassignable,useStateChangeTime,getEventService)
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
            if oldJobStatusList is not None and idxJob < len(oldJobStatusList):
                oldJobStatus = oldJobStatusList[idxJob]
            else:
                oldJobStatus = None
            # check for jumbo jobs
            if EventServiceUtils.isJumboJob(job):
                if job.jobStatus in ['defined','assigned','activated']:
                    pass
                else:
                    # check if there are done events
                    hasDone = proxy.hasDoneEvents(job.jediTaskID, job.PandaID, job)
                    if hasDone:
                        job.jobStatus = 'finished'
                    else:
                        if job.pilotErrorCode in [1144, '1144']:
                            job.jobStatus = 'cancelled'
                        else:
                            job.jobStatus = 'failed'
                        if job.taskBufferErrorDiag in ['', 'NULL', None]:
                            job.taskBufferErrorDiag = 'set {0} since no successful events'.format(job.jobStatus)
                            job.taskBufferErrorCode = ErrorCode.EC_EventServiceNoEvent
            if job.jobStatus in ['finished','failed','cancelled']:
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
        if newMover is not None:
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
        # get secrets for debug mode
        if isinstance(ret, str) and 'debug' in ret:
            tmpS, secrets = proxy.get_user_secrets(panda_config.pilot_secrets)
            if tmpS and secrets:
                ret = {'command': ret, 'secrets': secrets}
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret


    # update worker status by the pilot
    def updateWorkerPilotStatus(self, workerID, harvesterID, status):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB and buffer
        ret = proxy.updateWorkerPilotStatus(workerID, harvesterID, status)
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
    def archiveJobs(self,jobs,inJobsDefined,fromJobsWaiting=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # loop over all jobs
        returns = []
        for job in jobs:
            # update DB
            ret = proxy.archiveJob(job,inJobsDefined,fromJobsWaiting=fromJobsWaiting)
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
        if modeOn is True:
            if prodManager:
                limitNum = None
            elif workingGroup is not None:
                jobList = proxy.getActiveDebugJobs(workingGroup=workingGroup)
                limitNum = ProcessGroups.maxDebugWgJobs
            else:
                jobList = proxy.getActiveDebugJobs(dn=dn)
                limitNum = ProcessGroups.maxDebugJobs
            if limitNum and len(jobList) >= limitNum:
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
    def getJobs(self, nJobs, siteName, prodSourceLabel, cpu, mem, diskSpace, node, timeout, computingElement,
                atlasRelease, prodUserID, getProxyKey, countryGroup, workingGroup, allowOtherCountry,
                taskID, background, resourceType, harvester_id, worker_id, schedulerID, jobType, is_gu,
                via_topic):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get waiting jobs
        t_before = time.time()
        jobs,nSent = proxy.getJobs(nJobs, siteName, prodSourceLabel, cpu, mem, diskSpace, node, timeout, computingElement,
                                   atlasRelease ,prodUserID, countryGroup, workingGroup, allowOtherCountry,
                                   taskID, background, resourceType, harvester_id, worker_id, schedulerID, jobType,
                                   is_gu, via_topic)
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
        # get secret
        secrets_map = {}
        for job in jobs:
            if job.prodUserName not in secrets_map:
                if not job.use_secrets():
                    secret = None
                else:
                    # get secret
                    proxy = self.proxyPool.getProxy()
                    tmpS, secret = proxy.get_user_secrets(job.prodUserName)
                    if not tmpS:
                        secret = None
                    self.proxyPool.putProxy(proxy)
                secrets_map[job.prodUserName] = secret
            if job.is_debug_mode():
                if panda_config.pilot_secrets not in secrets_map:
                    # get secret
                    proxy = self.proxyPool.getProxy()
                    tmpS, secret = proxy.get_user_secrets(panda_config.pilot_secrets)
                    if not tmpS:
                        secret = None
                    self.proxyPool.putProxy(proxy)
                    secrets_map[panda_config.pilot_secrets] = secret
        # return
        return jobs + [nSent, proxyKey, secrets_map]


    # run task assignment
    def runTaskAssignment(self,jobs):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # loop over all jobs
        retList =[]
        newJobs =[]
        for job in jobs:
            ret = None
            if job.taskID not in ['NULL',0,'']:
                # get cloud
                cloudTask = proxy.getCloudTask(job.taskID)
                if cloudTask is not None and cloudTask.status == 'assigned':
                    ret = cloudTask.cloud
            if ret is None:
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


    # get fairshare policy
    def getFairsharePolicy(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # run
        res = proxy.getFairsharePolicy(True)
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
            tmpKeys = list(idStatus)
            tmpKeys.sort()
            pandaID = tmpKeys[-1]
            # get job
            tmpJobs = self.getFullJobStatus([pandaID])
            if tmpJobs == [] or tmpJobs[0] is None:
                return retNA
            pandaJob = tmpJobs[0]
            # non-merge job
            if '--mergeOutput' not in pandaJob.jobParameters:
                return retNA
            # loop over all sub datasets
            subDsList = []
            mergeStatus = None
            mergeIDs    = []
            for tmpFile in pandaJob.Files:
                if tmpFile.type in ['output','log']:
                    if tmpFile.destinationDBlock not in subDsList:
                        subDsList.append(tmpFile.destinationDBlock)
                        # get dataset
                        tmpDsSpec = self.queryDatasetWithMap({'name':tmpFile.destinationDBlock})
                        if tmpDsSpec is not None:
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
                                if tmpMergeID not in [0,None,'NULL']+mergeIDs:
                                    mergeIDs.append(tmpMergeID)
            # no merger most likely because jobs were killed
            if mergeStatus == 'generated' and mergeIDs == []:
                mergeStatus = 'aborted'
            # jobs are still runnign
            if mergeStatus is None:
                mergeStatus = 'standby'
            # return
            return {'status':mergeStatus,'mergeIDs':mergeIDs}
        except Exception:
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
    def peekJobs(self,jobIDs,fromDefined=True,fromActive=True,fromArchived=True,fromWaiting=True,forAnal=False,
                 use_json=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        retJobs = []
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID,fromDefined,fromActive,fromArchived,fromWaiting,forAnal)
            if res:
                if use_json:
                    retJobs.append(res.to_dict())
                else:
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
                for outKey in tmpRetInfo:
                    if outKey not in retInfo:
                        retInfo[outKey] = []
                    # append
                    for tmpItemRetInfo in tmpRetInfo[outKey]:
                        if tmpItemRetInfo not in retInfo[outKey]:
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
        if match is None:
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
        except Exception:
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
            if retJobMap[jobID] is None:
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
            if tmpJobs == [] or tmpJobs[0] is None:
                errStr = "ERROR: Cannot get PandaID=%s in DB " % pandaID
                if days is None:
                    errStr += "for the last 30 days. You may add &days=N to the URL"
                else:
                    errStr += "for the last {0} days. You may change &days=N in the URL".format(days)
                return errStr
            tmpJob = tmpJobs[0]
            # user job
            isUser = False
            for trf in ['runAthena', 'runGen', 'runcontainer', 'runMerge', 'buildJob', 'buildGen']:
                if trf in tmpJob.transformation:
                    isUser = True
                    break
            # check prodSourceLabel
            if tmpJob.prodSourceLabel == 'user':
                isUser = True
            if isUser:
                tmpAtls = [tmpJob.AtlasRelease]
                tmpRels = [re.sub('^AnalysisTransforms-*', '', tmpJob.homepackage)]
                tmpPars = [tmpJob.jobParameters]
                tmpTrfs = [tmpJob.transformation]
            else:
                # release and trf
                tmpAtls = tmpJob.AtlasRelease.split("\n")
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
                    if tmpFile.dataset not in dsFileMap:
                        dsFileMap[tmpFile.dataset] = []
                    if tmpFile.lfn not in dsFileMap[tmpFile.dataset]:
                        dsFileMap[tmpFile.dataset].append(tmpFile.scope+':'+tmpFile.lfn)
            # get
            for tmpDS in dsFileMap:
                tmpFileList = dsFileMap[tmpDS]
                for tmpLFN in tmpFileList:
                    scrStr += "rucio download "
                    scrStr += "%s\n" % tmpLFN
                    # ln
                    tmpScope,tmpBareLFN = tmpLFN.split(':')
                    scrStr += "ln -fs %s/%s ./%s\n" % (tmpScope,tmpBareLFN,tmpBareLFN)
            if isUser:
                scrStr += "\n#get trf\n"
                scrStr += "wget %s\n" % tmpTrfs[0]
                scrStr += "chmod +x %s\n" % tmpTrfs[0].split('/')[-1]
            scrStr += "\n#transform commands\n\n"
            for tmpIdx,tmpRel in enumerate(tmpRels):
                # asetup
                atlRel = re.sub('Atlas-', '', tmpAtls[tmpIdx])
                atlTags = re.split("/|_", tmpRel)
                if '' in atlTags:
                    atlTags.remove('')
                if atlRel != '' and atlRel not in atlTags and (re.search('^\d+\.\d+\.\d+$', atlRel) is None or isUser):
                    atlTags.append(atlRel)
                try:
                    cmtConfig = [s for s in tmpJob.cmtConfig.split('@') if s][-1]
                except Exception:
                    cmtConfig = ''
                scrStr += 'source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c %s\n' % cmtConfig
                scrStr += "asetup --platform=%s %s\n" % (tmpJob.cmtConfig.split('@')[0], ','.join(atlTags))
                # athenaMP
                if tmpJob.coreCount not in ['NULL',None] and tmpJob.coreCount > 1:
                    scrStr += "export ATHENA_PROC_NUMBER=%s\n" % tmpJob.coreCount
                    scrStr += "export ATHENA_CORE_NUMBER=%s\n" % tmpJob.coreCount
                # add double quotes for zsh
                tmpParamStr = tmpPars[tmpIdx]
                tmpSplitter = shlex.shlex(tmpParamStr, posix=True)
                tmpSplitter.whitespace = ' '
                tmpSplitter.whitespace_split = True
                # loop for params
                for tmpItem in tmpSplitter:
                    tmpMatch = re.search('^(-[^=]+=)(.+)$',tmpItem)
                    if tmpMatch is not None:
                        tmpArgName = tmpMatch.group(1)
                        tmpArgVal  = tmpMatch.group(2)
                        tmpArgIdx = tmpParamStr.find(tmpArgName) + len(tmpArgName)
                        # add "
                        if tmpParamStr[tmpArgIdx] != '"':
                            tmpParamStr = tmpParamStr.replace(tmpMatch.group(0),
                                                              tmpArgName+'"'+tmpArgVal+'"')
                # run trf
                if isUser:
                    scrStr += './'
                scrStr += "%s %s\n\n" % (tmpTrfs[tmpIdx].split('/')[-1], tmpParamStr)
            return scrStr
        except Exception:
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
            # retry event service merge
            toKill = True
            if 'keepUnmerged' in killOptions:
                tmpJobSpec = proxy.peekJob(id,True,True,False,False,False)
                if tmpJobSpec is not None:
                    if EventServiceUtils.isEventServiceMerge(tmpJobSpec):
                        # retry ES merge jobs not to discard events
                        proxy.retryJob(id,{},getNewPandaID=True,attemptNr=tmpJobSpec.attemptNr,
                                       recoverableEsMerge=True)
                    elif EventServiceUtils.isEventServiceJob(tmpJobSpec):
                        # get number of started events
                        nEvt = proxy.getNumStartedEvents(tmpJobSpec)
                        # not to kill jobset if there are started events
                        if nEvt is not None and nEvt > 0:
                            # set sub status if any
                            for killOpt in killOptions:
                                if killOpt.startswith('jobSubStatus'):
                                    tmpJobSpec.jobSubStatus = killOpt.split('=')[-1]
                                    break
                            # trigger ppE for ES jobs to properly trigger subsequent procedures
                            ret = proxy.archiveJob(tmpJobSpec, tmpJobSpec.jobStatus in ['defined','assigned'])
                            toKill = False
                            userInfo = {'prodSourceLabel': None}
            if toKill:
                ret,userInfo = proxy.killJob(id,user,code,prodManager,True,wgProdRole,killOptions)
            rets.append(ret)
            if ret and userInfo['prodSourceLabel'] in ['user','managed','test']:
                jobIDKey = (userInfo['prodUserID'],userInfo['jobDefinitionID'],userInfo['jobsetID'])
                if jobIDKey not in pandaIDforCloserMap:
                    pandaIDforCloserMap[jobIDKey] = id
        # release proxy
        self.proxyPool.putProxy(proxy)
        # run Closer
        try:
            if pandaIDforCloserMap != {}:
                for pandaIDforCloser in pandaIDforCloserMap.values():
                    tmpJobs = self.peekJobs([pandaIDforCloser])
                    tmpJob = tmpJobs[0]
                    if tmpJob is not None:
                        tmpDestDBlocks = []
                        # get destDBlock
                        for tmpFile in tmpJob.Files:
                            if tmpFile.type in ['output','log']:
                                if tmpFile.destinationDBlock not in tmpDestDBlocks:
                                    tmpDestDBlocks.append(tmpFile.destinationDBlock)
                        # run
                        cThr = Closer(self,tmpDestDBlocks,tmpJob)
                        cThr.start()
                        cThr.join()
        except Exception:
            pass
        # return
        return rets


    # reassign jobs
    def reassignJobs(self,ids,attempt=0,joinThr=False,forkSetupper=False,forPending=False,
                     firstSubmission=True):
        tmpLog = LogWrapper(_logger, 'reassignJobs')
        tmpLog.debug('start for {0} IDs'.format(len(ids)))
        # get DB proxy
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
                    if isinstance(tmpRet, tuple):
                        ret,tmpOldSubList = tmpRet
                    else:
                        ret,tmpOldSubList = tmpRet,[]
                    if ret is not None:
                        jobs.append(ret)
                        for tmpOldSub in tmpOldSubList:
                            oldSubMap.setdefault(tmpOldSub, ret)
                        continue
                # try to reset waiting job
                tmpRet = proxy.resetJob(id,False,keepSite=keepSiteFlag,getOldSubs=False,forPending=forPending)
                if isinstance(tmpRet, tuple):
                    ret,tmpOldSubList = tmpRet
                else:
                    ret,tmpOldSubList = tmpRet,[]
                if ret is not None:
                    jobs.append(ret)
                    # waiting jobs don't create sub or dis
                    continue
                # try to reset defined job
                if not forPending:
                    tmpRet = proxy.resetDefinedJob(id,keepSite=keepSiteFlag,getOldSubs=True)
                    if isinstance(tmpRet, tuple):
                        ret,tmpOldSubList = tmpRet
                    else:
                        ret,tmpOldSubList = tmpRet,[]
                    if ret is not None:
                        jobs.append(ret)
                        for tmpOldSub in tmpOldSubList:
                            oldSubMap.setdefault(tmpOldSub, ret)
                        continue
            except Exception as e:
                tmpLog.error('failed with {0} {1}'.format(str(e), traceback.format_exc()))
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # run Closer for old sub datasets
        if not forPending:
            for tmpOldSub in oldSubMap:
                tmpJob = oldSubMap[tmpOldSub]
                cThr = Closer(self,[tmpOldSub],tmpJob)
                cThr.start()
                cThr.join()
        tmpLog.debug('got {0} IDs'.format(len(jobs)))
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
        tmpLog.debug('done')
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
            if ret is not None:
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


    # get and lock sandbox files
    def getLockSandboxFiles(self, time_limit, n_files):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret= proxy.getLockSandboxFiles(time_limit, n_files)
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


    # get and lock dataset with a query
    def getLockDatasets(self, sqlQuery, varMapGet, modTimeOffset='', getVersion=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.getLockDatasets(sqlQuery, varMapGet, modTimeOffset, getVersion)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


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
        for userID in tmpRet:
            nJobs = tmpRet[userID]
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
    def updateSiteData(self,hostID,pilotRequests,interval=3):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.updateSiteData(hostID,pilotRequests,interval)
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

    # get ban users
    def get_ban_users(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.get_ban_users()
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


    # query an SQL
    def querySQL(self, sql, varMap, arraySize=1000):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.querySQLS(sql, varMap, arraySize)[1]
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # execute an SQL return with executemany
    def executemanySQL(self, sql, varMaps, arraySize=1000):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.executemanySQL(sql, varMaps, arraySize)
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
        if tmpStatus is True:
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
                             comQualifier=None, broadcast=False):
        # query an SQL return Status
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.sendCommandTaskPanda(jediTaskID,dn,prodRole,comStr,comComment,useCommit,
                                         properErrorCode, comQualifier, broadcast)
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
    def getJediTasksInTimeRange(self, dn, timeRangeStr, fullFlag=False, minTaskID=None, task_type='user'):
        # check DN
        if dn in ['NULL','','None',None]:
            return {}
        # check timeRange
        match = re.match('^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$',timeRangeStr)
        if match is None:
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
        ret = proxy.getJediTasksInTimeRange(dn, timeRange, fullFlag, minTaskID, task_type)
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
    def getEventRanges(self, pandaID, jobsetID, jediTaskID, nRanges, acceptJson, scattered, segment_id):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getEventRanges(pandaID, jobsetID, jediTaskID, nRanges, acceptJson, scattered, segment_id)
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
        except Exception:
            retVal = False
        # return
        return retVal,json.dumps(ret[1])



    # update even ranges
    def updateEventRanges(self,eventRanges,version=0):
        # decode json
        try:
            eventRanges = json.loads(eventRanges)
        except Exception:
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
    def throttleUserJobs(self,prodUserName, workingGroup, get_dict=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.throttleUserJobs(prodUserName, workingGroup, get_dict)
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
    def unThrottleUserJobs(self,prodUserName, workingGroup, get_dict=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.unThrottleUserJobs(prodUserName, workingGroup, get_dict)
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


    # get prodSourceLabel from task ID
    def getProdSourceLabelwithTaskID(self, taskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getProdSourceLabelwithTaskID(taskID)
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
    def resetFileStatusInJEDI(self,dn,prodManager,datasetName,lostFiles,recoverParent,simul=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.resetFileStatusInJEDI(dn,prodManager,datasetName,lostFiles,recoverParent,simul)
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



    # retry module action: set maxAttempt to the current attemptNr to avoid further retries
    def setNoRetry(self, jobID, jediTaskID, files):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.setNoRetry(jobID, jediTaskID, files)
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
    def getCoJumboJobsToBeFinished(self,timeLimit,minPriority,maxJobs):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getCoJumboJobsToBeFinished(timeLimit,minPriority,maxJobs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret



    # get number of events to be processed
    def getNumReadyEvents(self, jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getNumReadyEvents(jediTaskID)
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
    def reactivateTask(self, jediTaskID, keep_attempt_nr=False, trigger_job_generation=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reactivateTask(jediTaskID, keep_attempt_nr, trigger_job_generation)
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
    def reassignShare(self, jedi_task_ids, share_dest, reassign_running):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reassignShare(jedi_task_ids, share_dest, reassign_running)
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


    # send command to harvester or lock command
    def commandToHarvester(self, harvester_ID, command, ack_requested, status, lockInterval=None, comInterval= None, params=None):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.commandToHarvester(harvester_ID, command, ack_requested, status, lockInterval, comInterval, params)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    def getResourceTypes(self):
        """
        Get resource types (SCORE, MCORE, SCORE_HIMEM, MCORE_HIMEM) and their definitions
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.load_resource_types(formatting='dict')
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # report stat of workers
    def reportWorkerStats(self, harvesterID, siteName, paramsList):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reportWorkerStats(harvesterID, siteName, paramsList)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # report stat of workers
    def reportWorkerStats_jobtype(self, harvesterID, siteName, paramsList):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reportWorkerStats_jobtype(harvesterID, siteName, paramsList)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # get command locks
    def getCommandLocksHarvester(self, harvester_ID, command, lockedBy,
                                 lockInterval, commandInterval):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getCommandLocksHarvester(harvester_ID, command, lockedBy,
                                             lockInterval, commandInterval)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # release command lock
    def releaseCommandLockHarvester(self, harvester_ID, command, computingSite, resourceType, lockedBy):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.releaseCommandLockHarvester(harvester_ID, command, computingSite, resourceType, lockedBy)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # return
        return res


    # get active harvesters
    def getActiveHarvesters(self, interval):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getActiveHarvesters(interval)
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


    # update workers
    def updateServiceMetrics(self, harvesterID, data):
        """
        Update workers
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.updateServiceMetrics(harvesterID, data)
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
    def load_resource_types(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.load_resource_types()
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


    def reset_resource_type_task(self, jedi_task_id, use_commit = True):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.reset_resource_type_task(jedi_task_id, use_commit)
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


    # get activated job statistics per resource
    def getActivatedJobStatisticsPerResource(self, siteName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.getActivatedJobStatisticsPerResource(siteName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret_val


    # check Job status
    def checkJobStatus(self, pandaIDs):
        try:
            pandaIDs = pandaIDs.split(',')
        except Exception:
            pandaIDs = []
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retList = []
        for pandaID in pandaIDs:
            ret = proxy.checkJobStatus(pandaID)
            retList.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return retList


    # get stat of workers
    def getWorkerStats(self, siteName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getWorkerStats(siteName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get minimal resource
    def getMinimalResource(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getMinimalResource()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get unified pilot streaming queues
    def ups_get_queues(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.ups_get_queues()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # load harvester worker stats
    def ups_load_worker_stats(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.ups_load_worker_stats()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get the distribution of new workers to submit
    def ups_new_worker_distribution(self, queue, worker_stats):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.ups_new_worker_distribution(queue, worker_stats)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check event availability
    def checkEventsAvailability(self, pandaID, jobsetID, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.checkEventsAvailability(pandaID, jobsetID, jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get LNFs for jumbo job
    def getLFNsForJumbo(self, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getLFNsForJumbo(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get active job attribute
    def getActiveJobAttributes(self, pandaID, attrs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getActiveJobAttributes(pandaID, attrs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get original consumers
    def getOriginalConsumers(self, jediTaskID, jobsetID, pandaID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getOriginalConsumers(jediTaskID, jobsetID, pandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # add harvester dialog messages
    def addHarvesterDialogs(self, harvesterID, dialogs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.addHarvesterDialogs(harvesterID, dialogs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get job statistics per site and resource
    def getJobStatisticsPerSiteResource(self, timeWindow=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJobStatisticsPerSiteResource(timeWindow)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    # get job statistics per site, source label, and resource type
    def get_job_statistics_per_site_label_resource(self, time_window=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.get_job_statistics_per_site_label_resource(time_window)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    # set num slots for workload provisioning
    def setNumSlotsForWP(self, pandaQueueName, numSlots, gshare, resourceType, validPeriod):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # enable jumbo jobs
    def enableJumboJobs(self, jediTaskID, totalJumboJobs, nJumboPerSite):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.enableJumboJobs(jediTaskID, totalJumboJobs, nJumboPerSite)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # enable event service
    def enableEventService(self, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.enableEventService(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get JEDI file attributes
    def getJediFileAttributes(self, PandaID, jediTaskID, datasetID, fileID, attrs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJediFileAttributes(PandaID, jediTaskID, datasetID, fileID, attrs)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # check if super user
    def isSuperUser(self, userName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.isSuperUser(userName)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get workers for a job
    def getWorkersForJob(self, PandaID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getWorkersForJob(PandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get user job metadata
    def getUserJobMetadata(self, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getUserJobMetadata(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get jumbo job datasets
    def getJumboJobDatasets(self, n_days, grace_period):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJumboJobDatasets(n_days, grace_period)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get global shares status
    def getGShareStatus(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getGShareStatus()
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret


    # get output datasets
    def getOutputDatasetsJEDI(self, panda_id):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getOutputDatasetsJEDI(panda_id)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    # update/insert JSON queue information into the scheconfig replica
    def upsertQueuesInJSONSchedconfig(self, schedconfig_dump):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.upsertQueuesInJSONSchedconfig(schedconfig_dump)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    # update/insert SW tag information
    def loadSWTags(self, sw_tags):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.loadSWTags(sw_tags)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    # generate a harvester command to clean up the workers of a site
    def sweepPQ(self, panda_queue_des, status_list_des, ce_list_des, submission_host_list_des):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.sweepPQ(panda_queue_des, status_list_des, ce_list_des, submission_host_list_des)
        # release proxy
        self.proxyPool.putProxy(proxy)
        # return
        return ret

    # lock process
    def lockProcess_PANDA(self, component, pid, time_limit=5, force=False):
        proxy = self.proxyPool.getProxy()
        ret = proxy.lockProcess_PANDA(component, pid, time_limit, force)
        self.proxyPool.putProxy(proxy)
        return ret

    # unlock process
    def unlockProcess_PANDA(self, component, pid):
        proxy = self.proxyPool.getProxy()
        ret = proxy.unlockProcess_PANDA(component, pid)
        self.proxyPool.putProxy(proxy)
        return ret

    # check process lock
    def checkProcessLock_PANDA(self, component, pid, time_limit, check_base=False):
        proxy = self.proxyPool.getProxy()
        ret = proxy.checkProcessLock_PANDA(component, pid, time_limit, check_base)
        self.proxyPool.putProxy(proxy)
        return ret

    # insert job output report
    def insertJobOutputReport(self, panda_id, prod_source_label,
                                job_status, attempt_nr, data):
        proxy = self.proxyPool.getProxy()
        ret = proxy.insertJobOutputReport(panda_id, prod_source_label,
                                    job_status, attempt_nr, data)
        self.proxyPool.putProxy(proxy)
        return ret

    # deleted job output report
    def deleteJobOutputReport(self, panda_id, attempt_nr):
        proxy = self.proxyPool.getProxy()
        ret = proxy.deleteJobOutputReport(panda_id, attempt_nr)
        self.proxyPool.putProxy(proxy)
        return ret

    # update data of job output report
    def updateJobOutputReport(self, panda_id, attempt_nr, data):
        proxy = self.proxyPool.getProxy()
        ret = proxy.updateJobOutputReport(panda_id, attempt_nr, data)
        self.proxyPool.putProxy(proxy)
        return ret

    # get job output report
    def getJobOutputReport(self, panda_id, attempt_nr):
        proxy = self.proxyPool.getProxy()
        ret = proxy.getJobOutputReport(panda_id, attempt_nr)
        self.proxyPool.putProxy(proxy)
        return ret

    # lock job output report
    def lockJobOutputReport(self, panda_id, attempt_nr, pid, time_limit, take_over_from=None):
        proxy = self.proxyPool.getProxy()
        ret = proxy.lockJobOutputReport(panda_id, attempt_nr, pid, time_limit, take_over_from)
        self.proxyPool.putProxy(proxy)
        return ret

    # unlock job output report
    def unlockJobOutputReport(self, panda_id, attempt_nr, pid, lock_offset):
        proxy = self.proxyPool.getProxy()
        ret = proxy.unlockJobOutputReport(panda_id, attempt_nr, pid, lock_offset)
        self.proxyPool.putProxy(proxy)
        return ret

    # list pandaID and attemptNr of job output report
    def listJobOutputReport(self, only_unlocked=False, time_limit=5, limit=999999, grace_period=3, labels=None,
                            anti_labels=None):
        proxy = self.proxyPool.getProxy()
        ret = proxy.listJobOutputReport(only_unlocked, time_limit, limit, grace_period, labels, anti_labels)
        self.proxyPool.putProxy(proxy)
        return ret

    # update problematic resource info for user
    def update_problematic_resource_info(self, user_name, jedi_task_id, resource, problem_type):
        proxy = self.proxyPool.getProxy()
        ret = proxy.update_problematic_resource_info(user_name, jedi_task_id, resource, problem_type)
        self.proxyPool.putProxy(proxy)
        return ret

    # send command to a job
    def send_command_to_job(self, panda_id, com):
        proxy = self.proxyPool.getProxy()
        ret = proxy.send_command_to_job(panda_id, com)
        self.proxyPool.putProxy(proxy)
        return ret

    # get workers with stale states and update them with pilot information
    def get_workers_to_synchronize(self):
        proxy = self.proxyPool.getProxy()
        ret = proxy.get_workers_to_synchronize()
        self.proxyPool.putProxy(proxy)
        return ret

    # set user secret
    def set_user_secret(self, owner, key, value):
        proxy = self.proxyPool.getProxy()
        ret = proxy.set_user_secret(owner, key, value)
        self.proxyPool.putProxy(proxy)
        return ret

    # get user secrets
    def get_user_secrets(self, owner, keys=None, get_json=False):
        proxy = self.proxyPool.getProxy()
        ret = proxy.get_user_secrets(owner, keys, get_json)
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_write_sites(self, sites_list):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_write_sites(sites_list)
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_write_panda_sites(self, panda_site_list):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_write_panda_sites(panda_site_list)
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_write_ddm_endpoints(self, ddm_endpoint_list):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_write_ddm_endpoints(ddm_endpoint_list)
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_write_panda_ddm_relations(self, relation_list):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_write_panda_ddm_relations(relation_list)
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_read_sites(self):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_read_sites()
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_read_panda_sites(self):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_read_panda_sites()
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_read_ddm_endpoints(self):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_read_ddm_endpoints()
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_read_cric_sites(self):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_read_cric_sites()
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_read_cric_panda_sites(self):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_read_cric_panda_sites()
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_delete_sites(self, sites_to_delete):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_delete_sites(sites_to_delete)
        self.proxyPool.putProxy(proxy)
        return ret

    def configurator_delete_panda_sites(self, panda_sites_to_delete):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_delete_panda_sites(panda_sites_to_delete)
        self.proxyPool.putProxy(proxy)
        return ret
    def configurator_delete_ddm_endpoints(self, ddm_endpoints_to_delete):
        proxy = self.proxyPool.getProxy()
        ret = proxy.configurator_delete_ddm_endpoints(ddm_endpoints_to_delete)
        self.proxyPool.putProxy(proxy)
        return ret

# Singleton
taskBuffer = TaskBuffer()
