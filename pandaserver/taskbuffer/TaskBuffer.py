import datetime
import json
import re
import shlex
import sys
import time
import traceback
from threading import Lock

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.closer import Closer
from pandaserver.dataservice.setupper import Setupper
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import ErrorCode, EventServiceUtils, JobUtils, ProcessGroups
from pandaserver.taskbuffer.DBProxyPool import DBProxyPool

_logger = PandaLogger().getLogger("TaskBuffer")


class TaskBuffer:
    """
    task queue

    """

    # constructor
    def __init__(self):
        self.proxyPool = None
        self.lock = Lock()
        self.nDBConnection = None

        # save the requester for monitoring/logging purposes
        self.start_time = time.time()

        # site mapper
        self.site_mapper = None
        # update time for site mapper
        self.last_update_site_mapper = None

    def __repr__(self):
        return "TaskBuffer"

    # initialize
    def init(self, dbname, dbpass, nDBConnection=10, useTimeout=False, requester=None):
        # acquire lock
        self.lock.acquire()
        self.nDBConnection = nDBConnection

        # create Proxy Pool
        if self.proxyPool is None:
            _logger.info(f"creating DBProxyPool with n_connections={nDBConnection} on behalf of {requester}")
            self.start_time = time.time()
            self.proxyPool = DBProxyPool(dbname, dbpass, nDBConnection, useTimeout)

        # release lock
        self.lock.release()

    # cleanup
    def cleanup(self, requester=None):
        if self.proxyPool:
            try:
                pool_duration = time.time() - self.start_time
            except TypeError:
                pool_duration = -1  # duration unknown

            _logger.info(f"destroying DBProxyPool after n_seconds={pool_duration} on behalf of {requester}")
            self.proxyPool.cleanup()

    # get number of database connections
    def get_num_connections(self):
        return self.nDBConnection

    # get SiteMapper
    def get_site_mapper(self):
        time_now = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if self.last_update_site_mapper is None or datetime.datetime.now(datetime.timezone.utc).replace(
            tzinfo=None
        ) - self.last_update_site_mapper > datetime.timedelta(minutes=10):
            self.site_mapper = SiteMapper(self)
            self.last_update_site_mapper = time_now
        return self.site_mapper

    # check production role
    def checkProdRole(self, fqans):
        for fqan in fqans:
            # check production role
            match = re.search("/([^/]+)/Role=production", fqan)
            if match is not None:
                return True, match.group(1)
        return False, None

    # get priority parameters for user
    def getPrioParameters(self, jobs, user, fqans, userDefinedWG, validWorkingGroup):
        priorityOffset = 0
        serNum = 0
        weight = None
        prio_reduction = True
        # get boosted users and groups
        boost_dict = {}
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # check production role
        withProdRole, workingGroup = self.checkProdRole(fqans)
        if withProdRole and jobs != []:
            # check dataset name
            for tmpFile in jobs[-1].Files:
                if tmpFile.type in ["output", "log"] and not (tmpFile.lfn.startswith("group") or tmpFile.lfn.startswith("panda.um.group")):
                    # reset
                    withProdRole, workingGroup = False, None
                    break
        # reset nJob/weight for HC
        if jobs != []:
            if jobs[0].processingType in [
                "hammercloud",
                "gangarobot",
                "hammercloud-fax",
            ] or jobs[
                0
            ].processingType.startswith("gangarobot-"):
                serNum = 0
                weight = 0.0
            elif jobs[0].processingType in ["gangarobot", "gangarobot-pft"]:
                priorityOffset = 3000
            elif jobs[0].processingType in ["hammercloud-fax"]:
                priorityOffset = 1001
            else:
                # get users and groups to boost job priorities
                boost_dict = proxy.get_dict_to_boost_job_prio(jobs[-1].VO)
                if boost_dict:
                    prodUserName = proxy.cleanUserID(user)
                    # check boost list
                    if userDefinedWG and validWorkingGroup:
                        if "group" in boost_dict and jobs[-1].workingGroup in boost_dict["group"]:
                            priorityOffset = boost_dict["group"][jobs[-1].workingGroup]
                            weight = 0.0
                            prio_reduction = False
                    else:
                        if "user" in boost_dict and prodUserName in boost_dict["user"]:
                            priorityOffset = boost_dict["user"][prodUserName]
                            weight = 0.0
                            prio_reduction = False
        # check quota
        if weight is None:
            weight = proxy.checkQuota(user)
            # get nJob
            if jobs == []:
                serNum = proxy.getNumberJobsUser(user, workingGroup=userDefinedWG)
            elif userDefinedWG and validWorkingGroup:
                # check if group privileged
                isSU, isGU = proxy.isSuperUser(jobs[0].workingGroup)
                if not isSU:
                    serNum = proxy.getNumberJobsUser(user, workingGroup=jobs[0].workingGroup)
                else:
                    # set high priority for production role
                    serNum = 0
                    weight = 0.0
                    priorityOffset = 2000
            else:
                serNum = proxy.getNumberJobsUser(user, workingGroup=None)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return (
            withProdRole,
            workingGroup,
            priorityOffset,
            serNum,
            weight,
            prio_reduction,
        )

    # store Jobs into DB
    def storeJobs(
        self,
        jobs,
        user,
        joinThr=False,
        fqans=[],
        hostname="",
        checkSpecialHandling=True,
        toPending=False,
        oldPandaIDs=None,
        relationType=None,
        userVO="atlas",
        esJobsetMap=None,
        getEsJobsetMap=False,
        unprocessedMap=None,
        bulk_job_insert=False,
        trust_user=False,
    ):
        try:
            tmpLog = LogWrapper(_logger, f"storeJobs <{CoreUtils.clean_user_id(user)} nJobs={len(jobs)}>")
            tmpLog.debug(f"start toPending={toPending}")
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
            siteMapper = self.get_site_mapper()

            # check ban user
            if not trust_user and len(jobs) > 0:
                # get DB proxy
                proxy = self.proxyPool.getProxy()
                # check user status
                tmpStatus = proxy.checkBanUser(user, jobs[0].prodSourceLabel)
                # release proxy
                self.proxyPool.putProxy(proxy)
                # return if DN is blocked
                if not tmpStatus:
                    tmpLog.debug(f"end 1 since DN {user} is blocked")
                    if getEsJobsetMap:
                        return [], None, unprocessedMap
                    return []

            tmpLog.debug(f"checked ban user")
            # set parameters for user jobs
            if (
                len(jobs) > 0
                and (jobs[0].prodSourceLabel in JobUtils.analy_sources + JobUtils.list_ptest_prod_sources)
                and (not jobs[0].processingType in ["merge", "unmerge"])
            ):
                # get DB proxy
                proxy = self.proxyPool.getProxy()
                # get JobID and status
                userJobID, userJobsetID, userStatus = proxy.getUserParameter(user, jobs[0].jobDefinitionID, jobs[0].jobsetID)

                # check quota for express jobs
                if "express" in jobs[0].specialHandling:
                    expressQuota = proxy.getExpressJobs(user)
                    if expressQuota is not None and expressQuota["status"] and expressQuota["quota"] > 0:
                        nExpressJobs = expressQuota["quota"]
                        if nExpressJobs > 0:
                            useExpress = True
                # debug mode
                if jobs[0].is_debug_mode() or jobs[-1].is_debug_mode():
                    useDebugMode = True
                # release proxy
                self.proxyPool.putProxy(proxy)

                # extract country group
                for tmpFQAN in fqans:
                    match = re.search("^/atlas/([^/]+)/", tmpFQAN)
                    if match is not None:
                        tmpCountry = match.group(1)
                        # use country code or usatlas
                        if len(tmpCountry) == 2:
                            userCountry = tmpCountry
                            break
                        # usatlas
                        if tmpCountry in ["usatlas"]:
                            userCountry = "us"
                            break
            tmpLog.debug(f"set user job parameters")

            # return if DN is blocked
            if not userStatus:
                tmpLog.debug(f"end 2 since {user} DN is blocked")
                if getEsJobsetMap:
                    return ([], None, unprocessedMap)
                return []
            # extract VO
            for tmpFQAN in fqans:
                match = re.search("^/([^/]+)/", tmpFQAN)
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
            if len(jobs) > 0 and (jobs[0].prodSourceLabel in JobUtils.analy_sources) and (not jobs[0].processingType in ["merge", "unmerge"]):
                # extract user's working group from FQANs
                userWorkingGroupList = []
                for tmpFQAN in fqans:
                    match = re.search("/([^/]+)/Role=production", tmpFQAN)
                    if match is not None:
                        userWorkingGroupList.append(match.group(1))
                # check workingGroup
                if jobs[0].workingGroup not in ["", None, "NULL"]:
                    userDefinedWG = True
                    # check with FQANs
                    if jobs[0].workingGroup in userWorkingGroupList:
                        validWorkingGroup = True
                # using build for analysis
                if jobs[0].prodSourceLabel == "panda":
                    usingBuild = True
                # get priority parameters for user
                (
                    withProdRole,
                    workingGroup,
                    priorityOffset,
                    serNum,
                    weight,
                    prio_reduction,
                ) = self.getPrioParameters(jobs, user, fqans, userDefinedWG, validWorkingGroup)
                tmpLog.debug(f"workingGroup={jobs[0].workingGroup} serNum={serNum} weight={weight} pOffset={priorityOffset} reduction={prio_reduction}")
            tmpLog.debug(f"got prio parameters")
            # get DB proxy
            proxy = self.proxyPool.getProxy()
            tmpLog.debug(f"got proxy")
            # get total number of files
            totalNumFiles = 0
            for job in jobs:
                totalNumFiles += len(job.Files)
            # bulk fetch PandaIDs
            new_panda_ids = proxy.bulk_fetch_panda_ids(len(jobs))
            tmpLog.debug(f"got PandaIDs")
            # bulk fetch fileIDs
            fileIDPool = []
            if totalNumFiles > 0:
                fileIDPool = sorted(proxy.bulkFetchFileIDsPanda(totalNumFiles))
            # loop over all jobs
            ret = []
            newJobs = []
            nRunJob = 0
            if esJobsetMap is None:
                esJobsetMap = {}
            try:
                tmpLog.debug(f"jediTaskID={jobs[0].jediTaskID} len(esJobsetMap)={len(esJobsetMap)} nJobs={len(jobs)}")
            except Exception:
                pass
            job_ret_list = []
            params_for_bulk_insert = []
            special_handling_list = []
            num_original_event_service_jobs = 0
            for idxJob, job in enumerate(jobs):
                # set PandaID
                job.PandaID = new_panda_ids[idxJob]
                # set JobID. keep original JobID when retry
                if (
                    userJobID != -1
                    and job.prodSourceLabel in JobUtils.analy_sources
                    and (job.attemptNr in [0, "0", "NULL"] or (job.jobExecutionID not in [0, "0", "NULL"]) or job.lockedby == "jedi")
                    and (not jobs[0].processingType in ["merge", "unmerge"])
                ):
                    job.jobDefinitionID = userJobID
                # set jobsetID
                if job.prodSourceLabel in JobUtils.analy_sources + JobUtils.list_ptest_prod_sources:
                    job.jobsetID = userJobsetID
                # set specialHandling
                if job.prodSourceLabel in JobUtils.analy_sources:
                    if checkSpecialHandling:
                        specialHandling = ""
                        # debug mode
                        if useDebugMode and job.prodSourceLabel == "user":
                            specialHandling += "debug,"
                        # express mode
                        if useExpress and (nRunJob < nExpressJobs or job.prodSourceLabel == "panda"):
                            specialHandling += "express,"
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
                    if job.prodSourceLabel != "panda":
                        nRunJob += 1
                # set relocation flag
                if job.computingSite != "NULL" and job.relocationFlag != 2:
                    job.relocationFlag = 1
                # protection agains empty jobParameters
                if job.jobParameters in ["", None, "NULL"]:
                    job.jobParameters = " "
                # set country group and nJobs (=taskID)
                if job.prodSourceLabel in JobUtils.analy_sources:
                    if job.lockedby != "jedi":
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
                if hostname != "":
                    job.creationHost = hostname

                # process and set the job_label
                if not job.job_label or job.job_label not in (
                    JobUtils.PROD_PS,
                    JobUtils.ANALY_PS,
                ):
                    tmpSiteSpec = siteMapper.getSite(job.computingSite)
                    queue_type = tmpSiteSpec.type
                    if queue_type == "analysis":
                        job.job_label = JobUtils.ANALY_PS
                    elif queue_type == "production":
                        job.job_label = JobUtils.PROD_PS
                    elif queue_type == "unified":
                        if job.prodSourceLabel in JobUtils.analy_sources:
                            job.job_label = JobUtils.ANALY_PS
                        else:
                            # set production as default if not specified and neutral prodsourcelabel
                            job.job_label = JobUtils.PROD_PS
                    else:  # e.g. type = special
                        job.job_label = JobUtils.PROD_PS

                # extract file info, change specialHandling for event service
                origSH = job.specialHandling
                (
                    eventServiceInfo,
                    job.specialHandling,
                    esIndex,
                ) = EventServiceUtils.decodeFileInfo(job.specialHandling)
                origEsJob = False
                if eventServiceInfo != {}:
                    # set jobsetID
                    if esIndex in esJobsetMap:
                        job.jobsetID = esJobsetMap[esIndex]
                    else:
                        origEsJob = True
                        num_original_event_service_jobs += 1
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
                if not bulk_job_insert:
                    tmp_ret_i = proxy.insertNewJob(
                        job,
                        user,
                        serNum,
                        weight,
                        priorityOffset,
                        userVO,
                        toPending,
                        origEsJob,
                        eventServiceInfo,
                        oldPandaIDs=jobOldPandaIDs,
                        relationType=relationType,
                        fileIDPool=fileIDPool,
                        origSpecialHandling=origSH,
                        unprocessedMap=unprocessedMap,
                        prio_reduction=prio_reduction,
                    )
                    if unprocessedMap is not None:
                        tmp_ret_i, unprocessedMap = tmp_ret_i
                else:
                    # keep parameters for late bulk execution
                    params_for_bulk_insert.append(
                        [
                            [job, user, serNum, weight, priorityOffset, userVO, toPending],
                            {
                                "origEsJob": origEsJob,
                                "eventServiceInfo": eventServiceInfo,
                                "oldPandaIDs": jobOldPandaIDs,
                                "relationType": relationType,
                                "fileIDPool": fileIDPool,
                                "origSpecialHandling": origSH,
                                "unprocessedMap": unprocessedMap,
                                "prio_reduction": prio_reduction,
                            },
                        ]
                    )
                    special_handling_list.append(origSH)
                    tmp_ret_i = True
                if tmp_ret_i and origEsJob:
                    # mapping of jobsetID for event service
                    esJobsetMap[esIndex] = job.jobsetID
                job_ret_list.append([job, tmp_ret_i])
                serNum += 1
                try:
                    fileIDPool = fileIDPool[len(job.Files) :]
                except Exception:
                    fileIDPool = []
            # bulk insert
            if bulk_job_insert:
                # get jobset IDs for event service jobs
                if num_original_event_service_jobs > 0:
                    new_jobset_ids = proxy.bulk_fetch_panda_ids(num_original_event_service_jobs)
                else:
                    new_jobset_ids = []
                tmp_ret, job_ret_list = proxy.bulk_insert_new_jobs(jobs[0].jediTaskID, params_for_bulk_insert, new_jobset_ids, special_handling_list)
                if not tmp_ret:
                    raise RuntimeError("bulk job insert failed")

            # check returns
            for job, tmp_ret_i in job_ret_list:
                if not tmp_ret_i:
                    # reset if failed
                    job.PandaID = None
                else:
                    # append
                    newJobs.append(job)
                if job.prodSourceLabel in JobUtils.analy_sources + JobUtils.list_ptest_prod_sources:
                    ret.append((job.PandaID, job.jobDefinitionID, {"jobsetID": job.jobsetID}))
                else:
                    ret.append((job.PandaID, job.jobDefinitionID, job.jobName))
            # release DB proxy
            self.proxyPool.putProxy(proxy)
            # set up dataset
            if not toPending:
                if joinThr:
                    thr = Setupper(
                        self,
                        newJobs,
                    )
                    thr.start()
                    thr.join()
                else:
                    # cannot use 'thr =' because it may trigger garbage collector
                    Setupper(
                        self,
                        newJobs,
                    ).start()
            # return jobIDs
            tmpLog.debug("end successfully")
            if getEsJobsetMap:
                return (ret, esJobsetMap, unprocessedMap)
            return ret
        except Exception as e:
            tmpLog.error(f"{str(e)} {traceback.format_exc()}")
            errStr = "ERROR: ServerError with storeJobs"
            if getEsJobsetMap:
                return (errStr, None, unprocessedMap)
            return errStr

    # lock jobs for reassign
    def lockJobsForReassign(
        self,
        tableName,
        timeLimit,
        statList,
        labels,
        processTypes,
        sites,
        clouds,
        useJEDI=False,
        onlyReassignable=False,
        useStateChangeTime=False,
        getEventService=False,
    ):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.lockJobsForReassign(
            tableName,
            timeLimit,
            statList,
            labels,
            processTypes,
            sites,
            clouds,
            useJEDI,
            onlyReassignable,
            useStateChangeTime,
            getEventService,
        )
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # get a DB configuration value
    def getConfigValue(self, component, key, app="pandaserver", vo=None):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getConfigValue(component, key, app, vo)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # lock jobs for finisher
    def lockJobsForFinisher(self, timeNow, rownum, highPrio):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.lockJobsForFinisher(timeNow, rownum, highPrio)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # lock jobs for activator
    def lockJobsForActivator(self, timeLimit, rownum, prio):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.lockJobsForActivator(timeLimit, rownum, prio)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # update overall job information
    def updateJobs(
        self,
        jobs,
        inJobsDefined,
        oldJobStatusList=None,
        extraInfo=None,
        async_dataset_update=False,
    ):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # loop over all jobs
        returns = []
        ddmIDs = []
        ddmAttempt = 0
        newMover = None
        for idxJob, job in enumerate(jobs):
            # update DB
            tmpddmIDs = []
            if oldJobStatusList is not None and idxJob < len(oldJobStatusList):
                oldJobStatus = oldJobStatusList[idxJob]
            else:
                oldJobStatus = None
            # check for jumbo jobs
            if EventServiceUtils.isJumboJob(job):
                if job.jobStatus in ["defined", "assigned", "activated"]:
                    pass
                else:
                    # check if there are done events
                    hasDone = proxy.hasDoneEvents(job.jediTaskID, job.PandaID, job)
                    if hasDone:
                        job.jobStatus = "finished"
                    else:
                        if job.pilotErrorCode in [1144, "1144"]:
                            job.jobStatus = "cancelled"
                        else:
                            job.jobStatus = "failed"
                        if job.taskBufferErrorDiag in ["", "NULL", None]:
                            job.taskBufferErrorDiag = f"set {job.jobStatus} since no successful events"
                            job.taskBufferErrorCode = ErrorCode.EC_EventServiceNoEvent
            if job.jobStatus in ["finished", "failed", "cancelled"]:
                if async_dataset_update and job.jediTaskID not in [None, "NULL"]:
                    async_params = {
                        "exec_order": 0,
                        "PandaID": job.PandaID,
                        "jediTaskID": job.jediTaskID,
                    }
                else:
                    async_params = None
                ret, tmpddmIDs, ddmAttempt, newMover = proxy.archiveJob(job, inJobsDefined, extraInfo=extraInfo, async_params=async_params)
                if async_params is not None and ret:
                    proxy.async_update_datasets(job.PandaID)
            else:
                ret = proxy.updateJob(job, inJobsDefined, oldJobStatus=oldJobStatus, extraInfo=extraInfo)
            returns.append(ret)
            # collect IDs for reassign
            if ret:
                ddmIDs += tmpddmIDs
        # release proxy
        self.proxyPool.putProxy(proxy)
        # retry mover
        if newMover is not None:
            self.storeJobs([newMover], None, joinThr=True)
        # reassign jobs when ddm failed
        if ddmIDs != []:
            self.reassignJobs(ddmIDs, ddmAttempt, joinThr=True)

        return returns

    # update job jobStatus only
    def updateJobStatus(self, jobID, jobStatus, param, updateStateChange=False, attemptNr=None):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB and buffer
        ret, post_action = proxy.updateJobStatus(jobID, jobStatus, param, updateStateChange, attemptNr)
        # take post-action
        if post_action:
            # get semaphore for job cloning with runonce
            if post_action["action"] == "get_event":
                event_ret = proxy.getEventRanges(post_action["pandaID"], post_action["jobsetID"], post_action["jediTaskID"], 1, True, False, None)
                if not event_ret:
                    proxy.killJob(post_action["pandaID"], "job cloning", "", True)
                    ret = "tobekilled"
        # get secrets for debug mode
        if isinstance(ret, str) and "debug" in ret:
            tmpS, secrets = proxy.get_user_secrets(panda_config.pilot_secrets)
            if tmpS and secrets:
                ret = {"command": ret, "secrets": secrets}
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret

    # update worker status by the pilot
    def updateWorkerPilotStatus(self, workerID, harvesterID, status, node_id):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB and buffer
        ret = proxy.updateWorkerPilotStatus(workerID, harvesterID, status, node_id)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret

    # finalize pending analysis jobs
    def finalizePendingJobs(self, prodUserName, jobDefinitionID, waitLock=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB
        ret = proxy.finalizePendingJobs(prodUserName, jobDefinitionID, waitLock)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret

    # retry job
    def retryJob(
        self,
        jobID,
        param,
        failedInActive=False,
        changeJobInMem=False,
        inMemJob=None,
        getNewPandaID=False,
        attemptNr=None,
        recoverableEsMerge=False,
    ):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # update DB
        ret = proxy.retryJob(
            jobID,
            param,
            failedInActive,
            changeJobInMem,
            inMemJob,
            getNewPandaID,
            attemptNr,
            recoverableEsMerge,
        )
        # release proxy
        self.proxyPool.putProxy(proxy)
        return ret

    # activate jobs
    def activateJobs(self, jobs):
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
    def keepJobs(self, jobs):
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
    def archiveJobs(self, jobs, inJobsDefined, fromJobsWaiting=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # loop over all jobs
        returns = []
        for job in jobs:
            # update DB
            ret = proxy.archiveJob(job, inJobsDefined, fromJobsWaiting=fromJobsWaiting)
            returns.append(ret[0])
        # release proxy
        self.proxyPool.putProxy(proxy)
        return returns

    # set debug mode
    def setDebugMode(self, dn, pandaID, prodManager, modeOn, workingGroup):
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
                retStr = "You already hit the limit on the maximum number of debug subjobs "
                retStr += f"({limitNum} jobs). "
                retStr += "Please set the debug mode off for one of the following PandaIDs : "
                for tmpID in jobList:
                    retStr += f"{tmpID},"
                retStr = retStr[:-1]
                hitLimit = True
        if not hitLimit:
            # execute
            retStr = proxy.setDebugMode(dn, pandaID, prodManager, modeOn, workingGroup)
        # release proxy
        self.proxyPool.putProxy(proxy)
        return retStr

    # get jobs
    def getJobs(
        self,
        nJobs,
        siteName,
        prodSourceLabel,
        mem,
        diskSpace,
        node,
        timeout,
        computingElement,
        prodUserID,
        taskID,
        background,
        resourceType,
        harvester_id,
        worker_id,
        schedulerID,
        jobType,
        is_gu,
        via_topic,
    ):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get waiting jobs
        t_before = time.time()
        jobs, nSent = proxy.getJobs(
            nJobs,
            siteName,
            prodSourceLabel,
            mem,
            diskSpace,
            node,
            timeout,
            computingElement,
            prodUserID,
            taskID,
            background,
            resourceType,
            harvester_id,
            worker_id,
            schedulerID,
            jobType,
            is_gu,
            via_topic,
        )
        t_after = time.time()
        t_total = t_after - t_before
        _logger.debug(f"getJobs : took {t_total}s for {siteName} nJobs={nJobs} prodSourceLabel={prodSourceLabel}")
        # release proxy
        self.proxyPool.putProxy(proxy)

        # get secret
        secrets_map = {}
        for job in jobs:
            if job.use_secrets() and job.prodUserName not in secrets_map:
                # get secret
                proxy = self.proxyPool.getProxy()
                tmp_status, secret = proxy.get_user_secrets(job.prodUserName)
                if not tmp_status:
                    secret = None
                self.proxyPool.putProxy(proxy)
                secrets_map[job.prodUserName] = secret
            if job.is_debug_mode():
                if panda_config.pilot_secrets not in secrets_map:
                    # get secret
                    proxy = self.proxyPool.getProxy()
                    tmp_status, secret = proxy.get_user_secrets(panda_config.pilot_secrets)
                    if not tmp_status:
                        secret = None
                    self.proxyPool.putProxy(proxy)
                    secrets_map[panda_config.pilot_secrets] = secret

        return jobs + [nSent, {}, secrets_map]

    # get job status
    def getJobStatus(
        self,
        jobIDs,
        fromDefined=True,
        fromActive=True,
        fromArchived=True,
        fromWaiting=True,
    ):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retStatus = []
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID, fromDefined, fromActive, fromArchived, fromWaiting)
            if res:
                retStatus.append(res.jobStatus)
            else:
                retStatus.append(None)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retStatus

    # peek at jobs
    def peekJobs(
        self,
        jobIDs,
        fromDefined=True,
        fromActive=True,
        fromArchived=True,
        fromWaiting=True,
        forAnal=False,
        use_json=False,
    ):
        # get proxy
        proxy = self.proxyPool.getProxy()
        retJobs = []
        # peek at job
        for jobID in jobIDs:
            res = proxy.peekJob(jobID, fromDefined, fromActive, fromArchived, fromWaiting, forAnal)
            if res:
                if use_json:
                    retJobs.append(res.to_dict())
                else:
                    retJobs.append(res)
            else:
                retJobs.append(None)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retJobs

    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        retJobs = proxy.getPandaIDsWithTaskID(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retJobs

    # get full job status
    def getFullJobStatus(
        self,
        jobIDs,
        fromDefined=True,
        fromActive=True,
        fromArchived=True,
        fromWaiting=True,
        forAnal=True,
        days=30,
    ):
        retJobMap = {}
        # peek at job
        for jobID in jobIDs:
            # get DBproxy for each job to avoid occupying connection for long time
            proxy = self.proxyPool.getProxy()
            # peek job
            res = proxy.peekJob(jobID, fromDefined, fromActive, fromArchived, fromWaiting, forAnal)
            retJobMap[jobID] = res
            # release proxy
            self.proxyPool.putProxy(proxy)
        # get IDs
        for jobID in jobIDs:
            if retJobMap[jobID] is None:
                # get ArchiveDBproxy
                proxy = self.proxyPool.getProxy()
                # peek job
                res = proxy.peekJobLog(jobID, days)
                retJobMap[jobID] = res
                # release proxy
                self.proxyPool.putProxy(proxy)
        # sort
        retJobs = []
        for jobID in jobIDs:
            retJobs.append(retJobMap[jobID])

        return retJobs

    # get script for offline running
    def getScriptOfflineRunning(self, pandaID, days=None):
        try:
            # get job
            tmpJobs = self.getFullJobStatus([pandaID], days=days)
            if tmpJobs == [] or tmpJobs[0] is None:
                errStr = f"ERROR: Cannot get PandaID={pandaID} in DB "
                if days is None:
                    errStr += "for the last 30 days. You may add &days=N to the URL"
                else:
                    errStr += f"for the last {days} days. You may change &days=N in the URL"
                return errStr
            tmpJob = tmpJobs[0]
            # user job
            isUser = False
            for trf in [
                "runAthena",
                "runGen",
                "runcontainer",
                "runMerge",
                "buildJob",
                "buildGen",
            ]:
                if trf in tmpJob.transformation:
                    isUser = True
                    break
            # check prodSourceLabel
            if tmpJob.prodSourceLabel == "user":
                isUser = True
            if isUser:
                tmpAtls = [tmpJob.AtlasRelease]
                tmpRels = [re.sub("^AnalysisTransforms-*", "", tmpJob.homepackage)]
                tmpPars = [tmpJob.jobParameters]
                tmpTrfs = [tmpJob.transformation]
            else:
                # release and trf
                tmpAtls = tmpJob.AtlasRelease.split("\n")
                tmpRels = tmpJob.homepackage.split("\n")
                tmpPars = tmpJob.jobParameters.split("\n")
                tmpTrfs = tmpJob.transformation.split("\n")
            if not (len(tmpRels) == len(tmpPars) == len(tmpTrfs)):
                return "ERROR: The number of releases or parameters or trfs is inconsistent with others"
            # construct script
            scrStr = "#retrieve inputs\n\n"
            # collect inputs
            dsFileMap = {}
            for tmpFile in tmpJob.Files:
                if tmpFile.type == "input":
                    if tmpFile.dataset not in dsFileMap:
                        dsFileMap[tmpFile.dataset] = []
                    if tmpFile.lfn not in dsFileMap[tmpFile.dataset]:
                        dsFileMap[tmpFile.dataset].append(tmpFile.scope + ":" + tmpFile.lfn)
            # get
            for tmpDS in dsFileMap:
                tmpFileList = dsFileMap[tmpDS]
                for tmpLFN in tmpFileList:
                    scrStr += f"rucio download {tmpLFN} --no-subdir\n"
            if isUser:
                scrStr += "\n#get trf\n"
                scrStr += f"wget {tmpTrfs[0]}\n"
                scrStr += f"chmod +x {tmpTrfs[0].split('/')[-1]}\n"
            scrStr += "\n#transform commands\n\n"
            for tmpIdx, tmpRel in enumerate(tmpRels):
                # asetup
                atlRel = re.sub("Atlas-", "", tmpAtls[tmpIdx])
                atlTags = re.split("/|_", tmpRel)
                if "" in atlTags:
                    atlTags.remove("")
                if atlRel != "" and atlRel not in atlTags and (re.search("^\d+\.\d+\.\d+$", atlRel) is None or isUser):
                    atlTags.append(atlRel)
                try:
                    cmtConfig = [s for s in tmpJob.cmtConfig.split("@") if s][-1]
                except Exception:
                    cmtConfig = ""
                scrStr += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh -c %s\n" % cmtConfig
                scrStr += f"asetup --platform={tmpJob.cmtConfig.split('@')[0]} {','.join(atlTags)}\n"
                # athenaMP
                if tmpJob.coreCount not in ["NULL", None] and tmpJob.coreCount > 1:
                    scrStr += f"export ATHENA_PROC_NUMBER={tmpJob.coreCount}\n"
                    scrStr += f"export ATHENA_CORE_NUMBER={tmpJob.coreCount}\n"
                # add double quotes for zsh
                tmpParamStr = tmpPars[tmpIdx]
                tmpSplitter = shlex.shlex(tmpParamStr, posix=True)
                tmpSplitter.whitespace = " "
                tmpSplitter.whitespace_split = True
                # loop for params
                for tmpItem in tmpSplitter:
                    tmpMatch = re.search("^(-[^=]+=)(.+)$", tmpItem)
                    if tmpMatch is not None:
                        tmpArgName = tmpMatch.group(1)
                        tmpArgVal = tmpMatch.group(2)
                        tmpArgIdx = tmpParamStr.find(tmpArgName) + len(tmpArgName)
                        # add "
                        if tmpParamStr[tmpArgIdx] != '"':
                            tmpParamStr = tmpParamStr.replace(tmpMatch.group(0), tmpArgName + '"' + tmpArgVal + '"')
                # run trf
                if isUser:
                    scrStr += "./"
                scrStr += f"{tmpTrfs[tmpIdx].split('/')[-1]} {tmpParamStr}\n\n"
            return scrStr
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            _logger.error(f"getScriptOfflineRunning : {errType} {errValue}")
            return f"ERROR: ServerError in getScriptOfflineRunning with {errType} {errValue}"

    # kill jobs
    def killJobs(self, ids, user, code, prodManager, wgProdRole=[], killOptions=[]):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        rets = []
        # kill jobs
        pandaIDforCloserMap = {}
        for id in ids:
            # retry event service merge
            toKill = True
            if "keepUnmerged" in killOptions:
                tmpJobSpec = proxy.peekJob(id, True, True, False, False, False)
                if tmpJobSpec is not None:
                    if EventServiceUtils.isEventServiceMerge(tmpJobSpec):
                        # retry ES merge jobs not to discard events
                        proxy.retryJob(
                            id,
                            {},
                            getNewPandaID=True,
                            attemptNr=tmpJobSpec.attemptNr,
                            recoverableEsMerge=True,
                        )
                    elif EventServiceUtils.isEventServiceJob(tmpJobSpec):
                        # get number of started events
                        nEvt = proxy.getNumStartedEvents(tmpJobSpec)
                        # not to kill jobset if there are started events
                        if nEvt is not None and nEvt > 0:
                            # set sub status if any
                            for killOpt in killOptions:
                                if killOpt.startswith("jobSubStatus"):
                                    tmpJobSpec.jobSubStatus = killOpt.split("=")[-1]
                                    break
                            # trigger ppE for ES jobs to properly trigger subsequent procedures
                            ret = proxy.archiveJob(
                                tmpJobSpec,
                                tmpJobSpec.jobStatus in ["defined", "assigned"],
                            )
                            toKill = False
                            userInfo = {"prodSourceLabel": None}
            if toKill:
                ret, userInfo = proxy.killJob(id, user, code, prodManager, True, wgProdRole, killOptions)
            rets.append(ret)
            if ret and userInfo["prodSourceLabel"] in ["user", "managed", "test"]:
                jobIDKey = (
                    userInfo["prodUserID"],
                    userInfo["jobDefinitionID"],
                    userInfo["jobsetID"],
                )
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
                            if tmpFile.type in ["output", "log"]:
                                if tmpFile.destinationDBlock not in tmpDestDBlocks:
                                    tmpDestDBlocks.append(tmpFile.destinationDBlock)
                        # run
                        cThr = Closer(self, tmpDestDBlocks, tmpJob)
                        cThr.start()
                        cThr.join()
        except Exception:
            pass

        return rets

    # reassign jobs
    def reassignJobs(
        self,
        ids,
        attempt=0,
        joinThr=False,
        forPending=False,
        firstSubmission=True,
    ):
        tmpLog = LogWrapper(_logger, "reassignJobs")
        tmpLog.debug(f"start for {len(ids)} IDs")
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
                    tmpRet = proxy.resetJob(id, keepSite=keepSiteFlag, getOldSubs=True)
                    if isinstance(tmpRet, tuple):
                        ret, tmpOldSubList = tmpRet
                    else:
                        ret, tmpOldSubList = tmpRet, []
                    if ret is not None:
                        jobs.append(ret)
                        for tmpOldSub in tmpOldSubList:
                            oldSubMap.setdefault(tmpOldSub, ret)
                        continue
                # try to reset waiting job
                tmpRet = proxy.resetJob(
                    id,
                    False,
                    keepSite=keepSiteFlag,
                    getOldSubs=False,
                    forPending=forPending,
                )
                if isinstance(tmpRet, tuple):
                    ret, tmpOldSubList = tmpRet
                else:
                    ret, tmpOldSubList = tmpRet, []
                if ret is not None:
                    jobs.append(ret)
                    # waiting jobs don't create sub or dis
                    continue
                # try to reset defined job
                if not forPending:
                    tmpRet = proxy.resetDefinedJob(id, keepSite=keepSiteFlag, getOldSubs=True)
                    if isinstance(tmpRet, tuple):
                        ret, tmpOldSubList = tmpRet
                    else:
                        ret, tmpOldSubList = tmpRet, []
                    if ret is not None:
                        jobs.append(ret)
                        for tmpOldSub in tmpOldSubList:
                            oldSubMap.setdefault(tmpOldSub, ret)
                        continue
            except Exception as e:
                tmpLog.error(f"failed with {str(e)} {traceback.format_exc()}")
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # run Closer for old sub datasets
        if not forPending:
            for tmpOldSub in oldSubMap:
                tmpJob = oldSubMap[tmpOldSub]
                cThr = Closer(self, [tmpOldSub], tmpJob)
                cThr.start()
                cThr.join()
        tmpLog.debug(f"got {len(jobs)} IDs")
        # setup dataset
        if jobs != []:
            if joinThr:
                thr = Setupper(
                    self,
                    jobs,
                    resubmit=True,
                    first_submission=firstSubmission,
                )
                thr.start()
                thr.join()
            else:
                # cannot use 'thr =' because it may trigger garbage collector
                Setupper(
                    self,
                    jobs,
                    resubmit=True,
                    first_submission=firstSubmission,
                ).start()
        tmpLog.debug("done")

        return True

    # awake jobs in jobsWaiting
    def awakeJobs(self, ids):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        jobs = []
        # reset jobs
        for id in ids:
            # try to reset waiting job
            ret = proxy.resetJob(id, False)
            if ret is not None:
                jobs.append(ret)
        # release DB proxy
        self.proxyPool.putProxy(proxy)
        # setup dataset
        Setupper(self, jobs).start()

        return True

    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self, dataset, status, fileLFN=""):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query PandaID
        retList = proxy.updateInFilesReturnPandaIDs(dataset, status, fileLFN)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retList

    # update output files and return corresponding PandaIDs
    def updateOutFilesReturnPandaIDs(self, dataset, fileLFN=""):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query PandaID
        retList = proxy.updateOutFilesReturnPandaIDs(dataset, fileLFN)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retList

    # get _dis datasets associated to _sub
    def getAssociatedDisDatasets(self, subDsName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # query
        retList = proxy.getAssociatedDisDatasets(subDsName)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retList

    # insert sandbox file info
    def insertSandboxFileInfo(self, userName, hostName, fileName, fileSize, checkSum):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.insertSandboxFileInfo(userName, hostName, fileName, fileSize, checkSum)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get and lock sandbox files
    def getLockSandboxFiles(self, time_limit, n_files):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getLockSandboxFiles(time_limit, n_files)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # check duplicated sandbox file
    def checkSandboxFile(self, userName, fileSize, checkSum):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.checkSandboxFile(userName, fileSize, checkSum)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # insert datasets
    def insertDatasets(self, datasets):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        retList = []
        # insert
        for dataset in datasets:
            ret = proxy.insertDataset(dataset)
            retList.append(ret)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retList

    # get and lock dataset with a query
    def getLockDatasets(self, sqlQuery, varMapGet, modTimeOffset="", getVersion=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.getLockDatasets(sqlQuery, varMapGet, modTimeOffset, getVersion)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # query Dataset
    def queryDatasetWithMap(self, map):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query Dataset
        ret = proxy.queryDatasetWithMap(map)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # set GUIDs
    def setGUIDs(self, files):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # set GUIDs
        ret = proxy.setGUIDs(files)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # update dataset
    def updateDatasets(self, datasets, withLock=False, withCriteria="", criteriaMap={}):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # update Dataset
        retList = proxy.updateDataset(datasets, withLock, withCriteria, criteriaMap)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retList

    # trigger cleanup of internal datasets used by a task
    def trigger_cleanup_internal_datasets(self, task_id: int) -> bool:
        proxy = self.proxyPool.getProxy()
        ret = proxy.trigger_cleanup_internal_datasets(task_id)
        self.proxyPool.putProxy(proxy)
        return ret

    # count the number of files with map
    def countFilesWithMap(self, map):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # query files
        ret = proxy.countFilesWithMap(map)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get serial number for dataset
    def getSerialNumber(self, datasetname, definedFreshFlag=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getSerialNumber(datasetname, definedFreshFlag)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # add metadata
    def addMetadata(self, ids, metadataList, newStatusList):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # add metadata
        index = 0
        retList = []
        for id in ids:
            ret = proxy.addMetadata(id, metadataList[index], newStatusList[index])
            retList.append(ret)
            index += 1
        # release proxy
        self.proxyPool.putProxy(proxy)

        return retList

    # add stdout
    def addStdOut(self, id, stdout):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # add
        ret = proxy.addStdOut(id, stdout)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # extract name from DN
    def cleanUserID(self, id):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.cleanUserID(id)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # extract scope from dataset name
    def extractScope(self, name):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.extractScope(name)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get job statistics
    def getJobStatistics(
        self,
        archived=False,
        predefined=False,
        workingGroup="",
        countryGroup="",
        jobType="",
        forAnal=None,
        minPriority=None,
    ):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatistics(
            archived,
            predefined,
            workingGroup,
            countryGroup,
            jobType,
            forAnal,
            minPriority,
        )
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    def getJobStatisticsForExtIF(self, sourcetype=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatisticsForExtIF(sourcetype)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self, useMorePG=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getJobStatisticsPerProcessingType(useMorePG)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # update site data
    def updateSiteData(self, hostID, pilotRequests, interval=3):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.updateSiteData(hostID, pilotRequests, interval)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get current site data
    def getCurrentSiteData(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.getCurrentSiteData()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # insert nRunning in site data
    def insertnRunningInSiteData(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get serial number
        ret = proxy.insertnRunningInSiteData()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get site info
    def getSiteInfo(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get site info
        ret = proxy.getSiteInfo()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get cloud list
    def get_cloud_list(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get cloud list
        ret = proxy.get_cloud_list()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get special dispatcher parameters
    def get_special_dispatch_params(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.get_special_dispatch_params()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get email address
    def getEmailAddr(self, name, withDN=False, withUpTime=False):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getEmailAddr(name, withDN, withUpTime)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # set email address for a user
    def setEmailAddr(self, userName, emailAddr):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # set
        ret = proxy.setEmailAddr(userName, emailAddr)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get banned users
    def get_ban_users(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.get_ban_users()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get client version
    def getPandaClientVer(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.getPandaClientVer()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # register a token key
    def register_token_key(self, client_name, lifetime):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # register proxy key
        ret = proxy.register_token_key(client_name, lifetime)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # query an SQL return Status
    def querySQLS(self, sql, varMap, arraySize=1000):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.querySQLS(sql, varMap, arraySize)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # query an SQL
    def querySQL(self, sql, varMap, arraySize=1000):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.querySQLS(sql, varMap, arraySize)[1]
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # execute an SQL return with executemany
    def executemanySQL(self, sql, varMaps, arraySize=1000):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.executemanySQL(sql, varMaps, arraySize)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # check quota
    def checkQuota(self, dn):
        # query an SQL return Status
        proxy = self.proxyPool.getProxy()
        # get
        ret = proxy.checkQuota(dn)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # insert TaskParams
    def insertTaskParamsPanda(
        self,
        taskParams,
        user,
        prodRole,
        fqans=[],
        parent_tid=None,
        properErrorCode=False,
        allowActiveTask=False,
    ):
        # query an SQL return Status
        proxy = self.proxyPool.getProxy()
        # check user status
        tmpStatus = proxy.checkBanUser(user, None, True)
        if tmpStatus is True:
            # exec
            ret = proxy.insertTaskParamsPanda(
                taskParams,
                user,
                prodRole,
                fqans,
                parent_tid,
                properErrorCode,
                allowActiveTask,
            )
        elif tmpStatus == 1:
            ret = False, "Failed to update DN in PandaDB"
        elif tmpStatus == 2:
            ret = False, "Failed to insert user info to PandaDB"
        else:
            ret = False, f"The following DN is banned: DN={user}"
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # send command to task
    def sendCommandTaskPanda(
        self,
        jediTaskID,
        dn,
        prodRole,
        comStr,
        comComment=None,
        useCommit=True,
        properErrorCode=False,
        comQualifier=None,
        broadcast=False,
    ):
        # query an SQL return Status
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.sendCommandTaskPanda(
            jediTaskID,
            dn,
            prodRole,
            comStr,
            comComment,
            useCommit,
            properErrorCode,
            comQualifier,
            broadcast,
        )
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # update unmerged datasets to trigger merging
    def updateUnmergedDatasets(self, job, finalStatusDS, updateCompleted=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateUnmergedDatasets(job, finalStatusDS, updateCompleted)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get active JediTasks in a time range
    def getJediTasksInTimeRange(self, dn, timeRangeStr, fullFlag=False, minTaskID=None, task_type="user"):
        # check DN
        if dn in ["NULL", "", "None", None]:
            return {}
        # check timeRange
        match = re.match("^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$", timeRangeStr)
        if match is None:
            return {}
        timeRange = datetime.datetime(
            year=int(match.group(1)),
            month=int(match.group(2)),
            day=int(match.group(3)),
            hour=int(match.group(4)),
            minute=int(match.group(5)),
            second=int(match.group(6)),
        )
        # max range is 3 months
        maxRange = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(days=30)
        if timeRange < maxRange:
            timeRange = maxRange
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJediTasksInTimeRange(dn, timeRange, fullFlag, minTaskID, task_type)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get details of JediTask
    def getJediTaskDetails(self, jediTaskID, fullFlag, withTaskInfo):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJediTaskDetails(jediTaskID, fullFlag, withTaskInfo)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get a list of even ranges for a PandaID
    def getEventRanges(self, pandaID, jobsetID, jediTaskID, nRanges, acceptJson, scattered, segment_id):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getEventRanges(pandaID, jobsetID, jediTaskID, nRanges, acceptJson, scattered, segment_id)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # update an even range
    def updateEventRange(self, eventRangeID, eventStatus, cpuCore, cpuConsumptionTime, objstoreID=None):
        eventDict = {}
        eventDict["eventRangeID"] = eventRangeID
        eventDict["eventStatus"] = eventStatus
        eventDict["cpuCore"] = cpuCore
        eventDict["cpuConsumptionTime"] = cpuConsumptionTime
        eventDict["objstoreID"] = objstoreID
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

        return retVal, json.dumps(ret[1])

    # update even ranges
    def updateEventRanges(self, eventRanges, version=0):
        # decode json
        try:
            eventRanges = json.loads(eventRanges)
        except Exception:
            return json.dumps("ERROR : failed to convert eventRanges with json")
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateEventRanges(eventRanges, version)
        # release proxy
        self.proxyPool.putProxy(proxy)

        if version != 0:
            return ret
        return json.dumps(ret[0]), json.dumps(ret[1])

    # change task priority
    def changeTaskPriorityPanda(self, jediTaskID, newPriority):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.changeTaskPriorityPanda(jediTaskID, newPriority)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # throttle user jobs
    def throttleUserJobs(self, prodUserName, workingGroup, get_dict=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.throttleUserJobs(prodUserName, workingGroup, get_dict)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # unthrottle user jobs
    def unThrottleUserJobs(self, prodUserName, workingGroup, get_dict=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.unThrottleUserJobs(prodUserName, workingGroup, get_dict)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get throttled users
    def getThrottledUsers(self):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getThrottledUsers()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get the list of jobdefIDs for failed jobs in a task
    def getJobdefIDsForFailedJob(self, jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJobdefIDsForFailedJob(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # change task attribute
    def changeTaskAttributePanda(self, jediTaskID, attrName, attrValue):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.changeTaskAttributePanda(jediTaskID, attrName, attrValue)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # change split rule for task
    def changeTaskSplitRulePanda(self, jediTaskID, attrName, attrValue):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.changeTaskSplitRulePanda(jediTaskID, attrName, attrValue)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self, jediTaskID, increasedNr):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseAttemptNrPanda(jediTaskID, increasedNr)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get jediTaskID from taskName
    def getTaskIDwithTaskNameJEDI(self, userName, taskName):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getTaskIDwithTaskNameJEDI(userName, taskName)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # update error dialog for a jediTaskID
    def updateTaskErrorDialogJEDI(self, jediTaskID, msg):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateTaskErrorDialogJEDI(jediTaskID, msg)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # update modificationtime for a jediTaskID to trigger subsequent process
    def updateTaskModTimeJEDI(self, jediTaskID, newStatus=None):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.updateTaskModTimeJEDI(jediTaskID, newStatus)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # check input file status
    def checkInputFileStatusInJEDI(self, jobSpec):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.checkInputFileStatusInJEDI(jobSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # increase memory limit
    def increaseRamLimitJEDI(self, jediTaskID, jobRamCount):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseRamLimitJEDI(jediTaskID, jobRamCount)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # increase memory limit
    def increaseRamLimitJobJEDI(self, job, jobRamCount, jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseRamLimitJobJEDI(job, jobRamCount, jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # increase memory limit xtimes
    def increaseRamLimitJobJEDI_xtimes(self, job, jobRamCount, jediTaskID, attemptNr):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseRamLimitJobJEDI_xtimes(job, jobRamCount, jediTaskID, attemptNr)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # reduce input per job
    def reduce_input_per_job(self, panda_id, jedi_task_id, attempt_nr, excluded_rules, steps, dry_mode=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.reduce_input_per_job(panda_id, jedi_task_id, attempt_nr, excluded_rules, steps, dry_mode)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # reset files in JEDI
    def resetFileStatusInJEDI(self, dn, prodManager, datasetName, lostFiles, recoverParent, simul=False):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.resetFileStatusInJEDI(dn, prodManager, datasetName, lostFiles, recoverParent, simul)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # copy file records
    def copy_file_records(self, new_lfns, file_spec):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.copy_file_records(new_lfns, file_spec)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # retry module: get the defined rules
    def getRetrialRules(self):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getRetrialRules()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # retry module action: set max number of retries
    def setMaxAttempt(self, jobID, jediTaskID, files, attemptNr):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.setMaxAttempt(jobID, jediTaskID, files, attemptNr)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # retry module action: set maxAttempt to the current attemptNr to avoid further retries
    def setNoRetry(self, jobID, jediTaskID, files):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.setNoRetry(jobID, jediTaskID, files)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # retry module action: increase CPU Time
    def increaseCpuTimeTask(self, jobID, taskID, siteid, files, active):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.increaseCpuTimeTask(jobID, taskID, siteid, files, active)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # retry module action: recalculate the Task Parameters
    def requestTaskParameterRecalculation(self, taskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.requestTaskParameterRecalculation(taskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # add associate sub datasets for single consumer job
    def getDestDBlocksWithSingleConsumer(self, jediTaskID, PandaID, ngDatasets):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getDestDBlocksWithSingleConsumer(jediTaskID, PandaID, ngDatasets)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # check validity of merge job
    def isValidMergeJob(self, pandaID, jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.isValidMergeJob(pandaID, jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # Configurator: insert network matrix data
    def insertNetworkMatrixData(self, data):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.insertNetworkMatrixData(data)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # Configurator: delete old network matrix data
    def deleteOldNetworkData(self):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.deleteOldNetworkData()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get dispatch datasets per user
    def getDispatchDatasetsPerUser(self, vo, prodSourceLabel, onlyActive, withSize):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getDispatchDatasetsPerUser(vo, prodSourceLabel, onlyActive, withSize)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get task parameters
    def getTaskParamsPanda(self, jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getTaskParamsPanda(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get task attributes
    def getTaskAttributesPanda(self, jediTaskID, attrs):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getTaskAttributesPanda(jediTaskID, attrs)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # check for cloned jobs
    def checkClonedJob(self, jobSpec):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.checkClonedJob(jobSpec)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get co-jumbo jobs to be finished
    def getCoJumboJobsToBeFinished(self, timeLimit, minPriority, maxJobs):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getCoJumboJobsToBeFinished(timeLimit, minPriority, maxJobs)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get number of events to be processed
    def getNumReadyEvents(self, jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getNumReadyEvents(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # check if task is applicable for jumbo jobs
    def isApplicableTaskForJumbo(self, jediTaskID):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.isApplicableTaskForJumbo(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # cleanup jumbo jobs
    def cleanupJumboJobs(self, jediTaskID=None):
        # get proxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.cleanupJumboJobs(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # convert ObjID to endpoint
    def convertObjIDtoEndPoint(self, srcFileName, ObjID):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.convertObjIDtoEndPoint(srcFileName, ObjID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # get task status
    def getTaskStatus(self, jediTaskID):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getTaskStatus(jediTaskID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # reactivate task
    def reactivateTask(self, jediTaskID, keep_attempt_nr=False, trigger_job_generation=False):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reactivateTask(jediTaskID, keep_attempt_nr, trigger_job_generation)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # get event statistics
    def getEventStat(self, jediTaskID, PandaID):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getEventStat(jediTaskID, PandaID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # get nested dict of gshare names implying the tree structure
    def get_tree_of_gshare_names(self):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.get_tree_of_gshare_names()
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # get the HS06 distribution for global shares
    def get_hs_distribution(self):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.get_hs_distribution()
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # reassign share
    def reassignShare(self, jedi_task_ids, share_dest, reassign_running):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reassignShare(jedi_task_ids, share_dest, reassign_running)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

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

        return res

    def getTaskParamsMap(self, jediTaskID):
        """
        Return the taskParamsMap
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getTaskParamsPanda(jediTaskID)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

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

        return res

    # send command to harvester or lock command
    def commandToHarvester(
        self,
        harvester_ID,
        command,
        ack_requested,
        status,
        lockInterval=None,
        comInterval=None,
        params=None,
    ):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.commandToHarvester(
            harvester_ID,
            command,
            ack_requested,
            status,
            lockInterval,
            comInterval,
            params,
        )
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    def getResourceTypes(self):
        """
        Get resource types (SCORE, MCORE, ...) and their definitions
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.load_resource_types(formatting="dict")
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # report stat of workers
    def reportWorkerStats_jobtype(self, harvesterID, siteName, paramsList):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.reportWorkerStats_jobtype(harvesterID, siteName, paramsList)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # get command locks
    def getCommandLocksHarvester(self, harvester_ID, command, lockedBy, lockInterval, commandInterval):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.getCommandLocksHarvester(harvester_ID, command, lockedBy, lockInterval, commandInterval)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

        return res

    # release command lock
    def releaseCommandLockHarvester(self, harvester_ID, command, computingSite, resourceType, lockedBy):
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.releaseCommandLockHarvester(harvester_ID, command, computingSite, resourceType, lockedBy)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

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

        return res

    # heartbeat for harvester
    def harvesterIsAlive(self, user, host, harvesterID, data):
        """
        update harvester instance information
        """
        # get DB proxy
        proxy = self.proxyPool.getProxy()
        # exec
        res = proxy.harvesterIsAlive(user, host, harvesterID, data)
        # release DB proxy
        self.proxyPool.putProxy(proxy)

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

        return res

    # read the resource types from the DB
    def load_resource_types(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.load_resource_types()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret_val

    # get the resource of a task
    def get_resource_type_task(self, task_spec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.get_resource_type_task(task_spec)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret_val

    def reset_resource_type_task(self, jedi_task_id, use_commit=True):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.reset_resource_type_task(jedi_task_id, use_commit)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret_val

    # get the resource of a task
    def get_resource_type_job(self, job_spec):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret_val = proxy.get_resource_type_job(job_spec)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret_val

    # check Job status
    def checkJobStatus(self, pandaIDs):
        try:
            pandaIDs = pandaIDs.split(",")
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

        return retList

    # get stat of workers
    def getWorkerStats(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getWorkerStats()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get unified pilot streaming queues
    def ups_get_queues(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.ups_get_queues()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # load harvester worker stats
    def ups_load_worker_stats(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.ups_load_worker_stats()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get the distribution of new workers to submit
    def get_average_memory_workers(self, queue, harvester_id, target):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.get_average_memory_workers(queue, harvester_id, target)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get the distribution of new workers to submit
    def ups_new_worker_distribution(self, queue, worker_stats):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.ups_new_worker_distribution(queue, worker_stats)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # check event availability
    def checkEventsAvailability(self, pandaID, jobsetID, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.checkEventsAvailability(pandaID, jobsetID, jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get LNFs for jumbo job
    def getLFNsForJumbo(self, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getLFNsForJumbo(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get active job attribute
    def getActiveJobAttributes(self, pandaID, attrs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getActiveJobAttributes(pandaID, attrs)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get original consumers
    def getOriginalConsumers(self, jediTaskID, jobsetID, pandaID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getOriginalConsumers(jediTaskID, jobsetID, pandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # add harvester dialog messages
    def addHarvesterDialogs(self, harvesterID, dialogs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.addHarvesterDialogs(harvesterID, dialogs)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get job statistics per site and resource
    def getJobStatisticsPerSiteResource(self, timeWindow=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJobStatisticsPerSiteResource(timeWindow)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get job statistics per site, source label, and resource type
    def get_job_statistics_per_site_label_resource(self, time_window=None):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.get_job_statistics_per_site_label_resource(time_window)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # set num slots for workload provisioning
    def setNumSlotsForWP(self, pandaQueueName, numSlots, gshare, resourceType, validPeriod):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # enable jumbo jobs
    def enableJumboJobs(self, jediTaskID, totalJumboJobs, nJumboPerSite):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.enableJumboJobs(jediTaskID, totalJumboJobs, nJumboPerSite)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # enable event service
    def enableEventService(self, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.enableEventService(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get JEDI file attributes
    def getJediFileAttributes(self, PandaID, jediTaskID, datasetID, fileID, attrs):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJediFileAttributes(PandaID, jediTaskID, datasetID, fileID, attrs)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # check if super user
    def isSuperUser(self, userName):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.isSuperUser(userName)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get workers for a job
    def getWorkersForJob(self, PandaID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getWorkersForJob(PandaID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get user job metadata
    def getUserJobMetadata(self, jediTaskID):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getUserJobMetadata(jediTaskID)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get jumbo job datasets
    def getJumboJobDatasets(self, n_days, grace_period):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getJumboJobDatasets(n_days, grace_period)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get global shares status
    def getGShareStatus(self):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getGShareStatus()
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # get output datasets
    def getOutputDatasetsJEDI(self, panda_id):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.getOutputDatasetsJEDI(panda_id)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # update/insert JSON queue information into the scheconfig replica
    def upsertQueuesInJSONSchedconfig(self, schedconfig_dump):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.upsertQueuesInJSONSchedconfig(schedconfig_dump)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # update/insert SW tag information
    def loadSWTags(self, sw_tags):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.loadSWTags(sw_tags)
        # release proxy
        self.proxyPool.putProxy(proxy)

        return ret

    # generate a harvester command to clean up the workers of a site
    def sweepPQ(self, panda_queue_des, status_list_des, ce_list_des, submission_host_list_des):
        # get DBproxy
        proxy = self.proxyPool.getProxy()
        # exec
        ret = proxy.sweepPQ(panda_queue_des, status_list_des, ce_list_des, submission_host_list_des)
        # release proxy
        self.proxyPool.putProxy(proxy)

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
    def insertJobOutputReport(self, panda_id, prod_source_label, job_status, attempt_nr, data):
        proxy = self.proxyPool.getProxy()
        ret = proxy.insertJobOutputReport(panda_id, prod_source_label, job_status, attempt_nr, data)
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
    def listJobOutputReport(
        self,
        only_unlocked=False,
        time_limit=5,
        limit=999999,
        grace_period=3,
        labels=None,
        anti_labels=None,
    ):
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

    def carbon_write_region_emissions(self, emissions):
        proxy = self.proxyPool.getProxy()
        ret = proxy.carbon_write_region_emissions(emissions)
        self.proxyPool.putProxy(proxy)
        return ret

    def carbon_aggregate_emissions(self):
        proxy = self.proxyPool.getProxy()
        ret = proxy.carbon_aggregate_emissions()
        self.proxyPool.putProxy(proxy)
        return ret

    def get_files_in_datasets(self, task_id, dataset_types):
        proxy = self.proxyPool.getProxy()
        ret = proxy.get_files_in_datasets(task_id, dataset_types)
        self.proxyPool.putProxy(proxy)
        return ret

    def get_max_worker_id(self, harvester_id):
        proxy = self.proxyPool.getProxy()
        ret = proxy.get_max_worker_id(harvester_id)
        self.proxyPool.putProxy(proxy)
        return ret

    def get_events_status(self, ids):
        proxy = self.proxyPool.getProxy()
        ret = proxy.get_events_status(ids)
        self.proxyPool.putProxy(proxy)
        return ret

    def async_update_datasets(self, panda_id):
        proxy = self.proxyPool.getProxy()
        ret = proxy.async_update_datasets(panda_id)
        self.proxyPool.putProxy(proxy)
        return ret


# Singleton
taskBuffer = TaskBuffer()
