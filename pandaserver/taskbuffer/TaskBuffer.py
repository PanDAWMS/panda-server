import datetime
import json
import re
import time
import traceback
from contextlib import contextmanager
from threading import Lock
from typing import Any, Collection, Iterator, Sequence

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.closer import Closer
from pandaserver.dataservice.setupper import Setupper
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import ErrorCode, EventServiceUtils, JobUtils, ProcessGroups
from pandaserver.taskbuffer.DataCarousel import DataCarouselRequestSpec
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from pandaserver.taskbuffer.DBProxyPool import DBProxyPool
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.offline_run_script import generate_offline_run_script
from pandaserver.workflow.workflow_base import WFDataSpec, WFStepSpec, WorkflowSpec

_logger = PandaLogger().getLogger("TaskBuffer")


class TaskBuffer:
    """
    task queue

    """

    # The proxy pool is installed by init() before any query runs. It is declared
    # non-Optional on purpose, for the same reason as BaseModule.conn/cur: typing it
    # as Optional would only push a None check onto each of the ~250 call sites
    # without making any of them safer, since a query issued before init() is a bug
    # either way.
    proxyPool: DBProxyPool

    # constructor
    def __init__(self) -> None:
        self.proxyPool = None  # type: ignore[assignment]
        self.lock = Lock()
        self.nDBConnection: int = None  # type: ignore[assignment]

        # save the requester for monitoring/logging purposes
        self.start_time = time.time()

        # site mapper
        self.site_mapper: SiteMapper | None = None
        # update time for site mapper
        self.last_update_site_mapper = None

    def __repr__(self) -> str:
        return "TaskBuffer"

    # initialize
    def init(self, dbname: str, dbpass: str, nDBConnection: int = 10, useTimeout: bool = False, requester: str | None = None) -> None:
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
    def cleanup(self, requester: str | None = None) -> None:
        if self.proxyPool:
            try:
                pool_duration = time.time() - self.start_time
            except TypeError:
                pool_duration = -1  # duration unknown

            _logger.info(f"destroying DBProxyPool after n_seconds={pool_duration} on behalf of {requester}")
            self.proxyPool.cleanup()

    # transaction as a context manager
    # CANNOT be used with ConBridge or TaskBufferInterface which uses multiprocess.pipe
    @contextmanager
    def transaction(self, name: str | None = None, tmp_log: LogWrapper | None = None) -> Iterator[tuple[Any, LogWrapper]]:
        with self.proxyPool.get() as proxy:
            with proxy.transaction(name, tmp_log) as txn:
                if txn is None:
                    raise RuntimeError(f"Failed to start transaction {name}")
                # yield the transaction
                yield txn

    # get number of database connections
    def get_num_connections(self) -> int:
        return self.nDBConnection

    # get SiteMapper
    def get_site_mapper(self) -> SiteMapper:
        time_now = naive_utcnow()
        # the first test is what the second one already implies -- the two are set together
        # below -- and it is spelled out so that the return type can be the mapper rather than
        # an optional one
        if (
            self.site_mapper is None
            or self.last_update_site_mapper is None
            or datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - self.last_update_site_mapper > datetime.timedelta(minutes=10)
        ):
            self.site_mapper = SiteMapper(self)
            self.last_update_site_mapper = time_now
        return self.site_mapper

    # check production role
    def checkProdRole(self, fqans: list[str]) -> tuple[bool, str | None]:
        for fqan in fqans:
            # check production role
            match = re.search("/([^/]+)/Role=production", fqan)
            if match is not None:
                return True, match.group(1)
        return False, None

    # get priority parameters for user
    def getPrioParameters(
        self, jobs: list[JobSpec], user: str | None, fqans: list[str], userDefinedWG: bool, validWorkingGroup: bool
    ) -> tuple[bool, str | None, int, int, float, bool]:
        priorityOffset = 0
        serNum = 0
        weight = None
        prio_reduction = True

        # get DB proxy
        with self.proxyPool.get() as proxy:
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
                ] or jobs[0].processingType.startswith("gangarobot-"):
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
                        prodUserName = CoreUtils.clean_user_id(user)
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
                    serNum = proxy.getNumberJobsUser(user, workingGroup=userDefinedWG)  # type: ignore[arg-type]
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
        jobs: list[JobSpec],
        user: str | None,
        joinThr: bool = False,
        fqans: list[str] = [],
        hostname: str = "",
        checkSpecialHandling: bool = True,
        toPending: bool = False,
        oldPandaIDs: list[list[int]] | None = None,
        relationType: str | None = None,
        userVO: str = "atlas",
        esJobsetMap: dict[Any, Any] | None = None,
        getEsJobsetMap: bool = False,
        unprocessedMap: dict[Any, Any] | None = None,
        bulk_job_insert: bool = False,
        trust_user: bool = False,
    ) -> Any:
        try:
            tmpLog = LogWrapper(_logger, f"storeJobs <{CoreUtils.clean_user_id(user)} nJobs={len(jobs)}>")
            tmpLog.debug(f"start toPending={toPending}")
            # check quota for priority calculation
            weight = 0.0
            userJobID = -1
            userJobsetID: int | None = -1
            userStatus = True
            priorityOffset = 0
            userCountry = None
            siteMapper = self.get_site_mapper()

            # check ban user
            if not trust_user and len(jobs) > 0:
                # get DB proxy
                with self.proxyPool.get() as proxy:
                    # check user status
                    tmpStatus = proxy.checkBanUser(user, jobs[0].prodSourceLabel)
                # return if DN is blocked
                if not tmpStatus:
                    tmpLog.debug(f"end 1 since DN {user} is blocked")
                    if getEsJobsetMap:
                        return [], None, unprocessedMap
                    return []

            tmpLog.debug("checked ban user")
            # set parameters for user jobs
            if (
                len(jobs) > 0
                and (jobs[0].prodSourceLabel in JobUtils.analy_sources + JobUtils.list_ptest_prod_sources)
                and (not jobs[0].processingType in ["merge", "unmerge"])
            ):
                # get DB proxy
                with self.proxyPool.get() as proxy:
                    # get JobID and status
                    userJobID, userJobsetID, userStatus = proxy.getUserParameter(
                        user,
                        jobs[0].jobDefinitionID,  # type: ignore[arg-type]  # the id is a column, which is declared optional
                        jobs[0].jobsetID,
                    )

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
            tmpLog.debug("set user job parameters")

            # return if DN is blocked
            if not userStatus:
                tmpLog.debug(f"end 2 since {user} DN is blocked")
                if getEsJobsetMap:
                    return [], None, unprocessedMap
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
            tmpLog.debug("got prio parameters")
            # get DB proxy
            with self.proxyPool.get() as proxy:
                tmpLog.debug("got proxy")
                # get total number of files
                totalNumFiles = 0
                for job in jobs:
                    totalNumFiles += len(job.Files)
                # bulk fetch PandaIDs
                new_panda_ids = proxy.bulk_fetch_panda_ids(len(jobs))
                tmpLog.debug("got PandaIDs")
                # bulk fetch fileIDs
                fileIDPool = []
                if totalNumFiles > 0:
                    fileIDPool = sorted(proxy.bulkFetchFileIDsPanda(totalNumFiles))
                # loop over all jobs
                ret = []
                newJobs = []
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
                            esJobsetMap[esIndex] = None
                        # sort files since file order is important for positional event number
                        job.sortFiles()
                    if oldPandaIDs is not None and len(oldPandaIDs) > idxJob:
                        jobOldPandaIDs = oldPandaIDs[idxJob]
                    else:
                        jobOldPandaIDs = None
                    # check events for jumbo jobs
                    isOK = True
                    if EventServiceUtils.isJumboJob(job):
                        hasReadyEvents = proxy.hasReadyEvents(job.jediTaskID)  # type: ignore[arg-type]  # the id is a column, which is declared optional
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
                                {"esIndex": esIndex},
                            ]
                        )
                        special_handling_list.append(origSH)
                        tmp_ret_i = True
                    if tmp_ret_i and origEsJob and not bulk_job_insert:
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
                    tmp_ret, job_ret_list, es_jobset_map = proxy.bulk_insert_new_jobs(
                        jobs[0].jediTaskID,  # type: ignore[arg-type]  # the id is a column, which is declared optional
                        params_for_bulk_insert,
                        new_jobset_ids,
                        special_handling_list,
                    )
                    if not tmp_ret:
                        raise RuntimeError("bulk job insert failed")
                    # update esJobsetMap
                    esJobsetMap.update(es_jobset_map)

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
                return ret, esJobsetMap, unprocessedMap
            return ret
        except Exception as e:
            tmpLog.error(f"{str(e)} {traceback.format_exc()}")
            errStr = "ERROR: ServerError with storeJobs"
            if getEsJobsetMap:
                return errStr, None, unprocessedMap
            return errStr

    # lock jobs for reassign
    def lockJobsForReassign(
        self,
        tableName: str,
        timeLimit: datetime.datetime,
        statList: list[str],
        labels: list[str],
        processTypes: list[str],
        sites: list[str],
        clouds: list[str],
        useJEDI: bool = False,
        onlyReassignable: bool = False,
        useStateChangeTime: bool = False,
        getEventService: bool = False,
    ) -> tuple[bool, list[Any]]:
        with self.proxyPool.get() as proxy:
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
        return res

    # get a DB configuration value
    def getConfigValue(self, component: str, key: str, app: str = "pandaserver", vo: str | None = None, default: Any = None) -> Any:
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getConfigValue(component, key, app, vo)
            if res is None and default is not None:
                res = default
        return res

    # lock jobs for finisher
    def lockJobsForFinisher(self, timeNow: datetime.datetime, rownum: int, highPrio: bool) -> tuple[bool, list[Any]]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.lockJobsForFinisher(timeNow, rownum, highPrio)
        return res

    # lock jobs for activator
    def lockJobsForActivator(self, timeLimit: datetime.datetime, rownum: int, prio: int) -> tuple[bool, list[Any]]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.lockJobsForActivator(timeLimit, rownum, prio)
        return res

    # update overall job information
    def updateJobs(
        self,
        jobs: list[JobSpec],
        inJobsDefined: bool,
        oldJobStatusList: list[str | None] | None = None,
        extraInfo: dict[str, Any] | None = None,
        async_dataset_update: bool = False,
    ) -> list[bool]:
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # loop over all jobs
            returns = []
            ddmIDs = []
            ddmAttempt = 0
            newMover = None
            for idxJob, job in enumerate(jobs):
                # update DB
                tmpddmIDs: list[Any] = []
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
                        hasDone = proxy.hasDoneEvents(
                            job.jediTaskID,  # type: ignore[arg-type]  # the id is a column, which is declared optional
                            job.PandaID,  # type: ignore[arg-type]  # the id is a column, which is declared optional
                            job,
                        )
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
                        proxy.async_update_datasets(job.PandaID)  # type: ignore[arg-type]  # the id is a column, which is declared optional
                else:
                    ret = proxy.updateJob(job, inJobsDefined, oldJobStatus=oldJobStatus, extraInfo=extraInfo)
                returns.append(ret)
                # collect IDs for reassign
                if ret:
                    ddmIDs += tmpddmIDs
        # retry mover
        if newMover is not None:
            self.storeJobs([newMover], None, joinThr=True)
        # reassign jobs when ddm failed
        if ddmIDs != []:
            self.reassignJobs(ddmIDs, ddmAttempt, joinThr=True)

        return returns

    # update job jobStatus only
    def updateJobStatus(self, jobID: int, jobStatus: str, param: dict[str, Any], updateStateChange: bool = False, attemptNr: int | None = None) -> Any:
        # get DB proxy
        with self.proxyPool.get() as proxy:
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
        return ret

    # update worker status by the pilot
    def updateWorkerPilotStatus(self, workerID: int | str, harvesterID: str, status: str, node_id: str | None) -> bool:
        with self.proxyPool.get() as proxy:
            # update DB and buffer
            ret = proxy.updateWorkerPilotStatus(workerID, harvesterID, status, node_id)
        return ret

    def update_worker_node(
        self,
        site: str,
        panda_queue: str,
        host_name: str,
        cpu_model: str | None,
        cpu_model_normalized: str | None,
        n_logical_cpus: int | None,
        n_sockets: int | None,
        cores_per_socket: int | None,
        threads_per_core: int | None,
        cpu_architecture: str | None,
        cpu_architecture_level: str | None,
        clock_speed: float | None,
        total_memory: int | None,
        total_local_disk: int | None,
    ) -> tuple[bool, str]:
        with self.proxyPool.get() as proxy:
            # update DB and buffer
            ret = proxy.update_worker_node(
                site,
                panda_queue,
                host_name,
                cpu_model,
                cpu_model_normalized,
                n_logical_cpus,
                n_sockets,
                cores_per_socket,
                threads_per_core,
                cpu_architecture,
                cpu_architecture_level,
                clock_speed,
                total_memory,
                total_local_disk,
            )
        return ret

    def update_worker_node_gpu(
        self,
        site: str,
        host_name: str,
        vendor: str,
        model: str,
        count: int,
        vram: int,
        architecture: str,
        framework: str,
        framework_version: str,
        driver_version: str,
    ) -> tuple[bool, str]:
        with self.proxyPool.get() as proxy:
            # update DB and buffer
            ret = proxy.update_worker_node_gpu(
                site,
                host_name,
                vendor,
                model,
                count,
                vram,
                architecture,
                framework,
                framework_version,
                driver_version,
            )
        return ret

    def get_architecture_level_map(self) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_architecture_level_map()
        return ret

    def get_worker_node_gpu_map(self) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_worker_node_gpu_map()
        return ret

    def get_worker_node_metrics(
        self, site: str | None = None, panda_queue: str | None = None, host: str | None = None, key: str | None = None, days: int = 1
    ) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_worker_node_metrics(site, panda_queue, host, key, days)
        return ret

    # finalize pending analysis jobs
    def finalizePendingJobs(self, prodUserName: str, jobDefinitionID: int, waitLock: bool = False) -> bool:
        with self.proxyPool.get() as proxy:
            # update DB
            ret = proxy.finalizePendingJobs(prodUserName, jobDefinitionID, waitLock)
        return ret

    # retry job
    def retryJob(
        self,
        jobID: int,
        param: dict[str, Any],
        failedInActive: bool = False,
        changeJobInMem: bool = False,
        inMemJob: JobSpec | None = None,
        getNewPandaID: bool = False,
        attemptNr: int | None = None,
        recoverableEsMerge: bool = False,
    ) -> Any:
        with self.proxyPool.get() as proxy:
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
        return ret

    # activate jobs
    def activateJobs(self, jobs: list[JobSpec]) -> list[bool]:
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # loop over all jobs
            returns = []
            for job in jobs:
                # update DB
                ret = proxy.activateJob(job)
                returns.append(ret)
        return returns

    # send jobs to jobsWaiting
    def keepJobs(self, jobs: list[JobSpec]) -> list[bool]:
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # loop over all jobs
            returns = []
            for job in jobs:
                # update DB
                ret = proxy.keepJob(job)
                returns.append(ret)
        return returns

    # archive jobs
    def archiveJobs(self, jobs: list[JobSpec], inJobsDefined: bool, fromJobsWaiting: bool = False) -> list[bool]:
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # loop over all jobs
            returns = []
            for job in jobs:
                # update DB
                ret = proxy.archiveJob(job, inJobsDefined, fromJobsWaiting=fromJobsWaiting)
                returns.append(ret[0])
        return returns

    # set debug mode
    def setDebugMode(self, dn: str, pandaID: int, prodManager: bool, modeOn: bool, workingGroup: str | None) -> str | None:
        # get DB proxy
        retStr: str | None
        with self.proxyPool.get() as proxy:
            # check the number of debug jobs
            hitLimit = False
            if modeOn is True:
                # bound here because the production manager branch below sets no limit and
                # so never reads it
                jobList: list[Any] | None = None
                if prodManager:
                    limitNum = None
                elif workingGroup is not None:
                    jobList = proxy.getActiveDebugJobs(workingGroup=workingGroup)
                    limitNum = ProcessGroups.maxDebugWgJobs
                else:
                    jobList = proxy.getActiveDebugJobs(dn=dn)
                    limitNum = ProcessGroups.maxDebugJobs
                if limitNum and jobList is None:
                    # the subjobs already in debug mode could not be counted, so the limit
                    # cannot be checked and the mode must not be granted by default
                    return "Failed to count the subjobs already in debug mode"
                if limitNum and jobList is not None and len(jobList) >= limitNum:
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
        return retStr

    # get jobs
    def getJobs(
        self,
        nJobs: int,
        siteName: str,
        prodSourceLabel: str | None,
        mem: int | None,
        diskSpace: int | None,
        node: str | None,
        timeout: int,
        computingElement: str | None,
        prodUserID: str | None,
        taskID: int | None,
        background: bool,
        resourceType: str | None,
        harvester_id: str | None,
        worker_id: int | str | None,
        schedulerID: str | None,
        jobType: str | None,
        is_gu: bool,
        via_topic: bool,
        remaining_time: int | None,
        target_architecture: str | None,
    ) -> list[Any]:
        # get DBproxy
        with self.proxyPool.get() as proxy:
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
                remaining_time,
                target_architecture,
            )
            t_after = time.time()
            t_total = t_after - t_before
            _logger.debug(f"getJobs : took {t_total}s for {siteName} nJobs={nJobs} prodSourceLabel={prodSourceLabel}")

        # get secret
        secrets_map = {}
        for job in jobs:
            if job.use_secrets() and job.prodUserName not in secrets_map:
                # get secret
                with self.proxyPool.get() as proxy:
                    tmp_status, secret = proxy.get_user_secrets(job.prodUserName)
                    if not tmp_status:
                        secret = None
                secrets_map[job.prodUserName] = secret
            if job.is_debug_mode():
                if panda_config.pilot_secrets not in secrets_map:
                    # get secret
                    with self.proxyPool.get() as proxy:
                        tmp_status, secret = proxy.get_user_secrets(panda_config.pilot_secrets)
                        if not tmp_status:
                            secret = None
                    secrets_map[panda_config.pilot_secrets] = secret

        return jobs + [nSent, {}, secrets_map]

    # get job status
    def getJobStatus(
        self,
        jobIDs: Sequence[int | str],
        fromDefined: bool = True,
        fromActive: bool = True,
        fromArchived: bool = True,
        fromWaiting: bool = True,
    ) -> list[str | None]:
        # get DBproxy
        with self.proxyPool.get() as proxy:
            retStatus: list[str | None] = []
            # peek at job
            for jobID in jobIDs:
                res = proxy.peekJob(jobID, fromDefined, fromActive, fromArchived, fromWaiting)
                if res:
                    retStatus.append(res.jobStatus)
                else:
                    retStatus.append(None)
        return retStatus

    # peek at jobs
    def peekJobs(
        self,
        jobIDs: Sequence[int | str],
        fromDefined: bool = True,
        fromActive: bool = True,
        fromArchived: bool = True,
        fromWaiting: bool = True,
        forAnal: bool = False,
        use_json: bool = False,
    ) -> list[Any]:
        # get proxy
        with self.proxyPool.get() as proxy:
            retJobs: list[Any] = []
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
        return retJobs

    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self, jediTaskID: int, scout_only: bool = False, unsuccessful_only: bool = False) -> list[int]:
        """Get PanDA job IDs associated with a JEDI task.

        Args:
            jediTaskID: JEDI task ID.
            scout_only: When True, return only scout job IDs.
            unsuccessful_only: When True, return only job IDs with status failed, cancelled, or closed.

        Returns:
            list[int]: PanDA job IDs for the task.
        """
        # get DBproxy
        with self.proxyPool.get() as proxy:
            # exec
            retJobs = proxy.getPandaIDsWithTaskID(jediTaskID, scout_only=scout_only, unsuccessful_only=unsuccessful_only)
        return retJobs

    # get full job status
    def getFullJobStatus(
        self,
        jobIDs: Sequence[int | str],
        fromDefined: bool = True,
        fromActive: bool = True,
        fromArchived: bool = True,
        fromWaiting: bool = True,
        forAnal: bool = True,
        days: int | None = 90,
        use_json: bool = False,
    ) -> list[Any]:
        retJobMap = {}

        # peek at job
        for jobID in jobIDs:
            # get DBproxy for each job to avoid occupying connection for long time
            with self.proxyPool.get() as proxy:
                # peek job
                res = proxy.peekJob(jobID, fromDefined, fromActive, fromArchived, fromWaiting, forAnal)
                retJobMap[jobID] = res

        # get IDs
        for jobID in jobIDs:
            if retJobMap[jobID] is None:
                # get ArchiveDBproxy
                with self.proxyPool.get() as proxy:
                    # peek job
                    res = proxy.peekJobLog(jobID, days)
                    retJobMap[jobID] = res

        # sort
        retJobs: list[Any] = []
        for jobID in jobIDs:
            jobSpec = retJobMap[jobID]
            if use_json:
                if jobSpec is None:
                    retJobs.append(None)
                else:
                    retJobs.append(jobSpec.to_dict())
            else:
                retJobs.append(jobSpec)

        return retJobs

    # get script for offline running
    def getScriptOfflineRunning(self, pandaID: int, days: int | None = None) -> str:
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
            return generate_offline_run_script(tmpJobs[0])
        except Exception as e:
            _logger.error(f"getScriptOfflineRunning : {str(e)} {traceback.format_exc()}")
            return f"ERROR: ServerError in getScriptOfflineRunning with {str(e)}"

    # kill jobs
    def killJobs(
        self,
        ids: Sequence[int | str],
        user: str,
        code: int | str | None,
        prodManager: bool,
        wgProdRole: list[str] = [],
        killOptions: list[str] = [],
    ) -> list[bool]:
        tmp_log = LogWrapper(_logger, "killJobs")
        tmp_log.debug(f"start for {len(ids)} IDs")
        # get DBproxy
        with self.proxyPool.get() as proxy:
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
                                attemptNr=tmpJobSpec.attemptNr,  # type: ignore[arg-type]  # the id is a column, which is declared optional
                                recoverableEsMerge=True,
                            )
                        elif EventServiceUtils.isEventServiceJob(tmpJobSpec) and not EventServiceUtils.isJobCloningJob(tmpJobSpec):
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
                                # archiveJob reports its outcome first and then the DDM
                                # information this path has no use for. rets is a list of
                                # outcomes, so only the first element belongs in it
                                ret = proxy.archiveJob(
                                    tmpJobSpec,
                                    tmpJobSpec.jobStatus in ["defined", "assigned"],
                                )[0]
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
                        closer_process = Closer(self, tmpDestDBlocks, tmpJob)
                        closer_process.run()
        except Exception as e:
            tmp_log.error(f"failed with {str(e)} {traceback.format_exc()}")
        tmp_log.debug("done")
        return rets

    # reassign jobs
    def reassignJobs(
        self,
        ids: list[int],
        attempt: int = 0,
        joinThr: bool = False,
        forPending: bool = False,
        firstSubmission: bool = True,
    ) -> bool:
        tmp_log = LogWrapper(_logger, "reassignJobs")
        tmp_log.debug(f"start for {len(ids)} IDs")
        # get DB proxy
        with self.proxyPool.get() as proxy:
            jobs = []
            # reset jobs
            n_reset_waiting = 0
            n_reset_defined = 0
            for tmp_id in ids:
                try:
                    # try to reset a job in waiting table
                    ret = proxy.resetJob(
                        tmp_id,
                        activeTable=False,
                        forPending=True,
                    )
                    if ret is not None:
                        jobs.append(ret)
                        n_reset_waiting += 1
                        # waiting jobs don't create sub or dis
                        continue
                    # try to reset a job in defined table
                    ret = proxy.resetDefinedJob(tmp_id)
                    if ret is not None:
                        jobs.append(ret)
                        n_reset_defined += 1
                        continue
                except Exception as e:
                    tmp_log.error(f"failed with {str(e)} {traceback.format_exc()}")
        tmp_log.debug(f"got {len(jobs)} IDs in total: {n_reset_waiting} from Waiting, {n_reset_defined} from Defined")
        # trigger subsequent agent
        if jobs:
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
                # cannot use 'thr =' because it may trigger a garbage collector
                Setupper(
                    self,
                    jobs,
                    resubmit=True,
                    first_submission=firstSubmission,
                ).start()
        tmp_log.debug("done")

        return True

    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self, dataset: str, status: str, fileLFN: str = "") -> list[int]:
        with self.proxyPool.get() as proxy:
            retList = []
            # query PandaID
            retList = proxy.updateInFilesReturnPandaIDs(dataset, status, fileLFN)
        return retList

    # update input files for jobs at certain sites and return corresponding PandaIDs
    def update_input_files_at_sites_and_get_panda_ids(self, filename: str, sites: list[str]) -> list[int]:
        with self.proxyPool.get() as proxy:
            ret = proxy.update_input_files_at_sites_and_get_panda_ids(filename, sites)
        return ret

    # update output files and return corresponding PandaIDs
    def updateOutFilesReturnPandaIDs(self, dataset: str, fileLFN: str = "") -> list[int]:
        with self.proxyPool.get() as proxy:
            retList = []
            # query PandaID
            retList = proxy.updateOutFilesReturnPandaIDs(dataset, fileLFN)
        return retList

    # get _dis datasets associated to _sub
    def getAssociatedDisDatasets(self, subDsName: str) -> list[str]:
        with self.proxyPool.get() as proxy:
            retList = []
            # query
            retList = proxy.getAssociatedDisDatasets(subDsName)
        return retList

    # insert sandbox file info
    def insertSandboxFileInfo(self, userName: str, hostName: str, fileName: str, fileSize: int, checkSum: str) -> str:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.insertSandboxFileInfo(userName, hostName, fileName, fileSize, checkSum)
        return ret

    # get and lock sandbox files
    def getLockSandboxFiles(self, time_limit: int, n_files: int) -> list[tuple[Any, ...]] | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getLockSandboxFiles(time_limit, n_files)
        return ret

    # check duplicated sandbox file
    def checkSandboxFile(self, userName: str, fileSize: int, checkSum: int | str) -> str:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.checkSandboxFile(userName, fileSize, checkSum)
        return ret

    # insert datasets
    def insertDatasets(self, datasets: Collection[DatasetSpec]) -> list[bool]:
        # get DBproxy
        with self.proxyPool.get() as proxy:
            retList = []
            # insert
            for dataset in datasets:
                ret = proxy.insertDataset(dataset)
                retList.append(ret)
        return retList

    # get and lock dataset with a query
    def getLockDatasets(self, sqlQuery: str, varMapGet: dict[str, Any], modTimeOffset: str = "", getVersion: bool = False) -> list[Any]:
        with self.proxyPool.get() as proxy:
            # query Dataset
            ret = proxy.getLockDatasets(sqlQuery, varMapGet, modTimeOffset, getVersion)
        return ret

    # query Dataset
    def queryDatasetWithMap(self, map: dict[str, Any]) -> DatasetSpec | None:
        with self.proxyPool.get() as proxy:
            # query Dataset
            ret = proxy.queryDatasetWithMap(map)
        return ret

    # set GUIDs
    def setGUIDs(self, files: list[dict[str, Any]]) -> bool:
        with self.proxyPool.get() as proxy:
            # set GUIDs
            ret = proxy.setGUIDs(files)
        return ret

    # update dataset
    def updateDatasets(self, datasets: list[DatasetSpec], withLock: bool = False, withCriteria: str = "", criteriaMap: dict[str, Any] = {}) -> list[int]:
        with self.proxyPool.get() as proxy:
            # update Dataset
            retList = proxy.updateDataset(datasets, withLock, withCriteria, criteriaMap)
        return retList

    # trigger cleanup of internal datasets used by a task
    def trigger_cleanup_internal_datasets(self, task_id: int) -> bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.trigger_cleanup_internal_datasets(task_id)
        return ret

    # count the number of files with map
    def countFilesWithMap(self, map: dict[str, Any]) -> int:
        with self.proxyPool.get() as proxy:
            # query files
            ret = proxy.countFilesWithMap(map)
        return ret

    # get serial number for dataset
    def getSerialNumber(self, datasetname: str, definedFreshFlag: bool | None = None) -> tuple[int, bool]:
        with self.proxyPool.get() as proxy:
            # get serial number
            ret = proxy.getSerialNumber(datasetname, definedFreshFlag)
        return ret

    # add metadata
    def addMetadata(self, ids: list[int], metadataList: list[str], newStatusList: list[str]) -> list[bool]:
        # get DBproxy
        with self.proxyPool.get() as proxy:
            # add metadata
            index = 0
            retList = []
            for id in ids:
                ret = proxy.addMetadata(id, metadataList[index], newStatusList[index])
                retList.append(ret)
                index += 1
        return retList

    # add stdout
    def addStdOut(self, id: int, stdout: str) -> bool:
        with self.proxyPool.get() as proxy:
            # add
            ret = proxy.addStdOut(id, stdout)
        return ret

    # extract scope from dataset name
    def extractScope(self, name: str | None) -> str | None:
        with self.proxyPool.get() as proxy:
            # get
            ret = proxy.extractScope(name)
        return ret

    # get job statistics
    def getJobStatistics(self) -> dict[Any, Any] | None:
        with self.proxyPool.get() as proxy:
            # get serial number
            ret = proxy.getJobStatistics()
        return ret

    # get detailed job statistics with resource_type and prodsourcelabel
    def getDetailedJobStatistics(self) -> dict[Any, Any] | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.getDetailedJobStatistics()
        return ret

    # get job statistics for ExtIF. Source type is analysis or production
    def getJobStatisticsForExtIF(self, sourcetype: str | None = None) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # get serial number
            ret = proxy.getJobStatisticsForExtIF(sourcetype)
        return ret

    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # get serial number
            ret = proxy.getJobStatisticsPerProcessingType()
        return ret

    # update site data
    def updateSiteData(self, hostID: str, pilotRequests: dict[str, Any], interval: int = 3) -> bool:
        with self.proxyPool.get() as proxy:
            # get serial number
            ret = proxy.updateSiteData(hostID, pilotRequests, interval)
        return ret

    # get current site data
    def getCurrentSiteData(self) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            # get serial number
            ret = proxy.getCurrentSiteData()
        return ret

    # insert nRunning in site data
    def insertnRunningInSiteData(self) -> bool:
        with self.proxyPool.get() as proxy:
            # get serial number
            ret = proxy.insertnRunningInSiteData()
        return ret

    # get site info
    def getSiteInfo(self) -> tuple[dict[Any, Any], dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            # get site info
            ret = proxy.getSiteInfo()
        return ret

    # get cloud list
    def get_cloud_list(self) -> list[str]:
        with self.proxyPool.get() as proxy:
            # get cloud list
            ret = proxy.get_cloud_list()
        return ret

    # async request methods

    def upsert_machine_heartbeat(self, machine_name: str, service_name: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.upsert_machine_heartbeat(machine_name, service_name)

    def get_alive_machines(self, service_name: str | None, within_minutes: int = 60) -> list[str]:
        with self.proxyPool.get() as proxy:
            return proxy.get_alive_machines(service_name, within_minutes)

    def insert_async_request(
        self, request_id: str, request_type: str, parameters_json: str, service_name: str | None, machine_name: str | None, expected_machines_json: str | None
    ) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.insert_async_request(request_id, request_type, parameters_json, service_name, machine_name, expected_machines_json)

    def get_async_request(self, request_id: str) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_async_request(request_id)

    def get_pending_requests_for_machine(self, my_hostname: str, my_service: str, known_types: list[str]) -> list[dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.get_pending_requests_for_machine(my_hostname, my_service, known_types)

    def claim_async_result(self, request_id: str, machine_name: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.claim_async_result(request_id, machine_name)

    def finish_async_result(
        self,
        request_id: str,
        machine_name: str,
        status: str,
        result: str | None = None,
        error_msg: str | None = None,
        truncated: bool = False,
        stderr: str | None = None,
        return_code: int | None = None,
        retriable: bool = True,
    ) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.finish_async_result(
                request_id,
                machine_name,
                status,
                result,
                error_msg,
                truncated,
                stderr,
                return_code,
                retriable,
            )

    def touch_async_result(self, request_id: str, machine_name: str) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.touch_async_result(request_id, machine_name)

    def get_async_results(self, request_id: str) -> list[dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            return proxy.get_async_results(request_id)

    def cleanup_async_requests(self, retention_days: int = 7) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.cleanup_async_requests(retention_days)

    def recover_stale_results(self, machine_name: str, max_processing_seconds: int = 300, max_attempts: int = 3) -> bool:
        with self.proxyPool.get() as proxy:
            return proxy.recover_stale_results(machine_name, max_processing_seconds, max_attempts)

    # get special dispatcher parameters
    def get_special_dispatch_params(self) -> tuple[bool, dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.get_special_dispatch_params()
        return ret

    # get email address
    def getEmailAddr(self, name: str, withDN: bool = False, withUpTime: bool = False) -> Any:
        with self.proxyPool.get() as proxy:
            # get
            ret = proxy.getEmailAddr(name, withDN, withUpTime)
        return ret

    # set email address for a user
    def setEmailAddr(self, userName: str, emailAddr: str) -> bool:
        with self.proxyPool.get() as proxy:
            # set
            ret = proxy.setEmailAddr(userName, emailAddr)
        return ret

    # get banned users
    def get_ban_users(self) -> tuple[bool, dict[str, Any] | None]:
        with self.proxyPool.get() as proxy:
            # get
            ret = proxy.get_ban_users()
        return ret

    # register a token key
    def register_token_key(self, client_name: str, lifetime: int) -> bool:
        with self.proxyPool.get() as proxy:
            # register proxy key
            ret = proxy.register_token_key(client_name, lifetime)
        return ret

    # query an SQL return Status
    def querySQLS(self, sql: str, varMap: dict[str, Any], arraySize: int = 1000) -> tuple[Any, Any]:
        with self.proxyPool.get() as proxy:
            # get
            ret = proxy.querySQLS(sql, varMap, arraySize)
        return ret

    # query an SQL
    def querySQL(self, sql: str, varMap: dict[str, Any], arraySize: int = 1000) -> Any:
        # get DBproxy
        with self.proxyPool.get() as proxy:
            # get
            ret = proxy.querySQLS(sql, varMap, arraySize)[1]
        return ret

    # execute an SQL return with executemany
    def executemanySQL(self, sql: str, varMaps: list[dict[str, Any]], arraySize: int = 1000) -> Any:
        with self.proxyPool.get() as proxy:
            # get
            ret = proxy.executemanySQL(sql, varMaps, arraySize)
        return ret

    # check quota
    def checkQuota(self, dn: str) -> float:
        with self.proxyPool.get() as proxy:
            # get
            ret = proxy.checkQuota(dn)
        return ret

    # insert TaskParams
    def insertTaskParamsPanda(
        self,
        taskParams: Any,
        user: str,
        prodRole: bool,
        fqans: list[str] = [],
        parent_tid: int | None = None,
        properErrorCode: bool = False,
        allowActiveTask: bool = False,
        decode: bool = True,
    ) -> tuple[Any, Any]:
        # query an SQL return Status
        with self.proxyPool.get() as proxy:
            # check user status
            tmpStatus = proxy.checkBanUser(user, None, True)
            if tmpStatus is True:
                # exec
                ret = proxy.insertTaskParamsPanda(taskParams, user, prodRole, fqans, parent_tid, properErrorCode, allowActiveTask, decode)
            elif tmpStatus == 1:
                ret = False, "Failed to update DN in PandaDB"
            elif tmpStatus == 2:
                ret = False, "Failed to insert user info to PandaDB"
            else:
                ret = False, f"The following DN is banned: DN={user}"
        return ret

    def validate_ownership_or_production_role(self, task_id: int, dn: str, production_role: bool) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.validate_ownership_or_production_role(task_id, dn, production_role)
        return ret

    # send command to task
    def sendCommandTaskPanda(
        self,
        jediTaskID: int,
        dn: str,
        prodRole: bool,
        comStr: str,
        comComment: str | None = None,
        useCommit: bool = True,
        properErrorCode: bool = False,
        comQualifier: str | None = None,
        broadcast: bool = False,
    ) -> tuple[Any, str]:
        with self.proxyPool.get() as proxy:
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
        return ret

    # update unmerged datasets to trigger merging
    def updateUnmergedDatasets(self, job: JobSpec, finalStatusDS: list[DatasetSpec], updateCompleted: bool = False) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.updateUnmergedDatasets(job, finalStatusDS, updateCompleted)
        return ret

    # get active JediTasks in a time range
    def getJediTasksInTimeRange(
        self, dn: str | None, timeRangeStr: str, fullFlag: bool = False, minTaskID: int | None = None, task_type: str = "user"
    ) -> dict[Any, Any]:
        # check DN
        if dn in ["NULL", "", "None", None]:
            return {}
        # check timeRange
        match = re.match(r"^(\d+)-(\d+)-(\d+) (\d+):(\d+):(\d+)$", timeRangeStr)
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
        maxRange = naive_utcnow() - datetime.timedelta(days=30)
        if timeRange < maxRange:
            timeRange = maxRange
        # get proxy
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getJediTasksInTimeRange(dn, timeRange, fullFlag, minTaskID, task_type)
        return ret

    def getTaskIDsWithCriteria_JEDI(self, criteria: dict[str, Any], since: str | None = None, nTasks: int = 50) -> list[int] | None:
        with self.proxyPool.get() as proxy:
            return proxy.getTaskIDsWithCriteria_JEDI(criteria, since=since, nTasks=nTasks)

    # get details of JediTask
    def getJediTaskDetails(self, jediTaskID: int, fullFlag: bool, withTaskInfo: bool) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getJediTaskDetails(jediTaskID, fullFlag, withTaskInfo)
        return ret

    # get task details as a plain dict (read-only, no lock)
    def get_task_details_json(self, jedi_task_id: int, resolve_parent: bool = False, include_resolve_status: bool = False) -> Any:
        with self.proxyPool.get() as proxy:
            return proxy.get_task_details_json(
                jedi_task_id,
                resolve_parent=resolve_parent,
                include_resolve_status=include_resolve_status,
            )

    # get a list of even ranges for a PandaID
    def getEventRanges(
        self, pandaID: int, jobsetID: int, jediTaskID: int | None, nRanges: int, acceptJson: bool, scattered: bool, segment_id: int | None
    ) -> Any:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getEventRanges(pandaID, jobsetID, jediTaskID, nRanges, acceptJson, scattered, segment_id)
        return ret

    # update an even range
    def updateEventRange(
        self, eventRangeID: str, eventStatus: str, cpuCore: int | None, cpuConsumptionTime: int | None, objstoreID: int | None = None
    ) -> tuple[Any, str]:
        eventDict: dict[str, Any] = {}
        eventDict["eventRangeID"] = eventRangeID
        eventDict["eventStatus"] = eventStatus
        eventDict["cpuCore"] = cpuCore
        eventDict["cpuConsumptionTime"] = cpuConsumptionTime
        eventDict["objstoreID"] = objstoreID
        # get proxy
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.updateEventRanges([eventDict])
        # extract return
        try:
            retVal = ret[0][0]
        except Exception:
            retVal = False

        return retVal, json.dumps(ret[1])

    # update even ranges
    def updateEventRanges(self, eventRanges: str, version: int = 0) -> Any:
        # decode json
        try:
            eventRanges = json.loads(eventRanges)
        except Exception:
            return json.dumps("ERROR : failed to convert eventRanges with json")
        # get proxy
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.updateEventRanges(eventRanges, version)
        if version != 0:
            return ret
        return json.dumps(ret[0]), json.dumps(ret[1])

    # change task priority
    def changeTaskPriorityPanda(self, jediTaskID: int, newPriority: int) -> int | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.changeTaskPriorityPanda(jediTaskID, newPriority)
        return ret

    # throttle user jobs
    def throttleUserJobs(self, prodUserName: str, workingGroup: str | None, get_dict: bool = False) -> Any:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.throttleUserJobs(prodUserName, workingGroup, get_dict)
        return ret

    # unthrottle user jobs
    def unThrottleUserJobs(self, prodUserName: str, workingGroup: str | None, get_dict: bool = False) -> Any:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.unThrottleUserJobs(prodUserName, workingGroup, get_dict)
        return ret

    # get throttled users
    def getThrottledUsers(self) -> set[tuple[Any, Any]]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getThrottledUsers()
        return ret

    # get the list of jobdefIDs for failed jobs in a task
    def getJobdefIDsForFailedJob(self, jediTaskID: int) -> list[int]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getJobdefIDsForFailedJob(jediTaskID)
        return ret

    # change task attribute
    def changeTaskAttributePanda(self, jediTaskID: int, attrName: str, attrValue: Any) -> int | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.changeTaskAttributePanda(jediTaskID, attrName, attrValue)
        return ret

    # change split rule for task
    def changeTaskSplitRulePanda(self, jediTaskID: int, attrName: str, attrValue: Any) -> int | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.changeTaskSplitRulePanda(jediTaskID, attrName, attrValue)
        return ret

    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self, jediTaskID: int, increasedNr: int) -> tuple[int | None, str]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.increaseAttemptNrPanda(jediTaskID, increasedNr)
        return ret

    # get jediTaskID from taskName
    def getTaskIDwithTaskNameJEDI(self, userName: str, taskName: str) -> int | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getTaskIDwithTaskNameJEDI(userName, taskName)
        return ret

    # update error dialog for a jediTaskID
    def updateTaskErrorDialogJEDI(self, jediTaskID: int, msg: str) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.updateTaskErrorDialogJEDI(jediTaskID, msg)
        return ret

    # update modificationtime for a jediTaskID to trigger subsequent process
    def updateTaskModTimeJEDI(self, jediTaskID: int, newStatus: str | None = None) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.updateTaskModTimeJEDI(jediTaskID, newStatus)
        return ret

    # check input file status
    def checkInputFileStatusInJEDI(self, jobSpec: JobSpec) -> bool | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.checkInputFileStatusInJEDI(jobSpec)
        return ret

    # increase memory limit
    def increaseRamLimitJEDI(self, jediTaskID: int, jobRamCount: int | None) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.increaseRamLimitJEDI(jediTaskID, jobRamCount)
        return ret

    # increase memory limit
    def increaseRamLimitJobJEDI(self, job: JobSpec, jobRamCount: int | None, jediTaskID: int) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.increaseRamLimitJobJEDI(job, jobRamCount, jediTaskID)
        return ret

    # increase memory limit xtimes
    def increaseRamLimitJobJEDI_xtimes(self, job: JobSpec, jobRamCount: int | None, jediTaskID: int, attemptNr: int | None) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.increaseRamLimitJobJEDI_xtimes(job, jobRamCount, jediTaskID, attemptNr)
        return ret

    # reduce input per job
    def reduce_input_per_job(
        self, panda_id: int | None, jedi_task_id: int | None, attempt_nr: int | None, excluded_rules: str | None, steps: int, dry_mode: bool = False
    ) -> tuple[bool | None, str]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.reduce_input_per_job(panda_id, jedi_task_id, attempt_nr, excluded_rules, steps, dry_mode)
        return ret

    # reset files in JEDI
    def resetFileStatusInJEDI(
        self, dn: str, prodManager: bool, datasetName: str, lostFiles: list[str], recoverParent: bool, simul: bool = False
    ) -> tuple[bool, int | None, dict[str, list[str]] | None, str | None]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.resetFileStatusInJEDI(dn, prodManager, datasetName, lostFiles, recoverParent, simul)
        return ret

    # copy file records
    def copy_file_records(self, new_lfns: list[str], file_spec: FileSpec) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.copy_file_records(new_lfns, file_spec)
        return ret

    # retry module: get the defined rules
    def getRetrialRules(self) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getRetrialRules()
        return ret

    # retry module action: set max number of retries
    def setMaxAttempt(self, jobID: int, jediTaskID: int, files: list[FileSpec], attemptNr: int) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.setMaxAttempt(jobID, jediTaskID, files, attemptNr)
        return ret

    # error classification action: increase by one the max number of retries
    def increase_max_failure(self, job_id: int, task_id: int, files: list[FileSpec]) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.increase_max_failure(job_id, task_id, files)
        return ret

    # retry module action: set maxAttempt to the current attemptNr to avoid further retries
    def setNoRetry(self, jobID: int, jediTaskID: int, files: list[FileSpec]) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.setNoRetry(jobID, jediTaskID, files)
        return ret

    # retry module action: increase CPU Time
    def initialize_cpu_time_task(self, jobID: int, taskID: int, siteid: str, files: list[FileSpec], active: bool) -> tuple[float | None, str | None]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.initialize_cpu_time_task(jobID, taskID, siteid, files, active)
        return ret

    # retry module action: recalculate the Task Parameters
    def requestTaskParameterRecalculation(self, taskID: int) -> int:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.requestTaskParameterRecalculation(taskID)
        return ret

    # add associate sub datasets for single consumer job
    def getDestDBlocksWithSingleConsumer(self, jediTaskID: int, PandaID: int, ngDatasets: list[str]) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getDestDBlocksWithSingleConsumer(jediTaskID, PandaID, ngDatasets)
        return ret

    # check validity of merge job
    def isValidMergeJob(self, pandaID: int, jediTaskID: int) -> tuple[bool | None, str]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.isValidMergeJob(pandaID, jediTaskID)
        return ret

    # Configurator: insert network matrix data
    def insertNetworkMatrixData(self, data: list[tuple[Any, ...]]) -> tuple[None, str] | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.insertNetworkMatrixData(data)
        return ret

    # Configurator: delete old network matrix data
    def deleteOldNetworkData(self) -> tuple[None, str] | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.deleteOldNetworkData()
        return ret

    # get dispatch datasets per user
    def getDispatchDatasetsPerUser(self, vo: str, prodSourceLabel: str, onlyActive: bool, withSize: bool) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getDispatchDatasetsPerUser(vo, prodSourceLabel, onlyActive, withSize)
        return ret

    # get task parameters
    def getTaskParamsPanda(self, jediTaskID: int) -> str:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getTaskParamsPanda(jediTaskID)
        return ret

    # get task attributes
    def getTaskAttributesPanda(self, jediTaskID: int, attrs: list[str]) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getTaskAttributesPanda(jediTaskID, attrs)
        return ret

    # check for cloned jobs
    def checkClonedJob(self, jobSpec: JobSpec) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.checkClonedJob(jobSpec)
        return ret

    # get co-jumbo jobs to be finished
    def getCoJumboJobsToBeFinished(self, timeLimit: int, minPriority: int, maxJobs: int) -> list[Any] | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getCoJumboJobsToBeFinished(timeLimit, minPriority, maxJobs)
        return ret

    # get number of events to be processed
    def getNumReadyEvents(self, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getNumReadyEvents(jediTaskID)
        return ret

    # check if task is applicable for jumbo jobs
    def isApplicableTaskForJumbo(self, jediTaskID: int) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.isApplicableTaskForJumbo(jediTaskID)
        return ret

    # cleanup jumbo jobs
    def cleanupJumboJobs(self, jediTaskID: int | None = None) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.cleanupJumboJobs(jediTaskID)
        return ret

    # convert ObjID to endpoint
    def convertObjIDtoEndPoint(self, srcFileName: str, ObjID: str | int) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.convertObjIDtoEndPoint(srcFileName, ObjID)
        return res

    # get task status
    def getTaskStatus(self, jediTaskID: int) -> Sequence[Any]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getTaskStatus(jediTaskID)
        return res

    # get task status and superstatus
    def getTaskStatusSuperstatus(self, jediTaskID: int) -> Sequence[Any]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getTaskStatusSuperstatus(jediTaskID)
        return res

    # reactivate task
    def reactivateTask(self, jediTaskID: int, keep_attempt_nr: bool = False, trigger_job_generation: bool = False) -> tuple[int | None, str]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.reactivateTask(jediTaskID, keep_attempt_nr, trigger_job_generation)
        return res

    # get event statistics
    def getEventStat(self, jediTaskID: int, PandaID: int) -> dict[Any, int]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getEventStat(jediTaskID, PandaID)
        return res

    # get nested dict of gshare names implying the tree structure
    def get_tree_of_gshare_names(self) -> Any:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.get_tree_of_gshare_names()
        return res

    # reassign share
    def reassignShare(self, jedi_task_ids: list[int], share_dest: str, reassign_running: bool) -> tuple[int, str | None]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.reassignShare(jedi_task_ids, share_dest, reassign_running)
        return res

    def is_valid_share(self, share_name: str) -> bool:
        """
        Checks whether the share is a valid leave share
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.is_valid_share(share_name)
        return res

    def get_share_for_task(self, task: JediTaskSpec) -> str:
        """
        Return the share based on a task specification
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.get_share_for_task(task)
        return res

    def get_share_for_job(self, job: JobSpec) -> str:
        """
        Return the share based on a task specification
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.get_share_for_job(job)
        return res

    def getTaskParamsMap(self, jediTaskID: int) -> str:
        """
        Return the taskParamsMap
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getTaskParamsPanda(jediTaskID)
        return res

    def getCommands(self, harvester_id: str, n_commands: int) -> tuple[int, list[Any]]:
        """
        Get n commands for a particular harvester instance
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getCommands(harvester_id, n_commands)
        return res

    def ackCommands(self, command_ids: list[int]) -> int:
        """
        Acknowledge a list of command IDs
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.ackCommands(command_ids)
        return res

    # send command to harvester or lock command
    def commandToHarvester(
        self,
        harvester_ID: str,
        command: str,
        ack_requested: bool,
        status: str,
        lockInterval: int | None = None,
        comInterval: int | None = None,
        params: dict[str, Any] | None = None,
    ) -> bool:
        with self.proxyPool.get() as proxy:
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
        return res

    def getResourceTypes(self) -> list[Any]:
        """
        Get resource types (SCORE, MCORE, ...) and their definitions
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.load_resource_types(formatting="dict")
        return res

    # report stat of workers
    def reportWorkerStats_jobtype(self, harvesterID: str, siteName: str, paramsList: str) -> tuple[bool, str]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.reportWorkerStats_jobtype(harvesterID, siteName, paramsList)
        return res

    # get command locks
    def getCommandLocksHarvester(self, harvester_ID: str, command: str, lockedBy: str, lockInterval: int, commandInterval: int) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getCommandLocksHarvester(harvester_ID, command, lockedBy, lockInterval, commandInterval)
        return res

    # release command lock
    def releaseCommandLockHarvester(self, harvester_ID: str, command: str, computingSite: str, resourceType: str, lockedBy: str) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.releaseCommandLockHarvester(harvester_ID, command, computingSite, resourceType, lockedBy)
        return res

    # update workers
    def updateWorkers(self, harvesterID: str, data: list[dict[str, Any]]) -> list[Any] | None:
        """
        Update workers
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.updateWorkers(harvesterID, data)
        return res

    # update workers
    def updateServiceMetrics(self, harvesterID: str, data: list[Any]) -> list[bool] | None:
        """
        Update workers
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.updateServiceMetrics(harvesterID, data)
        return res

    # heartbeat for harvester
    def harvesterIsAlive(self, user: str, host: str, harvesterID: str, data: dict[str, Any]) -> str | None:
        """
        update harvester instance information
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.harvesterIsAlive(user, host, harvesterID, data)
        return res

    def storePilotLog(self, panda_id: int, pilot_log: str) -> int:
        """
        Store the pilot log in the pandalog table
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.storePilotLog(panda_id, pilot_log)
        return res

    # read the resource types from the DB
    def load_resource_types(self) -> list[Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret_val = proxy.load_resource_types()
        return ret_val

    # get the resource of a task
    def get_resource_type_task(self, task_spec: JediTaskSpec) -> str | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret_val = proxy.get_resource_type_task(task_spec)
        return ret_val

    def reset_resource_type_task(self, jedi_task_id: int, use_commit: bool = True) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret_val = proxy.reset_resource_type_task(jedi_task_id, use_commit)
        return ret_val

    # get the resource of a task
    def get_resource_type_job(self, job_spec: JobSpec) -> str | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret_val = proxy.get_resource_type_job(job_spec)
        return ret_val

    # check Job status
    def checkJobStatus(self, pandaIDs: str) -> list[dict[str, Any]]:
        try:
            id_list = pandaIDs.split(",")
        except Exception:
            id_list = []
        # get DBproxy
        with self.proxyPool.get() as proxy:
            # exec
            retList = []
            for pandaID in id_list:
                ret = proxy.checkJobStatus(pandaID)
                retList.append(ret)
        return retList

    # get stat of workers
    def getWorkerStats(self) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getWorkerStats()
        return ret

    # get unified pilot streaming queues
    def ups_get_queues(self) -> list[str]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.ups_get_queues()
        return ret

    # load harvester worker stats
    def ups_load_worker_stats(self) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.ups_load_worker_stats()
        return ret

    # get the distribution of new workers to submit
    def get_average_memory_workers(self, queue: str, harvester_id: str, target: int) -> tuple[float, float]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.get_average_memory_workers(queue, harvester_id, target)
        return ret

    # get the distribution of new workers to submit
    def ups_new_worker_distribution(self, queue: str, worker_stats: dict[str, Any]) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.ups_new_worker_distribution(queue, worker_stats)
        return ret

    # check event availability
    def checkEventsAvailability(self, pandaID: int, jobsetID: int, jediTaskID: int) -> int | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.checkEventsAvailability(pandaID, jobsetID, jediTaskID)
        return ret

    # get LNFs for jumbo job
    def getLFNsForJumbo(self, jediTaskID: int) -> Any:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getLFNsForJumbo(jediTaskID)
        return ret

    # get original consumers
    def getOriginalConsumers(self, jediTaskID: int, jobsetID: int, pandaID: int) -> list[Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getOriginalConsumers(jediTaskID, jobsetID, pandaID)
        return ret

    # add harvester dialog messages
    def addHarvesterDialogs(self, harvesterID: str, dialogs: list[dict[str, Any]]) -> bool:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.addHarvesterDialogs(harvesterID, dialogs)
        return ret

    # get job statistics per site and resource
    def getJobStatisticsPerSiteResource(self, timeWindow: int | None = None) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getJobStatisticsPerSiteResource(timeWindow)
        return ret

    # get job statistics per site, source label, and resource type
    def get_job_statistics_per_site_label_resource(self, time_window: int | None = None) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.get_job_statistics_per_site_label_resource(time_window)
        return ret

    # set num slots for workload provisioning
    def setNumSlotsForWP(
        self, pandaQueueName: str, numSlots: int, gshare: str | None, resourceType: str | None, validPeriod: int | str | None
    ) -> tuple[int, str]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.setNumSlotsForWP(pandaQueueName, numSlots, gshare, resourceType, validPeriod)
        return ret

    # enable jumbo jobs
    def enableJumboJobs(self, jediTaskID: int, totalJumboJobs: int, nJumboPerSite: int) -> tuple[int, str]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.enableJumboJobs(jediTaskID, totalJumboJobs, nJumboPerSite)
        return ret

    # enable event service
    def enableEventService(self, jediTaskID: int) -> tuple[int, str]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.enableEventService(jediTaskID)
        return ret

    # get JEDI file attributes
    def getJediFileAttributes(self, PandaID: int, jediTaskID: int, datasetID: int, fileID: int, attrs: list[str]) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getJediFileAttributes(PandaID, jediTaskID, datasetID, fileID, attrs)
        return ret

    # check if super user
    def isSuperUser(self, userName: str) -> tuple[bool, bool]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.isSuperUser(userName)
        return ret

    # get workers for a job
    def getWorkersForJob(self, PandaID: int) -> list[Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getWorkersForJob(PandaID)
        return ret

    # get user job metadata
    def getUserJobMetadata(self, jediTaskID: int) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getUserJobMetadata(jediTaskID)
        return ret

    # get jumbo job datasets
    def getJumboJobDatasets(self, n_days: int, grace_period: int) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getJumboJobDatasets(n_days, grace_period)
        return ret

    # get global shares status
    def getGShareStatus(self) -> list[dict[str, Any]]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getGShareStatus()
        return ret

    # get output datasets
    def getOutputDatasetsJEDI(self, panda_id: int) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.getOutputDatasetsJEDI(panda_id)
        return ret

    # update/insert JSON queue information into the scheconfig replica
    def upsertQueuesInJSONSchedconfig(self, schedconfig_dump: dict[str, Any] | None) -> str | None:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.upsertQueuesInJSONSchedconfig(schedconfig_dump)
        return ret

    # update/insert SW tag information
    def loadSWTags(self, sw_tags: dict[str, Any] | None) -> str:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.loadSWTags(sw_tags)
        return ret

    # generate a harvester command to clean up the workers of a site
    def sweepPQ(self, panda_queue_des: str, status_list_des: list[str], ce_list_des: str | list[str], submission_host_list_des: list[str]) -> str:
        with self.proxyPool.get() as proxy:
            # exec
            ret = proxy.sweepPQ(panda_queue_des, status_list_des, ce_list_des, submission_host_list_des)
        return ret

    # lock process
    def lockProcess_PANDA(self, component: str, pid: str, time_limit: int = 5, force: bool = False) -> bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.lockProcess_PANDA(component, pid, time_limit, force)
        return ret

    # unlock process
    def unlockProcess_PANDA(self, component: str, pid: str) -> bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.unlockProcess_PANDA(component, pid)
        return ret

    # check process lock
    def checkProcessLock_PANDA(self, component: str, pid: str, time_limit: int, check_base: bool = False) -> tuple[bool, Any]:
        with self.proxyPool.get() as proxy:
            ret = proxy.checkProcessLock_PANDA(component, pid, time_limit, check_base)
        return ret

    # insert job output report
    def insertJobOutputReport(self, panda_id: int, prod_source_label: str, job_status: str, attempt_nr: int, data: str) -> bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.insertJobOutputReport(panda_id, prod_source_label, job_status, attempt_nr, data)
        return ret

    # deleted job output report
    def deleteJobOutputReport(self, panda_id: int, attempt_nr: int | None) -> bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.deleteJobOutputReport(panda_id, attempt_nr)
        return ret

    # update data of job output report
    def updateJobOutputReport(self, panda_id: int, attempt_nr: int, data: str) -> bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.updateJobOutputReport(panda_id, attempt_nr, data)
        return ret

    # get job output report
    def getJobOutputReport(self, panda_id: int, attempt_nr: int | None) -> dict[str, Any]:
        with self.proxyPool.get() as proxy:
            ret = proxy.getJobOutputReport(panda_id, attempt_nr)
        return ret

    # lock job output report
    def lockJobOutputReport(self, panda_id: int, attempt_nr: int, pid: str, time_limit: int, take_over_from: str | None = None) -> list[Any] | bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.lockJobOutputReport(panda_id, attempt_nr, pid, time_limit, take_over_from)
        return ret

    # unlock job output report
    def unlockJobOutputReport(self, panda_id: int, attempt_nr: int | None, pid: str | None, lock_offset: int) -> list[Any] | bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.unlockJobOutputReport(panda_id, attempt_nr, pid, lock_offset)
        return ret

    # list pandaID and attemptNr of job output report
    def listJobOutputReport(
        self,
        only_unlocked: bool = False,
        time_limit: int = 5,
        limit: int = 999999,
        grace_period: int = 3,
        labels: list[str] | None = None,
        anti_labels: list[str] | None = None,
    ) -> list[Any] | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.listJobOutputReport(only_unlocked, time_limit, limit, grace_period, labels, anti_labels)
        return ret

    # update problematic resource info for user
    def update_problematic_resource_info(self, user_name: str, jedi_task_id: int, resource: str, problem_type: str) -> bool | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.update_problematic_resource_info(user_name, jedi_task_id, resource, problem_type)
        return ret

    # send command to a job
    def send_command_to_job(self, panda_id: int, com: str) -> tuple[bool, str]:
        with self.proxyPool.get() as proxy:
            ret = proxy.send_command_to_job(panda_id, com)
        return ret

    # get workers with stale states and update them with pilot information
    def get_workers_to_synchronize(self) -> list[Any] | dict[str, Any]:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_workers_to_synchronize()
        return ret

    # set user secret
    def set_user_secret(self, owner: str, key: str | None, value: str | None) -> tuple[bool, str]:
        with self.proxyPool.get() as proxy:
            ret = proxy.set_user_secret(owner, key, value)
        return ret

    # get user secrets
    def get_user_secrets(self, owner: str, keys: str | None = None, get_json: bool = False) -> tuple[bool, Any]:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_user_secrets(owner, keys, get_json)
        return ret

    def configurator_write_sites(self, sites_list: list[Any]) -> tuple[int, str | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_write_sites(sites_list)
        return ret

    def configurator_write_panda_sites(self, panda_site_list: list[Any]) -> tuple[int, str | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_write_panda_sites(panda_site_list)
        return ret

    def configurator_write_ddm_endpoints(self, ddm_endpoint_list: list[Any]) -> tuple[int, str | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_write_ddm_endpoints(ddm_endpoint_list)
        return ret

    def configurator_write_panda_ddm_relations(self, relation_list: list[Any]) -> tuple[int, str | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_write_panda_ddm_relations(relation_list)
        return ret

    def configurator_read_sites(self) -> set[str]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_read_sites()
        return ret

    def configurator_read_panda_sites(self) -> set[str]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_read_panda_sites()
        return ret

    def configurator_read_ddm_endpoints(self) -> set[str]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_read_ddm_endpoints()
        return ret

    def configurator_read_cric_sites(self) -> set[str]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_read_cric_sites()
        return ret

    def configurator_read_cric_panda_sites(self) -> set[str]:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_read_cric_panda_sites()
        return ret

    def configurator_delete_sites(self, sites_to_delete: Collection[str]) -> tuple[int, str | None] | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_delete_sites(sites_to_delete)
        return ret

    def configurator_delete_panda_sites(self, panda_sites_to_delete: Collection[str]) -> tuple[int, str | None] | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_delete_panda_sites(panda_sites_to_delete)
        return ret

    def configurator_delete_ddm_endpoints(self, ddm_endpoints_to_delete: Collection[str]) -> tuple[int, str | None] | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.configurator_delete_ddm_endpoints(ddm_endpoints_to_delete)
        return ret

    def carbon_write_region_emissions(self, emissions: list[dict[str, Any]]) -> tuple[int, str | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.carbon_write_region_emissions(emissions)
        return ret

    def carbon_aggregate_emissions(self) -> tuple[int, str | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.carbon_aggregate_emissions()
        return ret

    def get_files_in_datasets(self, task_id: int, dataset_types: str | Sequence[str], dataset_only: bool = False) -> list[dict[str, Any]] | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_files_in_datasets(task_id, dataset_types, dataset_only)
        return ret

    def get_max_worker_id(self, harvester_id: str) -> int | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_max_worker_id(harvester_id)
        return ret

    def get_events_status(self, ids: str) -> dict[str, Any] | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_events_status(ids)
        return ret

    def async_update_datasets(self, panda_id: int | None) -> bool | None:
        with self.proxyPool.get() as proxy:
            ret = proxy.async_update_datasets(panda_id)
        return ret

    def set_workload_metrics(self, jedi_task_id: int, panda_id: int | None, metrics: dict[str, Any]) -> bool:
        with self.proxyPool.get() as proxy:
            ret = proxy.set_workload_metrics(jedi_task_id, panda_id, metrics, True)
        return ret

    def get_workload_metrics(self, jedi_task_id: int, panda_id: int | None) -> tuple[bool, dict[str, Any] | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_workload_metrics(jedi_task_id, panda_id)
        return ret

    def get_jobs_metrics_in_task(self, jedi_task_id: int) -> tuple[bool, list[list[Any]] | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_jobs_metrics_in_task(jedi_task_id)
        return ret

    def enable_job_cloning(self, jedi_task_id: int, mode: str | None, multiplicity: int | None, num_sites: int | None) -> tuple[bool, str | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.enable_job_cloning(jedi_task_id, mode, multiplicity, num_sites)
        return ret

    def disable_job_cloning(self, jedi_task_id: int) -> tuple[bool, str | None]:
        with self.proxyPool.get() as proxy:
            ret = proxy.disable_job_cloning(jedi_task_id)
        return ret

    # gets statistics on the number of jobs with a specific status for each nucleus at each site
    def get_num_jobs_with_status_by_nucleus(self, vo: str, job_status: str) -> tuple[bool, dict[str, dict[str, int]]]:
        with self.proxyPool.get() as proxy:
            return proxy.get_num_jobs_with_status_by_nucleus(vo, job_status)

    # ==== JEDI taskbuffer functions ===========================

    # get JEDI task with jediTaskID
    def getTaskWithID_JEDI(
        self, jediTaskID: int, fullFlag: bool = False, lockTask: bool = False, pid: str | None = None, lockInterval: int | None = None, clearError: bool = False
    ) -> tuple[bool, JediTaskSpec | None]:
        with self.proxyPool.get() as proxy:
            return proxy.getTaskWithID_JEDI(jediTaskID, fullFlag, lockTask, pid, lockInterval, clearError)

    # update input files stage-in done (according to message from iDDS, called by other methods, etc.)
    def updateInputFilesStaged_JEDI(
        self,
        jeditaskid: int,
        scope: str | None,
        filenames_dict: dict[str, tuple[int | None, int | None]],
        chunk_size: int = 500,
        by: str | None = None,
        check_scope: bool = True,
    ) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.updateInputFilesStaged_JEDI(jeditaskid, scope, filenames_dict, chunk_size, by, check_scope)

    # ==== Data Carousel functions =============================

    # query data carousel request ID by dataset
    def get_data_carousel_request_id_by_dataset_JEDI(self, dataset: str) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_data_carousel_request_id_by_dataset_JEDI(dataset)

    # insert data carousel requests
    def insert_data_carousel_requests_JEDI(self, task_id: int, dc_req_specs: list[DataCarouselRequestSpec]) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.insert_data_carousel_requests_JEDI(task_id, dc_req_specs)

    # update a data carousel request
    def update_data_carousel_request_JEDI(self, dc_req_spec: DataCarouselRequestSpec) -> DataCarouselRequestSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.update_data_carousel_request_JEDI(dc_req_spec)

    # insert data carousel relations
    def insert_data_carousel_relations_JEDI(self, task_id: int, request_ids: list[int]) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.insert_data_carousel_relations_JEDI(task_id, request_ids)

    # get data carousel queued requests and info of their related tasks
    def get_data_carousel_queued_requests_JEDI(self) -> list[tuple[DataCarouselRequestSpec, list[JediTaskSpec]]] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_data_carousel_queued_requests_JEDI()

    # get data carousel requests of tasks by task status
    def get_data_carousel_requests_by_task_status_JEDI(
        self, status_filter_list: list[str] | None = None, status_exclusion_list: list[str] | None = None
    ) -> tuple[dict[Any, DataCarouselRequestSpec], dict[Any, list[Any]]] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_data_carousel_requests_by_task_status_JEDI(status_filter_list=status_filter_list, status_exclusion_list=status_exclusion_list)

    # get related tasks and their info of a data carousel request
    def get_related_tasks_of_data_carousel_request_JEDI(
        self, request_id: int, status_filter_list: list[str] | None = None, status_exclusion_list: list[str] | None = None
    ) -> dict[int, Any] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_related_tasks_of_data_carousel_request_JEDI(
                request_id, status_filter_list=status_filter_list, status_exclusion_list=status_exclusion_list
            )

    # get data carousel staging requests
    def get_data_carousel_staging_requests_JEDI(self, time_limit_minutes: int = 5) -> list[DataCarouselRequestSpec] | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_data_carousel_staging_requests_JEDI(time_limit_minutes=time_limit_minutes)

    # delete data carousel requests
    def delete_data_carousel_requests_JEDI(self, request_id_list: list[int]) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.delete_data_carousel_requests_JEDI(request_id_list)

    # clean up data carousel requests
    def clean_up_data_carousel_requests_JEDI(self, time_limit_days: int = 30) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.clean_up_data_carousel_requests_JEDI(time_limit_days)

    # cancel a data carousel request
    def cancel_data_carousel_request_JEDI(self, request_id: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.cancel_data_carousel_request_JEDI(request_id)

    # retire a data carousel request
    def retire_data_carousel_request_JEDI(self, request_id: int) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.retire_data_carousel_request_JEDI(request_id)

    # resubmit a data carousel request
    def resubmit_data_carousel_request_JEDI(self, request_id: int, exclude_prev_dst: bool = False) -> DataCarouselRequestSpec | bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.resubmit_data_carousel_request_JEDI(request_id, exclude_prev_dst)

    # ==== Workflow functions ==================================

    def get_workflow(self, workflow_id: int) -> WorkflowSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_workflow(workflow_id)

    def get_child_workflows(self, parent_id: int) -> list[WorkflowSpec]:
        with self.proxyPool.get() as proxy:
            return proxy.get_child_workflows(parent_id)

    def get_workflow_step(self, step_id: int) -> WFStepSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_workflow_step(step_id)

    def get_workflow_data(self, data_id: int) -> WFDataSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_workflow_data(data_id)

    def get_workflow_data_by_name(self, name: str, workflow_id: int | None = None) -> WFDataSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.get_workflow_data_by_name(name, workflow_id)

    def get_steps_of_workflow(
        self, workflow_id: int, status_filter_list: list[str] | None = None, status_exclusion_list: list[str] | None = None
    ) -> list[WFStepSpec]:
        with self.proxyPool.get() as proxy:
            return proxy.get_steps_of_workflow(workflow_id, status_filter_list, status_exclusion_list)

    def get_data_of_workflow(
        self,
        workflow_id: int,
        status_filter_list: list[str] | None = None,
        status_exclusion_list: list[str] | None = None,
        type_filter_list: list[str] | None = None,
    ) -> list[WFDataSpec]:
        with self.proxyPool.get() as proxy:
            return proxy.get_data_of_workflow(workflow_id, status_filter_list, status_exclusion_list, type_filter_list)

    def query_workflows(
        self, status_filter_list: list[str] | None = None, status_exclusion_list: list[str] | None = None, check_interval_sec: int = 300
    ) -> list[WorkflowSpec]:
        with self.proxyPool.get() as proxy:
            return proxy.query_workflows(status_filter_list, status_exclusion_list, check_interval_sec)

    def lock_workflow(self, workflow_id: int, locked_by: str, lock_expiration_sec: int = 120) -> bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.lock_workflow(workflow_id, locked_by, lock_expiration_sec)

    def unlock_workflow(self, workflow_id: int, locked_by: str) -> bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.unlock_workflow(workflow_id, locked_by)

    def lock_workflow_step(self, step_id: int, locked_by: str, lock_expiration_sec: int = 120) -> bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.lock_workflow_step(step_id, locked_by, lock_expiration_sec)

    def unlock_workflow_step(self, step_id: int, locked_by: str) -> bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.unlock_workflow_step(step_id, locked_by)

    def lock_workflow_data(self, data_id: int, locked_by: str, lock_expiration_sec: int = 120) -> bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.lock_workflow_data(data_id, locked_by, lock_expiration_sec)

    def unlock_workflow_data(self, data_id: int, locked_by: str) -> bool | None:
        with self.proxyPool.get() as proxy:
            return proxy.unlock_workflow_data(data_id, locked_by)

    def insert_workflow(self, workflow_spec: WorkflowSpec) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.insert_workflow(workflow_spec)

    def insert_workflow_step(self, wf_step_spec: WFStepSpec) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.insert_workflow_step(wf_step_spec)

    def insert_workflow_data(self, wf_data_spec: WFDataSpec) -> int | None:
        with self.proxyPool.get() as proxy:
            return proxy.insert_workflow_data(wf_data_spec)

    def update_workflow(self, workflow_spec: WorkflowSpec) -> WorkflowSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.update_workflow(workflow_spec)

    def update_workflow_step(self, wf_step_spec: WFStepSpec) -> WFStepSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.update_workflow_step(wf_step_spec)

    def update_workflow_data(self, wf_data_spec: WFDataSpec) -> WFDataSpec | None:
        with self.proxyPool.get() as proxy:
            return proxy.update_workflow_data(wf_data_spec)

    def upsert_workflow_entities(
        self,
        workflow_id: int | None,
        actions_dict: dict[str, str | None] | None = None,
        workflow_spec: WorkflowSpec | None = None,
        step_specs: list[WFStepSpec] | None = None,
        data_specs: list[WFDataSpec] | None = None,
    ) -> dict[str, int | None] | None:
        with self.proxyPool.get() as proxy:
            return proxy.upsert_workflow_entities(workflow_id, actions_dict, workflow_spec, step_specs, data_specs)

    def get_distinct_resource_types_per_site(self, jedi_task_id: int, threshold: float = 20.0) -> dict[str, set[str]]:
        with self.proxyPool.get() as proxy:
            ret = proxy.get_distinct_resource_types_per_site(jedi_task_id, threshold)
        return ret

    # ==========================================================


# Singleton
taskBuffer = TaskBuffer()
