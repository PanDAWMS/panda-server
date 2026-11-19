import datetime
import json
import re
import time
import traceback
import types
from collections.abc import Generator
from contextlib import contextmanager
from threading import Lock
from typing import TYPE_CHECKING, Any, Collection, Sequence

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.closer import Closer
from pandaserver.dataservice.setupper import Setupper
from pandaserver.srvcore import CoreUtils, task_command_log
from pandaserver.taskbuffer import ErrorCode, EventServiceUtils, JobUtils, OraDBProxy, ProcessGroups
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from pandaserver.taskbuffer.db_proxy_mods.base_module import get_db_error_code
from pandaserver.taskbuffer.DBProxyPool import DBProxyPool
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.offline_run_script import generate_offline_run_script

_logger = PandaLogger().getLogger("TaskBuffer")

if TYPE_CHECKING:
    # For a type checker TaskBuffer has the DBProxy API. At run time a DBProxy method that
    # TaskBuffer does not define itself is forwarded through the pool by TaskBuffer.__getattr__
    _DBProxyAPI = OraDBProxy.DBProxy
else:
    _DBProxyAPI = object


class TaskBuffer(_DBProxyAPI):
    """
    Access to the PanDA database through a pool of DB proxies

    Every public DBProxy method is a TaskBuffer method too: one that TaskBuffer does not define is
    forwarded by __getattr__, which takes a proxy from the pool for the call, as each hand-written
    delegation did. TaskBuffer defines a method itself only where it does more than that, for
    example when it reads state of the calling process (the task command key, the site mapper),
    which a DBProxy run by ConBridge in a child process cannot see, or combines several calls.
    """

    # The proxy pool is installed by init() before any query runs. It is declared
    # non-Optional on purpose, for the same reason as BaseModule.conn/cur: typing it
    # as Optional would only push a None check onto each call site, here and in __getattr__,
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
        self.last_update_site_mapper: datetime.datetime | None = None

    def __repr__(self) -> str:
        return "TaskBuffer"

    if not TYPE_CHECKING:
        # Hidden from type checkers, which see the DBProxy base instead and so still report a
        # misspelt name. Python calls this only for a name the instance and its class lack. The name
        # is looked up on the DBProxy class with the rule ConBridge uses for its child, never on the
        # instance, so an attribute not yet set (proxyPool before init()) is an AttributeError
        # rather than a recursion
        def __getattr__(self, name: str) -> Any:
            if name.startswith("_") or not isinstance(getattr(OraDBProxy.DBProxy, name, None), (types.MethodType, types.FunctionType)):
                raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

            def forward(*args: Any, **kwargs: Any) -> Any:
                with self.proxyPool.get() as proxy:
                    return getattr(proxy, name)(*args, **kwargs)

            forward.__name__ = name
            forward.__qualname__ = f"{type(self).__name__}.{name}"
            return forward

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
    def transaction(self, name: str | None = None, tmp_log: LogWrapper | None = None) -> Generator[tuple[Any, LogWrapper], None, None]:
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

    # get a DB configuration value
    def getConfigValue(self, component: str, key: str, app: str = "pandaserver", vo: str | None = None, default: Any = None) -> Any:
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getConfigValue(component, key, app, vo)
            if res is None and default is not None:
                res = default
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
            for idxJob, job in enumerate(jobs):
                # update DB
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
                    ret = proxy.archiveJob(job, inJobsDefined, extraInfo=extraInfo, async_params=async_params)
                    if async_params is not None and ret:
                        proxy.async_update_datasets(job.PandaID)  # type: ignore[arg-type]  # the id is a column, which is declared optional
                else:
                    ret = proxy.updateJob(job, inJobsDefined, oldJobStatus=oldJobStatus, extraInfo=extraInfo)
                returns.append(ret)
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
                returns.append(ret)
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
        container_name: str | None = None,
    ) -> list[Any]:
        # get DBproxy
        with self.proxyPool.get() as proxy:
            # get waiting jobs
            t_before = time.time()
            jobs, nSent = proxy.get_jobs_db(
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
                container_name,
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
        report_errors: bool = False,
    ) -> list[Any]:
        """
        Get jobs from the live tables, or else from the archive within the last days.

        Args:
            jobIDs(list): PandaIDs of the jobs
            fromDefined, fromActive, fromArchived, fromWaiting(bool): live tables to look in
            forAnal(bool): give None rather than a job with jobStatus unknown when a live table cannot be read
            days(int): how many days back the archive is looked in
            use_json(bool): give each job as a dict rather than a JobSpec
            report_errors(bool): give {"error": <code>} for a job a database error kept from being read, rather
                than None as for a job not found. The live tables are then tried once, not three times. A job a
                live table could not be read for is not looked for in the archive, since it may be in that table

        Returns:
            list: a job, None or, with report_errors, {"error": <code>} for each PandaID, in order
        """
        retJobMap: dict[int | str, JobSpec | dict[str, str] | None] = {}

        # peek at job
        for jobID in jobIDs:
            # get DBproxy for each job to avoid occupying connection for long time
            with self.proxyPool.get() as proxy:
                # peek job
                res: JobSpec | dict[str, str] | None
                try:
                    res = proxy.peekJob(jobID, fromDefined, fromActive, fromArchived, fromWaiting, forAnal, raise_error=report_errors)
                except Exception as e:
                    if not report_errors:
                        raise
                    res = {"error": get_db_error_code(e)}
                retJobMap[jobID] = res

        # get IDs
        for jobID in jobIDs:
            if retJobMap[jobID] is None:
                # get ArchiveDBproxy
                with self.proxyPool.get() as proxy:
                    # peek job
                    try:
                        res = proxy.peekJobLog(jobID, days, raise_error=report_errors)
                    except Exception as e:
                        if not report_errors:
                            raise
                        res = {"error": get_db_error_code(e)}
                    retJobMap[jobID] = res

        # sort
        retJobs: list[Any] = []
        for jobID in jobIDs:
            jobSpec = retJobMap[jobID]
            if use_json and isinstance(jobSpec, JobSpec):
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
                # archive, rather than kill, event service jobs that have started events
                toKill = True
                if "keepUnmerged" in killOptions:
                    tmpJobSpec = proxy.peekJob(id, True, True, False, False, False)
                    if tmpJobSpec is not None:
                        if EventServiceUtils.isEventServiceJob(tmpJobSpec) and not EventServiceUtils.isJobCloningJob(tmpJobSpec):
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

    # update dataset
    def updateDatasets(self, datasets: list[DatasetSpec], withLock: bool = False, withCriteria: str = "", criteriaMap: dict[str, Any] = {}) -> list[int]:
        with self.proxyPool.get() as proxy:
            # update Dataset
            retList = proxy.updateDataset(datasets, withLock, withCriteria, criteriaMap)
        return retList

    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self) -> dict[Any, Any]:
        with self.proxyPool.get() as proxy:
            # get serial number
            ret = proxy.getJobStatisticsPerProcessingType()
        return ret

    # async request methods

    # query an SQL
    def querySQL(self, sql: str, varMap: dict[str, Any], arraySize: int = 1000) -> Any:
        # get DBproxy
        with self.proxyPool.get() as proxy:
            # get
            ret = proxy.querySQLS(sql, varMap, arraySize)[1]
        return ret

    # queue the task of a workflow step
    def insert_step_task(self, task_params_map: dict[str, Any], user_dn: str, parent_tid: int | None = None) -> tuple[int | None, str]:
        # query an SQL return Status
        with self.proxyPool.get() as proxy:
            # check user status, as insertTaskParamsPanda does at this layer
            tmp_status = proxy.checkBanUser(user_dn, None, True)
            if tmp_status is True:
                ret = proxy.insert_step_task(task_params_map, user_dn, parent_tid)
            elif tmp_status == 1:
                ret = None, "Failed to update DN in PandaDB"
            elif tmp_status == 2:
                ret = None, "Failed to insert user info to PandaDB"
            else:
                ret = None, f"The following DN is banned: DN={user_dn}"
        return ret

    # get the working group of a submitter from their FQANs
    def get_working_group(self, fqans: list[str]) -> str | None:
        with self.proxyPool.get() as proxy:
            return proxy.getWorkingGroup(fqans)

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
                ret = proxy.insert_task_params_panda_db(
                    taskParams,
                    user,
                    prodRole,
                    fqans,
                    parent_tid,
                    properErrorCode,
                    allowActiveTask,
                    decode,
                    command_key=task_command_log.current_task_command.get(),
                )
            elif tmpStatus == 1:
                ret = False, "Failed to update DN in PandaDB"
            elif tmpStatus == 2:
                ret = False, "Failed to insert user info to PandaDB"
            else:
                ret = False, f"The following DN is banned: DN={user}"
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
            ret = proxy.send_command_task_panda_db(
                jediTaskID,
                dn,
                prodRole,
                comStr,
                comComment,
                useCommit,
                properErrorCode,
                comQualifier,
                broadcast,
                command_key=task_command_log.current_task_command.get(),
            )
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

    def getTaskParamsMap(self, jediTaskID: int) -> str:
        """
        Return the taskParamsMap
        """
        # get DB proxy
        with self.proxyPool.get() as proxy:
            # exec
            res = proxy.getTaskParamsPanda(jediTaskID)
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

    # ==== JEDI taskbuffer functions ===========================

    # ==== Data Carousel functions =============================

    # ==== Workflow functions ==================================

    # ==========================================================


# Singleton
taskBuffer = TaskBuffer()
