import copy
import datetime
import random
import re
import time
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils, srv_msg_utils
from pandaserver.taskbuffer import (
    ErrorCode,
    EventServiceUtils,
    JobUtils,
    PrioUtil,
    SupErrors,
)
from pandaserver.taskbuffer.db_proxy_mods.base_module import (
    BaseModule,
    SQL_QUEUE_TOPIC_async_dataset_update,
    varNUMBER,
)
from pandaserver.taskbuffer.db_proxy_mods.entity_module import get_entity_module
from pandaserver.taskbuffer.db_proxy_mods.metrics_module import get_metrics_module
from pandaserver.taskbuffer.db_proxy_mods.task_event_module import get_task_event_module
from pandaserver.taskbuffer.db_proxy_mods.worker_module import get_worker_module
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec, get_task_queued_time


# Module class to define job-related methods that use another module's methods or serve as their dependencies
class JobComplexModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # update Job status in jobsActive
    def updateJobStatus(self, pandaID, jobStatus, param, updateStateChange=False, attemptNr=None):
        comment = " /* DBProxy.updateJobStatus */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug(f"attemptNr={attemptNr} status={jobStatus}")
        sql0 = "SELECT commandToPilot,endTime,specialHandling,jobStatus,computingSite,cloud,prodSourceLabel,lockedby,jediTaskID,"
        sql0 += "jobsetID,jobDispatcherErrorDiag,supErrorCode,eventService,batchID "
        sql0 += "FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
        varMap0 = {}
        varMap0[":PandaID"] = pandaID
        sql1 = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:jobStatus"
        varMap = {}
        presetEndTime = False
        for key in list(param):
            if key in ["corruptedFiles"]:
                continue
            if param[key] is not None or key in ["jobDispatcherErrorDiag"]:
                param[key] = JobSpec.truncateStringAttr(key, param[key])
                sql1 += f",{key}=:{key}"
                varMap[f":{key}"] = param[key]
                if key == "endTime":
                    presetEndTime = True
                try:
                    # store positive error code even for pilot retry
                    if key == "pilotErrorCode" and param[key].startswith("-"):
                        varMap[f":{key}"] = param[key][1:]
                except Exception:
                    pass
            if key == "jobMetrics":
                # extract the memory leak from the pilot jobMetrics
                try:
                    tmpM = re.search("leak=(-?\d+\.*\d+)", param[key])
                    if tmpM is not None:
                        memoryLeak = int(float(tmpM.group(1)))
                        tmpKey = "memory_leak"
                        sql1 += ",{0}=:{0}".format(tmpKey)
                        varMap[f":{tmpKey}"] = memoryLeak
                except Exception:
                    pass

                # extract the chi2 measurement for the memory leak fitting
                try:
                    tmpM = re.search("chi2=(-?\d+\.*\d+)", param[key])
                    if tmpM is not None:
                        # keep measurement under 11 digits because of DB declaration
                        memory_leak_x2 = min(float(tmpM.group(1)), 10**11 - 1)
                        tmpKey = "memory_leak_x2"
                        sql1 += ",{0}=:{0}".format(tmpKey)
                        varMap[f":{tmpKey}"] = memory_leak_x2
                except Exception:
                    pass
        sql1W = " WHERE PandaID=:PandaID "
        varMap[":PandaID"] = pandaID
        if attemptNr is not None:
            sql0 += "AND attemptNr=:attemptNr "
            sql1W += "AND attemptNr=:attemptNr "
            varMap[":attemptNr"] = attemptNr
            varMap0[":attemptNr"] = attemptNr
        # prevent change from holding to transferring which doesn't register files to sub/tid
        if jobStatus == "transferring":
            sql1W += "AND NOT jobStatus=:ngStatus "
            varMap[":ngStatus"] = "holding"
        updatedFlag = False
        action_in_downstream = None
        nTry = 1
        for iTry in range(nTry):
            try:
                # begin transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10
                self.cur.execute(sql0 + comment, varMap0)
                res = self.cur.fetchone()
                if res is not None:
                    ret = ""
                    (
                        commandToPilot,
                        endTime,
                        specialHandling,
                        oldJobStatus,
                        computingSite,
                        cloud,
                        prodSourceLabel,
                        lockedby,
                        jediTaskID,
                        jobsetID,
                        jobDispatcherErrorDiag,
                        supErrorCode,
                        eventService,
                        batchID,
                    ) = res
                    # check debug mode and job cloning with runonce
                    is_job_cloning = False
                    if specialHandling:
                        tmpJobSpec = JobSpec()
                        tmpJobSpec.specialHandling = specialHandling
                        if tmpJobSpec.is_debug_mode():
                            ret += "debug,"
                        if EventServiceUtils.getJobCloningType(tmpJobSpec) == "runonce":
                            is_job_cloning = True
                    # FIXME
                    # else:
                    #    ret += 'debugoff,'
                    # kill command
                    if commandToPilot not in [None, ""]:
                        # soft kill
                        if supErrorCode in [ErrorCode.EC_EventServicePreemption]:
                            # commandToPilot = 'softkill'
                            pass
                        ret += f"{commandToPilot},"
                    ret = ret[:-1]
                    # convert empty to NULL
                    if ret == "":
                        ret = "NULL"
                    if oldJobStatus == "failed" and jobStatus in [
                        "holding",
                        "transferring",
                        "starting",
                        "running",
                    ]:
                        tmp_log.debug(f"skip to set {jobStatus} since it is already {oldJobStatus}")
                        ret = "alreadydone"
                    elif oldJobStatus == "transferring" and jobStatus == "holding" and jobDispatcherErrorDiag in [None, ""]:
                        # skip transferring -> holding
                        tmp_log.debug("skip to set holding since it is alredy in transferring")
                        ret = "alreadydone"
                    elif (
                        oldJobStatus == "holding"
                        and jobStatus == "holding"
                        and ("jobDispatcherErrorDiag" not in param or param["jobDispatcherErrorDiag"] not in [None, ""])
                    ):
                        # just ignore hearbeats for job recovery
                        tmp_log.debug("skip to reset holding")
                    elif (
                        oldJobStatus == "holding"
                        and jobStatus == "holding"
                        and jobDispatcherErrorDiag in [None, ""]
                        and "jobDispatcherErrorDiag" in param
                        and param["jobDispatcherErrorDiag"] in [None, ""]
                    ):
                        # special return to avoid duplicated XMLs
                        tmp_log.debug("skip to set holding since it was already set to holding by the final heartbeat")
                        ret = "alreadydone"
                    elif oldJobStatus == "merging":
                        # don't update merging
                        tmp_log.debug("skip to change from merging")
                    elif oldJobStatus in ["holding", "transferring"] and jobStatus == "starting":
                        # don't update holding
                        tmp_log.debug(f"skip to change {oldJobStatus} to {jobStatus} to avoid inconsistency")
                    elif oldJobStatus == "holding" and jobStatus == "running":
                        # don't update holding
                        tmp_log.debug(f"skip to change {oldJobStatus} to {jobStatus} not to return to active")
                    elif (
                        batchID not in ["", None]
                        and "batchID" in param
                        and param["batchID"] not in ["", None]
                        and batchID != param["batchID"]
                        and re.search("^\d+\.*\d+$", batchID) is None
                        and re.search("^\d+\.*\d+$", param["batchID"]) is None
                    ):
                        # invalid batchID
                        tmp_log.debug(
                            "to be killed since batchID mismatch old {} in {} vs new {} in {}".format(
                                batchID.replace("\n", ""),
                                oldJobStatus,
                                param["batchID"].replace("\n", ""),
                                jobStatus,
                            )
                        )
                        ret = "tobekilled"
                        # set supErrorCode and supErrorDiag
                        varMap = {}
                        varMap[":PandaID"] = pandaID
                        varMap[":code"] = SupErrors.error_codes["INVALID_BATCH_ID"]
                        clean_batch_id = param["batchID"].replace("\n", "")
                        varMap[":diag"] = f"got an update request with invalid batchID={clean_batch_id}"
                        varMap[":diag"] = JobSpec.truncateStringAttr("supErrorDiag", varMap[":diag"])
                        sqlSUP = "UPDATE ATLAS_PANDA.jobsActive4 SET supErrorCode=:code,supErrorDiag=:diag "
                        sqlSUP += "WHERE PandaID=:PandaID "
                        self.cur.execute(sqlSUP + comment, varMap)
                    else:
                        # change starting to running
                        if oldJobStatus == "running" and jobStatus == "starting":
                            tmp_log.debug(f"changed to {oldJobStatus} from {jobStatus} to avoid inconsistent update")
                            jobStatus = oldJobStatus
                        # update stateChangeTime
                        if updateStateChange or (jobStatus != oldJobStatus):
                            sql1 += ",stateChangeTime=CURRENT_DATE"
                        # set endTime if undefined for holding
                        if (jobStatus == "holding" or (jobStatus == "transferring" and oldJobStatus == "running")) and endTime is None and not presetEndTime:
                            sql1 += ",endTime=CURRENT_DATE "
                        # update startTime
                        if oldJobStatus in ["sent", "starting"] and jobStatus == "running" and ":startTime" not in varMap:
                            sql1 += ",startTime=CURRENT_DATE"
                        # update modification time
                        sql1 += ",modificationTime=CURRENT_DATE"
                        # update
                        varMap[":jobStatus"] = jobStatus
                        self.cur.execute(sql1 + sql1W + comment, varMap)
                        nUp = self.cur.rowcount
                        tmp_log.debug(f"attemptNr={attemptNr} nUp={nUp} old={oldJobStatus} new={jobStatus}")
                        if nUp == 1:
                            updatedFlag = True
                        if nUp == 0 and jobStatus == "transferring":
                            tmp_log.debug("ignore to update for transferring")
                        # update waiting ES jobs not to get reassigned
                        if updatedFlag and EventServiceUtils.isEventServiceSH(specialHandling):
                            # sql to update ES jobs
                            sqlUEA = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
                            sqlUEA += "WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID AND jobStatus=:jobStatus "
                            sqlUEL = "SELECT modificationTime FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
                            sqlUEL += "FOR UPDATE NOWAIT "
                            sqlUE = "UPDATE ATLAS_PANDA.jobsActive4 SET modificationTime=CURRENT_DATE "
                            sqlUE += "WHERE PandaID=:PandaID "
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":jobsetID"] = jobsetID
                            varMap[":jobStatus"] = "activated"
                            self.cur.execute(sqlUEA + comment, varMap)
                            resUEA = self.cur.fetchall()
                            nUE = 0
                            for (ueaPandaID,) in resUEA:
                                varMap = {}
                                varMap[":PandaID"] = ueaPandaID
                                try:
                                    # lock with NOWAIT
                                    self.cur.execute(sqlUEL + comment, varMap)
                                    resUEL = self.cur.fetchone()
                                    if resUEL is None:
                                        continue
                                except Exception:
                                    tmp_log.debug(f"skip to update associated ES={ueaPandaID}")
                                    continue
                                self.cur.execute(sqlUE + comment, varMap)
                                nUE += self.cur.rowcount
                            tmp_log.debug(f"updated {nUE} ES jobs")
                        # update fake co-jumbo jobs
                        if updatedFlag and eventService == EventServiceUtils.jumboJobFlagNumber:
                            # sql to update fake co-jumbo
                            sqlIFL = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
                            sqlIFL += "WHERE jediTaskID=:jediTaskID AND eventService=:eventService AND jobStatus=:jobStatus "
                            sqlIFL += "FOR UPDATE NOWAIT "
                            sqlIF = "UPDATE ATLAS_PANDA.jobsDefined4 SET modificationTime=CURRENT_DATE "
                            sqlIF += "WHERE jediTaskID=:jediTaskID AND eventService=:eventService AND jobStatus=:jobStatus "
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":eventService"] = EventServiceUtils.coJumboJobFlagNumber
                            varMap[":jobStatus"] = "waiting"
                            try:
                                # lock with NOWAIT
                                self.cur.execute(sqlIFL + comment, varMap)
                                resIFL = self.cur.fetchall()
                                self.cur.execute(sqlIF + comment, varMap)
                                nUE = self.cur.rowcount
                                tmp_log.debug(f"updated {nUE} fake co-jumbo jobs")
                            except Exception:
                                tmp_log.debug("skip to update fake co-jumbo jobs")
                        # update nFilesOnHold for JEDI RW calculation
                        if (
                            updatedFlag
                            and jobStatus == "transferring"
                            and oldJobStatus == "holding"
                            and hasattr(panda_config, "useJEDI")
                            and panda_config.useJEDI is True
                            and lockedby == "jedi"
                            and get_task_event_module(self).checkTaskStatusJEDI(jediTaskID, self.cur)
                        ):
                            # SQL to get file list from Panda
                            sqlJediFP = "SELECT datasetID,fileID,attemptNr FROM ATLAS_PANDA.filesTable4 "
                            sqlJediFP += "WHERE PandaID=:pandaID AND type IN (:type1,:type2) ORDER BY datasetID,fileID "
                            # SQL to check JEDI files
                            sqlJediFJ = "SELECT /*+ INDEX_RS_ASC(JEDI_DATASET_CONTENTS (JEDI_DATASET_CONTENTS.JEDITASKID JEDI_DATASET_CONTENTS.DATASETID JEDI_DATASET_CONTENTS.FILEID)) */ 1 FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                            sqlJediFJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                            sqlJediFJ += "AND attemptNr=:attemptNr AND status=:status AND keepTrack=:keepTrack "
                            # get file list
                            varMap = {}
                            varMap[":pandaID"] = pandaID
                            varMap[":type1"] = "input"
                            varMap[":type2"] = "pseudo_input"
                            self.cur.arraysize = 100000
                            self.cur.execute(sqlJediFP + comment, varMap)
                            resJediFile = self.cur.fetchall()
                            datasetContentsStat = {}
                            # loop over all files
                            for tmpDatasetID, tmpFileID, tmpAttemptNr in resJediFile:
                                # check file in JEDI
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = tmpDatasetID
                                varMap[":fileID"] = tmpFileID
                                varMap[":attemptNr"] = tmpAttemptNr
                                varMap[":status"] = "running"
                                varMap[":keepTrack"] = 1
                                self.cur.execute(sqlJediFJ + comment, varMap)
                                res = self.cur.fetchone()
                                if res is not None:
                                    if tmpDatasetID not in datasetContentsStat:
                                        datasetContentsStat[tmpDatasetID] = 0
                                    if jobStatus == "transferring":
                                        # increment nOnHold
                                        datasetContentsStat[tmpDatasetID] += 1
                                    else:
                                        # decrement nOnHold
                                        datasetContentsStat[tmpDatasetID] -= 1
                            # loop over all datasets
                            tmpDatasetIDs = sorted(datasetContentsStat)
                            for tmpDatasetID in tmpDatasetIDs:
                                diffNum = datasetContentsStat[tmpDatasetID]
                                # no difference
                                if diffNum == 0:
                                    continue
                                # SQL to lock
                                sqlJediDL = "SELECT nFilesOnHold FROM ATLAS_PANDA.JEDI_Datasets "
                                sqlJediDL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                                sqlJediDL += "FOR UPDATE NOWAIT "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = tmpDatasetID
                                tmp_log.debug(sqlJediDL + comment + str(varMap))
                                self.cur.execute(sqlJediDL + comment, varMap)
                                # SQL to update
                                sqlJediDU = "UPDATE ATLAS_PANDA.JEDI_Datasets SET "
                                if diffNum > 0:
                                    sqlJediDU += "nFilesOnHold=nFilesOnHold+:diffNum "
                                else:
                                    sqlJediDU += "nFilesOnHold=nFilesOnHold-:diffNum "
                                sqlJediDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                                sqlJediDU += "AND NOT type IN (:ngType1,:ngType2) "
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = tmpDatasetID
                                varMap[":diffNum"] = abs(diffNum)
                                varMap[":ngType1"] = "trn_log"
                                varMap[":ngType2"] = "trn_output"
                                tmp_log.debug(sqlJediDU + comment + str(varMap))
                                self.cur.execute(sqlJediDU + comment, varMap)
                        # first transition to running
                        if oldJobStatus in ("starting", "sent") and jobStatus == "running":
                            # update lastStart
                            sql_last_start_lock = (
                                "SELECT lastStart FROM ATLAS_PANDAMETA.siteData "
                                "WHERE site=:site AND hours=:hours AND flag IN (:flag1,:flag2) "
                                "FOR UPDATE NOWAIT "
                            )
                            sqlLS = "UPDATE ATLAS_PANDAMETA.siteData SET lastStart=CURRENT_DATE "
                            sqlLS += "WHERE site=:site AND hours=:hours AND flag IN (:flag1,:flag2) "
                            varMap = {}
                            varMap[":site"] = computingSite
                            varMap[":hours"] = 3
                            varMap[":flag1"] = "production"
                            varMap[":flag2"] = "analysis"
                            try:
                                self.cur.execute(sql_last_start_lock + comment, varMap)
                                self.cur.execute(sqlLS + comment, varMap)
                                tmp_log.debug("updated lastStart")
                            except Exception:
                                tmp_log.debug("skip to update lastStart")
                            # record queuing period
                            if jediTaskID and get_task_queued_time(specialHandling):
                                tmp_success = get_metrics_module(self).record_job_queuing_period(pandaID)
                                if tmp_success is True:
                                    tmp_log.debug("recorded queuing period")
                        # update input
                        if updatedFlag and jediTaskID is not None and jobStatus == "running" and oldJobStatus != jobStatus:
                            get_task_event_module(self).updateInputStatusJedi(jediTaskID, pandaID, jobStatus)
                        # register corrupted zip files
                        if updatedFlag and "corruptedFiles" in param and eventService == EventServiceUtils.esMergeJobFlagNumber:
                            # get exiting files
                            sqlCorF = "SELECT lfn FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type "
                            varMap = {}
                            varMap[":PandaID"] = pandaID
                            varMap[":type"] = "zipinput"
                            self.cur.execute(sqlCorF + comment, varMap)
                            resCorF = self.cur.fetchall()
                            exCorFiles = set()
                            for (tmpLFN,) in resCorF:
                                exCorFiles.add(tmpLFN)
                            # register files
                            tmpJobSpec = JobSpec()
                            tmpJobSpec.PandaID = pandaID
                            sqlCorIN = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
                            sqlCorIN += FileSpec.bindValuesExpression(useSeq=True)
                            for tmpLFN in param["corruptedFiles"].split(","):
                                tmpLFN = tmpLFN.strip()
                                if tmpLFN in exCorFiles or tmpLFN == "":
                                    continue
                                tmpFileSpec = FileSpec()
                                tmpFileSpec.jediTaskID = jediTaskID
                                tmpFileSpec.fsize = 0
                                tmpFileSpec.lfn = tmpLFN
                                tmpFileSpec.type = "zipinput"
                                tmpFileSpec.status = "corrupted"
                                tmpJobSpec.addFile(tmpFileSpec)
                                varMap = tmpFileSpec.valuesMap(useSeq=True)
                                self.cur.execute(sqlCorIN + comment, varMap)
                        # add params to execute getEventRanges later
                        if updatedFlag and is_job_cloning and jobStatus == "running" and oldJobStatus in ["sent", "starting"]:
                            action_in_downstream = {"action": "get_event", "pandaID": pandaID, "jobsetID": jobsetID, "jediTaskID": jediTaskID}
                            tmp_log.debug(f'take action={action_in_downstream["action"]} in downstream')
                        # try to update the lastupdate column in the harvester_rel_job_worker table to propagate
                        # changes to ElasticSearch
                        sqlJWU = "UPDATE ATLAS_PANDA.Harvester_Rel_Jobs_Workers SET lastUpdate=:lastUpdate "
                        sqlJWU += "WHERE PandaID=:PandaID "
                        varMap = {
                            ":PandaID": pandaID,
                            ":lastUpdate": naive_utcnow(),
                        }
                        self.cur.execute(sqlJWU + comment, varMap)
                        nRow = self.cur.rowcount
                        tmp_log.debug(f"{nRow} workers updated")

                        try:
                            # try to update the computing element from the harvester worker table
                            sql_ce = """
                                     UPDATE ATLAS_PANDA.jobsActive4
                                     SET computingelement = (SELECT * FROM (
                                       SELECT computingelement FROM ATLAS_PANDA.harvester_workers hw, ATLAS_PANDA.Harvester_Rel_Jobs_Workers hrjw
                                       WHERE hw.workerid = hrjw.workerid AND hw.harvesterid = hrjw.harvesterid AND hrjw.pandaid = :PandaID ORDER BY hw.workerid DESC
                                       ) WHERE rownum=1)
                                     where PandaID=:PandaID
                                     """
                            varMap = {":PandaID": pandaID}
                            self.cur.execute(sql_ce + comment, varMap)
                            nRow = self.cur.rowcount
                            tmp_log.debug(f"succeeded to update CE from harvester table (rowcount={nRow})")
                        except Exception:
                            tmp_log.error(f"updateJobStatus : failed to update CE from harvester table with {traceback.format_exc()}")
                    # push status change
                    self.push_job_status_message(None, pandaID, jobStatus, jediTaskID, specialHandling, extra_data={"computingsite": computingSite})
                else:
                    tmp_log.debug("not found")
                    # already deleted or bad attempt number
                    ret = "tobekilled"
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # record status change
                try:
                    if updatedFlag and oldJobStatus is not None and oldJobStatus != jobStatus:
                        self.recordStatusChange(
                            pandaID,
                            jobStatus,
                            infoMap={
                                "computingSite": computingSite,
                                "cloud": cloud,
                                "prodSourceLabel": prodSourceLabel,
                            },
                        )
                except Exception:
                    tmp_log.error("recordStatusChange in updateJobStatus")
                tmp_log.debug("done")
                return ret, action_in_downstream
            except Exception:
                # roll back
                self._rollback(True)
                if iTry + 1 < nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                # dump error
                self.dump_error_message(tmp_log)
                return False, None

    # update job information in jobsActive or jobsDefined
    def updateJob(self, job, inJobsDefined, oldJobStatus=None, extraInfo=None):
        comment = " /* DBProxy.updateJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        updatedFlag = False
        nTry = 3
        for iTry in range(nTry):
            try:
                job.modificationTime = naive_utcnow()
                # set stateChangeTime for defined->assigned
                if inJobsDefined:
                    job.stateChangeTime = job.modificationTime
                # make SQL
                if inJobsDefined:
                    sql1 = f"UPDATE ATLAS_PANDA.jobsDefined4 SET {job.bindUpdateChangesExpression()} "
                    sql_last_jobstatus = "SELECT jobStatus FROM ATLAS_PANDA.jobsDefined4 "
                else:
                    sql1 = f"UPDATE ATLAS_PANDA.jobsActive4 SET {job.bindUpdateChangesExpression()} "
                    sql_last_jobstatus = "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 "
                sql1 += "WHERE PandaID=:PandaID "
                sql_last_jobstatus += "WHERE PandaID=:PandaID "
                if inJobsDefined:
                    sql1 += " AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) "
                # begin transaction
                self.conn.begin()
                # get jobstatus before update
                varMap = {":PandaID": job.PandaID}
                tmp_log.debug(sql_last_jobstatus + comment + str(varMap))
                self.cur.execute(sql_last_jobstatus + comment, varMap)
                res_last_jobstatus = self.cur.fetchall()
                last_jobstatus = None
                for (js,) in res_last_jobstatus:
                    last_jobstatus = js
                    break
                # update
                varMap = job.valuesMap(onlyChanged=True)
                varMap[":PandaID"] = job.PandaID
                if inJobsDefined:
                    varMap[":oldJobStatus1"] = "assigned"
                    varMap[":oldJobStatus2"] = "defined"
                tmp_log.debug(sql1 + comment + str(varMap))
                self.cur.execute(sql1 + comment, varMap)
                n = self.cur.rowcount
                if n == 0:
                    # already killed or activated
                    tmp_log.debug(f"Not found")
                else:
                    # check if JEDI is used
                    useJEDI = False
                    if (
                        oldJobStatus != job.jobStatus
                        and (job.jobStatus in ["transferring", "merging"] or oldJobStatus in ["transferring", "merging"])
                        and hasattr(panda_config, "useJEDI")
                        and panda_config.useJEDI is True
                        and job.lockedby == "jedi"
                        and get_task_event_module(self).checkTaskStatusJEDI(job.jediTaskID, self.cur)
                    ):
                        useJEDI = True
                    # SQL to check JEDI files
                    sqlJediFJ = "SELECT /*+ INDEX_RS_ASC(JEDI_DATASET_CONTENTS (JEDI_DATASET_CONTENTS.JEDITASKID JEDI_DATASET_CONTENTS.DATASETID JEDI_DATASET_CONTENTS.FILEID)) */ 1 FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                    sqlJediFJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    sqlJediFJ += "AND attemptNr=:attemptNr AND status=:status AND keepTrack=:keepTrack "
                    datasetContentsStat = {}
                    # loop over all files
                    for file in job.Files:
                        sqlF = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                        varMap = file.valuesMap(onlyChanged=True)
                        if varMap != {}:
                            varMap[":row_ID"] = file.row_ID
                            tmp_log.debug(sqlF + comment + str(varMap))
                            self.cur.execute(sqlF + comment, varMap)
                        # actions for JEDI
                        if (
                            useJEDI
                            and (job.jobStatus == "transferring" or oldJobStatus == "transferring")
                            and file.type in ["input", "pseudo_input"]
                            and job.processingType != "pmerge"
                        ):
                            # check file in JEDI
                            varMap = {}
                            varMap[":jediTaskID"] = file.jediTaskID
                            varMap[":datasetID"] = file.datasetID
                            varMap[":fileID"] = file.fileID
                            varMap[":attemptNr"] = file.attemptNr
                            varMap[":status"] = "running"
                            varMap[":keepTrack"] = 1
                            self.cur.execute(sqlJediFJ + comment, varMap)
                            res = self.cur.fetchone()
                            if res is not None:
                                if file.datasetID not in datasetContentsStat:
                                    datasetContentsStat[file.datasetID] = {
                                        "diff": 0,
                                        "cType": "hold",
                                    }
                                if job.jobStatus == "transferring":
                                    # increment nOnHold
                                    datasetContentsStat[file.datasetID]["diff"] += 1
                                else:
                                    # decrement nOnHold
                                    datasetContentsStat[file.datasetID]["diff"] -= 1
                        elif useJEDI and job.jobStatus == "merging" and file.type in ["log", "output"] and file.status != "nooutput":
                            # SQL to update JEDI files
                            varMap = {}
                            varMap[":fileID"] = file.fileID
                            varMap[":attemptNr"] = file.attemptNr
                            varMap[":datasetID"] = file.datasetID
                            varMap[":keepTrack"] = 1
                            varMap[":jediTaskID"] = file.jediTaskID
                            varMap[":status"] = "ready"
                            varMap[":boundaryID"] = job.PandaID
                            varMap[":maxAttempt"] = file.attemptNr + 3
                            sqlJFile = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents "
                            sqlJFile += "SET status=:status,boundaryID=:boundaryID,maxAttempt=:maxAttempt"
                            for tmpKey in ["lfn", "GUID", "fsize", "checksum"]:
                                tmpVal = getattr(file, tmpKey)
                                if tmpVal == "NULL":
                                    if tmpKey in file._zeroAttrs:
                                        tmpVal = 0
                                    else:
                                        tmpVal = None
                                tmpMapKey = f":{tmpKey}"
                                sqlJFile += f",{tmpKey}={tmpMapKey}"
                                varMap[tmpMapKey] = tmpVal
                            sqlJFile += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                            sqlJFile += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
                            # update JEDI file
                            tmp_log.debug(sqlJFile + comment + str(varMap))
                            self.cur.execute(sqlJFile + comment, varMap)
                            nRow = self.cur.rowcount
                            if nRow == 1:
                                if file.datasetID not in datasetContentsStat:
                                    datasetContentsStat[file.datasetID] = {
                                        "diff": 0,
                                        "cType": "hold",
                                    }
                                datasetContentsStat[file.datasetID]["diff"] += 1
                        # update metadata in JEDI
                        if useJEDI and file.type in ["output", "log"] and extraInfo is not None:
                            varMap = {}
                            sqlFileMeta = ""
                            if "nevents" in extraInfo and file.lfn in extraInfo["nevents"]:
                                tmpKey = "nEvents"
                                tmpMapKey = f":{tmpKey}"
                                sqlFileMeta += f"{tmpKey}={tmpMapKey},"
                                varMap[tmpMapKey] = extraInfo["nevents"][file.lfn]
                            if "lbnr" in extraInfo and file.lfn in extraInfo["lbnr"]:
                                tmpKey = "lumiBlockNr"
                                tmpMapKey = f":{tmpKey}"
                                sqlFileMeta += f"{tmpKey}={tmpMapKey},"
                                varMap[tmpMapKey] = extraInfo["lbnr"][file.lfn]
                            if varMap != {}:
                                # update
                                varMap[":fileID"] = file.fileID
                                varMap[":attemptNr"] = file.attemptNr
                                varMap[":datasetID"] = file.datasetID
                                varMap[":jediTaskID"] = file.jediTaskID
                                varMap[":keepTrack"] = 1
                                sqlFileMeta = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET " + sqlFileMeta
                                sqlFileMeta = sqlFileMeta[:-1]
                                sqlFileMeta += " "
                                sqlFileMeta += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                                sqlFileMeta += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
                                tmp_log.debug(sqlFileMeta + comment + str(varMap))
                                self.cur.execute(sqlFileMeta + comment, varMap)
                    # loop over all JEDI datasets
                    tmpDatasetIDs = sorted(datasetContentsStat)
                    for tmpDatasetID in tmpDatasetIDs:
                        valMap = datasetContentsStat[tmpDatasetID]
                        diffNum = valMap["diff"]
                        cType = valMap["cType"]
                        # no difference
                        if diffNum == 0:
                            continue
                        # SQL to check lock
                        varMap = {}
                        varMap[":jediTaskID"] = job.jediTaskID
                        varMap[":datasetID"] = tmpDatasetID
                        sqlJediCL = "SELECT nFilesTobeUsed,nFilesOnHold,status FROM ATLAS_PANDA.JEDI_Datasets "
                        sqlJediCL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                        sqlJediCL += "FOR UPDATE NOWAIT "
                        tmp_log.debug(sqlJediCL + comment + str(varMap))
                        self.cur.execute(sqlJediCL + comment, varMap)
                        # SQL to update dataset
                        varMap = {}
                        varMap[":jediTaskID"] = job.jediTaskID
                        varMap[":datasetID"] = tmpDatasetID
                        varMap[":diffNum"] = abs(diffNum)
                        sqlJediDU = "UPDATE ATLAS_PANDA.JEDI_Datasets SET "
                        if cType == "hold":
                            if diffNum > 0:
                                sqlJediDU += "nFilesOnHold=nFilesOnHold+:diffNum "
                            else:
                                sqlJediDU += "nFilesOnHold=nFilesOnHold-:diffNum "
                        elif cType == "touse":
                            varMap[":status"] = "ready"
                            sqlJediDU += "nFilesTobeUsed=nFilesTobeUsed+:diffNum,status=:status "
                        sqlJediDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                        tmp_log.debug(sqlJediDU + comment + str(varMap))
                        self.cur.execute(sqlJediDU + comment, varMap)
                    # update job parameters
                    sqlJobP = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[":PandaID"] = job.PandaID
                    varMap[":param"] = job.jobParameters
                    self.cur.execute(sqlJobP + comment, varMap)
                    updatedFlag = True
                    # update input
                    if useJEDI and job.jobStatus in ["transferring"]:
                        get_task_event_module(self).updateInputStatusJedi(job.jediTaskID, job.PandaID, job.jobStatus)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # record status change
                try:
                    if updatedFlag and job.jobStatus != last_jobstatus:
                        self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
                        self.push_job_status_message(job, job.PandaID, job.jobStatus)
                except Exception:
                    tmp_log.error("recordStatusChange in updateJob")
                return True
            except Exception:
                # roll back
                self._rollback(True)
                if iTry + 1 < nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(3, 10))
                    continue
                self.dump_error_message(tmp_log)
                return False

    # cleanup jumbo jobs
    def cleanupJumboJobs(self, jediTaskID=None):
        comment = " /* DBProxy.cleanupJumboJobs */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            # sql to get jumbo jobs
            sql = "SELECT PandaID,jediTaskID,jobStatus FROM ATLAS_PANDA.jobsDefined4 WHERE eventService=:eventService "
            if jediTaskID is not None:
                sql += "AND jediTaskID=:jediTaskID "
            sql += "UNION "
            sql += "SELECT PandaID,jediTaskID,jobStatus FROM ATLAS_PANDA.jobsActive4 WHERE eventService=:eventService "
            if jediTaskID is not None:
                sql += "AND jediTaskID=:jediTaskID "
            # begin transaction
            self.conn.begin()
            # get jobs
            varMap = {}
            varMap[":eventService"] = EventServiceUtils.jumboJobFlagNumber
            self.cur.execute(sql + comment, varMap)
            resF = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # get ID mapping
            idMap = {}
            for pandaID, tmpJediTaskID, jobStatus in resF:
                if jobStatus in ["transferring", "running", "holding"]:
                    continue
                if tmpJediTaskID not in idMap:
                    idMap[tmpJediTaskID] = set()
                idMap[tmpJediTaskID].add(pandaID)
            tmp_log.debug(f"got {len(idMap)} tasks")
            # sql to check useJumbo
            sqlJ = "SELECT useJumbo FROM ATLAS_PANDA.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            # loop over all tasks
            for tmpJediTaskID in idMap:
                pandaIDs = idMap[tmpJediTaskID]
                # check useJumbo
                self.conn.begin()
                varMap = {}
                varMap[":jediTaskID"] = tmpJediTaskID
                self.cur.execute(sqlJ + comment, varMap)
                resJ = self.cur.fetchone()
                if resJ is not None and resJ[0] == "D":
                    disabledFlag = True
                    tmp_log.debug(f"kill disabled jumbo jobs for jediTaskID={tmpJediTaskID}")
                else:
                    disabledFlag = False
                if not self._commit():
                    raise RuntimeError("Commit error")
                if jediTaskID is not None or not get_task_event_module(self).isApplicableTaskForJumbo(tmpJediTaskID) or disabledFlag:
                    for pandaID in pandaIDs:
                        self.killJob(pandaID, "", "55", True)
                    tmp_log.debug(f"killed {len(pandaIDs)} jobs for jediTaskID={tmpJediTaskID}")
            tmp_log.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # kill job
    def killJob(
        self,
        pandaID,
        user,
        code,
        prodManager,
        getUserInfo=False,
        wgProdRole=[],
        killOpts=[],
    ):
        # code
        # 2  : expire
        # 3  : aborted
        # 4  : expire in waiting
        # 7  : retry by server
        # 8  : rebrokerage
        # 9  : force kill
        # 10 : fast rebrokerage in overloaded PQ
        # 50 : kill by JEDI
        # 51 : reassigned by JEDI
        # 52 : force kill by JEDI
        # 55 : killed since task is (almost) done
        # 60 : workload was terminated by the pilot without actual work
        # 91 : kill user jobs with prod role
        # 99 : force kill user jobs with prod role
        comment = " /* DBProxy.killJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")

        tmp_log.debug(f"code={code} role={prodManager} user={user} wg={wgProdRole} opts={killOpts}")
        timeStart = naive_utcnow()
        # check PandaID
        try:
            int(pandaID)
        except Exception:
            tmp_log.error(f"not an integer : {pandaID}")
            if getUserInfo:
                return False, {}
            return False

        #  While the code being a number, it's treated as a string in this function.
        if isinstance(code, int):
            code = str(code)

        sql0 = "SELECT prodUserID,prodSourceLabel,jobDefinitionID,jobsetID,workingGroup,specialHandling,jobStatus,taskBufferErrorCode,eventService FROM %s "
        sql0 += "WHERE PandaID=:PandaID "
        sql0 += "FOR UPDATE NOWAIT "
        sql1 = "UPDATE %s SET commandToPilot=:commandToPilot,taskBufferErrorDiag=:taskBufferErrorDiag WHERE PandaID=:PandaID "
        sql1 += "AND (commandToPilot IS NULL OR commandToPilot<>'tobekilled') "
        sql1F = "UPDATE %s SET commandToPilot=:commandToPilot,taskBufferErrorDiag=:taskBufferErrorDiag WHERE PandaID=:PandaID "
        sql2 = f"SELECT {JobSpec.columnNames()} "
        sql2 += "FROM %s WHERE PandaID=:PandaID AND jobStatus<>:jobStatus"
        sql3 = "DELETE FROM %s WHERE PandaID=:PandaID"
        sqlU = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID AND jobStatus IN (:oldJobStatus1,:oldJobStatus2,:oldJobStatus3,:oldJobStatus4) "
        sql4 = f"INSERT INTO ATLAS_PANDA.jobsArchived4 ({JobSpec.columnNames()}) "
        sql4 += JobSpec.bindValuesExpression()
        sqlF = "UPDATE ATLAS_PANDA.filesTable4 SET status=:status WHERE PandaID=:PandaID AND type IN (:type1,:type2)"
        sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
        sqlFile += "WHERE PandaID=:PandaID"
        try:
            flagCommand = False
            flagKilled = False
            userProdUserID = ""
            userProdSourceLabel = ""
            userJobDefinitionID = ""
            userJobsetID = ""
            updatedFlag = False
            # begin transaction
            self.conn.begin()
            for table in (
                "ATLAS_PANDA.jobsDefined4",
                "ATLAS_PANDA.jobsActive4",
            ):
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # begin transaction
                self.conn.begin()
                # get DN if user is not production DN
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.arraysize = 10
                self.cur.execute((sql0 + comment) % table, varMap)
                res = self.cur.fetchone()
                # not found
                if res is None:
                    continue

                # prevent prod proxy from killing analysis jobs
                (
                    userProdUserID,
                    userProdSourceLabel,
                    userJobDefinitionID,
                    userJobsetID,
                    workingGroup,
                    specialHandling,
                    jobStatusInDB,
                    taskBufferErrorCode,
                    eventService,
                ) = res
                # check group prod role
                validGroupProdRole = False
                if res[1] in ["managed", "test"] and workingGroup != "":
                    for tmpGroupProdRole in wgProdRole:
                        if tmpGroupProdRole == "":
                            continue
                        if re.search("(^|_)" + tmpGroupProdRole + "$", workingGroup, re.I) is not None:
                            validGroupProdRole = True
                            break
                if prodManager:
                    if res[1] in ["user", "panda"] and (
                        code
                        not in [
                            "2",
                            "4",
                            "7",
                            "8",
                            "9",
                            "50",
                            "51",
                            "52",
                            "91",
                            "10",
                            "99",
                        ]
                    ):
                        tmp_log.debug(f"ignored -> prod proxy tried to kill analysis job type={res[1]}")
                        break
                    tmp_log.debug("using prod role")
                elif validGroupProdRole:
                    # WGs with prod role
                    tmp_log.debug(f"using group prod role for workingGroup={workingGroup}")
                    pass
                else:
                    cn1 = CoreUtils.clean_user_id(res[0])
                    cn2 = CoreUtils.clean_user_id(user)
                    tmp_log.debug(f"Owner:{cn1} - Requester:{cn2} ")
                    if cn1 != cn2:
                        tmp_log.debug("ignored since Owner != Requester")
                        break
                # event service
                useEventService = EventServiceUtils.isEventServiceSH(specialHandling) or eventService in [
                    EventServiceUtils.jumboJobFlagNumber,
                    EventServiceUtils.coJumboJobFlagNumber,
                ]
                useEventServiceMerge = EventServiceUtils.isEventServiceMergeSH(specialHandling)
                # update
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":commandToPilot"] = "tobekilled"
                varMap[":taskBufferErrorDiag"] = f"killed by {user}"
                if code in ["2", "9", "10", "52", "51", "60", "99"]:
                    # ignore commandToPilot for force kill
                    self.cur.execute((sql1F + comment) % table, varMap)
                elif useEventService or jobStatusInDB in ["merging"]:
                    # use force kill for event service or merging
                    self.cur.execute((sql1F + comment) % table, varMap)
                else:
                    self.cur.execute((sql1 + comment) % table, varMap)
                retU = self.cur.rowcount
                if retU == 0:
                    continue
                # set flag
                flagCommand = True
                # select
                varMap = {}
                varMap[":PandaID"] = pandaID
                if ((userProdSourceLabel in ["managed", "test", None] or "test" in userProdSourceLabel) and code in ["9", "52"]) or (
                    prodManager and code == "99"
                ):
                    # use dummy for force kill
                    varMap[":jobStatus"] = "dummy"
                elif (useEventService and not EventServiceUtils.isJobCloningSH(specialHandling)) or jobStatusInDB in ["merging"]:
                    # use dummy for force kill
                    varMap[":jobStatus"] = "dummy"
                else:
                    varMap[":jobStatus"] = "running"
                self.cur.arraysize = 10
                self.cur.execute((sql2 + comment) % table, varMap)
                res = self.cur.fetchall()
                if len(res) == 0:
                    continue
                # instantiate JobSpec
                job = JobSpec()
                job.pack(res[0])
                # delete
                if table == "ATLAS_PANDA.jobsDefined4":
                    varMap = {}
                    varMap[":PandaID"] = pandaID
                    varMap[":oldJobStatus1"] = "assigned"
                    varMap[":oldJobStatus2"] = "defined"
                    varMap[":oldJobStatus3"] = "pending"
                    varMap[":oldJobStatus4"] = "waiting"
                    self.cur.execute(sqlU + comment, varMap)
                else:
                    varMap = {}
                    varMap[":PandaID"] = pandaID
                    self.cur.execute((sql3 + comment) % table, varMap)
                retD = self.cur.rowcount
                if retD == 0:
                    continue
                oldJobStatus = job.jobStatus
                # error code
                if job.jobStatus != "failed":
                    currentTime = naive_utcnow()
                    # set status etc. for non-failed jobs
                    if job.endTime in [None, "NULL"]:
                        job.endTime = currentTime
                    # reset startTime for aCT where starting jobs don't acutally get started
                    if job.jobStatus == "starting":
                        job.startTime = job.endTime
                    job.modificationTime = currentTime
                    if code in ["2", "4"]:
                        # expire
                        job.jobStatus = "closed"
                        job.jobSubStatus = "toreassign"
                        job.taskBufferErrorCode = ErrorCode.EC_Expire
                        job.taskBufferErrorDiag = f"expired in {oldJobStatus}. status unchanged since {str(job.stateChangeTime)}"
                    elif code == "3":
                        # aborted
                        job.taskBufferErrorCode = ErrorCode.EC_Aborted
                        job.taskBufferErrorDiag = "aborted by ExtIF"
                    elif code == "8":
                        # reassigned by rebrokeage
                        job.taskBufferErrorCode = ErrorCode.EC_Reassigned
                        job.taskBufferErrorDiag = f"reassigned to another site by rebrokerage. new {user}"
                        job.commandToPilot = None
                    elif code in ["50", "52"]:
                        # killed by JEDI
                        job.taskBufferErrorCode = ErrorCode.EC_Kill
                        job.taskBufferErrorDiag = user
                    elif code == "51":
                        # reassigned by JEDI
                        job.jobStatus = "closed"
                        job.jobSubStatus = "toreassign"
                        job.taskBufferErrorCode = ErrorCode.EC_Kill
                        job.taskBufferErrorDiag = "reassigned by JEDI"
                    elif code == "55":
                        # killed since task is (almost) done
                        job.jobStatus = "closed"
                        job.jobSubStatus = "taskdone"
                        job.taskBufferErrorCode = ErrorCode.EC_Kill
                        job.taskBufferErrorDiag = "killed since task is (almost) done"
                    elif code == "60":
                        # terminated by the pilot. keep jobSubStatus reported by the pilot
                        job.jobStatus = "closed"
                        job.taskBufferErrorCode = ErrorCode.EC_Kill
                        job.taskBufferErrorDiag = "closed by the pilot"
                    elif code == "10":
                        job.jobStatus = "closed"
                        job.taskBufferErrorCode = ErrorCode.EC_FastRebrokerage
                        job.taskBufferErrorDiag = "fast rebrokerage due to Nq/Nr overshoot"
                    else:
                        # killed
                        job.taskBufferErrorCode = ErrorCode.EC_Kill
                        job.taskBufferErrorDiag = f"killed by {user}"
                    # set job status
                    if job.jobStatus != "closed":
                        job.jobStatus = "cancelled"
                else:
                    # keep status for failed jobs
                    job.modificationTime = naive_utcnow()
                    if code == "7":
                        # retried by server
                        job.taskBufferErrorCode = ErrorCode.EC_Retried
                        job.taskBufferErrorDiag = f"retrying at another site. new {user}"
                        job.commandToPilot = None
                job.stateChangeTime = job.modificationTime
                # insert
                self.cur.execute(sql4 + comment, job.valuesMap())
                # update file
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":status"] = "failed"
                varMap[":type1"] = "output"
                varMap[":type2"] = "log"
                self.cur.execute(sqlF + comment, varMap)
                # update files,metadata,parametes
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":modificationTime"] = job.modificationTime
                self.cur.execute(sqlFMod + comment, varMap)
                self.cur.execute(sqlMMod + comment, varMap)
                self.cur.execute(sqlPMod + comment, varMap)
                flagKilled = True
                updatedFlag = True
                # update JEDI tables
                if (
                    hasattr(panda_config, "useJEDI")
                    and panda_config.useJEDI is True
                    and job.lockedby == "jedi"
                    and get_task_event_module(self).checkTaskStatusJEDI(job.jediTaskID, self.cur)
                ):
                    # read files
                    varMap = {}
                    varMap[":PandaID"] = pandaID
                    self.cur.arraysize = 10000
                    self.cur.execute(sqlFile + comment, varMap)
                    resFs = self.cur.fetchall()
                    for resF in resFs:
                        fileSpec = FileSpec()
                        fileSpec.pack(resF)
                        job.addFile(fileSpec)
                    # actions for event service unless it was already retried
                    if taskBufferErrorCode not in [
                        ErrorCode.EC_Reassigned,
                        ErrorCode.EC_Retried,
                        ErrorCode.EC_PilotRetried,
                    ]:
                        # kill associated consumers for event service
                        if useEventService:
                            get_task_event_module(self).killEventServiceConsumers(job, True, False)
                            if job.computingSite != EventServiceUtils.siteIdForWaitingCoJumboJobs:
                                get_task_event_module(self).killUnusedEventServiceConsumers(job, False, killAll=True, checkAttemptNr=True)
                            get_task_event_module(self).updateRelatedEventServiceJobs(job, True)
                            if not job.notDiscardEvents():
                                get_task_event_module(self).killUnusedEventRanges(job.jediTaskID, job.jobsetID)
                            if eventService == EventServiceUtils.jumboJobFlagNumber:
                                get_task_event_module(self).hasDoneEvents(job.jediTaskID, job.PandaID, job, False)
                        elif useEventServiceMerge:
                            get_task_event_module(self).updateRelatedEventServiceJobs(job, True)
                    # disable reattempt
                    if job.processingType == "pmerge" and "keepUnmerged" not in killOpts and code != "51":
                        get_task_event_module(self).disableFurtherReattempt(job)
                    # update JEDI
                    self.propagateResultToJEDI(job, self.cur, oldJobStatus)
                break
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            timeDelta = naive_utcnow() - timeStart
            tmp_log.debug(f"com={flagCommand} kill={flagKilled} time={timeDelta.seconds}")
            # record status change
            try:
                if updatedFlag:
                    self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
                    self.push_job_status_message(job, job.PandaID, job.jobStatus)
            except Exception:
                tmp_log.error("recordStatusChange in killJob")
            if getUserInfo:
                return (flagCommand or flagKilled), {
                    "prodUserID": userProdUserID,
                    "prodSourceLabel": userProdSourceLabel,
                    "jobDefinitionID": userJobDefinitionID,
                    "jobsetID": userJobsetID,
                }
            return flagCommand or flagKilled
        except Exception:
            self.dump_error_message(tmp_log)
            # roll back
            self._rollback()
            timeDelta = naive_utcnow() - timeStart
            tmp_log.debug(f"time={timeDelta.seconds}")
            if getUserInfo:
                return False, {}
            return False

    # update unmerged jobs
    def updateUnmergedJobs(self, job, fileIDs=None, async_params=None):
        comment = " /* JediDBProxy.updateUnmergedJobs */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        tmp_log.debug(f"start with {async_params}")
        # get PandaID which produced unmerged files
        umPandaIDs = []
        umCheckedIDs = []
        if fileIDs is None:
            fileIDs = set()
        # sql to get PandaIDs
        sqlUMP = "SELECT PandaID,attemptNr FROM ATLAS_PANDA.filesTable4 "
        sqlUMP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlUMP += "AND type IN (:type1,:type2) ORDER BY attemptNr DESC "
        # sql to check job status
        sqlUMS = "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
        # look for unmerged files
        for tmpFile in job.Files:
            if tmpFile.isUnMergedInput():
                # only fileIDs which reach max attempt
                if len(fileIDs) > 0 and tmpFile.fileID not in fileIDs:
                    continue
                varMap = {}
                varMap[":jediTaskID"] = tmpFile.jediTaskID
                varMap[":datasetID"] = tmpFile.datasetID
                varMap[":fileID"] = tmpFile.fileID
                varMap[":type1"] = "output"
                varMap[":type2"] = "log"
                self.cur.arraysize = 100
                self.cur.execute(sqlUMP + comment, varMap)
                resUMP = self.cur.fetchall()
                # loop for job in merging state
                for tmpPandaID, tmpAttemptNr in resUMP:
                    # skip checked PandaIDs
                    if tmpPandaID in umCheckedIDs:
                        continue
                    # append to avoid redundant check
                    umCheckedIDs.append(tmpPandaID)
                    # check job status
                    varMap = {}
                    varMap[":PandaID"] = tmpPandaID
                    self.cur.execute(sqlUMS + comment, varMap)
                    resUMS = self.cur.fetchone()
                    # unmerged job should be in merging state
                    if resUMS is not None and resUMS[0] == "merging":
                        # append
                        umPandaIDs.append(tmpPandaID)
                        break
        # finish unmerge jobs
        sqlJFJ = f"SELECT {JobSpec.columnNames()} "
        sqlJFJ += "FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
        sqlJFF = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
        sqlJFF += "WHERE PandaID=:PandaID"
        for tmpPandaID in umPandaIDs:
            # read job
            varMap = {}
            varMap[":PandaID"] = tmpPandaID
            self.cur.arraysize = 10
            self.cur.execute(sqlJFJ + comment, varMap)
            resJFJ = self.cur.fetchone()
            umJob = JobSpec()
            umJob.pack(resJFJ)
            umJob.jobStatus = job.jobStatus
            if umJob.jobStatus in ["failed"] or umJob.isCancelled():
                umJob.taskBufferErrorCode = ErrorCode.EC_MergeFailed
                umJob.taskBufferErrorDiag = f"merge job {umJob.jobStatus}"
                umJob.jobSubStatus = f"merge_{umJob.jobStatus}"
            # read files
            self.cur.arraysize = 10000
            self.cur.execute(sqlJFF + comment, varMap)
            resJFFs = self.cur.fetchall()
            for resJFF in resJFFs:
                umFile = FileSpec()
                umFile.pack(resJFF)
                if umFile.status not in ["nooutput"]:
                    umFile.status = umJob.jobStatus
                umJob.addFile(umFile)
            # finish
            tmp_log.debug(f"update unmerged PandaID={umJob.PandaID}")
            self.archiveJob(umJob, False, useCommit=False, async_params=async_params)
        return

    # archive job to jobArchived and remove the job from jobsActive or jobsDefined
    def archiveJob(
        self,
        job,
        fromJobsDefined,
        useCommit=True,
        extraInfo=None,
        fromJobsWaiting=False,
        async_params=None,
    ):
        comment = " /* DBProxy.archiveJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID} jediTaskID={job.jediTaskID}")
        tmp_log.debug(f"start status={job.jobStatus} label={job.prodSourceLabel} " f"type={job.processingType} async_params={async_params}")
        start_time = naive_utcnow()
        if fromJobsDefined or fromJobsWaiting:
            sql0 = "SELECT jobStatus FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID "
            sql1 = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
        else:
            sql0 = "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID FOR UPDATE "
            sql1 = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
        sql2 = f"INSERT INTO ATLAS_PANDA.jobsArchived4 ({JobSpec.columnNames()}) "
        sql2 += JobSpec.bindValuesExpression()
        updatedJobList = []
        nTry = 1
        for iTry in range(nTry):
            try:
                # begin transaction
                if useCommit:
                    self.conn.begin()
                # check if JEDI is used
                useJEDI = False
                if (
                    hasattr(panda_config, "useJEDI")
                    and panda_config.useJEDI is True
                    and job.lockedby == "jedi"
                    and get_task_event_module(self).checkTaskStatusJEDI(job.jediTaskID, self.cur)
                ):
                    useJEDI = True
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                # delete downstream jobs first
                ddmIDs = []
                newJob = None
                ddmAttempt = 0
                if job.prodSourceLabel == "panda" and job.jobStatus == "failed":
                    # look for outputs
                    upOutputs = []
                    for file in job.Files:
                        if file.type == "output":
                            upOutputs.append(file.lfn)
                    toBeClosedSubList = {}
                    topUserDsList = []
                    # look for downstream jobs
                    sqlD = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 WHERE type=:type AND lfn=:lfn GROUP BY PandaID"
                    sqlDJS = f"SELECT {JobSpec.columnNames()} "
                    sqlDJS += "FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                    sqlDJD = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                    sqlDJI = f"INSERT INTO ATLAS_PANDA.jobsArchived4 ({JobSpec.columnNames()}) "
                    sqlDJI += JobSpec.bindValuesExpression()
                    sqlDFup = "UPDATE ATLAS_PANDA.filesTable4 SET status=:status WHERE PandaID=:PandaID AND type IN (:type1,:type2)"
                    sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlGetSub = "SELECT DISTINCT destinationDBlock FROM ATLAS_PANDA.filesTable4 WHERE type=:type AND PandaID=:PandaID"
                    sqlCloseSub = 'UPDATE /*+ INDEX_RS_ASC(TAB("DATASETS"."NAME")) */ ATLAS_PANDA.Datasets tab '
                    sqlCloseSub += "SET status=:status,modificationDate=CURRENT_DATE WHERE name=:name"
                    sqlDFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
                    sqlDFile += "WHERE PandaID=:PandaID"
                    for upFile in upOutputs:
                        tmp_log.debug(f"look for downstream jobs for {upFile}")
                        if useCommit:
                            self.conn.begin()
                        # select PandaID
                        varMap = {}
                        varMap[":lfn"] = upFile
                        varMap[":type"] = "input"
                        self.cur.arraysize = 100000
                        self.cur.execute(sqlD + comment, varMap)
                        res = self.cur.fetchall()
                        if useCommit:
                            if not self._commit():
                                raise RuntimeError("Commit error")
                        iDownJobs = 0
                        nDownJobs = len(res)
                        nDownChunk = 20
                        inTransaction = False
                        tmp_log.debug(f"found {nDownJobs} downstream jobs for {upFile}")
                        # loop over all downstream IDs
                        for (downID,) in res:
                            if useCommit:
                                if not inTransaction:
                                    self.conn.begin()
                                    inTransaction = True
                            tmp_log.debug(f"delete : {downID} ({iDownJobs}/{nDownJobs})")
                            iDownJobs += 1
                            # select jobs
                            varMap = {}
                            varMap[":PandaID"] = downID
                            self.cur.arraysize = 10
                            self.cur.execute(sqlDJS + comment, varMap)
                            resJob = self.cur.fetchall()
                            if len(resJob) == 0:
                                if useCommit and (iDownJobs % nDownChunk) == 0:
                                    if not self._commit():
                                        raise RuntimeError("Commit error")
                                    inTransaction = False
                                continue
                            # instantiate JobSpec
                            dJob = JobSpec()
                            dJob.pack(resJob[0])
                            # delete
                            varMap = {}
                            varMap[":PandaID"] = downID
                            self.cur.execute(sqlDJD + comment, varMap)
                            retD = self.cur.rowcount
                            if retD == 0:
                                if useCommit and (iDownJobs % nDownChunk) == 0:
                                    if not self._commit():
                                        raise RuntimeError("Commit error")
                                    inTransaction = False
                                continue
                            # error code
                            dJob.jobStatus = "cancelled"
                            dJob.endTime = naive_utcnow()
                            dJob.taskBufferErrorCode = ErrorCode.EC_Kill
                            dJob.taskBufferErrorDiag = "killed by Panda server : upstream job failed"
                            dJob.modificationTime = dJob.endTime
                            dJob.stateChangeTime = dJob.endTime
                            # insert
                            self.cur.execute(sqlDJI + comment, dJob.valuesMap())
                            # update file status
                            varMap = {}
                            varMap[":PandaID"] = downID
                            varMap[":status"] = "failed"
                            varMap[":type1"] = "output"
                            varMap[":type2"] = "log"
                            self.cur.execute(sqlDFup + comment, varMap)
                            # update files,metadata,parametes
                            varMap = {}
                            varMap[":PandaID"] = downID
                            varMap[":modificationTime"] = dJob.modificationTime
                            self.cur.execute(sqlFMod + comment, varMap)
                            self.cur.execute(sqlMMod + comment, varMap)
                            self.cur.execute(sqlPMod + comment, varMap)
                            # collect to record state change
                            updatedJobList.append(dJob)
                            # update JEDI tables
                            if useJEDI:
                                # read files
                                varMap = {}
                                varMap[":PandaID"] = downID
                                self.cur.arraysize = 100000
                                self.cur.execute(sqlDFile + comment, varMap)
                                resDFiles = self.cur.fetchall()
                                for resDFile in resDFiles:
                                    tmpDFile = FileSpec()
                                    tmpDFile.pack(resDFile)
                                    dJob.addFile(tmpDFile)
                                self.propagateResultToJEDI(dJob, self.cur)
                            # set tobeclosed to sub datasets
                            if dJob.jobDefinitionID not in toBeClosedSubList:
                                # init
                                toBeClosedSubList[dJob.jobDefinitionID] = []
                                # get sub datasets
                                varMap = {}
                                varMap[":type"] = "output"
                                varMap[":PandaID"] = downID
                                self.cur.arraysize = 1000
                                self.cur.execute(sqlGetSub + comment, varMap)
                                resGetSub = self.cur.fetchall()
                                if len(resGetSub) == 0:
                                    if useCommit and (iDownJobs % nDownChunk) == 0:
                                        if not self._commit():
                                            raise RuntimeError("Commit error")
                                        inTransaction = False
                                    continue
                                # loop over all sub datasets
                                for (tmpDestinationDBlock,) in resGetSub:
                                    if re.search("_sub\d+$", tmpDestinationDBlock) is None:
                                        continue
                                    if tmpDestinationDBlock not in toBeClosedSubList[dJob.jobDefinitionID]:
                                        # set tobeclosed
                                        varMap = {}
                                        varMap[":status"] = "tobeclosed"
                                        varMap[":name"] = tmpDestinationDBlock
                                        self.cur.execute(sqlCloseSub + comment, varMap)
                                        tmp_log.debug(f"set tobeclosed for {tmpDestinationDBlock}")
                                        # append
                                        toBeClosedSubList[dJob.jobDefinitionID].append(tmpDestinationDBlock)
                                        # close top-level user dataset
                                        topUserDsName = re.sub("_sub\d+$", "", tmpDestinationDBlock)
                                        if not useJEDI and topUserDsName != tmpDestinationDBlock and topUserDsName not in topUserDsList:
                                            # set tobeclosed
                                            varMap = {}
                                            if dJob.processingType.startswith("gangarobot") or dJob.processingType.startswith("hammercloud"):
                                                varMap[":status"] = "completed"
                                            else:
                                                varMap[":status"] = "tobeclosed"
                                            varMap[":name"] = topUserDsName
                                            self.cur.execute(sqlCloseSub + comment, varMap)
                                            tmp_log.debug(f"set {varMap[':status']} for {topUserDsName}")
                                            # append
                                            topUserDsList.append(topUserDsName)
                            if useCommit and (iDownJobs % nDownChunk) == 0:
                                if not self._commit():
                                    raise RuntimeError("Commit error")
                                inTransaction = False
                        if useCommit and inTransaction:
                            if not self._commit():
                                raise RuntimeError("Commit error")

                # main job
                if useCommit:
                    self.conn.begin()
                oldJobSubStatus = None
                # get current status
                currentJobStatus = None
                if fromJobsDefined or fromJobsWaiting:
                    varMap = {}
                    varMap[":PandaID"] = job.PandaID
                    self.cur.execute(sql0 + comment, varMap)
                    res0 = self.cur.fetchone()
                    if res0 is not None:
                        (currentJobStatus,) = res0
                else:
                    # lock job so that events are not dispatched during the processing
                    varMap = {}
                    varMap[":PandaID"] = job.PandaID
                    self.cur.execute(sql0 + comment, varMap)
                    res0 = self.cur.fetchone()
                # check input status for ES merge
                if useJEDI and EventServiceUtils.isEventServiceMerge(job) and job.jobStatus == "finished":
                    retInputStat = get_task_event_module(self).checkInputFileStatusInJEDI(job, useCommit=False, withLock=True)
                    tmp_log.debug(f"checkInput for ES merge -> {retInputStat}")
                    if retInputStat is None:
                        raise RuntimeError(f"archiveJob : {job.PandaID} failed to check input")
                    if retInputStat is False:
                        tmp_log.debug("set jobStatus=failed due to inconsistent input")
                        job.jobStatus = "failed"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceInconsistentIn
                        job.taskBufferErrorDiag = "inconsistent file status between Panda and JEDI"
                        for fileSpec in job.Files:
                            if fileSpec.type in ["output", "log"]:
                                fileSpec.status = "failed"
                # actions for jobs without tasks
                if not useJEDI:
                    # update HS06sec for non-JEDI jobs (e.g. HC)
                    hs06sec = get_entity_module(self).setHS06sec(job.PandaID, inActive=True)
                    tmp_log.debug(f"calculated hs06sec {hs06sec}")
                    if hs06sec is not None:
                        job.hs06sec = hs06sec

                    # update the g of CO2 emitted by the job
                    try:
                        gco2_regional, gco2_global = get_entity_module(self).set_co2_emissions(job.PandaID, in_active=True)
                        tmp_log.debug(f"calculated gCO2 regional {gco2_regional} and global {gco2_global}")
                        if gco2_regional is not None:
                            job.gco2_regional = gco2_regional
                        if gco2_global is not None:
                            job.gco2_global = gco2_global
                    except Exception:
                        tmp_log.error(f"failed calculating gCO2 with {traceback.format_exc()}")

                # actions for successful normal ES jobs
                if useJEDI and EventServiceUtils.isEventServiceJob(job) and not EventServiceUtils.isJobCloningJob(job):
                    # update some job attributes
                    hs06sec = get_entity_module(self).setHS06sec(job.PandaID, inActive=True)
                    if hs06sec is not None:
                        job.hs06sec = hs06sec

                    # update the g of CO2 emitted by the job
                    try:
                        gco2_regional, gco2_global = get_entity_module(self).set_co2_emissions(job.PandaID, in_active=True)
                        tmp_log.debug(f"calculated gCO2 regional {gco2_regional} and global {gco2_global}")
                        if gco2_regional is not None:
                            job.gco2_regional = gco2_regional
                        if gco2_global is not None:
                            job.gco2_global = gco2_global
                    except Exception:
                        tmp_log.error(f"failed calculating gCO2 with {traceback.format_exc()}")

                    # post-processing
                    oldJobSubStatus = job.jobSubStatus
                    if oldJobSubStatus == "NULL":
                        oldJobSubStatus = None
                    retEvS, retNewPandaID = self.ppEventServiceJob(job, currentJobStatus, False)
                    tmp_log.debug(f"ppE -> {retEvS}")
                    # DB error
                    if retEvS is None:
                        raise RuntimeError("Failed to retry for Event Service")
                    elif retEvS == 0:
                        # retry event ranges
                        job.jobStatus = "merging"
                        job.jobSubStatus = "es_retry"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceRetried
                        job.taskBufferErrorDiag = f"closed to retry unprocessed event ranges in PandaID={retNewPandaID}"
                    elif retEvS in [2, 10]:
                        # goes to merging
                        if retEvS == 2:
                            job.jobStatus = "merging"
                        else:
                            job.jobStatus = "closed"
                        job.jobSubStatus = "es_merge"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceMerge
                        job.taskBufferErrorDiag = f"closed to merge pre-merged files in PandaID={retNewPandaID}"
                        # kill unused event service consumers
                        get_task_event_module(self).killUnusedEventServiceConsumers(job, False, killAll=True)
                    elif retEvS == 3:
                        # maximum attempts reached
                        job.jobStatus = "failed"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceMaxAttempt
                        job.taskBufferErrorDiag = "maximum event attempts reached"
                        # kill other consumers
                        get_task_event_module(self).killEventServiceConsumers(job, False, False)
                        get_task_event_module(self).killUnusedEventServiceConsumers(job, False, killAll=True, checkAttemptNr=True)
                    elif retEvS == 4:
                        # other consumers are running
                        job.jobStatus = "merging"
                        job.jobSubStatus = "es_wait"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceWaitOthers
                        job.taskBufferErrorDiag = "no further action since other Event Service consumers were still running"
                    elif retEvS == 5:
                        # didn't process any event ranges
                        job.jobStatus = "closed"
                        job.jobSubStatus = "es_inaction"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceUnprocessed
                        job.taskBufferErrorDiag = "didn't process any events on WN or reached last job attempt and take no further action"
                    elif retEvS == 6:
                        # didn't process any event ranges and last consumer
                        job.jobStatus = "failed"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceLastUnprocessed
                        job.taskBufferErrorDiag = "didn't process any events on WN and give up since this is the last consumer"
                    elif retEvS == 7:
                        # all event ranges failed
                        job.jobStatus = "failed"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceAllFailed
                        job.taskBufferErrorDiag = "all event ranges failed"
                    elif retEvS == 8:
                        # retry event ranges but no events were processed
                        job.jobStatus = "closed"
                        job.jobSubStatus = "es_noevent"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceNoEvent
                        job.taskBufferErrorDiag = f"didn't process any events on WN and retry unprocessed even ranges in PandaID={retNewPandaID}"
                    elif retEvS == 9:
                        # closed in bad job status
                        job.jobStatus = "closed"
                        job.jobSubStatus = "es_badstatus"
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceBadStatus
                        job.taskBufferErrorDiag = "closed in bad jobStatus like defined and pending"
                    # additional actions when retry
                    codeListWithRetry = [0, 4, 5, 8, 9]
                    if retEvS in codeListWithRetry and job.computingSite != EventServiceUtils.siteIdForWaitingCoJumboJobs:
                        # check jumbo flag
                        sqlJumbo = f"SELECT useJumbo FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                        sqlJumbo += "WHERE jediTaskID=:jediTaskID "
                        varMap = {}
                        varMap[":jediTaskID"] = job.jediTaskID
                        self.cur.execute(sqlJumbo + comment, varMap)
                        resJumbo = self.cur.fetchone()
                        if resJumbo is not None:
                            (useJumbo,) = resJumbo
                        else:
                            useJumbo = None
                        tmp_log.debug(f"useJumbo={useJumbo}")
                        # no new jobs
                        if retNewPandaID is None and (retEvS != 4 or EventServiceUtils.isCoJumboJob(job) or useJumbo is not None):
                            nActiveConsumers = get_task_event_module(self).getActiveConsumers(job.jediTaskID, job.jobsetID, job.PandaID)
                            # create a fake cojumbo
                            if (
                                nActiveConsumers == 0
                                and retEvS in [4, 5]
                                and (EventServiceUtils.isCoJumboJob(job) or useJumbo is not None)
                                and job.computingSite != EventServiceUtils.siteIdForWaitingCoJumboJobs
                            ):
                                nActiveConsumers = get_task_event_module(self).makeFakeCoJumbo(job)
                            # no ES queues for retry
                            if nActiveConsumers == 0:
                                job.jobStatus = "failed"
                                job.taskBufferErrorCode = ErrorCode.EC_EventServiceNoEsQueues
                                job.taskBufferErrorDiag = "no ES queues available for new consumers"
                                tmp_log.debug(f"set {job.jobStatus} since {job.taskBufferErrorDiag}")
                    # kill unused event ranges
                    if job.jobStatus == "failed":
                        if not job.notDiscardEvents():
                            get_task_event_module(self).killUnusedEventRanges(job.jediTaskID, job.jobsetID)
                        get_task_event_module(self).updateRelatedEventServiceJobs(job, True)
                elif useJEDI and EventServiceUtils.isEventServiceJob(job) and EventServiceUtils.isJobCloningJob(job):
                    # check for cloned jobs
                    retJC = self.checkClonedJob(job, False)
                    # DB error
                    if retJC is None:
                        raise RuntimeError("Failed to take post-action for cloned job")
                    elif retJC["lock"] is True:
                        # kill other clones if the job done after locking semaphore
                        get_task_event_module(self).killEventServiceConsumers(job, False, False)
                        get_task_event_module(self).killUnusedEventServiceConsumers(job, False, killAll=True)
                    else:
                        # failed to lock semaphore
                        if retJC["last"] is False:
                            # set closed if it is not the last clone
                            job.jobStatus = "closed"
                            job.jobSubStatus = "jc_unlock"
                            job.taskBufferErrorCode = ErrorCode.EC_JobCloningUnlock
                            if retJC["win"] is not None:
                                job.taskBufferErrorDiag = f"closed since another clone PandaID={retJC['win']} got semaphore"
                            else:
                                job.taskBufferErrorDiag = "closed since failed to lock semaphore"
                elif useJEDI and EventServiceUtils.is_fine_grained_job(job):
                    # fine-grained
                    n_done, n_remain = self.check_fine_grained_processing(job)
                    if n_done > 0 or n_remain == 0:
                        job.jobStatus = "finished"
                        if n_remain == 0:
                            job.jobSubStatus = "fg_done"
                        else:
                            job.jobSubStatus = "fg_partial"
                    else:
                        job.jobSubStatus = "fg_stumble"
                # release unprocessed samples for HPO
                if job.is_hpo_workflow():
                    get_task_event_module(self).release_unprocessed_events(job.jediTaskID, job.PandaID)
                # delete from jobsDefined/Active
                varMap = {}
                varMap[":PandaID"] = job.PandaID
                if fromJobsDefined:
                    varMap[":oldJobStatus1"] = "assigned"
                    varMap[":oldJobStatus2"] = "defined"
                self.cur.execute(sql1 + comment, varMap)
                n = self.cur.rowcount
                if n == 0:
                    # already deleted
                    raise RuntimeError(f"PandaID={job.PandaID} already deleted")
                else:
                    # insert
                    job.modificationTime = naive_utcnow()
                    job.stateChangeTime = job.modificationTime
                    if job.endTime == "NULL":
                        job.endTime = job.modificationTime
                    self.cur.execute(sql2 + comment, job.valuesMap())
                    # update files
                    for file in job.Files:
                        sqlF = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                        varMap = file.valuesMap(onlyChanged=True)
                        if varMap != {}:
                            varMap[":row_ID"] = file.row_ID
                            tmp_log.debug(sqlF + comment + str(varMap))
                            self.cur.execute(sqlF + comment, varMap)
                    # update metadata and parameters
                    sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[":PandaID"] = job.PandaID
                    varMap[":modificationTime"] = job.modificationTime
                    self.cur.execute(sqlFMod + comment, varMap)
                    self.cur.execute(sqlMMod + comment, varMap)
                    self.cur.execute(sqlPMod + comment, varMap)
                    # increment the number of failed jobs in _dis
                    myDisList = []
                    if job.jobStatus == "failed" and job.prodSourceLabel in [
                        "managed",
                        "test",
                    ]:
                        for tmpFile in job.Files:
                            if tmpFile.type == "input" and tmpFile.dispatchDBlock not in ["", "NULL", None] and tmpFile.dispatchDBlock not in myDisList:
                                varMap = {}
                                varMap[":name"] = tmpFile.dispatchDBlock
                                # check currentfiles
                                sqlGetCurFiles = """SELECT /*+ BEGIN_OUTLINE_DATA """
                                sqlGetCurFiles += """INDEX_RS_ASC(@"SEL$1" "TAB"@"SEL$1" ("DATASETS"."NAME")) """
                                sqlGetCurFiles += """OUTLINE_LEAF(@"SEL$1") ALL_ROWS """
                                sqlGetCurFiles += """IGNORE_OPTIM_EMBEDDED_HINTS """
                                sqlGetCurFiles += """END_OUTLINE_DATA */ """
                                sqlGetCurFiles += "currentfiles,vuid FROM ATLAS_PANDA.Datasets tab WHERE name=:name"
                                self.cur.execute(sqlGetCurFiles + comment, varMap)
                                resCurFiles = self.cur.fetchone()
                                tmp_log.debug(f"{str(resCurFiles)}")
                                if resCurFiles is not None:
                                    # increment currentfiles only for the first failed job since that is enough
                                    tmpCurrentFiles, tmpVUID = resCurFiles
                                    tmp_log.debug(f"{tmpFile.dispatchDBlock} currentfiles={tmpCurrentFiles}")
                                    if tmpCurrentFiles == 0:
                                        tmp_log.debug(f"{tmpFile.dispatchDBlock} update currentfiles")
                                        varMap = {}
                                        varMap[":vuid"] = tmpVUID
                                        sqlFailedInDis = "UPDATE ATLAS_PANDA.Datasets "
                                        sqlFailedInDis += "SET currentfiles=currentfiles+1 WHERE vuid=:vuid"
                                        self.cur.execute(sqlFailedInDis + comment, varMap)
                                myDisList.append(tmpFile.dispatchDBlock)
                    # collect to record state change
                    updatedJobList.append(job)
                    # updates JEDI tables except for a successful ES consumer job awaiting merging or other active
                    # consumers, or a closed/cancelled cloning job without getting a semaphore
                    if useJEDI:
                        to_propagate = True
                        if EventServiceUtils.isEventServiceJob(job):
                            if EventServiceUtils.isJobCloningJob(job):
                                if job.isCancelled():
                                    # check semaphore
                                    check_jc = self.checkClonedJob(job, False)
                                    if check_jc["lock"] is False:
                                        tmp_log.debug("not propagate results to JEDI for cloning job without semaphore")
                                        to_propagate = False
                            elif job.isCancelled() or job.jobStatus == "merging":
                                tmp_log.debug("not propagate results to JEDI for intermediate ES consumer")
                                to_propagate = False
                        if to_propagate:
                            self.propagateResultToJEDI(
                                job,
                                self.cur,
                                extraInfo=extraInfo,
                                async_params=async_params,
                            )
                    # update related ES jobs when ES-merge job is done
                    if (
                        useJEDI
                        and EventServiceUtils.isEventServiceMerge(job)
                        and job.taskBufferErrorCode not in [ErrorCode.EC_PilotRetried]
                        and not job.isCancelled()
                    ):
                        if job.jobStatus == "failed":
                            get_task_event_module(self).updateRelatedEventServiceJobs(job, True)
                        else:
                            get_task_event_module(self).updateRelatedEventServiceJobs(job)
                # propagate successful result to unmerge job
                if useJEDI and job.processingType == "pmerge" and job.jobStatus == "finished":
                    self.updateUnmergedJobs(job, async_params=async_params)
                # overwrite job status
                tmpJobStatus = job.jobStatus
                sqlPRE = "SELECT /* use_json_type */ scj.data.pledgedcpu FROM ATLAS_PANDA.schedconfig_json scj WHERE scj.panda_queue=:siteID "

                sqlOJS = "UPDATE ATLAS_PANDA.jobsArchived4 SET jobStatus=:jobStatus,jobSubStatus=:jobSubStatus WHERE PandaID=:PandaID "
                if (
                    oldJobSubStatus in ["pilot_failed", "es_heartbeat"]
                    or oldJobSubStatus == "pilot_killed"
                    and job.jobSubStatus in ["es_noevent", "es_inaction"]
                ):
                    # check if preemptable
                    isPreemptable = False
                    varMap = {}
                    varMap[":siteID"] = job.computingSite
                    self.cur.execute(sqlPRE + comment, varMap)
                    resPRE = self.cur.fetchone()
                    if resPRE is not None:
                        try:
                            if int(resPRE[0]) == -1:
                                isPreemptable = True
                        except Exception:
                            pass
                    # overwrite job status
                    varMap = {}
                    varMap[":PandaID"] = job.PandaID
                    if isPreemptable and oldJobSubStatus not in ["pilot_failed"]:
                        varMap[":jobStatus"] = "closed"
                        varMap[":jobSubStatus"] = "es_preempted"
                    else:
                        varMap[":jobStatus"] = "failed"
                        varMap[":jobSubStatus"] = oldJobSubStatus
                    self.cur.execute(sqlOJS + comment, varMap)
                    tmpJobStatus = varMap[":jobStatus"]
                if EventServiceUtils.isEventServiceJob(job):
                    if (
                        job.jobStatus in ["failed", "closed"]
                        and job.taskBufferErrorCode
                        in [
                            ErrorCode.EC_EventServiceLastUnprocessed,
                            ErrorCode.EC_EventServiceUnprocessed,
                        ]
                        and job.nEvents > 0
                    ):
                        varMap = {}
                        varMap[":PandaID"] = job.PandaID
                        varMap[":jobStatus"] = "merging"
                        if oldJobSubStatus in ["es_toolong"]:
                            varMap[":jobSubStatus"] = oldJobSubStatus
                        else:
                            varMap[":jobSubStatus"] = "es_wait"
                        self.cur.execute(sqlOJS + comment, varMap)
                        tmpJobStatus = varMap[":jobStatus"]
                        tmp_log.debug("change failed to merging")
                    elif (
                        job.jobStatus in ["failed"]
                        and job.taskBufferErrorCode
                        in [
                            ErrorCode.EC_EventServiceLastUnprocessed,
                            ErrorCode.EC_EventServiceUnprocessed,
                        ]
                        and (
                            oldJobSubStatus in ["pilot_noevents"]
                            or (job.pilotErrorCode == 0 and job.ddmErrorCode == 0 and job.supErrorCode == 0 and job.jobDispatcherErrorCode == 0)
                        )
                    ):
                        varMap = {}
                        varMap[":PandaID"] = job.PandaID
                        varMap[":jobStatus"] = "closed"
                        varMap[":jobSubStatus"] = oldJobSubStatus
                        self.cur.execute(sqlOJS + comment, varMap)
                        tmpJobStatus = varMap[":jobStatus"]
                        tmp_log.debug(f"change failed to closed for {oldJobSubStatus}")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                # record status change
                try:
                    for tmpJob in updatedJobList:
                        self.recordStatusChange(
                            tmpJob.PandaID,
                            tmpJobStatus,
                            jobInfo=tmpJob,
                            useCommit=useCommit,
                        )
                        extra_info_dict = {
                            "job_nevents": tmpJob.nEvents,
                            "job_ninputfiles": tmpJob.nInputFiles,
                            "job_noutputdatafiles": tmpJob.nOutputDataFiles,
                            "job_ninputdatafiles": tmpJob.nInputDataFiles,
                            "job_inputfilebytes": tmpJob.inputFileBytes,
                            "job_outputfilebytes": tmpJob.outputFileBytes,
                            "job_hs06sec": tmpJob.hs06sec,
                        }
                        self.push_job_status_message(
                            tmpJob,
                            tmpJob.PandaID,
                            tmpJobStatus,
                            extra_data=extra_info_dict,
                        )
                except Exception:
                    tmp_log.error("recordStatusChange in archiveJob")
                exec_time = naive_utcnow() - start_time
                tmp_log.debug("done OK. took %s.%03d sec" % (exec_time.seconds, exec_time.microseconds / 1000))
                return True, ddmIDs, ddmAttempt, newJob
            except Exception:
                # roll back
                if useCommit:
                    self._rollback(True)
                # error
                self.dump_error_message(tmp_log)
                exec_time = naive_utcnow() - start_time
                tmp_log.debug("done NG. took %s.%03d sec" % (exec_time.seconds, exec_time.microseconds / 1000))
                if not useCommit:
                    raise RuntimeError("archiveJob failed")
                return False, [], 0, None

    # check fine-grained job
    def check_fine_grained_processing(self, job_spec):
        comment = " /* DBProxy.check_fine_grained_processing */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job_spec.PandaID} jediTaskID={job_spec.jediTaskID}")
        try:
            # sql to release events
            sqlW = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
            sqlW += "SET PandaID=:jobsetID,status=:newEventStatus "
            sqlW += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID AND status<>:eventStatus "
            # sql to count successful events
            sqlC = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlC += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID AND status=:eventStatus "
            # sql to count remaining events
            sqlU = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlU += "WHERE jediTaskID=:jediTaskID AND PandaID=:jobsetID "
            # release
            varMap = {
                ":jediTaskID": job_spec.jediTaskID,
                ":PandaID": job_spec.PandaID,
                ":jobsetID": job_spec.jobsetID,
                ":eventStatus": EventServiceUtils.ST_finished,
                ":newEventStatus": EventServiceUtils.ST_ready,
            }
            self.cur.execute(sqlW + comment, varMap)
            n_release = self.cur.rowcount
            # count successful events
            varMap = {
                ":jediTaskID": job_spec.jediTaskID,
                ":PandaID": job_spec.PandaID,
                ":eventStatus": EventServiceUtils.ST_finished,
            }
            self.cur.execute(sqlC + comment, varMap)
            (n_done,) = self.cur.fetchone()
            # count remaining events
            varMap = {
                ":jediTaskID": job_spec.jediTaskID,
                ":jobsetID": job_spec.jobsetID,
            }
            self.cur.execute(sqlU + comment, varMap)
            (n_remain,) = self.cur.fetchone()
            tmp_log.debug(f"done={n_done} release/remain={n_release}/{n_remain}")
            return n_done, n_remain
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            raise RuntimeError(comment + " failed")

    # check for cloned jobs
    def checkClonedJob(self, jobSpec, useCommit=True):
        comment = " /* DBProxy.checkClonedJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={jobSpec.PandaID}")
        tmp_log.debug("start")
        try:
            # return value {'lock': True if the job locked the semaphore,
            #               'last': True if the job is the last clone
            #               'win': PandaID of winner
            # None : fatal error
            retValue = {"lock": False, "last": False, "win": None}
            # begin transaction
            if useCommit:
                self.conn.begin()
            self.cur.arraysize = 10000
            # check if semaphore is locked
            sqlED = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlED += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID "
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":pandaID"] = jobSpec.PandaID
            self.cur.execute(sqlED + comment, varMap)
            resEU = self.cur.fetchone()
            (nRowEU,) = resEU
            if nRowEU > 0:
                retValue["lock"] = True
            else:
                # get PandaID of the winner
                sqlWP = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlWP += "distinct PandaID "
                sqlWP += f"FROM {panda_config.schemaJEDI}.JEDI_Events tab "
                sqlWP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                for tmpFileSpec in jobSpec.Files:
                    if tmpFileSpec.type == "input":
                        varMap = {}
                        varMap[":jediTaskID"] = tmpFileSpec.jediTaskID
                        varMap[":datasetID"] = tmpFileSpec.datasetID
                        varMap[":fileID"] = tmpFileSpec.fileID
                        self.cur.execute(sqlWP + comment, varMap)
                        resWP = self.cur.fetchone()
                        if resWP is not None:
                            retValue["win"] = resWP[0]
                            break
            # get PandaIDs of clones
            sqlCP = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
            sqlCP += "WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            sqlCP += "UNION "
            sqlCP += "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
            sqlCP += "WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":jobsetID"] = jobSpec.jobsetID
            self.cur.execute(sqlCP + comment, varMap)
            resCP = self.cur.fetchall()
            pandaIDsList = set()
            for (pandaID,) in resCP:
                if pandaID != jobSpec.PandaID:
                    pandaIDsList.add(pandaID)
            if len(pandaIDsList) == 0:
                retValue["last"] = True
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmp_log.debug(retValue)
            return retValue
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    def get_average_memory_jobs(self, computingsite, target):
        """
        Calculates the average memory for running and queued (starting) jobs at a particular panda queue.
        This function is equivalent to the get_average_memory_workers (for PULL), but is meant for PUSH queues.

        :param computingsite: name of the PanDA queue
        :param target: memory target for the queue in MB. This value is only used in the logging

        :return: average_memory_running_submitted, average_memory_running
        """

        comment = " /* DBProxy.get_average_memory_jobs */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            sql_running_and_submitted = (
                f"SELECT /*+ RESULT_CACHE */ COMPUTINGSITE, SUM(NJOBS * PRORATED_MEM_AVG) / SUM(NJOBS) AS avg_memory "
                f"FROM {panda_config.schemaPANDA}.JOBS_SHARE_STATS "
                f"WHERE COMPUTINGSITE = :computingsite "
                f"AND jobstatus IN ('running', 'starting') "
                f"GROUP BY COMPUTINGSITE"
            )

            sql_running = (
                f"SELECT /*+ RESULT_CACHE */ COMPUTINGSITE, SUM(NJOBS * PRORATED_MEM_AVG) / SUM(NJOBS) AS avg_memory "
                f"FROM {panda_config.schemaPANDA}.JOBS_SHARE_STATS "
                f"WHERE COMPUTINGSITE = :computingsite "
                f"AND jobstatus = 'running' "
                f"GROUP BY COMPUTINGSITE"
            )

            var_map = {":computingsite": computingsite}

            self.cur.execute(sql_running_and_submitted + comment, var_map)
            results = self.cur.fetchone()
            try:
                average_memory_running_submitted = results[1] if results[1] is not None else 0
            except TypeError:
                average_memory_running_submitted = 0

            self.cur.execute(sql_running + comment, var_map)
            results = self.cur.fetchone()
            try:
                average_memory_running = results[1] if results[1] is not None else 0
            except TypeError:
                average_memory_running = 0

            tmp_log.info(
                f"computingsite={computingsite} currently has "
                f"meanrss_running_submitted={average_memory_running_submitted} "
                f"meanrss_running={average_memory_running} "
                f"meanrss_target={target} MB"
            )
            return average_memory_running_submitted, average_memory_running

        except Exception:
            self.dump_error_message(tmp_log)
            return 0, 0

    def construct_where_clause(
        self,
        site_name,
        mem,
        disk_space,
        background,
        resource_type,
        prod_source_label,
        computing_element,
        is_gu,
        job_type,
        prod_user_id,
        task_id,
        average_memory_limit,
        remaining_time,
    ):
        get_val_map = {":oldJobStatus": "activated", ":computingSite": site_name}

        sql_where_clause = "WHERE jobStatus=:oldJobStatus AND computingSite=:computingSite "

        if mem not in [0, "0"]:
            sql_where_clause += "AND (minRamCount<=:minRamCount OR minRamCount=0) "
            get_val_map[":minRamCount"] = mem

        if disk_space not in [0, "0"]:
            sql_where_clause += "AND (maxDiskCount<=:maxDiskCount OR maxDiskCount=0) "
            get_val_map[":maxDiskCount"] = disk_space

        if remaining_time > 0:
            sql_where_clause += "AND (maxWalltime IS NULL OR maxWalltime<=:maxWalltime) "
            get_val_map[":maxWalltime"] = remaining_time

        if background is True:
            sql_where_clause += "AND jobExecutionID=1 "

        if resource_type is not None:
            sql_where_clause += "AND resource_type=:resourceType "
            get_val_map[":resourceType"] = resource_type

        if prod_source_label == "user":
            sql_where_clause += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3) "
            get_val_map[":prodSourceLabel1"] = "user"
            get_val_map[":prodSourceLabel2"] = "panda"
            get_val_map[":prodSourceLabel3"] = "install"
        elif prod_source_label in [None, "managed"]:
            sql_where_clause += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3,:prodSourceLabel4) "
            get_val_map[":prodSourceLabel1"] = "managed"
            get_val_map[":prodSourceLabel2"] = "test"
            get_val_map[":prodSourceLabel3"] = "prod_test"
            get_val_map[":prodSourceLabel4"] = "install"
        elif prod_source_label == "test" and computing_element is not None:
            if is_gu and job_type == "user":
                sql_where_clause += "AND processingType=:processingType1 "
                get_val_map[":processingType1"] = "gangarobot"
            else:
                sql_where_clause += "AND (processingType=:processingType1 OR prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3)) "
                get_val_map[":processingType1"] = "gangarobot"
                get_val_map[":prodSourceLabel1"] = "prod_test"
                get_val_map[":prodSourceLabel2"] = "install"
                get_val_map[":prodSourceLabel3"] = "test"
        elif prod_source_label == "unified":
            sql_where_clause += (
                "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3,:prodSourceLabel4,:prodSourceLabel5,:prodSourceLabel6) "
            )
            get_val_map[":prodSourceLabel1"] = "managed"
            get_val_map[":prodSourceLabel2"] = "test"
            get_val_map[":prodSourceLabel3"] = "prod_test"
            get_val_map[":prodSourceLabel4"] = "install"
            get_val_map[":prodSourceLabel5"] = "user"
            get_val_map[":prodSourceLabel6"] = "panda"
        else:
            sql_where_clause += "AND prodSourceLabel=:prodSourceLabel "
            get_val_map[":prodSourceLabel"] = prod_source_label

        if prod_user_id is not None:
            compact_dn = CoreUtils.clean_user_id(prod_user_id)
            if compact_dn in ["", "NULL", None]:
                compact_dn = prod_user_id
            sql_where_clause += "AND prodUserName=:prodUserName "
            get_val_map[":prodUserName"] = compact_dn

        if task_id not in [None, "NULL"]:
            sql_where_clause += "AND jediTaskID=:taskID "
            get_val_map[":taskID"] = task_id

        if average_memory_limit:
            sql_where_clause += "AND minramcount / NVL(corecount, 1)<=:average_memory_limit "
            get_val_map[":average_memory_limit"] = average_memory_limit

        return sql_where_clause, get_val_map

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
        remaining_time,
    ):
        """
        1. Construct where clause (sql_where_clause) based on applicable filters for request
        2. Select n jobs with the highest priorities and the lowest pandaids
        3. Update the jobs to status SENT
        4. Pack the files and if jobs are AES also the event ranges
        """
        comment = " /* DBProxy.getJobs */"
        timeStart = naive_utcnow()
        tmp_log = self.create_tagged_logger(comment, f"{siteName} {datetime.datetime.isoformat(timeStart)}")
        tmp_log.debug("Start")

        # Number of PanDAIDs that will be tried
        if hasattr(panda_config, "nJobsInGetJob"):
            maxAttemptIDx = panda_config.nJobsInGetJob
        else:
            maxAttemptIDx = 10

        # There is the case where the grid has no workloads and running HIMEM jobs is better than running no jobs
        ignore_meanrss = self.getConfigValue("meanrss", "IGNORE_MEANRSS")

        # get the configuration for maximum workers of each type
        is_push_queue = False
        average_memory_target = None
        average_memory_limit = None
        pq_data_des = get_entity_module(self).get_config_for_pq(siteName)
        if ignore_meanrss == True:
            tmp_log.debug("Ignoring meanrss limit and accepting any job")
        elif not pq_data_des:
            tmp_log.debug("Error retrieving queue configuration from DB, limits can not be applied")
        else:
            try:
                if pq_data_des["meanrss"] != 0:
                    average_memory_target = pq_data_des["meanrss"]
            except KeyError:
                pass
            try:
                workflow = pq_data_des["workflow"]
                if workflow and workflow.startswith("push"):
                    is_push_queue = True
            except KeyError:
                pass

        if is_push_queue and average_memory_target:
            average_memory_jobs_running_submitted, average_memory_jobs_running = self.get_average_memory_jobs(siteName, average_memory_target)
            if average_memory_jobs_running_submitted > average_memory_target or average_memory_jobs_running > average_memory_target:
                average_memory_limit = average_memory_target
                tmp_log.info(f"Queue {siteName} meanRSS will be throttled to jobs under {average_memory_limit}MB")

        # generate the WHERE clauses based on the requirements for the job
        sql_where_clause, getValMap = self.construct_where_clause(
            site_name=siteName,
            mem=mem,
            disk_space=diskSpace,
            background=background,
            resource_type=resourceType,
            prod_source_label=prodSourceLabel,
            computing_element=computingElement,
            is_gu=is_gu,
            job_type=jobType,
            prod_user_id=prodUserID,
            task_id=taskID,
            average_memory_limit=average_memory_limit,
            remaining_time=remaining_time,
        )

        # get the sorting criteria (global shares, age, etc.)
        sorting_sql, sorting_varmap = get_entity_module(self).getSortingCriteria(siteName, maxAttemptIDx)
        if sorting_varmap:  # copy the var map, but not the sql, since it has to be at the very end
            for tmp_key in sorting_varmap:
                getValMap[tmp_key] = sorting_varmap[tmp_key]

        retJobs = []
        nSent = 0
        getValMapOrig = copy.copy(getValMap)

        try:
            timeLimit = datetime.timedelta(seconds=timeout - 10)

            # get nJobs
            for iJob in range(nJobs):
                getValMap = copy.copy(getValMapOrig)
                pandaID = 0

                nTry = 1
                for iTry in range(nTry):
                    # set siteID
                    tmpSiteID = siteName
                    # get file lock
                    tmp_log.debug("lock")
                    if (naive_utcnow() - timeStart) < timeLimit:
                        toGetPandaIDs = True
                        pandaIDs = []
                        specialHandlingMap = {}

                        if toGetPandaIDs:
                            # get PandaIDs
                            sqlP = "SELECT /*+ INDEX_RS_ASC(tab (PRODSOURCELABEL COMPUTINGSITE JOBSTATUS) ) */ PandaID,currentPriority,specialHandling FROM ATLAS_PANDA.jobsActive4 tab "
                            sqlP += sql_where_clause

                            if sorting_sql:
                                sqlP = "SELECT * FROM (" + sqlP
                                sqlP += sorting_sql

                            tmp_log.debug(sqlP + comment + str(getValMap))
                            # start transaction
                            self.conn.begin()
                            # select
                            self.cur.arraysize = 100000
                            self.cur.execute(sqlP + comment, getValMap)
                            resIDs = self.cur.fetchall()
                            # commit
                            if not self._commit():
                                raise RuntimeError("Commit error")

                            for (
                                tmpPandaID,
                                tmpCurrentPriority,
                                tmpSpecialHandling,
                            ) in resIDs:
                                pandaIDs.append(tmpPandaID)
                                specialHandlingMap[tmpPandaID] = tmpSpecialHandling

                        if pandaIDs == []:
                            tmp_log.debug("no PandaIDs")
                            retU = 0  # retU: return from update
                        else:
                            # update
                            for indexID, tmpPandaID in enumerate(pandaIDs):
                                # max attempts
                                if indexID > maxAttemptIDx:
                                    break
                                # lock first
                                sqlPL = "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 " "WHERE PandaID=:PandaID FOR UPDATE NOWAIT "
                                # update
                                sqlJ = "UPDATE ATLAS_PANDA.jobsActive4 "
                                sqlJ += "SET jobStatus=:newJobStatus,modificationTime=CURRENT_DATE,modificationHost=:modificationHost,startTime=CURRENT_DATE"
                                varMap = {}
                                varMap[":PandaID"] = tmpPandaID
                                varMap[":newJobStatus"] = "sent"
                                varMap[":oldJobStatus"] = "activated"
                                varMap[":modificationHost"] = node
                                # set CE
                                if computingElement is not None:
                                    sqlJ += ",computingElement=:computingElement"
                                    varMap[":computingElement"] = computingElement
                                # set schedulerID
                                if schedulerID is not None:
                                    sqlJ += ",schedulerID=:schedulerID"
                                    varMap[":schedulerID"] = schedulerID

                                # background flag
                                if background is not True:
                                    sqlJ += ",jobExecutionID=0"
                                sqlJ += " WHERE PandaID=:PandaID AND jobStatus=:oldJobStatus"
                                # SQL to get nSent
                                sentLimit = timeStart - datetime.timedelta(seconds=60)
                                sqlSent = "SELECT count(*) FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus "
                                sqlSent += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
                                sqlSent += "AND computingSite=:computingSite "
                                sqlSent += "AND modificationTime>:modificationTime "
                                varMapSent = {}
                                varMapSent[":jobStatus"] = "sent"
                                varMapSent[":computingSite"] = tmpSiteID
                                varMapSent[":modificationTime"] = sentLimit
                                varMapSent[":prodSourceLabel1"] = "managed"
                                varMapSent[":prodSourceLabel2"] = "test"

                                # start transaction
                                self.conn.begin()
                                # pre-lock
                                prelocked = False
                                try:
                                    varMapPL = {":PandaID": tmpPandaID}
                                    tmp_log.debug(sqlPL + comment + str(varMapPL))
                                    self.cur.execute(sqlPL + comment, varMapPL)
                                    prelocked = True
                                except Exception:
                                    tmp_log.debug("cannot pre-lock")
                                # update
                                if prelocked:
                                    tmp_log.debug(sqlJ + comment + str(varMap))
                                    self.cur.execute(sqlJ + comment, varMap)
                                    retU = self.cur.rowcount
                                    tmp_log.debug(f"retU={retU}")
                                else:
                                    retU = 0
                                if retU != 0:
                                    # get nSent for production jobs
                                    if prodSourceLabel in [None, "managed"]:
                                        tmp_log.debug(sqlSent + comment + str(varMapSent))
                                        self.cur.execute(sqlSent + comment, varMapSent)
                                        resSent = self.cur.fetchone()
                                        if resSent is not None:
                                            (nSent,) = resSent
                                    # insert job and worker mapping
                                    if harvester_id is not None and worker_id is not None:
                                        # insert worker if missing
                                        get_worker_module(self).updateWorkers(
                                            harvester_id,
                                            [
                                                {
                                                    "workerID": worker_id,
                                                    "nJobs": 1,
                                                    "status": "running",
                                                    "lastUpdate": naive_utcnow(),
                                                }
                                            ],
                                            useCommit=False,
                                        )
                                        # insert mapping
                                        sqlJWH = "SELECT 1 FROM ATLAS_PANDA.Harvester_Instances WHERE harvester_ID=:harvesterID "

                                        sqlJWC = "SELECT PandaID FROM ATLAS_PANDA.Harvester_Rel_Jobs_Workers "
                                        sqlJWC += "WHERE harvesterID=:harvesterID AND workerID=:workerID AND PandaID=:PandaID "

                                        sqlJWI = "INSERT INTO ATLAS_PANDA.Harvester_Rel_Jobs_Workers (harvesterID,workerID,PandaID,lastUpdate) "
                                        sqlJWI += "VALUES (:harvesterID,:workerID,:PandaID,:lastUpdate) "

                                        sqlJWU = "UPDATE ATLAS_PANDA.Harvester_Rel_Jobs_Workers SET lastUpdate=:lastUpdate "
                                        sqlJWU += "WHERE harvesterID=:harvesterID AND workerID=:workerID AND PandaID=:PandaID "

                                        varMap = dict()
                                        varMap[":harvesterID"] = harvester_id

                                        self.cur.execute(sqlJWH + comment, varMap)
                                        resJWH = self.cur.fetchone()
                                        if resJWH is None:
                                            tmp_log.debug(f"getJobs : Site {tmpSiteID} harvester_id={harvester_id} not found")
                                        else:
                                            varMap = dict()
                                            varMap[":harvesterID"] = harvester_id
                                            varMap[":workerID"] = worker_id
                                            varMap[":PandaID"] = tmpPandaID
                                            self.cur.execute(sqlJWC + comment, varMap)
                                            resJWC = self.cur.fetchone()
                                            varMap = dict()
                                            varMap[":harvesterID"] = harvester_id
                                            varMap[":workerID"] = worker_id
                                            varMap[":PandaID"] = tmpPandaID
                                            varMap[":lastUpdate"] = naive_utcnow()
                                            if resJWC is None:
                                                # insert
                                                self.cur.execute(sqlJWI + comment, varMap)
                                            else:
                                                # update
                                                self.cur.execute(sqlJWU + comment, varMap)
                                # commit
                                if not self._commit():
                                    raise RuntimeError("Commit error")
                                # succeeded
                                if retU != 0:
                                    pandaID = tmpPandaID
                                    break
                    else:
                        tmp_log.debug("do nothing")
                        retU = 0
                    # release file lock
                    tmp_log.debug("unlock")
                    # succeeded
                    if retU != 0:
                        break
                    if iTry + 1 < nTry:
                        # time.sleep(0.5)
                        pass
                # failed to UPDATE
                if retU == 0:
                    # reset pandaID
                    pandaID = 0
                tmp_log.debug(f"retU {retU} : PandaID {pandaID} - {prodSourceLabel}")
                if pandaID == 0:
                    break

                # start transaction
                self.conn.begin()
                # query to get the DB entry for a specific PanDA ID
                sql_select_job = f"SELECT {JobSpec.columnNames()} FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
                varMap = {}
                varMap[":PandaID"] = pandaID
                self.cur.arraysize = 10
                self.cur.execute(sql_select_job + comment, varMap)
                res = self.cur.fetchone()
                if len(res) == 0:
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    break
                # instantiate Job
                job = JobSpec()
                job.pack(res)

                # sql to read range
                sqlRR = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlRR += "PandaID,job_processID,attemptNr,objStore_ID,zipRow_ID,path_convention "
                sqlRR += f"FROM {panda_config.schemaJEDI}.JEDI_Events tab "
                sqlRR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:eventStatus "
                # sql to read log bucket IDs
                sqlLBK = "SELECT jobMetrics FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "
                sqlLBK += "UNION "
                sqlLBK += "SELECT jobMetrics FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30) "
                # read files
                sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
                sqlFile += "WHERE PandaID=:PandaID ORDER BY row_ID "
                # read LFN and dataset name for output files
                sqlFileOut = "SELECT lfn,dataset FROM ATLAS_PANDA.filesTable4 "
                sqlFileOut += "WHERE PandaID=:PandaID AND type=:type "
                # read files from JEDI for jumbo jobs
                sqlFileJEDI = "SELECT lfn,GUID,fsize,checksum "
                sqlFileJEDI += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlFileJEDI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                sqlFileJEDI += "ORDER BY lfn "
                # read zip file
                sqlZipFile = "SELECT lfn,destinationSE,fsize,checksum FROM ATLAS_PANDA.filesTable4 "
                sqlZipFile += "WHERE row_ID=:row_ID "
                sqlZipFile += "UNION "
                sqlZipFile += "SELECT lfn,destinationSE,fsize,checksum FROM ATLAS_PANDAARCH.filesTable_ARCH "
                sqlZipFile += "WHERE row_ID=:row_ID "
                self.cur.arraysize = 10000
                self.cur.execute(sqlFile + comment, varMap)
                resFs = self.cur.fetchall()
                eventRangeIDs = {}
                esDonePandaIDs = []
                esOutputZipMap = {}
                esZipRow_IDs = set()
                esOutputFileMap = {}
                # use new file format for ES
                useNewFileFormatForES = False
                if job.AtlasRelease is not None:
                    try:
                        tmpMajorVer = job.AtlasRelease.split("-")[-1].split(".")[0]
                        if int(tmpMajorVer) == 20:
                            useNewFileFormatForES = True
                    except Exception:
                        pass
                for resF in resFs:
                    file = FileSpec()
                    file.pack(resF)
                    # add files except event service merge or jumbo
                    if (not EventServiceUtils.isEventServiceMerge(job) and not EventServiceUtils.isJumboJob(job)) or file.type in ["output", "log"]:
                        job.addFile(file)
                    # read real input files for jumbo jobs
                    elif EventServiceUtils.isJumboJob(job):
                        # get files
                        varMap = {}
                        varMap[":jediTaskID"] = file.jediTaskID
                        varMap[":datasetID"] = file.datasetID
                        self.cur.execute(sqlFileJEDI + comment, varMap)
                        resFileJEDI = self.cur.fetchall()
                        for tmpLFN, tmpGUID, tmpFsize, tmpChecksum in resFileJEDI:
                            newFileSpec = FileSpec()
                            newFileSpec.pack(resF)
                            newFileSpec.lfn = tmpLFN
                            newFileSpec.GUID = tmpGUID
                            newFileSpec.fsize = tmpFsize
                            newFileSpec.checksum = tmpChecksum
                            # add file
                            job.addFile(newFileSpec)
                        continue
                    # construct input files from event ranges for event service merge
                    if EventServiceUtils.isEventServiceMerge(job):
                        # only for input
                        if file.type not in ["output", "log"]:
                            # get ranges
                            varMap = {}
                            varMap[":jediTaskID"] = file.jediTaskID
                            varMap[":datasetID"] = file.datasetID
                            varMap[":fileID"] = file.fileID
                            varMap[":eventStatus"] = EventServiceUtils.ST_done
                            self.cur.execute(sqlRR + comment, varMap)
                            resRR = self.cur.fetchall()
                            for (
                                esPandaID,
                                job_processID,
                                attemptNr,
                                objStoreID,
                                zipRow_ID,
                                pathConvention,
                            ) in resRR:
                                tmpEventRangeID = get_task_event_module(self).makeEventRangeID(
                                    file.jediTaskID,
                                    esPandaID,
                                    file.fileID,
                                    job_processID,
                                    attemptNr,
                                )
                                if file.fileID not in eventRangeIDs:
                                    eventRangeIDs[file.fileID] = {}
                                addFlag = False
                                if job_processID not in eventRangeIDs[file.fileID]:
                                    addFlag = True
                                else:
                                    oldEsPandaID = eventRangeIDs[file.fileID][job_processID]["pandaID"]
                                    if esPandaID > oldEsPandaID:
                                        addFlag = True
                                        if oldEsPandaID in esDonePandaIDs:
                                            esDonePandaIDs.remove(oldEsPandaID)
                                if addFlag:
                                    # append
                                    if pathConvention is not None:
                                        objStoreID = f"{objStoreID}/{pathConvention}"
                                    eventRangeIDs[file.fileID][job_processID] = {
                                        "pandaID": esPandaID,
                                        "eventRangeID": tmpEventRangeID,
                                        "objStoreID": objStoreID,
                                    }
                                    # zip file in jobMetrics
                                    if esPandaID not in esDonePandaIDs:
                                        esDonePandaIDs.append(esPandaID)
                                        # get jobMetrics
                                        varMap = {}
                                        varMap[":PandaID"] = esPandaID
                                        self.cur.execute(sqlLBK + comment, varMap)
                                        resLBK = self.cur.fetchone()
                                        if resLBK is not None and resLBK[0] is not None:
                                            outputZipBucketID = None
                                            tmpPatch = re.search("outputZipBucketID=(\d+)", resLBK[0])
                                            if tmpPatch is not None:
                                                outputZipBucketID = tmpPatch.group(1)
                                            outputZipName = None
                                            tmpPatch = re.search("outputZipName=([^ ]+)", resLBK[0])
                                            if tmpPatch is not None:
                                                outputZipName = tmpPatch.group(1)
                                            if outputZipBucketID is not None and outputZipName is not None:
                                                if esPandaID not in esOutputZipMap:
                                                    esOutputZipMap[esPandaID] = []
                                                esOutputZipMap[esPandaID].append(
                                                    {
                                                        "name": outputZipName,
                                                        "osid": outputZipBucketID,
                                                    }
                                                )
                                    # output LFN and dataset
                                    if esPandaID not in esOutputFileMap:
                                        esOutputFileMap[esPandaID] = dict()
                                        varMap = {}
                                        varMap[":PandaID"] = esPandaID
                                        varMap[":type"] = "output"
                                        self.cur.execute(sqlFileOut + comment, varMap)
                                        resFileOut = self.cur.fetchall()
                                        for tmpOutLFN, tmpOutDataset in resFileOut:
                                            esOutputFileMap[esPandaID][tmpOutDataset] = tmpOutLFN
                                # zip file in fileTable
                                if zipRow_ID is not None and zipRow_ID not in esZipRow_IDs:
                                    esZipRow_IDs.add(zipRow_ID)
                                    varMap = {}
                                    varMap[":row_ID"] = zipRow_ID
                                    self.cur.execute(sqlZipFile + comment, varMap)
                                    resZip = self.cur.fetchone()
                                    if resZip is not None:
                                        (
                                            outputZipName,
                                            outputZipBucketID,
                                            outputZipFsize,
                                            outputZipChecksum,
                                        ) = resZip
                                        if esPandaID not in esOutputZipMap:
                                            esOutputZipMap[esPandaID] = []
                                        esOutputZipMap[esPandaID].append(
                                            {
                                                "name": outputZipName,
                                                "osid": outputZipBucketID,
                                                "fsize": outputZipFsize,
                                                "checksum": outputZipChecksum,
                                            }
                                        )
                # make input for event service output merging
                mergeInputOutputMap = {}
                mergeInputFiles = []
                mergeFileObjStoreMap = {}
                mergeZipPandaIDs = []
                for tmpFileID in eventRangeIDs:
                    tmpMapEventRangeID = eventRangeIDs[tmpFileID]
                    jobProcessIDs = sorted(tmpMapEventRangeID)
                    # make input
                    for jobProcessID in jobProcessIDs:
                        for tmpFileSpec in job.Files:
                            if tmpFileSpec.type not in ["output"]:
                                continue
                            esPandaID = tmpMapEventRangeID[jobProcessID]["pandaID"]
                            tmpInputFileSpec = copy.copy(tmpFileSpec)
                            tmpInputFileSpec.type = "input"
                            outLFN = tmpInputFileSpec.lfn
                            # change LFN
                            if esPandaID in esOutputFileMap and tmpInputFileSpec.dataset in esOutputFileMap[esPandaID]:
                                tmpInputFileSpec.lfn = esOutputFileMap[esPandaID][tmpInputFileSpec.dataset]
                            # change attemptNr back to the original, which could have been changed by ES merge retry
                            if not useNewFileFormatForES:
                                origLFN = re.sub("\.\d+$", ".1", tmpInputFileSpec.lfn)
                                outLFN = re.sub("\.\d+$", ".1", outLFN)
                            else:
                                origLFN = re.sub("\.\d+$", ".1_000", tmpInputFileSpec.lfn)
                                outLFN = re.sub("\.\d+$", ".1_000", outLFN)
                            # append eventRangeID as suffix
                            tmpInputFileSpec.lfn = origLFN + "." + tmpMapEventRangeID[jobProcessID]["eventRangeID"]
                            # make input/output map
                            if outLFN not in mergeInputOutputMap:
                                mergeInputOutputMap[outLFN] = []
                            mergeInputOutputMap[outLFN].append(tmpInputFileSpec.lfn)
                            # add file
                            if esPandaID not in esOutputZipMap:
                                # no zip
                                mergeInputFiles.append(tmpInputFileSpec)
                                # mapping for ObjStore
                                mergeFileObjStoreMap[tmpInputFileSpec.lfn] = tmpMapEventRangeID[jobProcessID]["objStoreID"]
                            elif esPandaID not in mergeZipPandaIDs:
                                # zip
                                mergeZipPandaIDs.append(esPandaID)
                                for tmpEsOutZipFile in esOutputZipMap[esPandaID]:
                                    # copy for zip
                                    tmpZipInputFileSpec = copy.copy(tmpInputFileSpec)
                                    # add prefix
                                    tmpZipInputFileSpec.lfn = "zip://" + tmpEsOutZipFile["name"]
                                    if "fsize" in tmpEsOutZipFile:
                                        tmpZipInputFileSpec.fsize = tmpEsOutZipFile["fsize"]
                                    if "checksum" in tmpEsOutZipFile:
                                        tmpZipInputFileSpec.checksum = tmpEsOutZipFile["checksum"]
                                    mergeInputFiles.append(tmpZipInputFileSpec)
                                    # mapping for ObjStore
                                    mergeFileObjStoreMap[tmpZipInputFileSpec.lfn] = tmpEsOutZipFile["osid"]
                for tmpInputFileSpec in mergeInputFiles:
                    job.addFile(tmpInputFileSpec)

                # job parameters
                sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                varMap = {}
                varMap[":PandaID"] = job.PandaID
                self.cur.execute(sqlJobP + comment, varMap)
                for (clobJobP,) in self.cur:
                    try:
                        job.jobParameters = clobJobP.read()
                    except AttributeError:
                        job.jobParameters = str(clobJobP)
                    break

                # remove or extract parameters for merge
                if EventServiceUtils.isEventServiceJob(job) or EventServiceUtils.isJumboJob(job) or EventServiceUtils.isCoJumboJob(job):
                    try:
                        job.jobParameters = re.sub(
                            "<PANDA_ESMERGE_.+>.*</PANDA_ESMERGE_.+>",
                            "",
                            job.jobParameters,
                        )
                    except Exception:
                        pass
                    # sort files since file order is important for positional event number
                    job.sortFiles()
                elif EventServiceUtils.isEventServiceMerge(job):
                    try:
                        origJobParameters = job.jobParameters
                        tmpMatch = re.search(
                            "<PANDA_ESMERGE_JOBP>(.*)</PANDA_ESMERGE_JOBP>",
                            origJobParameters,
                        )
                        job.jobParameters = tmpMatch.group(1)
                        tmpMatch = re.search(
                            "<PANDA_ESMERGE_TRF>(.*)</PANDA_ESMERGE_TRF>",
                            origJobParameters,
                        )
                        job.transformation = tmpMatch.group(1)
                    except Exception:
                        pass
                    # pass in/out map for merging via metadata
                    job.metadata = [mergeInputOutputMap, mergeFileObjStoreMap]

                # read task parameters
                if job.lockedby == "jedi":
                    sqlTP = f"SELECT ioIntensity,ioIntensityUnit FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
                    varMap = {}
                    varMap[":jediTaskID"] = job.jediTaskID
                    self.cur.execute(sqlTP + comment, varMap)
                    resTP = self.cur.fetchone()
                    if resTP is not None:
                        ioIntensity, ioIntensityUnit = resTP
                        job.set_task_attribute("ioIntensity", ioIntensity)
                        job.set_task_attribute("ioIntensityUnit", ioIntensityUnit)

                if not self._commit():
                    raise RuntimeError("Commit error")

                # append the job to the returned list
                retJobs.append(job)

                # record status change
                try:
                    self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
                except Exception:
                    tmp_log.error("recordStatusChange in getJobs")
                self.push_job_status_message(job, job.PandaID, job.jobStatus)
                if via_topic and job.is_push_job():
                    tmp_log.debug("delete job message")
                    mb_proxy_queue = self.get_mb_proxy("panda_pilot_queue")
                    srv_msg_utils.delete_job_message(mb_proxy_queue, job.PandaID)
            return retJobs, nSent
        except Exception as e:
            self.dump_error_message(tmp_log)
            # roll back
            self._rollback()
            return [], 0

    # record retry history
    def recordRetryHistoryJEDI(self, jediTaskID, newPandaID, oldPandaIDs, relationType, no_late_bulk_exec=True, extracted_sqls=None):
        comment = " /* DBProxy.recordRetryHistoryJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={newPandaID}")
        tmp_log.debug("start")
        # sql to check record
        sqlCK = f"SELECT jediTaskID FROM {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
        sqlCK += "WHERE jediTaskID=:jediTaskID AND oldPandaID=:oldPandaID AND newPandaID=:newPandaID AND originPandaID=:originPandaID "
        # sql to insert record
        sqlIN = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
        if relationType is None:
            sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID) "
            sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID) "
        else:
            sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID,relationType) "
            sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID,:relationType) "
        for oldPandaID in oldPandaIDs:
            # get origin
            originIDs = self.getOriginPandaIDsJEDI(oldPandaID, jediTaskID, self.cur)
            for originID in originIDs:
                # check
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":oldPandaID"] = oldPandaID
                varMap[":newPandaID"] = newPandaID
                varMap[":originPandaID"] = originID
                self.cur.execute(sqlCK + comment, varMap)
                resCK = self.cur.fetchone()
                # insert
                if resCK is None:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":oldPandaID"] = oldPandaID
                    varMap[":newPandaID"] = newPandaID
                    varMap[":originPandaID"] = originID
                    if relationType is not None:
                        varMap[":relationType"] = relationType
                    if no_late_bulk_exec:
                        self.cur.execute(sqlIN + comment, varMap)
                    else:
                        extracted_sqls.setdefault("retry_history", {"sql": sqlIN + comment, "vars": []})
                        extracted_sqls["retry_history"]["vars"].append(varMap)
        # return
        tmp_log.debug("done")

    # extract scope from dataset name
    def extractScope(self, name):
        try:
            if name.lower().startswith("user") or name.lower().startswith("group"):
                # return None if there are not enough fields
                if len(name.split(".")) < 2:
                    return None
                # check if user scope needs to be in lowercase
                user_scope_in_lowercase = True
                try:
                    if self.jedi_config and hasattr(self.jedi_config.ddm, "user_scope_in_lowercase") and self.jedi_config.ddm.user_scope_in_lowercase is False:
                        user_scope_in_lowercase = False
                except Exception:
                    pass
                if user_scope_in_lowercase:
                    name = name.lower()
                scope = ".".join(name.split(".")[:2])
            else:
                scope = name.split(".")[0]
            return scope
        except Exception as e:
            return None

    # insert job to jobsDefined
    def insertNewJob(
        self,
        job,
        user,
        serNum,
        weight=0.0,
        priorityOffset=0,
        userVO=None,
        toPending=False,
        origEsJob=False,
        eventServiceInfo=None,
        oldPandaIDs=None,
        relationType=None,
        fileIDPool=[],
        origSpecialHandling=None,
        unprocessedMap=None,
        prio_reduction=True,
        no_late_bulk_exec=True,
        extracted_sqls=None,
        new_jobset_id=None,
    ):
        comment = " /* DBProxy.insertNewJob */"
        tmp_log = self.create_tagged_logger(comment, f"<JediTaskID={job.jediTaskID} idPool={len(fileIDPool)}")

        # insert jobs to jobsDefined4
        table_name = "jobsDefined4"
        if not toPending:
            # direct submission to the PanDA server
            job.jobStatus = "defined"
        elif job.computingSite == EventServiceUtils.siteIdForWaitingCoJumboJobs:
            # put co-jumbo jobs in waiting
            job.jobStatus = "waiting"
        else:
            # jobs from JEDI
            job.jobStatus = "pending"

        sql1 = f"INSERT INTO {panda_config.schemaPANDA}.{table_name} ({JobSpec.columnNames()}) "
        sql1 += JobSpec.bindValuesExpression(useSeq=False)

        # host and time information
        job.modificationHost = self.hostname
        job.creationTime = naive_utcnow()
        job.modificationTime = job.creationTime
        job.stateChangeTime = job.creationTime
        job.prodDBUpdateTime = job.creationTime
        # DN
        if job.prodUserID == "NULL" or job.prodSourceLabel in ["user", "panda"]:
            job.prodUserID = user

        # compact username
        job.prodUserName = CoreUtils.clean_user_id(job.prodUserID)
        if job.prodUserName in ["", "NULL"]:
            # use prodUserID as compact username
            job.prodUserName = job.prodUserID

        # VO
        job.VO = userVO

        # priority
        if job.assignedPriority != "NULL":
            job.currentPriority = job.assignedPriority
        if job.prodSourceLabel == "install":
            job.currentPriority = 4100
        elif job.prodUserName in ["artprod"] and job.prodSourceLabel in [
            "user",
            "panda",
        ]:
            job.currentPriority = 7000
        elif job.prodSourceLabel == "user":
            if job.processingType == "pmerge" and job.currentPriority not in ["NULL", None]:
                # avoid prio reduction for merge jobs
                pass
            else:
                if not prio_reduction:
                    job.currentPriority = priorityOffset
                    if job.isScoutJob():
                        job.currentPriority += 1
                elif job.currentPriority not in ["NULL", None] and (job.isScoutJob() or job.currentPriority >= JobUtils.priorityTasksToJumpOver):
                    pass
                else:
                    job.currentPriority = PrioUtil.calculatePriority(priorityOffset, serNum, weight)
                    if "express" in job.specialHandling:
                        job.currentPriority = 6000
        elif job.prodSourceLabel == "panda":
            job.currentPriority = 2000 + priorityOffset
            if "express" in job.specialHandling:
                job.currentPriority = 6500

        # set attempt numbers
        if job.prodSourceLabel in ["user", "panda"] + JobUtils.list_ptest_prod_sources:
            if job.attemptNr in [None, "NULL", ""]:
                job.attemptNr = 0
            if job.maxAttempt in [None, "NULL", ""]:
                job.maxAttempt = 0
            # set maxAttempt to attemptNr to disable server/pilot retry
            if job.maxAttempt == -1:
                job.maxAttempt = job.attemptNr
            else:
                # set maxAttempt to have server/pilot retries for retried jobs
                if job.maxAttempt <= job.attemptNr:
                    job.maxAttempt = job.attemptNr + 2

        # obtain the share and resource type
        if job.gshare in ("NULL", None, ""):
            job.gshare = get_entity_module(self).get_share_for_job(job)
        tmp_log.debug(f"resource_type is set to {job.resource_type}")
        tmp_log.debug(f"jediTaskID={job.jediTaskID} SH={origSpecialHandling} origEsJob={origEsJob} eInfo={eventServiceInfo}")
        if job.resource_type in ("NULL", None, ""):
            try:
                job.resource_type = get_entity_module(self).get_resource_type_job(job)
                tmp_log.debug(f"reset resource_type to {job.resource_type}")
            except Exception:
                job.resource_type = "Undefined"
                tmp_log.error(f"reset resource_type excepted with: {traceback.format_exc()}")

        try:
            # use JEDI
            if hasattr(panda_config, "useJEDI") and panda_config.useJEDI is True and job.lockedby == "jedi":
                useJEDI = True
            else:
                useJEDI = False

            # begin transaction
            if no_late_bulk_exec:
                self.conn.begin()

            # get jobsetID for event service
            if not no_late_bulk_exec and new_jobset_id is not None:
                job.jobsetID = new_jobset_id
            elif origEsJob:
                if self.backend == "mysql":
                    # fake sequence
                    sql = " INSERT INTO ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ (col) VALUES (NULL) "
                    self.cur.arraysize = 10
                    self.cur.execute(sql + comment, {})
                    sql2 = """ SELECT LAST_INSERT_ID() """
                    self.cur.execute(sql2 + comment, {})
                    (job.jobsetID,) = self.cur.fetchone()
                else:
                    sqlESS = "SELECT ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ.nextval FROM dual "
                    self.cur.arraysize = 10
                    self.cur.execute(sqlESS + comment, {})
                    (job.jobsetID,) = self.cur.fetchone()

            # get originPandaID
            originPandaID = None
            if oldPandaIDs is not None and len(oldPandaIDs) > 0:
                varMap = {}
                varMap[":jediTaskID"] = job.jediTaskID
                varMap[":pandaID"] = oldPandaIDs[0]
                sqlOrigin = f"SELECT originPandaID FROM {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
                sqlOrigin += "WHERE jediTaskID=:jediTaskID AND newPandaID=:pandaID "
                type_var_names_str, type_var_map = get_sql_IN_bind_variables(EventServiceUtils.relationTypesForJS, prefix=":", value_as_suffix=True)
                sqlOrigin += f"AND (relationType IS NULL OR NOT relationType IN ({type_var_names_str})) "
                varMap.update(type_var_map)
                self.cur.execute(sqlOrigin + comment, varMap)
                resOrigin = self.cur.fetchone()
                if resOrigin is not None:
                    (originPandaID,) = resOrigin
                else:
                    originPandaID = oldPandaIDs[0]
            if originPandaID is None:
                originPandaID = job.PandaID
            newJobName = re.sub("\$ORIGINPANDAID", str(originPandaID), job.jobName)
            # update jobName
            if newJobName != job.jobName:
                job.jobName = newJobName

            # insert job
            if no_late_bulk_exec:
                varMap = job.valuesMap(useSeq=False)
                self.cur.execute(sql1 + comment, varMap)
            else:
                extracted_sqls["job"] = {"sql": sql1 + comment, "vars": [job.valuesMap(useSeq=False)]}

            # get jobsetID
            if job.jobsetID in [None, "NULL", -1]:
                jobsetID = 0
            else:
                jobsetID = job.jobsetID
            jobsetID = "%06d" % jobsetID
            try:
                strJediTaskID = str(job.jediTaskID)
            except Exception:
                strJediTaskID = ""

            # reset changed attribute list
            job.resetChangedList()
            # insert files
            tmp_log.debug(f"inserted {job.PandaID} label:{job.prodSourceLabel} prio:{job.currentPriority} jediTaskID:{job.jediTaskID}")
            # sql with SEQ
            sqlFile = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
            sqlFile += FileSpec.bindValuesExpression(useSeq=True)
            sqlFile += " RETURNING row_ID INTO :newRowID"
            # sql without SEQ
            sqlFileW = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
            sqlFileW += FileSpec.bindValuesExpression(useSeq=False)
            dynNumEvents = EventServiceUtils.isDynNumEventsSH(job.specialHandling)
            dynFileMap = {}
            dynLfnIdMap = {}
            totalInputEvents = 0
            indexFileID = 0
            varMapsForFile = []
            nFilesWaitingMap = {}
            nEventsToProcess = 0

            # failed related ES jobs
            if origEsJob and eventServiceInfo is not None and not job.notDiscardEvents():
                get_task_event_module(self).updateRelatedEventServiceJobs(job, killEvents=False, forceFailed=True)
            for file in job.Files:
                file.row_ID = None
                if file.status not in ["ready", "cached"]:
                    file.status = "unknown"
                # replace $PANDAID with real PandaID
                file.lfn = re.sub("\$PANDAID", "%05d" % job.PandaID, file.lfn)
                # replace $JOBSETID with real jobsetID
                if job.prodSourceLabel not in ["managed"]:
                    file.lfn = re.sub("\$JOBSETID", jobsetID, file.lfn)
                    try:
                        file.lfn = re.sub("\$JEDITASKID", strJediTaskID, file.lfn)
                    except Exception:
                        pass
                # avoid duplicated files for dynamic number of events
                toSkipInsert = False
                if dynNumEvents and file.type in ["input", "pseudo_input"]:
                    if file.lfn not in dynFileMap:
                        dynFileMap[file.lfn] = set()
                    else:
                        toSkipInsert = True
                        dynFileMap[file.lfn].add(
                            (
                                file.jediTaskID,
                                file.datasetID,
                                file.fileID,
                                file.attemptNr,
                            )
                        )
                # set scope
                if file.type in ["output", "log"] and job.VO in ["atlas"]:
                    file.scope = self.extractScope(file.dataset)
                # insert
                if not toSkipInsert:
                    if indexFileID < len(fileIDPool):
                        file.row_ID = fileIDPool[indexFileID]
                        varMap = file.valuesMap(useSeq=False)
                        varMapsForFile.append(varMap)
                        indexFileID += 1
                    else:
                        varMap = file.valuesMap(useSeq=True)
                        varMap[":newRowID"] = self.cur.var(varNUMBER)
                        self.cur.execute(sqlFile + comment, varMap)
                        # get rowID
                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newRowID"]))
                        file.row_ID = int(val)
                    dynLfnIdMap[file.lfn] = file.row_ID
                    # reset changed attribute list
                    file.resetChangedList()
                # update JEDI table
                if useJEDI:
                    # skip if no JEDI
                    if file.fileID == "NULL":
                        continue
                    # input for waiting co-jumbo jobs
                    isWaiting = None
                    isFileForWaitingCoJumbo = False
                    if file.type not in ["output", "log"]:
                        if job.computingSite == EventServiceUtils.siteIdForWaitingCoJumboJobs:
                            isFileForWaitingCoJumbo = True
                        # check is_waiting
                        sqlJediFileIsW = "SELECT is_waiting FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                        sqlJediFileIsW += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                        varMap = {}
                        varMap[":fileID"] = file.fileID
                        varMap[":jediTaskID"] = file.jediTaskID
                        varMap[":datasetID"] = file.datasetID
                        self.cur.execute(sqlJediFileIsW + comment, varMap)
                        resJediFileIsW = self.cur.fetchone()
                        if resJediFileIsW is not None:
                            (isWaiting,) = resJediFileIsW
                    # update Dataset_Contents table
                    varMap = {}
                    varMap[":fileID"] = file.fileID
                    if isFileForWaitingCoJumbo:
                        # not change status for wating co-jumbo jobs to allow new jobs to pickup files
                        varMap[":status"] = "picked"
                        varMap[":is_waiting"] = "Y"
                    else:
                        varMap[":status"] = "running"
                    varMap[":oldStatusI"] = "picked"
                    varMap[":oldStatusO"] = "defined"
                    varMap[":attemptNr"] = file.attemptNr
                    varMap[":datasetID"] = file.datasetID
                    varMap[":keepTrack"] = 1
                    varMap[":jediTaskID"] = file.jediTaskID
                    if isFileForWaitingCoJumbo:
                        varMap[":PandaID"] = job.jobsetID
                    else:
                        varMap[":PandaID"] = file.PandaID
                    varMap[":jobsetID"] = job.jobsetID
                    sqlJediFile = "UPDATE /*+ INDEX_RS_ASC(JEDI_DATASET_CONTENTS (JEDI_DATASET_CONTENTS.JEDITASKID JEDI_DATASET_CONTENTS.DATASETID JEDI_DATASET_CONTENTS.FILEID)) */ ATLAS_PANDA.JEDI_Dataset_Contents SET status=:status,PandaID=:PandaID,jobsetID=:jobsetID"
                    if file.type in ["output", "log"]:
                        sqlJediFile += ",outPandaID=:PandaID"
                    if isFileForWaitingCoJumbo:
                        sqlJediFile += ",is_waiting=:is_waiting"
                    else:
                        sqlJediFile += ",is_waiting=NULL"
                    sqlJediFile += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    sqlJediFile += "AND attemptNr=:attemptNr AND status IN (:oldStatusI,:oldStatusO) AND keepTrack=:keepTrack "
                    self.cur.execute(sqlJediFile + comment, varMap)
                    # get number of inputs for waiting co-jumbo jobs
                    if (isFileForWaitingCoJumbo or isWaiting is not None) and self.cur.rowcount > 0:
                        if file.datasetID not in nFilesWaitingMap:
                            nFilesWaitingMap[file.datasetID] = 0
                        if isFileForWaitingCoJumbo and isWaiting is None:
                            nFilesWaitingMap[file.datasetID] += 1
                        elif not isFileForWaitingCoJumbo and isWaiting is not None:
                            nFilesWaitingMap[file.datasetID] -= 1
                    # no insert for dynamic number of events
                    if toSkipInsert:
                        continue
                    # insert events for ES
                    if origEsJob and eventServiceInfo is not None and file.lfn in eventServiceInfo:
                        # discard old successful event ranges
                        sqlJediOdEvt = (
                            "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                        )
                        sqlJediOdEvt += f"{panda_config.schemaJEDI}.JEDI_Events tab "
                        sqlJediOdEvt += "SET status=:newStatus "
                        sqlJediOdEvt += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                        sqlJediOdEvt += "AND status IN (:esFinished,:esDone) "
                        varMap = {}
                        varMap[":jediTaskID"] = file.jediTaskID
                        varMap[":datasetID"] = file.datasetID
                        varMap[":fileID"] = file.fileID
                        if not job.notDiscardEvents():
                            varMap[":newStatus"] = EventServiceUtils.ST_discarded
                        else:
                            varMap[":newStatus"] = EventServiceUtils.ST_done
                        varMap[":esDone"] = EventServiceUtils.ST_done
                        varMap[":esFinished"] = EventServiceUtils.ST_finished
                        tmp_log.debug(sqlJediOdEvt + comment + str(varMap))
                        self.cur.execute(sqlJediOdEvt + comment, varMap)
                        # cancel old unprocessed event ranges
                        sqlJediCEvt = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                        sqlJediCEvt += f"{panda_config.schemaJEDI}.JEDI_Events tab "
                        sqlJediCEvt += "SET status=:newStatus "
                        sqlJediCEvt += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                        sqlJediCEvt += "AND NOT status IN (:esFinished,:esDone,:esDiscarded,:esCancelled,:esFailed,:esFatal,:esCorrupted) "
                        sqlJediCEvt += "AND (is_jumbo IS NULL OR (is_jumbo=:isJumbo AND status NOT IN (:esSent,:esRunning))) "
                        varMap[":newStatus"] = EventServiceUtils.ST_cancelled
                        varMap[":esDiscarded"] = EventServiceUtils.ST_discarded
                        varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
                        varMap[":esCorrupted"] = EventServiceUtils.ST_corrupted
                        varMap[":esFatal"] = EventServiceUtils.ST_fatal
                        varMap[":esFailed"] = EventServiceUtils.ST_failed
                        varMap[":esSent"] = EventServiceUtils.ST_sent
                        varMap[":esRunning"] = EventServiceUtils.ST_running
                        varMap[":isJumbo"] = EventServiceUtils.eventTableIsJumbo
                        tmp_log.debug(sqlJediCEvt + comment + str(varMap))
                        self.cur.execute(sqlJediCEvt + comment, varMap)
                        # unset processed_upto for old failed events
                        sqlJediFEvt = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                        sqlJediFEvt += f"{panda_config.schemaJEDI}.JEDI_Events tab "
                        sqlJediFEvt += "SET processed_upto_eventID=NULL "
                        sqlJediFEvt += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                        sqlJediFEvt += "AND status=:esFailed AND processed_upto_eventID IS NOT NULL "
                        varMap = {}
                        varMap[":jediTaskID"] = file.jediTaskID
                        varMap[":datasetID"] = file.datasetID
                        varMap[":fileID"] = file.fileID
                        varMap[":esFailed"] = EventServiceUtils.ST_failed
                        tmp_log.debug(sqlJediFEvt + comment + str(varMap))
                        self.cur.execute(sqlJediFEvt + comment, varMap)

                        # get successful event ranges
                        okRanges = set()
                        if job.notDiscardEvents():
                            sqlJediOks = (
                                "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                            )
                            sqlJediOks += f"jediTaskID,fileID,job_processID FROM {panda_config.schemaJEDI}.JEDI_Events tab "
                            sqlJediOks += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                            sqlJediOks += "AND (status=:esDone OR (is_jumbo=:isJumbo AND status IN (:esSent,:esRunning))) "
                            varMap = {}
                            varMap[":jediTaskID"] = file.jediTaskID
                            varMap[":datasetID"] = file.datasetID
                            varMap[":fileID"] = file.fileID
                            varMap[":esDone"] = EventServiceUtils.ST_done
                            varMap[":esSent"] = EventServiceUtils.ST_sent
                            varMap[":esRunning"] = EventServiceUtils.ST_running
                            varMap[":isJumbo"] = EventServiceUtils.eventTableIsJumbo
                            self.cur.execute(sqlJediOks + comment, varMap)
                            resOks = self.cur.fetchall()
                            for (
                                tmpOk_jediTaskID,
                                tmpOk_fileID,
                                tmpOk_job_processID,
                            ) in resOks:
                                okRanges.add(f"{tmpOk_jediTaskID}-{tmpOk_fileID}-{tmpOk_job_processID}")
                        # insert new ranges
                        sqlJediEvent = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Events "
                        sqlJediEvent += "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
                        sqlJediEvent += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID,"
                        sqlJediEvent += "event_offset"
                        sqlJediEvent += ") "
                        sqlJediEvent += "VALUES(:jediTaskID,:datasetID,:pandaID,:fileID,:attemptNr,:eventStatus,"
                        sqlJediEvent += ":startEvent,:startEvent,:lastEvent,:processedEvent,"
                        sqlJediEvent += ":eventOffset"
                        sqlJediEvent += ") "
                        varMaps = []
                        iEvent = 1
                        while iEvent <= eventServiceInfo[file.lfn]["nEvents"]:
                            varMap = {}
                            varMap[":jediTaskID"] = file.jediTaskID
                            varMap[":datasetID"] = file.datasetID
                            varMap[":pandaID"] = job.jobsetID
                            varMap[":fileID"] = file.fileID
                            varMap[":attemptNr"] = eventServiceInfo[file.lfn]["maxAttempt"]
                            varMap[":eventStatus"] = EventServiceUtils.ST_ready
                            varMap[":processedEvent"] = 0
                            varMap[":startEvent"] = eventServiceInfo[file.lfn]["startEvent"] + iEvent
                            iEvent += eventServiceInfo[file.lfn]["nEventsPerRange"]
                            if iEvent > eventServiceInfo[file.lfn]["nEvents"]:
                                iEvent = eventServiceInfo[file.lfn]["nEvents"] + 1
                            lastEvent = eventServiceInfo[file.lfn]["startEvent"] + iEvent - 1
                            varMap[":lastEvent"] = lastEvent
                            # add offset for positional event numbers
                            if not job.inFilePosEvtNum():
                                varMap[":startEvent"] += totalInputEvents
                                varMap[":lastEvent"] += totalInputEvents
                            # keep jobsetID
                            varMap[":eventOffset"] = job.jobsetID
                            # skip if already succeeded
                            tmpKey = f"{varMap[':jediTaskID']}-{varMap[':fileID']}-{varMap[':startEvent']}"
                            if tmpKey in okRanges:
                                continue
                            varMaps.append(varMap)
                            nEventsToProcess += 1
                        tmp_log.debug(f"{job.PandaID} insert {len(varMaps)} event ranges jediTaskID:{job.jediTaskID}")
                        if no_late_bulk_exec:
                            self.cur.executemany(sqlJediEvent + comment, varMaps)
                        else:
                            extracted_sqls["event"] = {"sql": sqlJediEvent + comment, "vars": varMaps}
                        tmp_log.debug(f"{job.PandaID} inserted {len(varMaps)} event ranges jediTaskID:{job.jediTaskID}")
                        totalInputEvents += eventServiceInfo[file.lfn]["nEvents"]
            if job.notDiscardEvents() and origEsJob and nEventsToProcess == 0:
                job.setAllOkEvents()
                sqlJediJSH = "UPDATE ATLAS_PANDA.jobsDefined4 "
                sqlJediJSH += "SET specialHandling=:specialHandling WHERE PandaID=:PandaID "
                varMap = dict()
                varMap[":specialHandling"] = job.specialHandling
                varMap[":PandaID"] = job.PandaID
                self.cur.execute(sqlJediJSH + comment, varMap)
            # use score if not so many events are available
            if origEsJob and unprocessedMap is not None:
                unprocessedMap[job.jobsetID] = nEventsToProcess
            if EventServiceUtils.isEventServiceJob(job) and not EventServiceUtils.isJobCloningJob(job) and unprocessedMap is not None:
                if job.coreCount not in [None, "", "NULL"] and job.coreCount > 1:
                    minUnprocessed = self.getConfigValue("dbproxy", "AES_MINEVENTSFORMCORE")
                    if minUnprocessed is not None:
                        minUnprocessed = max(minUnprocessed, job.coreCount)
                        if unprocessedMap[job.jobsetID] < minUnprocessed and unprocessedMap[job.jobsetID] > 0:
                            get_task_event_module(self).setScoreSiteToEs(job, f"insertNewJob : {job.PandaID}", comment)
            # bulk insert files
            if len(varMapsForFile) > 0:
                tmp_log.debug(f"{job.PandaID} bulk insert {len(varMapsForFile)} files for jediTaskID:{job.jediTaskID}")
                if no_late_bulk_exec:
                    self.cur.executemany(sqlFileW + comment, varMapsForFile)
                else:
                    extracted_sqls["file"] = {"sql": sqlFileW + comment, "vars": varMapsForFile}
            # update nFilesWaiting
            if len(nFilesWaitingMap) > 0:
                sqlJediNFW = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlJediNFW += "SET nFilesWaiting=nFilesWaiting+:nDiff "
                sqlJediNFW += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                for tmpDatasetID in nFilesWaitingMap:
                    nDiff = nFilesWaitingMap[tmpDatasetID]
                    varMap = {}
                    varMap[":jediTaskID"] = job.jediTaskID
                    varMap[":datasetID"] = tmpDatasetID
                    varMap[":nDiff"] = nDiff
                    self.cur.execute(sqlJediNFW + comment, varMap)
            # insert events for dynamic number of events
            if dynFileMap != {}:
                # insert new ranges
                sqlJediEvent = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Events "
                sqlJediEvent += "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
                sqlJediEvent += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID) "
                sqlJediEvent += "VALUES(:jediTaskID,:datasetID,:pandaID,:fileID,:attemptNr,:eventStatus,"
                sqlJediEvent += ":processID,:startEvent,:lastEvent,:processedEvent) "
                varMaps = []
                for tmpLFN in dynFileMap:
                    dynFiles = dynFileMap[tmpLFN]
                    for (
                        tmpJediTaskID,
                        tmpDatasetID,
                        tmpFileID,
                        tmpAttemptNr,
                    ) in dynFiles:
                        varMap = {}
                        varMap[":jediTaskID"] = tmpJediTaskID
                        varMap[":datasetID"] = tmpDatasetID
                        varMap[":pandaID"] = job.PandaID
                        varMap[":fileID"] = tmpFileID
                        varMap[":attemptNr"] = tmpAttemptNr + 1  # to avoid 0
                        varMap[":eventStatus"] = EventServiceUtils.ST_discarded
                        varMap[":processID"] = dynLfnIdMap[tmpLFN]
                        varMap[":processedEvent"] = -1
                        varMap[":startEvent"] = 0
                        varMap[":lastEvent"] = 0
                        varMaps.append(varMap)
                if no_late_bulk_exec:
                    self.cur.executemany(sqlJediEvent + comment, varMaps)
                else:
                    extracted_sqls["dynamic"] = {"sql": sqlJediEvent + comment, "vars": varMaps}
                tmp_log.debug(f"{job.PandaID} inserted {len(varMaps)} dyn events jediTaskID:{job.jediTaskID}")
            # update t_task
            if useJEDI and job.prodSourceLabel not in ["panda"] and job.processingType != "pmerge":
                varMap = {}
                varMap[":jediTaskID"] = job.jediTaskID
                varMap[":nJobs"] = 1
                schemaDEFT = panda_config.schemaDEFT
                sqlTtask = f"UPDATE {schemaDEFT}.T_TASK "
                sqlTtask += "SET total_req_jobs=total_req_jobs+:nJobs,timestamp=CURRENT_DATE "
                sqlTtask += "WHERE taskid=:jediTaskID "
                if no_late_bulk_exec:
                    tmp_log.debug(sqlTtask + comment + str(varMap))
                    self.cur.execute(sqlTtask + comment, varMap)
                else:
                    extracted_sqls["t_task"] = {"sql": sqlTtask + comment, "vars": [varMap]}
                tmp_log.debug(f"{job.PandaID} updated T_TASK jediTaskID:{job.jediTaskID}")
            # metadata
            if job.prodSourceLabel in ["user", "panda"] and job.metadata != "":
                sqlMeta = "INSERT INTO ATLAS_PANDA.metaTable (PandaID,metaData) VALUES (:PandaID,:metaData)"
                varMap = {}
                varMap[":PandaID"] = job.PandaID
                varMap[":metaData"] = job.metadata
                tmp_log.debug(f"{job.PandaID} inserting meta jediTaskID:{job.jediTaskID}")
                if no_late_bulk_exec:
                    self.cur.execute(sqlMeta + comment, varMap)
                else:
                    extracted_sqls["meta"] = {"sql": sqlMeta + comment, "vars": [varMap]}
                tmp_log.debug(f"{job.PandaID} inserted meta jediTaskID:{job.jediTaskID}")
            # job parameters
            if job.prodSourceLabel not in ["managed"]:
                job.jobParameters = re.sub("\$JOBSETID", jobsetID, job.jobParameters)
                try:
                    job.jobParameters = re.sub("\$JEDITASKID", strJediTaskID, job.jobParameters)
                except Exception:
                    pass
            sqlJob = "INSERT INTO ATLAS_PANDA.jobParamsTable (PandaID,jobParameters) VALUES (:PandaID,:param)"
            varMap = {}
            varMap[":PandaID"] = job.PandaID
            varMap[":param"] = job.jobParameters
            tmp_log.debug(f"{job.PandaID} inserting jobParam jediTaskID:{job.jediTaskID}")
            if no_late_bulk_exec:
                self.cur.execute(sqlJob + comment, varMap)
            else:
                extracted_sqls["jobparams"] = {"sql": sqlJob + comment, "vars": [varMap]}
            tmp_log.debug(f"{job.PandaID} inserted jobParam jediTaskID:{job.jediTaskID}")
            # update input
            if (
                useJEDI
                and not EventServiceUtils.isJumboJob(job)
                and job.computingSite != EventServiceUtils.siteIdForWaitingCoJumboJobs
                and not (EventServiceUtils.isEventServiceJob(job) and not origEsJob)
            ):
                get_task_event_module(self).updateInputStatusJedi(
                    job.jediTaskID, job.PandaID, "queued", no_late_bulk_exec=no_late_bulk_exec, extracted_sqls=extracted_sqls
                )
            # record retry history
            if oldPandaIDs is not None and len(oldPandaIDs) > 0:
                tmp_log.debug(f"{job.PandaID} recording history nOld={len(oldPandaIDs)} jediTaskID:{job.jediTaskID}")
                self.recordRetryHistoryJEDI(job.jediTaskID, job.PandaID, oldPandaIDs, relationType, no_late_bulk_exec, extracted_sqls)
                tmp_log.debug(f"{job.PandaID} recorded history jediTaskID:{job.jediTaskID}")
            # record jobset
            if origEsJob:
                self.recordRetryHistoryJEDI(
                    job.jediTaskID,
                    job.PandaID,
                    [job.jobsetID],
                    EventServiceUtils.relationTypeJS_ID,
                    no_late_bulk_exec,
                    extracted_sqls,
                )
                # record jobset history
                if oldPandaIDs is not None and len(oldPandaIDs) > 0:
                    # get old jobsetID
                    for oldPandaID in oldPandaIDs:
                        oldJobsetID = self.getJobsetIDforPandaID(oldPandaID, job.jediTaskID)
                        if oldJobsetID is not None:
                            self.recordRetryHistoryJEDI(
                                job.jediTaskID,
                                job.jobsetID,
                                [oldJobsetID],
                                EventServiceUtils.relationTypeJS_Retry,
                                no_late_bulk_exec,
                                extracted_sqls,
                            )
            # record jobset mapping for event service
            if EventServiceUtils.isEventServiceJob(job) and EventServiceUtils.isResurrectConsumers(job.specialHandling):
                self.recordRetryHistoryJEDI(
                    job.jediTaskID,
                    job.jobsetID,
                    [job.PandaID],
                    EventServiceUtils.relationTypeJS_Map,
                    no_late_bulk_exec,
                    extracted_sqls,
                )
            if no_late_bulk_exec:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                tmp_log.debug(f"{job.PandaID} all OK jediTaskID:{job.jediTaskID}")
                # record status change
                try:
                    self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
                except Exception:
                    tmp_log.error("recordStatusChange in insertNewJob")
                self.push_job_status_message(job, job.PandaID, job.jobStatus, job.jediTaskID, origSpecialHandling)
            else:
                self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job, no_late_bulk_exec=False, extracted_sqls=extracted_sqls)
            if unprocessedMap is not None:
                return True, unprocessedMap
            return True
        except Exception:
            # roll back
            if no_late_bulk_exec:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            if unprocessedMap is not None:
                return False, unprocessedMap
            return False

    # bulk insert new jobs
    def bulk_insert_new_jobs(self, jedi_task_id, arg_list, new_jobset_id_list, special_handling_list):
        comment = " /* DBProxy.bulk_insert_new_jobs */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        try:
            start_time = naive_utcnow()
            tmp_log.debug("start")
            sql_key_list = ["job", "event", "file", "dynamic", "t_task", "meta", "jobparams", "retry_history", "state_change", "jedi_input"]
            self.conn.begin()
            return_list = []
            extracted_sqls = {}
            es_jobset_map = {}
            for args, kwargs, extra_params in arg_list:
                tmp_extracted_sqls = {}
                new_kwargs = {
                    "no_late_bulk_exec": False,
                    "extracted_sqls": tmp_extracted_sqls,
                }
                if kwargs.get("origEsJob"):
                    new_kwargs["new_jobset_id"] = new_jobset_id_list.pop(0)
                    if extra_params["esIndex"]:
                        es_jobset_map[extra_params["esIndex"]] = new_kwargs["new_jobset_id"]
                elif kwargs.get("eventServiceInfo"):
                    if extra_params["esIndex"] in es_jobset_map:
                        new_kwargs["new_jobset_id"] = es_jobset_map[extra_params["esIndex"]]
                kwargs.update(new_kwargs)
                ret = self.insertNewJob(*args, **kwargs)
                job = args[0]
                if kwargs.get("unprocessedMap") is not None:
                    tmp_ret, _ = ret
                else:
                    tmp_ret = ret
                if not tmp_ret:
                    job.PandaID = None
                else:
                    # combine SQLs
                    for target_key in sql_key_list:
                        if target_key in tmp_extracted_sqls:
                            extracted_sqls.setdefault(target_key, {"sqls": [], "vars": {}})
                            if tmp_extracted_sqls[target_key]["sql"] not in extracted_sqls[target_key]["sqls"]:
                                extracted_sqls[target_key]["sqls"].append(tmp_extracted_sqls[target_key]["sql"])
                                extracted_sqls[target_key]["vars"][tmp_extracted_sqls[target_key]["sql"]] = []
                            extracted_sqls[target_key]["vars"][tmp_extracted_sqls[target_key]["sql"]] += tmp_extracted_sqls[target_key]["vars"]
                return_list.append([job, ret])
            # consolidate SQLs for t_task
            if "t_task" in extracted_sqls:
                for sql in extracted_sqls["t_task"]["sqls"]:
                    old_vars = extracted_sqls["t_task"]["vars"][sql]
                    n_jobs_map = {}
                    for var in old_vars:
                        n_jobs_map.setdefault(var[":jediTaskID"], 0)
                        n_jobs_map[var[":jediTaskID"]] += var[":nJobs"]
                    extracted_sqls["t_task"]["vars"][sql] = []
                    for k, v in n_jobs_map.items():
                        extracted_sqls["t_task"]["vars"][sql].append({":jediTaskID": k, ":nJobs": v})
            # bulk execution
            tmp_log.debug(f"bulk execution for {len(arg_list)} jobs")
            for target_key in sql_key_list:
                if target_key not in extracted_sqls:
                    continue
                for sql in extracted_sqls[target_key]["sqls"]:
                    self.cur.executemany(sql, extracted_sqls[target_key]["vars"][sql])
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # send messages
            for (job, _), special_handling in zip(return_list, special_handling_list):
                self.push_job_status_message(job, job.PandaID, job.jobStatus, job.jediTaskID, special_handling)
            exec_time = naive_utcnow() - start_time
            tmp_log.debug("done OK. took %s.%03d sec" % (exec_time.seconds, exec_time.microseconds / 1000))
            return True, return_list, es_jobset_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            exec_time = naive_utcnow() - start_time
            tmp_log.debug("done NG. took %s.%03d sec" % (exec_time.seconds, exec_time.microseconds / 1000))
            return False, None, None

    # get origin PandaIDs
    def getOriginPandaIDsJEDI(self, pandaID, jediTaskID, cur):
        comment = " /* DBProxy.getOriginPandaIDsJEDI */"
        # get parent IDs
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":newPandaID"] = pandaID
        sqlFJ = f"SELECT MIN(originPandaID) FROM {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
        sqlFJ += "WHERE jediTaskID=:jediTaskID AND newPandaID=:newPandaID "
        type_var_names_str, type_var_map = get_sql_IN_bind_variables(EventServiceUtils.relationTypesForJS, prefix=":", value_as_suffix=True)
        sqlFJ += f"AND (relationType IS NULL OR NOT relationType IN ({type_var_names_str})) "
        varMap.update(type_var_map)
        cur.execute(sqlFJ + comment, varMap)
        resT = cur.fetchone()
        retList = []
        if resT is None:
            # origin
            retList.append(pandaID)
        else:
            # use only one origin since tracking the whole tree brings too many origins
            (originPandaID,) = resT
            if originPandaID is None:
                # origin
                retList.append(pandaID)
            else:
                retList.append(originPandaID)
        # return
        return retList

    # get jobsetID for PandaID
    def getJobsetIDforPandaID(self, pandaID, jediTaskID):
        comment = " /* DBProxy.getJobsetIDforPandaID */"
        # get parent IDs
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":newPandaID"] = pandaID
        varMap[":relationType"] = EventServiceUtils.relationTypeJS_ID
        sqlFJ = f"SELECT oldPandaID FROM {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
        sqlFJ += "WHERE jediTaskID=:jediTaskID AND newPandaID=:newPandaID "
        sqlFJ += "AND relationType=:relationType "
        self.cur.execute(sqlFJ + comment, varMap)
        resT = self.cur.fetchone()
        if resT is not None:
            return resT[0]
        return None

    # update JEDI for pilot retry
    def updateForPilotRetryJEDI(self, job, cur, onlyHistory=False, relationType=None):
        comment = " /* DBProxy.updateForPilotRetryJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        # sql to update file
        sqlFJI = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlFJI += "SET attemptNr=attemptNr+1,failedAttempt=failedAttempt+1,PandaID=:PandaID "
        sqlFJI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlFJI += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
        sqlFJO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlFJO += "SET attemptNr=attemptNr+1,failedAttempt=failedAttempt+1,PandaID=:PandaID,outPandaID=:PandaID "
        sqlFJO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlFJO += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
        sqlFP = "UPDATE ATLAS_PANDA.filesTable4 SET attemptNr=attemptNr+1 "
        sqlFP += "WHERE row_ID=:row_ID "
        if not onlyHistory:
            for tmpFile in job.Files:
                # skip if no JEDI
                if tmpFile.fileID == "NULL":
                    continue
                # update JEDI contents
                varMap = {}
                varMap[":jediTaskID"] = tmpFile.jediTaskID
                varMap[":datasetID"] = tmpFile.datasetID
                varMap[":fileID"] = tmpFile.fileID
                varMap[":attemptNr"] = tmpFile.attemptNr
                varMap[":PandaID"] = tmpFile.PandaID
                varMap[":keepTrack"] = 1
                if tmpFile.type in ["output", "log"]:
                    sqlFJ = sqlFJO
                else:
                    sqlFJ = sqlFJI
                tmp_log.debug(sqlFJ + comment + str(varMap))
                cur.execute(sqlFJ + comment, varMap)
                nRow = cur.rowcount
                if nRow == 1:
                    # update fileTable if JEDI contents was updated
                    varMap = {}
                    varMap[":row_ID"] = tmpFile.row_ID
                    tmp_log.debug(sqlFP + comment + str(varMap))
                    cur.execute(sqlFP + comment, varMap)
        # get origin
        originIDs = self.getOriginPandaIDsJEDI(job.parentID, job.jediTaskID, cur)
        # sql to record retry history
        sqlRH = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
        sqlRH += "(jediTaskID,oldPandaID,newPandaID,originPandaID,relationType) "
        sqlRH += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID,:relationType) "
        # record retry history
        for originID in originIDs:
            varMap = {}
            varMap[":jediTaskID"] = job.jediTaskID
            varMap[":oldPandaID"] = job.parentID
            varMap[":newPandaID"] = job.PandaID
            varMap[":originPandaID"] = originID
            if relationType is None:
                varMap[":relationType"] = "retry"
            else:
                varMap[":relationType"] = relationType
            cur.execute(sqlRH + comment, varMap)
        # record jobset
        if EventServiceUtils.isEventServiceMerge(job) and relationType is None:
            varMap = {}
            varMap[":jediTaskID"] = job.jediTaskID
            varMap[":oldPandaID"] = job.jobsetID
            varMap[":newPandaID"] = job.PandaID
            varMap[":originPandaID"] = job.jobsetID
            varMap[":relationType"] = EventServiceUtils.relationTypeJS_ID
            cur.execute(sqlRH + comment, varMap)
        return

    # check attemptNr for more retry
    def checkMoreRetryJEDI(self, job):
        comment = " /* DBProxy.self.checkMoreRetryJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        tmp_log.debug(f"start")
        # sql to get files
        sqlGF = "SELECT datasetID,fileID,attemptNr FROM ATLAS_PANDA.filesTable4 "
        sqlGF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
        # sql to check file
        sqlFJ = f"SELECT attemptNr,maxAttempt,failedAttempt,maxFailure FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
        sqlFJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlFJ += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack AND PandaID=:PandaID "
        # get files
        varMap = {}
        varMap[":PandaID"] = job.PandaID
        varMap[":type1"] = "input"
        varMap[":type2"] = "pseudo_input"
        self.cur.execute(sqlGF + comment, varMap)
        resGF = self.cur.fetchall()
        for datasetID, fileID, attemptNr in resGF:
            # check JEDI contents
            varMap = {}
            varMap[":jediTaskID"] = job.jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":fileID"] = fileID
            varMap[":attemptNr"] = attemptNr
            varMap[":PandaID"] = job.PandaID
            varMap[":keepTrack"] = 1
            self.cur.execute(sqlFJ + comment, varMap)
            resFJ = self.cur.fetchone()
            if resFJ is None:
                continue
            attemptNr, maxAttempt, failedAttempt, maxFailure = resFJ
            if maxAttempt is None:
                continue
            if attemptNr + 1 >= maxAttempt:
                # hit the limit
                tmp_log.debug(f"NG - fileID={fileID} no more attempt attemptNr({attemptNr})+1>=maxAttempt({maxAttempt})")
                return False
            if maxFailure is not None and failedAttempt is not None and failedAttempt + 1 >= maxFailure:
                # hit the limit
                tmp_log.debug(f"NG - fileID={fileID} no more attempt failedAttempt({failedAttempt})+1>=maxFailure({maxFailure})")
                return False
        tmp_log.debug(f"OK")
        return True

    # retry analysis job
    def retryJob(
        self,
        pandaID,
        param,
        failedInActive=False,
        changeJobInMem=False,
        inMemJob=None,
        getNewPandaID=False,
        attemptNr=None,
        recoverableEsMerge=False,
    ):
        comment = " /* DBProxy.retryJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={pandaID}")
        tmp_log.debug(f"inActive={failedInActive}")
        sql1 = f"SELECT {JobSpec.columnNames()} FROM ATLAS_PANDA.jobsActive4 "
        sql1 += "WHERE PandaID=:PandaID "
        if failedInActive:
            sql1 += "AND jobStatus=:jobStatus "
        updatedFlag = False
        nTry = 3
        for iTry in range(nTry):
            try:
                retValue = False
                if not changeJobInMem:
                    # begin transaction
                    self.conn.begin()
                    # select
                    varMap = {}
                    varMap[":PandaID"] = pandaID
                    if failedInActive:
                        varMap[":jobStatus"] = "failed"
                    self.cur.arraysize = 10
                    self.cur.execute(sql1 + comment, varMap)
                    res = self.cur.fetchall()
                    if len(res) == 0:
                        tmp_log.debug("PandaID not found")
                        self._rollback()
                        return retValue
                    job = JobSpec()
                    job.pack(res[0])
                else:
                    job = inMemJob
                # don't use getNewPandaID for buildJob since the order of PandaIDs is broken
                if getNewPandaID and job.prodSourceLabel in ["panda"]:
                    if not changeJobInMem:
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                    # return
                    return retValue
                # convert attemptNr to int
                try:
                    attemptNr = int(attemptNr)
                except Exception:
                    tmp_log.debug(f"attemptNr={attemptNr} non-integer")
                    attemptNr = -999
                # check attemptNr
                if attemptNr is not None:
                    if job.attemptNr != attemptNr:
                        tmp_log.debug(f"bad attemptNr job.{job.attemptNr} != pilot.{attemptNr}")
                        if not changeJobInMem:
                            # commit
                            if not self._commit():
                                raise RuntimeError("Commit error")
                        # return
                        return retValue
                # check if already retried
                if job.taskBufferErrorCode in [
                    ErrorCode.EC_Reassigned,
                    ErrorCode.EC_Retried,
                    ErrorCode.EC_PilotRetried,
                ]:
                    tmp_log.debug(f"already retried {job.taskBufferErrorCode}")
                    if not changeJobInMem:
                        # commit
                        if not self._commit():
                            raise RuntimeError("Commit error")
                    # return
                    return retValue
                # use JEDI
                useJEDI = False
                if (
                    hasattr(panda_config, "useJEDI")
                    and panda_config.useJEDI is True
                    and job.lockedby == "jedi"
                    and get_task_event_module(self).checkTaskStatusJEDI(job.jediTaskID, self.cur)
                ):
                    useJEDI = True
                # check pilot retry
                usePilotRetry = False
                if (
                    job.prodSourceLabel in ["user", "panda"] + JobUtils.list_ptest_prod_sources
                    and "pilotErrorCode" in param
                    and param["pilotErrorCode"].startswith("-")
                    and job.maxAttempt > job.attemptNr
                    and (not job.processingType.startswith("gangarobot") or job.processingType == "gangarobot-rctest")
                    and not job.processingType.startswith("hammercloud")
                ):
                    usePilotRetry = True
                # retry for ES merge
                if recoverableEsMerge and EventServiceUtils.isEventServiceMerge(job) and job.maxAttempt > job.attemptNr:
                    usePilotRetry = True
                # check if it's analysis job # FIXME once pilot retry works correctly the conditions below will be cleaned up
                if (
                    (
                        (job.prodSourceLabel == "user" or job.prodSourceLabel == "panda")
                        and not job.processingType.startswith("gangarobot")
                        and not job.processingType.startswith("hammercloud")
                        and "pilotErrorCode" in param
                        and param["pilotErrorCode"] in ["1200", "1201", "1213"]
                        and (not job.computingSite.startswith("ANALY_LONG_"))
                        and job.attemptNr < 2
                    )
                    or failedInActive
                    or usePilotRetry
                ) and job.commandToPilot != "tobekilled":
                    # check attemptNr for JEDI
                    moreRetryForJEDI = True
                    if useJEDI:
                        moreRetryForJEDI = self.checkMoreRetryJEDI(job)
                    # OK in JEDI
                    if moreRetryForJEDI:
                        tmp_log.debug(f"reset PandaID:{job.PandaID} #{job.attemptNr}")
                        if not changeJobInMem:
                            # job parameters
                            sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                            varMap = {}
                            varMap[":PandaID"] = job.PandaID
                            self.cur.execute(sqlJobP + comment, varMap)
                            for (clobJobP,) in self.cur:
                                try:
                                    job.jobParameters = clobJobP.read()
                                except AttributeError:
                                    job.jobParameters = str(clobJobP)
                                break
                        # reset job
                        job.jobStatus = "activated"
                        job.startTime = None
                        job.modificationTime = naive_utcnow()
                        job.attemptNr = job.attemptNr + 1
                        if usePilotRetry:
                            job.currentPriority -= 10
                        job.endTime = None
                        job.transExitCode = None
                        job.batchID = None
                        for attr in job._attributes:
                            if attr.endswith("ErrorCode") or attr.endswith("ErrorDiag"):
                                setattr(job, attr, None)
                        # remove flag related to pledge-resource handling
                        if job.specialHandling not in [None, "NULL", ""]:
                            newSpecialHandling = re.sub(",*localpool", "", job.specialHandling)
                            if newSpecialHandling == "":
                                job.specialHandling = None
                            else:
                                job.specialHandling = newSpecialHandling
                        # send it to long queue for analysis jobs
                        oldComputingSite = job.computingSite
                        if not changeJobInMem:
                            if job.computingSite.startswith("ANALY"):
                                longSite = None
                                tmpLongSiteList = []
                                tmpLongSite = re.sub("^ANALY_", "ANALY_LONG_", job.computingSite)
                                tmpLongSite = re.sub("_\d+$", "", tmpLongSite)
                                tmpLongSiteList.append(tmpLongSite)
                                tmpLongSite = job.computingSite + "_LONG"
                                tmpLongSiteList.append(tmpLongSite)
                                tmpLongSite = re.sub("SHORT", "LONG", job.computingSite)
                                if tmpLongSite != job.computingSite:
                                    tmpLongSiteList.append(tmpLongSite)
                                # loop over all possible long sitenames
                                for tmpLongSite in tmpLongSiteList:
                                    varMap = {}
                                    varMap[":siteID"] = tmpLongSite
                                    varMap[":status"] = "online"
                                    sqlSite = "SELECT /* use_json_type */ COUNT(*) FROM ATLAS_PANDA.schedconfig_json scj WHERE scj.panda_queue=:siteID AND scj.data.status=:status"
                                    self.cur.execute(sqlSite + comment, varMap)
                                    resSite = self.cur.fetchone()
                                    if resSite is not None and resSite[0] > 0:
                                        longSite = tmpLongSite
                                        break
                                # use long site if exists
                                if longSite is not None:
                                    tmp_log.debug(f"sending PandaID:{job.PandaID} to {longSite}")
                                    job.computingSite = longSite
                                    # set destinationSE if queue is changed
                                    if oldComputingSite == job.destinationSE:
                                        job.destinationSE = job.computingSite
                        if not changeJobInMem:
                            # select files
                            varMap = {}
                            varMap[":PandaID"] = job.PandaID
                            if not getNewPandaID:
                                varMap[":type1"] = "log"
                                varMap[":type2"] = "output"
                            sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
                            if not getNewPandaID:
                                sqlFile += "WHERE PandaID=:PandaID AND (type=:type1 OR type=:type2)"
                            else:
                                sqlFile += "WHERE PandaID=:PandaID"
                            self.cur.arraysize = 100
                            self.cur.execute(sqlFile + comment, varMap)
                            resFs = self.cur.fetchall()
                        else:
                            # get log or output files only
                            resFs = []
                            for tmpFile in job.Files:
                                if tmpFile.type in ["log", "output"]:
                                    resFs.append(tmpFile)
                        # loop over all files
                        for resF in resFs:
                            if not changeJobInMem:
                                # set PandaID
                                file = FileSpec()
                                file.pack(resF)
                                job.addFile(file)
                            else:
                                file = resF
                            # set new GUID
                            if file.type == "log":
                                file.GUID = str(uuid.uuid4())
                            # don't change input or lib.tgz, or ES merge output/log since it causes a problem with input name construction
                            if (
                                file.type in ["input", "pseudo_input"]
                                or (file.type == "output" and job.prodSourceLabel == "panda")
                                or (file.type == "output" and file.lfn.endswith(".lib.tgz") and job.prodSourceLabel in JobUtils.list_ptest_prod_sources)
                            ):
                                continue
                            # append attemptNr to LFN
                            oldName = file.lfn
                            file.lfn = re.sub("\.\d+$", "", file.lfn)
                            file.lfn = f"{file.lfn}.{job.attemptNr}"
                            newName = file.lfn
                            # set destinationSE
                            if oldComputingSite == file.destinationSE:
                                file.destinationSE = job.computingSite
                            # modify jobParameters
                            if not recoverableEsMerge:
                                sepPatt = "('|\"|%20|:)" + oldName + "('|\"|%20| )"
                            else:
                                sepPatt = "('|\"| |:|=)" + oldName + "('|\"| |<|$)"
                            matches = re.findall(sepPatt, job.jobParameters)
                            for match in matches:
                                oldPatt = match[0] + oldName + match[-1]
                                newPatt = match[0] + newName + match[-1]
                                job.jobParameters = re.sub(oldPatt, newPatt, job.jobParameters)
                            if not changeJobInMem and not getNewPandaID:
                                # reset file status
                                if file.type in ["output", "log"]:
                                    file.status = "unknown"
                                # update files
                                sqlFup = f"UPDATE ATLAS_PANDA.filesTable4 SET {file.bindUpdateChangesExpression()}" + "WHERE row_ID=:row_ID"
                                varMap = file.valuesMap(onlyChanged=True)
                                if varMap != {}:
                                    varMap[":row_ID"] = file.row_ID
                                    self.cur.execute(sqlFup + comment, varMap)
                        # set site to ES merger job
                        if recoverableEsMerge and EventServiceUtils.isEventServiceMerge(job):
                            get_task_event_module(self).setSiteForEsMerge(job, False, comment, comment)
                        if not changeJobInMem:
                            # reuse original PandaID
                            if not getNewPandaID:
                                # update job
                                sql2 = f"UPDATE ATLAS_PANDA.jobsActive4 SET {job.bindUpdateChangesExpression()} "
                                sql2 += "WHERE PandaID=:PandaID "
                                varMap = job.valuesMap(onlyChanged=True)
                                varMap[":PandaID"] = job.PandaID
                                self.cur.execute(sql2 + comment, varMap)
                                # update job parameters
                                sqlJobP = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                                varMap = {}
                                varMap[":PandaID"] = job.PandaID
                                varMap[":param"] = job.jobParameters
                                self.cur.execute(sqlJobP + comment, varMap)
                                updatedFlag = True
                            else:
                                # read metadata
                                sqlMeta = "SELECT metaData FROM ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"
                                varMap = {}
                                varMap[":PandaID"] = job.PandaID
                                self.cur.execute(sqlMeta + comment, varMap)
                                for (clobJobP,) in self.cur:
                                    try:
                                        job.metadata = clobJobP.read()
                                    except AttributeError:
                                        job.metadata = str(clobJobP)
                                    break
                                # insert job with new PandaID
                                sql1 = f"INSERT INTO ATLAS_PANDA.jobsActive4 ({JobSpec.columnNames()}) "
                                sql1 += JobSpec.bindValuesExpression(useSeq=True)
                                sql1 += " RETURNING PandaID INTO :newPandaID"
                                # set parentID
                                job.parentID = job.PandaID
                                job.creationTime = naive_utcnow()
                                job.modificationTime = job.creationTime
                                varMap = job.valuesMap(useSeq=True)
                                varMap[":newPandaID"] = self.cur.var(varNUMBER)
                                # insert
                                retI = self.cur.execute(sql1 + comment, varMap)
                                # set PandaID
                                val = self.getvalue_corrector(self.cur.getvalue(varMap[":newPandaID"]))
                                job.PandaID = int(val)
                                tmp_log.debug(f"Generate new PandaID {job.parentID} -> {job.PandaID} #{job.attemptNr}")
                                # insert files
                                sqlFile = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
                                sqlFile += FileSpec.bindValuesExpression(useSeq=True)
                                sqlFile += " RETURNING row_ID INTO :newRowID"
                                for file in job.Files:
                                    # reset rowID
                                    file.row_ID = None
                                    # insert
                                    varMap = file.valuesMap(useSeq=True)
                                    varMap[":newRowID"] = self.cur.var(varNUMBER)
                                    self.cur.execute(sqlFile + comment, varMap)
                                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newRowID"]))
                                    file.row_ID = int(val)
                                # job parameters
                                sqlJob = "INSERT INTO ATLAS_PANDA.jobParamsTable (PandaID,jobParameters) VALUES (:PandaID,:param)"
                                varMap = {}
                                varMap[":PandaID"] = job.PandaID
                                varMap[":param"] = job.jobParameters
                                self.cur.execute(sqlJob + comment, varMap)
                                # set error code to original job to avoid being retried by another process
                                sqlE = "UPDATE ATLAS_PANDA.jobsActive4 SET taskBufferErrorCode=:errCode,taskBufferErrorDiag=:errDiag WHERE PandaID=:PandaID"
                                varMap = {}
                                varMap[":PandaID"] = job.parentID
                                varMap[":errCode"] = ErrorCode.EC_PilotRetried
                                varMap[":errDiag"] = f"retrying at the same site. new PandaID={job.PandaID}"
                                self.cur.execute(sqlE + comment, varMap)
                                # propagate change to JEDI
                                if useJEDI:
                                    self.updateForPilotRetryJEDI(job, self.cur)
                        # set return
                        if not getNewPandaID:
                            retValue = True
                if not changeJobInMem:
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    # record status change
                    try:
                        if updatedFlag:
                            self.recordStatusChange(job.PandaID, job.jobStatus, jobInfo=job)
                            self.push_job_status_message(job, job.PandaID, job.jobStatus)
                    except Exception:
                        tmp_log.error("recordStatusChange in retryJob")
                return retValue
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                # error report
                self.dump_error_message(tmp_log)
                return False

    # propagate result to JEDI
    def propagateResultToJEDI(
        self,
        jobSpec,
        cur,
        oldJobStatus=None,
        extraInfo=None,
        finishPending=False,
        waitLock=False,
        async_params=None,
    ):
        comment = " /* DBProxy.propagateResultToJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={jobSpec.PandaID} jediTaskID={jobSpec.jediTaskID}")
        datasetContentsStat = {}
        # loop over all files
        finishUnmerge = set()
        trigger_reattempt = False
        tmp_log.debug(f"waitLock={waitLock} async_params={async_params}")
        # make pseudo files for dynamic number of events
        if EventServiceUtils.isDynNumEventsSH(jobSpec.specialHandling):
            pseudoFiles = get_task_event_module(self).create_pseudo_files_for_dyn_num_events(jobSpec, tmp_log)
        else:
            pseudoFiles = []
        # flag for job cloning
        useJobCloning = False
        if EventServiceUtils.isEventServiceJob(jobSpec) and EventServiceUtils.isJobCloningJob(jobSpec):
            useJobCloning = True
        # set delete flag to events
        if (EventServiceUtils.isEventServiceJob(jobSpec) or EventServiceUtils.isEventServiceMerge(jobSpec)) and jobSpec.jobStatus in [
            "finished",
            "failed",
            "cancelled",
        ]:
            # sql to check attemptNr
            sqlDelC = f"SELECT attemptNr FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlDelC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            # sql to set delete flag
            sqlDelE = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlDelE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
            sqlDelE += "SET file_not_deleted=CASE WHEN objStore_ID IS NULL THEN NULL ELSE :delFlag END "
            if jobSpec.jobStatus == "finished":
                sqlDelE += ",status=CASE WHEN status=:st_done THEN :st_merged ELSE status END "
            sqlDelE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            for fileSpec in jobSpec.Files:
                if fileSpec.type not in ["input", "pseudo_input"]:
                    continue
                # check attemptNr
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = fileSpec.datasetID
                varMap[":fileID"] = fileSpec.fileID
                self.cur.execute(sqlDelC + comment, varMap)
                (tmpAttemptNr,) = self.cur.fetchone()
                if fileSpec.attemptNr != tmpAttemptNr:
                    tmp_log.debug(f"skip to set Y for datasetID={fileSpec.datasetID} fileID={fileSpec.fileID} due to attemptNr mismatch")
                    continue
                # set del flag
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = fileSpec.datasetID
                varMap[":fileID"] = fileSpec.fileID
                varMap[":delFlag"] = "Y"
                if jobSpec.jobStatus == "finished":
                    varMap[":st_done"] = EventServiceUtils.ST_done
                    varMap[":st_merged"] = EventServiceUtils.ST_merged
                tmp_log.debug(sqlDelE + comment + str(varMap))
                self.cur.execute(sqlDelE + comment, varMap)
                retDelE = self.cur.rowcount
                tmp_log.debug(f"set Y to {retDelE} event ranges")
        # loop over all files to update file or dataset attributes
        for fileSpec in jobSpec.Files + pseudoFiles:
            # skip if no JEDI
            if fileSpec.fileID == "NULL":
                continue
            # do nothing for unmerged output/log files when merged job successfully finishes,
            # since they were already updated by merged job
            if jobSpec.jobStatus == "finished" and fileSpec.isUnMergedOutput():
                continue
            # check file status
            varMap = {}
            varMap[":fileID"] = fileSpec.fileID
            varMap[":datasetID"] = fileSpec.datasetID
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            # no attemptNr check for premerge since attemptNr can be incremented by pmerge
            if not (jobSpec.isCancelled() and fileSpec.isUnMergedOutput()):
                varMap[":attemptNr"] = fileSpec.attemptNr
            sqlFileStat = "SELECT status,is_waiting FROM ATLAS_PANDA.JEDI_Dataset_Contents "
            sqlFileStat += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            if not (jobSpec.isCancelled() and fileSpec.isUnMergedOutput()):
                sqlFileStat += "AND attemptNr=:attemptNr "
            sqlFileStat += "FOR UPDATE "
            if not waitLock:
                sqlFileStat += "NOWAIT "
            n_try = 5
            for i_try in range(n_try):
                try:
                    tmp_log.debug(f"Trying to lock file {i_try+1}/{n_try} sql:{sqlFileStat} var:{str(varMap)}")
                    cur.execute(sqlFileStat + comment, varMap)
                    break
                except Exception as e:
                    if i_try + 1 == n_try:
                        raise e
                    time.sleep(1)
            resFileStat = self.cur.fetchone()
            if resFileStat is not None:
                oldFileStatus, oldIsWaiting = resFileStat
            else:
                oldFileStatus, oldIsWaiting = None, None
            # skip if already cancelled
            if oldFileStatus in ["cancelled"]:
                continue
            # update Dataset Contents table
            updateMetadata = False
            updateAttemptNr = False
            updateNumEvents = False
            updateFailedAttempt = False
            varMap = {}
            varMap[":fileID"] = fileSpec.fileID
            varMap[":datasetID"] = fileSpec.datasetID
            varMap[":keepTrack"] = 1
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            if not (jobSpec.isCancelled() and fileSpec.isUnMergedOutput()):
                varMap[":attemptNr"] = fileSpec.attemptNr
            # set file status
            if fileSpec.type in ["input", "pseudo_input"]:
                hasInput = True
                updateAttemptNr = True
                if (
                    (
                        (jobSpec.jobStatus == "finished" and not EventServiceUtils.is_fine_grained_job(jobSpec))
                        or (jobSpec.jobSubStatus == "fg_done" and EventServiceUtils.is_fine_grained_job(jobSpec))
                    )
                    and not jobSpec.is_hpo_workflow()
                    and fileSpec.status != "skipped"
                ):
                    varMap[":status"] = "finished"
                    if fileSpec.type in ["input", "pseudo_input"]:
                        updateNumEvents = True
                else:
                    # set ready for next attempt
                    varMap[":status"] = "ready"
                    if jobSpec.jobStatus == "failed" and not jobSpec.is_hpo_workflow():
                        updateFailedAttempt = True
            else:
                if jobSpec.isCancelled():
                    # use only cancelled for all flavors
                    varMap[":status"] = "cancelled"
                else:
                    varMap[":status"] = jobSpec.jobStatus
                if fileSpec.status == "nooutput":
                    varMap[":status"] = fileSpec.status
                elif jobSpec.jobStatus == "finished":
                    varMap[":status"] = "finished"
                    # update metadata
                    updateMetadata = True
                    # update nEvents
                    updateNumEvents = True
                elif fileSpec.status == "merging":
                    # set ready to merge files for failed jobs
                    varMap[":status"] = "ready"
                    # update metadata
                    updateMetadata = True
            sqlFile = "UPDATE /*+ INDEX_RS_ASC(JEDI_DATASET_CONTENTS (JEDI_DATASET_CONTENTS.JEDITASKID JEDI_DATASET_CONTENTS.DATASETID JEDI_DATASET_CONTENTS.FILEID)) */ ATLAS_PANDA.JEDI_Dataset_Contents SET status=:status,is_waiting=NULL"
            # attempt number
            if updateAttemptNr is True:
                # increment attemptNr for next attempt
                if not jobSpec.is_hpo_workflow():
                    sqlFile += ",attemptNr=attemptNr+1"
                else:
                    sqlFile += ",attemptNr=MOD(attemptNr+1,maxAttempt)"
            # failed attempts
            if updateFailedAttempt is True:
                sqlFile += ",failedAttempt=failedAttempt+1"
            # set correct PandaID for job cloning
            if useJobCloning:
                varMap[":PandaID"] = jobSpec.PandaID
                if fileSpec.type in ["log", "output"]:
                    sqlFile += ",outPandaID=:PandaID,PandaID=:PandaID"
                else:
                    sqlFile += ",PandaID=:PandaID"
            # metadata
            if updateMetadata:
                # set file metadata
                for tmpKey in ["lfn", "GUID", "fsize", "checksum"]:
                    tmpVal = getattr(fileSpec, tmpKey)
                    if tmpVal == "NULL":
                        if tmpKey in fileSpec._zeroAttrs:
                            tmpVal = 0
                        else:
                            tmpVal = None
                    tmpMapKey = f":{tmpKey}"
                    sqlFile += f",{tmpKey}={tmpMapKey}"
                    varMap[tmpMapKey] = tmpVal
                # extra metadata
                if extraInfo is not None:
                    # nevents
                    if "nevents" in extraInfo and fileSpec.lfn in extraInfo["nevents"]:
                        tmpKey = "nEvents"
                        tmpMapKey = f":{tmpKey}"
                        sqlFile += f",{tmpKey}={tmpMapKey}"
                        varMap[tmpMapKey] = extraInfo["nevents"][fileSpec.lfn]
                    # LB number
                    if "lbnr" in extraInfo and fileSpec.lfn in extraInfo["lbnr"]:
                        tmpKey = "lumiBlockNr"
                        tmpMapKey = f":{tmpKey}"
                        sqlFile += f",{tmpKey}={tmpMapKey}"
                        varMap[tmpMapKey] = extraInfo["lbnr"][fileSpec.lfn]
                # reset keepTrack unless merging
                if fileSpec.status != "merging":
                    sqlFile += ",keepTrack=NULL"
                else:
                    # set boundaryID for merging
                    sqlFile += ",boundaryID=:boundaryID"
                    varMap[":boundaryID"] = jobSpec.PandaID
                    # set max attempt
                    sqlFile += ",maxAttempt=attemptNr+3"
            sqlFile += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlFile += "AND keepTrack=:keepTrack "
            if not (jobSpec.isCancelled() and fileSpec.isUnMergedOutput()):
                sqlFile += "AND attemptNr=:attemptNr "
            tmp_log.debug(sqlFile + comment + str(varMap))
            cur.execute(sqlFile + comment, varMap)
            nRow = cur.rowcount
            if nRow == 1 and fileSpec.status not in ["nooutput"]:
                datasetID = fileSpec.datasetID
                fileStatus = varMap[":status"]
                if datasetID not in datasetContentsStat:
                    datasetContentsStat[datasetID] = {
                        "nFilesUsed": 0,
                        "nFilesFinished": 0,
                        "nFilesFailed": 0,
                        "nFilesOnHold": 0,
                        "nFilesTobeUsed": 0,
                        "nEvents": 0,
                        "nEventsUsed": 0,
                        "nFilesWaiting": 0,
                    }
                # read nEvents
                if updateNumEvents:
                    sqlEVT = "SELECT nEvents,startEvent,endEvent,keepTrack FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                    sqlEVT += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    if not waitLock:
                        sqlEVT += "FOR UPDATE NOWAIT "
                    varMap = {}
                    varMap[":fileID"] = fileSpec.fileID
                    varMap[":datasetID"] = fileSpec.datasetID
                    varMap[":jediTaskID"] = jobSpec.jediTaskID
                    tmp_log.debug(sqlEVT + comment + str(varMap))
                    cur.execute(sqlEVT + comment, varMap)
                    resEVT = self.cur.fetchone()
                    if resEVT is not None:
                        tmpNumEvents, tmpStartEvent, tmpEndEvent, tmpKeepTrack = resEVT
                        if tmpNumEvents is not None:
                            try:
                                if fileSpec.type in ["input", "pseudo_input"]:
                                    if tmpKeepTrack == 1:
                                        # keep track on how many events successfully used
                                        if tmpStartEvent is not None and tmpEndEvent is not None:
                                            datasetContentsStat[datasetID]["nEventsUsed"] += tmpEndEvent - tmpStartEvent + 1
                                        else:
                                            datasetContentsStat[datasetID]["nEventsUsed"] += tmpNumEvents
                                else:
                                    datasetContentsStat[datasetID]["nEvents"] += tmpNumEvents
                            except Exception:
                                pass
                # update file counts
                isDone = False
                if fileSpec.status == "merging" and (finishPending or jobSpec.prodSourceLabel not in ["user", "panda"]):
                    # files to be merged for pending failed jobs
                    datasetContentsStat[datasetID]["nFilesOnHold"] += 1
                elif fileStatus == "ready":
                    # check attemptNr and maxAttempt when the file failed (ready = input failed)
                    # skip secondary datasets which have maxAttempt=None
                    sqlAttNr = "SELECT attemptNr,maxAttempt,failedAttempt,maxFailure FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                    sqlAttNr += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    varMap = {}
                    varMap[":fileID"] = fileSpec.fileID
                    varMap[":datasetID"] = fileSpec.datasetID
                    varMap[":jediTaskID"] = jobSpec.jediTaskID
                    tmp_log.debug(sqlAttNr + comment + str(varMap))
                    cur.execute(sqlAttNr + comment, varMap)
                    resAttNr = self.cur.fetchone()
                    if resAttNr is not None:
                        newAttemptNr, maxAttempt, failedAttempt, maxFailure = resAttNr
                        if maxAttempt is not None:
                            if maxAttempt > newAttemptNr and (maxFailure is None or maxFailure > failedAttempt):
                                if oldFileStatus == "ready":
                                    # don't change nFilesUsed when fake co-jumbo is done
                                    pass
                                elif fileSpec.status != "merging":
                                    # decrement nUsed to trigger reattempt
                                    datasetContentsStat[datasetID]["nFilesUsed"] -= 1
                                    trigger_reattempt = True
                                else:
                                    # increment nTobeUsed to trigger merging
                                    datasetContentsStat[datasetID]["nFilesTobeUsed"] += 1
                            else:
                                # no more reattempt
                                datasetContentsStat[datasetID]["nFilesFailed"] += 1
                                isDone = True
                                # merge job failed
                                if jobSpec.processingType == "pmerge":
                                    # update unmerged file
                                    sqlUmFile = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET status=:status,keepTrack=NULL "
                                    sqlUmFile += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                                    varMap = {}
                                    varMap[":fileID"] = fileSpec.fileID
                                    varMap[":datasetID"] = fileSpec.datasetID
                                    varMap[":jediTaskID"] = jobSpec.jediTaskID
                                    varMap[":status"] = "notmerged"
                                    tmp_log.debug(sqlUmFile + comment + str(varMap))
                                    cur.execute(sqlUmFile + comment, varMap)
                                    # set flag to update unmerged jobs
                                    finishUnmerge.add(fileSpec.fileID)
                elif fileStatus in ["finished", "lost"]:
                    # successfully used or produced, or lost
                    datasetContentsStat[datasetID]["nFilesFinished"] += 1
                    isDone = True
                else:
                    # failed to produce the file
                    datasetContentsStat[datasetID]["nFilesFailed"] += 1
                # changed from transferring
                if fileSpec.type in ["input", "pseudo_input"]:
                    if oldJobStatus == "transferring":
                        datasetContentsStat[datasetID]["nFilesOnHold"] -= 1
                # reset is_waiting
                if oldIsWaiting is not None:
                    datasetContentsStat[datasetID]["nFilesWaiting"] -= 1
                    if isDone:
                        datasetContentsStat[datasetID]["nFilesUsed"] += 1
                # killed during merging
                if jobSpec.isCancelled() and oldJobStatus == "merging" and fileSpec.isUnMergedOutput():
                    # get corresponding sub
                    varMap = {}
                    varMap[":pandaID"] = jobSpec.PandaID
                    varMap[":fileID"] = fileSpec.fileID
                    varMap[":datasetID"] = fileSpec.datasetID
                    varMap[":jediTaskID"] = jobSpec.jediTaskID
                    sqlGetDest = "SELECT destinationDBlock FROM ATLAS_PANDA.filesTable4 "
                    sqlGetDest += "WHERE pandaID=:pandaID AND jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    tmp_log.debug(sqlGetDest + comment + str(varMap))
                    cur.execute(sqlGetDest + comment, varMap)
                    (preMergedDest,) = self.cur.fetchone()
                    # check if corresponding sub is closed
                    varMap = {}
                    varMap[":name"] = preMergedDest
                    varMap[":subtype"] = "sub"
                    sqlCheckDest = "SELECT status FROM ATLAS_PANDA.Datasets "
                    sqlCheckDest += "WHERE name=:name AND subtype=:subtype "
                    tmp_log.debug(sqlCheckDest + comment + str(varMap))
                    cur.execute(sqlCheckDest + comment, varMap)
                    tmpResDestStat = self.cur.fetchone()
                    if tmpResDestStat is not None:
                        (preMergedDestStat,) = tmpResDestStat
                    else:
                        preMergedDestStat = "notfound"
                        tmp_log.debug(f"{preMergedDest} not found for datasetID={datasetID}")
                    if preMergedDestStat not in ["tobeclosed", "completed"]:
                        datasetContentsStat[datasetID]["nFilesOnHold"] -= 1
                    else:
                        tmp_log.debug(f"not change nFilesOnHold for datasetID={datasetID} since sub is in {preMergedDestStat}")
                        # increment nUsed when mergeing is killed before merge job is generated
                        if oldFileStatus == "ready":
                            datasetContentsStat[datasetID]["nFilesUsed"] += 1
        # update JEDI_Datasets table
        nOutEvents = 0
        if datasetContentsStat != {}:
            tmpDatasetIDs = sorted(datasetContentsStat)
            for tmpDatasetID in tmpDatasetIDs:
                tmp_log.debug(f"trying to lock datasetID={tmpDatasetID}")
                tmpContentsStat = datasetContentsStat[tmpDatasetID]
                sqlJediDL = "SELECT nFilesUsed,nFilesFailed,nFilesTobeUsed,nFilesFinished," "nFilesOnHold,type,masterID,status FROM ATLAS_PANDA.JEDI_Datasets "
                sqlJediDL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                sqlJediDLnoL = sqlJediDL
                sqlJediDL += "FOR UPDATE "
                if not waitLock:
                    sqlJediDL += "NOWAIT "
                varMap = {}
                varMap[":jediTaskID"] = jobSpec.jediTaskID
                varMap[":datasetID"] = tmpDatasetID
                if async_params is None:
                    cur.execute(sqlJediDL + comment, varMap)
                else:
                    cur.execute(sqlJediDLnoL + comment, varMap)
                tmpResJediDL = self.cur.fetchone()
                (
                    t_nFilesUsed,
                    t_nFilesFailed,
                    t_nFilesTobeUsed,
                    t_nFilesFinished,
                    t_nFilesOnHold,
                    t_type,
                    t_masterID,
                    t_status,
                ) = tmpResJediDL
                tmp_log.debug(
                    f"datasetID={tmpDatasetID} had nFilesTobeUsed={t_nFilesTobeUsed} "
                    f"nFilesUsed={t_nFilesUsed} nFilesFinished={t_nFilesFinished} "
                    f"nFilesFailed={t_nFilesFailed} status={t_status}"
                )
                if async_params is not None:
                    self.insert_to_query_pool(
                        SQL_QUEUE_TOPIC_async_dataset_update,
                        async_params["PandaID"],
                        async_params["jediTaskID"],
                        sqlJediDL,
                        varMap,
                        async_params["exec_order"],
                    )
                    async_params["exec_order"] += 1
                # sql to update nFiles info
                toUpdateFlag = False
                eventsToRead = False
                sqlJediDS = "UPDATE ATLAS_PANDA.JEDI_Datasets SET "
                for tmpStatKey in tmpContentsStat:
                    tmpStatVal = tmpContentsStat[tmpStatKey]
                    if tmpStatVal == 0:
                        continue
                    if tmpStatVal > 0:
                        sqlJediDS += f"{tmpStatKey}={tmpStatKey}+{tmpStatVal},"
                    else:
                        sqlJediDS += f"{tmpStatKey}={tmpStatKey}-{abs(tmpStatVal)},"
                    toUpdateFlag = True
                    if tmpStatKey == "nEvents" and tmpStatVal > nOutEvents:
                        nOutEvents = tmpStatVal
                sqlJediDS = sqlJediDS[:-1]
                sqlJediDS += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                varMap = {}
                varMap[":jediTaskID"] = jobSpec.jediTaskID
                varMap[":datasetID"] = tmpDatasetID
                # update
                if toUpdateFlag:
                    tmp_log.debug(sqlJediDS + comment + str(varMap))
                    if async_params is not None:
                        self.insert_to_query_pool(
                            SQL_QUEUE_TOPIC_async_dataset_update,
                            async_params["PandaID"],
                            async_params["jediTaskID"],
                            sqlJediDS,
                            varMap,
                            async_params["exec_order"],
                        )
                        async_params["exec_order"] += 1
                    else:
                        cur.execute(sqlJediDS + comment, varMap)
                    # update events in corrupted input files
                    if (
                        EventServiceUtils.isEventServiceMerge(jobSpec)
                        and jobSpec.jobStatus == "failed"
                        and jobSpec.pilotErrorCode in EventServiceUtils.PEC_corruptedInputFiles + EventServiceUtils.PEC_corruptedInputFilesTmp
                        and t_type in ["input", "pseudo_input"]
                        and t_masterID is None
                        and (tmpContentsStat["nFilesUsed"] < 0 or tmpContentsStat["nFilesFailed"] > 0)
                    ):
                        toSet = True
                        if jobSpec.pilotErrorCode in EventServiceUtils.PEC_corruptedInputFilesTmp:
                            # check failure count for temporary errors
                            toSet = get_metrics_module(self).checkFailureCountWithCorruptedFiles(jobSpec.jediTaskID, jobSpec.PandaID)
                        if toSet:
                            get_task_event_module(self).setCorruptedEventRanges(jobSpec.jediTaskID, jobSpec.PandaID)
        # update task queued time
        if trigger_reattempt and get_task_queued_time(jobSpec.specialHandling):
            sql_update_tq = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET queuedTime=CURRENT_DATE WHERE jediTaskID=:jediTaskID AND queuedTime IS NULL "
            var_map = {":jediTaskID": jobSpec.jediTaskID}
            tmp_log.debug(sql_update_tq + comment + str(var_map))
            if async_params is not None:
                self.insert_to_query_pool(
                    SQL_QUEUE_TOPIC_async_dataset_update,
                    async_params["PandaID"],
                    async_params["jediTaskID"],
                    sql_update_tq,
                    var_map,
                    async_params["exec_order"],
                )
                async_params["exec_order"] += 1
            else:
                cur.execute(sql_update_tq + comment, var_map)
        # add jobset info for job cloning
        if useJobCloning:
            self.recordRetryHistoryJEDI(
                jobSpec.jediTaskID,
                jobSpec.PandaID,
                [jobSpec.jobsetID],
                EventServiceUtils.relationTypeJS_ID,
            )
        # update jumbo flag
        if jobSpec.eventService == EventServiceUtils.jumboJobFlagNumber:
            # check site
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            sqlJumboS = f"SELECT site FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            cur.execute(sqlJumboS + comment, varMap)
            tmpResS = self.cur.fetchone()
            (jumboSite,) = tmpResS
            # count number of events for jumbo
            newUseJumbo = "L"
            """
            varMap = {}
            varMap[':jediTaskID'] = jobSpec.jediTaskID
            varMap[':eventStatus']  = EventServiceUtils.ST_ready
            varMap[':minAttemptNr'] = 0
            sqlJumboC = "SELECT COUNT(*) FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlJumboC += "WHERE jediTaskID=:jediTaskID AND status=:eventStatus AND attemptNr>:minAttemptNr ".format(panda_config.schemaJEDI)
            cur.execute(sqlJumboC+comment,varMap)
            tmpResC = self.cur.fetchone()
            if tmpResC is not None:
                nEventsJumbo, = tmpResC
                tmp_log.debug('{0} event ranges available for jumbo'.format(nEventsJumbo))
                # no more events
                if nEventsJumbo == 0 and jumboSite is None:
                    newUseJumbo = 'D'
            """
            # update flag
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":newJumbo"] = newUseJumbo
            varMap[":notUseJumbo"] = "D"
            sqlJumboF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlJumboF += "SET useJumbo=:newJumbo WHERE jediTaskID=:jediTaskID "
            sqlJumboF += "AND useJumbo IS NOT NULL AND useJumbo<>:notUseJumbo "
            cur.execute(sqlJumboF + comment, varMap)
            nRow = cur.rowcount
            tmp_log.debug(f"set task.useJumbo={varMap[':newJumbo']} with {nRow}")
        # update input
        if (
            not EventServiceUtils.isJumboJob(jobSpec)
            and not (jobSpec.computingSite == EventServiceUtils.siteIdForWaitingCoJumboJobs and not jobSpec.isCancelled())
            and jobSpec.taskBufferErrorCode not in [ErrorCode.EC_PilotRetried]
        ):
            get_task_event_module(self).updateInputStatusJedi(jobSpec.jediTaskID, jobSpec.PandaID, jobSpec.jobStatus)
        # update t_task
        if jobSpec.jobStatus == "finished" and jobSpec.prodSourceLabel not in ["panda"]:
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":noutevents"] = nOutEvents
            schemaDEFT = panda_config.schemaDEFT
            sqlTtask = f"UPDATE {schemaDEFT}.T_TASK "
            if jobSpec.processingType != "pmerge":
                updateNumDone = True
                sqlTtask += "SET total_done_jobs=total_done_jobs+1,timestamp=CURRENT_DATE,total_events=LEAST(9999999999,total_events+:noutevents) "
            else:
                updateNumDone = False
                sqlTtask += "SET timestamp=CURRENT_DATE,total_events=LEAST(9999999999,total_events+:noutevents) "
            sqlTtask += "WHERE taskid=:jediTaskID "
            tmp_log.debug(sqlTtask + comment + str(varMap))
            cur.execute(sqlTtask + comment, varMap)
            nRow = cur.rowcount
            # get total_done_jobs
            if updateNumDone and nRow == 1:
                varMap = {}
                varMap[":jediTaskID"] = jobSpec.jediTaskID
                sqlNumDone = f"SELECT total_done_jobs FROM {schemaDEFT}.T_TASK "
                sqlNumDone += "WHERE taskid=:jediTaskID "
                cur.execute(sqlNumDone + comment, varMap)
                tmpResNumDone = self.cur.fetchone()
                if tmpResNumDone is not None:
                    (numDone,) = tmpResNumDone
                    if numDone in [5, 100]:
                        # reset walltimeUnit to recalculate task parameters
                        varMap = {}
                        varMap[":jediTaskID"] = jobSpec.jediTaskID
                        sqlRecal = "UPDATE ATLAS_PANDA.JEDI_Tasks SET walltimeUnit=NULL WHERE jediTaskId=:jediTaskID "
                        msgStr = "trigger recalculation of task parameters "
                        msgStr += f"with nDoneJobs={numDone} for jediTaskID={jobSpec.jediTaskID}"
                        tmp_log.debug(msgStr)
                        cur.execute(sqlRecal + comment, varMap)
        # propagate failed result to unmerge job
        if len(finishUnmerge) > 0:
            self.updateUnmergedJobs(jobSpec, finishUnmerge, async_params=async_params)
        # update some job attributes
        get_entity_module(self).setHS06sec(jobSpec.PandaID)

        # update the g of CO2 emitted by the job
        try:
            gco2_regional, gco2_global = get_entity_module(self).set_co2_emissions(jobSpec.PandaID)
            tmp_log.debug(f"calculated gCO2 regional {gco2_regional} and global {gco2_global}")
        except Exception:
            tmp_log.error(f"failed calculating gCO2 with {traceback.format_exc()}")

        # task and job metrics
        if get_task_queued_time(jobSpec.specialHandling):
            # update task queued time
            get_metrics_module(self).update_task_queued_activated_times(jobSpec.jediTaskID)
            # record job queuing time if the job didn't start running
            get_metrics_module(self).record_job_queuing_period(jobSpec.PandaID, jobSpec)

        # return
        return True

    # finalize pending jobs
    def finalizePendingJobs(self, prodUserName, jobDefinitionID, waitLock=False):
        comment = " /* DBProxy.finalizePendingJobs */"
        tmp_log = self.create_tagged_logger(comment, f"user={prodUserName} jobdefID={jobDefinitionID}")
        tmp_log.debug("start")
        sql0 = "SELECT PandaID,lockedBy,jediTaskID FROM ATLAS_PANDA.jobsActive4 "
        sql0 += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
        sql0 += "AND prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus "
        sqlU = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:newJobStatus "
        sqlU += "WHERE PandaID=:PandaID AND jobStatus=:jobStatus "
        sql1 = f"SELECT {JobSpec.columnNames()} FROM ATLAS_PANDA.jobsActive4 "
        sql1 += "WHERE PandaID=:PandaID AND jobStatus=:jobStatus "
        sql2 = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID AND jobStatus=:jobStatus "
        sql3 = f"INSERT INTO ATLAS_PANDA.jobsArchived4 ({JobSpec.columnNames()}) "
        sql3 += JobSpec.bindValuesExpression()
        sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        try:
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # select
            varMap = {}
            varMap[":jobStatus"] = "failed"
            varMap[":prodUserName"] = prodUserName
            varMap[":jobDefinitionID"] = jobDefinitionID
            varMap[":prodSourceLabel"] = "user"
            self.cur.execute(sql0 + comment, varMap)
            resPending = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # lock
            pPandaIDs = []
            lockedBy = None
            jediTaskID = None
            for pandaID, tmpLockedBy, tmpJediTaskID in resPending:
                if lockedBy is None:
                    lockedBy = tmpLockedBy
                if jediTaskID is None:
                    jediTaskID = tmpJediTaskID
                pPandaIDs.append(pandaID)
            # check if JEDI is used
            useJEDI = False
            if (
                hasattr(panda_config, "useJEDI")
                and panda_config.useJEDI is True
                and lockedBy == "jedi"
                and get_task_event_module(self).checkTaskStatusJEDI(jediTaskID, self.cur)
            ):
                useJEDI = True
            # loop over all PandaIDs
            for pandaID in pPandaIDs:
                # begin transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[":jobStatus"] = "failed"
                varMap[":newJobStatus"] = "holding"
                varMap[":PandaID"] = pandaID
                self.cur.execute(sqlU + comment, varMap)
                retU = self.cur.rowcount
                if retU == 0:
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                # get job
                varMap = {}
                varMap[":PandaID"] = pandaID
                varMap[":jobStatus"] = "holding"
                self.cur.arraysize = 10
                self.cur.execute(sql1 + comment, varMap)
                res = self.cur.fetchall()
                if len(res) == 0:
                    tmp_log.debug(f"PandaID {pandaID} not found")
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    continue
                job = JobSpec()
                job.pack(res[0])
                job.jobStatus = "failed"
                job.modificationTime = naive_utcnow()
                # delete
                self.cur.execute(sql2 + comment, varMap)
                n = self.cur.rowcount
                if n == 0:
                    # already killed
                    tmp_log.debug(f"Not found {pandaID}")
                else:
                    tmp_log.debug(f"finalizing {pandaID}")
                    # insert
                    self.cur.execute(sql3 + comment, job.valuesMap())
                    # update files,metadata,parametes
                    varMap = {}
                    varMap[":PandaID"] = pandaID
                    varMap[":modificationTime"] = job.modificationTime
                    self.cur.execute(sqlFMod + comment, varMap)
                    self.cur.execute(sqlMMod + comment, varMap)
                    self.cur.execute(sqlPMod + comment, varMap)
                    # update JEDI tables
                    if useJEDI:
                        # read files
                        sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
                        sqlFile += "WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[":PandaID"] = pandaID
                        self.cur.arraysize = 100000
                        self.cur.execute(sqlFile + comment, varMap)
                        resFs = self.cur.fetchall()
                        for resF in resFs:
                            tmpFile = FileSpec()
                            tmpFile.pack(resF)
                            job.addFile(tmpFile)
                        self.propagateResultToJEDI(job, self.cur, finishPending=True, waitLock=waitLock)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                self.push_job_status_message(job, pandaID, varMap[":newJobStatus"])
            tmp_log.debug(f"done {len(pPandaIDs)} jobs")
            return True
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # get job statistics per site, prodsourcelabel (managed, user, test...), and resource type (SCORE, MCORE...)
    def get_job_statistics_per_site_label_resource(self, time_window):
        comment = " /* DBProxy.get_job_statistics_per_site_label_resource */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        sql_defined = (
            "SELECT computingSite, jobStatus, gshare, resource_type, COUNT(*) FROM ATLAS_PANDA.jobsDefined4 "
            "GROUP BY computingSite, jobStatus, gshare, resource_type "
        )

        sql_failed = (
            "SELECT computingSite, jobStatus, gshare, resource_type, COUNT(*) FROM ATLAS_PANDA.jobsActive4 "
            "WHERE jobStatus = :jobStatus AND modificationTime > :modificationTime "
            "GROUP BY computingSite, jobStatus, gshare, resource_type "
        )

        sql_active_mv = (
            "SELECT /*+ RESULT_CACHE */ computingSite, jobStatus, gshare, resource_type, SUM(njobs) "
            "FROM ATLAS_PANDA.JOBS_SHARE_STATS "
            "WHERE jobStatus <> :jobStatus "
            "GROUP BY computingSite, jobStatus, gshare, resource_type "
        )

        sql_archived = (
            "SELECT /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ "
            "computingSite, jobStatus, gshare, resource_type, COUNT(*) "
            "FROM ATLAS_PANDA.jobsArchived4 tab "
            "WHERE modificationTime > :modificationTime "
            "GROUP BY computingSite, jobStatus, gshare, resource_type"
        )

        ret = dict()
        try:
            if time_window is None:
                time_floor = naive_utcnow() - datetime.timedelta(hours=12)
            else:
                time_floor = naive_utcnow() - datetime.timedelta(minutes=int(time_window))

            sql_var_list = [
                (sql_defined, {}),
                (sql_failed, {":jobStatus": "failed", ":modificationTime": time_floor}),
                (sql_active_mv, {":jobStatus": "failed"}),
                (sql_archived, {":modificationTime": time_floor}),
            ]

            for sql_tmp, var_map in sql_var_list:
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 10000
                # select
                sql_tmp = sql_tmp + comment
                self.cur.execute(sql_tmp, var_map)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                get_entity_module(self).reload_shares()

                # create map
                share_label_map = dict()
                for computing_site, job_status, gshare, resource_type, n_jobs in res:
                    if gshare not in share_label_map:
                        for share in get_entity_module(self).leave_shares:
                            if gshare == share.name:
                                prod_source_label = share.prodsourcelabel
                                if "|" in prod_source_label:
                                    prod_source_label = prod_source_label.split("|")[0]
                                    prod_source_label = prod_source_label.replace(".*", "")
                                share_label_map[gshare] = prod_source_label
                                break
                        if gshare not in share_label_map:
                            share_label_map[gshare] = "unknown"
                    prod_source_label = share_label_map[gshare]
                    ret.setdefault(computing_site, dict())
                    ret[computing_site].setdefault(prod_source_label, dict())
                    ret[computing_site][prod_source_label].setdefault(resource_type, dict())
                    ret[computing_site][prod_source_label][resource_type].setdefault(job_status, 0)
                    ret[computing_site][prod_source_label][resource_type][job_status] += n_jobs

            # for zero
            state_list = ["assigned", "activated", "running", "finished", "failed"]
            for computing_site in ret:
                for prod_source_label in ret[computing_site]:
                    for resource_type in ret[computing_site][prod_source_label]:
                        for job_status in state_list:
                            ret[computing_site][prod_source_label][resource_type].setdefault(job_status, 0)

            # return
            tmp_log.debug(f"done")
            return ret
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return dict()

    # post-process for event service job
    def ppEventServiceJob(self, job, currentJobStatus, useCommit=True):
        comment = " /* DBProxy.ppEventServiceJob */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        pandaID = job.PandaID
        attemptNr = job.attemptNr
        tmp_log.debug(f"start attemptNr={attemptNr}")
        try:
            # return values
            # 0 : generated a retry job
            # 1 : not retried due to a harmless reason
            # 2 : generated a merge job
            # 3 : max attempts reached
            # 4 : not generated a merge job since other consumers are still running
            # 5 : didn't process any events on WN
            # 6 : didn't process any events on WN and fail since the last one
            # 7 : all event ranges failed
            # 8 : generated a retry job but no events were processed
            # 9 : closed in bad job status
            # 10 : generated a merge job but didn't process any events by itself
            # None : fatal error
            retValue = 1, None
            # begin transaction
            if useCommit:
                self.conn.begin()
            self.cur.arraysize = 10
            # make job spec to not change the original
            jobSpec = copy.copy(job)
            jobSpec.Files = []
            # check if event service job
            if not EventServiceUtils.isEventServiceJob(jobSpec):
                tmp_log.debug(f"no event service job")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                return retValue
            # check if already retried or not good for retry
            if jobSpec.taskBufferErrorCode in [
                ErrorCode.EC_EventServiceRetried,
                ErrorCode.EC_EventServiceMerge,
                ErrorCode.EC_EventServiceInconsistentIn,
                ErrorCode.EC_EventServiceBadStatus,
            ]:
                tmp_log.debug(f"already post-processed for event service with EC={jobSpec.taskBufferErrorCode}")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                return retValue
            # check if JEDI is used
            if (
                hasattr(panda_config, "useJEDI")
                and panda_config.useJEDI is True
                and jobSpec.lockedby == "jedi"
                and get_task_event_module(self).checkTaskStatusJEDI(jobSpec.jediTaskID, self.cur)
            ):
                pass
            else:
                tmp_log.debug(f"JEDI is not used")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                return retValue
            # use an input file as lock since FOR UPDATE doesn't work on JEDI_Events
            lockFileSpec = None
            for fileSpec in job.Files:
                if fileSpec.type in ["input", "pseudo_input"]:
                    if lockFileSpec is None or lockFileSpec.fileID > fileSpec.fileID:
                        lockFileSpec = fileSpec
            if lockFileSpec is not None:
                # sql to lock the file
                sqlLIF = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlLIF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlLIF += "FOR UPDATE NOWAIT "
                varMap = dict()
                varMap[":jediTaskID"] = lockFileSpec.jediTaskID
                varMap[":datasetID"] = lockFileSpec.datasetID
                varMap[":fileID"] = lockFileSpec.fileID
                tmp_log.debug(f"locking {str(varMap)}")
                self.cur.execute(sqlLIF + comment, varMap)
                tmp_log.debug(f"locked")
            # change event status processed by jumbo jobs
            nRowDoneJumbo = 0
            nRowFailedJumbo = 0
            if EventServiceUtils.isCoJumboJob(jobSpec):
                # sql to change event status
                sqlJE = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlJE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
                sqlJE += "SET status=:newStatus "
                sqlJE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlJE += "AND status=:oldStatus AND is_jumbo=:isJumbo "
                # sql to lock failed events
                sqlJFL = sqlJE + "AND processed_upto_eventID IS NOT NULL "
                # sql to copy failed events
                sqlJFC = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Events "
                sqlJFC += "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
                sqlJFC += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID,"
                sqlJFC += "event_offset,is_jumbo) "
                sqlJFC += "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlJFC += "jediTaskID,datasetID,event_offset,fileID,attemptNr-1,:newStatus,"
                sqlJFC += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID,"
                sqlJFC += "event_offset,NULL "
                sqlJFC += f"FROM {panda_config.schemaJEDI}.JEDI_Events tab "
                sqlJFC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlJFC += "AND status=:oldStatus AND processed_upto_eventID IS NOT NULL AND is_jumbo=:isJumbo "
                # sql to release failed events
                sqlJFR = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlJFR += f"{panda_config.schemaJEDI}.JEDI_Events tab "
                sqlJFR += "SET PandaID=:pandaID,status=:newStatus,processed_upto_eventID=NULL "
                sqlJFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlJFR += "AND status=:oldStatus AND is_jumbo=:isJumbo "
                sqlJFR += "AND processed_upto_eventID IS NOT NULL "
                for fileSpec in job.Files:
                    if fileSpec.type != "input":
                        continue
                    # done events in jumbo jobs
                    varMap = {}
                    varMap[":jediTaskID"] = fileSpec.jediTaskID
                    varMap[":datasetID"] = fileSpec.datasetID
                    varMap[":fileID"] = fileSpec.fileID
                    varMap[":oldStatus"] = EventServiceUtils.ST_finished
                    varMap[":newStatus"] = EventServiceUtils.ST_done
                    varMap[":isJumbo"] = EventServiceUtils.eventTableIsJumbo
                    self.cur.execute(sqlJE + comment, varMap)
                    nRowDoneJumbo += self.cur.rowcount
                    # lock failed events
                    varMap = {}
                    varMap[":jediTaskID"] = fileSpec.jediTaskID
                    varMap[":datasetID"] = fileSpec.datasetID
                    varMap[":fileID"] = fileSpec.fileID
                    varMap[":oldStatus"] = EventServiceUtils.ST_failed
                    varMap[":newStatus"] = EventServiceUtils.ST_reserved_fail
                    varMap[":isJumbo"] = EventServiceUtils.eventTableIsJumbo
                    self.cur.execute(sqlJFL + comment, varMap)
                    tmpNumRow = self.cur.rowcount
                    if tmpNumRow > 0:
                        # copy failed events
                        varMap = {}
                        varMap[":jediTaskID"] = fileSpec.jediTaskID
                        varMap[":datasetID"] = fileSpec.datasetID
                        varMap[":fileID"] = fileSpec.fileID
                        varMap[":oldStatus"] = EventServiceUtils.ST_reserved_fail
                        varMap[":newStatus"] = EventServiceUtils.ST_ready
                        varMap[":isJumbo"] = EventServiceUtils.eventTableIsJumbo
                        self.cur.execute(sqlJFC + comment, varMap)
                        # release failed events. Change PandaID to avoid unique constraint of PK
                        varMap = {}
                        varMap[":jediTaskID"] = fileSpec.jediTaskID
                        varMap[":datasetID"] = fileSpec.datasetID
                        varMap[":fileID"] = fileSpec.fileID
                        varMap[":pandaID"] = pandaID
                        varMap[":oldStatus"] = EventServiceUtils.ST_reserved_fail
                        varMap[":newStatus"] = EventServiceUtils.ST_failed
                        varMap[":isJumbo"] = EventServiceUtils.eventTableIsJumbo
                        self.cur.execute(sqlJFR + comment, varMap)
                        nRowFailedJumbo += tmpNumRow
                tmp_log.debug(f"set done for jumbo to {nRowDoneJumbo} event ranges")
                tmp_log.debug(f"copied {nRowFailedJumbo} failed event ranges in jumbo")
            # change status to done
            sqlED = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events SET status=:newStatus "
            sqlED += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND status=:oldStatus "
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":pandaID"] = pandaID
            varMap[":oldStatus"] = EventServiceUtils.ST_finished
            varMap[":newStatus"] = EventServiceUtils.ST_done
            self.cur.execute(sqlED + comment, varMap)
            nRowDone = self.cur.rowcount
            tmp_log.info(f"set done to n_er_done={nRowDone} event ranges")
            # release unprocessed event ranges
            sqlEC = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events "
            if jobSpec.decAttOnFailedES():
                sqlEC += "SET status=:newStatus,pandaID=:jobsetID "
            else:
                sqlEC += "SET status=:newStatus,attemptNr=attemptNr-1,pandaID=:jobsetID "
            sqlEC += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND NOT status IN (:esDone,:esFailed,:esDiscarded,:esCancelled) "
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":pandaID"] = pandaID
            varMap[":jobsetID"] = jobSpec.jobsetID
            varMap[":esDone"] = EventServiceUtils.ST_done
            varMap[":esFailed"] = EventServiceUtils.ST_failed
            varMap[":esDiscarded"] = EventServiceUtils.ST_discarded
            varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
            varMap[":newStatus"] = EventServiceUtils.ST_ready
            self.cur.execute(sqlEC + comment, varMap)
            nRowReleased = self.cur.rowcount
            tmp_log.info(f"released n_er_released={nRowReleased} event ranges")
            # copy failed event ranges
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":pandaID"] = pandaID
            varMap[":jobsetID"] = jobSpec.jobsetID
            varMap[":esFailed"] = EventServiceUtils.ST_failed
            varMap[":newStatus"] = EventServiceUtils.ST_ready
            sqlEF = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Events "
            sqlEF += "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
            sqlEF += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID,event_offset) "
            sqlEF += "SELECT jediTaskID,datasetID,:jobsetID,fileID,attemptNr-1,:newStatus,"
            sqlEF += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID,event_offset "
            sqlEF += f"FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlEF += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND status=:esFailed "
            sqlEF += "AND processed_upto_eventID IS NOT NULL "
            self.cur.execute(sqlEF + comment, varMap)
            nRowCopied = self.cur.rowcount
            tmp_log.debug(f"copied {nRowCopied} failed event ranges")
            # unset processed_upto for failed events
            sqlUP = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events SET processed_upto_eventID=NULL "
            sqlUP += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND status=:esFailed "
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":pandaID"] = pandaID
            varMap[":esFailed"] = EventServiceUtils.ST_failed
            self.cur.execute(sqlUP + comment, varMap)
            nRowFailed = self.cur.rowcount
            tmp_log.info(f"failed n_er_failed={nRowFailed} event ranges")
            sqlEU = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlEU += f"COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlEU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND attemptNr=:minAttempt "
            sqlEU += "AND NOT status IN (:esDiscarded,:esCancelled) "
            # look for hopeless event ranges
            nRowFatal = 0
            for fileSpec in job.Files:
                if fileSpec.type != "input":
                    continue
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = fileSpec.datasetID
                varMap[":fileID"] = fileSpec.fileID
                varMap[":minAttempt"] = 0
                varMap[":esDiscarded"] = EventServiceUtils.ST_discarded
                varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
                self.cur.execute(sqlEU + comment, varMap)
                resEU = self.cur.fetchone()
                if resEU is not None:
                    nRowFatal += resEU[0]
            # there is hopeless event ranges
            tmp_log.info(f"has n_hopeless={nRowFatal} hopeless event ranges")
            if nRowFatal != 0:
                if jobSpec.acceptPartialFinish():
                    # set fatal to hopeless event ranges
                    sqlFH = f"UPDATE {panda_config.schemaJEDI}.JEDI_Events SET status=:esFatal "
                    sqlFH += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID AND attemptNr=:minAttempt AND status<>:esFatal "
                    varMap = {}
                    varMap[":jediTaskID"] = jobSpec.jediTaskID
                    varMap[":jobsetID"] = jobSpec.jobsetID
                    varMap[":esFatal"] = EventServiceUtils.ST_fatal
                    varMap[":minAttempt"] = 0
                    self.cur.execute(sqlFH + comment, varMap)
            # look for event ranges to process
            sqlERP = f"SELECT job_processID FROM {panda_config.schemaJEDI}.JEDI_Events "
            sqlERP += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID AND status=:esReady "
            sqlERP += "AND attemptNr>:minAttempt "
            varMap = {}
            varMap[":jediTaskID"] = jobSpec.jediTaskID
            varMap[":jobsetID"] = jobSpec.jobsetID
            varMap[":esReady"] = EventServiceUtils.ST_ready
            varMap[":minAttempt"] = 0
            self.cur.execute(sqlERP + comment, varMap)
            resERP = self.cur.fetchall()
            nRow = len(resERP)
            tmp_log.info(f"left n_er_unprocessed={nRow} unprocessed event ranges")
            otherRunning = False
            hasDoneRange = False
            if nRow == 0:
                # check if other consumers finished
                sqlEOC = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlEOC += f"job_processID,attemptNr,status,processed_upto_eventID,PandaID FROM {panda_config.schemaJEDI}.JEDI_Events tab "
                sqlEOC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlEOC += "AND ((NOT status IN (:esDone,:esDiscarded,:esCancelled,:esFatal,:esFailed,:esCorrupted) AND attemptNr>:minAttempt) "
                sqlEOC += "OR (status=:esFailed AND processed_upto_eventID IS NOT NULL)) "
                # count the number of done ranges
                sqlCDO = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlCDO += f"COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events tab "
                sqlCDO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlCDO += "AND status=:esDone AND rownum=1 "
                for fileSpec in job.Files:
                    if fileSpec.type == "input":
                        varMap = {}
                        varMap[":jediTaskID"] = fileSpec.jediTaskID
                        varMap[":datasetID"] = fileSpec.datasetID
                        varMap[":fileID"] = fileSpec.fileID
                        varMap[":esDone"] = EventServiceUtils.ST_done
                        varMap[":esDiscarded"] = EventServiceUtils.ST_discarded
                        varMap[":esCancelled"] = EventServiceUtils.ST_cancelled
                        varMap[":esCorrupted"] = EventServiceUtils.ST_corrupted
                        varMap[":esFatal"] = EventServiceUtils.ST_fatal
                        varMap[":esFailed"] = EventServiceUtils.ST_failed
                        varMap[":minAttempt"] = 0
                        self.cur.execute(sqlEOC + comment, varMap)
                        resEOC = self.cur.fetchone()
                        if resEOC is not None:
                            # there are unprocessed ranges
                            otherRunning = True
                            eocDump = dict()
                            eocDump["jediTaskID"] = fileSpec.jediTaskID
                            eocDump["datasetID"] = fileSpec.datasetID
                            eocDump["fileID"] = fileSpec.fileID
                            eocDump["job_processID"] = resEOC[0]
                            eocDump["attemptNr"] = resEOC[1]
                            eocDump["status"] = resEOC[2]
                            eocDump["processed_upto_eventID"] = resEOC[3]
                            eocDump["PandaID"] = resEOC[4]
                            tmp_log.debug(f"some event ranges still running like {str(eocDump)}")
                            break
                        # check if there are done ranges
                        if not hasDoneRange:
                            varMap = {}
                            varMap[":jediTaskID"] = fileSpec.jediTaskID
                            varMap[":datasetID"] = fileSpec.datasetID
                            varMap[":fileID"] = fileSpec.fileID
                            varMap[":esDone"] = EventServiceUtils.ST_done
                            self.cur.execute(sqlCDO + comment, varMap)
                            resCDO = self.cur.fetchone()
                            (nCDORow,) = resCDO
                            if nCDORow != 0:
                                hasDoneRange = True
                # do merging since all ranges were done
                if not otherRunning:
                    doMerging = True
            else:
                doMerging = False
            # do nothing since other consumers are still running
            if otherRunning:
                tmp_log.debug(f"do nothing as other consumers are still running")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                if nRowDone == 0:
                    # didn't process any events
                    retValue = 5, None
                else:
                    # processed some events
                    retValue = 4, None
                return retValue
            # all failed
            if doMerging and not hasDoneRange:
                # fail immediately
                tmp_log.debug(f"all event ranges failed")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                retValue = 7, None
                return retValue
            # fail immediately if not all events were done in the largest attemptNr
            if (jobSpec.attemptNr >= jobSpec.maxAttempt and not (doMerging and hasDoneRange)) or (doMerging and nRowFatal > 0):
                tmp_log.debug(f"no more retry since not all events were done in the largest attemptNr")
                # check if there is active consumer
                sqlAC = "SELECT COUNT(*) FROM ("
                sqlAC += "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
                sqlAC += "WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
                sqlAC += "UNION "
                sqlAC += "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
                sqlAC += "WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
                sqlAC += ") "
                varMap = {}
                varMap[":jediTaskID"] = jobSpec.jediTaskID
                varMap[":jobsetID"] = jobSpec.jobsetID
                self.cur.execute(sqlAC + comment, varMap)
                resAC = self.cur.fetchone()
                (numActiveEC,) = resAC
                tmp_log.debug(f"num of active consumers = {numActiveEC}")
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError("Commit error")
                if numActiveEC <= 1:
                    # last one
                    retValue = 6, None
                else:
                    # there are active consumers
                    retValue = 5, None
                return retValue
            # no merging for inaction ES jobs
            if doMerging and nRowDoneJumbo == 0 and nRowDone == 0 and not job.allOkEvents():
                tmp_log.debug(f"skip merge generation since nDone=0")
                retValue = 5, None
                return retValue
            # change waiting file status
            if doMerging and EventServiceUtils.isCoJumboJob(jobSpec):
                # update file
                sqlUWF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlUWF += "SET status=:newStatus,is_waiting=NULL "
                sqlUWF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlUWF += "AND attemptNr=:attemptNr AND status=:oldStatus AND keepTrack=:keepTrack "
                # update dataset
                sqlUWD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlUWD += "SET nFilesUsed=nFilesUsed+:nDiff,nFilesWaiting=nFilesWaiting-:nDiff "
                sqlUWD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                nFilesUsedMap = {}
                for fileSpec in job.Files:
                    if fileSpec.type not in ["input", "pseudo_input"]:
                        continue
                    varMap = {}
                    varMap[":jediTaskID"] = fileSpec.jediTaskID
                    varMap[":datasetID"] = fileSpec.datasetID
                    varMap[":fileID"] = fileSpec.fileID
                    varMap[":attemptNr"] = fileSpec.attemptNr
                    varMap[":newStatus"] = "running"
                    varMap[":oldStatus"] = "ready"
                    varMap[":keepTrack"] = 1
                    self.cur.execute(sqlUWF + comment, varMap)
                    nDiff = self.cur.rowcount
                    if nDiff > 0:
                        nFilesUsedMap.setdefault(fileSpec.datasetID, 0)
                        nFilesUsedMap[fileSpec.datasetID] += nDiff
                for datasetID in nFilesUsedMap:
                    nDiff = nFilesUsedMap[datasetID]
                    varMap = {}
                    varMap[":jediTaskID"] = jobSpec.jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":nDiff"] = nDiff
                    self.cur.execute(sqlUWD + comment, varMap)
            # check if there is fatal range
            hasFatalRange = False
            if doMerging:
                sqlCFE = f"SELECT COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
                sqlCFE += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID AND "
                sqlCFE += "status=:esFatal AND rownum=1 "
                varMap = {}
                varMap[":jediTaskID"] = jobSpec.jediTaskID
                varMap[":jobsetID"] = jobSpec.jobsetID
                varMap[":esFatal"] = EventServiceUtils.ST_fatal
                self.cur.execute(sqlCFE + comment, varMap)
                resCFE = self.cur.fetchone()
                (nRowCEF,) = resCFE
                tmp_log.debug(f"{nRowCEF} fatal event ranges ")
                if nRowCEF > 0:
                    hasFatalRange = True
            # reset job attributes
            jobSpec.startTime = None
            jobSpec.creationTime = naive_utcnow()
            jobSpec.modificationTime = jobSpec.creationTime
            jobSpec.stateChangeTime = jobSpec.creationTime
            jobSpec.prodDBUpdateTime = jobSpec.creationTime
            jobSpec.attemptNr += 1
            jobSpec.batchID = None
            jobSpec.schedulerID = None
            jobSpec.pilotID = None
            if doMerging:
                jobSpec.maxAttempt = jobSpec.attemptNr
                jobSpec.currentPriority = 5000
            else:
                jobSpec.currentPriority += 1
            jobSpec.endTime = None
            jobSpec.transExitCode = None
            jobSpec.jobMetrics = None
            jobSpec.jobSubStatus = None
            jobSpec.actualCoreCount = None
            jobSpec.hs06sec = None
            jobSpec.nEvents = None
            jobSpec.cpuConsumptionTime = None
            # disable background flag
            jobSpec.jobExecutionID = 0
            if hasFatalRange:
                jobSpec.jobSubStatus = "partial"
            for attr in jobSpec._attributes:
                for patt in [
                    "ErrorCode",
                    "ErrorDiag",
                    "CHAR",
                    "BYTES",
                    "RSS",
                    "PSS",
                    "VMEM",
                    "SWAP",
                ]:
                    if attr.endswith(patt):
                        setattr(jobSpec, attr, None)
                        break
            # read files
            varMap = {}
            varMap[":PandaID"] = pandaID
            sqlFile = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
            sqlFile += "WHERE PandaID=:PandaID"
            self.cur.arraysize = 100000
            self.cur.execute(sqlFile + comment, varMap)
            resFs = self.cur.fetchall()
            # loop over all files
            for resF in resFs:
                # add
                fileSpec = FileSpec()
                fileSpec.pack(resF)
                jobSpec.addFile(fileSpec)
                # reset file status
                if fileSpec.type in ["output", "log"]:
                    fileSpec.status = "unknown"
            # set current status if unspecified
            if currentJobStatus is None:
                currentJobStatus = "activated"
                for fileSpec in jobSpec.Files:
                    if fileSpec.type == "input" and fileSpec.status != "ready":
                        currentJobStatus = "assigned"
                        break
            if doMerging and currentJobStatus == "assigned":
                # send merge jobs to activated since input data don't have to move
                tmp_log.debug(f"sending to activated")
                jobSpec.jobStatus = "activated"
            elif currentJobStatus in ["defined", "assigned", "waiting", "pending"]:
                jobSpec.jobStatus = currentJobStatus
            else:
                jobSpec.jobStatus = "activated"
            # read job parameters
            sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
            varMap = {}
            varMap[":PandaID"] = jobSpec.PandaID
            self.cur.execute(sqlJobP + comment, varMap)
            for (clobJobP,) in self.cur:
                try:
                    jobSpec.jobParameters = clobJobP.read()
                except AttributeError:
                    jobSpec.jobParameters = str(clobJobP)
                break
            # changes some attributes
            noNewJob = False
            closedInBadStatus = False
            if not doMerging:
                minUnprocessed = self.getConfigValue("dbproxy", "AES_MINEVENTSFORMCORE")

                sqlCore = (
                    "SELECT /* use_json_type */ scj.data.corecount, scj.data.status, scj.data.jobseed "
                    "FROM ATLAS_PANDA.schedconfig_json scj "
                    "WHERE scj.panda_queue=:siteid "
                )

                varMap = {}
                varMap[":siteid"] = jobSpec.computingSite
                self.cur.execute(sqlCore + comment, varMap)
                resCore = self.cur.fetchone()
                if resCore is not None:
                    coreCount, tmpState, tmpJobSeed = resCore
                    if coreCount is not None:
                        coreCount = int(coreCount)
                        if minUnprocessed is None:
                            minUnprocessed = coreCount
                        else:
                            minUnprocessed = max(minUnprocessed, coreCount)

                    if tmpState not in ["online", "brokeroff"] or tmpJobSeed == "std":
                        noNewJob = True
                if jobSpec.coreCount > 1 and minUnprocessed is not None and minUnprocessed > nRow:
                    get_task_event_module(self).setScoreSiteToEs(jobSpec, comment, comment)
                # not to repeat useless consumers
                if currentJobStatus in ["defined", "pending"]:
                    noNewJob = True
                    closedInBadStatus = True
            else:
                # extract parameters for merge
                try:
                    tmpMatch = re.search(
                        "<PANDA_ESMERGE_TRF>(.*)</PANDA_ESMERGE_TRF>",
                        jobSpec.jobParameters,
                    )
                    jobSpec.transformation = tmpMatch.group(1)
                except Exception:
                    pass
                try:
                    tmpMatch = re.search("<PANDA_EVSMERGE>(.*)</PANDA_EVSMERGE>", jobSpec.jobParameters)
                    jobSpec.jobParameters = tmpMatch.group(1)
                except Exception:
                    pass
                # use siteid of jumbo jobs to generate merge jobs for fake co-jumbo
                isFakeCJ = False
                if jobSpec.computingSite == EventServiceUtils.siteIdForWaitingCoJumboJobs:
                    isFakeCJ = True
                    # sql to get PandaIDs of jumbo jobs
                    sqlJJ = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                    sqlJJ += f"DISTINCT PandaID FROM {panda_config.schemaJEDI}.JEDI_Events tab "
                    sqlJJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    sqlJJ += "AND status=:esDone AND is_jumbo=:isJumbo "
                    # sql to get siteid of jumbo job
                    sqlJJS = "SELECT computingSite FROM ATLAS_PANDA.jobsActive4 "
                    sqlJJS += "WHERE PandaID=:PandaID "
                    sqlJJS += "UNION "
                    sqlJJS += "SELECT computingSite FROM ATLAS_PANDA.jobsArchived4 "
                    sqlJJS += "WHERE PandaID=:PandaID "
                    sqlJJS += "UNION "
                    sqlJJS += "SELECT computingSite FROM ATLAS_PANDAARCH.jobsArchived "
                    sqlJJS += "WHERE PandaID=:PandaID AND modificationTime>CURRENT_DATE-30 "
                    # look for jumbo jobs
                    toEscape = False
                    for fileSpec in job.Files:
                        if fileSpec.type != "input":
                            continue
                        # get PandaIDs of jumbo jobs
                        varMap = {}
                        varMap[":jediTaskID"] = fileSpec.jediTaskID
                        varMap[":datasetID"] = fileSpec.datasetID
                        varMap[":fileID"] = fileSpec.fileID
                        varMap[":esDone"] = EventServiceUtils.ST_done
                        varMap[":isJumbo"] = EventServiceUtils.eventTableIsJumbo
                        self.cur.execute(sqlJJ + comment, varMap)
                        resJJList = self.cur.fetchall()
                        for (jPandaID,) in resJJList:
                            # get siteid of jumbo job
                            varMap = {}
                            varMap[":PandaID"] = jPandaID
                            self.cur.execute(sqlJJS + comment, varMap)
                            resJJS = self.cur.fetchone()
                            if resJJS is not None:
                                tmpStr = f"changed co-jumbo site {jobSpec.computingSite} "
                                jobSpec.computingSite = resJJS[0]
                                tmpStr += f"to {jobSpec.computingSite}"
                                toEscape = True
                                tmp_log.debug(tmpStr)
                                break
                        if toEscape:
                            break
                # change special handling and set the share to express for merge jobs
                EventServiceUtils.setEventServiceMerge(jobSpec)
                # set site
                get_task_event_module(self).setSiteForEsMerge(jobSpec, isFakeCJ, comment, comment)
                jobSpec.coreCount = None
                jobSpec.minRamCount = 2000

            # reset resource type
            jobSpec.resource_type = get_entity_module(self).get_resource_type_job(jobSpec)

            # no new job since ES is disabled
            if noNewJob:
                jobSpec.PandaID = None
                msgStr = f"No new job since event service is disabled or queue is offline or old job status {currentJobStatus} is not active"
                tmp_log.debug(msgStr)
            else:
                # update input
                if doMerging:
                    get_task_event_module(self).updateInputStatusJedi(jobSpec.jediTaskID, jobSpec.PandaID, "merging")
                else:
                    get_task_event_module(self).updateInputStatusJedi(jobSpec.jediTaskID, jobSpec.PandaID, "queued", checkOthers=True)
                # insert job with new PandaID
                if jobSpec.jobStatus in ["defined", "assigned", "pending", "waiting"]:
                    table_name = "jobsDefined4"
                else:
                    table_name = "jobsActive4"
                sql1 = f"INSERT INTO {panda_config.schemaPANDA}.{table_name} ({JobSpec.columnNames()}) "
                sql1 += JobSpec.bindValuesExpression(useSeq=True)
                sql1 += " RETURNING PandaID INTO :newPandaID"
                # set parentID
                jobSpec.parentID = jobSpec.PandaID
                varMap = jobSpec.valuesMap(useSeq=True)
                varMap[":newPandaID"] = self.cur.var(varNUMBER)
                # insert
                if not noNewJob:
                    retI = self.cur.execute(sql1 + comment, varMap)
                    # set PandaID
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newPandaID"]))
                    jobSpec.PandaID = int(val)
                else:
                    jobSpec.PandaID = None
                msgStr = f"Generate new PandaID -> {jobSpec.PandaID}#{jobSpec.attemptNr} at {jobSpec.computingSite} "
                if doMerging:
                    msgStr += "for merge"
                else:
                    msgStr += "for retry"
                tmp_log.debug(msgStr)
                # insert files
                sqlFile = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
                sqlFile += FileSpec.bindValuesExpression(useSeq=True)
                sqlFile += " RETURNING row_ID INTO :newRowID"
                sqlMaxFail = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlMaxFail += "SET maxFailure=(CASE "
                sqlMaxFail += "WHEN maxFailure IS NULL THEN failedAttempt+:increase "
                sqlMaxFail += "WHEN maxFailure>failedAttempt+:increase THEN failedAttempt+:increase "
                sqlMaxFail += "ELSE maxFailure "
                sqlMaxFail += "END) "
                sqlMaxFail += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlMaxFail += "AND keepTrack=:keepTrack "
                for fileSpec in jobSpec.Files:
                    # skip zip
                    if fileSpec.type.startswith("zip"):
                        continue
                    # reset rowID
                    fileSpec.row_ID = None
                    # change GUID and LFN for log
                    if fileSpec.type == "log":
                        fileSpec.GUID = str(uuid.uuid4())
                        if doMerging:
                            fileSpec.lfn = re.sub(
                                f"\\.{pandaID}$",
                                "".format(jobSpec.PandaID),
                                fileSpec.lfn,
                            )
                        else:
                            fileSpec.lfn = re.sub(
                                f"\\.{pandaID}$",
                                f".{jobSpec.PandaID}",
                                fileSpec.lfn,
                            )
                    # insert
                    varMap = fileSpec.valuesMap(useSeq=True)
                    varMap[":newRowID"] = self.cur.var(varNUMBER)
                    self.cur.execute(sqlFile + comment, varMap)
                    val = self.getvalue_corrector(self.cur.getvalue(varMap[":newRowID"]))
                    fileSpec.row_ID = int(val)
                    # change max failure for esmerge
                    if doMerging and fileSpec.type in ["input", "pseudo_input"]:
                        varMap = {}
                        varMap[":jediTaskID"] = fileSpec.jediTaskID
                        varMap[":datasetID"] = fileSpec.datasetID
                        varMap[":fileID"] = fileSpec.fileID
                        varMap[":increase"] = 5
                        varMap[":keepTrack"] = 1
                        self.cur.execute(sqlMaxFail + comment, varMap)
                # insert job parameters
                sqlJob = "INSERT INTO ATLAS_PANDA.jobParamsTable (PandaID,jobParameters) VALUES (:PandaID,:param)"
                varMap = {}
                varMap[":PandaID"] = jobSpec.PandaID
                varMap[":param"] = jobSpec.jobParameters
                self.cur.execute(sqlJob + comment, varMap)
                # propagate change to JEDI
                if doMerging:
                    relationType = "es_merge"
                else:
                    relationType = None
                self.updateForPilotRetryJEDI(jobSpec, self.cur, onlyHistory=True, relationType=relationType)
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            # set return
            if not doMerging:
                if closedInBadStatus:
                    # closed in bad status
                    retValue = 9, jobSpec.PandaID
                elif nRowDone == 0:
                    # didn't process any events
                    retValue = 8, jobSpec.PandaID
                else:
                    # processed some events
                    retValue = 0, jobSpec.PandaID
            else:
                if nRowDone == 0:
                    retValue = 10, jobSpec.PandaID
                else:
                    retValue = 2, jobSpec.PandaID
            # record status change
            try:
                if not noNewJob:
                    self.recordStatusChange(
                        jobSpec.PandaID,
                        jobSpec.jobStatus,
                        jobInfo=jobSpec,
                        useCommit=useCommit,
                    )
                    self.push_job_status_message(jobSpec, jobSpec.PandaID, jobSpec.jobStatus)
            except Exception:
                tmp_log.error("recordStatusChange in ppEventServiceJob")
            tmp_log.debug(f"done for doMergeing={doMerging}")
            if retValue[-1] == "NULL":
                retValue = retValue[0], None
            return retValue
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None, None


# get module
def get_job_complex_module(base_mod) -> JobComplexModule:
    return base_mod.get_composite_module("job_complex")
