import copy
import datetime
import glob
import json
import os
import random
import re
import time
from typing import Dict, List, Tuple

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils, srv_msg_utils
from pandaserver.taskbuffer import EventServiceUtils, PrioUtil
from pandaserver.taskbuffer.DataCarousel import (
    DataCarouselRequestSpec,
    DataCarouselRequestStatus,
    get_resubmit_request_spec,
)
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from pandaserver.taskbuffer.db_proxy_mods.base_module import (
    BaseModule,
    SQL_QUEUE_TOPIC_async_dataset_update,
    memoize,
    varNUMBER,
)
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JediDatasetSpec import (
    INPUT_TYPES_var_map,
    INPUT_TYPES_var_str,
    JediDatasetSpec,
)
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.JobSpec import JobSpec


# Module class to define miscellaneous standalone methods that are independent of another module's methods
class MiscStandaloneModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self, jediTaskID):
        comment = " /* DBProxy.getPandaIDsWithTaskID */"
        tmp_log = self.create_tagged_logger(comment, f"<jediTaskID={jediTaskID}>")
        tmp_log.debug("start")
        # SQL
        sql = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        sql += "UNION "
        sql += "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        sql += "UNION "
        sql += "SELECT PandaID FROM ATLAS_PANDA.jobsArchived4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000000
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retList = []
            for (pandaID,) in res:
                retList.append(pandaID)

            tmp_log.debug(f"found {len(retList)} IDs")
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # change task priority
    def changeTaskPriorityPanda(self, jediTaskID, newPriority):
        comment = " /* DBProxy.changeTaskPriorityPanda */"
        tmp_log = self.create_tagged_logger(comment, f"<jediTaskID={jediTaskID}>")
        tmp_log.debug(f"newPrio={newPriority}")
        try:
            # sql to update JEDI task table
            sqlT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET currentPriority=:newPriority WHERE jediTaskID=:jediTaskID "
            # sql to update DEFT task table
            schemaDEFT = panda_config.schemaDEFT
            sqlD = f"UPDATE {schemaDEFT}.T_TASK SET current_priority=:newPriority,timestamp=CURRENT_DATE WHERE taskid=:jediTaskID "
            # update job priorities
            sqlJ = "UPDATE ATLAS_PANDA.{0} SET currentPriority=:newPriority WHERE jediTaskID=:jediTaskID "
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":newPriority"] = newPriority
            # update JEDI
            self.cur.execute(sqlT + comment, varMap)
            nRow = self.cur.rowcount
            if nRow == 1:
                # update jobs
                for tableName in ["jobsActive4", "jobsDefined4"]:
                    self.cur.execute(sqlJ.format(tableName) + comment, varMap)
            # update DEFT
            self.cur.execute(sqlD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {nRow}")
            return nRow
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get jediTaskID from taskName
    def getTaskIDwithTaskNameJEDI(self, userName, taskName):
        comment = " /* DBProxy.getTaskIDwithTaskNameJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"<userName={userName} taskName={taskName}")
        tmp_log.debug(f"start")
        try:
            # begin transaction
            self.conn.begin()
            # sql to get jediTaskID
            sqlGF = f"SELECT MAX(jediTaskID) FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlGF += "WHERE userName=:userName AND taskName=:taskName "
            varMap = {}
            varMap[":userName"] = userName
            varMap[":taskName"] = taskName
            self.cur.execute(sqlGF + comment, varMap)
            resFJ = self.cur.fetchone()
            if resFJ is not None:
                (jediTaskID,) = resFJ
            else:
                jediTaskID = None
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"jediTaskID={jediTaskID}")
            return jediTaskID
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # update modificationtime for a jediTaskID to trigger subsequent process
    def updateTaskModTimeJEDI(self, jediTaskID, newStatus):
        comment = " /* DBProxy.updateTaskErrorDialogJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"<jediTaskID={jediTaskID}>")
        tmp_log.debug(f"start")
        try:
            # begin transaction
            self.conn.begin()
            # update mod time
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            if newStatus is not None:
                varMap[":newStatus"] = newStatus
            sqlUE = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET "
            sqlUE += "modificationTime=CURRENT_DATE-1,"
            if newStatus is not None:
                sqlUE += "status=:newStatus,oldStatus=NULL,"
            sqlUE = sqlUE[:-1]
            sqlUE += " WHERE jediTaskID=:jediTaskID "
            self.cur.execute(sqlUE + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    def initialize_cpu_time_task(self, job_id, task_id, site_id, files, active):
        """
        Increases the CPU time of a task
        walltime = basewalltime + cpuefficiency*CPUTime*nEvents/Corepower/Corecount

        CPU time: execution time per event
        Walltime: time for a job
        Corepower: HS06 score
        Basewalltime: Setup time, time to download, etc. taken by the pilot
        """
        comment = " /* DBProxy.initialize_cpu_time_task */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job_id}; jediTaskID={task_id}; siteID={site_id}")
        tmp_log.debug("start")

        # See if there are successful jobs for this task. If yes, skip this method
        sql = (
            f"SELECT 1 FROM "
            f"(SELECT 1 FROM atlas_panda.jobsarchived4 "
            f"WHERE jeditaskid = :jedi_task_id AND jobstatus = 'finished' AND transformation NOT LIKE '%build%' AND ROWNUM = 1 "
            f"UNION ALL "
            f"SELECT 1 FROM atlas_pandaarch.jobsarchived "
            f"WHERE jeditaskid = :jedi_task_id AND jobstatus = 'finished' AND transformation NOT LIKE '%build%' AND ROWNUM = 1) "
            f"WHERE ROWNUM = 1"
        )
        var_map = {"jedi_task_id": task_id}
        self.cur.execute(sql + comment, var_map)
        exists = False
        if self.cur.fetchone():
            exists = True

        # if we found a successful job, we skip the CPU time increase
        if exists:
            tmp_log.debug(f"Task {task_id} already has successful jobs, skipping CPU time increase and leaving it up to the scouting mechanism")
            return None, None

        # Get the site information from schedconfig
        sql = (
            "SELECT /* use_json_type */ sc.data.maxtime, sc.data.corepower, NVL(TO_NUMBER(sc.data.corecount), 1) AS corecount "
            "FROM ATLAS_PANDA.schedconfig_json sc "
            "WHERE sc.panda_queue= :site_id "
        )

        var_map = {"site_id": site_id}
        self.cur.execute(sql + comment, var_map)
        site_parameters = self.cur.fetchone()

        if not site_parameters:
            tmp_log.debug(f"No site parameters retrieved for {site_id}")

        (max_time_site, core_power_site, core_count_site) = site_parameters
        tmp_log.debug(f"site_id {site_id} has parameters: max_time_site {max_time_site}, core_power_site {core_power_site}, core_count_site {core_count_site}")
        if (not max_time_site) or (not core_power_site) or (not core_count_site):
            tmp_log.debug(f"One or more site parameters are not defined for {site_id}... nothing to do")
            return None, None
        else:
            (max_time_site, core_power_site, core_count_site) = (
                int(max_time_site),
                float(core_power_site),
                int(core_count_site),
            )

        # Get the task information
        sql = """
        SELECT jt.cputime, jt.walltime, jt.basewalltime, jt.cpuefficiency, jt.cputimeunit
        FROM ATLAS_PANDA.jedi_tasks jt
        WHERE jt.jeditaskid=:jeditaskid
        """
        var_map = {"jeditaskid": task_id}
        self.cur.execute(sql + comment, var_map)
        task_parameters = self.cur.fetchone()

        if not task_parameters:
            tmp_log.debug(f"No task parameters retrieved for jeditaskid {task_id}... nothing to do")
            return None, None

        (old_cputime, walltime, basewalltime, cpuefficiency, old_cputime_unit) = task_parameters
        if not cpuefficiency or not basewalltime:
            tmp_log.debug(f"CPU efficiency and/or basewalltime are not defined for task {task_id}... nothing to do")
            return None, None

        tmp_log.debug(
            f"task {task_id} has parameters: cputime {old_cputime} {old_cputime_unit}, walltime {walltime}, "
            f"basewalltime {basewalltime}, cpuefficiency {cpuefficiency}"
        )

        old_cputime_normalized = None
        if old_cputime is not None:
            if old_cputime_unit == "HS06sPerEvent":
                old_cputime_normalized = old_cputime
            elif old_cputime_unit == "mHS06sPerEvent":
                old_cputime_normalized = old_cputime / 1000  # convert to HS06sPerEvent

        # Get the file information
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")
        input_files = list(
            filter(
                lambda pandafile: pandafile.type in input_types and re.search("DBRelease", pandafile.lfn) is None,
                files,
            )
        )
        input_fileIDs = [input_file.fileID for input_file in input_files]
        input_datasetIDs = [input_file.datasetID for input_file in input_files]

        if input_fileIDs:
            var_map = {":taskID": task_id}

            # Bind the files
            file_var_names_str, file_var_map = get_sql_IN_bind_variables(input_fileIDs, prefix=":file")
            var_map.update(file_var_map)

            # Bind the datasets
            ds_var_names_str, ds_var_map = get_sql_IN_bind_variables(input_datasetIDs, prefix=":dataset")
            var_map.update(ds_var_map)

            sql_select = f"""
            SELECT jdc.fileid, jdc.nevents, jdc.startevent, jdc.endevent
            FROM ATLAS_PANDA.JEDI_Dataset_Contents jdc, ATLAS_PANDA.JEDI_Datasets jd
            WHERE jdc.JEDITaskID = :taskID
            AND jdc.datasetID IN ({ds_var_names_str})
            AND jdc.fileID IN ({file_var_names_str})
            AND jd.datasetID = jdc.datasetID
            AND jd.masterID IS NULL
            """
            self.cur.execute(sql_select + comment, var_map)

            result_list = self.cur.fetchall()
            n_events_total = 0
            for fileid, n_events, start_event, end_event in result_list:
                tmp_log.debug(f"event information: fileid {fileid}, n_events {n_events}, start_event {start_event}, end_event {end_event}")

                start_event = start_event if start_event is not None else 0
                end_event = end_event if end_event is not None else 0
                if end_event and start_event:
                    n_events_total += end_event - start_event
                elif n_events:
                    n_events_total += n_events

            if not n_events_total:
                tmp_log.debug(f"n_events could not be calculated for job {job_id}... nothing to do")
                return None, None
        else:
            tmp_log.debug(f"No input files for job {job_id}, so could not update CPU time for task {task_id}")
            return None, None

        # Get the corecount from the job spec
        var_map = {"task_id": task_id, "job_id": job_id}
        sql_select = f"""
        SELECT jact4.corecount 
        FROM ATLAS_PANDA.jobsactive4 jact4
        WHERE jeditaskid = :task_id AND pandaid = :job_id
        UNION
        SELECT jarc4.corecount 
        FROM ATLAS_PANDA.jobsarchived4 jarc4
        WHERE jeditaskid = :task_id AND pandaid = :job_id
        UNION
        SELECT jarch.corecount 
        FROM ATLAS_PANDAARCH.jobsarchived jarch
        WHERE jeditaskid = :task_id AND pandaid = :job_id
        """
        self.cur.execute(sql_select + comment, var_map)

        results = self.cur.fetchone()
        try:
            core_count_job = results[0]
        except (IndexError, TypeError):
            core_count_job = None

        if not core_count_job:
            core_count_job = 1  # Default to 1 if no core_count is defined in the job spec
        tmp_log.debug(f"core_count_job: {core_count_job}")

        # Calculate the new CPU time
        try:
            new_cputime_unit = "HS06sPerEvent"
            new_cputime = ((max_time_site - basewalltime) * core_power_site * core_count_job * 1.1 / (cpuefficiency / 100.0) / n_events_total) * 1.5
            new_cputime_normalized = new_cputime

            if new_cputime and new_cputime < 10:
                new_cputime = new_cputime * 1000
                new_cputime_unit = "mHS06sPerEvent"

            # the entry is stored without decimals in the DB
            new_cputime = int(new_cputime)

            tmp_log.debug(f"Old CPU time is {old_cputime} {old_cputime_unit} and possible new CPU time is {new_cputime} {new_cputime_unit}")

            if old_cputime_normalized is not None and new_cputime_normalized is not None and old_cputime_normalized > new_cputime_normalized:
                tmp_log.debug(
                    f"Skipping CPU time increase since old CPU time {old_cputime_normalized} HS06sPerEvent "
                    f"> new CPU time {new_cputime_normalized} HS06sPerEvent"
                )
                return None, None

            if active:  # only run the update if active mode. Otherwise return what would have been done
                sql_update_cputime = """
                UPDATE ATLAS_PANDA.jedi_tasks SET cputime=:new_cputime, cputimeunit=:new_cputime_unit
                WHERE jeditaskid=:jeditaskid
                """
                var_map = {":new_cputime": new_cputime, ":new_cputime_unit": new_cputime_unit, ":jeditaskid": task_id}
                self.conn.begin()
                self.cur.execute(sql_update_cputime + comment, var_map)
                if not self._commit():
                    raise RuntimeError("Commit error")

                tmp_log.debug(f"Successfully updated the task CPU time from {old_cputime} to {new_cputime} {new_cputime_unit}")
            else:
                tmp_log.debug("Not updating the task CPU time since active mode is False.")

            return new_cputime, new_cputime_unit

        except (ZeroDivisionError, TypeError) as e:
            tmp_log.debug(f"Exception while updating the task CPU time: {e}")
            return None, None

    def requestTaskParameterRecalculation(self, taskID):
        """
        Requests the recalculation of the CPU time of a task:
         1. set the walltimeUnit to NULL and the modificationTime to Now
         2. AtlasProdWatchDog > JediDBProxy.setScoutJobDataToTasks will pick up tasks with walltimeUnit == NULL
            and modificationTime > Now - 24h. This will trigger a recalculation of the task parameters (outDiskCount,
            outDiskUnit, outDiskCount, walltime, walltimeUnit, cpuTime, ioIntensity, ioIntensityUnit, ramCount, ramUnit,
            workDiskCount, workDiskUnit, workDiskCount)
        """
        comment = " /* DBProxy.requestTaskParameterRecalculation */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={taskID}")
        tmp_log.debug("start")

        timeNow = naive_utcnow()
        timeLimit = timeNow - datetime.timedelta(minutes=30)

        # update the task if it was not already updated in the last 30 minutes (avoid continuous recalculation)
        sql = """
               UPDATE ATLAS_PANDA.jedi_tasks
               SET walltimeUnit=NULL, modificationTime=:timeNow
               WHERE jediTaskId=:taskID AND modificationTime < :timeLimit
               """
        varMap = {"taskID": taskID, "timeNow": timeNow, "timeLimit": timeLimit}
        self.conn.begin()
        self.cur.execute(sql, varMap)

        rowcount = self.cur.rowcount

        if not self._commit():
            raise RuntimeError("Commit error")

        tmp_log.debug("Forced recalculation of CPUTime")
        return rowcount

    # get task parameters
    def getTaskParamsPanda(self, jediTaskID):
        comment = " /* DBProxy.getTaskParamsPanda */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            # sql to get task parameters
            sqlRR = f"SELECT jedi_task_parameters FROM {panda_config.schemaDEFT}.T_TASK "
            sqlRR += "WHERE taskid=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlRR + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # read clob
            taskParams = ""
            for (clobJobP,) in self.cur:
                if clobJobP is not None:
                    try:
                        taskParams = clobJobP.read()
                    except AttributeError:
                        taskParams = str(clobJobP)
                break
            tmp_log.debug("done")
            return taskParams
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return ""

    # get task attributes
    def getTaskAttributesPanda(self, jediTaskID, attrs):
        comment = " /* DBProxy.getTaskAttributesPanda */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            # sql to get task attributes
            sqlRR = "SELECT "
            for attr in attrs:
                sqlRR += f"{attr},"
            sqlRR = sqlRR[:-1]
            sqlRR += f" FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlRR += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlRR + comment, varMap)
            resRR = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retVal = {}
            if resRR is not None:
                for idx, attr in enumerate(attrs):
                    retVal[attr] = resRR[idx]
            tmp_log.debug(f"done {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get task status
    def getTaskStatus(self, jediTaskID):
        comment = " /* DBProxy.getTaskStatus */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            # sql to update input file status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sql = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sql += "WHERE jediTaskID=:jediTaskID "

            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if res:
                tmp_log.debug(f"task {jediTaskID} has status: {res[0]} ")
            else:
                res = []
                tmp_log.debug(f"task {jediTaskID} not found")
            return res
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # reactivate task
    def reactivateTask(self, jediTaskID, keep_attempt_nr=False, trigger_job_generation=False):
        comment = " /* DBProxy.reactivateTask */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            # sql to update task status
            sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sql += "SET status=:status "
            sql += "WHERE jediTaskID=:jediTaskID "
            # sql to get datasetIDs for master
            sqlM = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlM += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3) "
            # sql to increase attempt numbers and update status
            sqlAB = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            if keep_attempt_nr:
                sqlAB += "SET status=:status "
            else:
                sqlAB += "SET status=:status,attemptNr=0 "
            sqlAB += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # sql to update datasets
            sqlD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlD += "SET status=:status,nFilesUsed=0,nFilesTobeUsed=nFiles,nFilesFinished=0,nFilesFailed=0 "
            sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # update task status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":status"] = "ready"
            self.cur.execute(sql + comment, varMap)
            res = self.cur.rowcount
            # get datasetIDs for master
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            varMap[":type3"] = "random_seed"
            self.cur.execute(sqlM + comment, varMap)
            resM = self.cur.fetchall()
            total_nFiles = 0

            for (datasetID,) in resM:
                # increase attempt numbers
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":status"] = "ready"

                # update status and attempt number for datasets
                self.cur.execute(sqlAB + comment, varMap)
                nFiles = self.cur.rowcount

                # update dataset
                if nFiles > 0:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":status"] = "ready"
                    tmp_log.debug(sqlD + comment + str(varMap))
                    self.cur.execute(sqlD + comment, varMap)
                    total_nFiles += nFiles

            tmpMsg = f"updated {total_nFiles} inputs and task {jediTaskID} was reactivated "
            tmp_log.debug(tmpMsg)
            tmp_log.sendMsg(tmpMsg, "jedi", "pandasrv")
            retVal = 0, tmpMsg
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # send message
            if trigger_job_generation:
                # message
                msg = srv_msg_utils.make_message("generate_job", taskid=jediTaskID)
                mb_proxy = self.get_mb_proxy("panda_jedi")
                if mb_proxy:
                    mb_proxy.send(msg)
                    tmp_log.debug(f"sent generate_job message: {msg}")
                else:
                    tmp_log.debug("message queue is not configured")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None, "DB error"

    # get event statistics
    def getEventStat(self, jediTaskID, PandaID):
        comment = " /* DBProxy.getEventStat */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} PandaID={PandaID}")
        tmp_log.debug("start")
        try:
            # sql to get event stats
            sql = f"SELECT status,COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events "
            sql += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
            sql += "GROUP BY status "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get stats
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":PandaID"] = PandaID
            self.cur.execute(sql + comment, varMap)
            resM = self.cur.fetchall()
            retMap = {}
            for eventStatus, cnt in resM:
                retMap[eventStatus] = cnt
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # update error dialog for a jediTaskID
    def updateTaskErrorDialogJEDI(self, jediTaskID, msg):
        comment = " /* DBProxy.updateTaskErrorDialogJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug(f"start")
        try:
            # begin transaction
            self.conn.begin()
            # get existing dialog
            sqlGF = f"SELECT errorDialog FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlGF += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlGF + comment, varMap)
            resFJ = self.cur.fetchone()
            if resFJ is not None:
                # update existing dialog
                (errorDialog,) = resFJ
                errorDialog = msg
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":errorDialog"] = errorDialog
                sqlUE = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET errorDialog=:errorDialog,modificationTime=CURRENT_DATE "
                sqlUE += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sqlUE + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self, jediTaskID, increasedNr):
        comment = " /* DBProxy.increaseAttemptNrPanda */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug(f"increasedNr={increasedNr}")
        try:
            # sql to check task status
            sqlT = f"SELECT status,oldStatus FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlT += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # get task status
            self.cur.execute(sqlT + comment, varMap)
            resT = self.cur.fetchone()
            if resT is None:
                tmpMsg = f"jediTaskID={jediTaskID} not found"
                tmp_log.debug(tmpMsg)
                retVal = 1, tmpMsg
            else:
                taskStatus, oldStatus = resT
                # check task status
                okStatusList = ["running", "scouting", "ready"]
                if taskStatus not in okStatusList and oldStatus not in okStatusList:
                    tmpMsg = f"command rejected since status={taskStatus} or oldStatus={oldStatus} not in {str(okStatusList)}"
                    tmp_log.debug(tmpMsg)
                    retVal = 2, tmpMsg
                else:
                    # sql to get datasetIDs for master
                    sqlM = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                    sqlM += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) "
                    # sql to increase attempt numbers
                    sqlAB = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlAB += "SET maxAttempt=CASE WHEN maxAttempt > attemptNr THEN maxAttempt+:increasedNr ELSE attemptNr+:increasedNr END "
                    sqlAB += ",proc_status=CASE WHEN maxAttempt > attemptNr AND maxFailure > failedAttempt THEN proc_status ELSE :proc_status END "
                    sqlAB += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND keepTrack=:keepTrack "
                    # sql to increase attempt numbers and failure counts
                    sqlAF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlAF += "SET maxAttempt=CASE WHEN maxAttempt > attemptNr THEN maxAttempt+:increasedNr ELSE attemptNr+:increasedNr END "
                    sqlAF += ",maxFailure=maxFailure+:increasedNr "
                    sqlAF += ",proc_status=CASE WHEN maxAttempt > attemptNr AND maxFailure > failedAttempt THEN proc_status ELSE :proc_status END "
                    sqlAF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND keepTrack=:keepTrack "
                    # sql to update datasets
                    sqlD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                    sqlD += "SET nFilesUsed=nFilesUsed-:nFilesReset,nFilesFailed=nFilesFailed-:nFilesReset "
                    sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                    # get datasetIDs for master
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":type1"] = "input"
                    varMap[":type2"] = "pseudo_input"
                    self.cur.execute(sqlM + comment, varMap)
                    resM = self.cur.fetchall()
                    total_nFilesIncreased = 0
                    total_nFilesReset = 0
                    for (datasetID,) in resM:
                        # increase attempt numbers
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = datasetID
                        varMap[":status"] = "ready"
                        varMap[":proc_status"] = "ready"
                        varMap[":keepTrack"] = 1
                        varMap[":increasedNr"] = increasedNr
                        nFilesIncreased = 0
                        nFilesReset = 0
                        # still active and maxFailure is undefined
                        sqlA = sqlAB + "AND maxAttempt>attemptNr AND maxFailure IS NULL "
                        self.cur.execute(sqlA + comment, varMap)
                        nRow = self.cur.rowcount
                        nFilesIncreased += nRow
                        # still active and maxFailure is defined
                        sqlA = sqlAF + "AND maxAttempt>attemptNr AND (maxFailure IS NOT NULL AND maxFailure>failedAttempt) "
                        self.cur.execute(sqlA + comment, varMap)
                        nRow = self.cur.rowcount
                        nFilesIncreased += nRow
                        # already done and maxFailure is undefined
                        sqlA = sqlAB + "AND maxAttempt<=attemptNr AND maxFailure IS NULL "
                        self.cur.execute(sqlA + comment, varMap)
                        nRow = self.cur.rowcount
                        nFilesReset += nRow
                        nFilesIncreased += nRow
                        # already done and maxFailure is defined
                        sqlA = sqlAF + "AND (maxAttempt<=attemptNr OR (maxFailure IS NOT NULL AND maxFailure=failedAttempt)) "
                        self.cur.execute(sqlA + comment, varMap)
                        nRow = self.cur.rowcount
                        nFilesReset += nRow
                        nFilesIncreased += nRow
                        # update dataset
                        if nFilesReset > 0:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":datasetID"] = datasetID
                            varMap[":nFilesReset"] = nFilesReset
                            tmp_log.debug(sqlD + comment + str(varMap))
                            self.cur.execute(sqlD + comment, varMap)
                        total_nFilesIncreased += nFilesIncreased
                        total_nFilesReset += nFilesReset
                    tmpMsg = f"increased attemptNr for {total_nFilesIncreased} inputs ({total_nFilesReset} reactivated)"
                    tmp_log.debug(tmpMsg)
                    tmp_log.sendMsg(tmpMsg, "jedi", "pandasrv")
                    retVal = 0, tmpMsg
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None, "DB error"

    # insert sandbox file info
    def insertSandboxFileInfo(self, userName, hostName, fileName, fileSize, checkSum):
        comment = " /* DBProxy.insertSandboxFileInfo */"
        tmp_log = self.create_tagged_logger(comment, f"userName={userName}")
        sqlC = "SELECT userName,fileSize,checkSum FROM ATLAS_PANDAMETA.userCacheUsage "
        sqlC += "WHERE hostName=:hostName AND fileName=:fileName FOR UPDATE"

        sql = "INSERT INTO ATLAS_PANDAMETA.userCacheUsage "
        sql += "(userName,hostName,fileName,fileSize,checkSum,creationTime,modificationTime) "
        sql += "VALUES (:userName,:hostName,:fileName,:fileSize,:checkSum,CURRENT_DATE,CURRENT_DATE) "

        try:
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[":hostName"] = hostName
            varMap[":fileName"] = fileName
            self.cur.arraysize = 10
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchall()
            if len(res) != 0:
                tmp_log.debug(f"skip {hostName} {fileName} since already exists")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                return "WARNING: file exist"
            # insert
            varMap = {}
            varMap[":userName"] = userName
            varMap[":hostName"] = hostName
            varMap[":fileName"] = fileName
            varMap[":fileSize"] = fileSize
            varMap[":checkSum"] = checkSum
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return "OK"
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return "ERROR: DB failure"

    # get and lock sandbox files
    def getLockSandboxFiles(self, time_limit, n_files):
        comment = " /* DBProxy.getLockSandboxFiles */"
        tmp_log = self.create_tagged_logger(comment)
        sqlC = (
            "SELECT * FROM ("
            "SELECT userName,hostName,fileName,creationTime,modificationTime FROM ATLAS_PANDAMETA.userCacheUsage "
            "WHERE modificationTime<:timeLimit AND (fileName like 'sources%' OR fileName like 'jobO%') ) "
            "WHERE rownum<:nRows "
        )
        sqlU = "UPDATE ATLAS_PANDAMETA.userCacheUsage SET modificationTime=CURRENT_DATE " "WHERE userName=:userName AND fileName=:fileName "
        try:
            tmp_log.debug("start")
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[":timeLimit"] = time_limit
            varMap[":nRows"] = n_files
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchall()
            retList = []
            for userName, hostName, fileName, creationTime, modificationTime in res:
                retList.append((userName, hostName, fileName, creationTime, modificationTime))
                # lock
                varMap = dict()
                varMap[":userName"] = userName
                varMap[":fileName"] = fileName
                self.cur.execute(sqlU + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"locked {len(retList)} files")
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # check duplicated sandbox file
    def checkSandboxFile(self, dn, fileSize, checkSum):
        comment = " /* DBProxy.checkSandboxFile */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"dn={dn} size={fileSize} checksum={checkSum}")
        sqlC = "SELECT hostName,fileName FROM ATLAS_PANDAMETA.userCacheUsage "
        sqlC += "WHERE userName=:userName AND fileSize=:fileSize AND checkSum=:checkSum "
        sqlC += "AND hostName<>:ngHostName AND creationTime>CURRENT_DATE-3 "
        sqlC += "AND creationTime>CURRENT_DATE-3 "
        try:
            retStr = "NOTFOUND"
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[":userName"] = compactDN
            varMap[":fileSize"] = fileSize
            varMap[":checkSum"] = str(checkSum)
            varMap[":ngHostName"] = "localhost.localdomain"
            self.cur.arraysize = 10
            self.cur.execute(sqlC + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if len(res) != 0:
                hostName, fileName = res[0]
                retStr = f"FOUND:{hostName}:{fileName}"
            tmp_log.debug(f"{retStr}")
            return retStr
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return "ERROR: DB failure"

    # insert dataset
    def insertDataset(self, dataset, tablename="ATLAS_PANDA.Datasets"):
        comment = " /* DBProxy.insertDataset */"
        tmp_log = self.create_tagged_logger(comment, f"dataset={dataset.name}")
        tmp_log.debug("start")
        sql0 = f"SELECT COUNT(*) FROM {tablename} WHERE vuid=:vuid "
        sql1 = f"INSERT INTO {tablename} "
        sql1 += f"({DatasetSpec.columnNames()}) "
        sql1 += DatasetSpec.bindValuesExpression()
        sql2 = f"SELECT name FROM {tablename} WHERE vuid=:vuid "
        # time information
        dataset.creationdate = naive_utcnow()
        dataset.modificationdate = dataset.creationdate
        try:
            # subtype
            if dataset.subType in ["", "NULL", None]:
                # define using name
                if re.search("_dis\d+$", dataset.name) is not None:
                    dataset.subType = "dis"
                elif re.search("_sub\d+$", dataset.name) is not None:
                    dataset.subType = "sub"
                else:
                    dataset.subType = "top"
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[":vuid"] = dataset.vuid
            self.cur.execute(sql0 + comment, varMap)
            (nDS,) = self.cur.fetchone()
            tmp_log.debug(f"nDS={nDS} with {dataset.vuid}")
            if nDS == 0:
                # insert
                tmp_log.debug(sql1 + comment + str(dataset.valuesMap()))
                self.cur.execute(sql1 + comment, dataset.valuesMap())
                # check name in DB
                varMap = {}
                varMap[":vuid"] = dataset.vuid
                self.cur.execute(sql2 + comment, varMap)
                (nameInDB,) = self.cur.fetchone()
                tmp_log.debug(f"inDB -> {nameInDB} {dataset.name == nameInDB}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # get and lock dataset with a query
    def getLockDatasets(self, sqlQuery, varMapGet, modTimeOffset="", getVersion=False):
        comment = " /* DBProxy.getLockDatasets */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"{sqlQuery},{str(varMapGet)},{modTimeOffset}")
        sqlGet = (
            "SELECT /*+ INDEX_RS_ASC(tab(STATUS,TYPE,MODIFICATIONDATE)) */ vuid,name,modificationdate,version,transferStatus FROM ATLAS_PANDA.Datasets tab WHERE "
            + sqlQuery
        )
        sqlLock = "UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE"
        if modTimeOffset != "":
            sqlLock += f"+{modTimeOffset}"
        sqlLock += ",transferStatus=MOD(transferStatus+1,10)"
        if getVersion:
            sqlLock += ",version=:version"
        sqlLock += " WHERE vuid=:vuid AND transferStatus=:transferStatus"
        retList = []
        try:
            # begin transaction
            self.conn.begin()
            # get datasets
            self.cur.arraysize = 1000000
            self.cur.execute(sqlGet + comment, varMapGet)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all datasets
            if res is not None and len(res) != 0:
                for vuid, name, modificationdate, version, transferStatus in res:
                    # lock
                    varMapLock = {}
                    varMapLock[":vuid"] = vuid
                    varMapLock[":transferStatus"] = transferStatus
                    if getVersion:
                        try:
                            varMapLock[":version"] = str(int(version) + 1)
                        except Exception:
                            varMapLock[":version"] = str(1)
                    # begin transaction
                    self.conn.begin()
                    # update for lock
                    self.cur.execute(sqlLock + comment, varMapLock)
                    retU = self.cur.rowcount
                    # commit
                    if not self._commit():
                        raise RuntimeError("Commit error")
                    if retU > 0:
                        # append
                        if not getVersion:
                            retList.append((vuid, name, modificationdate))
                        else:
                            retList.append((vuid, name, modificationdate, version))
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # retrun
            return retList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # query dataset with map
    def queryDatasetWithMap(self, map):
        comment = " /* DBProxy.queryDatasetWithMap */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"{map}")
        if "name" in map:
            sql1 = """SELECT /*+ BEGIN_OUTLINE_DATA """
            sql1 += """INDEX_RS_ASC(@"SEL$1" "TAB"@"SEL$1" ("DATASETS"."NAME")) """
            sql1 += """OUTLINE_LEAF(@"SEL$1") ALL_ROWS """
            sql1 += """IGNORE_OPTIM_EMBEDDED_HINTS """
            sql1 += """END_OUTLINE_DATA */ """
            sql1 += f"{DatasetSpec.columnNames()} FROM ATLAS_PANDA.Datasets tab"
        else:
            sql1 = f"SELECT {DatasetSpec.columnNames()} FROM ATLAS_PANDA.Datasets"
        varMap = {}
        for key in map:
            if len(varMap) == 0:
                sql1 += f" WHERE {key}=:{key}"
            else:
                sql1 += f" AND {key}=:{key}"
            varMap[f":{key}"] = map[key]
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 100
            tmp_log.debug(sql1 + comment + str(varMap))
            self.cur.execute(sql1 + comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # instantiate Dataset
            if res is not None and len(res) != 0:
                dataset = DatasetSpec()
                dataset.pack(res[0])
                return dataset
            tmp_log.error(f"dataset not found")
            return None
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return None

    # update dataset
    def updateDataset(self, datasets, withLock, withCriteria, criteriaMap):
        comment = " /* DBProxy.updateDataset */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        sql1 = f"UPDATE ATLAS_PANDA.Datasets SET {DatasetSpec.bindUpdateExpression()} "
        sql1 += "WHERE vuid=:vuid"
        if withCriteria != "":
            sql1 += f" AND {withCriteria}"
        retList = []
        try:
            # start transaction
            self.conn.begin()
            for dataset in datasets:
                tmp_log.debug(f"dataset={dataset.name} status={dataset.status})")
                # time information
                dataset.modificationdate = naive_utcnow()
                # update
                varMap = dataset.valuesMap()
                varMap[":vuid"] = dataset.vuid
                for cKey in criteriaMap:
                    varMap[cKey] = criteriaMap[cKey]
                tmp_log.debug(sql1 + comment + str(varMap))
                self.cur.execute(sql1 + comment, varMap)
                retU = self.cur.rowcount
                if retU != 0 and retU != 1:
                    raise RuntimeError(f"Invalid rerun {retU}")
                retList.append(retU)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"ret:{retList}")
            return retList
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return []

    # trigger cleanup of internal datasets used by a task
    def trigger_cleanup_internal_datasets(self, task_id: int) -> bool:
        """
        Set deleting flag to dispatch datasets used by a task, which triggers deletion in datasetManager
        """
        comment = " /* DBProxy.trigger_cleanup_internal_datasets */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={task_id}")
        tmp_log.debug("start")
        sql1 = (
            f"UPDATE {panda_config.schemaPANDA}.Datasets SET status=:newStatus,modificationdate=CURRENT_DATE "
            "WHERE type=:type AND MoverID=:taskID AND status IN (:status_d,:status_c) "
        )
        try:
            # start transaction
            self.conn.begin()
            # update
            var_map = {
                ":type": "dispatch",
                ":newStatus": "deleting",
                ":taskID": task_id,
                ":status_d": "defined",
                ":status_c": "completed",
            }
            self.cur.execute(sql1 + comment, var_map)
            ret_u = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"set flag to {ret_u} dispatch datasets")
            return True
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    # get serial number for dataset, insert dummy datasets to increment SN
    def getSerialNumber(self, datasetname, definedFreshFlag=None):
        comment = " /* DBProxy.getSerialNumber */"
        tmp_log = self.create_tagged_logger(comment, f"datasetname={datasetname}")
        try:
            tmp_log.debug(f"fresh={definedFreshFlag}")
            if isinstance(datasetname, str):
                datasetname = datasetname.encode("ascii", "ignore")
                tmp_log.debug(f"converted unicode for {datasetname}")
            # start transaction
            self.conn.begin()
            # check freshness
            if definedFreshFlag is None:
                # select
                varMap = {}
                varMap[":name"] = datasetname
                varMap[":type"] = "output"
                sql = "SELECT /*+ INDEX_RS_ASC(TAB (DATASETS.NAME)) */ COUNT(*) FROM ATLAS_PANDA.Datasets tab WHERE type=:type AND name=:name"
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchone()
                # fresh dataset or not
                if res is not None and len(res) != 0 and res[0] > 0:
                    freshFlag = False
                else:
                    freshFlag = True
            else:
                # use predefined flag
                freshFlag = definedFreshFlag
            # get serial number
            if self.backend == "oracle":
                sql = "SELECT ATLAS_PANDA.SUBCOUNTER_SUBID_SEQ.nextval FROM dual"
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                (sn,) = self.cur.fetchone()
            elif self.backend == "postgres":
                sql = f"SELECT {panda_config.schemaPANDA}.SUBCOUNTER_SUBID_SEQ.nextval"
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                (sn,) = self.cur.fetchone()
            else:
                # panda_config.backend == 'mysql'
                # fake sequence
                sql = " INSERT INTO ATLAS_PANDA.SUBCOUNTER_SUBID_SEQ (col) VALUES (NULL) "
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                sql2 = """ SELECT LAST_INSERT_ID() """
                self.cur.execute(sql2 + comment, {})
                (sn,) = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"SN={sn} {freshFlag}")
            return (sn, freshFlag)
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return (-1, False)

    # count the number of files with map
    def countFilesWithMap(self, map):
        comment = " /* DBProxy.countFilesWithMap */"
        tmp_log = self.create_tagged_logger(comment)
        sql1 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ COUNT(*) FROM ATLAS_PANDA.filesTable4 tab"
        varMap = {}
        for key in map:
            if len(varMap) == 0:
                sql1 += f" WHERE {key}=:{key}"
            else:
                sql1 += f" AND {key}=:{key}"
            varMap[f":{key}"] = map[key]
        nTry = 3
        for iTry in range(nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                tmp_log.debug(f"{sql1} {str(map)}")
                self.cur.arraysize = 10
                retS = self.cur.execute(sql1 + comment, varMap)
                res = self.cur.fetchone()
                tmp_log.debug(f"{retS} {str(res)}")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                nFiles = 0
                if res is not None:
                    nFiles = res[0]
                return nFiles
            except Exception:
                # roll back
                self._rollback()
                if iTry + 1 < nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dump_error_message(tmp_log)
                return -1

    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self, dataset, status, fileLFN=""):
        comment = " /* DBProxy.updateInFilesReturnPandaIDs */"
        tmp_log = self.create_tagged_logger(comment, f"dataset={dataset}")
        tmp_log.debug(f"{fileLFN})")
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status=:status WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        varMap = {}
        varMap[":status"] = status
        varMap[":dispatchDBlock"] = dataset
        if fileLFN != "":
            sql0 += " AND lfn=:lfn"
            sql1 += " AND lfn=:lfn"
            varMap[":lfn"] = fileLFN
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                retS = self.cur.execute(sql0 + comment, varMap)
                resS = self.cur.fetchall()
                # update
                retU = self.cur.execute(sql1 + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # collect PandaIDs
                retList = []
                for tmpRowID, tmpPandaID in resS:
                    # append
                    if tmpPandaID not in retList:
                        retList.append(tmpPandaID)
                # return
                tmp_log.debug(f"ret={str(retList)}")
                return retList
            except Exception:
                # roll back
                self._rollback()
                # error report
                if iTry + 1 < self.nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dump_error_message(tmp_log)
        return []

    # update input files for jobs at certain sites and return corresponding PandaIDs
    def update_input_files_at_sites_and_get_panda_ids(self, filename: str, sites: list) -> list:
        """
        Update input files with a LFN for jobs at certain sites and return corresponding PandaIDs

        :param filename: LFN of the input file to be updated
        :param sites: List of site names where the jobs are running

        :return: List of PandaIDs of the jobs whose input files were updated
        """
        comment = " /* DBProxy.update_input_files_at_sites_and_get_panda_ids */"
        tmp_log = self.create_tagged_logger(comment, f"lfn={filename}")
        tmp_log.debug("start at sites: " + ",".join(sites))
        sql_to_get_ids = (
            "SELECT tabJ.PandaID FROM ATLAS_PANDA.jobsDefined4 tabJ, ATLAS_PANDA.filesTable4 tabF WHERE tabF.PandaID=tabJ.PandaID AND tabF.lfn=:lfn "
            "AND tabF.type='input' AND tabF.status IN ('defined', 'unknown', 'pending') AND tabJ.computingSite IN ("
        )
        var_map = {":lfn": filename}
        for idx, site in enumerate(sites):
            tmp_key = f":site_{idx}"
            var_map[tmp_key] = site
            sql_to_get_ids += f"{tmp_key},"
        sql_to_get_ids = sql_to_get_ids[:-1] + ") "
        return_list = []
        try:
            # start transaction
            self.conn.begin()
            # get IDs
            self.cur.arraysize = 10000
            self.cur.execute(sql_to_get_ids + comment, var_map)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # sql to update files
            sql_to_update_files = (
                "UPDATE ATLAS_PANDA.filesTable4 tab SET status='ready' WHERE PandaID=:PandaID AND lfn=:lfn AND type='input' AND status<>'ready' "
            )
            for (tmp_pandaID,) in res:
                var_map = {":PandaID": tmp_pandaID, ":lfn": filename}
                # start transaction
                self.conn.begin()
                self.cur.execute(sql_to_update_files + comment, var_map)
                if self.cur.rowcount > 0:
                    tmp_log.debug(f"updated input file status to 'ready' for PandaID={tmp_pandaID}")
                    return_list.append(tmp_pandaID)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"done")
            return return_list
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
        return []

    # update output files and return corresponding PandaIDs
    def updateOutFilesReturnPandaIDs(self, dataset, fileLFN=""):
        comment = " /* DBProxy.updateOutFilesReturnPandaIDs */"
        tmp_log = self.create_tagged_logger(comment, f"dataset={dataset}")
        tmp_log.debug(f"{fileLFN}")
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND status=:status"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status='ready' WHERE destinationDBlock=:destinationDBlock AND status=:status"
        varMap = {}
        varMap[":status"] = "transferring"
        varMap[":destinationDBlock"] = dataset
        if fileLFN != "":
            sql0 += " AND lfn=:lfn"
            sql1 += " AND lfn=:lfn"
            varMap[":lfn"] = fileLFN
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                retS = self.cur.execute(sql0 + comment, varMap)
                resS = self.cur.fetchall()
                # update
                retList = []
                retU = self.cur.execute(sql1 + comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # collect PandaIDs
                retList = []
                for tmpRowID, tmpPandaID in resS:
                    # append
                    if tmpPandaID not in retList:
                        retList.append(tmpPandaID)
                # return
                tmp_log.debug(f"ret={str(retList)}")
                return retList
            except Exception:
                # roll back
                self._rollback()
                # error report
                if iTry + 1 < self.nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dump_error_message(tmp_log)
        return []

    # get _dis datasets associated to _sub
    def getAssociatedDisDatasets(self, subDsName):
        comment = " /* DBProxy.getAssociatedDisDatasets */"
        tmp_log = self.create_tagged_logger(comment, f"subDsName={subDsName}")
        tmp_log.debug(f"start")
        sqlF = (
            "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ distinct PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock"
        )
        sqlJ = "SELECT distinct dispatchDBlock FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type"
        try:
            # start transaction
            self.conn.begin()
            # get PandaIDs
            varMap = {}
            varMap[":destinationDBlock"] = subDsName
            self.cur.arraysize = 10000
            self.cur.execute(sqlF + comment, varMap)
            resS = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # loop over all PandaIDs
            retList = []
            for (pandaID,) in resS:
                # start transaction
                self.conn.begin()
                # get _dis name
                varMap = {}
                varMap[":type"] = "input"
                varMap[":PandaID"] = pandaID
                self.cur.arraysize = 1000
                self.cur.execute(sqlJ + comment, varMap)
                resD = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # append
                for (disName,) in resD:
                    if disName is not None and disName not in retList:
                        retList.append(disName)
            # return
            tmp_log.debug(f"ret={str(retList)}")
            return retList
        except Exception:
            # roll back
            self._rollback()
            self.dump_error_message(tmp_log)
            return []

    # set GUIDs
    def setGUIDs(self, files):
        comment = " /* DBProxy.setGUIDs */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug(f"{files}")
        sql0 = "UPDATE ATLAS_PANDA.filesTable4 SET GUID=:GUID,fsize=:fsize,checksum=:checksum,scope=:scope WHERE lfn=:lfn"
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 1000000
                # update
                for file in files:
                    varMap = {}
                    varMap[":GUID"] = file["guid"]
                    varMap[":lfn"] = file["lfn"]
                    if file["checksum"] in ["", "NULL"]:
                        varMap[":checksum"] = None
                    else:
                        varMap[":checksum"] = file["checksum"]
                    varMap[":fsize"] = file["fsize"]
                    if "scope" not in file or file["scope"] in ["", "NULL"]:
                        varMap[":scope"] = None
                    else:
                        varMap[":scope"] = file["scope"]
                    self.cur.execute(sql0 + comment, varMap)
                    retU = self.cur.rowcount
                    tmp_log.debug(f"retU {retU}")
                    if retU < 0:
                        raise RuntimeError("SQL error")
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                return True
            except Exception:
                # roll back
                self._rollback()
                # error report
                if iTry + 1 < self.nTry:
                    tmp_log.debug(f"retry : {iTry}")
                    time.sleep(random.randint(10, 20))
                    continue
                self.dump_error_message(tmp_log)
        return False

    # get special dispatcher parameters
    def get_special_dispatch_params(self):
        """
        Get the following special parameters for dispatcher.Z
          Authorized name lists for proxy, key-pair, and token-key retrieval
          Key pairs
          Token keys
        """
        comment = " /* DBProxy.get_special_dispatch_params */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            return_map = {}
            # set autocommit on
            self.conn.begin()
            self.cur.arraysize = 100000
            # get token keys
            token_keys = {}
            sql = f"SELECT dn, credname FROM {panda_config.schemaMETA}.proxykey WHERE expires>:limit ORDER BY expires DESC "
            var_map = {":limit": naive_utcnow()}
            self.cur.execute(sql + comment, var_map)
            res_list = self.cur.fetchall()
            for client_name, token_key in res_list:
                token_keys.setdefault(client_name, {"fullList": [], "latest": token_key})
                token_keys[client_name]["fullList"].append(token_key)
            return_map["tokenKeys"] = token_keys
            tmp_list = [f"""{k}:{len(token_keys[k]["fullList"])}""" for k in token_keys]
            tmp_log.debug(f"""got token keys {",".join(tmp_list)}""")
            # select to get the list of authorized users
            allow_key = []
            allow_proxy = []
            allow_token = []
            sql = "SELECT DISTINCT name, gridpref FROM ATLAS_PANDAMETA.users " "WHERE (status IS NULL OR status<>:ngStatus) AND gridpref IS NOT NULL "
            var_map = {":ngStatus": "disabled"}
            self.cur.execute(sql + comment, var_map)
            res_list = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            for compactDN, gridpref in res_list:
                # users authorized for proxy retrieval
                if PrioUtil.PERMISSION_PROXY in gridpref:
                    if compactDN not in allow_proxy:
                        allow_proxy.append(compactDN)
                # users authorized for key-pair retrieval
                if PrioUtil.PERMISSION_KEY in gridpref:
                    if compactDN not in allow_key:
                        allow_key.append(compactDN)
                # users authorized for token-key retrieval
                if PrioUtil.PERMISSION_TOKEN_KEY in gridpref:
                    if compactDN not in allow_token:
                        allow_token.append(compactDN)
            return_map["allowKeyPair"] = allow_key
            return_map["allowProxy"] = allow_proxy
            return_map["allowTokenKey"] = allow_token
            tmp_log.debug(
                f"got authed users key-pair:{len(return_map['allowKeyPair'])}, proxy:{len(return_map['allowProxy'])}, token-key:{len(return_map['allowTokenKey'])}"
            )
            # read key pairs
            keyPair = {}
            try:
                keyFileNames = glob.glob(panda_config.keyDir + "/*")
                for keyName in keyFileNames:
                    tmpF = open(keyName)
                    keyPair[os.path.basename(keyName)] = tmpF.read()
                    tmpF.close()
            except Exception as e:
                tmp_log.error(f"failed read key-pairs with {str(e)}")
            return_map["keyPair"] = keyPair
            tmp_log.debug(f"got {len(return_map['keyPair'])} key-pair files")
            tmp_log.debug("done")
            return return_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get original consumers
    def getOriginalConsumers(self, jediTaskID, jobsetID, pandaID):
        comment = " /* DBProxy.getOriginalConsumers */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} jobsetID={jobsetID} PandaID={pandaID}")
        tmp_log.debug("start")
        try:
            # sql to get sites where consumers are active
            sqlA = "SELECT computingSite FROM ATLAS_PANDA.jobsActive4 WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            sqlA += "UNION "
            sqlA += "SELECT computingSite FROM ATLAS_PANDA.jobsDefined4 WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            # sql to get original IDs
            sqlG = f"SELECT oldPandaID FROM {panda_config.schemaJEDI}.JEDI_Job_Retry_History "
            sqlG += "WHERE jediTaskID=:jediTaskID AND newPandaID=:jobsetID AND relationType=:relationType "
            # sql to check computingSite
            sqlC1 = "SELECT computingSite FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID "
            sqlC2 = "SELECT computingSite FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30) "
            # sql to get job info
            sqlJ = f"SELECT {JobSpec.columnNames()} "
            sqlJ += "FROM {0} "
            sqlJ += "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30) "
            sqlF = f"SELECT {FileSpec.columnNames()} FROM ATLAS_PANDA.filesTable4 "
            sqlF += "WHERE PandaID=:PandaID "
            sqlP = "SELECT jobParameters FROM {0} WHERE PandaID=:PandaID "
            # get sites
            aSites = set()
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jobsetID"] = jobsetID
            self.cur.execute(sqlA + comment, varMap)
            resA = self.cur.fetchall()
            for (computingSite,) in resA:
                aSites.add(computingSite)
            # get original IDs
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap[":jobsetID"] = jobsetID
            varMap[":relationType"] = EventServiceUtils.relationTypeJS_Map
            self.cur.execute(sqlG + comment, varMap)
            resG = self.cur.fetchall()
            jobList = []
            for (pandaID,) in resG:
                # check computingSite
                varMap = dict()
                varMap[":PandaID"] = pandaID
                self.cur.execute(sqlC1 + comment, varMap)
                resC = self.cur.fetchone()
                if resC is None:
                    # try archived
                    self.cur.execute(sqlC2 + comment, varMap)
                    resC = self.cur.fetchone()
                    inArchived = True
                else:
                    inArchived = False
                # skip since it is not yet archived and thus is still active
                if resC is None:
                    continue
                (computingSite,) = resC
                # skip since there is an active consumer at the site
                if computingSite in aSites:
                    continue
                # get job
                if inArchived:
                    self.cur.execute(sqlJ.format("ATLAS_PANDAARCH.jobsArchived") + comment, varMap)
                else:
                    self.cur.execute(sqlJ.format("ATLAS_PANDA.jobsArchived4") + comment, varMap)
                resJ = self.cur.fetchone()
                if resJ is not None:
                    jobSpec = JobSpec()
                    jobSpec.pack(resJ)
                    # get files
                    self.cur.execute(sqlF + comment, varMap)
                    resFs = self.cur.fetchall()
                    if len(resFs) == 0:
                        continue
                    for resF in resFs:
                        fileSpec = FileSpec()
                        fileSpec.pack(resF)
                        jobSpec.addFile(fileSpec)
                    # get job params
                    if inArchived:
                        self.cur.execute(
                            sqlP.format("ATLAS_PANDAARCH.jobParamsTable_ARCH") + comment,
                            varMap,
                        )
                    else:
                        self.cur.execute(sqlP.format("ATLAS_PANDA.jobParamsTable") + comment, varMap)
                    for (clobJobP,) in self.cur:
                        if clobJobP is not None:
                            try:
                                jobSpec.jobParameters = clobJobP.read()
                            except AttributeError:
                                jobSpec.jobParameters = str(clobJobP)
                        break
                    # add
                    jobList.append(jobSpec)
                    aSites.add(computingSite)
            tmp_log.debug(f"got {len(jobList)} consumers")
            return jobList
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return []

    # update unmerged datasets to trigger merging
    def updateUnmergedDatasets(self, job, finalStatusDS, updateCompleted=False):
        comment = " /* JediDBProxy.updateUnmergedDatasets */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job.PandaID}")
        # get PandaID which produced unmerged files
        umPandaIDs = []
        umCheckedIDs = []
        # sql to get file counts
        sqlGFC = "SELECT status,PandaID,outPandaID FROM ATLAS_PANDA.JEDI_Dataset_Contents "
        sqlGFC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND PandaID IS NOT NULL "
        # sql to update nFiles in JEDI datasets
        sqlUNF = "UPDATE ATLAS_PANDA.JEDI_Datasets "
        sqlUNF += "SET nFilesOnHold=0,nFiles=:nFiles,"
        sqlUNF += "nFilesUsed=:nFilesUsed,nFilesTobeUsed=:nFilesTobeUsed "
        sqlUNF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to check nFiles
        sqlUCF = "SELECT nFilesTobeUsed,nFilesUsed FROM ATLAS_PANDA.JEDI_Datasets "
        sqlUCF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to update dataset status
        sqlUDS = "UPDATE ATLAS_PANDA.JEDI_Datasets "
        sqlUDS += "SET status=:status,modificationTime=CURRENT_DATE "
        sqlUDS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to update dataset status in panda
        sqlUDP = "UPDATE ATLAS_PANDA.Datasets "
        sqlUDP += "SET status=:status "
        sqlUDP += "WHERE vuid=:vuid AND NOT status IN (:statusR,:statusD) "
        try:
            tmp_log.debug(f"start")
            # begin transaction
            self.conn.begin()
            # update dataset in panda
            toSkip = False
            for datasetSpec in finalStatusDS:
                varMap = {}
                varMap[":vuid"] = datasetSpec.vuid
                varMap[":status"] = "tobeclosed"
                varMap[":statusR"] = "tobeclosed"
                if not updateCompleted:
                    varMap[":statusD"] = "completed"
                else:
                    varMap[":statusD"] = "dummy"
                tmp_log.debug(sqlUDP + comment + str(varMap))
                self.cur.execute(sqlUDP + comment, varMap)
                nRow = self.cur.rowcount
                if nRow != 1:
                    toSkip = True
                    tmp_log.debug(f"failed to lock {datasetSpec.name}")
            # look for unmerged files
            if not toSkip:
                updatedDS = []
                for tmpFile in job.Files:
                    if tmpFile.isUnMergedOutput():
                        if tmpFile.datasetID in updatedDS:
                            continue
                        updatedDS.append(tmpFile.datasetID)
                        # get file counts
                        varMap = {}
                        varMap[":jediTaskID"] = tmpFile.jediTaskID
                        varMap[":datasetID"] = tmpFile.datasetID
                        self.cur.arraysize = 100000
                        tmp_log.debug(sqlGFC + comment + str(varMap))
                        self.cur.execute(sqlGFC + comment, varMap)
                        resListGFC = self.cur.fetchall()
                        varMap = {}
                        tmpNumFiles = 0
                        tmpNumReady = 0
                        for tmpFileStatus, tmpPandaID, tmpOutPandaID in resListGFC:
                            if tmpFileStatus in [
                                "finished",
                                "failed",
                                "cancelled",
                                "notmerged",
                                "ready",
                                "lost",
                                "broken",
                                "picked",
                                "nooutput",
                            ]:
                                pass
                            elif tmpFileStatus == "running" and tmpPandaID != tmpOutPandaID:
                                pass
                            else:
                                continue
                            tmpNumFiles += 1
                            if tmpFileStatus in ["ready"]:
                                tmpNumReady += 1
                        # update nFiles
                        varMap = {}
                        varMap[":jediTaskID"] = tmpFile.jediTaskID
                        varMap[":datasetID"] = tmpFile.datasetID
                        varMap[":nFiles"] = tmpNumFiles
                        varMap[":nFilesTobeUsed"] = tmpNumFiles
                        varMap[":nFilesUsed"] = tmpNumFiles - tmpNumReady
                        self.cur.arraysize = 10
                        tmp_log.debug(sqlUNF + comment + str(varMap))
                        self.cur.execute(sqlUNF + comment, varMap)
                        nRow = self.cur.rowcount
                        if nRow == 1:
                            # check nFilesTobeUsed
                            varMap = {}
                            varMap[":jediTaskID"] = tmpFile.jediTaskID
                            varMap[":datasetID"] = tmpFile.datasetID
                            self.cur.execute(sqlUCF + comment, varMap)
                            resUCF = self.cur.fetchone()
                            if resUCF is not None:
                                nFilesTobeUsed, nFilesUsed = resUCF
                                varMap = {}
                                varMap[":jediTaskID"] = tmpFile.jediTaskID
                                varMap[":datasetID"] = tmpFile.datasetID
                                if nFilesTobeUsed - nFilesUsed > 0:
                                    varMap[":status"] = "ready"
                                else:
                                    varMap[":status"] = "done"
                                # update dataset status
                                tmp_log.debug(sqlUDS + comment + str(varMap))
                                self.cur.execute(sqlUDS + comment, varMap)
                        else:
                            tmp_log.debug(f"skip jediTaskID={tmpFile.jediTaskID} datasetID={tmpFile.datasetID}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # get throttled users
    def getThrottledUsers(self):
        comment = " /* DBProxy.getThrottledUsers */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        retVal = set()
        try:
            # sql to get users
            sqlT = "SELECT distinct prodUserName,workingGroup FROM ATLAS_PANDA.jobsActive4 "
            sqlT += "WHERE prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus AND relocationFlag=:relocationFlag "
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[":prodSourceLabel"] = "user"
            varMap[":relocationFlag"] = 3
            varMap[":jobStatus"] = "throttled"
            # get datasets
            self.cur.execute(sqlT + comment, varMap)
            resPs = self.cur.fetchall()
            for prodUserName, workingGroup in resPs:
                retVal.add((prodUserName, workingGroup))
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # reset files in JEDI
    def resetFileStatusInJEDI(
        self, dn: str, is_prod_manager: bool, dataset_name: str, lost_files: List[str], recover_parent: bool, simul: bool
    ) -> Tuple[bool, int | None, Dict[str, List[str]] | None, str | None]:
        """
        Reset file status in JEDI for lost files
        1) check ownership
        2) set file status from 'finished' to 'lost' in JEDI_Dataset_Contents
        3) cancel events in JEDI_Events
        4) if recoverParent is True, set parent files from 'finished' to 'ready'
        5) if simul is True, do not commit changes
        6) return list of input files that produced the lost files

        :param dn: DN of the requester
        :param is_prod_manager: whether the requester is a production manager
        :param dataset_name: name of the dataset
        :param lost_files: list of lost LFNs
        :param recover_parent: whether to recover parent files
        :param simul: whether to simulate without committing changes
        :return: tuple of
            success flag,
            jediTaskID (None if failed),
            dictionary of list of input files that produced the lost files (None if failed),
            error message (None if succeeded)
        """
        comment = " /* DBProxy.resetFileStatusInJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"datasetName={dataset_name}")
        tmp_log.debug("start")
        error_message = None
        try:
            # list of lost input files
            lostInputFiles = {}
            # get compact DN
            compactDN = CoreUtils.clean_user_id(dn)
            if compactDN in ["", "NULL", None]:
                compactDN = dn
            tmp_log.debug(f"userName={compactDN}")
            toSkip = False
            # begin transaction
            self.conn.begin()
            # get jediTaskID
            varMap = {}
            varMap[":type1"] = "log"
            varMap[":type2"] = "output"
            varMap[":name1"] = dataset_name
            varMap[":name2"] = dataset_name.split(":")[-1]
            sqlGI = f"SELECT jediTaskID,datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlGI += "WHERE type IN (:type1,:type2) AND datasetName IN (:name1,:name2) "
            self.cur.execute(sqlGI + comment, varMap)
            resGI = self.cur.fetchall()
            # use the largest datasetID since broken tasks might have been retried
            jediTaskID = None
            datasetID = None
            for tmpJediTaskID, tmpDatasetID in resGI:
                if jediTaskID is None or jediTaskID < tmpJediTaskID:
                    jediTaskID = tmpJediTaskID
                    datasetID = tmpDatasetID
                elif datasetID < tmpDatasetID:
                    datasetID = tmpDatasetID
            if jediTaskID is None:
                error_message = "jediTaskID not found"
                tmp_log.debug(error_message)
                toSkip = True
            if not toSkip:
                # get task status and owner
                tmp_log.debug(f"jediTaskID={jediTaskID} datasetID={datasetID}")
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                sqlOW = f"SELECT status,userName,useJumbo FROM {panda_config.schemaJEDI}.JEDI_Tasks "
                sqlOW += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sqlOW + comment, varMap)
                resOW = self.cur.fetchone()
                taskStatus, ownerName, useJumbo = resOW
                # check ownership
                if not is_prod_manager and ownerName != compactDN:
                    error_message = f"Not the owner: {ownerName} != {compactDN}"
                    tmp_log.debug(error_message)
                    toSkip = True
            if not toSkip:
                # get affected PandaIDs
                sqlLP = f"SELECT pandaID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlLP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND lfn=:lfn "
                # get files to update status
                sql_get_files = (
                    f"SELECT datasetID,fileID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    "WHERE jediTaskID=:jediTaskID AND type=:type AND status=:oldStatus AND PandaID=:PandaID "
                )
                # sql to update file status
                sqlUFO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlUFO += "SET status=:newStatus "
                sqlUFO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oldStatus AND PandaID=:PandaID "
                # sql to cancel events
                sqlCE = "UPDATE /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
                sqlCE += f"{panda_config.schemaJEDI}.JEDI_Events tab "
                sqlCE += "SET status=:status "
                sqlCE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlCE += "AND status IN (:esFinished,:esDone,:esMerged) "
                # get affected PandaIDs
                lostPandaIDs = set([])
                nDiff = {}
                for lostFile in lost_files:
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":lfn"] = lostFile
                    self.cur.execute(sqlLP + comment, varMap)
                    resLP = self.cur.fetchone()
                    if resLP is not None:
                        (pandaID,) = resLP
                        lostPandaIDs.add(pandaID)
                        # get the file and co-produced files to lost
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":PandaID"] = pandaID
                        varMap[":type"] = "output"
                        varMap[":oldStatus"] = "finished"
                        self.cur.execute(sql_get_files + comment, varMap)
                        res_files = self.cur.fetchall()
                        if res_files:
                            for tmpDatasetID, tmpFileID in res_files:
                                # update file status
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = tmpDatasetID
                                varMap[":fileID"] = tmpFileID
                                varMap[":PandaID"] = pandaID
                                varMap[":newStatus"] = "lost"
                                varMap[":oldStatus"] = "finished"
                                tmp_log.debug(sqlUFO + comment + str(varMap))
                                if not simul:
                                    self.cur.execute(sqlUFO + comment, varMap)
                                    nRow = self.cur.rowcount
                                    if nRow > 0:
                                        nDiff[tmpDatasetID] = nDiff.get(tmpDatasetID, 0) + 1
                                else:
                                    nDiff[tmpDatasetID] = nDiff.get(tmpDatasetID, 0) + 1
                tmp_log.debug(f"PandaIDs produced lost files = {str(lostPandaIDs)}")
                tmp_log.debug("update output datasets")
                # get nEvents
                sqlGNE = "SELECT SUM(c.nEvents) "
                sqlGNE += "FROM {0}.JEDI_Datasets d,{0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
                sqlGNE += "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID "
                sqlGNE += "AND d.jediTaskID=:jediTaskID AND d.datasetID=:datasetID AND c.status=:status "
                # update output dataset statistics
                sqlUDO = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlUDO += "SET nFilesFinished=nFilesFinished-:nDiff,state=NULL,nEvents=:nEvents "
                sqlUDO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                for tmpDatasetID, n_diff_value in nDiff.items():
                    # get nEvents
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = tmpDatasetID
                    varMap[":status"] = "finished"
                    self.cur.execute(sqlGNE + comment, varMap)
                    (tmp_counts,) = self.cur.fetchone()
                    # update nFilesFinished
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = tmpDatasetID
                    varMap[":nDiff"] = n_diff_value
                    varMap[":nEvents"] = tmp_counts
                    tmp_log.debug(sqlUDO + comment + str(varMap))
                    if not simul:
                        self.cur.execute(sqlUDO + comment, varMap)
                # get input datasets
                tmp_log.debug("update input datasets")
                sqlID = f"SELECT datasetID,datasetName,masterID FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlID += "WHERE jediTaskID=:jediTaskID AND type=:type "
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":type"] = "input"
                self.cur.execute(sqlID + comment, varMap)
                resID = self.cur.fetchall()
                inputDatasets = {}
                masterID = None
                for tmpDatasetID, tmpDatasetName, tmpMasterID in resID:
                    inputDatasets[tmpDatasetID] = tmpDatasetName
                    if tmpMasterID is None:
                        masterID = tmpDatasetID
                # sql to get affected inputs
                if useJumbo is None:
                    sqlAI = f"SELECT fileID,datasetID,lfn,outPandaID FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlAI += "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2,:type3) AND PandaID=:PandaID "
                else:
                    sqlAI = f"SELECT fileID,datasetID,lfn,NULL FROM {panda_config.schemaPANDA}.filesTable4 "
                    sqlAI += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
                    sqlAI += "UNION "
                    sqlAI = f"SELECT fileID,datasetID,lfn,NULL FROM {panda_config.schemaPANDA}.filesTable4 "
                    sqlAI += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) AND modificationTime>CURRENT_TIMESTAMP-365 "
                # sql to update input file status
                sqlUFI = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlUFI += "SET status=:newStatus,attemptNr=attemptNr+1 "
                sqlUFI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oldStatus "
                # get affected inputs
                datasetCountMap = {}
                for lostPandaID in lostPandaIDs:
                    varMap = {}
                    if useJumbo is None:
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":PandaID"] = lostPandaID
                        varMap[":type1"] = "input"
                        varMap[":type2"] = "pseudo_input"
                        varMap[":type3"] = "output"
                    else:
                        varMap[":PandaID"] = lostPandaID
                        varMap[":type1"] = "input"
                        varMap[":type2"] = "pseudo_input"
                    self.cur.execute(sqlAI + comment, varMap)
                    resAI = self.cur.fetchall()
                    newResAI = []
                    for tmpItem in resAI:
                        tmpFileID, tmpDatasetID, tmpLFN, tmpOutPandaID = tmpItem
                        # skip output file
                        if lostPandaID == tmpOutPandaID:
                            continue
                        # input for merged files
                        if tmpOutPandaID is not None:
                            varMap = {}
                            varMap[":jediTaskID"] = jediTaskID
                            varMap[":PandaID"] = tmpOutPandaID
                            varMap[":type1"] = "input"
                            varMap[":type2"] = "pseudo_input"
                            varMap[":type3"] = "dummy"
                            self.cur.execute(sqlAI + comment, varMap)
                            resAI2 = self.cur.fetchall()
                            for tmpItem in resAI2:
                                newResAI.append(tmpItem)
                        else:
                            newResAI.append(tmpItem)
                    for tmpFileID, tmpDatasetID, tmpLFN, tmpOutPandaID in newResAI:
                        # collect if dataset was already deleted
                        is_lost = False
                        if recover_parent and tmpDatasetID == masterID:
                            lostInputFiles.setdefault(inputDatasets[tmpDatasetID], [])
                            lostInputFiles[inputDatasets[tmpDatasetID]].append(tmpLFN)
                            is_lost = True
                        # reset file status
                        if tmpDatasetID not in datasetCountMap:
                            datasetCountMap[tmpDatasetID] = 0
                        varMap = {}
                        varMap[":jediTaskID"] = jediTaskID
                        varMap[":datasetID"] = tmpDatasetID
                        varMap[":fileID"] = tmpFileID
                        if is_lost:
                            varMap[":newStatus"] = "lost"
                        else:
                            varMap[":newStatus"] = "ready"
                        varMap[":oldStatus"] = "finished"
                        if not simul:
                            self.cur.execute(sqlUFI + comment, varMap)
                            nRow = self.cur.rowcount
                        else:
                            tmp_log.debug(sqlUFI + comment + str(varMap))
                            nRow = 1
                        if nRow > 0:
                            datasetCountMap[tmpDatasetID] += 1
                            if useJumbo is not None:
                                # cancel events
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":datasetID"] = tmpDatasetID
                                varMap[":fileID"] = tmpFileID
                                varMap[":status"] = EventServiceUtils.ST_cancelled
                                varMap[":esFinished"] = EventServiceUtils.ST_finished
                                varMap[":esDone"] = EventServiceUtils.ST_done
                                varMap[":esMerged"] = EventServiceUtils.ST_merged
                                if not simul:
                                    self.cur.execute(sqlCE + comment, varMap)
                                else:
                                    tmp_log.debug(sqlCE + comment + str(varMap))
                # update dataset statistics
                sqlUDI = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
                sqlUDI += "SET nFilesUsed=nFilesUsed-:nDiff,nFilesFinished=nFilesFinished-:nDiff,"
                sqlUDI += "nEventsUsed=(SELECT SUM(CASE WHEN startEvent IS NULL THEN nEvents ELSE endEvent-startEvent+1 END) "
                sqlUDI += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                sqlUDI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status) "
                sqlUDI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                for tmpDatasetID in datasetCountMap:
                    nDiff = datasetCountMap[tmpDatasetID]
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = tmpDatasetID
                    varMap[":nDiff"] = nDiff
                    varMap[":status"] = "finished"
                    tmp_log.debug(sqlUDI + comment + str(varMap))
                    if not simul:
                        self.cur.execute(sqlUDI + comment, varMap)
                # update task status
                if taskStatus == "done":
                    sqlUT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET status=:newStatus WHERE jediTaskID=:jediTaskID "
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":newStatus"] = "finished"
                    if not simul:
                        self.cur.execute(sqlUT + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True, jediTaskID, lostInputFiles, error_message
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, None, None, "database error"

    # copy file records
    def copy_file_records(self, new_lfns, file_spec):
        comment = " /* DBProxy.copy_file_records */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={file_spec.PandaID} oldLFN={file_spec.lfn}")
        tmp_log.debug(f"start with {len(new_lfns)} files")
        try:
            # begin transaction
            self.conn.begin()
            for idx_lfn, new_lfn in enumerate(new_lfns):
                # reset rowID
                tmpFileSpec = copy.copy(file_spec)
                tmpFileSpec.lfn = new_lfn
                if idx_lfn > 0:
                    tmpFileSpec.row_ID = None
                # insert file in JEDI
                if idx_lfn > 0 and tmpFileSpec.jediTaskID not in [None, "NULL"] and tmpFileSpec.fileID not in ["", "NULL", None]:
                    # get fileID
                    sqlFileID = "SELECT ATLAS_PANDA.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM dual "
                    self.cur.execute(sqlFileID + comment)
                    (newFileID,) = self.cur.fetchone()
                    # read file in JEDI
                    varMap = {}
                    varMap[":jediTaskID"] = tmpFileSpec.jediTaskID
                    varMap[":datasetID"] = tmpFileSpec.datasetID
                    varMap[":fileID"] = tmpFileSpec.fileID
                    sqlGI = f"SELECT * FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlGI += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    self.cur.execute(sqlGI + comment, varMap)
                    resGI = self.cur.fetchone()
                    tmpFileSpec.fileID = newFileID
                    if resGI is not None:
                        # make sql and map
                        sqlJI = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                        sqlJI += "VALUES ("
                        varMap = {}
                        for columDesc, columVal in zip(self.cur.description, resGI):
                            columName = columDesc[0]
                            # overwrite fileID
                            if columName.upper() == "FILEID":
                                columVal = tmpFileSpec.fileID
                            keyName = f":{columName}"
                            varMap[keyName] = columVal
                            sqlJI += f"{keyName},"
                        sqlJI = sqlJI[:-1]
                        sqlJI += ") "
                        # insert file in JEDI
                        self.cur.execute(sqlJI + comment, varMap)
                if idx_lfn > 0:
                    # insert file in Panda
                    sqlFile = f"INSERT INTO ATLAS_PANDA.filesTable4 ({FileSpec.columnNames()}) "
                    sqlFile += FileSpec.bindValuesExpression(useSeq=True)
                    varMap = tmpFileSpec.valuesMap(useSeq=True)
                    self.cur.execute(sqlFile + comment, varMap)
                else:
                    # update LFN
                    sqlFSF = "UPDATE ATLAS_PANDA.filesTable4 SET lfn=:lfn "
                    sqlFSF += "WHERE row_ID=:row_ID "
                    varMap = {}
                    varMap[":lfn"] = tmpFileSpec.lfn
                    varMap[":row_ID"] = tmpFileSpec.row_ID
                    self.cur.execute(sqlFSF + comment, varMap)
                # update LFN in JEDI
                if tmpFileSpec.fileID not in ["", "NULL", None]:
                    sqlJF = f"UPDATE {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
                    sqlJF += "SET lfn=:lfn "
                    sqlJF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    varMap = {}
                    varMap[":lfn"] = tmpFileSpec.lfn
                    varMap[":jediTaskID"] = tmpFileSpec.jediTaskID
                    varMap[":datasetID"] = tmpFileSpec.datasetID
                    varMap[":fileID"] = tmpFileSpec.fileID
                    self.cur.execute(sqlJF + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False

    # get error definitions from DB (values cached for 1 hour)
    @memoize
    def getRetrialRules(self):
        # Logging
        comment = " /* DBProxy.getRetrialRules */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")

        # SQL to extract the error definitions
        sql = """
        SELECT re.retryerror_id, re.errorsource, re.errorcode, re.errorDiag, re.parameters, re.architecture, re.release, re.workqueue_id, ra.retry_action, re.active, ra.active
        FROM ATLAS_PANDA.RETRYERRORS re, ATLAS_PANDA.RETRYACTIONS ra
        WHERE re.retryaction=ra.retryaction_id
        AND (CURRENT_TIMESTAMP < re.expiration_date or re.expiration_date IS NULL)
        """
        self.cur.execute(sql + comment, {})
        definitions = self.cur.fetchall()  # example of output: [('pilotErrorCode', 1, None, None, None, None, 'no_retry', 'Y', 'Y'),...]

        # commit
        if not self._commit():
            raise RuntimeError("Commit error")

        # tmp_log.debug("definitions %s"%(definitions))

        retrial_rules = {}
        for definition in definitions:
            (
                retryerror_id,
                error_source,
                error_code,
                error_diag,
                parameters,
                architecture,
                release,
                wqid,
                action,
                e_active,
                a_active,
            ) = definition

            # Convert the parameter string into a dictionary
            try:
                # 1. Convert a string like "key1=value1&key2=value2" into [[key1, value1],[key2,value2]]
                params_list = map(
                    lambda key_value_pair: key_value_pair.split("="),
                    parameters.split("&"),
                )
                # 2. Convert a list [[key1, value1],[key2,value2]] into {key1: value1, key2: value2}
                params_dict = dict((key, value) for (key, value) in params_list)
            except AttributeError:
                params_dict = {}
            except ValueError:
                params_dict = {}

            # Calculate if action and error combination should be active
            if e_active == "Y" and a_active == "Y":
                active = True  # Apply the action for this error
            else:
                active = False  # Do not apply the action for this error, only log

            retrial_rules.setdefault(error_source, {})
            retrial_rules[error_source].setdefault(error_code, [])
            retrial_rules[error_source][error_code].append(
                {
                    "error_id": retryerror_id,
                    "error_diag": error_diag,
                    "action": action,
                    "params": params_dict,
                    "architecture": architecture,
                    "release": release,
                    "wqid": wqid,
                    "active": active,
                }
            )
        # tmp_log.debug("Loaded retrial rules from DB: %s" %retrial_rules)
        return retrial_rules

    def setMaxAttempt(self, jobID, taskID, files, maxAttempt):
        # Logging
        comment = " /* DBProxy.setMaxAttempt */"
        tmp_log = self.create_tagged_logger(comment, f"jobID={jobID} taskID={taskID}")
        tmp_log.debug("start")

        # Update the file entries to avoid JEDI generating new jobs
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")
        input_files = list(
            filter(
                lambda pandafile: pandafile.type in input_types and re.search("DBRelease", pandafile.lfn) is None,
                files,
            )
        )
        input_fileIDs = [input_file.fileID for input_file in input_files]
        input_datasetIDs = [input_file.datasetID for input_file in input_files]

        if input_fileIDs:
            try:
                # Start transaction
                self.conn.begin()

                varMap = {}
                varMap[":taskID"] = taskID
                varMap[":pandaID"] = jobID

                # Bind the files
                file_var_names_str, file_var_map = get_sql_IN_bind_variables(input_fileIDs, prefix=":file")
                varMap.update(file_var_map)

                # Bind the datasets
                ds_var_names_str, ds_var_map = get_sql_IN_bind_variables(input_datasetIDs, prefix=":dataset")
                varMap.update(ds_var_map)

                # Get the minimum maxAttempt value of the files
                sql_select = f"""
                select min(maxattempt) from ATLAS_PANDA.JEDI_Dataset_Contents
                WHERE JEDITaskID = :taskID
                AND datasetID IN ({ds_var_names_str})
                AND fileID IN ({file_var_names_str})
                AND pandaID = :pandaID
                """
                self.cur.execute(sql_select + comment, varMap)
                try:
                    maxAttempt_select = self.cur.fetchone()[0]
                except (TypeError, IndexError):
                    maxAttempt_select = None

                # Don't update the maxAttempt if the new value is higher than the old value
                if maxAttempt_select and maxAttempt_select > maxAttempt:
                    varMap[":maxAttempt"] = min(maxAttempt, maxAttempt_select)

                    sql_update = f"""
                    UPDATE ATLAS_PANDA.JEDI_Dataset_Contents
                    SET maxAttempt=:maxAttempt
                    WHERE JEDITaskID = :taskID
                    AND datasetID IN ({ds_var_names_str})
                    AND fileID IN ({file_var_names_str})
                    AND pandaID = :pandaID
                    """

                    self.cur.execute(sql_update + comment, varMap)

                # Commit updates
                if not self._commit():
                    raise RuntimeError("Commit error")
            except Exception:
                # roll back
                self._rollback()
                # error
                self.dump_error_message(tmp_log)
                return False

        tmp_log.debug("done")
        return True

    def increase_max_failure(self, job_id, task_id, files):
        """Increase the max failure number by one for specific files."""
        comment = " /* DBProxy.increase_max_failure */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={job_id} jediTaskID={task_id}")
        tmp_log.debug("start")

        # Update the file entries to increase the max attempt number by one
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")
        input_files = [pandafile for pandafile in files if pandafile.type in input_types and re.search("DBRelease", pandafile.lfn) is None]
        input_file_ids = [input_file.fileID for input_file in input_files]
        input_dataset_ids = [input_file.datasetID for input_file in input_files]

        if input_file_ids:
            try:
                # Start transaction
                self.conn.begin()

                var_map = {
                    ":taskID": task_id,
                    ":pandaID": job_id,
                }

                # Bind the files
                file_var_names_str, file_var_map = get_sql_IN_bind_variables(input_file_ids, prefix=":file")
                var_map.update(file_var_map)

                # Bind the datasets
                ds_var_names_str, ds_var_map = get_sql_IN_bind_variables(input_dataset_ids, prefix=":dataset")
                var_map.update(ds_var_map)

                sql_update = f"""
                UPDATE ATLAS_PANDA.JEDI_Dataset_Contents
                SET maxFailure = maxFailure + 1
                WHERE JEDITaskID = :taskID
                AND datasetID IN ({ds_var_names_str})
                AND fileID IN ({file_var_names_str})
                AND pandaID = :pandaID
                """

                self.cur.execute(sql_update + comment, var_map)

                # Commit updates
                if not self._commit():
                    raise RuntimeError("Commit error")

            except Exception:
                # Roll back
                self._rollback()
                # Log error
                self.dump_error_message(tmp_log)
                return False

        tmp_log.debug("done")
        return True

    def setNoRetry(self, jobID, taskID, files):
        # Logging
        comment = " /* DBProxy.setNoRetry */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={jobID} jediTaskID={taskID}")
        tmp_log.debug("start")

        # Update the file entries to avoid JEDI generating new jobs
        input_types = ("input", "pseudo_input", "pp_input", "trn_log", "trn_output")
        input_files = list(
            filter(
                lambda pandafile: pandafile.type in input_types and re.search("DBRelease", pandafile.lfn) is None,
                files,
            )
        )
        input_fileIDs = [input_file.fileID for input_file in input_files]
        input_datasetIDs = [input_file.datasetID for input_file in input_files]

        if input_fileIDs:
            try:
                # Start transaction
                self.conn.begin()

                # loop over all datasets
                for datasetID in input_datasetIDs:
                    varMap = {}
                    varMap[":taskID"] = taskID
                    varMap[":datasetID"] = datasetID
                    varMap[":keepTrack"] = 1

                    # Bind the files
                    file_var_names_str, file_var_map = get_sql_IN_bind_variables(input_fileIDs, prefix=":file")
                    varMap.update(file_var_map)

                    sql_update = f"""
                    UPDATE ATLAS_PANDA.JEDI_Dataset_Contents
                    SET maxAttempt=attemptNr
                    WHERE JEDITaskID = :taskID
                    AND datasetID=:datasetID
                    AND fileID IN ({file_var_names_str})
                    AND maxAttempt IS NOT NULL AND attemptNr IS NOT NULL
                    AND maxAttempt > attemptNr
                    AND (maxFailure IS NULL OR failedAttempt IS NULL OR maxFailure > failedAttempt)
                    AND keepTrack=:keepTrack
                    AND status=:status
                    """

                    # update files in 'running' status. These files do NOT need to be counted for the nFiles*
                    varMap[":status"] = "running"
                    self.cur.execute(sql_update + comment, varMap)

                    # update files in 'ready' status. These files need to be counted for the nFiles*
                    varMap[":status"] = "ready"
                    self.cur.execute(sql_update + comment, varMap)
                    rowcount = self.cur.rowcount

                    # update datasets
                    if rowcount > 0:
                        sql_dataset = "UPDATE ATLAS_PANDA.JEDI_Datasets "
                        sql_dataset += "SET nFilesUsed=nFilesUsed+:nDiff,nFilesFailed=nFilesFailed+:nDiff "
                        sql_dataset += "WHERE jediTaskID=:taskID AND datasetID=:datasetID "
                        varMap = dict()
                        varMap[":taskID"] = taskID
                        varMap[":datasetID"] = datasetID
                        varMap[":nDiff"] = rowcount
                        self.cur.execute(sql_dataset + comment, varMap)

                # Commit updates
                if not self._commit():
                    raise RuntimeError("Commit error")
            except Exception:
                # roll back
                self._rollback()
                # error
                self.dump_error_message(tmp_log)
                return False

        tmp_log.debug("done")
        return True

    # add associate sub datasets for single consumer job
    def getDestDBlocksWithSingleConsumer(self, jediTaskID, PandaID, ngDatasets):
        comment = " /* DBProxy.getDestDBlocksWithSingleConsumer */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} PandaID={PandaID}")
        tmp_log.debug("start")
        try:
            retMap = {}
            checkedDS = set()
            # sql to get files
            sqlF = "SELECT datasetID,fileID FROM ATLAS_PANDA.JEDI_Events "
            sqlF += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
            # sql to get PandaIDs
            sqlP = "SELECT distinct PandaID FROM ATLAS_PANDA.filesTable4 "
            sqlP += "WHERE jediTaskID=:jediTaskID ANd datasetID=:datasetID AND fileID=:fileID "
            # sql to get sub datasets
            sqlD = "SELECT destinationDBlock,datasetID FROM ATLAS_PANDA.filesTable4 "
            sqlD += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
            # sql to get PandaIDs in merging
            sqlM = "SELECT distinct PandaID FROM ATLAS_PANDA.filesTable4 "
            sqlM += "WHERE jediTaskID=:jediTaskID ANd datasetID=:datasetID AND status=:status "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":PandaID"] = PandaID
            # begin transaction
            self.conn.begin()
            # get files
            self.cur.execute(sqlF + comment, varMap)
            resF = self.cur.fetchall()
            for datasetID, fileID in resF:
                # get parallel jobs
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":datasetID"] = datasetID
                varMap[":fileID"] = fileID
                self.cur.execute(sqlP + comment, varMap)
                resP = self.cur.fetchall()
                for (sPandaID,) in resP:
                    if sPandaID == PandaID:
                        continue
                    # get sub datasets of parallel jobs
                    varMap = {}
                    varMap[":PandaID"] = sPandaID
                    varMap[":type1"] = "output"
                    varMap[":type2"] = "log"
                    self.cur.execute(sqlD + comment, varMap)
                    resD = self.cur.fetchall()
                    subDatasets = []
                    subDatasetID = None
                    for destinationDBlock, datasetID in resD:
                        if destinationDBlock in ngDatasets:
                            continue
                        if destinationDBlock in checkedDS:
                            continue
                        checkedDS.add(destinationDBlock)
                        subDatasets.append(destinationDBlock)
                        subDatasetID = datasetID
                    if subDatasets == []:
                        continue
                    # get merging PandaID which uses sub dataset
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    varMap[":datasetID"] = datasetID
                    varMap[":status"] = "merging"
                    self.cur.execute(sqlM + comment, varMap)
                    resM = self.cur.fetchone()
                    if resM is not None:
                        (mPandaID,) = resM
                        retMap[mPandaID] = subDatasets
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(retMap)} jobs")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get dispatch datasets per user
    def getDispatchDatasetsPerUser(self, vo, prodSourceLabel, onlyActive, withSize):
        comment = " /* DBProxy.getDispatchDatasetsPerUser */"
        tmp_log = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel}")
        tmp_log.debug("start")
        # mapping for table and job status
        tableStatMap = {"jobsDefined4": ["defined", "assigned"]}
        if not onlyActive:
            tableStatMap["jobsActive4"] = None
            tableStatMap["jobsArchived4"] = None
        try:
            userDispMap = {}
            for tableName in tableStatMap:
                statusList = tableStatMap[tableName]
                # make sql to get dispatch datasets
                varMap = {}
                varMap[":vo"] = vo
                varMap[":label"] = prodSourceLabel
                varMap[":dType"] = "dispatch"
                sqlJ = "SELECT distinct prodUserName,dispatchDBlock,jediTaskID,currentFiles "
                sqlJ += "FROM {0}.{1} j, {0}.Datasets d ".format(panda_config.schemaPANDA, tableName)
                sqlJ += "WHERE vo=:vo AND prodSourceLabel=:label "
                if statusList is not None:
                    jobstat_var_names_str, jobstat_var_map = get_sql_IN_bind_variables(statusList, prefix=":jobStat_", value_as_suffix=True)
                    sqlJ += f"AND jobStatus IN ({jobstat_var_names_str}) "
                    varMap.update(jobstat_var_map)
                sqlJ += "AND dispatchDBlock IS NOT NULL "
                sqlJ += "AND d.name=j.dispatchDBlock AND d.modificationDate>CURRENT_DATE-14 "
                sqlJ += "AND d.type=:dType "
                # begin transaction
                self.conn.begin()
                # get dispatch datasets
                self.cur.execute(sqlJ + comment, varMap)
                resJ = self.cur.fetchall()
                if not self._commit():
                    raise RuntimeError("Commit error")
                # make map
                for prodUserName, dispatchDBlock, jediTaskID, dsSize in resJ:
                    transferType = "transfer"
                    try:
                        if dispatchDBlock.split(".")[4] == "prestaging":
                            transferType = "prestaging"
                    except Exception:
                        pass
                    userDispMap.setdefault(prodUserName, {})
                    userDispMap[prodUserName].setdefault(transferType, {"datasets": set(), "size": 0, "tasks": set()})
                    if dispatchDBlock not in userDispMap[prodUserName][transferType]["datasets"]:
                        userDispMap[prodUserName][transferType]["datasets"].add(dispatchDBlock)
                        userDispMap[prodUserName][transferType]["tasks"].add(jediTaskID)
                        userDispMap[prodUserName][transferType]["size"] += dsSize
            tmp_log.debug("done")
            return userDispMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # bulk fetch PandaIDs
    def bulk_fetch_panda_ids(self, num_ids):
        comment = " /* JediDBProxy.bulk_fetch_panda_ids */"
        tmp_log = self.create_tagged_logger(comment, f"num_ids={num_ids}")
        tmp_log.debug("start")
        try:
            new_ids = []
            var_map = {}
            var_map[":nIDs"] = num_ids
            # sql to get fileID
            sqlFID = "SELECT ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ.nextval FROM "
            sqlFID += "(SELECT level FROM dual CONNECT BY level<=:nIDs) "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlFID + comment, var_map)
            resFID = self.cur.fetchall()
            for (id,) in resFID:
                new_ids.append(id)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(new_ids)} IDs")
            return sorted(new_ids)
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # bulk fetch fileIDs
    def bulkFetchFileIDsPanda(self, nIDs):
        comment = " /* JediDBProxy.bulkFetchFileIDsPanda */"
        tmp_log = self.create_tagged_logger(comment, f"nIDs={nIDs}")
        tmp_log.debug("start")
        try:
            newFileIDs = []
            varMap = {}
            varMap[":nIDs"] = nIDs
            # sql to get fileID
            sqlFID = "SELECT ATLAS_PANDA.FILESTABLE4_ROW_ID_SEQ.nextval FROM "
            sqlFID += "(SELECT level FROM dual CONNECT BY level<=:nIDs) "
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlFID + comment, varMap)
            resFID = self.cur.fetchall()
            for (fileID,) in resFID:
                newFileIDs.append(fileID)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(newFileIDs)} IDs")
            return newFileIDs
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # get LNFs for jumbo job
    def getLFNsForJumbo(self, jediTaskID):
        comment = " /* DBProxy.getLFNsForJumbo */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmp_log.debug("start")
        try:
            sqlS = "SELECT lfn,scope FROM {0}.JEDI_Datasets d, {0}.JEDI_Dataset_Contents c ".format(panda_config.schemaJEDI)
            sqlS += "WHERE d.jediTaskID=c.jediTaskID AND d.datasetID=c.datasetID AND d.jediTaskID=:jediTaskID "
            sqlS += "AND d.type IN (:type1,:type2) AND d.masterID IS NULL "
            retSet = set()
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            self.cur.execute(sqlS + comment, varMap)
            res = self.cur.fetchall()
            for tmpLFN, tmpScope in res:
                name = f"{tmpScope}:{tmpLFN}"
                retSet.add(name)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"has {len(retSet)} LFNs")
            return retSet
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return []

    # get number of started events
    def getNumStartedEvents(self, jobSpec):
        comment = " /* DBProxy.getNumStartedEvents */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={jobSpec.PandaID}")
        tmp_log.debug("start")
        try:
            # count the number of started ranges
            sqlCDO = "SELECT /*+ INDEX_RS_ASC(tab JEDI_EVENTS_FILEID_IDX) NO_INDEX_FFS(tab JEDI_EVENTS_PK) NO_INDEX_SS(tab JEDI_EVENTS_PK) */ "
            sqlCDO += f"COUNT(*) FROM {panda_config.schemaJEDI}.JEDI_Events tab "
            sqlCDO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlCDO += "AND status IN (:esSent,:esRunning,:esFinished,:esDone) "
            # start transaction
            self.conn.begin()
            nEvt = 0
            for fileSpec in jobSpec.Files:
                if fileSpec.type != "input":
                    continue
                varMap = {}
                varMap[":jediTaskID"] = fileSpec.jediTaskID
                varMap[":datasetID"] = fileSpec.datasetID
                varMap[":fileID"] = fileSpec.fileID
                varMap[":esSent"] = EventServiceUtils.ST_sent
                varMap[":esRunning"] = EventServiceUtils.ST_running
                varMap[":esFinished"] = EventServiceUtils.ST_finished
                varMap[":esDone"] = EventServiceUtils.ST_done
                self.cur.execute(sqlCDO + comment, varMap)
                res = self.cur.fetchone()
                if res is not None:
                    nEvt += res[0]
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"{nEvt} events started")
            return nEvt
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get JEDI file attributes
    def getJediFileAttributes(self, PandaID, jediTaskID, datasetID, fileID, attrs):
        comment = " /* DBProxy.getJediFileAttributes */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={PandaID}")
        tmp_log.debug(f"start for jediTaskID={jediTaskID} datasetId={datasetID} fileID={fileID}")
        try:
            # sql to get task attributes
            sqlRR = "SELECT "
            for attr in attrs:
                sqlRR += f"{attr},"
            sqlRR = sqlRR[:-1]
            sqlRR += f" FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlRR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            varMap[":fileID"] = fileID
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlRR + comment, varMap)
            resRR = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retVal = {}
            if resRR is not None:
                for idx, attr in enumerate(attrs):
                    retVal[attr] = resRR[idx]
            tmp_log.debug(f"done {str(retVal)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get jumbo job datasets
    def getJumboJobDatasets(self, n_days, grace_period):
        comment = " /* DBProxy.getJumboJobDatasets */"
        tmp_log = self.create_tagged_logger(comment, f"nDays={n_days}")
        tmp_log.debug("start")
        try:
            # sql to get workers
            sqlC = "SELECT t.jediTaskID,d.datasetName,t.status FROM ATLAS_PANDA.JEDI_Tasks t,ATLAS_PANDA.JEDI_Datasets d "
            sqlC += "WHERE t.prodSourceLabel='managed' AND t.useJumbo IS NOT NULL "
            sqlC += "AND t.modificationTime>CURRENT_DATE-:days AND t.modificationTime<CURRENT_DATE-:grace_period "
            sqlC += "AND t.status IN ('finished','done') "
            sqlC += "AND d.jediTaskID=t.jediTaskID AND d.type='output' "
            varMap = {}
            varMap[":days"] = n_days
            varMap[":grace_period"] = grace_period
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlC + comment, varMap)
            resCs = self.cur.fetchall()
            retMap = dict()
            nDS = 0
            for jediTaskID, datasetName, status in resCs:
                retMap.setdefault(jediTaskID, {"status": status, "datasets": []})
                retMap[jediTaskID]["datasets"].append(datasetName)
                nDS += 1
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {nDS} datasets")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get output datasets
    def getOutputDatasetsJEDI(self, panda_id):
        comment = " /* DBProxy.getOutputDatasetsJEDI */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id}")
        tmp_log.debug("start")
        try:
            # sql to get workers
            sqlC = "SELECT d.datasetID,d.datasetName FROM ATLAS_PANDA.filesTable4 f,ATLAS_PANDA.JEDI_Datasets d "
            sqlC += "WHERE f.PandaID=:PandaID AND f.type IN (:type1,:type2) AND d.jediTaskID=f.jediTaskID AND d.datasetID=f.datasetID "
            varMap = {}
            varMap[":PandaID"] = panda_id
            varMap[":type1"] = "output"
            varMap[":type2"] = "log"
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlC + comment, varMap)
            retMap = dict()
            resCs = self.cur.fetchall()
            for datasetID, datasetName in resCs:
                retMap[datasetID] = datasetName
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"got {len(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # lock process
    def lockProcess_PANDA(self, component, pid, time_limit, force=False):
        comment = " /* DBProxy.lockProcess_PANDA */"
        tmp_log = self.create_tagged_logger(comment, f"component={component} pid={pid}")
        # defaults
        vo = "default"
        prodSourceLabel = "default"
        cloud = "default"
        workqueue_id = 0
        resource_name = "default"
        tmp_log.debug("start")
        try:
            retVal = False
            # sql to check
            sqlCT = (
                "SELECT lockedBy "
                "FROM {0}.JEDI_Process_Lock "
                "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
                "AND cloud=:cloud AND workqueue_id=:workqueue_id "
                "AND resource_type=:resource_name AND component=:component "
                "AND lockedTime>:lockedTime "
                "FOR UPDATE"
            ).format(panda_config.schemaJEDI)
            # sql to delete
            sqlCD = (
                "DELETE FROM {0}.JEDI_Process_Lock "
                "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
                "AND cloud=:cloud AND workqueue_id=:workqueue_id "
                "AND resource_type=:resource_name AND component=:component "
            ).format(panda_config.schemaJEDI)
            # sql to insert
            sqlFR = (
                "INSERT INTO {0}.JEDI_Process_Lock "
                "(vo, prodSourceLabel, cloud, workqueue_id, resource_type, component, lockedBy, lockedTime) "
                "VALUES(:vo, :prodSourceLabel, :cloud, :workqueue_id, :resource_name, :component, :lockedBy, CURRENT_DATE) "
            ).format(panda_config.schemaJEDI)
            # start transaction
            self.conn.begin()
            # check
            if not force:
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                varMap[":lockedTime"] = naive_utcnow() - datetime.timedelta(minutes=time_limit)
                self.cur.execute(sqlCT + comment, varMap)
                resCT = self.cur.fetchone()
            else:
                resCT = None
            if resCT is not None:
                tmp_log.debug(f"skipped, locked by {resCT[0]}")
            else:
                # delete
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                self.cur.execute(sqlCD + comment, varMap)
                # insert
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                varMap[":lockedBy"] = pid
                self.cur.execute(sqlFR + comment, varMap)
                tmp_log.debug("successfully locked")
                retVal = True
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # unlock process
    def unlockProcess_PANDA(self, component, pid):
        comment = " /* DBProxy.unlockProcess_PANDA */"
        tmp_log = self.create_tagged_logger(comment, f"component={component} pid={pid}")
        # defaults
        vo = "default"
        prodSourceLabel = "default"
        cloud = "default"
        workqueue_id = 0
        resource_name = "default"
        tmp_log.debug("start")
        try:
            retVal = False
            # sql to delete
            sqlCD = (
                "DELETE FROM {0}.JEDI_Process_Lock "
                "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud "
                "AND workqueue_id=:workqueue_id AND lockedBy=:lockedBy "
                "AND resource_type=:resource_name AND component=:component "
            ).format(panda_config.schemaJEDI)
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":cloud"] = cloud
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            varMap[":component"] = component
            varMap[":lockedBy"] = pid
            self.cur.execute(sqlCD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            retVal = True
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # check process lock
    def checkProcessLock_PANDA(self, component, pid, time_limit, check_base=False):
        comment = " /* DBProxy.checkProcessLock_PANDA */"
        tmp_log = self.create_tagged_logger(comment, f"component={component} pid={pid}")
        # defaults
        vo = "default"
        prodSourceLabel = "default"
        cloud = "default"
        workqueue_id = 0
        resource_name = "default"
        tmp_log.debug("start")
        try:
            retVal = False, None
            # sql to check
            sqlCT = (
                "SELECT lockedBy, lockedTime "
                "FROM {0}.JEDI_Process_Lock "
                "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
                "AND cloud=:cloud AND workqueue_id=:workqueue_id "
                "AND resource_type=:resource_name AND component=:component "
                "AND lockedTime>:lockedTime "
            ).format(panda_config.schemaJEDI)
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":cloud"] = cloud
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            varMap[":component"] = component
            varMap[":lockedTime"] = naive_utcnow() - datetime.timedelta(minutes=time_limit)
            self.cur.execute(sqlCT + comment, varMap)
            resCT = self.cur.fetchone()
            if resCT is not None:
                lockedBy, lockedTime = resCT
                if check_base:
                    # check only base part
                    if not lockedBy.startswith(pid):
                        retVal = True, lockedTime
                else:
                    # check whole string
                    if lockedBy != pid:
                        retVal = True, lockedTime
                if retVal[0]:
                    tmp_log.debug(f"found locked by {lockedBy} at {lockedTime.strftime('%Y-%m-%d_%H:%M:%S')}")
                else:
                    tmp_log.debug("found unlocked")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return retVal

    # update problematic resource info for user
    def update_problematic_resource_info(self, user_name, jedi_task_id, resource, problem_type):
        comment = " /* DBProxy.update_problematic_resource_info */"
        tmp_log = self.create_tagged_logger(comment, f"user={user_name} jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        retVal = False
        try:
            if problem_type not in ["dest", None]:
                tmp_log.debug(f"unknown problem type: {problem_type}")
                return None
            sqlR = "SELECT pagecache FROM ATLAS_PANDAMETA.users " "WHERE name=:name "
            sqlW = "UPDATE ATLAS_PANDAMETA.users SET pagecache=:data " "WHERE name=:name "
            # string to use a dict key
            jedi_task_id = str(jedi_task_id)
            # start transaction
            self.conn.begin()
            # read
            varMap = {}
            varMap[":name"] = user_name
            self.cur.execute(sqlR + comment, varMap)
            data = self.cur.fetchone()
            if data is None:
                tmp_log.debug("user not found")
            else:
                try:
                    data = json.loads(data[0])
                except Exception:
                    data = {}
                if problem_type is not None:
                    data.setdefault(problem_type, {})
                    data[problem_type].setdefault(jedi_task_id, {})
                    data[problem_type][jedi_task_id].setdefault(resource, None)
                    old = data[problem_type][jedi_task_id][resource]
                    if old is None or datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(
                        old, datetime.timezone.utc
                    ) > datetime.timedelta(days=1):
                        retVal = True
                        data[problem_type][jedi_task_id][resource] = time.time()
                # delete old data
                for p in list(data):
                    for t in list(data[p]):
                        for r in list(data[p][t]):
                            ts = data[p][t][r]
                            if datetime.datetime.now(datetime.timezone.utc) - datetime.datetime.fromtimestamp(ts, datetime.timezone.utc) > datetime.timedelta(
                                days=7
                            ):
                                del data[p][t][r]
                        if not data[p][t]:
                            del data[p][t]
                    if not data[p]:
                        del data[p]
                # update
                varMap = {}
                varMap[":name"] = user_name
                varMap[":data"] = json.dumps(data)
                self.cur.execute(sqlW + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done with {retVal} : {str(data)}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get LFNs in datasets
    def get_files_in_datasets(self, task_id, dataset_types):
        comment = " /* DBProxy.get_lfns_in_datasets */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={task_id}")
        tmp_log.debug("start")
        try:
            varMap = {}
            varMap[":jediTaskID"] = task_id
            sqlD = f"SELECT datasetName,datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
            # Old API expects comma separated types, while new API is taking directly a tuple of dataset types
            if type(dataset_types) == str:
                dataset_types = dataset_types.split(",")
            dstype_var_names_str, dstype_var_map = get_sql_IN_bind_variables(dataset_types, prefix=":", value_as_suffix=True)
            sqlD += f"AND type IN ({dstype_var_names_str}) "
            varMap.update(dstype_var_map)
            sqlS = f"SELECT lfn,scope,fileID,status FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID ORDER BY fileID "
            retVal = []
            # start transaction
            self.conn.begin()
            self.cur.execute(sqlD + comment, varMap)
            res = self.cur.fetchall()
            for datasetName, datasetID in res:
                datasetDict = {}
                datasetDict["name"] = datasetName
                datasetDict["id"] = datasetID
                # read files
                varMap = {}
                varMap[":jediTaskID"] = task_id
                varMap[":datasetID"] = datasetID
                self.cur.execute(sqlS + comment, varMap)
                resF = self.cur.fetchall()
                fileList = []
                for lfn, fileScope, fileID, status in resF:
                    fileDict = {
                        "lfn": lfn,
                        "scope": fileScope,
                        "id": fileID,
                        "status": status,
                    }
                    fileList.append(fileDict)
                retVal.append({"dataset": datasetDict, "files": fileList})
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # update datasets asynchronously outside propagateResultToJEDI to avoid row contentions
    def async_update_datasets(self, panda_id):
        comment = " /* DBProxy.async_update_datasets */"
        g_tmp_log = self.create_tagged_logger(comment)
        g_tmp_log.debug("start")
        try:
            if panda_id is not None:
                panda_id_list = [panda_id]
            else:
                # get PandaIDs
                sql = f"SELECT DISTINCT PandaID FROM {panda_config.schemaPANDA}.SQL_QUEUE WHERE topic=:topic AND creationTime<:timeLimit"
                varMap = {
                    ":topic": SQL_QUEUE_TOPIC_async_dataset_update,
                    ":timeLimit": naive_utcnow() - datetime.timedelta(minutes=1),
                }
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 10000
                self.cur.execute(sql + comment, varMap)
                panda_id_list = [i[0] for i in self.cur.fetchall()]
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            if not panda_id_list:
                g_tmp_log.debug("done since no IDs are available")
                return None
            # loop over all IDs
            for tmp_id in panda_id_list:
                tmp_log = self.create_tagged_logger(comment, f"PandaID={tmp_id}")
                sqlL = "SELECT data FROM {0}.SQL_QUEUE WHERE topic=:topic AND PandaID=:PandaID ORDER BY " "execution_order FOR UPDATE NOWAIT ".format(
                    panda_config.schemaPANDA
                )
                sqlD = f"DELETE FROM {panda_config.schemaPANDA}.SQL_QUEUE WHERE PandaID=:PandaID "
                n_try = 5
                all_ok = True
                for i_try in range(n_try):
                    all_ok = True
                    query_list = []
                    tmp_log.debug(f"Trying PandaID={tmp_id} {i_try+1}/{n_try}")
                    tmp_data_list = None
                    # start transaction
                    self.conn.begin()
                    # lock queries
                    try:
                        var_map = {
                            ":topic": SQL_QUEUE_TOPIC_async_dataset_update,
                            ":PandaID": tmp_id,
                        }
                        self.cur.execute(sqlL + comment, var_map)
                        tmp_data_list = self.cur.fetchall()
                    except Exception:
                        tmp_log.debug("cannot lock queries")
                        all_ok = False
                    if tmp_data_list:
                        # execute queries
                        if all_ok:
                            for (tmp_data,) in tmp_data_list:
                                sql, var_map = json.loads(tmp_data)
                                query_list.append((sql, var_map))
                                try:
                                    self.cur.execute(sql + comment, var_map)
                                except Exception:
                                    tmp_log.error(f'failed to execute "{sql}" var={str(var_map)}')
                                    self.dump_error_message(tmp_log)
                                    all_ok = False
                                    break
                        # delete queries
                        if all_ok:
                            var_map = {":PandaID": tmp_id}
                            self.cur.execute(sqlD + comment, var_map)
                    # commit
                    if all_ok:
                        if not self._commit():
                            raise RuntimeError("Commit error")
                        for sql, var_map in query_list:
                            tmp_log.debug(sql + str(var_map))
                        tmp_log.debug("done")
                        break
                    else:
                        self._rollback()
                        if i_try + 1 < n_try:
                            time.sleep(1)
                if not all_ok:
                    tmp_log.error("all attempts failed")
            g_tmp_log.debug(f"processed {len(panda_id_list)} IDs")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(g_tmp_log)
            return False

    # insert data carousel requests
    def insert_data_carousel_requests_JEDI(self, task_id, dc_req_specs):
        comment = " /* JediDBProxy.insert_data_carousel_requests_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={task_id}")
        tmp_log.debug("start")
        try:
            # start transaction
            self.conn.begin()
            # insert requests
            n_req_inserted = 0
            n_rel_inserted = 0
            n_req_reused = 0
            n_rel_reused = 0
            for dc_req_spec in dc_req_specs:
                # sql to query request of the dataset
                status_var_names_str, status_var_map = get_sql_IN_bind_variables(DataCarouselRequestStatus.reusable_statuses, prefix=":status")
                sql_query = (
                    f"SELECT request_id "
                    f"FROM {panda_config.schemaJEDI}.data_carousel_requests "
                    f"WHERE dataset=:dataset "
                    f"AND status IN ({status_var_names_str}) "
                )
                var_map = {":dataset": dc_req_spec.dataset}
                var_map.update(status_var_map)
                self.cur.execute(sql_query + comment, var_map)
                res = self.cur.fetchall()
                # check if already existing request for the dataset
                the_request_id = None
                if res:
                    # have existing request; reuse it
                    for (request_id,) in res:
                        the_request_id = request_id
                        n_req_reused += 1
                        break
                else:
                    # no existing request; insert new one
                    # sql to insert request
                    sql_insert_request = (
                        f"INSERT INTO {panda_config.schemaJEDI}.data_carousel_requests ({dc_req_spec.columnNames()}) "
                        f"{dc_req_spec.bindValuesExpression()} "
                        f"RETURNING request_id INTO :new_request_id "
                    )
                    var_map = dc_req_spec.valuesMap(useSeq=True)
                    var_map[":new_request_id"] = self.cur.var(varNUMBER)
                    self.cur.execute(sql_insert_request + comment, var_map)
                    the_request_id = int(self.getvalue_corrector(self.cur.getvalue(var_map[":new_request_id"])))
                    n_req_inserted += 1
                if the_request_id is None:
                    raise RuntimeError("the_request_id is None")
                # sql to query relation
                sql_rel_query = (
                    f"SELECT request_id, task_id "
                    f"FROM {panda_config.schemaJEDI}.data_carousel_relations "
                    f"WHERE request_id=:request_id AND task_id=:task_id "
                )
                var_map = {":request_id": the_request_id, ":task_id": task_id}
                self.cur.execute(sql_rel_query + comment, var_map)
                res = self.cur.fetchall()
                if res:
                    # have existing relation; skipped
                    n_rel_reused += 1
                else:
                    # sql to insert relation
                    sql_insert_relation = (
                        f"INSERT INTO {panda_config.schemaJEDI}.data_carousel_relations (request_id, task_id) " f"VALUES(:request_id, :task_id) "
                    )
                    self.cur.execute(sql_insert_relation + comment, var_map)
                    n_rel_inserted += 1
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(
                f"inserted {n_req_inserted}/{len(dc_req_specs)} requests and {n_rel_inserted} relations ; reused {n_req_reused} requests and {n_rel_reused} relations"
            )
            return n_req_inserted
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # update a data carousel request
    def update_data_carousel_request_JEDI(self, dc_req_spec):
        comment = " /* JediDBProxy.update_data_carousel_request_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={dc_req_spec.request_id}")
        tmp_log.debug("start")
        try:
            # start transaction
            self.conn.begin()
            # sql to update request
            dc_req_spec.modification_time = naive_utcnow()
            sql_update = (
                f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests " f"SET {dc_req_spec.bindUpdateChangesExpression()} " "WHERE request_id=:request_id "
            )
            var_map = dc_req_spec.valuesMap(useSeq=False, onlyChanged=True)
            var_map[":request_id"] = dc_req_spec.request_id
            self.cur.execute(sql_update + comment, var_map)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"updated {dc_req_spec.bindUpdateChangesExpression()}")
            return dc_req_spec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get data carousel queued requests and info of their related tasks
    def get_data_carousel_queued_requests_JEDI(self):
        comment = " /* JediDBProxy.get_data_carousel_queued_requests_JEDI */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # initialize
            ret_list = []
            # start transaction
            self.conn.begin()
            # sql to query queued requests with gshare and priority info from related tasks
            sql_query_req = (
                f"SELECT {DataCarouselRequestSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.data_carousel_requests " f"WHERE status=:status "
            )
            var_map = {":status": DataCarouselRequestStatus.queued}
            self.cur.execute(sql_query_req + comment, var_map)
            res_list = self.cur.fetchall()
            if res_list:
                for res in res_list:
                    # make request spec
                    dc_req_spec = DataCarouselRequestSpec()
                    dc_req_spec.pack(res)
                    # query info of related tasks
                    sql_query_tasks = (
                        f"SELECT t.jediTaskID, t.gshare, COALESCE(t.currentPriority, t.taskPriority), t.taskType, t.userName, t.workingGroup "
                        f"FROM {panda_config.schemaJEDI}.data_carousel_relations rel, {panda_config.schemaJEDI}.JEDI_Tasks t "
                        f"WHERE rel.request_id=:request_id AND rel.task_id=t.jediTaskID "
                    )
                    var_map = {":request_id": dc_req_spec.request_id}
                    self.cur.execute(sql_query_tasks + comment, var_map)
                    res_tasks = self.cur.fetchall()
                    task_specs = []
                    for task_id, gshare, priority, task_type, user_name, working_group in res_tasks:
                        task_spec = JediTaskSpec()
                        task_spec.jediTaskID = task_id
                        task_spec.gshare = gshare
                        task_spec.currentPriority = priority
                        task_spec.taskType = task_type
                        task_spec.userName = user_name
                        task_spec.workingGroup = working_group
                        task_specs.append(task_spec)
                    # add
                    ret_list.append((dc_req_spec, task_specs))
            else:
                tmp_log.debug("no queued request")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"got {len(ret_list)} queued requests")
            return ret_list
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get data carousel requests of tasks by task status
    def get_data_carousel_requests_by_task_status_JEDI(self, status_filter_list=None, status_exclusion_list=None):
        comment = " /* JediDBProxy.get_data_carousel_requests_by_task_status_JEDI */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # initialize
            ret_requests_map = {}
            ret_relation_map = {}
            # start transaction
            self.conn.begin()
            # sql to query queued requests with gshare and priority info from related tasks
            sql_query_id = (
                f"SELECT rel.request_id, rel.task_id "
                f"FROM {panda_config.schemaJEDI}.data_carousel_relations rel, {panda_config.schemaJEDI}.JEDI_Tasks t "
                f"WHERE rel.task_id=t.jediTaskID "
            )
            var_map = {}
            if status_filter_list:
                status_var_names_str, status_var_map = get_sql_IN_bind_variables(status_filter_list, prefix=":status")
                sql_query_id += f"AND t.status IN ({status_var_names_str}) "
                var_map.update(status_var_map)
            if status_exclusion_list:
                antistatus_var_names_str, antistatus_var_map = get_sql_IN_bind_variables(status_exclusion_list, prefix=":antistatus")
                sql_query_id += f"AND t.status NOT IN ({antistatus_var_names_str}) "
                var_map.update(antistatus_var_map)
            self.cur.execute(sql_query_id + comment, var_map)
            res_list = self.cur.fetchall()
            if res_list:
                for request_id, task_id in res_list:
                    # fill relation map
                    ret_relation_map.setdefault(task_id, [])
                    ret_relation_map[task_id].append(request_id)
                    if request_id in ret_requests_map:
                        # already got the request spec; skip
                        continue
                    else:
                        # query info of related tasks
                        sql_query_requests = (
                            f"SELECT {DataCarouselRequestSpec.columnNames()} "
                            f"FROM {panda_config.schemaJEDI}.data_carousel_requests "
                            f"WHERE request_id=:request_id "
                        )
                        var_map = {":request_id": request_id}
                        self.cur.execute(sql_query_requests + comment, var_map)
                        req_res_list = self.cur.fetchall()
                        # make request spec
                        dc_req_spec = DataCarouselRequestSpec()
                        for req_res in req_res_list:
                            dc_req_spec.pack(req_res)
                            break
                        # fill requests map
                        ret_requests_map[request_id] = dc_req_spec
            else:
                tmp_log.debug("no queued request")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"got {len(ret_requests_map)} requests of {len(ret_relation_map)} active tasks")
            return ret_requests_map, ret_relation_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get data carousel staging requests
    def get_data_carousel_staging_requests_JEDI(self, time_limit_minutes=5):
        comment = " /* JediDBProxy.get_data_carousel_staging_requests_JEDI */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # initialize
            ret_list = []
            # start transaction
            self.conn.begin()
            # sql to query staging requests
            sql_query_req = (
                f"SELECT {DataCarouselRequestSpec.columnNames()} "
                f"FROM {panda_config.schemaJEDI}.data_carousel_requests "
                f"WHERE status=:status "
                f"AND ( check_time IS NULL OR check_time<=:check_time_max ) "
            )
            now_time = naive_utcnow()
            var_map = {":status": DataCarouselRequestStatus.staging, ":check_time_max": now_time - datetime.timedelta(minutes=time_limit_minutes)}
            self.cur.execute(sql_query_req + comment, var_map)
            res_list = self.cur.fetchall()
            if res_list:
                now_time = naive_utcnow()
                sql_update = f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests " f"SET check_time=:check_time " f"WHERE request_id=:request_id "
                for res in res_list:
                    # make request spec
                    dc_req_spec = DataCarouselRequestSpec()
                    dc_req_spec.pack(res)
                    # update check time
                    var_map = {":request_id": dc_req_spec.request_id, ":check_time": now_time}
                    self.cur.execute(sql_update + comment, var_map)
                    # add
                    ret_list.append(dc_req_spec)
            else:
                tmp_log.debug("no queued request")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"got {len(ret_list)} queued requests")
            return ret_list
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # delete data carousel requests
    def delete_data_carousel_requests_JEDI(self, request_id_list):
        comment = " /* JediDBProxy.delete_data_carousel_requests_JEDI */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # start transaction
            self.conn.begin()
            # sql to delete terminated requests
            status_var_names_str, status_var_map = get_sql_IN_bind_variables(DataCarouselRequestStatus.final_statuses, prefix=":status")
            sql_delete_req = (
                f"DELETE {panda_config.schemaJEDI}.data_carousel_requests " f"WHERE request_id=:request_id " f"AND status IN ({status_var_names_str}) "
            )
            var_map_base = {}
            var_map_base.update(status_var_map)
            var_map_list = []
            for request_id in request_id_list:
                var_map = var_map_base.copy()
                var_map[":request_id"] = request_id
                var_map_list.append(var_map)
            self.cur.executemany(sql_delete_req + comment, var_map_list)
            ret_req = self.cur.rowcount
            # sql to delete relations
            sql_delete_rel = (
                f"DELETE {panda_config.schemaJEDI}.data_carousel_relations rel "
                f"WHERE rel.request_id NOT IN "
                f"(SELECT req.request_id FROM {panda_config.schemaJEDI}.data_carousel_requests req) "
            )
            self.cur.execute(sql_delete_rel + comment, {})
            ret_rel = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"cleaned up {ret_req}/{len(request_id_list)} requests and {ret_rel} relations")
            return ret_req
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # clean up data carousel requests
    def clean_up_data_carousel_requests_JEDI(self, time_limit_days=30):
        comment = " /* JediDBProxy.clean_up_data_carousel_requests_JEDI */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # start transaction
            self.conn.begin()
            # sql to delete terminated requests
            now_time = naive_utcnow()
            status_var_names_str, status_var_map = get_sql_IN_bind_variables(DataCarouselRequestStatus.final_statuses, prefix=":status")
            sql_delete_req = (
                f"DELETE {panda_config.schemaJEDI}.data_carousel_requests " f"WHERE status IN ({status_var_names_str}) " f"AND end_time<=:end_time_max "
            )
            var_map = {":end_time_max": now_time - datetime.timedelta(days=time_limit_days)}
            var_map.update(status_var_map)
            self.cur.execute(sql_delete_req + comment, var_map)
            ret_req = self.cur.rowcount
            # sql to delete relations
            sql_delete_rel = (
                f"DELETE {panda_config.schemaJEDI}.data_carousel_relations rel "
                f"WHERE rel.request_id NOT IN "
                f"(SELECT req.request_id FROM {panda_config.schemaJEDI}.data_carousel_requests req) "
            )
            self.cur.execute(sql_delete_rel + comment, {})
            ret_rel = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"cleaned up {ret_req} requests and {ret_rel} relations older than {time_limit_days} days")
            return ret_req
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # cancel a data carousel request
    def cancel_data_carousel_request_JEDI(self, request_id):
        comment = " /* JediDBProxy.cancel_data_carousel_request_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id}")
        tmp_log.debug("start")
        try:
            # start transaction
            self.conn.begin()
            # sql to update request status to cancelled
            now_time = naive_utcnow()
            status_var_names_str, status_var_map = get_sql_IN_bind_variables(DataCarouselRequestStatus.active_statuses, prefix=":old_status")
            sql_update = (
                f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests "
                f"SET status=:new_status, end_time=:now_time, modification_time=:now_time "
                f"WHERE request_id=:request_id "
                f"AND status IN ({status_var_names_str}) "
            )
            var_map = {
                ":request_id": request_id,
                ":new_status": DataCarouselRequestStatus.cancelled,
                ":now_time": now_time,
            }
            var_map.update(status_var_map)
            self.cur.execute(sql_update + comment, var_map)
            ret_req = self.cur.rowcount
            if not ret_req:
                tmp_log.warning(f"already terminated; cannot be cancelled ; skipped")
            else:
                tmp_log.debug(f"cancelled request")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return ret_req
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # retire a data carousel request
    def retire_data_carousel_request_JEDI(self, request_id):
        comment = " /* JediDBProxy.retire_data_carousel_request_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id}")
        tmp_log.debug("start")
        try:
            # start transaction
            self.conn.begin()
            # sql to update request status to retired
            now_time = naive_utcnow()
            sql_update = (
                f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests "
                f"SET status=:new_status, modification_time=:now_time "
                f"WHERE request_id=:request_id "
                f"AND status=:old_status "
            )
            var_map = {
                ":request_id": request_id,
                ":old_status": DataCarouselRequestStatus.done,
                ":new_status": DataCarouselRequestStatus.retired,
                ":now_time": now_time,
            }
            self.cur.execute(sql_update + comment, var_map)
            ret_req = self.cur.rowcount
            if not ret_req:
                tmp_log.warning(f"not done; cannot be retired ; skipped")
            else:
                tmp_log.debug(f"retired request")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return ret_req
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # resubmit a data carousel request
    def resubmit_data_carousel_request_JEDI(self, request_id, exclude_prev_dst=False):
        comment = " /* JediDBProxy.resubmit_data_carousel_request_JEDI */"
        to_resubmit = False
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id} exclude_prev_dst={exclude_prev_dst}")
        tmp_log.debug("start")
        try:
            # start transaction
            self.conn.begin()
            # get request spec
            dc_req_spec = None
            status_var_names_str, status_var_map = get_sql_IN_bind_variables(DataCarouselRequestStatus.resubmittable_statuses, prefix=":status")
            sql_query_req = (
                f"SELECT {DataCarouselRequestSpec.columnNames()} "
                f"FROM {panda_config.schemaJEDI}.data_carousel_requests "
                f"WHERE request_id=:request_id "
                f"AND status IN ({status_var_names_str}) "
            )
            var_map = {":request_id": request_id}
            var_map.update(status_var_map)
            self.cur.execute(sql_query_req + comment, var_map)
            res_list = self.cur.fetchall()
            for res in res_list:
                # make request spec
                dc_req_spec = DataCarouselRequestSpec()
                dc_req_spec.pack(res)
                break
            # prepare new request spec to resubmit
            if dc_req_spec:
                dc_req_spec_to_resubmit = get_resubmit_request_spec(dc_req_spec, exclude_prev_dst)
            else:
                # roll back
                self._rollback()
                return False
            # sql to update old request status (staging to cancelled, done to retired, others intact)
            now_time = naive_utcnow()
            if dc_req_spec.status == DataCarouselRequestStatus.staging:
                new_status = DataCarouselRequestStatus.cancelled
                sql_update = (
                    f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests "
                    f"SET status=:new_status, end_time=:now_time, modification_time=:now_time "
                    f"WHERE request_id=:request_id "
                )
                var_map = {
                    ":request_id": request_id,
                    ":new_status": new_status,
                    ":now_time": now_time,
                }
                self.cur.execute(sql_update + comment, var_map)
                ret_req = self.cur.rowcount
                if not ret_req:
                    tmp_log.warning(f"cannot be cancelled ; skipped")
                    # roll back
                    self._rollback()
                    return False
                else:
                    tmp_log.debug(f"cancelled request")
            elif dc_req_spec.status == DataCarouselRequestStatus.done:
                new_status = DataCarouselRequestStatus.retired
                sql_update = (
                    f"UPDATE {panda_config.schemaJEDI}.data_carousel_requests "
                    f"SET status=:new_status, modification_time=:now_time "
                    f"WHERE request_id=:request_id "
                )
                var_map = {
                    ":request_id": request_id,
                    ":new_status": new_status,
                    ":now_time": now_time,
                }
                self.cur.execute(sql_update + comment, var_map)
                ret_req = self.cur.rowcount
                if not ret_req:
                    tmp_log.warning(f"cannot be retired ; skipped")
                    # roll back
                    self._rollback()
                    return False
                else:
                    tmp_log.debug(f"retired request")
            else:
                (f"already {dc_req_spec.status} ; skipped")
            # resubmit new request
            # sql to insert request
            sql_insert_request = (
                f"INSERT INTO {panda_config.schemaJEDI}.data_carousel_requests ({dc_req_spec_to_resubmit.columnNames()}) "
                f"{dc_req_spec_to_resubmit.bindValuesExpression()} "
                f"RETURNING request_id INTO :new_request_id "
            )
            var_map = dc_req_spec_to_resubmit.valuesMap(useSeq=True)
            var_map[":new_request_id"] = self.cur.var(varNUMBER)
            self.cur.execute(sql_insert_request + comment, var_map)
            new_request_id = int(self.getvalue_corrector(self.cur.getvalue(var_map[":new_request_id"])))
            if new_request_id is None:
                raise RuntimeError("new_request_id is None")
            tmp_log.debug(f"resubmitted request with new_request_id={new_request_id}")
            # sql to update relations according to the relations of the old request
            sql_update_relations = (
                f"UPDATE {panda_config.schemaJEDI}.data_carousel_relations " f"SET request_id=:new_request_id " f"WHERE request_id=:old_request_id "
            )
            var_map = {":new_request_id": new_request_id, ":old_request_id": request_id}
            self.cur.execute(sql_update_relations + comment, var_map)
            ret_rel = self.cur.rowcount
            tmp_log.debug(f"updated {ret_rel} relations about new_request_id={new_request_id}")
            # fill new request_id
            dc_req_spec_resubmitted = dc_req_spec_to_resubmit
            dc_req_spec_resubmitted.request_id = new_request_id
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            return dc_req_spec_resubmitted
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get pending data carousel tasks and their input datasets
    def get_pending_dc_tasks_JEDI(self, task_type="prod", time_limit_minutes=60):
        comment = " /* JediDBProxy.get_pending_dc_tasks_JEDI */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # sql to get pending tasks
            sql_tasks = (
                "SELECT tabT.jediTaskID, tabT.splitRule "
                "FROM {0}.JEDI_Tasks tabT, {0}.JEDI_AUX_Status_MinTaskID tabA "
                "WHERE tabT.status=:status AND tabA.status=tabT.status "
                "AND tabT.taskType=:taskType AND tabT.modificationTime<:timeLimit".format(panda_config.schemaJEDI)
            )
            # sql to get input dataset
            sql_ds = (
                "SELECT tabD.datasetID, tabD.datasetName "
                "FROM {0}.JEDI_Datasets tabD "
                "WHERE tabD.jediTaskID=:jediTaskID AND tabD.type IN (:type1, :type2) ".format(panda_config.schemaJEDI)
            )
            # initialize
            ret_tasks_dict = {}
            # start transaction
            self.conn.begin()
            # get pending tasks
            var_map = {":status": "pending", ":taskType": task_type}
            var_map[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=time_limit_minutes)
            self.cur.execute(sql_tasks + comment, var_map)
            res = self.cur.fetchall()
            if res:
                for task_id, split_rule in res:
                    tmp_taskspec = JediTaskSpec()
                    tmp_taskspec.splitRule = split_rule
                    if tmp_taskspec.inputPreStaging():
                        # is data carousel task
                        var_map = {
                            ":jediTaskID": task_id,
                            ":type1": "input",
                            ":type2": "pseudo_input",
                        }
                        self.cur.execute(sql_ds + comment, var_map)
                        ds_res = self.cur.fetchall()
                        if ds_res:
                            ret_tasks_dict[task_id] = []
                            for ds_id, ds_name in ds_res:
                                ret_tasks_dict[task_id].append(ds_name)
            else:
                tmp_log.debug("no pending task")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"found pending dc tasks: {ret_tasks_dict}")
            return ret_tasks_dict
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get datasets of input and lib, to update data locality records
    def get_tasks_inputdatasets_JEDI(self, vo):
        comment = " /* JediDBProxy.get_tasks_inputdatasets_JEDI */"
        # last update time
        tmpLog = self.create_tagged_logger(comment, f"vo={vo}")
        tmpLog.debug("start")
        now_ts = naive_utcnow()
        try:
            retVal = None
            # sql to get all jediTaskID and datasetID of input
            sql = (
                "SELECT tabT.jediTaskID,datasetID, tabD.datasetName "
                "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_Datasets tabD,{0}.JEDI_AUX_Status_MinTaskID tabA "
                "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
                "AND tabT.vo=:vo AND tabT.status IN ('running', 'ready', 'scouting', 'pending') "
                "AND tabD.type IN ('input') AND tabD.masterID IS NULL "
            ).format(panda_config.schemaJEDI)
            # start transaction
            self.conn.begin()
            # get
            varMap = {}
            varMap[":vo"] = vo
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchall()
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            retVal = res
            tmpLog.debug(f"done with {nRows} rows")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # update dataset locality
    def updateDatasetLocality_JEDI(self, jedi_taskid, datasetid, rse):
        comment = " /* JediDBProxy.updateDatasetLocality_JEDI */"
        # last update time
        timestamp = naive_utcnow()
        timestamp_str = timestamp.strftime("%Y-%m-%d_%H:%M:%S")
        tmpLog = self.create_tagged_logger(comment, f"taskID={jedi_taskid} datasetID={datasetid} rse={rse} timestamp={timestamp_str}")
        # tmpLog.debug('start')
        try:
            retVal = False
            # sql to check
            sqlC = f"SELECT timestamp FROM {panda_config.schemaJEDI}.JEDI_Dataset_Locality WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND rse=:rse "
            # sql to insert
            sqlI = (
                "INSERT INTO {0}.JEDI_Dataset_Locality " "(jediTaskID, datasetID, rse, timestamp) " "VALUES (:jediTaskID, :datasetID, :rse, :timestamp)"
            ).format(panda_config.schemaJEDI)
            # sql to update
            sqlU = (
                "UPDATE {0}.JEDI_Dataset_Locality " "SET timestamp=:timestamp " "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND rse=:rse "
            ).format(panda_config.schemaJEDI)
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":jediTaskID"] = jedi_taskid
            varMap[":datasetID"] = datasetid
            varMap[":rse"] = rse
            self.cur.execute(sqlC + comment, varMap)
            resC = self.cur.fetchone()
            varMap[":timestamp"] = timestamp
            if resC is None:
                # insert if missing
                tmpLog.debug("insert")
                self.cur.execute(sqlI + comment, varMap)
            else:
                # update
                tmpLog.debug("update")
                self.cur.execute(sqlU + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            retVal = True
            # tmpLog.debug('done')
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # delete outdated dataset locality records
    def deleteOutdatedDatasetLocality_JEDI(self, before_timestamp):
        comment = " /* JediDBProxy.deleteOutdatedDatasetLocality_JEDI */"
        # last update time
        before_timestamp_str = before_timestamp.strftime("%Y-%m-%d_%H:%M:%S")
        tmpLog = self.create_tagged_logger(comment, f"before_timestamp={before_timestamp_str}")
        tmpLog.debug("start")
        try:
            retVal = 0
            # sql to delete
            sqlD = f"DELETE {panda_config.schemaJEDI}.Jedi_Dataset_Locality WHERE timestamp<=:timestamp "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":timestamp"] = before_timestamp
            # delete
            self.cur.execute(sqlD + comment, varMap)
            retVal = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug(f"done, deleted {retVal} records")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # append input datasets for incremental execution
    def appendDatasets_JEDI(self, jediTaskID, inMasterDatasetSpecList, inSecDatasetSpecList):
        comment = " /* JediDBProxy.appendDatasets_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        goDefined = False
        refreshContents = False
        commandStr = "incexec"
        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # check task status
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlTK = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE "
            self.cur.execute(sqlTK + comment, varMap)
            resTK = self.cur.fetchone()
            if resTK is None:
                # task not found
                msgStr = "task not found"
                tmpLog.debug(msgStr)
            else:
                (taskStatus,) = resTK
                # invalid status
                if taskStatus != JediTaskSpec.commandStatusMap()[commandStr]["done"]:
                    msgStr = f"invalid status={taskStatus} for dataset appending"
                    tmpLog.debug(msgStr)
                else:
                    timeNow = naive_utcnow()
                    # list of master dataset names
                    master_dataset_names = [datasetSpec.datasetName for datasetSpec in inMasterDatasetSpecList]
                    # get existing input datasets
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    sqlDS = "SELECT datasetName,datasetID,status,nFilesTobeUsed,nFilesUsed,masterID "
                    sqlDS += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
                    sqlDS += f"WHERE jediTaskID=:jediTaskID AND type IN ({INPUT_TYPES_var_str}) "
                    varMap.update(INPUT_TYPES_var_map)
                    self.cur.execute(sqlDS + comment, varMap)
                    resDS = self.cur.fetchall()
                    # check if existing datasets are available, and update status if necessary
                    sql_ex = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET status=:status WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                    existingDatasets = {}
                    for datasetName, dataset_id, datasetStatus, nFilesTobeUsed, nFilesUsed, masterID in resDS:
                        # only master datasets with remaining files
                        try:
                            if masterID is None and (nFilesTobeUsed - nFilesUsed > 0 or datasetStatus in JediDatasetSpec.statusToUpdateContents()):
                                # the dataset was removed before and then added to the container again
                                to_update_status = False
                                if datasetStatus == "removed" and datasetName in master_dataset_names:
                                    to_update_status = True
                                    var_map = {":status": "defined", ":jediTaskID": jediTaskID, ":datasetID": dataset_id}
                                # the dataset was removed
                                elif datasetStatus != "removed" and datasetName not in master_dataset_names:
                                    to_update_status = True
                                    var_map = {":status": "removed", ":jediTaskID": jediTaskID, ":datasetID": dataset_id}
                                # update status for removed/recovered dataset
                                if to_update_status:
                                    tmpLog.debug(f"""set status={var_map[":status"]} from {datasetStatus} for {datasetName}""")
                                    self.cur.execute(sql_ex + comment, var_map)
                                    datasetStatus = var_map[":status"]
                                if datasetStatus != "removed":
                                    goDefined = True
                                    if datasetStatus in JediDatasetSpec.statusToUpdateContents():
                                        refreshContents = True
                        except Exception:
                            pass
                        existingDatasets[datasetName] = datasetStatus
                    # insert datasets
                    sqlID = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
                    sqlID += JediDatasetSpec.bindValuesExpression()
                    sqlID += " RETURNING datasetID INTO :newDatasetID"
                    for datasetSpec in inMasterDatasetSpecList:
                        # skip existing datasets
                        if datasetSpec.datasetName in existingDatasets:
                            # check dataset status and remaining files
                            if existingDatasets[datasetSpec.datasetName] in JediDatasetSpec.statusToUpdateContents():
                                goDefined = True
                            continue
                        datasetSpec.creationTime = timeNow
                        datasetSpec.modificationTime = timeNow
                        varMap = datasetSpec.valuesMap(useSeq=True)
                        varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                        # insert dataset
                        tmpLog.debug(f"append {datasetSpec.datasetName}")
                        self.cur.execute(sqlID + comment, varMap)
                        val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                        datasetID = int(val)
                        masterID = datasetID
                        datasetSpec.datasetID = datasetID
                        # insert secondary datasets
                        for datasetSpec in inSecDatasetSpecList:
                            datasetSpec.creationTime = timeNow
                            datasetSpec.modificationTime = timeNow
                            datasetSpec.masterID = masterID
                            varMap = datasetSpec.valuesMap(useSeq=True)
                            varMap[":newDatasetID"] = self.cur.var(varNUMBER)
                            # insert dataset
                            self.cur.execute(sqlID + comment, varMap)
                            val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
                            datasetID = int(val)
                            datasetSpec.datasetID = datasetID
                        goDefined = True
                    # update task
                    sqlUT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
                    sqlUT += "SET status=:status,lockedBy=NULL,lockedTime=NULL,modificationtime=:updateTime,stateChangeTime=CURRENT_DATE "
                    sqlUT += "WHERE jediTaskID=:jediTaskID "
                    varMap = {}
                    varMap[":jediTaskID"] = jediTaskID
                    if goDefined:
                        # no new datasets
                        if inMasterDatasetSpecList == [] and not refreshContents:
                            # pass to JG
                            varMap[":status"] = "ready"
                        else:
                            # pass to ContentsFeeder
                            varMap[":status"] = "defined"
                    else:
                        # go to finalization since no datasets are appended
                        varMap[":status"] = "prepared"
                    # set old update time to trigger subsequent process
                    varMap[":updateTime"] = naive_utcnow() - datetime.timedelta(hours=6)
                    tmpLog.debug(f"set taskStatus={varMap[':status']}")
                    self.cur.execute(sqlUT + comment, varMap)
                    # add missing record_task_status_change and push_task_status_message updates
                    self.record_task_status_change(jediTaskID)
                    self.push_task_status_message(None, jediTaskID, varMap[":status"])

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # insert dataset to the JEDI datasets table
    def insertDataset_JEDI(self, datasetSpec):
        comment = " /* JediDBProxy.insertDataset_JEDI */"
        tmpLog = self.create_tagged_logger(comment)
        tmpLog.debug("start")
        try:
            # set attributes
            timeNow = naive_utcnow()
            datasetSpec.creationTime = timeNow
            datasetSpec.modificationTime = timeNow
            # sql
            sql = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Datasets ({JediDatasetSpec.columnNames()}) "
            sql += JediDatasetSpec.bindValuesExpression()
            sql += " RETURNING datasetID INTO :newDatasetID"
            varMap = datasetSpec.valuesMap(useSeq=True)
            varMap[":newDatasetID"] = self.cur.var(varNUMBER)
            # begin transaction
            self.conn.begin()
            # insert dataset
            self.cur.execute(sql + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            val = self.getvalue_corrector(self.cur.getvalue(varMap[":newDatasetID"]))
            return True, int(val)
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, None

    # update JEDI dataset
    def updateDataset_JEDI(self, datasetSpec, criteria, lockTask):
        comment = " /* JediDBProxy.updateDataset_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"datasetID={datasetSpec.datasetID}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = False, 0
        # no criteria
        if criteria == {}:
            tmpLog.error("no selection criteria")
            return failedRet
        # check criteria
        for tmpKey in criteria.keys():
            if not hasattr(datasetSpec, tmpKey):
                tmpLog.error(f"unknown attribute {tmpKey} is used in criteria")
                return failedRet
        try:
            # set attributes
            timeNow = naive_utcnow()
            datasetSpec.modificationTime = timeNow
            # values for UPDATE
            varMap = datasetSpec.valuesMap(useSeq=False, onlyChanged=True)
            # sql for update
            sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET {datasetSpec.bindUpdateChangesExpression()} WHERE "
            useAND = False
            for tmpKey, tmpVal in criteria.items():
                crKey = f":cr_{tmpKey}"
                if useAND:
                    sql += " AND"
                else:
                    useAND = True
                sql += f" {tmpKey}={crKey}"
                varMap[crKey] = tmpVal

            # sql for loc
            varMapLock = {}
            varMapLock[":jediTaskID"] = datasetSpec.jediTaskID
            sqlLock = f"SELECT 1 FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID FOR UPDATE"
            # begin transaction
            self.conn.begin()
            # lock task
            if lockTask:
                self.cur.execute(sqlLock + comment, varMapLock)
            # update dataset
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {nRows} rows")
            return True, nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # update JEDI dataset attributes
    def updateDatasetAttributes_JEDI(self, jediTaskID, datasetID, attributes):
        comment = " /* JediDBProxy.updateDatasetAttributes_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={datasetID}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = False
        try:
            # sql for update
            sql = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets SET "
            # values for UPDATE
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            for tmpKey, tmpVal in attributes.items():
                crKey = f":{tmpKey}"
                sql += f"{tmpKey}={crKey},"
                varMap[crKey] = tmpVal
            sql = sql[:-1]
            sql += " "
            sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # begin transaction
            self.conn.begin()
            # update dataset
            tmpLog.debug(sql + comment + str(varMap))
            self.cur.execute(sql + comment, varMap)
            # the number of updated rows
            nRows = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {nRows} rows")
            return True, nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get JEDI dataset attributes
    def getDatasetAttributes_JEDI(self, jediTaskID, datasetID, attributes):
        comment = " /* JediDBProxy.getDatasetAttributes_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={datasetID}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = {}
        try:
            # sql for get attributes
            sql = "SELECT "
            for tmpKey in attributes:
                sql += f"{tmpKey},"
            sql = sql[:-1] + " "
            sql += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sql += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # values for UPDATE
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make return
            retMap = {}
            if res is not None:
                for tmpIdx, tmpKey in enumerate(attributes):
                    retMap[tmpKey] = res[tmpIdx]
            tmpLog.debug(f"got {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get JEDI dataset attributes with map
    def getDatasetAttributesWithMap_JEDI(self, jediTaskID, criteria, attributes):
        comment = " /* JediDBProxy.getDatasetAttributesWithMap_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} criteria={str(criteria)}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = {}
        try:
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            # sql for get attributes
            sql = "SELECT "
            for tmpKey in attributes:
                sql += f"{tmpKey},"
            sql = sql[:-1] + " "
            sql += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sql += "WHERE jediTaskID=:jediTaskID "
            for crKey, crVal in criteria.items():
                sql += "AND {0}=:{0} ".format(crKey)
                varMap[f":{crKey}"] = crVal
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make return
            retMap = {}
            if res is not None:
                for tmpIdx, tmpKey in enumerate(attributes):
                    retMap[tmpKey] = res[tmpIdx]
            tmpLog.debug(f"got {str(retMap)}")
            return retMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get JEDI dataset with datasetID
    def getDatasetWithID_JEDI(self, jediTaskID, datasetID):
        comment = " /* JediDBProxy.getDatasetWithID_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetID={datasetID}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = False, None
        try:
            # sql
            sql = f"SELECT {JediDatasetSpec.columnNames()} "
            sql += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            varMap[":datasetID"] = datasetID
            # begin transaction
            self.conn.begin()
            # select
            self.cur.execute(sql + comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if res is not None:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(res)
            else:
                datasetSpec = None
            tmpLog.debug("done")
            return True, datasetSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get JEDI datasets with jediTaskID
    def getDatasetsWithJediTaskID_JEDI(self, jediTaskID, datasetTypes=None):
        comment = " /* JediDBProxy.getDatasetsWithJediTaskID_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} datasetTypes={datasetTypes}")
        tmpLog.debug("start")
        # return value for failure
        failedRet = False, None
        try:
            # sql
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sql = f"SELECT {JediDatasetSpec.columnNames()} "
            sql += f"FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID "
            if datasetTypes is not None:
                dstype_var_names_str, dstype_var_map = get_sql_IN_bind_variables(datasetTypes, prefix=":type_", value_as_suffix=True)
                sql += f"AND type IN ({dstype_var_names_str}) "
                varMap.update(dstype_var_map)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            self.cur.execute(sql + comment, varMap)
            tmpResList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # make file specs
            datasetSpecList = []
            for tmpRes in tmpResList:
                datasetSpec = JediDatasetSpec()
                datasetSpec.pack(tmpRes)
                datasetSpecList.append(datasetSpec)
            tmpLog.debug(f"done with {len(datasetSpecList)} datasets")
            return True, datasetSpecList
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # get jediTaskIDs with dataset attributes
    def get_task_ids_with_dataset_attributes(self, dataset_attributes: dict, only_active_tasks: bool = True) -> tuple[bool, list[int] | None]:
        """Get jediTaskIDs with dataset attributes.
        Args:
            dataset_attributes (dict): A dictionary of dataset attributes to filter on.
            only_active_tasks (bool): If True, only consider active tasks.
        Returns:
            tuple: A tuple containing a boolean indicating success, and a list of jediTaskIDs or None.
        """
        comment = " /* JediDBProxy.get_task_ids_with_dataset_attributes */"
        tmp_log = self.create_tagged_logger(comment, f"dataset_attributes={dataset_attributes} only_active_tasks={only_active_tasks}")
        tmp_log.debug("start")
        try:
            # sql
            sql = (
                f"SELECT DISTINCT  tabT.jediTaskID FROM {panda_config.schemaJEDI}.JEDI_Tasks tabT,{panda_config.schemaJEDI}.JEDI_AUX_Status_MinTaskID tabA,{panda_config.schemaJEDI}.JEDI_Datasets tabD "
                "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID AND tabT.jediTaskID=tabD.jediTaskID "
            )
            for k, v in dataset_attributes.items():
                sql += f"AND tabD.{k}=:{k} "
            var_map = copy.copy(dataset_attributes)
            if only_active_tasks:
                task_var_names_str, task_var_map = get_sql_IN_bind_variables(
                    JediTaskSpec.statusToRejectExtChange(), prefix=":task_status_", value_as_suffix=True
                )
                sql += f"AND tabT.status NOT IN ({task_var_names_str}) "
                var_map.update(task_var_map)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # select
            print(sql, str(var_map))
            self.cur.execute(sql + comment, var_map)
            tmp_res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            task_id_list = [row[0] for row in tmp_res]
            tmp_log.debug(f"done with {len(task_id_list)} tasks")
            return True, task_id_list
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return False, None

    # extend lifetime of sandbox file
    def extendSandboxLifetime_JEDI(self, jedi_taskid, file_name):
        comment = " /* JediDBProxy.extendSandboxLifetime_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_taskid}")
        try:
            self.conn.begin()
            retVal = False
            # sql to update
            sqlC = f"UPDATE {panda_config.schemaMETA}.userCacheUsage SET creationTime=CURRENT_DATE WHERE fileName=:fileName "
            varMap = {}
            varMap[":fileName"] = file_name
            self.cur.execute(sqlC + comment, varMap)
            nRows = self.cur.rowcount
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done {file_name} with {nRows}")
            # return
            return nRows
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # lock process
    def lockProcess_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, forceOption, timeLimit):
        comment = " /* JediDBProxy.lockProcess_JEDI */"
        # defaults
        if cloud is None:
            cloud = "default"
        if workqueue_id is None:
            workqueue_id = 0
        if resource_name is None:
            resource_name = "default"
        if component is None:
            component = "default"
        tmpLog = self.create_tagged_logger(
            comment, f"vo={vo} label={prodSourceLabel} cloud={cloud} queue={workqueue_id} resource_type={resource_name} component={component} pid={pid}"
        )
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to check
            sqlCT = "SELECT lockedBy "
            sqlCT += f"FROM {panda_config.schemaJEDI}.JEDI_Process_Lock "
            sqlCT += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            sqlCT += "AND resource_type=:resource_name AND component=:component "
            sqlCT += "AND lockedTime>:timeLimit "
            sqlCT += "FOR UPDATE"
            # sql to delete
            sqlCD = f"DELETE FROM {panda_config.schemaJEDI}.JEDI_Process_Lock "
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            sqlCD += "AND resource_type=:resource_name AND component=:component "
            # sql to insert
            sqlFR = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Process_Lock "
            sqlFR += "(vo, prodSourceLabel, cloud, workqueue_id, resource_type, component, lockedBy, lockedTime) "
            sqlFR += "VALUES(:vo, :prodSourceLabel, :cloud, :workqueue_id, :resource_name, :component, :lockedBy, CURRENT_DATE) "
            # start transaction
            self.conn.begin()
            # check
            if not forceOption:
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=timeLimit)
                self.cur.execute(sqlCT + comment, varMap)
                resCT = self.cur.fetchone()
            else:
                resCT = None
            if resCT is not None:
                tmpLog.debug(f"skipped, locked by {resCT[0]}")
            else:
                # delete
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                self.cur.execute(sqlCD + comment, varMap)
                # insert
                varMap = {}
                varMap[":vo"] = vo
                varMap[":prodSourceLabel"] = prodSourceLabel
                varMap[":cloud"] = cloud
                varMap[":workqueue_id"] = workqueue_id
                varMap[":resource_name"] = resource_name
                varMap[":component"] = component
                varMap[":lockedBy"] = pid
                self.cur.execute(sqlFR + comment, varMap)
                tmpLog.debug("successfully locked")
                retVal = True
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # unlock process
    def unlockProcess_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid):
        comment = " /* JediDBProxy.unlockProcess_JEDI */"
        # defaults
        if cloud is None:
            cloud = "default"
        if workqueue_id is None:
            workqueue_id = 0
        if resource_name is None:
            resource_name = "default"
        if component is None:
            component = "default"
        tmpLog = self.create_tagged_logger(
            comment, f"vo={vo} label={prodSourceLabel} cloud={cloud} queue={workqueue_id} resource_type={resource_name} component={component} pid={pid}"
        )
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to delete
            sqlCD = f"DELETE FROM {panda_config.schemaJEDI}.JEDI_Process_Lock "
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud "
            sqlCD += "AND workqueue_id=:workqueue_id AND lockedBy=:lockedBy "
            sqlCD += "AND resource_type=:resource_name AND component=:component "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":cloud"] = cloud
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            varMap[":component"] = component
            varMap[":lockedBy"] = pid
            self.cur.execute(sqlCD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            retVal = True
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # unlock process with PID
    def unlockProcessWithPID_JEDI(self, vo, prodSourceLabel, workqueue_id, resource_name, pid, useBase):
        comment = " /* JediDBProxy.unlockProcessWithPID_JEDI */"
        tmpLog = self.create_tagged_logger(
            comment, f"vo={vo} label={prodSourceLabel} queue={workqueue_id} resource_type={resource_name} pid={pid} useBase={useBase}"
        )
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to delete
            sqlCD = f"DELETE FROM {panda_config.schemaJEDI}.JEDI_Process_Lock "
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
            sqlCD += "AND workqueue_id=:workqueue_id "
            sqlCD += "AND resource_name=:resource_name "
            if useBase:
                sqlCD += "AND lockedBy LIKE :lockedBy "
            else:
                sqlCD += "AND lockedBy=:lockedBy "
            # start transaction
            self.conn.begin()
            # delete
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            if useBase:
                varMap[":lockedBy"] = pid + "%"
            else:
                varMap[":lockedBy"] = pid
            self.cur.execute(sqlCD + comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug("done")
            retVal = True
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal

    # check process lock
    def checkProcessLock_JEDI(self, vo, prodSourceLabel, cloud, workqueue_id, resource_name, component, pid, checkBase):
        comment = " /* JediDBProxy.checkProcessLock_JEDI */"
        # defaults
        if cloud is None:
            cloud = "default"
        if workqueue_id is None:
            workqueue_id = 0
        if resource_name is None:
            resource_name = "default"
        if component is None:
            component = "default"
        tmpLog = self.create_tagged_logger(
            comment, f"vo={vo} label={prodSourceLabel} cloud={cloud} queue={workqueue_id} resource_type={resource_name} component={component} pid={pid}"
        )
        tmpLog.debug("start")
        try:
            retVal = False
            # sql to check
            sqlCT = "SELECT lockedBy "
            sqlCT += f"FROM {panda_config.schemaJEDI}.JEDI_Process_Lock "
            sqlCT += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND cloud=:cloud AND workqueue_id=:workqueue_id "
            sqlCT += "AND resource_type=:resource_name AND component=:component "
            sqlCT += "AND lockedTime>:timeLimit "
            # start transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[":vo"] = vo
            varMap[":prodSourceLabel"] = prodSourceLabel
            varMap[":cloud"] = cloud
            varMap[":workqueue_id"] = workqueue_id
            varMap[":resource_name"] = resource_name
            varMap[":component"] = component
            varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=5)
            self.cur.execute(sqlCT + comment, varMap)
            resCT = self.cur.fetchone()
            if resCT is not None:
                (lockedBy,) = resCT
                if checkBase:
                    # check only base part
                    if not lockedBy.startswith(pid):
                        retVal = True
                else:
                    # check whole string
                    if lockedBy != pid:
                        retVal = True
                if retVal is True:
                    tmpLog.debug(f"skipped locked by {lockedBy}")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            tmpLog.debug(f"done with {retVal}")
            return retVal
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return retVal
