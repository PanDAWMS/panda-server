import datetime
import json
import re
import sys
from typing import Dict

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule
from pandaserver.taskbuffer.JobSpec import JobSpec, get_task_queued_time


# Module class to define metrics related methods
class MetricsModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # set job or task metrics
    def set_workload_metrics(self, jedi_task_id: int, panda_id: int | None, metrics: dict, use_commit: bool = True) -> bool:
        """
        Set job or task metrics

        :param jedi_task_id: jediTaskID
        :param panda_id: PandaID. None to set task
        :param metrics: metrics data
        :param use_commit: use commit
        :return: True if success
        """
        comment = " /* DBProxy.set_workload_metrics */"
        if panda_id is not None:
            tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id} PandaID={panda_id}")
        else:
            tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        try:
            if panda_id is not None:
                table_name = "Job_Metrics"
                var_map = {":jediTaskID": jedi_task_id, ":PandaID": panda_id}
            else:
                table_name = "Task_Metrics"
                var_map = {":jediTaskID": jedi_task_id}
            # check if data is already there
            sql_check = f"SELECT data FROM {panda_config.schemaPANDA}.{table_name} WHERE jediTaskID=:jediTaskID "
            if panda_id is not None:
                sql_check += "AND PandaID=:PandaID "
            # insert data
            sql_insert = f"INSERT INTO {panda_config.schemaPANDA}.{table_name} "
            if panda_id is not None:
                sql_insert += "(jediTaskID,PandaID,creationTime,modificationTime,data) VALUES(:jediTaskID,:PandaID,CURRENT_DATE,CURRENT_DATE,:data) "
            else:
                sql_insert += "(jediTaskID,creationTime,modificationTime,data) VALUES(:jediTaskID,CURRENT_DATE,CURRENT_DATE,:data) "
            # update data
            sql_update = f"UPDATE {panda_config.schemaPANDA}.{table_name} SET modificationTime=CURRENT_DATE,data=:data WHERE jediTaskID=:jediTaskID "
            if panda_id is not None:
                sql_update += "AND PandaID=:PandaID "
            # start transaction
            if use_commit:
                self.conn.begin()
            # check if data is already there
            self.cur.execute(sql_check + comment, var_map)
            # read data
            tmp_data = None
            for (clob_data,) in self.cur:
                try:
                    tmp_data = clob_data.read()
                except AttributeError:
                    tmp_data = str(clob_data)
                break
            if not tmp_data:
                # insert new data
                var_map[":data"] = json.dumps(metrics, cls=CoreUtils.NonJsonObjectEncoder)
                self.cur.execute(sql_insert + comment, var_map)
                tmp_log.debug("inserted")
            else:
                # update existing data
                tmp_data = json.loads(tmp_data, object_hook=CoreUtils.as_python_object)
                tmp_data.update(metrics)
                var_map[":data"] = json.dumps(tmp_data, cls=CoreUtils.NonJsonObjectEncoder)
                self.cur.execute(sql_update + comment, var_map)
                tmp_log.debug("updated")
            if use_commit:
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

    # get job or task metrics
    def get_workload_metrics(self, jedi_task_id: int, panda_id: int = None) -> tuple[bool, dict | None]:
        """
        Get job metrics or task metrics

        :param jedi_task_id: jediTaskID
        :param panda_id: PandaID. None to get task metrics
        :return: (False, None) if failed, otherwise (True, metrics)
        """
        comment = " /* DBProxy.get_workload_metrics */"
        if panda_id is not None:
            tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id} PandaID={panda_id}")
        else:
            tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        try:
            if panda_id is not None:
                table_name = "Job_Metrics"
                var_map = {":jediTaskID": jedi_task_id, ":PandaID": panda_id}
            else:
                table_name = "Task_Metrics"
                var_map = {":jediTaskID": jedi_task_id}
            # get data
            sql_get = f"SELECT data FROM {panda_config.schemaPANDA}.{table_name} WHERE jediTaskID=:jediTaskID "
            if panda_id is not None:
                sql_get += "AND PandaID=:PandaID "
            self.cur.execute(sql_get + comment, var_map)
            # read data
            metrics = None
            for (clob_data,) in self.cur:
                try:
                    metrics = clob_data.read()
                except AttributeError:
                    metrics = str(clob_data)
                break
            if metrics is not None:
                metrics = json.loads(metrics, object_hook=CoreUtils.as_python_object)
                tmp_log.debug(f"got {sys.getsizeof(metrics)} bytes")
            else:
                tmp_log.debug("no data")
            return True, metrics
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return False, None

    # get jobs' metrics in a task
    def get_jobs_metrics_in_task(self, jedi_task_id: int) -> tuple[bool, list | None]:
        """
        Get metrics of jobs in a task

        :param jedi_task_id: jediTaskID
        :return: (False, None) if failed, otherwise (True, list of [PandaID, metrics])
        """
        comment = " /* DBProxy.get_jobs_metrics_in_task */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        try:
            var_map = {":jediTaskID": jedi_task_id}
            # get data
            sql_get = f"SELECT PandaID,data FROM {panda_config.schemaPANDA}.Job_Metrics WHERE jediTaskID=:jediTaskID "
            self.cur.execute(sql_get + comment, var_map)
            # read data
            metrics_list = []
            for panda_id, clob_data in self.cur:
                try:
                    tmp_data = clob_data.read()
                except AttributeError:
                    tmp_data = str(clob_data)
                metrics_list.append([panda_id, json.loads(tmp_data, object_hook=CoreUtils.as_python_object)])
            tmp_log.debug(f"got metrics for {len(metrics_list)} jobs")
            return True, metrics_list
        except Exception:
            # error
            self.dump_error_message(tmp_log)
            return False, None

    # update task queued and activated times
    def update_task_queued_activated_times(self, jedi_task_id: int) -> None:
        """
        Update task queued time depending on the number of inputs to be processed. Update queued and activated times if they were None and inputs become available.
        Set None if no input is available and record the queuing duration to task metrics.

        :param jedi_task_id: task's jediTaskID
        """
        comment = " /* DBProxy.update_task_queued_activated_times */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        # check if the task is queued
        sql_check = (
            f"SELECT status,oldStatus,queuedTime,activatedTime,currentPriority,gshare FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
        )
        var_map = {":jediTaskID": jedi_task_id}
        self.cur.execute(sql_check + comment, var_map)
        res = self.cur.fetchone()
        if not res:
            # not found
            tmp_log.debug("task not found")
            return
        (task_status, task_old_status, queued_time, activated_time, current_priority, global_share) = res
        has_input = False
        active_status_list = ("ready", "running", "scouting", "scouted")
        if task_status in active_status_list or (task_old_status in active_status_list and task_status == "pending"):
            # check if the task has unprocessed inputs
            sql_input = (
                f"SELECT 1 FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) AND masterID IS NULL "
                "AND nFilesToBeUsed>0 AND nFilesToBeUSed>nFilesUsed "
            )
            var_map = {":jediTaskID": jedi_task_id, ":type1": "input", ":type2": "pseudo_input"}
            self.cur.execute(sql_input + comment, var_map)
            res = self.cur.fetchone()
            has_input = res is not None
            # set queued time if it was None and inputs are available
            if has_input:
                if queued_time is None:
                    # update queued time
                    tmp_log.debug(f"set queued time when task is {task_status}")
                    sql_update = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET queuedTime=CURRENT_DATE WHERE jediTaskID=:jediTaskID AND queuedTime IS NULL "
                    var_map = {":jediTaskID": jedi_task_id}
                    self.cur.execute(sql_update + comment, var_map)
                    n_row = self.cur.rowcount
                    if n_row > 0 and activated_time is None:
                        # set activated time
                        tmp_log.debug(f"set activated time when task is {task_status}")
                        sql_update = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET activatedTime=CURRENT_DATE WHERE jediTaskID=:jediTaskID AND activatedTime IS NULL "
                        var_map = {":jediTaskID": jedi_task_id}
                        self.cur.execute(sql_update + comment, var_map)
                else:
                    tmp_log.debug(f"keep current queued time {queued_time.strftime('%Y-%m-%d %H:%M:%S')}")
        # record queuing duration since the task has no more input to process
        if queued_time is not None and not has_input:
            # unset queued time
            tmp_log.debug(f"unset queued time and record duration when task is {task_status}")
            sql_update = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET queuedTime=NULL WHERE jediTaskID=:jediTaskID AND queuedTime IS NOT NULL "
            var_map = {":jediTaskID": jedi_task_id}
            self.cur.execute(sql_update + comment, var_map)
            # get task metrics dict
            tmp_success, task_metrics = self.get_workload_metrics(jedi_task_id, None)
            if not tmp_success:
                err_str = f"failed to get task metrics for jediTaskId={jedi_task_id}"
                tmp_log.error(err_str)
                return
            # new
            if not task_metrics:
                task_metrics = {}
            # add duration
            task_metrics.setdefault("queuingPeriods", [])
            if len(task_metrics["queuingPeriods"]) < 10000:
                task_metrics["queuingPeriods"].append({"start": queued_time, "end": naive_utcnow(), "status": task_status})
                tmp_success = self.set_workload_metrics(jedi_task_id, None, task_metrics, False)
                if not tmp_success:
                    err_str = f"failed to update task metrics for jediTaskId={jedi_task_id}"
                    tmp_log.error(err_str)
                    return
            else:
                tmp_log.debug("skipped since queuing period list is too long")
        tmp_log.debug(f"done in {task_status}")

    # record job queuing period
    def record_job_queuing_period(self, panda_id: int, job_spec: JobSpec = None) -> bool | None:
        """
        Record queuing period in job metrics. Skip if job.jobMetrics doesn't contain task queued time

        :param panda_id: Job's PandaID
        :param job_spec: job spec. None to get it from the database
        :return: True if success. False if failed. None if skipped
        """
        comment = " /* DBProxy.record_job_queuing_period */"
        tmp_log = self.create_tagged_logger(comment, f"PandaID={panda_id}")
        tmp_log.debug(f"start with job spec: {job_spec is None}")
        # get task queued time
        if job_spec is None:
            sql_check = f"SELECT jediTaskID,jobStatus,specialHandling FROM {panda_config.schemaPANDA}.jobsActive4 WHERE PandaID=:PandaID "
            var_map = {":PandaID": panda_id}
            self.cur.execute(sql_check + comment, var_map)
            res = self.cur.fetchone()
            if not res:
                # not found
                tmp_log.debug("job not found")
                return None
            jedi_task_id, job_status, tmp_str = res
        else:
            jedi_task_id = job_spec.jediTaskID
            job_status = job_spec.jobStatus
            tmp_str = job_spec.specialHandling
        task_queued_time = get_task_queued_time(tmp_str)
        # record queuing duration
        if jedi_task_id and task_queued_time:
            tmp_log.debug(f"to record queuing period")
            # get job metrics dict
            tmp_success, job_metrics = self.get_workload_metrics(jedi_task_id, panda_id)
            if not tmp_success:
                err_str = "Failed to get job metrics "
                tmp_log.error(err_str)
                return False
            # new
            if not job_metrics:
                job_metrics = {}
            # add duration
            if "queuingPeriod" in job_metrics:
                tmp_log.debug("skipped since queuing period already exists")
                return None
            else:
                job_metrics["queuingPeriod"] = {
                    "start": task_queued_time,
                    "end": naive_utcnow(),
                    "status": job_status,
                }
                tmp_success = self.set_workload_metrics(jedi_task_id, panda_id, job_metrics, False)
                if not tmp_success:
                    err_str = "Failed to update job metrics"
                    tmp_log.error(err_str)
                    return False
            tmp_log.debug(f"done in {job_status}")
            return True
        else:
            tmp_log.debug(f"skipped as jediTaskID={jedi_task_id} taskQueuedTime={task_queued_time}")
            return None

    # unset task active period
    def unset_task_activated_time(self, jedi_task_id: int, task_status: str = None) -> bool | None:
        """
        Unset activated time and record active period in task metrics

        :param jedi_task_id: task's JediTaskID
        :param task_status: task status. None to get it from the database
        :return: True if success. False if failed. None if skipped
        """
        frozen_status_list = ("done", "finished", "failed", "broken", "paused", "exhausted")
        if task_status is not None and task_status not in frozen_status_list:
            return None
        comment = " /* DBProxy.record_task_active_period */"
        tmp_log = self.create_tagged_logger(comment, f"JediTaskID={jedi_task_id}")
        tmp_log.debug(f"start")
        # get activated time
        sql_check = f"SELECT status,activatedTime FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
        var_map = {":jediTaskID": jedi_task_id}
        self.cur.execute(sql_check + comment, var_map)
        res = self.cur.fetchone()
        if not res:
            # not found
            tmp_log.debug("task not found")
            return None
        task_status, activated_time = res
        # record active duration
        if task_status in frozen_status_list and activated_time:
            tmp_log.debug(f"to record active period when task is {task_status}")
            # get task metrics dict
            tmp_success, task_metrics = self.get_workload_metrics(jedi_task_id, None)
            if not tmp_success:
                err_str = "Failed to get task metrics "
                tmp_log.error(err_str)
                return False
            # new
            if not task_metrics:
                task_metrics = {}
            # add duration
            task_metrics.setdefault("activePeriod", [])
            task_metrics["activePeriod"].append(
                {
                    "start": activated_time,
                    "end": naive_utcnow(),
                    "status": task_status,
                }
            )
            tmp_success = self.set_workload_metrics(jedi_task_id, None, task_metrics, False)
            if not tmp_success:
                err_str = "Failed to update task metrics"
                tmp_log.error(err_str)
                return False
            # unset activated time
            tmp_log.debug(f"unset activated time")
            sql_update = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET activatedTime=NULL WHERE jediTaskID=:jediTaskID AND activatedTime IS NOT NULL "
            var_map = {":jediTaskID": jedi_task_id}
            self.cur.execute(sql_update + comment, var_map)
        tmp_log.debug(f"done in {task_status}")
        return True

    # check failure count due to corrupted files
    def checkFailureCountWithCorruptedFiles(self, jediTaskID, pandaID):
        comment = " /* DBProxy.checkFailureCountWithCorruptedFiles */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} pandaID={pandaID}")
        # sql to failure counts
        sqlBD = "SELECT f2.lfn,COUNT(*) FROM ATLAS_PANDA.filesTable4 f1, ATLAS_PANDA.filesTable4 f2 "
        sqlBD += "WHERE f1.PandaID=:PandaID AND f1.type=:type AND f1.status=:status "
        sqlBD += "AND f2.lfn=f1.lfn AND f2.type=:type AND f2.status=:status AND f2.jediTaskID=:jediTaskID "
        sqlBD += "GROUP BY f2.lfn "
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":PandaID"] = pandaID
        varMap[":status"] = "corrupted"
        varMap[":type"] = "zipinput"
        self.cur.execute(sqlBD + comment, varMap)
        resBD = self.cur.fetchall()
        tooMany = False
        for lfn, nFailed in resBD:
            tmp_log.debug(f"{nFailed} failures with {lfn}")
            if nFailed >= 3:
                tooMany = True
        tmp_log.debug(f"too many failures : {tooMany}")
        return tooMany

    # calculate failure metrics, such as single failure rate and failed HEPScore, for a task
    def get_task_failure_metrics(self, task_id, use_commit=True):
        comment = " /* JediDBProxy.get_task_failure_metrics */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={task_id}")
        tmp_log.debug("start")
        try:
            # start transaction
            if use_commit:
                self.conn.begin()
            # sql to get metrics
            sql = (
                f"SELECT SUM(is_finished),SUM(is_failed),SUM(HS06SEC*is_finished),SUM(HS06SEC*is_failed) "
                f"FROM ("
                f"SELECT PandaID, HS06SEC, CASE WHEN jobStatus='finished' THEN 1 ELSE 0 END is_finished, "
                f"CASE WHEN jobStatus='failed' THEN 1 ELSE 0 END is_failed "
                f"FROM {panda_config.schemaPANDA}.jobsArchived4 "
                f"WHERE jediTaskID=:jediTaskID AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
                f"UNION "
                f"SELECT PandaID, HS06SEC, CASE WHEN jobStatus='finished' THEN 1 ELSE 0 END is_finished, "
                f"CASE WHEN jobStatus='failed' THEN 1 ELSE 0 END is_failed "
                f"FROM {panda_config.schemaPANDAARCH}.jobsArchived "
                f"WHERE jediTaskID=:jediTaskID AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
                f")"
            )
            var_map = {
                ":jediTaskID": task_id,
                ":prodSourceLabel1": "managed",
                ":prodSourceLabel2": "user",
            }
            self.cur.execute(sql + comment, var_map)
            num_finished, num_failed, good_hep_score_sec, bad_hep_score_sec = self.cur.fetchone()
            ret_dict = {
                "num_failed": num_failed,
                "single_failure_rate": (
                    round(num_failed / (num_finished + num_failed), 3)
                    if num_finished is not None and num_failed is not None and num_finished + num_failed
                    else None
                ),
                "failed_hep_score_hour": int(bad_hep_score_sec / 60 / 60) if bad_hep_score_sec is not None else None,
                "failed_hep_score_ratio": (
                    round(bad_hep_score_sec / (good_hep_score_sec + bad_hep_score_sec), 3)
                    if good_hep_score_sec is not None and bad_hep_score_sec is not None and good_hep_score_sec + bad_hep_score_sec
                    else None
                ),
            }
            # commit
            if use_commit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"got {ret_dict}")
            return ret_dict
        except Exception:
            # roll back
            if use_commit:
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get averaged disk IO
    def getAvgDiskIO_JEDI(self):
        comment = " /* JediDBProxy.getAvgDiskIO_JEDI */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            # sql
            sql = f"SELECT sum(prorated_diskio_avg * njobs) / sum(njobs), computingSite FROM {panda_config.schemaPANDA}.JOBS_SHARE_STATS "
            sql += "WHERE jobStatus=:jobStatus GROUP BY computingSite "
            var_map = dict()
            var_map[":jobStatus"] = "running"
            # begin transaction
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            resFL = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            ret_map = dict()
            for avg, computing_site in resFL:
                if avg:
                    avg = float(avg)
                ret_map[computing_site] = avg
            tmp_log.debug("done")
            return ret_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return {}

    # get carbon footprint for a task, the level has to be 'regional' or 'global'. If misspelled, it defaults to 'global'
    def get_task_carbon_footprint(self, jedi_task_id, level):
        comment = " /* JediDBProxy.get_task_carbon_footprint */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id} n_files={level}")
        tmp_log.debug("start")

        if level == "regional":
            gco2_column = "GCO2_REGIONAL"
        else:
            gco2_column = "GCO2_GLOBAL"

        try:
            sql = (
                "SELECT jobstatus, SUM(sum_gco2) FROM ( "
                "  SELECT jobstatus, SUM({gco2_column}) sum_gco2 FROM {active_schema}.jobsarchived4 "
                "  WHERE jeditaskid =:jeditaskid "
                "  GROUP BY jobstatus "
                "  UNION "
                "  SELECT jobstatus, SUM({gco2_column}) sum_gco2 FROM {archive_schema}.jobsarchived "
                "  WHERE jeditaskid =:jeditaskid "
                "  GROUP BY jobstatus)"
                "GROUP BY jobstatus".format(gco2_column=gco2_column, active_schema=panda_config.schemaJEDI, archive_schema=panda_config.schemaPANDAARCH)
            )
            var_map = {":jeditaskid": jedi_task_id}

            # start transaction
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            results = self.cur.fetchall()

            footprint = {"total": 0}
            data = False
            for job_status, g_co2 in results:
                if not g_co2:
                    g_co2 = 0
                else:
                    data = True
                footprint[job_status] = g_co2
                footprint["total"] += g_co2

            # commit
            if not self._commit():
                raise RuntimeError("Commit error")

            tmp_log.debug(f"done: {footprint}")

            if not data:
                return None

            return footprint
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

    # get job statistics with work queue
    def getJobStatisticsWithWorkQueue_JEDI(self, vo, prodSourceLabel, minPriority=None, cloud=None):
        comment = " /* DBProxy.getJobStatisticsWithWorkQueue_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prodSourceLabel} cloud={cloud}")
        tmpLog.debug(f"start minPriority={minPriority}")
        sql0 = "SELECT computingSite,cloud,jobStatus,workQueue_ID,COUNT(*) FROM %s "
        sql0 += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
        if cloud is not None:
            sql0 += "AND cloud=:cloud "
        tmpPrioMap = {}
        if minPriority is not None:
            sql0 += "AND currentPriority>=:minPriority "
            tmpPrioMap[":minPriority"] = minPriority
        sql0 += "GROUP BY computingSite,cloud,prodSourceLabel,jobStatus,workQueue_ID "
        sqlMV = sql0
        sqlMV = re.sub("COUNT\(\*\)", "SUM(num_of_jobs)", sqlMV)
        sqlMV = re.sub("SELECT ", "SELECT /*+ RESULT_CACHE */ ", sqlMV)
        tables = [f"{panda_config.schemaPANDA}.jobsActive4", f"{panda_config.schemaPANDA}.jobsDefined4"]
        if minPriority is not None:
            # read the number of running jobs with prio<=MIN
            tables.append(f"{panda_config.schemaPANDA}.jobsActive4")
            sqlMVforRun = re.sub("currentPriority>=", "currentPriority<=", sqlMV)
        varMap = {}
        varMap[":vo"] = vo
        varMap[":prodSourceLabel"] = prodSourceLabel
        if cloud is not None:
            varMap[":cloud"] = cloud
        for tmpPrio in tmpPrioMap.keys():
            varMap[tmpPrio] = tmpPrioMap[tmpPrio]
        returnMap = {}
        try:
            iActive = 0
            for table in tables:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                useRunning = None
                if table == f"{panda_config.schemaPANDA}.jobsActive4":
                    mvTableName = f"{panda_config.schemaPANDA}.MV_JOBSACTIVE4_STATS"
                    # first count non-running and then running if minPriority is specified
                    if minPriority is not None:
                        if iActive == 0:
                            useRunning = False
                        else:
                            useRunning = True
                        iActive += 1
                    if useRunning in [None, False]:
                        sqlExeTmp = (sqlMV + comment) % mvTableName
                    else:
                        sqlExeTmp = (sqlMVforRun + comment) % mvTableName
                else:
                    sqlExeTmp = (sql0 + comment) % table
                self.cur.execute(sqlExeTmp, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
                # create map
                for computingSite, cloud, jobStatus, workQueue_ID, nCount in res:
                    # count the number of non-running with prio>=MIN
                    if useRunning is True and jobStatus != "running":
                        continue
                    # count the number of running with prio<=MIN
                    if useRunning is False and jobStatus == "running":
                        continue
                    # add site
                    if computingSite not in returnMap:
                        returnMap[computingSite] = {}
                    # add workQueue
                    if workQueue_ID not in returnMap[computingSite]:
                        returnMap[computingSite][workQueue_ID] = {}
                    # add jobstatus
                    if jobStatus not in returnMap[computingSite][workQueue_ID]:
                        returnMap[computingSite][workQueue_ID][jobStatus] = 0
                    # add
                    returnMap[computingSite][workQueue_ID][jobStatus] += nCount
            # return
            tmpLog.debug("done")
            return True, returnMap
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, {}

    # get core statistics with VO and prodSourceLabel
    def get_core_statistics(self, vo: str, prod_source_label: str) -> [bool, dict]:
        comment = " /* DBProxy.get_core_statistics */"
        tmpLog = self.create_tagged_logger(comment, f"vo={vo} label={prod_source_label}")
        tmpLog.debug("start")
        sql0 = f"SELECT /*+ RESULT_CACHE */ computingSite,jobStatus,SUM(num_of_cores) FROM {panda_config.schemaPANDA}.MV_JOBSACTIVE4_STATS "
        sql0 += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel "
        sql0 += "GROUP BY computingSite,cloud,prodSourceLabel,jobStatus "
        var_map = {":vo": vo, ":prodSourceLabel": prod_source_label}
        return_map = {}
        try:
            self.conn.begin()
            # select
            self.cur.arraysize = 10000
            self.cur.execute(sql0 + comment, var_map)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # create map
            for computing_site, job_status, n_core in res:
                # add site
                return_map.setdefault(computing_site, {})
                # add status
                return_map[computing_site].setdefault(job_status, 0)
                # add num cores
                return_map[computing_site][job_status] += n_core
            # return
            tmpLog.debug("done")
            return True, return_map
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False, {}

    # get job statistics by global share
    def getJobStatisticsByGlobalShare(self, vo, exclude_rwq):
        """
        :param vo: Virtual Organization
        :param exclude_rwq: True/False. Indicates whether we want to indicate special workqueues from the statistics
        """
        comment = " /* DBProxy.getJobStatisticsByGlobalShare */"
        tmpLog = self.create_tagged_logger(comment, f" vo={vo}")
        tmpLog.debug("start")

        # define the var map of query parameters
        var_map = {":vo": vo}

        # sql to query on pre-cached job statistics tables (JOBS_SHARE_STATS and JOBSDEFINED_SHARE_STATS)
        sql_jt = """
               SELECT /*+ RESULT_CACHE */ computingSite, jobStatus, gShare, SUM(njobs) FROM %s
               WHERE vo=:vo
               """

        if exclude_rwq:
            sql_jt += f"""
               AND workqueue_id NOT IN
               (SELECT queue_id FROM {panda_config.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource')
               """

        sql_jt += """
               GROUP BY computingSite, jobStatus, gshare
               """

        tables = [f"{panda_config.schemaPANDA}.JOBS_SHARE_STATS", f"{panda_config.schemaPANDA}.JOBSDEFINED_SHARE_STATS"]

        return_map = {}
        try:
            for table in tables:
                self.cur.arraysize = 10000
                sql_exe = (sql_jt + comment) % table
                self.cur.execute(sql_exe, var_map)
                res = self.cur.fetchall()

                # create map
                for panda_site, status, gshare, n_count in res:
                    # add site
                    return_map.setdefault(panda_site, {})
                    # add global share
                    return_map[panda_site].setdefault(gshare, {})
                    # add job status
                    return_map[panda_site][gshare].setdefault(status, 0)
                    # increase count
                    return_map[panda_site][gshare][status] += n_count

            tmpLog.debug("done")
            return True, return_map
        except Exception:
            self.dump_error_message(tmpLog)
            return False, {}

    def getJobStatisticsByResourceType(self, workqueue):
        """
        This function will return the job statistics for a particular workqueue, broken down by resource type
        (SCORE, MCORE, etc.)
        :param workqueue: workqueue object
        """
        comment = " /* DBProxy.getJobStatisticsByResourceType */"
        tmpLog = self.create_tagged_logger(comment, f"workqueue={workqueue}")
        tmpLog.debug("start")

        # define the var map of query parameters
        var_map = {":vo": workqueue.VO}

        # sql to query on pre-cached job statistics tables (JOBS_SHARE_STATS and JOBSDEFINED_SHARE_STATS)
        sql_jt = "SELECT /*+ RESULT_CACHE */ jobstatus, resource_type, SUM(njobs) FROM %s WHERE vo=:vo "

        if workqueue.is_global_share:
            sql_jt += "AND gshare=:gshare "
            sql_jt += f"AND workqueue_id NOT IN (SELECT queue_id FROM {panda_config.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource') "
            var_map[":gshare"] = workqueue.queue_name
        else:
            sql_jt += "AND workqueue_id=:workqueue_id "
            var_map[":workqueue_id"] = workqueue.queue_id

        sql_jt += "GROUP BY jobstatus, resource_type "

        tables = [f"{panda_config.schemaPANDA}.JOBS_SHARE_STATS", f"{panda_config.schemaPANDA}.JOBSDEFINED_SHARE_STATS"]

        return_map = {}
        try:
            for table in tables:
                self.cur.arraysize = 10000
                sql_exe = (sql_jt + comment) % table
                self.cur.execute(sql_exe, var_map)
                res = self.cur.fetchall()

                # create map
                for status, resource_type, n_count in res:
                    return_map.setdefault(status, {})
                    return_map[status][resource_type] = n_count

            tmpLog.debug("done")
            return True, return_map
        except Exception:
            self.dump_error_message(tmpLog)
            return False, {}

    def getJobStatisticsByResourceTypeSite(self, workqueue):
        """
        This function will return the job statistics per site for a particular workqueue, broken down by resource type
        (SCORE, MCORE, etc.)
        :param workqueue: workqueue object
        """
        comment = " /* DBProxy.getJobStatisticsByResourceTypeSite */"
        tmpLog = self.create_tagged_logger(comment, f"workqueue={workqueue}")
        tmpLog.debug("start")

        # define the var map of query parameters
        var_map = {":vo": workqueue.VO}

        # sql to query on pre-cached job statistics tables (JOBS_SHARE_STATS and JOBSDEFINED_SHARE_STATS)
        sql_jt = "SELECT /*+ RESULT_CACHE */ jobstatus, resource_type, computingSite, SUM(njobs) FROM %s WHERE vo=:vo "

        if workqueue.is_global_share:
            sql_jt += "AND gshare=:gshare "
            sql_jt += f"AND workqueue_id NOT IN (SELECT queue_id FROM {panda_config.schemaPANDA}.jedi_work_queue WHERE queue_function = 'Resource') "
            var_map[":gshare"] = workqueue.queue_name
        else:
            sql_jt += "AND workqueue_id=:workqueue_id "
            var_map[":workqueue_id"] = workqueue.queue_id

        sql_jt += "GROUP BY jobstatus, resource_type, computingSite "

        tables = [f"{panda_config.schemaPANDA}.JOBS_SHARE_STATS", f"{panda_config.schemaPANDA}.JOBSDEFINED_SHARE_STATS"]

        return_map = {}
        try:
            for table in tables:
                self.cur.arraysize = 10000
                sql_exe = (sql_jt + comment) % table
                self.cur.execute(sql_exe, var_map)
                res = self.cur.fetchall()

                # create map
                for status, resource_type, computingSite, n_count in res:
                    return_map.setdefault(computingSite, {})
                    return_map[computingSite].setdefault(resource_type, {})
                    return_map[computingSite][resource_type][status] = n_count

            tmpLog.debug("done")
            return True, return_map
        except Exception:
            self.dump_error_message(tmpLog)
            return False, {}

    # gets statistics on the number of jobs with a specific status for each nucleus at each site
    def get_num_jobs_with_status_by_nucleus(self, vo: str, job_status: str) -> [bool, Dict[str, Dict[str, int]]]:
        """
        This function will return the number of jobs with a specific status for each nucleus at each site.

        :param vo: Virtual Organization
        :param job_status: Job status
        :return: True/False and Dictionary with the number of jobs with a specific status for each nucleus at each site
        """
        comment = " /* DBProxy.get_num_jobs_with_status_by_nucleus */"
        tmp_log = self.create_tagged_logger(comment, f" vo={vo} status={job_status}")
        tmp_log.debug("start")

        # define the var map of query parameters
        var_map = {":vo": vo, ":job_status": job_status}

        # sql to query on pre-cached job statistics table
        sql_jt = """
               SELECT /*+ RESULT_CACHE */ computingSite, nucleus, SUM(njobs) FROM %s
               WHERE vo=:vo AND jobStatus=:job_status GROUP BY computingSite, nucleus
               """

        if job_status in ["transferring", "running", "activated" "holding"]:
            table = f"{panda_config.schemaPANDA}.JOBS_SHARE_STATS"
        else:
            table = f"{panda_config.schemaPANDA}.JOBSDEFINED_SHARE_STATS"

        return_map = {}
        try:
            self.cur.arraysize = 10000
            sql_exe = (sql_jt + comment) % table
            self.cur.execute(sql_exe, var_map)
            res = self.cur.fetchall()

            # create a dict
            for panda_site, nucleus, n_jobs in res:
                if n_jobs:
                    # add site
                    return_map.setdefault(panda_site, {})
                    # add stat per nucleus
                    return_map[panda_site][nucleus] = n_jobs
            tmp_log.debug("done")
            return True, return_map
        except Exception:
            self.dump_error_message(tmp_log)
            return False, {}


# get metrics module
def get_metrics_module(base_mod) -> MetricsModule:
    return base_mod.get_composite_module("metrics")
