import datetime
import json
import sys

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule
from pandaserver.taskbuffer.JobSpec import JobSpec, get_task_queued_time


# Module class to define metrics related methods
class MetricsModule(BaseModule):
    # constructor
    def __init__(self, log_stream: PandaLogger):
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
            method_name, tmp_log = self.create_method_name_logger(comment, f"jediTaskID={jedi_task_id} PandaID={panda_id}")
        else:
            method_name, tmp_log = self.create_method_name_logger(comment, f"jediTaskID={jedi_task_id}")
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
            method_name, tmp_log = self.create_method_name_logger(comment, f"jediTaskID={jedi_task_id} PandaID={panda_id}")
        else:
            method_name, tmp_log = self.create_method_name_logger(comment, f"jediTaskID={jedi_task_id}")
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
        method_name, tmp_log = self.create_method_name_logger(comment, f"jediTaskID={jedi_task_id}")
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

    # update task queued time
    def update_task_queued_time(self, jedi_task_id: int):
        """
        Update task queued time depending on the number of inputs to be processed. Update it if it was None and inputs become available.
        Set None if no input is available and record the queuing duration to task metrics.

        :param jedi_task_id: jediTaskID of the task
        """
        comment = " /* DBProxy.update_task_queued_time */"
        method_name, tmp_log = self.create_method_name_logger(comment, f"jediTaskID={jedi_task_id}")
        tmp_log.debug("start")
        # check if the task is queued
        sql_check = f"SELECT status,oldStatus,queuedTime,currentPriority,gshare FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
        var_map = {":jediTaskID": jedi_task_id}
        self.cur.execute(sql_check + comment, var_map)
        res = self.cur.fetchone()
        if not res:
            # not found
            tmp_log.debug("task not found")
            return
        (task_status, task_old_status, queued_time, current_priority, global_share) = res
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
                task_metrics["queuingPeriods"].append(
                    {"start": queued_time, "end": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None), "status": task_status}
                )
                tmp_success = self.set_workload_metrics(jedi_task_id, None, task_metrics, False)
                if not tmp_success:
                    err_str = f"failed to update task metrics for jediTaskId={jedi_task_id}"
                    tmp_log.error(err_str)
                    return
            else:
                tmp_log.debug("skipped since queuing period list is too long")
        tmp_log.debug("done")

    # record job queuing period
    def record_job_queuing_period(self, panda_id: int, job_spec: JobSpec = None) -> bool | None:
        """
        Record queuing period in job metrics. Skip if job.jobMetrics doesn't contain task queued time

        :param panda_id: Job's PandaID
        :param job_spec: job spec. None to get it from the database
        :return: True if success. False if failed. None if skipped
        """
        comment = " /* DBProxy.record_job_queuing_period */"
        method_name, tmp_log = self.create_method_name_logger(comment, f"PandaID={panda_id}")
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
                    "end": datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None),
                    "status": job_status,
                }
                tmp_success = self.set_workload_metrics(jedi_task_id, panda_id, job_metrics, False)
                if not tmp_success:
                    err_str = "Failed to update job metrics"
                    tmp_log.error(err_str)
                    return False
            tmp_log.debug("done")
            return True
        else:
            tmp_log.debug(f"skipped as jediTaskID={jedi_task_id} taskQueuedTime={task_queued_time}")
            return None
