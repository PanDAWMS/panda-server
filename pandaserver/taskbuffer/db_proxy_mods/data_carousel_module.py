import datetime

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.taskbuffer.DataCarousel import (
    DataCarouselRequestSpec,
    DataCarouselRequestStatus,
    get_resubmit_request_spec,
)
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule, varNUMBER
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec


# Module class to define Data Carousel related methods
class DataCarouselModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # query data carousel request ID by dataset
    def get_data_carousel_request_id_by_dataset_JEDI(self, dataset):
        comment = " /* JediDBProxy.get_data_carousel_request_id_by_dataset_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"dataset={dataset}")
        tmp_log.debug("start")
        try:
            # sql to query request of the dataset
            status_var_names_str, status_var_map = get_sql_IN_bind_variables(DataCarouselRequestStatus.reusable_statuses, prefix=":status_")
            sql_query = (
                f"SELECT request_id "
                f"FROM {panda_config.schemaJEDI}.data_carousel_requests "
                f"WHERE dataset=:dataset "
                f"AND status IN ({status_var_names_str}) "
            )
            var_map = {":dataset": dataset}
            var_map.update(status_var_map)
            self.cur.execute(sql_query + comment, var_map)
            res = self.cur.fetchone()
            if res is None:
                tmp_log.debug("no such request")
                return None
            request_id = res[0]
            tmp_log.debug(f"found request_id={request_id}")
            return request_id
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return None

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

    # insert data carousel relations
    def insert_data_carousel_relations_JEDI(self, task_id, request_ids):
        comment = " /* JediDBProxy.insert_data_carousel_relations_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={task_id}")
        tmp_log.debug("start")
        try:
            # start transaction
            self.conn.begin()
            # insert relations
            n_rel_inserted = 0
            n_rel_reused = 0
            for request_id in request_ids:
                # sql to query relation
                sql_rel_query = (
                    f"SELECT request_id, task_id "
                    f"FROM {panda_config.schemaJEDI}.data_carousel_relations "
                    f"WHERE request_id=:request_id AND task_id=:task_id "
                )
                var_map = {":request_id": request_id, ":task_id": task_id}
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
            tmp_log.debug(f"inserted {n_rel_inserted} relations ; reused {n_rel_reused} relations")
            return n_rel_inserted
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
                tmp_log.debug(f"status filter: {status_filter_list}")
            if status_exclusion_list:
                antistatus_var_names_str, antistatus_var_map = get_sql_IN_bind_variables(status_exclusion_list, prefix=":antistatus")
                sql_query_id += f"AND t.status NOT IN ({antistatus_var_names_str}) "
                var_map.update(antistatus_var_map)
                tmp_log.debug(f"status exclusion filter: {status_exclusion_list}")
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

    # get related tasks and their info of a data carousel request
    def get_related_tasks_of_data_carousel_request_JEDI(self, request_id, status_filter_list=None, status_exclusion_list=None):
        comment = " /* JediDBProxy.get_related_tasks_of_data_carousel_request_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id}")
        tmp_log.debug("start")
        try:
            # initialize
            ret_tasks_dict = {}
            # start transaction
            self.conn.begin()
            # sql to query related tasks
            sql_query = (
                f"SELECT rel.task_id, t.status "
                f"FROM {panda_config.schemaJEDI}.data_carousel_relations rel, {panda_config.schemaJEDI}.JEDI_Tasks t "
                f"WHERE rel.task_id=t.jediTaskID "
                f"AND rel.request_id=:request_id "
            )
            var_map = {":request_id": request_id}
            if status_filter_list:
                status_var_names_str, status_var_map = get_sql_IN_bind_variables(status_filter_list, prefix=":status")
                sql_query += f"AND t.status IN ({status_var_names_str}) "
                var_map.update(status_var_map)
                tmp_log.debug(f"status filter: {status_filter_list}")
            if status_exclusion_list:
                antistatus_var_names_str, antistatus_var_map = get_sql_IN_bind_variables(status_exclusion_list, prefix=":antistatus")
                sql_query += f"AND t.status NOT IN ({antistatus_var_names_str}) "
                var_map.update(antistatus_var_map)
                tmp_log.debug(f"status exclusion filter: {status_exclusion_list}")
            self.cur.execute(sql_query + comment, var_map)
            res_list = self.cur.fetchall()
            if res_list:
                for task_id, status in res_list:
                    ret_tasks_dict[task_id] = {"task_id": task_id, "status": status}
            else:
                tmp_log.debug("no related task")
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            # return
            tmp_log.debug(f"got {len(ret_tasks_dict)} related tasks")
            return ret_tasks_dict
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
                f"DELETE FROM {panda_config.schemaJEDI}.data_carousel_requests " f"WHERE request_id=:request_id " f"AND status IN ({status_var_names_str}) "
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
                f"DELETE FROM {panda_config.schemaJEDI}.data_carousel_relations rel "
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
                f"DELETE FROM {panda_config.schemaJEDI}.data_carousel_requests " f"WHERE status IN ({status_var_names_str}) " f"AND end_time<=:end_time_max "
            )
            var_map = {":end_time_max": now_time - datetime.timedelta(days=time_limit_days)}
            var_map.update(status_var_map)
            self.cur.execute(sql_delete_req + comment, var_map)
            ret_req = self.cur.rowcount
            # sql to delete relations
            sql_delete_rel = (
                f"DELETE FROM {panda_config.schemaJEDI}.data_carousel_relations rel "
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
