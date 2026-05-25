import datetime
import json

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule


class AsyncRequestModule(BaseModule):
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    def upsert_machine_heartbeat(self, machine_name: str, service_name: str) -> bool:
        """
        Update or insert the liveness record for this machine.

        :param machine_name: hostname of the machine sending the heartbeat
        :param service_name: name of the service running on the machine
        :return: True on success, False on DB error
        """
        comment = " /* DBProxy.upsert_machine_heartbeat */"
        tmp_log = self.create_tagged_logger(comment, f"machine={machine_name} service={service_name}")
        tmp_log.debug("start")
        try:
            now = naive_utcnow()
            sql_check = "SELECT 1 FROM ATLAS_PANDA.machine_heartbeat WHERE machine_name=:machine_name "
            sql_update = "UPDATE ATLAS_PANDA.machine_heartbeat " "SET service_name=:service_name, last_seen=:now " "WHERE machine_name=:machine_name "
            sql_insert = "INSERT INTO ATLAS_PANDA.machine_heartbeat (machine_name, service_name, last_seen) " "VALUES (:machine_name, :service_name, :now) "
            self.conn.begin()
            self.cur.execute(sql_check + comment, {":machine_name": machine_name})
            exists = self.cur.fetchone() is not None
            var_map = {":machine_name": machine_name, ":service_name": service_name, ":now": now}
            if exists:
                self.cur.execute(sql_update + comment, var_map)
            else:
                self.cur.execute(sql_insert + comment, var_map)
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    def get_alive_machines(self, service_name: str, within_seconds: int = 600) -> list[str]:
        """
        Return hostnames in the service that have sent a heartbeat within within_seconds.

        :param service_name: name of the service to filter on
        :param within_seconds: liveness window in seconds; default 600
        :return: list of hostnames; empty list on DB error or no matches
        """
        comment = " /* DBProxy.get_alive_machines */"
        tmp_log = self.create_tagged_logger(comment, f"service={service_name}")
        tmp_log.debug("start")
        try:
            threshold = naive_utcnow() - datetime.timedelta(seconds=within_seconds)
            sql = "SELECT machine_name FROM ATLAS_PANDA.machine_heartbeat " "WHERE service_name=:service_name AND last_seen>=:threshold "
            var_map = {":service_name": service_name, ":threshold": threshold}
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, var_map)
            res = self.cur.fetchall()
            if not self._commit():
                raise RuntimeError("Commit error")
            machines = [row[0] for row in res] if res else []
            tmp_log.debug(f"return {len(machines)} records")
            return machines
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return []

    def insert_async_request(
        self,
        request_id: str,
        request_type: str,
        parameters_json: str,
        service_name: str,
        machine_name: str,
        expected_machines_json: str,
        retention_days: int = 7,
    ) -> bool:
        """
        Insert a new async request row, and opportunistically prune rows older than retention_days.

        :param request_id: unique request identifier (UUID)
        :param request_type: handler key registered in processor.HANDLERS (e.g. "grep")
        :param parameters_json: JSON-encoded handler parameters
        :param service_name: service that should process this request
        :param machine_name: target hostname, or empty for any machine in the service
        :param expected_machines_json: JSON-encoded list of hostnames expected to respond
        :param retention_days: prune rows older than this many days; default 7
        :return: True if inserted, False on DB error
        """
        comment = " /* DBProxy.insert_async_request */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id}")
        tmp_log.debug("start")
        try:
            sql_insert = (
                "INSERT INTO ATLAS_PANDA.async_requests "
                "(request_id, request_type, service_name, machine_name, parameters, expected_machines, created_at) "
                "VALUES (:request_id, :request_type, :service_name, :machine_name, :parameters, :expected_machines, :created_at) "
            )
            now = naive_utcnow()
            var_map = {
                ":request_id": request_id,
                ":request_type": request_type,
                ":service_name": service_name,
                ":machine_name": machine_name,
                ":parameters": parameters_json,
                ":expected_machines": expected_machines_json,
                ":created_at": now,
            }
            self.conn.begin()
            self.cur.execute(sql_insert + comment, var_map)
            # opportunistic prune of stale rows (child table first for FK)
            threshold = now - datetime.timedelta(days=retention_days)
            sql_del_results = (
                "DELETE FROM ATLAS_PANDA.async_results " "WHERE request_id IN (SELECT request_id FROM ATLAS_PANDA.async_requests WHERE created_at<:threshold) "
            )
            self.cur.execute(sql_del_results + comment, {":threshold": threshold})
            sql_del_requests = "DELETE FROM ATLAS_PANDA.async_requests WHERE created_at<:threshold "
            self.cur.execute(sql_del_requests + comment, {":threshold": threshold})
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    def get_async_request(self, request_id: str) -> dict | None:
        """
        Return a single async_requests row as a dict.

        :param request_id: unique request identifier
        :return: row as a dict keyed by column name, or None if not found or on DB error
        """
        comment = " /* DBProxy.get_async_request */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id}")
        tmp_log.debug("start")
        try:
            sql = (
                "SELECT request_id, request_type, service_name, machine_name, "
                "parameters, expected_machines, created_at "
                "FROM ATLAS_PANDA.async_requests WHERE request_id=:request_id "
            )
            var_map = {":request_id": request_id}
            self.conn.begin()
            self.cur.arraysize = 1
            self.cur.execute(sql + comment, var_map)
            row = self.cur.fetchone()
            if not self._commit():
                raise RuntimeError("Commit error")
            if row is None:
                tmp_log.debug("done (not found)")
                return None
            keys = ["request_id", "request_type", "service_name", "machine_name", "parameters", "expected_machines", "created_at"]
            tmp_log.debug("done (found)")
            return dict(zip(keys, row))
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return None

    def get_pending_requests_for_machine(self, my_hostname: str, my_service: str, known_types: list[str]) -> list[dict]:
        """
        Return async_requests rows that this machine should process (not yet claimed or pending retry).

        :param my_hostname: hostname of the requesting machine
        :param my_service: service name of the requesting machine
        :param known_types: request types this machine knows how to handle
        :return: list of rows as dicts; empty list on DB error, empty known_types, or no matches
        """
        comment = " /* DBProxy.get_pending_requests_for_machine */"
        tmp_log = self.create_tagged_logger(comment, f"host={my_hostname} service={my_service}")
        tmp_log.debug("start")
        if not known_types:
            tmp_log.debug("return 0 records")
            return []
        try:
            type_vars, type_var_map = get_sql_IN_bind_variables(known_types, prefix=":type")
            sql = f"""
                SELECT r.request_id, r.request_type, r.service_name, r.machine_name,
                       r.parameters, r.expected_machines, r.created_at
                FROM ATLAS_PANDA.async_requests r
                WHERE r.request_type IN ({type_vars})
                  AND (r.machine_name=:my_hostname OR r.service_name=:my_service)
                  AND (
                    NOT EXISTS (
                      SELECT 1 FROM ATLAS_PANDA.async_results ar
                      WHERE ar.request_id=r.request_id AND ar.machine_name=:my_hostname
                    )
                    OR EXISTS (
                      SELECT 1 FROM ATLAS_PANDA.async_results ar
                      WHERE ar.request_id=r.request_id AND ar.machine_name=:my_hostname
                        AND ar.status='pending'
                    )
                  )
            """
            var_map = {":my_hostname": my_hostname, ":my_service": my_service}
            var_map.update(type_var_map)
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, var_map)
            res = self.cur.fetchall()
            if not self._commit():
                raise RuntimeError("Commit error")
            if not res:
                tmp_log.debug("return 0 records")
                return []
            keys = ["request_id", "request_type", "service_name", "machine_name", "parameters", "expected_machines", "created_at"]
            tmp_log.debug(f"return {len(res)} records")
            return [dict(zip(keys, row)) for row in res]
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return []

    def claim_async_result(self, request_id: str, machine_name: str) -> bool:
        """
        Claim a result row for this machine.

        If no row exists: INSERT status='running', attempts=1.
        If status='pending' row exists (retry): UPDATE status='running', increment attempts, reset timestamps.

        :param request_id: unique request identifier
        :param machine_name: hostname claiming the result
        :return: True if claimed; False if the row already exists in a non-pending state (race guard) or on DB error
        """
        comment = " /* DBProxy.claim_async_result */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id} machine={machine_name}")
        tmp_log.debug("start")
        try:
            self.conn.begin()
            now = naive_utcnow()
            # check current state
            sql_check = "SELECT status, attempts FROM ATLAS_PANDA.async_results " "WHERE request_id=:request_id AND machine_name=:machine_name "
            var_map = {":request_id": request_id, ":machine_name": machine_name}
            self.cur.arraysize = 1
            self.cur.execute(sql_check + comment, var_map)
            row = self.cur.fetchone()
            if row is None:
                # no row yet — insert fresh claim
                sql = (
                    "INSERT INTO ATLAS_PANDA.async_results "
                    "(request_id, machine_name, status, attempts, started_at) "
                    "VALUES (:request_id, :machine_name, 'running', 1, :started_at) "
                )
                self.cur.execute(sql + comment, {":request_id": request_id, ":machine_name": machine_name, ":started_at": now})
                tmp_log.debug("claimed via insert")
            elif row[0] == "pending":
                # retry — update existing row
                sql = (
                    "UPDATE ATLAS_PANDA.async_results "
                    "SET status='running', attempts=attempts+1, started_at=:started_at, "
                    "    finished_at=NULL, result=NULL, error_msg=NULL, truncated=0 "
                    "WHERE request_id=:request_id AND machine_name=:machine_name "
                )
                self.cur.execute(sql + comment, {":request_id": request_id, ":machine_name": machine_name, ":started_at": now})
                tmp_log.debug("claimed via retry update")
            else:
                # already claimed by another instance or in a terminal state
                tmp_log.debug("already claimed, skipping")
                self._rollback()
                return False
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    def finish_async_result(
        self,
        request_id: str,
        machine_name: str,
        status: str,
        result: str | None = None,
        error_msg: str | None = None,
        truncated: bool = False,
    ) -> bool:
        """
        Update a result row to a terminal state (done or failed).

        :param request_id: unique request identifier
        :param machine_name: hostname owning the result row
        :param status: terminal status, e.g. "done" or "failed"
        :param result: handler output payload, or None
        :param error_msg: error message when status is "failed", or None
        :param truncated: True if result was truncated to fit the column
        :return: True on success, False on DB error
        """
        comment = " /* DBProxy.finish_async_result */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id} machine={machine_name} status={status}")
        tmp_log.debug("start")
        try:
            sql = (
                "UPDATE ATLAS_PANDA.async_results "
                "SET status=:status, result=:result, error_msg=:error_msg, "
                "    truncated=:truncated, finished_at=:finished_at "
                "WHERE request_id=:request_id AND machine_name=:machine_name "
            )
            var_map = {
                ":request_id": request_id,
                ":machine_name": machine_name,
                ":status": status,
                ":result": result,
                ":error_msg": error_msg,
                ":truncated": 1 if truncated else 0,
                ":finished_at": naive_utcnow(),
            }
            self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    def get_async_results(self, request_id: str) -> list[dict]:
        """
        Return all async_results rows for a request as a list of dicts.

        :param request_id: unique request identifier
        :return: list of rows as dicts; empty list on DB error or no matches
        """
        comment = " /* DBProxy.get_async_results */"
        tmp_log = self.create_tagged_logger(comment, f"request_id={request_id}")
        tmp_log.debug("start")
        try:
            sql = (
                "SELECT machine_name, status, result, truncated, error_msg, attempts, started_at, finished_at "
                "FROM ATLAS_PANDA.async_results WHERE request_id=:request_id "
            )
            var_map = {":request_id": request_id}
            self.conn.begin()
            self.cur.arraysize = 1000
            self.cur.execute(sql + comment, var_map)
            res = self.cur.fetchall()
            if not self._commit():
                raise RuntimeError("Commit error")
            if not res:
                tmp_log.debug("return 0 records")
                return []
            keys = ["machine_name", "status", "result", "truncated", "error_msg", "attempts", "started_at", "finished_at"]
            tmp_log.debug(f"return {len(res)} records")
            return [dict(zip(keys, row)) for row in res]
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return []

    def cleanup_async_requests(self, retention_days: int = 7) -> bool:
        """
        Delete async_results and async_requests rows older than retention_days.

        :param retention_days: delete rows whose created_at is older than this many days; default 7
        :return: True on success, False on DB error
        """
        comment = " /* DBProxy.cleanup_async_requests */"
        tmp_log = self.create_tagged_logger(comment)
        tmp_log.debug("start")
        try:
            threshold = naive_utcnow() - datetime.timedelta(days=retention_days)
            self.conn.begin()
            # child rows first (FK constraint)
            sql_results = (
                "DELETE FROM ATLAS_PANDA.async_results "
                "WHERE request_id IN ("
                "  SELECT request_id FROM ATLAS_PANDA.async_requests WHERE created_at<:threshold"
                ") "
            )
            self.cur.execute(sql_results + comment, {":threshold": threshold})
            sql_requests = "DELETE FROM ATLAS_PANDA.async_requests WHERE created_at<:threshold "
            self.cur.execute(sql_requests + comment, {":threshold": threshold})
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug("done")
            return True
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return False

    def recover_stale_results(self, machine_name: str, max_processing_seconds: int = 300, max_attempts: int = 3) -> bool:
        """
        Reset or fail running result rows that have exceeded the processing budget.

        Rows on this machine in status 'running' with started_at older than max_processing_seconds
        are reset to 'pending' for retry while attempts < max_attempts, or marked 'failed' once
        the attempt cap is reached.

        :param machine_name: hostname whose rows to recover
        :param max_processing_seconds: age threshold for declaring a row stale; default 300
        :param max_attempts: maximum retry attempts before giving up; default 3
        :return: True on success, False on DB error
        """
        comment = " /* DBProxy.recover_stale_results */"
        tmp_log = self.create_tagged_logger(comment, f"machine={machine_name}")
        tmp_log.debug("start")
        try:
            threshold = naive_utcnow() - datetime.timedelta(seconds=max_processing_seconds)
            self.conn.begin()
            # fetch stale rows
            sql_select = (
                "SELECT request_id, attempts FROM ATLAS_PANDA.async_results " "WHERE machine_name=:machine_name AND status='running' AND started_at<:threshold "
            )
            var_map = {":machine_name": machine_name, ":threshold": threshold}
            self.cur.arraysize = 1000
            self.cur.execute(sql_select + comment, var_map)
            rows = self.cur.fetchall()
            now = naive_utcnow()
            for request_id, attempts in rows or []:
                if attempts < max_attempts:
                    sql_retry = (
                        "UPDATE ATLAS_PANDA.async_results SET status='pending', finished_at=NULL "
                        "WHERE request_id=:request_id AND machine_name=:machine_name "
                    )
                    self.cur.execute(sql_retry + comment, {":request_id": request_id, ":machine_name": machine_name})
                    tmp_log.debug(f"reset request_id={request_id} to pending (attempt {attempts}/{max_attempts})")
                else:
                    sql_fail = (
                        "UPDATE ATLAS_PANDA.async_results "
                        "SET status='failed', error_msg='max retries exceeded', finished_at=:now "
                        "WHERE request_id=:request_id AND machine_name=:machine_name "
                    )
                    self.cur.execute(sql_fail + comment, {":request_id": request_id, ":machine_name": machine_name, ":now": now})
                    tmp_log.warning(f"gave up on request_id={request_id} after {attempts} attempts")
            if not self._commit():
                raise RuntimeError("Commit error")
            tmp_log.debug(f"done, processed {len(rows or [])} stale rows")
            return True
        except Exception:
            self._rollback()
            self.dump_error_message(tmp_log)
            return False
