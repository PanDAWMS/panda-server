import json
import os
import re
import sys
from datetime import datetime, timedelta

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import ErrorCode, JobUtils
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule
from pandaserver.taskbuffer.db_proxy_mods.entity_module import get_entity_module
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.workflow_core import WFDataSpec, WFStepSpec, WorkflowSpec


# Module class to define methods related to workflow
class WorkflowModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    def get_workflow(self, workflow_id: int) -> WorkflowSpec | None:
        """
        Retrieve a workflow specification by its ID

        Args:
            workflow_id (int): ID of the workflow to retrieve

        Returns:
            WorkflowSpec | None: The workflow specification if found, otherwise None
        """
        comment = " /* DBProxy.get_workflow */"
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_id}")
        sql = f"SELECT {WorkflowSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.workflows " f"WHERE workflow_id=:workflow_id "
        var_map = {":workflow_id": workflow_id}
        self.cur.execute(sql + comment, var_map)
        res_list = self.cur.fetchall()
        if res_list is not None:
            if len(res_list) > 1:
                tmp_log.error("more than one workflows; unexpected")
            else:
                for res in res_list:
                    workflow_spec = WorkflowSpec()
                    workflow_spec.pack(res)
                    return workflow_spec
        else:
            tmp_log.warning("no workflow found; skipped")
            return None

    def get_workflow_step(self, step_id: int) -> WFStepSpec | None:
        """
        Retrieve a workflow step specification by its ID

        Args:
            step_id (int): ID of the workflow step to retrieve

        Returns:
            WFStepSpec | None: The workflow step specification if found, otherwise None
        """
        comment = " /* DBProxy.get_workflow_step */"
        tmp_log = self.create_tagged_logger(comment, f"step_id={step_id}")
        sql = f"SELECT {WFStepSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.workflow_steps " f"WHERE step_id=:step_id "
        var_map = {":step_id": step_id}
        self.cur.execute(sql + comment, var_map)
        res_list = self.cur.fetchall()
        if res_list is not None:
            if len(res_list) > 1:
                tmp_log.error("more than one steps; unexpected")
            else:
                for res in res_list:
                    wf_step_spec = WFStepSpec()
                    wf_step_spec.pack(res)
                    return wf_step_spec
        else:
            tmp_log.warning("no step found; skipped")
            return None

    def get_workflow_data(self, data_id: int) -> WFDataSpec | None:
        """
        Retrieve a workflow data specification by its ID

        Args:
            data_id (int): ID of the workflow data to retrieve

        Returns:
            WFDataSpec | None: The workflow data specification if found, otherwise None
        """
        comment = " /* DBProxy.get_workflow_data */"
        tmp_log = self.create_tagged_logger(comment, f"data_id={data_id}")
        sql = f"SELECT {WFDataSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.workflow_data " f"WHERE data_id=:data_id "
        var_map = {":data_id": data_id}
        self.cur.execute(sql + comment, var_map)
        res_list = self.cur.fetchall()
        if res_list is not None:
            if len(res_list) > 1:
                tmp_log.error("more than one data; unexpected")
            else:
                for res in res_list:
                    wf_data_spec = WFDataSpec()
                    wf_data_spec.pack(res)
                    return wf_data_spec
        else:
            tmp_log.warning("no data found; skipped")
            return None

    def get_steps_of_workflow(self, workflow_id: int) -> list[WFStepSpec]:
        """
        Retrieve all workflow steps for a given workflow ID

        Args:
            workflow_id (int): ID of the workflow to retrieve steps for

        Returns:
            list[WFStepSpec]: List of workflow step specifications
        """
        comment = " /* DBProxy.get_steps_of_workflow */"
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_id}")
        sql = f"SELECT {WFStepSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.workflow_steps " f"WHERE workflow_id=:workflow_id "
        var_map = {":workflow_id": workflow_id}
        self.cur.execute(sql + comment, var_map)
        res_list = self.cur.fetchall()
        if res_list is not None:
            wf_step_specs = []
            for res in res_list:
                wf_step_spec = WFStepSpec()
                wf_step_spec.pack(res)
                wf_step_specs.append(wf_step_spec)
            return wf_step_specs
        else:
            tmp_log.warning("no steps found; skipped")
            return []

    def get_data_of_workflow(self, workflow_id: int) -> list[WFDataSpec]:
        """
        Retrieve all workflow data for a given workflow ID

        Args:
            workflow_id (int): ID of the workflow to retrieve data for

        Returns:
            list[WFDataSpec]: List of workflow data specifications
        """
        comment = " /* DBProxy.get_data_of_workflow */"
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_id}")
        sql = f"SELECT {WFDataSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.workflow_data " f"WHERE workflow_id=:workflow_id "
        var_map = {":workflow_id": workflow_id}
        self.cur.execute(sql + comment, var_map)
        res_list = self.cur.fetchall()
        if res_list is not None:
            wf_data_specs = []
            for res in res_list:
                wf_data_spec = WFDataSpec()
                wf_data_spec.pack(res)
                wf_data_specs.append(wf_data_spec)
            return wf_data_specs
        else:
            tmp_log.warning("no data found; skipped")
            return []

    def lock_workflow(self, workflow_id: int, locked_by: str, lock_expiration_sec: int = 120) -> bool | None:
        """
        Lock a workflow to prevent concurrent modifications

        Args:
            workflow_id (int): ID of the workflow to lock
            locked_by (str): Identifier of the entity locking the workflow
            lock_expiration_sec (int): Time in seconds after which the lock expires

        Returns:
            bool | None: True if the lock was acquired, False if not, None if an error occurred
        """
        comment = " /* DBProxy.lock_workflow */"
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_id}, locked_by={locked_by}")
        tmp_log.debug("start")
        try:
            now_time = naive_utcnow()
            sql_lock = (
                f"UPDATE {panda_config.schemaJEDI}.workflows "
                "SET locked_by=:locked_by, lock_time=:lock_time "
                "WHERE workflow_id=:workflow_id "
                "AND (locked_by IS NULL OR locked_by=:locked_by OR lock_time<:min_lock_time)"
            )
            var_map = {
                ":locked_by": self.full_pid,
                ":lock_time": now_time,
                ":workflow_id": workflow_id,
                ":min_lock_time": now_time - timedelta(seconds=lock_expiration_sec),
            }
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                cur.execute(sql_lock + comment, var_map)
                row_count = cur.rowcount
                if row_count is None:
                    tmp_log.error(f"failed to update DB to lock; skipped")
                elif row_count > 1:
                    tmp_log.error(f"more than one workflow updated to lock; unexpected")
                elif row_count == 0:
                    # no row updated; did not get the lock
                    tmp_log.debug(f"did not get lock; skipped")
                    return False
                elif row_count == 1:
                    # successfully locked the workflow
                    tmp_log.debug(f"got lock")
                    return True
        except Exception as e:
            tmp_log.error(f"failed to lock workflow: {e}")

    def unlock_workflow(self, workflow_id: int, locked_by: str) -> bool | None:
        """
        Unlock a workflow to allow modifications

        Args:
            workflow_id (int): ID of the workflow to unlock
            locked_by (str): Identifier of the entity unlocking the workflow

        Returns:
            bool | None: True if the unlock was successful, False if not, None if an error occurred
        """
        comment = " /* DBProxy.unlock_workflow */"
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_id}, locked_by={locked_by}")
        tmp_log.debug("start")
        try:
            sql_unlock = (
                f"UPDATE {panda_config.schemaJEDI}.workflows " "SET locked_by=NULL, lock_time=NULL " "WHERE workflow_id=:workflow_id AND locked_by=:locked_by"
            )
            var_map = {":workflow_id": workflow_id, ":locked_by": locked_by}
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                cur.execute(sql_unlock + comment, var_map)
                row_count = cur.rowcount
                if row_count is None:
                    tmp_log.error(f"failed to update DB to unlock; skipped")
                elif row_count > 1:
                    tmp_log.error(f"more than one workflow updated to unlock; unexpected")
                elif row_count == 0:
                    # no row updated; did not get the unlock
                    tmp_log.debug(f"no workflow updated to unlock; skipped")
                    return False
                elif row_count == 1:
                    # successfully unlocked the workflow
                    tmp_log.debug(f"released lock")
                    return True
        except Exception as e:
            tmp_log.error(f"failed to unlock workflow: {e}")

    def lock_workflow_step(self, step_id: int, locked_by: str, lock_expiration_sec: int = 120) -> bool | None:
        """
        Lock a workflow step to prevent concurrent modifications

        Args:
            step_id (int): ID of the workflow step to lock
            locked_by (str): Identifier of the entity locking the workflow step
            lock_expiration_sec (int): Time in seconds after which the lock expires

        Returns:
            bool | None: True if the lock was acquired, False if not, None if an error occurred
        """
        comment = " /* DBProxy.lock_workflow_step */"
        tmp_log = self.create_tagged_logger(comment, f"step_id={step_id}, locked_by={locked_by}")
        tmp_log.debug("start")
        try:
            now_time = naive_utcnow()
            sql_lock = (
                f"UPDATE {panda_config.schemaJEDI}.workflow_steps "
                "SET locked_by=:locked_by, lock_time=:lock_time "
                "WHERE step_id=:step_id "
                "AND (locked_by IS NULL OR locked_by=:locked_by OR lock_time<:min_lock_time)"
            )
            var_map = {
                ":locked_by": self.full_pid,
                ":lock_time": now_time,
                ":step_id": step_id,
                ":min_lock_time": now_time - timedelta(seconds=lock_expiration_sec),
            }
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                cur.execute(sql_lock + comment, var_map)
                row_count = cur.rowcount
                if row_count is None:
                    tmp_log.error(f"failed to update DB to lock; skipped")
                elif row_count > 1:
                    tmp_log.error(f"more than one step updated to lock; unexpected")
                elif row_count == 0:
                    # no row updated; did not get the lock
                    tmp_log.debug(f"did not get lock; skipped")
                    return False
                elif row_count == 1:
                    # successfully locked the workflow step
                    tmp_log.debug(f"got lock")
                    return True
        except Exception as e:
            tmp_log.error(f"failed to lock workflow step: {e}")

    def unlock_workflow_step(self, step_id: int, locked_by: str) -> bool | None:
        """
        Unlock a workflow step to allow modifications

        Args:
            step_id (int): ID of the workflow step to unlock
            locked_by (str): Identifier of the entity unlocking the workflow step

        Returns:
            bool | None: True if the unlock was successful, False if not, None if an error occurred
        """
        comment = " /* DBProxy.unlock_workflow_step */"
        tmp_log = self.create_tagged_logger(comment, f"step_id={step_id}, locked_by={locked_by}")
        tmp_log.debug("start")
        try:
            sql_unlock = (
                f"UPDATE {panda_config.schemaJEDI}.workflow_steps " "SET locked_by=NULL, lock_time=NULL " "WHERE step_id=:step_id AND locked_by=:locked_by"
            )
            var_map = {":step_id": step_id, ":locked_by": locked_by}
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                cur.execute(sql_unlock + comment, var_map)
                row_count = cur.rowcount
                if row_count is None:
                    tmp_log.error(f"failed to update DB to unlock; skipped")
                elif row_count > 1:
                    tmp_log.error(f"more than one step updated to unlock; unexpected")
                elif row_count == 0:
                    # no row updated; did not get the unlock
                    tmp_log.debug(f"no step updated to unlock; skipped")
                    return False
                elif row_count == 1:
                    # successfully unlocked the workflow step
                    tmp_log.debug(f"released lock")
                    return True
        except Exception as e:
            tmp_log.error(f"failed to unlock workflow step: {e}")

    def lock_workflow_data(self, data_id: int, locked_by: str, lock_expiration_sec: int = 120) -> bool | None:
        """
        Lock a workflow data to prevent concurrent modifications

        Args:
            data_id (int): ID of the workflow data to lock
            locked_by (str): Identifier of the entity locking the workflow data
            lock_expiration_sec (int): Time in seconds after which the lock expires

        Returns:
            bool | None: True if the lock was acquired, False if not, None if an error occurred
        """
        comment = " /* DBProxy.lock_workflow_data */"
        tmp_log = self.create_tagged_logger(comment, f"data_id={data_id}, locked_by={locked_by}")
        tmp_log.debug("start")
        try:
            now_time = naive_utcnow()
            sql_lock = (
                f"UPDATE {panda_config.schemaJEDI}.workflow_data "
                "SET locked_by=:locked_by, lock_time=:lock_time "
                "WHERE data_id=:data_id "
                "AND (locked_by IS NULL OR locked_by=:locked_by OR lock_time<:min_lock_time)"
            )
            var_map = {
                ":locked_by": self.full_pid,
                ":lock_time": now_time,
                ":data_id": data_id,
                ":min_lock_time": now_time - timedelta(seconds=lock_expiration_sec),
            }
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                cur.execute(sql_lock + comment, var_map)
                row_count = cur.rowcount
                if row_count is None:
                    tmp_log.error(f"failed to update DB to lock; skipped")
                elif row_count > 1:
                    tmp_log.error(f"more than one data updated to lock; unexpected")
                elif row_count == 0:
                    # no row updated; did not get the lock
                    tmp_log.debug(f"did not get lock; skipped")
                    return False
                elif row_count == 1:
                    # successfully locked the workflow data
                    tmp_log.debug(f"got lock")
                    return True
        except Exception as e:
            tmp_log.error(f"failed to lock workflow data: {e}")

    def unlock_workflow_data(self, data_id: int, locked_by: str) -> bool | None:
        """
        Unlock a workflow data to allow modifications

        Args:
            data_id (int): ID of the workflow data to unlock
            locked_by (str): Identifier of the entity unlocking the workflow data

        Returns:
            bool | None: True if the unlock was successful, False if not, None if an error occurred
        """
        comment = " /* DBProxy.unlock_workflow_data */"
        tmp_log = self.create_tagged_logger(comment, f"data_id={data_id}, locked_by={locked_by}")
        tmp_log.debug("start")
        try:
            sql_unlock = (
                f"UPDATE {panda_config.schemaJEDI}.workflow_data " "SET locked_by=NULL, lock_time=NULL " "WHERE data_id=:data_id AND locked_by=:locked_by"
            )
            var_map = {":data_id": data_id, ":locked_by": locked_by}
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                cur.execute(sql_unlock + comment, var_map)
                row_count = cur.rowcount
                if row_count is None:
                    tmp_log.error(f"failed to update DB to unlock; skipped")
                elif row_count > 1:
                    tmp_log.error(f"more than one data updated to unlock; unexpected")
                elif row_count == 0:
                    # no row updated; did not get the unlock
                    tmp_log.debug(f"no data updated to unlock; skipped")
                    return False
                elif row_count == 1:
                    # successfully unlocked the workflow data
                    tmp_log.debug(f"released lock")
                    return True
        except Exception as e:
            tmp_log.error(f"failed to unlock workflow data: {e}")

    def update_workflow(self, workflow_spec: WorkflowSpec) -> WorkflowSpec | None:
        """
        Update a workflow specification in the database

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to update

        Returns:
            WorkflowSpec | None: The updated workflow specification if successful, otherwise None
        """
        comment = " /* DBProxy.update_workflow */"
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("start")
        try:
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # sql to update workflow
                workflow_spec.modification_time = naive_utcnow()
                sql_update = (
                    f"UPDATE {panda_config.schemaJEDI}.workflows " f"SET {workflow_spec.bindUpdateChangesExpression()} " "WHERE workflow_id=:workflow_id "
                )
                var_map = workflow_spec.valuesMap(useSeq=False, onlyChanged=True)
                var_map[":workflow_id"] = workflow_spec.workflow_id
                cur.execute(sql_update + comment, var_map)
                tmp_log.debug(f"updated {workflow_spec.bindUpdateChangesExpression()}")
            return workflow_spec
        except Exception:
            return None

    def update_workflow_step(self, wf_step_spec: WFStepSpec) -> WFStepSpec | None:
        """
        Update a workflow step specification in the database

        Args:
            wf_step_spec (WFStepSpec): The workflow step specification to update

        Returns:
            WFStepSpec | None: The updated workflow step specification if successful, otherwise None
        """
        comment = " /* DBProxy.update_workflow_step */"
        tmp_log = self.create_tagged_logger(comment, f"step_id={wf_step_spec.step_id}")
        tmp_log.debug("start")
        try:
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # sql to update workflow step
                wf_step_spec.modification_time = naive_utcnow()
                sql_update = f"UPDATE {panda_config.schemaJEDI}.workflow_steps " f"SET {wf_step_spec.bindUpdateChangesExpression()} " "WHERE step_id=:step_id "
                var_map = wf_step_spec.valuesMap(useSeq=False, onlyChanged=True)
                var_map[":step_id"] = wf_step_spec.step_id
                cur.execute(sql_update + comment, var_map)
                tmp_log.debug(f"updated {wf_step_spec.bindUpdateChangesExpression()}")
            return wf_step_spec
        except Exception:
            return None

    def update_workflow_data(self, wf_data_spec: WFDataSpec) -> WFDataSpec | None:
        """
        Update a workflow data specification in the database

        Args:
            wf_data_spec (WFDataSpec): The workflow data specification to update

        Returns:
            WFDataSpec | None: The updated workflow data specification if successful, otherwise None
        """
        comment = " /* DBProxy.update_workflow_data */"
        tmp_log = self.create_tagged_logger(comment, f"data_id={wf_data_spec.data_id}")
        tmp_log.debug("start")
        try:
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # sql to update workflow data
                wf_data_spec.modification_time = naive_utcnow()
                sql_update = f"UPDATE {panda_config.schemaJEDI}.workflow_data " f"SET {wf_data_spec.bindUpdateChangesExpression()} " "WHERE data_id=:data_id "
                var_map = wf_data_spec.valuesMap(useSeq=False, onlyChanged=True)
                var_map[":data_id"] = wf_data_spec.data_id
                cur.execute(sql_update + comment, var_map)
                tmp_log.debug(f"updated {wf_data_spec.bindUpdateChangesExpression()}")
            return wf_data_spec
        except Exception:
            return None
