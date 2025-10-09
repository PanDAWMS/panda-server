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
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule, varNUMBER
from pandaserver.taskbuffer.db_proxy_mods.entity_module import get_entity_module
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.workflow.workflow_base import (
    WFDataSpec,
    WFDataStatus,
    WFStepSpec,
    WFStepStatus,
    WorkflowSpec,
    WorkflowStatus,
)


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
                    step_spec = WFStepSpec()
                    step_spec.pack(res)
                    return step_spec
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
                    data_spec = WFDataSpec()
                    data_spec.pack(res)
                    return data_spec
        else:
            tmp_log.warning("no data found; skipped")
            return None

    def get_steps_of_workflow(self, workflow_id: int, status_filter_list: list | None = None, status_exclusion_list: list | None = None) -> list[WFStepSpec]:
        """
        Retrieve all workflow steps for a given workflow ID

        Args:
            workflow_id (int): ID of the workflow to retrieve steps for
            status_filter_list (list | None): List of statuses to filter the steps by (optional)
            status_exclusion_list (list | None): List of statuses to exclude the steps by (optional)

        Returns:
            list[WFStepSpec]: List of workflow step specifications
        """
        comment = " /* DBProxy.get_steps_of_workflow */"
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_id}")
        sql = f"SELECT {WFStepSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.workflow_steps " f"WHERE workflow_id=:workflow_id "
        var_map = {":workflow_id": workflow_id}
        if status_filter_list:
            status_var_names_str, status_var_map = get_sql_IN_bind_variables(status_filter_list, prefix=":status")
            sql += f"AND status IN ({status_var_names_str}) "
            var_map.update(status_var_map)
        if status_exclusion_list:
            antistatus_var_names_str, antistatus_var_map = get_sql_IN_bind_variables(status_exclusion_list, prefix=":antistatus")
            sql += f"AND status NOT IN ({antistatus_var_names_str}) "
            var_map.update(antistatus_var_map)
        self.cur.execute(sql + comment, var_map)
        res_list = self.cur.fetchall()
        if res_list is not None:
            step_specs = []
            for res in res_list:
                step_spec = WFStepSpec()
                step_spec.pack(res)
                step_specs.append(step_spec)
            return step_specs
        else:
            tmp_log.warning("no steps found; skipped")
            return []

    def get_data_of_workflow(self, workflow_id: int, status_filter_list: list | None = None, status_exclusion_list: list | None = None) -> list[WFDataSpec]:
        """
        Retrieve all workflow data for a given workflow ID

        Args:
            workflow_id (int): ID of the workflow to retrieve data for
            status_filter_list (list | None): List of statuses to filter the data by (optional)
            status_exclusion_list (list | None): List of statuses to exclude the data by (optional)

        Returns:
            list[WFDataSpec]: List of workflow data specifications
        """
        comment = " /* DBProxy.get_data_of_workflow */"
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_id}")
        sql = f"SELECT {WFDataSpec.columnNames()} " f"FROM {panda_config.schemaJEDI}.workflow_data " f"WHERE workflow_id=:workflow_id "
        var_map = {":workflow_id": workflow_id}
        if status_filter_list:
            status_var_names_str, status_var_map = get_sql_IN_bind_variables(status_filter_list, prefix=":status")
            sql += f"AND status IN ({status_var_names_str}) "
            var_map.update(status_var_map)
        if status_exclusion_list:
            antistatus_var_names_str, antistatus_var_map = get_sql_IN_bind_variables(status_exclusion_list, prefix=":antistatus")
            sql += f"AND status NOT IN ({antistatus_var_names_str}) "
            var_map.update(antistatus_var_map)
        self.cur.execute(sql + comment, var_map)
        res_list = self.cur.fetchall()
        if res_list is not None:
            data_specs = []
            for res in res_list:
                data_spec = WFDataSpec()
                data_spec.pack(res)
                data_specs.append(data_spec)
            return data_specs
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

    def insert_workflow(self, workflow_spec: WorkflowSpec) -> int | None:
        """
        Insert a new workflow specification into the database

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to insert

        Returns:
            int | None: The ID of the inserted workflow if successful, otherwise None
        """
        comment = " /* DBProxy.insert_workflow */"
        tmp_log = self.create_tagged_logger(comment, "")
        tmp_log.debug("start")
        try:
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # sql to insert workflow
                workflow_spec.creation_time = naive_utcnow()
                sql_insert = (
                    f"INSERT INTO {panda_config.schemaJEDI}.workflows ({workflow_spec.columnNames()}) "
                    f"{workflow_spec.bindValuesExpression()} "
                    f"RETURNING workflow_id INTO :new_workflow_id "
                )
                var_map = workflow_spec.valuesMap(useSeq=True)
                var_map[":new_workflow_id"] = self.cur.var(varNUMBER)
                self.cur.execute(sql_insert + comment, var_map)
                workflow_id = int(self.getvalue_corrector(self.cur.getvalue(var_map[":new_workflow_id"])))
            tmp_log.debug(f"inserted workflow_id={workflow_id}")
            return workflow_id
        except Exception:
            return None

    def insert_workflow_step(self, step_spec: WFStepSpec) -> int | None:
        """
        Insert a new workflow step specification into the database

        Args:
            step_spec (WFStepSpec): The workflow step specification to insert

        Returns:
            int | None: The ID of the inserted workflow step if successful, otherwise None
        """
        comment = " /* DBProxy.insert_workflow_step */"
        tmp_log = self.create_tagged_logger(comment, "")
        tmp_log.debug("start")
        try:
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # sql to insert workflow step
                step_spec.creation_time = naive_utcnow()
                sql_insert = (
                    f"INSERT INTO {panda_config.schemaJEDI}.workflow_steps ({step_spec.columnNames()}) "
                    f"{step_spec.bindValuesExpression()} "
                    f"RETURNING step_id INTO :new_step_id "
                )
                var_map = step_spec.valuesMap(useSeq=True)
                var_map[":new_step_id"] = self.cur.var(varNUMBER)
                self.cur.execute(sql_insert + comment, var_map)
                step_id = int(self.getvalue_corrector(self.cur.getvalue(var_map[":new_step_id"])))
            tmp_log.debug(f"inserted step_id={step_id}")
            return step_id
        except Exception:
            return None

    def insert_workflow_data(self, data_spec: WFDataSpec) -> int | None:
        """
        Insert a new workflow data specification into the database

        Args:
            data_spec (WFDataSpec): The workflow data specification to insert

        Returns:
            int | None: The ID of the inserted workflow data if successful, otherwise None
        """
        comment = " /* DBProxy.insert_workflow_data */"
        tmp_log = self.create_tagged_logger(comment, "")
        tmp_log.debug("start")
        try:
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # sql to insert workflow data
                data_spec.creation_time = naive_utcnow()
                sql_insert = (
                    f"INSERT INTO {panda_config.schemaJEDI}.workflow_data ({data_spec.columnNames()}) "
                    f"{data_spec.bindValuesExpression()} "
                    f"RETURNING data_id INTO :new_data_id "
                )
                var_map = data_spec.valuesMap(useSeq=True)
                var_map[":new_data_id"] = self.cur.var(varNUMBER)
                self.cur.execute(sql_insert + comment, var_map)
                data_id = int(self.getvalue_corrector(self.cur.getvalue(var_map[":new_data_id"])))
            tmp_log.debug(f"inserted data_id={data_id}")
            return data_id
        except Exception:
            return None

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

    def update_workflow_step(self, step_spec: WFStepSpec) -> WFStepSpec | None:
        """
        Update a workflow step specification in the database

        Args:
            step_spec (WFStepSpec): The workflow step specification to update

        Returns:
            WFStepSpec | None: The updated workflow step specification if successful, otherwise None
        """
        comment = " /* DBProxy.update_workflow_step */"
        tmp_log = self.create_tagged_logger(comment, f"step_id={step_spec.step_id}")
        tmp_log.debug("start")
        try:
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # sql to update workflow step
                step_spec.modification_time = naive_utcnow()
                sql_update = f"UPDATE {panda_config.schemaJEDI}.workflow_steps " f"SET {step_spec.bindUpdateChangesExpression()} " "WHERE step_id=:step_id "
                var_map = step_spec.valuesMap(useSeq=False, onlyChanged=True)
                var_map[":step_id"] = step_spec.step_id
                cur.execute(sql_update + comment, var_map)
                tmp_log.debug(f"updated {step_spec.bindUpdateChangesExpression()}")
            return step_spec
        except Exception:
            return None

    def update_workflow_data(self, data_spec: WFDataSpec) -> WFDataSpec | None:
        """
        Update a workflow data specification in the database

        Args:
            data_spec (WFDataSpec): The workflow data specification to update

        Returns:
            WFDataSpec | None: The updated workflow data specification if successful, otherwise None
        """
        comment = " /* DBProxy.update_workflow_data */"
        tmp_log = self.create_tagged_logger(comment, f"data_id={data_spec.data_id}")
        tmp_log.debug("start")
        try:
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # sql to update workflow data
                data_spec.modification_time = naive_utcnow()
                sql_update = f"UPDATE {panda_config.schemaJEDI}.workflow_data " f"SET {data_spec.bindUpdateChangesExpression()} " "WHERE data_id=:data_id "
                var_map = data_spec.valuesMap(useSeq=False, onlyChanged=True)
                var_map[":data_id"] = data_spec.data_id
                cur.execute(sql_update + comment, var_map)
                tmp_log.debug(f"updated {data_spec.bindUpdateChangesExpression()}")
            return data_spec
        except Exception:
            return None

    def upsert_workflow_entities(
        self,
        workflow_id: int | None,
        actions_dict: dict | None = None,
        workflow_spec: WorkflowSpec | None = None,
        step_specs: list[WFStepSpec] | None = None,
        data_specs: list[WFDataSpec] | None = None,
    ) -> dict | None:
        """
        Update or insert (if not existing) steps and data associated with a workflow within a transaction

        Args:
            workflow_id (int | None): ID of the workflow to update, or None if to insert
            actions_dict (dict | None): Dictionary of actions (insert, update, or None) to perform on the entities (workflow, steps, data), e.g. {"workflow": None, "steps": "insert", "data": "update"}
            workflow_spec (WorkflowSpec|None): The workflow specification to update or insert
            step_specs (list[WFStepSpec]|None): List of workflow step specifications to update or insert
            data_specs (list[WFDataSpec]|None): List of workflow data specifications to update or insert

        Returns:
            dict | None: Dictionary containing the number of steps and data upserted, or None if an error occurred
        """
        comment = " /* DBProxy.upsert_workflow_entities */"
        # Determine actions of each entity
        action_of_workflow = None
        action_of_steps = None
        action_of_data = None
        if actions_dict:
            if (tmp_action_of_workflow := actions_dict.get("workflow")) and workflow_spec:
                if tmp_action_of_workflow == "insert" and workflow_id is None:
                    action_of_workflow = "insert"
                elif tmp_action_of_workflow == "update" and workflow_id is not None and workflow_spec.workflow_id == workflow_id:
                    action_of_workflow = "update"
            action_of_steps = actions_dict.get("steps") if (workflow_id and step_specs) else None
            action_of_data = actions_dict.get("data") if (workflow_id and data_specs) else None
        actions_dict = {
            "workflow": action_of_workflow,
            "steps": action_of_steps,
            "data": action_of_data,
        }
        # log
        tmp_log = self.create_tagged_logger(comment, f"workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug(f"start, actions={actions_dict}")
        # skip if no action specified
        if not any(actions_dict.values()):
            self.log.warning("no action specified; skipped")
            return None
        try:
            n_steps_upserted = 0
            n_data_upserted = 0
            with self.transaction(tmp_log=tmp_log) as (cur, _):
                # action for data
                if action_of_data == "insert":
                    for data_spec in data_specs:
                        data_spec.creation_time = naive_utcnow()
                        sql_insert = (
                            f"INSERT INTO {panda_config.schemaJEDI}.workflow_data ({data_spec.columnNames()}) "
                            f"{data_spec.bindValuesExpression()} "
                            f"RETURNING data_id INTO :new_data_id "
                        )
                        var_map = data_spec.valuesMap(useSeq=True)
                        var_map[":new_data_id"] = self.cur.var(varNUMBER)
                        self.cur.execute(sql_insert + comment, var_map)
                        data_id = int(self.getvalue_corrector(self.cur.getvalue(var_map[":new_data_id"])))
                        data_spec.data_id = data_id
                        n_data_upserted += 1
                        tmp_log.debug(f"inserted a data workflow_id={workflow_id} data_id={data_id}")
                elif action_of_data == "update":
                    for data_spec in data_specs:
                        data_spec.modification_time = naive_utcnow()
                        sql_update = (
                            f"UPDATE {panda_config.schemaJEDI}.workflow_data " f"SET {data_spec.bindUpdateChangesExpression()} " "WHERE data_id=:data_id "
                        )
                        var_map = data_spec.valuesMap(useSeq=False, onlyChanged=True)
                        var_map[":data_id"] = data_spec.data_id
                        self.cur.execute(sql_update + comment, var_map)
                        n_data_upserted += 1
                        tmp_log.debug(f"updated a data workflow_id={workflow_id} data_id={data_spec.data_id}")
                # action for steps
                if action_of_steps == "insert":
                    for step_spec in step_specs:
                        step_spec.creation_time = naive_utcnow()
                        sql_insert = (
                            f"INSERT INTO {panda_config.schemaJEDI}.workflow_steps ({step_spec.columnNames()}) "
                            f"{step_spec.bindValuesExpression()} "
                            f"RETURNING step_id INTO :new_step_id "
                        )
                        var_map = step_spec.valuesMap(useSeq=True)
                        var_map[":new_step_id"] = self.cur.var(varNUMBER)
                        self.cur.execute(sql_insert + comment, var_map)
                        step_id = int(self.getvalue_corrector(self.cur.getvalue(var_map[":new_step_id"])))
                        step_spec.step_id = step_id
                        n_steps_upserted += 1
                        tmp_log.debug(f"inserted a step workflow_id={workflow_id} step_id={step_id}")
                elif action_of_steps == "update":
                    for step_spec in step_specs:
                        step_spec.modification_time = naive_utcnow()
                        sql_update = (
                            f"UPDATE {panda_config.schemaJEDI}.workflow_steps " f"SET {step_spec.bindUpdateChangesExpression()} " "WHERE step_id=:step_id "
                        )
                        var_map = step_spec.valuesMap(useSeq=False, onlyChanged=True)
                        var_map[":step_id"] = step_spec.step_id
                        self.cur.execute(sql_update + comment, var_map)
                        n_steps_upserted += 1
                        tmp_log.debug(f"updated a step workflow_id={workflow_id} step_id={step_spec.step_id}")
                # action for workflow
                if action_of_workflow == "insert":
                    workflow_spec.creation_time = naive_utcnow()
                    sql_insert = (
                        f"INSERT INTO {panda_config.schemaJEDI}.workflows ({workflow_spec.columnNames()}) "
                        f"{workflow_spec.bindValuesExpression()} "
                        f"RETURNING workflow_id INTO :new_workflow_id "
                    )
                    var_map = workflow_spec.valuesMap(useSeq=True)
                    var_map[":new_workflow_id"] = self.cur.var(varNUMBER)
                    self.cur.execute(sql_insert + comment, var_map)
                    workflow_id = int(self.getvalue_corrector(self.cur.getvalue(var_map[":new_workflow_id"])))
                    workflow_spec.workflow_id = workflow_id
                    tmp_log.debug(f"inserted a workflow workflow_id={workflow_id}")
                elif action_of_workflow == "update":
                    workflow_spec.modification_time = naive_utcnow()
                    sql_update = (
                        f"UPDATE {panda_config.schemaJEDI}.workflows " f"SET {workflow_spec.bindUpdateChangesExpression()} " "WHERE workflow_id=:workflow_id "
                    )
                    var_map = workflow_spec.valuesMap(useSeq=False, onlyChanged=True)
                    var_map[":workflow_id"] = workflow_spec.workflow_id
                    self.cur.execute(sql_update + comment, var_map)
                    tmp_log.debug(f"updated a workflow workflow_id={workflow_spec.workflow_id}")
                tmp_log.debug("actions completed")
            # Summary
            tmp_log.debug(f"done, actions={actions_dict}, upserted workflow_id={workflow_id} with {n_steps_upserted} steps and {n_data_upserted} data")
            return {"workflow_id": workflow_id, "steps": n_steps_upserted, "data": n_data_upserted}
        except Exception:
            return None
