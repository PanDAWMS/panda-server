import datetime
import json
import os
import re
import sys

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
            tmp_log.warning("no request found; skipped")
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
            tmp_log.warning("no request found; skipped")
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
            tmp_log.warning("no request found; skipped")
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
