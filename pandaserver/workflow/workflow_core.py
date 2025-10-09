import copy
import functools
import importlib
import json
import os
import random
import re
import socket
import time
import traceback
from collections import namedtuple
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.srvcore.CoreUtils import clean_user_id
from pandaserver.workflow.workflow_base import (
    WFDataSpec,
    WFDataStatus,
    WFDataType,
    WFStepSpec,
    WFStepStatus,
    WFStepType,
    WorkflowSpec,
    WorkflowStatus,
)
from pandaserver.workflow.workflow_parser import (
    json_serialize_default,
    parse_raw_request,
)

# import polars as pl  # isort:skip


# main logger
logger = PandaLogger().getLogger(__name__.split(".")[-1])

# named tuple for attribute with type
AttributeWithType = namedtuple("AttributeWithType", ["attribute", "type"])


# ==== Plugin Map ==============================================

PLUGIN_RAW_MAP = {
    "step_handler": {
        "panda_task": ("panda_task_step_handler", "PandaTaskStepHandler"),
        # Add more step handler plugins here
    },
    # "data_handler": {
    #     "example_data": ("example_data_handler", "ExampleDataHandler"),
    # },
    # Add more plugin types here
}

# map of flovar to plugin classes
flavor_plugin_map = {}
for plugin_type, plugins in PLUGIN_RAW_MAP.items():
    flavor_plugin_map[plugin_type] = {}
    for flavor, (module_name, class_name) in plugins.items():
        try:
            full_module_name = f"pandaserver.workflow.{plugin_type}_plugins.{module_name}"
            module = importlib.import_module(full_module_name)
            cls = getattr(module, class_name)
            flavor_plugin_map[plugin_type][flavor] = cls
            logger.debug(f"Imported {plugin_type} plugin {flavor} from {module_name}.{class_name}")
        except Exception as e:
            logger.error(f"Failed to import {plugin_type} plugin {flavor} from {module_name}.{class_name}: {e}")


# ==== Functions ===============================================


def get_plugin(plugin_type: str, flavor: str):
    """
    Get the plugin class for the given type and flavor

    Args:
        plugin_type (str): Type of the plugin (e.g., "step_handler", "data_handler")
        flavor (str): Flavor of the plugin (e.g., "panda_task")

    Returns:
        class: The plugin class if found, otherwise None
    """
    return flavor_plugin_map.get(plugin_type, {}).get(flavor)


# ==== Workflow Interface ======================================


class WorkflowInterface(object):
    """
    Interface for workflow management methods
    """

    def __init__(self, task_buffer, *args, **kwargs):
        """
        Constructor

        Args:
            task_buffer (TaskBufferInterface): Interface to the task buffer
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        self.tbif = task_buffer
        self.ddm_if = rucioAPI
        self.full_pid = f"{socket.getfqdn().split('.')[0]}-{os.getpgrp()}-{os.getpid()}"

    #### Context managers for locking

    @contextmanager
    def workflow_lock(self, workflow_id: int, lock_expiration_sec: int = 120):
        """
        Context manager to lock a workflow

        Args:
            workflow_id (int): ID of the workflow to lock
            lock_expiration_sec (int): Time in seconds after which the lock expires

        Yields:
            WorkflowSpec | None: The locked workflow specification if the lock was acquired, otherwise None
        """
        if self.tbif.lock_workflow(workflow_id, self.full_pid, lock_expiration_sec):
            try:
                # get the workflow spec locked
                locked_spec = self.tbif.get_workflow(workflow_id)
                # yield and run wrapped function
                yield locked_spec
            finally:
                self.tbif.unlock_workflow(workflow_id, self.full_pid)
        else:
            # lock not acquired
            yield None

    @contextmanager
    def workflow_step_lock(self, step_id: int, lock_expiration_sec: int = 120):
        """
        Context manager to lock a workflow step

        Args:
            step_id (int): ID of the workflow step to lock
            lock_expiration_sec (int): Time in seconds after which the lock expires

        Yields:
            WFStepSpec | None: The locked workflow step specification if the lock was acquired, otherwise None
        """
        if self.tbif.lock_workflow_step(step_id, self.full_pid, lock_expiration_sec):
            try:
                # get the workflow step spec locked
                locked_spec = self.tbif.get_workflow_step(step_id)
                # yield and run wrapped function
                yield locked_spec
            finally:
                self.tbif.unlock_workflow_step(step_id, self.full_pid)
        else:
            # lock not acquired
            yield None

    @contextmanager
    def workflow_data_lock(self, data_id: int, lock_expiration_sec: int = 120):
        """
        Context manager to lock workflow data

        Args:
            data_id (int): ID of the workflow data to lock
            lock_expiration_sec (int): Time in seconds after which the lock expires

        Yields:
            WFDataSpec | None: The locked workflow data specification if the lock was acquired, otherwise None
        """
        if self.tbif.lock_workflow_data(data_id, self.full_pid, lock_expiration_sec):
            try:
                # get the workflow data spec locked
                locked_spec = self.tbif.get_workflow_data(data_id)
                # yield and run wrapped function
                yield locked_spec
            finally:
                self.tbif.unlock_workflow_data(data_id, self.full_pid)
        else:
            # lock not acquired
            yield None

    # Add methods for workflow management here

    def register_workflow(
        self,
        prodsourcelabel: str,
        username: str,
        workflow_name: str | None = None,
        workflow_definition: dict | None = None,
        raw_request_params: dict | None = None,
        *args,
        **kwargs,
    ) -> int | None:
        """
        Register a new workflow

        Args:
            prodsourcelabel (str): Production source label for the workflow
            username (str): Username of the person registering the workflow
            workflow_name (str | None): Name of the workflow
            workflow_definition (dict | None): Dictionary of workflow definition
            raw_request_params (dict | None): Dictionary of parameters of the raw request
            *args: Additional arguments
            **kwargs: Additional keyword arguments

        Returns:
            int | None: The ID of the registered workflow if successful, otherwise None
        """
        tmp_log = LogWrapper(logger, f"register_workflow prodsourcelabel={prodsourcelabel} username={username} name={workflow_name}")
        tmp_log.debug("Start")
        # Implementation of workflow registration logic
        ...
        workflow_spec = WorkflowSpec()
        workflow_spec.prodsourcelabel = prodsourcelabel
        workflow_spec.username = clean_user_id(username)
        if workflow_name is not None:
            workflow_spec.name = workflow_name
        if workflow_definition is not None:
            workflow_spec.definition_json = json.dumps(workflow_definition, default=json_serialize_default)
        elif raw_request_params is not None:
            workflow_spec.raw_request_json = json.dumps(raw_request_params, default=json_serialize_default)
        else:
            tmp_log.error(f"Either workflow_definition or raw_request_params must be provided")
            return None
        workflow_spec.creation_time = naive_utcnow()
        workflow_spec.status = WorkflowStatus.registered
        # Insert to DB
        ret_workflow_id = self.tbif.insert_workflow(workflow_spec)
        if ret_workflow_id is None:
            tmp_log.error(f"Failed to register workflow")
            return None
        tmp_log.info(f"Registered workflow workflow_id={ret_workflow_id}")
        return ret_workflow_id

    # ---- Data status transitions -----------------------------

    def process_data_registered(self, data_spec: WFDataSpec):
        """
        Process data in registered status
        To prepare for checking the data

        Args:
            data_spec (WFDataSpec): The workflow data specification to process
        """
        tmp_log = LogWrapper(logger, f"process_data_registered data_id={data_spec.data_id}")
        tmp_log.debug("Start")
        # Check status
        if data_spec.status != WFDataStatus.registered:
            tmp_log.warning(f"Data status changed unexpectedly from {WFDataStatus.registered} to {data_spec.status}; skipped")
            return
        # Process
        try:
            # For now, just update status to checking
            data_spec.status = WFDataStatus.checking
            data_spec.modification_time = naive_utcnow()
            self.tbif.update_workflow_data(data_spec)
            tmp_log.info(f"Done, status={data_spec.status}")
        except Exception:
            tmp_log.error(f"Got error ; {traceback.format_exc()}")

    # ---- Step status transitions -----------------------------

    def process_step_registered(self, step_spec: WFStepSpec):
        """
        Process a step in registered status
        To prepare for checking the step

        Args:
            step_spec (WFStepSpec): The workflow step specification to process
        """
        tmp_log = LogWrapper(logger, f"process_step_registered step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Check status
        if step_spec.status != WFStepStatus.registered:
            tmp_log.warning(f"Step status changed unexpectedly from {WFStepStatus.registered} to {step_spec.status}; skipped")
            return
        # Process
        try:
            # For now, just update status to pending
            step_spec.status = WFStepStatus.pending
            step_spec.modification_time = naive_utcnow()
            self.tbif.update_workflow_step(step_spec)
            tmp_log.info(f"Done, status={step_spec.status}")
        except Exception:
            tmp_log.error(f"Got error ; {traceback.format_exc()}")

    def process_step_pending(self, step_spec: WFStepSpec, data_spec_map: Dict[str, WFDataSpec] | None = None):
        """
        Process a step in pending status
        To check the inputs of the step

        Args:
            step_spec (WFStepSpec): The workflow step specification to process
            data_spec_map (Dict[str, WFDataSpec] | None): Optional map of data name to WFDataSpec for the workflow
        """
        tmp_log = LogWrapper(logger, f"process_step_pending workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Check status
        if step_spec.status != WFStepStatus.pending:
            tmp_log.warning(f"Step status changed unexpectedly from {WFStepStatus.pending} to {step_spec.status}; skipped")
            return
        # Process
        try:
            # Get data spec map of the workflow
            if data_spec_map is None:
                data_specs = self.tbif.get_workflow_data(workflow_id=step_spec.workflow_id)
                data_spec_map = {data_spec.name: data_spec for data_spec in data_specs}
            # Input data list of the step
            step_spec_definition = step_spec.definition_json_map
            input_data_list = step_spec_definition.get("input_data_list", [])
            # Check if all input data are good, aka ready as input
            all_inputs_good = True
            for input_data_name in input_data_list:
                data_spec = data_spec_map.get(input_data_name)
                if data_spec is None:
                    tmp_log.warning(f"Input data {input_data_name} not found in workflow data")
                    all_inputs_good = False
                    break
                elif data_spec.status not in WFDataStatus.good_input_statuses:
                    tmp_log.debug(f"Input data {input_data_name} status {data_spec.status} is not ready for input")
                    all_inputs_good = False
                    break
            # If not all inputs are good, just return and wait for next round
            if not all_inputs_good:
                tmp_log.debug(f"Some input data are not good; skipped")
                return
            # All inputs are good, register outputs of the step and update step status to ready
            tmp_log.debug(f"All input data are good; proceeding")
            output_data_type = WFDataType.mid
            if not step_spec_definition.get("is_tail"):
                # is intermediate step, register their outputs as mid type
                output_data_list = step_spec_definition.get("output_data_list", [])
                now_time = naive_utcnow()
                for output_data_name in output_data_list:
                    data_spec = WFDataSpec()
                    data_spec.workflow_id = step_spec.workflow_id
                    data_spec.name = output_data_name
                    data_spec.target_id = None  # to be filled later
                    data_spec.status = WFDataStatus.registered
                    data_spec.type = WFDataType.mid
                    data_spec.flavor = "ddm_ds"  # FIXME: hardcoded flavor, should be configurable
                    data_spec.creation_time = now_time
                    self.tbif.insert_workflow_data(data_spec)
                    tmp_log.debug(f"Registered mid data {output_data_name} of step_id={step_spec.step_id}")
            step_spec.status = WFStepStatus.ready
            step_spec.modification_time = naive_utcnow()
            self.tbif.update_workflow_step(step_spec)
            tmp_log.info(f"Done, status={step_spec.status}")
        except Exception:
            tmp_log.error(f"Got error ; {traceback.format_exc()}")

    # ---- Workflow status transitions -------------------------

    def process_workflow_registered(self, workflow_spec: WorkflowSpec):
        """
        Process a workflow in registered status
        To parse to get workflow definition from raw request

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process
        """
        tmp_log = LogWrapper(logger, f"process_workflow_registered workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("Start")
        # Check status
        if workflow_spec.status != WorkflowStatus.registered:
            tmp_log.warning(f"Workflow status changed unexpectedly from {WorkflowStatus.registered} to {workflow_spec.status}; skipped")
            return
        # Process
        try:
            if workflow_spec.definition_json is not None:
                # Already has definition, skip parsing
                tmp_log.debug(f"Workflow already has definition; skipped parsing")
            else:
                # Parse the workflow definition from raw request
                raw_request_dict = workflow_spec.raw_request_json_map
                sandbox_url = os.path.join(raw_request_dict["sourceURL"], "cache", raw_request_dict["sandbox"])
                log_token = f'< user="{workflow_spec.username}" outDS={raw_request_dict["outDS"]}>'
                is_ok, is_fatal, workflow_definition_dict = parse_raw_request(
                    sandbox_url=sandbox_url,
                    log_token=log_token,
                    user_name=workflow_spec.username,
                    raw_request_dict=raw_request_dict,
                )
                # Failure handling
                if is_fatal:
                    tmp_log.error(f"Fatal error in parsing raw request; cancelled the workflow")
                    workflow_spec.status = WorkflowStatus.cancelled
                    workflow_spec.set_parameter("cancel_reason", "Fatal error in parsing raw request")
                    self.tbif.update_workflow(workflow_spec)
                    return
                if not is_ok:
                    tmp_log.warning(f"Failed to parse raw request; skipped")
                    return
                # Parsed successfully, update definition
                workflow_spec.definition_json = json.dumps(workflow_definition_dict, default=json_serialize_default)
                tmp_log.debug(f"Parsed raw request into definition")
            # Update status to parsed
            workflow_spec.status = WorkflowStatus.parsed
            # Update DB
            self.tbif.update_workflow(workflow_spec)
            tmp_log.info(f"Done, status={workflow_spec.status}")
        except Exception:
            tmp_log.error(f"Got error ; {traceback.format_exc()}")

    def process_workflow_parsed(self, workflow_spec: WorkflowSpec):
        """
        Process a workflow in parsed status
        Register steps, and update its status
        Parse raw request into workflow definition, register steps, and update its status

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process
        """
        tmp_log = LogWrapper(logger, f"process_workflow_parsed workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("Start")
        # Check status
        if workflow_spec.status != WorkflowStatus.parsed:
            tmp_log.warning(f"Workflow status changed unexpectedly from {WorkflowStatus.parsed} to {workflow_spec.status}; skipped")
            return
        # Process
        try:
            # Parse the workflow definition
            workflow_definition_dict = workflow_spec.definition_json_map
            if workflow_definition_dict is None:
                tmp_log.error(f"Workflow definition is None; cancelled the workflow")
                workflow_spec.status = WorkflowStatus.cancelled
                workflow_spec.set_parameter("cancel_reason", "Workflow definition is None")
                self.tbif.update_workflow(workflow_spec)
                return
            # initialize
            data_specs = []
            step_specs = []
            now_time = naive_utcnow()
            # Register root inputs and outputs
            for input_name, input_target in workflow_definition_dict["root_inputs"].items():
                data_spec = WFDataSpec()
                data_spec.workflow_id = workflow_spec.workflow_id
                data_spec.name = input_name
                data_spec.target_id = input_target
                data_spec.status = WFDataStatus.registered
                data_spec.type = WFDataType.input
                data_spec.flavor = "ddm_ds"  # FIXME: hardcoded flavor, should be configurable
                data_spec.creation_time = now_time
                data_specs.append(data_spec)
            for output_name, output_dict in workflow_definition_dict["root_outputs"].items():
                data_spec = WFDataSpec()
                data_spec.workflow_id = workflow_spec.workflow_id
                data_spec.name = output_name
                data_spec.target_id = output_dict.get("value")
                data_spec.status = WFDataStatus.registered
                data_spec.type = WFDataType.output
                data_spec.flavor = "ddm_ds"  # FIXME: hardcoded flavor, should be configurable
                data_spec.creation_time = now_time
                data_specs.append(data_spec)
            # Register steps based on nodes in the definition
            for node in workflow_definition_dict["nodes"]:
                # FIXME: not yet consider scatter, condition, loop, etc.
                if not (node.get("condition") or node.get("scatter") or node.get("loop")):
                    step_spec = WFStepSpec()
                    step_spec.workflow_id = workflow_spec.workflow_id
                    step_spec.member_id = node["id"]
                    step_spec.name = node["name"]
                    step_spec.status = WFStepStatus.registered
                    step_spec.type = WFStepType.ordinary
                    step_spec.flavor = "panda_task"  # FIXME: hardcoded flavor, should be configurable
                    # step definition
                    step_definition = copy.deepcopy(node)
                    # resolve inputs and outputs
                    input_data_set = set()
                    output_data_set = set()
                    for input_target in step_definition.get("inputs", {}).values():
                        if not input_target.get("source"):
                            continue
                        sources = []
                        if isinstance(input_target["source"], list):
                            sources = copy.deepcopy(input_target["source"])
                        else:
                            sources = [input_target["source"]]
                        input_data_set.update(sources)
                    for output_name in step_definition.get("outputs", {}).keys():
                        output_data_set.add(output_name)
                    step_definition["input_data_list"] = list(input_data_set)
                    step_definition["output_data_list"] = list(output_data_set)
                    step_spec.definition_json_map = step_definition
                    step_spec.creation_time = now_time
                    step_specs.append(step_spec)
            # Update status to checking
            workflow_spec.status = WorkflowStatus.checking
            # Upsert DB
            self.tbif.upsert_workflow_entities(
                workflow_spec.workflow_id,
                actions_dict={"workflow": "update", "steps": "insert", "data": "insert"},
                workflow_spec=workflow_spec,
                step_specs=step_specs,
                data_specs=data_specs,
            )
            tmp_log.info(f"Done, inserted {len(step_specs)} steps and {len(data_specs)} data, status={workflow_spec.status}")
        except Exception:
            tmp_log.error(f"Got error ; {traceback.format_exc()}")

    def process_workflow_starting(self, workflow_spec: WorkflowSpec):
        """
        Process a workflow in starting status
        To start the steps in the workflow

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process
        """
        tmp_log = LogWrapper(logger, f"process_workflow_starting workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("Start")
        # Check status
        if workflow_spec.status != WorkflowStatus.starting:
            tmp_log.warning(f"Workflow status changed unexpectedly from {WorkflowStatus.starting} to {workflow_spec.status}; skipped")
            return
        # Process
        try:
            # Get steps in registered status
            step_specs = self.tbif.get_steps_of_workflow(workflow_id=workflow_spec.workflow_id, status_filter_list=[WFStepStatus.registered])
            if not step_specs:
                tmp_log.warning(f"No steps in {WFStepStatus.registered} status; skipped")
                return
            # Get data spec map of the workflow
            data_specs = self.tbif.get_data_of_workflow(workflow_id=workflow_spec.workflow_id)
            data_spec_map = {data_spec.name: data_spec for data_spec in data_specs}
            # Process each step
            for step_spec in step_specs:
                with self.workflow_step_lock(step_spec.step_id) as locked_step_spec:
                    if locked_step_spec is None:
                        tmp_log.warning(f"Failed to acquire lock for step_id={step_spec.step_id}; skipped")
                        continue
                    if locked_step_spec.status != WFStepStatus.registered:
                        tmp_log.warning(f"Step status changed unexpectely from {WFStepStatus.registered} to {locked_step_spec.status}; skipped")
                        continue
                    step_spec = locked_step_spec
                    # Process the step
                    match step_spec.status:
                        case WFStepStatus.registered:
                            self.process_step_registered(step_spec)
                        case WFStepStatus.pending:
                            self.process_step_pending(step_spec, data_spec_map=data_spec_map)
                        case _:
                            # tmp_log.debug(f"Step status {step_spec.status} is not handled in this context; skipped")
                            continue
            tmp_log.info(f"Done processing steps in {WFStepStatus.registered} status")
        except Exception:
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
