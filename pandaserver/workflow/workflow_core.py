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
    WFDataProcessResult,
    WFDataSpec,
    WFDataStatus,
    WFDataType,
    WFStepProcessResult,
    WFStepSpec,
    WFStepStatus,
    WFStepTargetCheckResult,
    WFStepTargetSubmitResult,
    WFStepType,
    WorkflowProcessResult,
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

# ==== Global Parameters =======================================

WORKFLOW_CHECK_INTERVAL_SEC = 300
STEP_CHECK_INTERVAL_SEC = 300
DATA_CHECK_INTERVAL_SEC = 300

# ==== Plugin Map ==============================================

PLUGIN_RAW_MAP = {
    "step_handler": {
        "panda_task": ("panda_task_step_handler", "PandaTaskStepHandler"),
        # Add more step handler plugins here
    },
    "data_handler": {
        "ddm_collection": ("ddm_collection_data_handler", "DDMCollectionDataHandler"),
    },
    # Add more plugin types here
}

# map of flavor to plugin classes
flavor_plugin_class_map = {}
for plugin_type, plugins in PLUGIN_RAW_MAP.items():
    flavor_plugin_class_map[plugin_type] = {}
    for flavor, (module_name, class_name) in plugins.items():
        try:
            full_module_name = f"pandaserver.workflow.{plugin_type}_plugins.{module_name}"
            module = importlib.import_module(full_module_name)
            cls = getattr(module, class_name)
            flavor_plugin_class_map[plugin_type][flavor] = cls
            logger.debug(f"Imported {plugin_type} plugin {flavor} from {module_name}.{class_name}")
        except Exception as e:
            logger.error(f"Failed to import {plugin_type} plugin {flavor} from {module_name}.{class_name}: {e}")


# ==== Functions ===============================================


def get_plugin_class(plugin_type: str, flavor: str):
    """
    Get the plugin class for the given type and flavor

    Args:
        plugin_type (str): Type of the plugin (e.g., "step_handler", "data_handler")
        flavor (str): Flavor of the plugin (e.g., "panda_task")

    Returns:
        class: The plugin class if found, otherwise None
    """
    return flavor_plugin_class_map.get(plugin_type, {}).get(flavor)


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
        self.plugin_map = {}

    def get_plugin(self, plugin_type: str, flavor: str):
        """
        Get the plugin instance for the given type and flavor

        Args:
            plugin_type (str): Type of the plugin (e.g., "step_handler", "data_handler")
            flavor (str): Flavor of the plugin (e.g., "panda_task")

        Returns:
            Any: The plugin instance if found, otherwise None
        """
        plugin = self.plugin_map.get(plugin_type, {}).get(flavor)
        if plugin is not None:
            return plugin
        else:
            # not yet loaded, try to load
            cls = get_plugin_class(plugin_type, flavor)
            if cls is not None:
                self.plugin_map.setdefault(plugin_type, {})[flavor] = cls(task_buffer=self.tbif, ddm_if=self.ddm_if)
                plugin = self.plugin_map[plugin_type][flavor]
        return plugin

    # --- Context managers for locking -------------------------

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

    # --- Workflow operation -----------------------------------

    def register_workflow(
        self,
        prodsourcelabel: str,
        user_dn: str,
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
            user_dn (str): Distinguished name of the user submitting the workflow
            workflow_name (str | None): Name of the workflow
            workflow_definition (dict | None): Dictionary of workflow definition
            raw_request_params (dict | None): Dictionary of parameters of the raw request
            *args: Additional arguments
            **kwargs: Additional keyword arguments

        Returns:
            int | None: The ID of the registered workflow if successful, otherwise None
        """
        username = clean_user_id(user_dn)
        tmp_log = LogWrapper(logger, f"register_workflow prodsourcelabel={prodsourcelabel} username={username} name={workflow_name}")
        tmp_log.debug(f'Start, user_dn is "{user_dn}"')
        # Implementation of workflow registration logic
        ...
        workflow_spec = WorkflowSpec()
        workflow_spec.prodsourcelabel = prodsourcelabel
        workflow_spec.username = username
        if workflow_name is not None:
            workflow_spec.name = workflow_name
        if workflow_definition is not None:
            workflow_definition["user_dn"] = user_dn
            workflow_spec.definition_json = json.dumps(workflow_definition, default=json_serialize_default)
        elif raw_request_params is not None:
            raw_request_params["user_dn"] = user_dn
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

    def cancel_workflow(self, workflow_id: int) -> bool: ...

    # --- Step operation ---------------------------------------

    def cancel_step(self, step_id: int) -> bool: ...

    # ---- Data status transitions -----------------------------

    def process_data_registered(self, data_spec: WFDataSpec) -> WFDataProcessResult:
        """
        Process data in registered status
        To prepare for checking the data

        Args:
            data_spec (WFDataSpec): The workflow data specification to process

        Returns:
            WFDataProcessResult: The result of processing the data
        """
        tmp_log = LogWrapper(logger, f"process_data_registered data_id={data_spec.data_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFDataProcessResult()
        # Check status
        if data_spec.status != WFDataStatus.registered:
            process_result.message = f"Data status changed unexpectedly from {WFDataStatus.registered} to {data_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # For now, just update status to checking
            data_spec.status = WFDataStatus.checking
            self.tbif.update_workflow_data(data_spec)
            tmp_log.info(f"Done, status={data_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"{process_result.message}")
        return process_result

    def process_data_checking(self, data_spec: WFDataSpec) -> WFDataProcessResult:
        """
        Process data in checking status
        To check the conditions about whether the data is available

        Args:
            data_spec (WFDataSpec): The workflow data specification to process

        Returns:
            WFDataProcessResult: The result of processing the data
        """
        tmp_log = LogWrapper(logger, f"process_data_checking data_id={data_spec.data_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFDataProcessResult()
        # Check status
        if data_spec.status != WFDataStatus.checking:
            process_result.message = f"Data status changed unexpectedly from {WFDataStatus.checking} to {data_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # Check data availability
            # FIXME: For now, always advance to checked_nonex
            data_spec.status = WFDataStatus.checked_nonex
            data_spec.check_time = naive_utcnow()
            self.tbif.update_workflow_data(data_spec)
            tmp_log.info(f"Done, status={data_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"{process_result.message}")
        return process_result

    def process_data_checked(self, data_spec: WFDataSpec) -> WFDataProcessResult:
        """
        Process data in checked status
        To advance to next status based on check result

        Args:
            data_spec (WFDataSpec): The workflow data specification to process

        Returns:
            WFDataProcessResult: The result of processing the data
        """
        tmp_log = LogWrapper(logger, f"process_data_checked data_id={data_spec.data_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFDataProcessResult()
        # Check status
        if data_spec.status not in (WFDataStatus.checked_nonex, WFDataStatus.checked_partex, WFDataStatus.checked_exist):
            process_result.message = f"Data status changed unexpectedly from checked_* to {data_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            original_status = data_spec.status
            # Update data status based on check result
            now_time = naive_utcnow()
            match data_spec.status:
                case WFDataStatus.checked_nonex:
                    # Data does not exist, advance to generating_start
                    data_spec.status = WFDataStatus.generating_start
                    data_spec.check_time = now_time
                    data_spec.start_time = now_time
                    self.tbif.update_workflow_data(data_spec)
                case WFDataStatus.checked_partex:
                    # Data partially exist, advance to waiting_ready
                    data_spec.status = WFDataStatus.waiting_ready
                    data_spec.check_time = now_time
                    self.tbif.update_workflow_data(data_spec)
                case WFDataStatus.checked_exist:
                    # Data already fully exist, advance to done_exist
                    data_spec.status = WFDataStatus.done_exist
                    data_spec.check_time = now_time
                    data_spec.end_time = now_time
                    self.tbif.update_workflow_data(data_spec)
            tmp_log.info(f"Done, from {original_status} to status={data_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"{process_result.message}")
        return process_result

    def process_data_generating(self, data_spec: WFDataSpec) -> WFDataProcessResult:
        """
        Process data in generating status
        To check the status of the data being generated

        Args:
            data_spec (WFDataSpec): The workflow data specification to process

        Returns:
            WFDataProcessResult: The result of processing the data
        """
        tmp_log = LogWrapper(logger, f"process_data_generating data_id={data_spec.data_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFDataProcessResult()
        # Check status
        if data_spec.status not in (WFDataStatus.generating_start, WFDataStatus.generating_ready):
            process_result.message = f"Data status changed unexpectedly from generating_* to {data_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            original_status = data_spec.status
            # Get the data handler plugin
            data_handler = self.get_plugin("data_handler", data_spec.flavor)
            # Check the data status
            check_result = data_handler.check_target(data_spec)
            if not check_result.success or check_result.data_status is None:
                process_result.message = f"Failed to check data; {check_result.message}"
                tmp_log.error(f"{process_result.message}")
                return process_result
            # Update data status
            now_time = naive_utcnow()
            match original_status:
                case WFDataStatus.generating_start:
                    if check_result.data_status in WFDataStatus.after_generating_start_statuses:
                        # Data status advanced
                        data_spec.status = check_result.data_status
                        process_result.new_status = data_spec.status
                    elif check_result.data_status == WFDataStatus.generating_start:
                        # Still in generating_start, do nothing
                        pass
                    else:
                        tmp_log.warning(f"Invalid data_status {check_result.data_status} from target check result; skipped")
                    self.tbif.update_workflow_data(data_spec)
                case WFDataStatus.generating_ready:
                    if check_result.data_status in WFDataStatus.after_generating_ready_statuses:
                        # Data status advanced to terminal
                        data_spec.status = check_result.data_status
                        process_result.new_status = data_spec.status
                        data_spec.end_time = now_time
                    elif check_result.data_status == WFDataStatus.generating_ready:
                        # Still in generating_ready, do nothing
                        pass
                    else:
                        tmp_log.warning(f"Invalid data_status {check_result.data_status} from target check result; skipped")
            data_spec.check_time = now_time
            self.tbif.update_workflow_data(data_spec)
            tmp_log.info(f"Done, from {original_status} to status={data_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"{process_result.message}")
        return process_result

    def process_data_waiting(self, data_spec: WFDataSpec) -> WFDataProcessResult:
        """
        Process data in waiting status
        To check the status of the data being waited for, probably generating by other workflow steps or external sources

        Args:
            data_spec (WFDataSpec): The workflow data specification to process

        Returns:
            WFDataProcessResult: The result of processing the data
        """
        tmp_log = LogWrapper(logger, f"process_data_waiting data_id={data_spec.data_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFDataProcessResult()
        # Check status
        if data_spec.status not in (WFDataStatus.waiting_ready,):
            process_result.message = f"Data status changed unexpectedly from waiting_* to {data_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            original_status = data_spec.status
            # Get the data handler plugin
            data_handler = self.get_plugin("data_handler", data_spec.flavor)
            # Check the data status
            check_result = data_handler.check_target(data_spec)
            if not check_result.success or check_result.data_status is None:
                process_result.message = f"Failed to check data; {check_result.message}"
                tmp_log.error(f"{process_result.message}")
                return process_result
            # Update data status
            now_time = naive_utcnow()
            match original_status:
                case WFDataStatus.waiting_ready:
                    if check_result.data_status in WFDataStatus.after_waiting_ready_statuses:
                        # Data status advanced to terminal
                        data_spec.status = check_result.data_status
                        process_result.new_status = data_spec.status
                        data_spec.end_time = now_time
                    elif check_result.data_status == WFDataStatus.waiting_ready:
                        # Still in waiting_ready, do nothing
                        pass
                    else:
                        tmp_log.warning(f"Invalid data_status {check_result.data_status} from target check result; skipped")
            data_spec.check_time = now_time
            self.tbif.update_workflow_data(data_spec)
            tmp_log.info(f"Done, from {original_status} to status={data_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"{process_result.message}")
        return process_result

    def process_data(self, data_specs: List[WFDataSpec]) -> Dict:
        """
        Process a list of workflow data specifications

        Args:
            data_specs (List[WFDataSpec]): List of workflow data specifications to process

        Returns:
            Dict: Statistics of the processing results
        """
        tmp_log = LogWrapper(logger, f"process_data workflow_id={data_specs[0].workflow_id}")
        n_data = len(data_specs)
        tmp_log.debug(f"Start, processing {n_data} data specs")
        data_status_stats = {"n_data": n_data, "changed": {}, "unchanged": {}, "processed": {}, "n_processed": 0}
        for data_spec in data_specs:
            with self.workflow_data_lock(data_spec.data_id) as locked_data_spec:
                if locked_data_spec is None:
                    tmp_log.warning(f"Failed to acquire lock for data_id={data_spec.data_id}; skipped")
                    continue
                data_spec = locked_data_spec
                # Process the data
                tmp_res = None
                match data_spec.status:
                    case WFDataStatus.registered:
                        tmp_res = self.process_data_registered(data_spec)
                    case WFDataStatus.checking:
                        tmp_res = self.process_data_checking(data_spec)
                    case WFDataStatus.checked_nonex | WFDataStatus.checked_partex | WFDataStatus.checked_exist:
                        tmp_res = self.process_data_checked(data_spec)
                    case WFDataStatus.generating_start | WFDataStatus.generating_ready:
                        tmp_res = self.process_data_generating(data_spec)
                    case WFDataStatus.waiting_ready:
                        tmp_res = self.process_data_waiting(data_spec)
                    case _:
                        tmp_log.debug(f"Data status {data_spec.status} is not handled in this context; skipped")
                        continue
                if tmp_res and tmp_res.success:
                    # update stats
                    if tmp_res.new_status and tmp_res.new_status != data_spec.status:
                        data_status_stats["changed"].setdefault(data_spec.status, 0)
                        data_status_stats["changed"][data_spec.status] += 1
                    else:
                        data_status_stats["unchanged"].setdefault(data_spec.status, 0)
                        data_status_stats["unchanged"][data_spec.status] += 1
                    data_status_stats["processed"].setdefault(data_spec.status, 0)
                    data_status_stats["processed"][data_spec.status] += 1
                    data_status_stats["n_processed"] += 1
        tmp_log.info(f"Done, processed data specs: {data_status_stats}")

    # ---- Step status transitions -----------------------------

    def process_step_registered(self, step_spec: WFStepSpec) -> WFStepProcessResult:
        """
        Process a step in registered status
        To prepare for checking the step

        Args:
            step_spec (WFStepSpec): The workflow step specification to process

        Returns:
            WFStepProcessResult: The result of processing the step
        """
        tmp_log = LogWrapper(logger, f"process_step_registered step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFStepProcessResult()
        # Check status
        if step_spec.status != WFStepStatus.registered:
            process_result.message = f"Step status changed unexpectedly from {WFStepStatus.registered} to {step_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            step_spec.status = WFStepStatus.checking
            self.tbif.update_workflow_step(step_spec)
            tmp_log.info(f"Done, status={step_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"{process_result.message}")
        return process_result

    def process_step_checking(self, step_spec: WFStepSpec) -> WFStepProcessResult:
        """
        Process a step in checking status
        To check the conditions about whether to process the step

        Args:
            step_spec (WFStepSpec): The workflow step specification to process

        Returns:
            WFStepProcessResult: The result of processing the step
        """
        tmp_log = LogWrapper(logger, f"process_step_checking workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFStepProcessResult()
        # Check status
        if step_spec.status != WFStepStatus.checking:
            process_result.message = f"Step status changed unexpectedly from {WFStepStatus.checking} to {step_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # FIXME: For now, always advance to checked_true
            if True:
                step_spec.status = WFStepStatus.checked_true
                self.tbif.update_workflow_step(step_spec)
                tmp_log.info(f"Done, status={step_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"{process_result.message}")
        return process_result

    def process_step_checked(self, step_spec: WFStepSpec) -> WFStepProcessResult:
        """
        Process a step in checked status
        To advance to pending or closed based on check result

        Args:
            step_spec (WFStepSpec): The workflow step specification to process

        Returns:
            WFStepProcessResult: The result of processing the step
        """
        tmp_log = LogWrapper(logger, f"process_step_checked workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFStepProcessResult()
        # Check status
        if step_spec.status not in WFStepStatus.checked_statuses:
            process_result.message = f"Step status changed unexpectedly from checked_* to {step_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        original_status = step_spec.status
        try:
            now_time = naive_utcnow()
            match step_spec.status:
                case WFStepStatus.checked_true:
                    # Conditions met, advance to pending
                    step_spec.status = WFStepStatus.pending
                    step_spec.check_time = now_time
                    self.tbif.update_workflow_step(step_spec)
                case WFStepStatus.checked_false:
                    # Conditions not met, advanced to closed
                    step_spec.status = WFStepStatus.closed
                    step_spec.check_time = now_time
                    self.tbif.update_workflow_step(step_spec)
            tmp_log.info(f"Done, from {original_status} to status={step_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"{process_result.message}")
        return process_result

    def process_step_pending(self, step_spec: WFStepSpec, data_spec_map: Dict[str, WFDataSpec] | None = None) -> WFStepProcessResult:
        """
        Process a step in pending status
        To check the inputs of the step

        Args:
            step_spec (WFStepSpec): The workflow step specification to process
            data_spec_map (Dict[str, WFDataSpec] | None): Optional map of data name to WFDataSpec for the workflow

        Returns:
            WFStepProcessResult: The result of processing the step
        """
        tmp_log = LogWrapper(logger, f"process_step_pending workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFStepProcessResult()
        # Check status
        if step_spec.status != WFStepStatus.pending:
            process_result.message = f"Step status changed unexpectedly from {WFStepStatus.pending} to {step_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # Get data spec map of the workflow
            if data_spec_map is None:
                data_specs = self.tbif.get_workflow_data(workflow_id=step_spec.workflow_id)
                data_spec_map = {data_spec.name: data_spec for data_spec in data_specs}
            # Input data list of the step
            step_spec_definition = step_spec.definition_json_map
            input_data_list = step_spec_definition.get("input_data_list")
            if input_data_list is None:
                process_result.message = f"Step definition does not have input_data_list; skipped"
                tmp_log.warning(f"{process_result.message}")
                return process_result
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
                process_result.success = True
                return process_result
            # All inputs are good, register outputs of the step and update step status to ready
            tmp_log.debug(f"All input data are good; proceeding")
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
                    data_spec.flavor = "ddm_collection"  # FIXME: hardcoded flavor, should be configurable
                    data_spec.creation_time = now_time
                    self.tbif.insert_workflow_data(data_spec)
                    tmp_log.debug(f"Registered mid data {output_data_name} of step_id={step_spec.step_id}")
                    # update data_spec_map
                    data_spec_map[output_data_name] = data_spec
            step_spec.status = WFStepStatus.ready
            self.tbif.update_workflow_step(step_spec)
            process_result.success = True
            process_result.new_status = step_spec.status
            tmp_log.info(f"Done, status={step_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return process_result

    def process_step_ready(self, step_spec: WFStepSpec) -> WFStepProcessResult:
        """
        Process a step in ready status
        To submit the step for execution

        Args:
            step_spec (WFStepSpec): The workflow step specification to process

        Returns:
            WFStepProcessResult: The result of processing the step
        """
        tmp_log = LogWrapper(logger, f"process_step_ready workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFStepProcessResult()
        # Check status
        if step_spec.status != WFStepStatus.ready:
            process_result.message = f"Step status changed unexpectedly from {WFStepStatus.ready} to {step_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # Get the step handler plugin
            step_handler = self.get_plugin("step_handler", step_spec.flavor)
            # Submit the step
            submit_result = step_handler.submit_target(step_spec)
            if not submit_result.success or submit_result.target_id is None:
                process_result.message = f"Failed to submit step; {submit_result.message}"
                tmp_log.error(f"{process_result.message}")
                return process_result
            # Update step status to submitted
            step_spec.target_id = submit_result.target_id
            step_spec.status = WFStepStatus.submitted
            self.tbif.update_workflow_step(step_spec)
            process_result.success = True
            process_result.new_status = step_spec.status
            tmp_log.info(f"Submitted step, flavor={step_spec.flavor}, target_id={step_spec.target_id}, status={step_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return process_result

    def process_step_submitted(self, step_spec: WFStepSpec) -> WFStepProcessResult:
        """
        Process a step in submitted status
        To check the status of the submitted step

        Args:
            step_spec (WFStepSpec): The workflow step specification to process

        Returns:
            WFStepProcessResult: The result of processing the step
        """
        tmp_log = LogWrapper(logger, f"process_step_submitted workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFStepProcessResult()
        # Check status
        if step_spec.status != WFStepStatus.submitted:
            process_result.message = f"Step status changed unexpectedly from {WFStepStatus.submitted} to {step_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # Get the step handler plugin
            step_handler = self.get_plugin("step_handler", step_spec.flavor)
            # Check the step status
            check_result = step_handler.check_target(step_spec)
            if not check_result.success or check_result.step_status is None:
                process_result.message = f"Failed to check step; {check_result.message}"
                tmp_log.error(f"{process_result.message}")
                return process_result
            # Update step status
            if check_result.step_status in WFStepStatus.after_submitted_statuses:
                # Step status advanced
                step_spec.status = check_result.step_status
                process_result.new_status = step_spec.status
            elif check_result.step_status == WFStepStatus.submitted:
                # Still in submitted, do nothing
                pass
            else:
                tmp_log.warning(f"Invalid step_status {check_result.step_status} from target check result; skipped")
            now_time = naive_utcnow()
            step_spec.check_time = now_time
            self.tbif.update_workflow_step(step_spec)
            process_result.success = True
            tmp_log.info(f"Checked step, flavor={step_spec.flavor}, target_id={step_spec.target_id}, status={step_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return process_result

    def process_step_running(self, step_spec: WFStepSpec) -> WFStepProcessResult:
        """
        Process a step in running status
        To check the status of the running step

        Args:
            step_spec (WFStepSpec): The workflow step specification to process

        Returns:
            WFStepProcessResult: The result of processing the step
        """
        tmp_log = LogWrapper(logger, f"process_step_running workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WFStepProcessResult()
        # Check status
        if step_spec.status != WFStepStatus.running:
            process_result.message = f"Step status changed unexpectedly from {WFStepStatus.running} to {step_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # Get the step handler plugin
            step_handler = self.get_plugin("step_handler", step_spec.flavor)
            # Check the step status
            check_result = step_handler.check_target(step_spec)
            if not check_result.success or check_result.step_status is None:
                process_result.message = f"Failed to check step; {check_result.message}"
                tmp_log.error(f"{process_result.message}")
                return process_result
            # Update step status
            if check_result.step_status in WFStepStatus.after_running_statuses:
                # Step status advanced
                step_spec.status = check_result.step_status
                process_result.new_status = step_spec.status
            elif check_result.step_status == WFStepStatus.running:
                # Still in running, do nothing
                pass
            else:
                tmp_log.warning(f"Invalid step_status {check_result.step_status} from target check result; skipped")
            now_time = naive_utcnow()
            step_spec.check_time = now_time
            self.tbif.update_workflow_step(step_spec)
            process_result.success = True
            tmp_log.info(f"Checked step, flavor={step_spec.flavor}, target_id={step_spec.target_id}, status={step_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return process_result

    def process_steps(self, step_specs: List[WFStepSpec], data_spec_map: Dict[str, WFDataSpec] | None = None) -> Dict:
        """
        Process a list of workflow steps

        Args:
            step_specs (List[WFStepSpec]): List of workflow step specifications to process
            data_spec_map (Dict[str, WFDataSpec] | None): Optional map of data name to WFDataSpec for the workflow

        Returns:
            Dict: Statistics of the processing results
        """
        tmp_log = LogWrapper(logger, f"process_steps workflow_id={step_spec.workflow_id}")
        n_steps = len(step_specs)
        tmp_log.debug(f"Start, processing {n_steps} steps")
        steps_status_stats = {"n_steps": n_steps, "changed": {}, "unchanged": {}, "processed": {}, "n_processed": 0}
        for step_spec in step_specs:
            with self.workflow_step_lock(step_spec.step_id) as locked_step_spec:
                if locked_step_spec is None:
                    tmp_log.warning(f"Failed to acquire lock for step_id={step_spec.step_id}; skipped")
                    continue
                step_spec = locked_step_spec
                # Process the step
                tmp_res = None
                match step_spec.status:
                    case WFStepStatus.registered:
                        tmp_res = self.process_step_registered(step_spec)
                    case WFStepStatus.pending:
                        tmp_res = self.process_step_pending(step_spec, data_spec_map=data_spec_map)
                    case WFStepStatus.ready:
                        tmp_res = self.process_step_ready(step_spec)
                    case WFStepStatus.submitted:
                        tmp_res = self.process_step_submitted(step_spec)
                    case WFStepStatus.running:
                        tmp_res = self.process_step_running(step_spec)
                    case _:
                        tmp_log.debug(f"Step status {step_spec.status} is not handled in this context; skipped")
                        continue
                if tmp_res and tmp_res.success:
                    # update stats
                    if tmp_res.new_status and tmp_res.new_status != step_spec.status:
                        steps_status_stats["changed"].setdefault(step_spec.status, 0)
                        steps_status_stats["changed"][step_spec.status] += 1
                    else:
                        steps_status_stats["unchanged"].setdefault(step_spec.status, 0)
                        steps_status_stats["unchanged"][step_spec.status] += 1
                    steps_status_stats["processed"].setdefault(step_spec.status, 0)
                    steps_status_stats["processed"][step_spec.status] += 1
                    steps_status_stats["n_processed"] += 1
        tmp_log.info(f"Done, processed steps: {steps_status_stats}")

    # ---- Workflow status transitions -------------------------

    def process_workflow_registered(self, workflow_spec: WorkflowSpec) -> WorkflowProcessResult:
        """
        Process a workflow in registered status
        To parse to get workflow definition from raw request

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process

        Returns:
            WorkflowProcessResult: The result of processing the workflow
        """
        tmp_log = LogWrapper(logger, f"process_workflow_registered workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WorkflowProcessResult()
        # Check status
        if workflow_spec.status != WorkflowStatus.registered:
            process_result.message = f"Workflow status changed unexpectedly from {WorkflowStatus.registered} to {workflow_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
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
                is_ok, is_fatal, workflow_definition = parse_raw_request(
                    sandbox_url=sandbox_url,
                    log_token=log_token,
                    user_name=workflow_spec.username,
                    raw_request_dict=raw_request_dict,
                )
                # Failure handling
                # if is_fatal:
                if False:  # disable fatal for now
                    process_result.message = f"Fatal error in parsing raw request; cancelled the workflow"
                    tmp_log.error(f"{process_result.message}")
                    workflow_spec.status = WorkflowStatus.cancelled
                    workflow_spec.set_parameter("cancel_reason", "Fatal error in parsing raw request")
                    self.tbif.update_workflow(workflow_spec)
                    return process_result
                if not is_ok:
                    process_result.message = f"Failed to parse raw request; skipped"
                    tmp_log.warning(f"{process_result.message}")
                    return process_result
                # extra info from raw request
                workflow_definition["user_dn"] = raw_request_dict.get("user_dn")
                # Parsed successfully, update definition
                workflow_spec.definition_json = json.dumps(workflow_definition, default=json_serialize_default)
                tmp_log.debug(f"Parsed raw request into definition")
            # Update status to parsed
            # workflow_spec.status = WorkflowStatus.parsed
            workflow_spec.status = WorkflowStatus.checked  # skip parsed for now
            # Update DB
            self.tbif.update_workflow(workflow_spec)
            process_result.success = True
            process_result.new_status = workflow_spec.status
            tmp_log.info(f"Done, status={workflow_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return process_result

    def process_workflow_checked(self, workflow_spec: WorkflowSpec) -> WorkflowProcessResult:
        """
        Process a workflow in checked status
        Register steps, and update its status
        Parse raw request into workflow definition, register steps, and update its status

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process

        Returns:
            WorkflowProcessResult: The result of processing the workflow
        """
        tmp_log = LogWrapper(logger, f"process_workflow_checked workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WorkflowProcessResult()
        # Check status
        if workflow_spec.status != WorkflowStatus.checked:
            process_result.message = f"Workflow status changed unexpectedly from {WorkflowStatus.checked} to {workflow_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # Parse the workflow definition
            workflow_definition = workflow_spec.definition_json_map
            if workflow_definition is None:
                process_result.message = f"Workflow definition is None; cancelled the workflow"
                tmp_log.error(f"{process_result.message}")
                workflow_spec.status = WorkflowStatus.cancelled
                workflow_spec.set_parameter("cancel_reason", "Workflow definition is None")
                self.tbif.update_workflow(workflow_spec)
                return process_result
            # initialize
            data_specs = []
            step_specs = []
            now_time = naive_utcnow()
            # Register root inputs and outputs
            for input_name, input_target in workflow_definition["root_inputs"].items():
                data_spec = WFDataSpec()
                data_spec.workflow_id = workflow_spec.workflow_id
                data_spec.name = input_name
                data_spec.target_id = input_target
                data_spec.status = WFDataStatus.registered
                data_spec.type = WFDataType.input
                data_spec.flavor = "ddm_collection"  # FIXME: hardcoded flavor, should be configurable
                data_spec.creation_time = now_time
                data_specs.append(data_spec)
            for output_name, output_dict in workflow_definition["root_outputs"].items():
                data_spec = WFDataSpec()
                data_spec.workflow_id = workflow_spec.workflow_id
                data_spec.name = output_name
                data_spec.target_id = output_dict.get("value")
                data_spec.status = WFDataStatus.registered
                data_spec.type = WFDataType.output
                data_spec.flavor = "ddm_collection"  # FIXME: hardcoded flavor, should be configurable
                data_spec.creation_time = now_time
                data_specs.append(data_spec)
            # Register steps based on nodes in the definition
            for node in workflow_definition["nodes"]:
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
                    # propagate user name and DN from workflow to step
                    step_definition["user_name"] = workflow_spec.username
                    step_definition["user_dn"] = workflow_definition.get("user_dn")
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
            # Update status to starting
            workflow_spec.status = WorkflowStatus.starting
            # Upsert DB
            self.tbif.upsert_workflow_entities(
                workflow_spec.workflow_id,
                actions_dict={"workflow": "update", "steps": "insert", "data": "insert"},
                workflow_spec=workflow_spec,
                step_specs=step_specs,
                data_specs=data_specs,
            )
            process_result.success = True
            process_result.new_status = workflow_spec.status
            tmp_log.info(f"Done, inserted {len(step_specs)} steps and {len(data_specs)} data, status={workflow_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return process_result

    def process_workflow_starting(self, workflow_spec: WorkflowSpec) -> WorkflowProcessResult:
        """
        Process a workflow in starting status
        To start the steps in the workflow

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process

        Returns:
            WorkflowProcessResult: The result of processing the workflow
        """
        tmp_log = LogWrapper(logger, f"process_workflow_starting workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WorkflowProcessResult()
        # Check status
        if workflow_spec.status != WorkflowStatus.starting:
            process_result.message = f"Workflow status changed unexpectedly from {WorkflowStatus.starting} to {workflow_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # Get data
            data_specs = self.tbif.get_data_of_workflow(workflow_id=workflow_spec.workflow_id)
            # Get steps in registered status
            required_step_statuses = [WFStepStatus.registered, WFStepStatus.pending, WFStepStatus.ready, WFStepStatus.submitted]
            over_advanced_step_statuses = [WFStepStatus.running, WFStepStatus.done, WFStepStatus.failed]
            step_specs = self.tbif.get_steps_of_workflow(workflow_id=workflow_spec.workflow_id, status_filter_list=required_step_statuses)
            over_advanced_step_specs = self.tbif.get_steps_of_workflow(workflow_id=workflow_spec.workflow_id, status_filter_list=over_advanced_step_statuses)
            if not step_specs:
                process_result.message = f"No step in required status; skipped"
                tmp_log.warning(f"{process_result.message}")
                return process_result
            if over_advanced_step_specs:
                process_result.message = f"Some steps are not in required status; force to advance the workflow"
                tmp_log.warning(f"{process_result.message}")
                # Advance the workflow to running directly
                workflow_spec.status = WorkflowStatus.running
                workflow_spec.start_time = naive_utcnow()
                self.tbif.update_workflow(workflow_spec)
                process_result.success = True
                process_result.new_status = workflow_spec.status
                tmp_log.info(f"Done, forced advanced to status={workflow_spec.status}")
                return process_result
            # Get data spec map of the workflow
            data_specs = self.tbif.get_data_of_workflow(workflow_id=workflow_spec.workflow_id)
            data_spec_map = {data_spec.name: data_spec for data_spec in data_specs}
            # Process steps
            steps_status_stats = self.process_steps(step_specs, data_spec_map=data_spec_map)
            # Update workflow status to running if any of step is submitted
            if steps_status_stats["processed"].get(WFStepStatus.submitted):
                workflow_spec.status = WorkflowStatus.running
                workflow_spec.start_time = naive_utcnow()
                self.tbif.update_workflow(workflow_spec)
                process_result.success = True
                process_result.new_status = workflow_spec.status
                tmp_log.info(f"Done, advanced to status={workflow_spec.status}")
            else:
                process_result.success = True
                tmp_log.info(f"Done, status remains {workflow_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return process_result

    def process_workflow_running(self, workflow_spec: WorkflowSpec) -> WorkflowProcessResult:
        """
        Process a workflow in running status
        To monitor the steps in the workflow

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process

        Returns:
            WorkflowProcessResult: The result of processing the workflow
        """
        tmp_log = LogWrapper(logger, f"process_workflow_running workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("Start")
        # Initialize
        process_result = WorkflowProcessResult()
        # Check status
        if workflow_spec.status != WorkflowStatus.running:
            process_result.message = f"Workflow status changed unexpectedly from {WorkflowStatus.running} to {workflow_spec.status}; skipped"
            tmp_log.warning(f"{process_result.message}")
            return process_result
        # Process
        try:
            # Get steps
            step_specs = self.tbif.get_steps_of_workflow(workflow_id=workflow_spec.workflow_id)
            if not step_specs:
                process_result.message = f"No step in required status; skipped"
                tmp_log.warning(f"{process_result.message}")
                return process_result
            # Get data spec map of the workflow
            data_specs = self.tbif.get_data_of_workflow(workflow_id=workflow_spec.workflow_id)
            data_spec_map = {data_spec.name: data_spec for data_spec in data_specs}
            output_data_spec_map = {data_spec.name: data_spec for data_spec in data_specs if data_spec.type == WFDataType.output}
            # Check if all output data are good
            all_outputs_good = None
            for output_data_name, output_data_spec in output_data_spec_map.items():
                if output_data_spec.status in WFDataStatus.good_output_statuses:
                    if all_outputs_good is None:
                        all_outputs_good = True
                else:
                    all_outputs_good = False
                    break
            if all_outputs_good is True:
                # All outputs are good, mark the workflow as done
                workflow_spec.status = WorkflowStatus.done
                workflow_spec.end_time = naive_utcnow()
                self.tbif.update_workflow(workflow_spec)
                process_result.success = True
                process_result.new_status = workflow_spec.status
                tmp_log.info(f"Done, all output data are good; advanced to status={workflow_spec.status}")
                return process_result
            # Process each step
            steps_status_stats = self.process_steps(step_specs, data_spec_map=data_spec_map)
            # Update workflow status by steps
            if (processed_steps_stats := steps_status_stats["processed"]) and (
                processed_steps_stats.get(WFStepStatus.failed) or processed_steps_stats.get(WFStepStatus.cancelled)
            ):
                # TODO: cancel all unfinished steps
                # self.cancel_step(...)
                # mark workflow as failed
                tmp_log.warning(f"workflow failed due to some steps failed or cancelled")
                workflow_spec.status = WorkflowStatus.failed
                workflow_spec.start_time = naive_utcnow()
                self.tbif.update_workflow(workflow_spec)
                process_result.success = True
                process_result.new_status = workflow_spec.status
                tmp_log.info(f"Done, advanced to status={workflow_spec.status}")
            else:
                process_result.success = True
                tmp_log.info(f"Done, status remains {workflow_spec.status}")
        except Exception as e:
            process_result.message = f"Got error {str(e)}"
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return process_result

    def process_workflow(self, workflow_spec: WorkflowSpec) -> WorkflowProcessResult:
        """
        Process a workflow based on its current status

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process

        Returns:
            WorkflowProcessResult: The result of processing the workflow
        """
        tmp_log = LogWrapper(logger, f"process_workflow workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug(f"Start, current status={workflow_spec.status}")
        # Initialize
        process_result = WorkflowProcessResult()
        # Process based on status
        match workflow_spec.status:
            case WorkflowStatus.registered:
                process_result = self.process_workflow_registered(workflow_spec)
            case WorkflowStatus.checked:
                process_result = self.process_workflow_checked(workflow_spec)
            case WorkflowStatus.starting:
                process_result = self.process_workflow_starting(workflow_spec)
            case WorkflowStatus.running:
                process_result = self.process_workflow_running(workflow_spec)
            case _:
                process_result.message = f"Workflow status {workflow_spec.status} is not handled in this context; skipped"
                tmp_log.warning(f"{process_result.message}")
        return process_result

    # ---- Process all workflows -------------------------------------

    def process_active_workflows(self) -> Dict:
        """
        Process all active workflows in the system

        Returns:
            Dict: Statistics of the processing results
        """
        tmp_log = LogWrapper(logger, "process_active_workflows")
        tmp_log.debug("Start")
        # Initialize
        workflows_status_stats = {"n_workflows": 0, "changed": {}, "unchanged": {}, "processed": {}, "n_processed": 0}
        try:
            # Get workflows
            workflow_specs = self.tbif.get_workflows(status_filter_list=WorkflowStatus.active_statuses, check_interval=WORKFLOW_CHECK_INTERVAL_SEC)
            n_workflows = len(workflow_specs)
            tmp_log.debug(f"Got {n_workflows} workflows to process")
            if n_workflows == 0:
                tmp_log.info("Done, no workflow to process")
                return workflows_status_stats
            # Process each workflow
            for workflow_spec in workflow_specs:
                with self.workflow_lock(workflow_spec.workflow_id) as locked_workflow_spec:
                    if locked_workflow_spec is None:
                        tmp_log.warning(f"Failed to acquire lock for workflow_id={workflow_spec.workflow_id}; skipped")
                        continue
                    workflow_spec = locked_workflow_spec
                    # Process the workflow
                    tmp_res = self.process_workflow(workflow_spec)
                    if tmp_res and tmp_res.success:
                        # update stats
                        if tmp_res.new_status and tmp_res.new_status != workflow_spec.status:
                            workflows_status_stats["changed"].setdefault(workflow_spec.status, 0)
                            workflows_status_stats["changed"][workflow_spec.status] += 1
                        else:
                            workflows_status_stats["unchanged"].setdefault(workflow_spec.status, 0)
                            workflows_status_stats["unchanged"][workflow_spec.status] += 1
                        workflows_status_stats["processed"].setdefault(workflow_spec.status, 0)
                        workflows_status_stats["processed"][workflow_spec.status] += 1
                        workflows_status_stats["n_processed"] += 1
            workflows_status_stats["n_workflows"] = n_workflows
            tmp_log.info(f"Done, processed workflows: {workflows_status_stats}")
        except Exception as e:
            tmp_log.error(f"Got error ; {traceback.format_exc()}")
        return workflows_status_stats
