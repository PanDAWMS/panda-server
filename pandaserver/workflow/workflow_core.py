import copy
import functools
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

# import polars as pl  # isort:skip


# main logger
logger = PandaLogger().getLogger(__name__.split(".")[-1])

# named tuple for attribute with type
AttributeWithType = namedtuple("AttributeWithType", ["attribute", "type"])


# ===============================================================


def json_serialize_default(obj):
    """
    Default JSON serializer for non-serializable objects

    Args:
        obj (Any): Object to serialize

    Returns:
        Any: JSON serializable object
    """
    # convert set to list
    if isinstance(obj, set):
        return list(obj)
    return obj


# ===============================================================


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
        self, prodsourcelabel: str, username: str, workflow_name: str = None, workflow_definition: dict = None, raw_request_params: dict = None, *args, **kwargs
    ) -> int | None:
        """
        Register a new workflow

        Args:
            prodsourcelabel (str): Production source label for the workflow
            username (str): Username of the person registering the workflow
            workflow_name (str): Name of the workflow
            workflow_definition (dict): Dictionary of workflow definition
            raw_request_params (dict): Dictionary of parameters of the raw request
            *args: Additional arguments
            **kwargs: Additional keyword arguments

        Returns:
            int | None: The ID of the registered workflow if successful, otherwise None
        """
        tmp_log = LogWrapper(logger, f"register_workflow prodsourcelabel={prodsourcelabel} username={username} name={workflow_name}")
        tmp_log.debug("start")
        # Implementation of workflow registration logic
        ...
        workflow_spec = WorkflowSpec()
        workflow_spec.prodsourcelabel = prodsourcelabel
        workflow_spec.username = username
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
        workflow_spec.status = "registered"
        # Insert to DB
        ret_workflow_id = self.tbif.insert_workflow(workflow_spec)
        if ret_workflow_id is None:
            tmp_log.error(f"Failed to register workflow")
            return None
        tmp_log.info(f"Registered workflow workflow_id={ret_workflow_id}")
        return ret_workflow_id

    # ---- Workflow status transitions -------------------------

    def process_workflow_registered(self, workflow_spec: WorkflowSpec):
        """
        Process a workflow in registered status
        Parse the workflow definition, register steps, and update its status

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process
        """
        tmp_log = LogWrapper(logger, f"process_workflow_registered workflow_id={workflow_spec.workflow_id}")
        tmp_log.debug("start")
        # try:
        #     # Parse the workflow definition
        #     workflow_definition_dict = json.loads(workflow_spec.definition_json)
        #     # initialize
        #     data_specs = []
        #     step_specs = []
        #     now_time = naive_utcnow()
        #     # Register root inputs and outputs
        #     for input_name, input_target in workflow_definition_dict["root_inputs"].items():
        #         data_spec = WFDataSpec()
        #         data_spec.workflow_id = workflow_spec.workflow_id
        #         data_spec.name = input_name
        #         data_spec.target_id = input_target
        #         data_spec.status = WFDataStatus.registered
        #         data_spec.type = WFDataType.input
        #         data_spec.flavor = "ddm_ds"  # FIXME: hardcoded flavor, should be configurable
        #         data_spec.creation_time = now_time
        #         data_specs.append(data_spec)
        #     for output_name, output_dict in workflow_definition_dict["root_outputs"].items():
        #         data_spec = WFDataSpec()
        #         data_spec.workflow_id = workflow_spec.workflow_id
        #         data_spec.name = output_name
        #         data_spec.target_id = output_dict.get("value")
        #         data_spec.status = WFDataStatus.registered
        #         data_spec.type = WFDataType.output
        #         data_spec.flavor = "ddm_ds"  # FIXME: hardcoded flavor, should be configurable
        #         data_spec.creation_time = now_time
        #         data_specs.append(data_spec)
        #     # Register steps based on nodes in the definition
        #     for node in workflow_definition_dict["nodes"]:
        #         # FIXME: not yet consider scatter, condition, loop, etc.
        #         if not (node.get("condition") or node.get("scatter") or node.get("loop")):
        #             step_spec = WFStepSpec()
        #             step_spec.workflow_id = workflow_spec.workflow_id
        #             step_spec.member_id = node["id"]
        #             step_spec.name = node["name"]
        #             step_spec.status = "registered"
        #             step_spec.type = WFStepType.ordinary
        #             step_spec.flavor = "panda_task"  # FIXME: hardcoded flavor, should be configurable
        #             step_spec.definition_json = json.dumps(node, default=json_serialize_default)
        #             step_spec.creation_time = now_time
        #             step_specs.append(step_spec)
        #     # FIXME: temporary, skip data checking and go to starting directly
        #     workflow_spec.status = "starting"
        #     # Upsert DB
        #     self.tbif.upsert_workflow_entities(
        #         workflow_spec.workflow_id,
        #         actions_dict={"workflow": "update", "steps": "insert", "data": "insert"},
        #         workflow_spec=workflow_spec,
        #         step_specs=step_specs,
        #         data_specs=data_specs,
        #     )
        #     tmp_log.info(f"Processed workflow registered, workflow_id={workflow_spec.workflow_id}, steps={len(step_specs)}, data={len(data_specs)}")
        # except Exception:
        #     tmp_log.error(f"got error ; {traceback.format_exc()}")

    # ---- Data status transitions -----------------------------
