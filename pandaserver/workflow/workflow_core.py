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

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.workflow.workflow_specs import WFDataSpec, WFStepSpec, WorkflowSpec

import polars as pl  # isort:skip


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

    def register_workflow(self, prodsourcelabel: str, username: str, workflow_name: str, workflow_definition_json: str, *args, **kwargs) -> int | None:
        """
        Register a new workflow

        Args:
            prodsourcelabel (str): Production source label for the workflow
            username (str): Username of the person registering the workflow
            name (str): Name of the workflow
            workflow_definition_json (str): JSON string defining the workflow
            *args: Additional arguments
            **kwargs: Additional keyword arguments

        Returns:
            int | None: The ID of the registered workflow if successful, otherwise None
        """
        # Implementation of workflow registration logic
        ...
        workflow_spec = WorkflowSpec()
        workflow_spec.prodsourcelabel = prodsourcelabel
        workflow_spec.name = workflow_name
        workflow_spec.username = username
        workflow_spec.definition_json = workflow_definition_json
        workflow_spec.creation_time = naive_utcnow()
        workflow_spec.status = "registered"
        # Insert to DB
        ret_workflow_id = self.tbif.insert_workflow(workflow_spec)
        if ret_workflow_id is None:
            logger.error(f"Failed to register workflow prodsourcelabel={prodsourcelabel} name={workflow_name}")
            return None
        logger.info(f"Registered workflow prodsourcelabel={prodsourcelabel} name={workflow_name} workflow_id={ret_workflow_id}")
        return ret_workflow_id

    #### Workflow status transitions

    def process_workflow_registered(self, workflow_spec: WorkflowSpec):
        """
        Process a workflow in registered status
        Parse the workflow definition, register steps, and update its status

        Args:
            workflow_spec (WorkflowSpec): The workflow specification to process
        """
        # Parse the workflow definition
        try:
            workflow_definition_dict = json.loads(workflow_spec.definition_json)
            # Register steps based on nodes in the definition
            for node in workflow_definition_dict["nodes"]:
                step_spec = WFStepSpec()
                step_spec.workflow_id = workflow_spec.workflow_id
                step_spec.member_id = node["id"]
                step_spec.status = "registered"
                step_spec.type = node.get("type", "default")
                # step_spec.parameters = json.dumps(node.get("parameters", {}))
                step_spec.creation_time = naive_utcnow()
        except Exception as e:
            logger.error(f"Failed to parse workflow definition for workflow_id={workflow_spec.workflow_id}: {e}")

        # FIXME: temporary, skip data checking and go to starting directly
        workflow_spec.status = "starting"
        # Update DB
        self.tbif.update_workflow(workflow_spec)
