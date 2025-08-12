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
from dataclasses import MISSING, InitVar, asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.base import SpecBase
from pandacommon.pandautils.PandaUtils import get_sql_IN_bind_variables, naive_utcnow

from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI

import polars as pl  # isort:skip


# main logger
logger = PandaLogger().getLogger(__name__.split(".")[-1])

# named tuple for attribute with type
AttributeWithType = namedtuple("AttributeWithType", ["attribute", "type"])


# ==============================================================


class WorkflowBaseSpec(SpecBase):
    """
    Base class for workflow related specifications
    """

    @property
    def parameter_map(self) -> dict:
        """
        Get the dictionary parsed by the parameters attribute in JSON
        Possible parameters:
            ...

        Returns:
            dict : dict of parameters if it is JSON or empty dict if null
        """
        if self.parameters is None:
            return {}
        else:
            return json.loads(self.parameters)

    @parameter_map.setter
    def parameter_map(self, value_map: dict):
        """
        Set the dictionary and store in parameters attribute in JSON

        Args:
            value_map (dict): dict to set the parameter map
        """
        self.parameters = json.dumps(value_map)

    def get_parameter(self, param: str) -> Any:
        """
        Get the value of one parameter. None as default

        Args:
            param (str): parameter name

        Returns:
            Any : value of the parameter; None if parameter not set
        """
        tmp_dict = self.parameter_map
        return tmp_dict.get(param)

    def set_parameter(self, param: str, value):
        """
        Set the value of one parameter and store in parameters attribute in JSON

        Args:
            param (str): parameter name
            value (Any): value of the parameter to set; must be JSON-serializable
        """
        tmp_dict = self.parameter_map
        tmp_dict[param] = value
        self.parameter_map = tmp_dict

    def update_parameters(self, params: dict):
        """
        Update values of parameters with a dict and store in parameters attribute in JSON

        Args:
            params (dict): dict of parameter names and values to set
        """
        tmp_dict = self.parameter_map
        tmp_dict.update(params)
        self.parameter_map = tmp_dict


class WorkflowSpec(WorkflowBaseSpec):
    """
    Workflow specification
    """

    # attributes with types
    attributes_with_types = (
        AttributeWithType("workflow_id", int),
        AttributeWithType("name", str),
        AttributeWithType("status", str),
        AttributeWithType("prodsourcelabel", str),
        AttributeWithType("username", str),
        AttributeWithType("creation_time", datetime),
        AttributeWithType("start_time", datetime),
        AttributeWithType("end_time", datetime),
        AttributeWithType("modification_time", datetime),
        AttributeWithType("check_time", datetime),
        AttributeWithType("locked_by", str),
        AttributeWithType("lock_time", datetime),
        AttributeWithType("definition_json", str),
        AttributeWithType("parameters", str),
    )
    # attributes
    attributes = tuple([attr.attribute for attr in attributes_with_types])
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ()
    # mapping between sequence and attr
    _seqAttrMap = {"workflow_id": f"{panda_config.schemaJEDI}.WORKFLOW_ID_SEQ.nextval"}


class WFStepSpec(WorkflowBaseSpec):
    """
    Workflow Step specification
    """

    # attributes with types
    attributes_with_types = (
        AttributeWithType("step_id", int),
        AttributeWithType("name", str),
        AttributeWithType("workflow_id", int),
        AttributeWithType("member_id", int),
        AttributeWithType("type", str),
        AttributeWithType("status", str),
        AttributeWithType("flavor", str),
        AttributeWithType("target_id", str),
        AttributeWithType("creation_time", datetime),
        AttributeWithType("start_time", datetime),
        AttributeWithType("end_time", datetime),
        AttributeWithType("modification_time", datetime),
        AttributeWithType("check_time", datetime),
        AttributeWithType("locked_by", str),
        AttributeWithType("lock_time", datetime),
        AttributeWithType("parameters", str),
    )
    # attributes
    attributes = tuple([attr.attribute for attr in attributes_with_types])
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ()
    # mapping between sequence and attr
    _seqAttrMap = {"workflow_id": f"{panda_config.schemaJEDI}.WORKFLOW_STEP_ID_SEQ.nextval"}


class WFDataSpec(WorkflowBaseSpec):
    """
    Workflow Data specification
    """

    # attributes with types
    attributes_with_types = (
        AttributeWithType("data_id", int),
        AttributeWithType("name", str),
        AttributeWithType("workflow_id", int),
        AttributeWithType("type", str),
        AttributeWithType("status", str),
        AttributeWithType("flavor", str),
        AttributeWithType("target_id", str),
        AttributeWithType("creation_time", datetime),
        AttributeWithType("start_time", datetime),
        AttributeWithType("end_time", datetime),
        AttributeWithType("modification_time", datetime),
        AttributeWithType("check_time", datetime),
        AttributeWithType("locked_by", str),
        AttributeWithType("lock_time", datetime),
        AttributeWithType("parameters", str),
    )
    # attributes
    attributes = tuple([attr.attribute for attr in attributes_with_types])
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ()
    # mapping between sequence and attr
    _seqAttrMap = {"workflow_id": f"{panda_config.schemaJEDI}.WORKFLOW_DATA_ID_SEQ.nextval"}


# ==============================================================


class WorkflowInterface(object):
    """
    Interface for workflow management methods
    """

    def __init__(self, taskbuffer_if, *args, **kwargs):
        """
        Constructor

        Args:
            taskbuffer_if (TaskBufferInterface): Interface to the task buffer
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        self.tbif = taskbuffer_if
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

    def register_workflow(self, prodsourcelabel: str, username: str, name: str, workflow_definition_json: str, *args, **kwargs):
        """
        Register a new workflow

        Args:
            prodsourcelabel (str): Production source label for the workflow
            username (str): Username of the person registering the workflow
            name (str): Name of the workflow
            workflow_definition_json (str): JSON string defining the workflow
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        # Implementation of workflow registration logic
        ...
        workflow_spec = WorkflowSpec()
        workflow_spec.prodsourcelabel = prodsourcelabel
        workflow_spec.name = name
        workflow_spec.username = username
        workflow_spec.definition_json = workflow_definition_json
        workflow_spec.creation_time = naive_utcnow()
        workflow_spec.status = "registered"
        # Update DB
        ret_workflow_spec = self.tbif.update_workflow(workflow_spec, *args, **kwargs)
        if ret_workflow_spec is None:
            logger.error(f"Failed to register workflow prodsourcelabel={prodsourcelabel} name={name}")
            return None
        logger.info(f"Registered workflow prodsourcelabel={prodsourcelabel} name={name} workflow_id={ret_workflow_spec.workflow_id}")
        return ret_workflow_spec

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
