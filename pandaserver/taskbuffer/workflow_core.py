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
        AttributeWithType("gshare", str),
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
        AttributeWithType("workflow_id", int),
        AttributeWithType("member_id", int),
        AttributeWithType("status", str),
        AttributeWithType("type", str),
        AttributeWithType("task_id", int),
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
        AttributeWithType("workflow_id", int),
        AttributeWithType("status", str),
        AttributeWithType("type", str),
        AttributeWithType("lfn", str),
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

    # Add methods for workflow management here
    # e.g., create_workflow, update_workflow, delete_workflow, etc.
