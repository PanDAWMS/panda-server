import json
from collections import namedtuple
from dataclasses import MISSING, InitVar, asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.base import SpecBase

from pandaserver.config import panda_config

# main logger
logger = PandaLogger().getLogger(__name__.split(".")[-1])

# named tuple for attribute with type
AttributeWithType = namedtuple("AttributeWithType", ["attribute", "type"])


# ==== Status of Entities ======================================


class WorkflowStatus(object):
    """
    Class to define the status of workflows
    """

    registered = "registered"
    parsed = "parsed"
    checking = "checking"
    checked = "checked"
    starting = "starting"
    running = "running"
    done = "done"
    failed = "failed"
    cancelled = "cancelled"


class WFStepStatus(object):
    """
    Class to define the status of workflow steps
    """

    registered = "registered"
    pending = "pending"
    ready = "ready"
    submitted = "submitted"
    running = "running"
    done = "done"
    failed = "failed"
    cancelled = "cancelled"


class WFDataStatus(object):
    """
    Class to define the status of workflow data
    """

    registered = "registered"
    checking = "checking"
    checked_nonex = "checked_nonex"
    checked_exist = "checked_exist"
    generating_start = "generating_start"
    generating_ready = "generating_ready"
    done_generated = "done_generated"
    done_skipped = "done_skipped"
    cancelled = "cancelled"
    retired = "retired"


# ==== Types ===================================================


class WFStepType(object):
    """
    Class to define the types of workflow steps
    """

    ...
    ordinary = "ordinary"


class WFDataType(object):
    """
    Class to define the types of workflow data
    """

    input = "input"
    output = "output"
    mid = "mid"


# ==== Specifications ==========================================


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
        AttributeWithType("parent_id", int),
        AttributeWithType("loop_count", int),
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
        AttributeWithType("raw_request_json", str),
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

    @property
    def raw_request_json_map(self) -> dict:
        """
        Get the dictionary parsed by raw_request_json attribute in JSON

        Returns:
            dict : dict of raw_request_json if it is JSON or empty dict if null
        """
        if self.raw_request_json is None:
            return {}
        else:
            return json.loads(self.raw_request_json)

    @raw_request_json_map.setter
    def raw_request_json_map(self, value_map: dict):
        """
        Set the dictionary and store in raw_request_json attribute in JSON

        Args:
            value_map (dict): dict to set the raw_request_json map
        """
        self.raw_request_json = json.dumps(value_map)

    @property
    def definition_json_map(self) -> dict:
        """
        Get the dictionary parsed by definition_json attribute in JSON

        Returns:
            dict : dict of definition_json if it is JSON or empty dict if null
        """
        if self.definition_json is None:
            return {}
        else:
            return json.loads(self.definition_json)

    @definition_json_map.setter
    def definition_json_map(self, value_map: dict):
        """
        Set the dictionary and store in definition_json attribute in JSON

        Args:
            value_map (dict): dict to set the definition_json map
        """
        self.definition_json = json.dumps(value_map)


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
    _seqAttrMap = {"step_id": f"{panda_config.schemaJEDI}.WORKFLOW_STEP_ID_SEQ.nextval"}

    @property
    def definition_json_map(self) -> dict:
        """
        Get the dictionary parsed by definition_json attribute in JSON

        Returns:
            dict : dict of definition_json if it is JSON or empty dict if null
        """
        if self.definition_json is None:
            return {}
        else:
            return json.loads(self.definition_json)

    @definition_json_map.setter
    def definition_json_map(self, value_map: dict):
        """
        Set the dictionary and store in definition_json attribute in JSON

        Args:
            value_map (dict): dict to set the definition_json map
        """
        self.definition_json = json.dumps(value_map)


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
    _seqAttrMap = {"data_id": f"{panda_config.schemaJEDI}.WORKFLOW_DATA_ID_SEQ.nextval"}
