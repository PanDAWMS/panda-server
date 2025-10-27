import json
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.workflow.data_handler_plugins.base_data_handler import BaseStepHandler
from pandaserver.workflow.workflow_base import (
    WFDataSpec,
    WFDataStatus,
    WFDataTargetCheckResult,
    WFDataType,
    WFStepSpec,
    WFStepStatus,
    WFStepType,
    WorkflowSpec,
    WorkflowStatus,
)

# main logger
logger = PandaLogger().getLogger(__name__.split(".")[-1])


class DDMCollectionDIDType:
    """
    Data Identifier Types for DDM Collections
    """

    DATASET = "DATASET"
    CONTAINER = "CONTAINER"


class DDMCollectionState:
    """
    States for DDM Collections
    """

    open = "open"
    closed = "closed"
    missing = "missing"


class DDMCollectionDataHandler(BaseStepHandler):
    """
    Handler for DDM collection data in the workflow.
    This class is responsible for managing the DDM collection data within a workflow.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the data handler with necessary parameters.
        """
        # Initialize base class or any required modules here
        super().__init__(*args, **kwargs)
        self.ddmIF = rucioAPI

    def check_target(self, data_spec: WFDataSpec, **kwargs) -> WFDataTargetCheckResult:
        """
        Check the status of the DDM collection data target.
        This method should be implemented to handle the specifics of DDM collection data status checking.

        Args:
            data_spec (WFDataSpec): The data specification containing details about the data to be checked.
            **kwargs: Additional keyword arguments that may be required for checking.

        Returns:
            WFDataTargetCheckResult: An object containing the result of the check, including success status, current data status, and message.
        """
        tmp_log = LogWrapper(logger, f"check_target workflow_id={data_spec.workflow_id} data_id={data_spec.data_id}")
        # Initialize
        check_result = WFDataTargetCheckResult()
        # Check data type
        if data_spec.type != WFDataType.ddm_collection:
            tmp_log.warning(f"type={data_spec.type} not ddm_collection; skipped")
            check_result.message = f"type not ddm_collection; skipped"
            return check_result
        # TODO: Implement the actual checking logic here
        collection = data_spec.target_id
        collection_meta = self.ddmIF.get_dataset_metadata(collection, ignore_missing=True)
        if collection_meta is None:
            check_result.success = False
            check_result.message = f"Failed to get metadata for collection {collection}"
            tmp_log.error(f"{check_result.message}")
            return check_result
        match collection_meta.get("state"):
            case DDMCollectionState.missing:
                check_result.status = WFDataStatus.generating_start
            case DDMCollectionState.open:
                check_result.status = WFDataStatus.generating_ready
            case DDMCollectionState.closed:
                check_result.status = WFDataStatus.done_generated
        check_result.metadata = collection_meta
