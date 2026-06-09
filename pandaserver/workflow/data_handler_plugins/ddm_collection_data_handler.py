import json
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.workflow.data_handler_plugins.base_data_handler import BaseDataHandler
from pandaserver.workflow.workflow_base import (
    WFDataSpec,
    WFDataStatus,
    WFDataTargetCheckResult,
    WFDataTargetCheckStatus,
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


class DDMCollectionDataHandler(BaseDataHandler):
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
        self.plugin_flavor = "ddm_collection"

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
        # Check data flavor
        if data_spec.flavor != self.plugin_flavor:
            tmp_log.warning(f"flavor={data_spec.flavor} not {self.plugin_flavor}; skipped")
            check_result.message = f"flavor not {self.plugin_flavor}; skipped"
            return check_result
        # Check DDM collection status
        collection = data_spec.target_id
        collection_meta = self.ddm_if.get_dataset_metadata(collection, ignore_missing=True)
        if collection_meta is None:
            check_result.success = False
            check_result.message = f"Failed to get metadata for collection {collection}"
            tmp_log.error(f"{check_result.message}")
            return check_result
        match collection_meta.get("state"):
            case DDMCollectionState.missing:
                check_result.check_status = WFDataTargetCheckStatus.nonexist
            case DDMCollectionState.open:
                if collection_meta.get("length", 0) == 0:
                    check_result.check_status = WFDataTargetCheckStatus.insuffi
                else:
                    check_result.check_status = WFDataTargetCheckStatus.suffice
            case DDMCollectionState.closed:
                check_result.check_status = WFDataTargetCheckStatus.complete
        check_result.metadata = collection_meta
        check_result.success = True
        tmp_log.info(f"Got collection {collection} check_status={check_result.check_status}")
        return check_result

    def combine_targets(self, target_ids: list, combined_name: str | None = None) -> str:
        """
        Combine multiple DDM collection target IDs into a single Rucio container.

        Args:
            target_ids (list): List of DDM collection names to aggregate.
            combined_name (str | None): Name for the new Rucio container. Should be caller-supplied
                and deterministic to ensure idempotency across retries.

        Returns:
            str: The combined container name, or empty string on failure.
        """
        tmp_log = LogWrapper(logger, "combine_targets")
        if not target_ids:
            tmp_log.warning("Empty target_ids list; returning empty string")
            return ""
        if len(target_ids) == 1:
            return target_ids[0]
        if combined_name is None:
            first = target_ids[0]
            scope = first.split(":")[0] if ":" in first else "user"
            combined_name = f"{scope}:wf_combined_{uuid.uuid4().hex}/"
        ok = self.ddm_if.register_container(combined_name, datasets=target_ids)
        if not ok:
            tmp_log.error(f"Failed to register combined container {combined_name}")
            return ""
        tmp_log.info(f"Registered combined container {combined_name} with {len(target_ids)} datasets")
        return combined_name
