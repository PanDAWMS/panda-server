from typing import TYPE_CHECKING, Any

from pandaserver.workflow.workflow_base import (
    WFDataSpec,
    WFDataStatus,
    WFDataTargetCheckResult,
    WFDataType,
    WFStepSpec,
    WFStepStatus,
    WFStepTargetCheckResult,
    WFStepTargetSubmitResult,
    WFStepType,
    WorkflowSpec,
    WorkflowStatus,
)

if TYPE_CHECKING:
    # Importing this for real would put rucio behind every data handler, which none of
    # them needs to be constructed. Annotations are evaluated at runtime in this tree, so
    # the use below is quoted.
    from pandaserver.dataservice.ddm import RucioAPI


class BaseDataHandler:
    """
    Base class for data handlers in the workflow system.
    This class provides a common interface and some utility methods for data handlers.
    """

    def __init__(self, task_buffer: Any, ddm_if: "RucioAPI", *args: Any, **kwargs: Any) -> None:
        """
        Initialize the step handler with necessary parameters.

        Args:
            task_buffer: The task buffer interface to interact with the task database.
            ddm_if: The DDM interface to interact with the DDM system.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        # A TaskBuffer when the API server builds the workflow interface, or JEDI's
        # JediTaskBufferInterface, which forwards every method to JediTaskBuffer through
        # CommandSendInterface. panda-server cannot name the JEDI class and __getattr__ is
        # invisible to a type checker, so Any is as close as this gets.
        self.tbif = task_buffer
        self.ddm_if = ddm_if

    def check_target(self, data_spec: WFDataSpec, **kwargs: Any) -> WFDataTargetCheckResult:
        """
        Check the status of the data target.
        This method should be implemented by subclasses to handle the specifics of data target status checking.
        This method should NOT modify data_spec. Any update information should be stored in the WFStepTargetCheckResult returned instead.

        Args:
            data_spec (WFDataSpec): The data specification to check.
            **kwargs: Additional keyword arguments.

        Returns:
            WFDataTargetCheckResult: The result of the target check.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def combine_targets(self, target_ids: list[str], combined_name: str | None = None) -> str:
        """
        Combine multiple target IDs into a single new target via the plugin's functionality.

        Used to aggregate scatter-child outputs into one entity (e.g. a Rucio container) that
        can be stored as a single data_spec.target_id and consumed by downstream steps.

        Args:
            target_ids (list): List of raw target_id strings to combine.
            combined_name (str | None): Suggested name for the combined target. If None,
                the implementation may derive one. Should be deterministic for idempotency.

        Returns:
            str: The name of the combined target, or empty string on failure.
        """
        raise NotImplementedError("Subclasses must implement this method.")
