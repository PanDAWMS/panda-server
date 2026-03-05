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


class BaseDataHandler:
    """
    Base class for data handlers in the workflow system.
    This class provides a common interface and some utility methods for data handlers.
    """

    def __init__(self, task_buffer, *args, **kwargs):
        """
        Initialize the step handler with necessary parameters.

        Args:
            task_buffer: The task buffer interface to interact with the task database.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        self.tbif = task_buffer

    def check_target(self, data_spec: WFDataSpec, **kwargs) -> WFDataTargetCheckResult:
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
