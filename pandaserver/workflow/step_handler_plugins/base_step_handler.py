import dataclasses

from pandaserver.workflow.workflow_base import (
    WFDataSpec,
    WFDataStatus,
    WFDataType,
    WFStepSpec,
    WFStepStatus,
    WFStepTargetCheckResult,
    WFStepTargetSubmitResult,
    WFStepType,
    WorkflowSpec,
    WorkflowStatus,
)


class BaseStepHandler:
    """
    Base class for step handlers in the workflow.
    This class provides a common interface and some utility methods for step handlers.
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

    def submit_target(self, step_spec: WFStepSpec, **kwargs) -> WFStepTargetSubmitResult:
        """
        Submit a target for processing the step.
        This method should be implemented by subclasses to handle the specifics of target submission.
        This method should NOT modify step_spec. Any update information should be stored in the WFStepTargetSubmitResult returned instead.

        Args:
            step_spec (WFStepSpec): Specifications of the workflow step whose target is to be submitted.

        Returns:
            WFStepTargetSubmitResult: An object containing the result of the submission, including success status, target ID, and message.

        """
        raise NotImplementedError("Subclasses must implement this method.")

    def check_target(self, step_spec: WFStepSpec, **kwargs) -> WFStepTargetCheckResult:
        """
        Check the status of the submitted target.
        This method should be implemented by subclasses to handle the specifics of target status checking.
        This method should NOT modify step_spec. Any update information should be stored in the WFStepTargetCheckResult returned instead.

        Args:
            step_spec (WFStepSpec): Specifications of the workflow step to be checked.

        Returns:
            WFStepTargetCheckResult: An object containing the result of the check, including success status, current step status, and message.
        """
        raise NotImplementedError("Subclasses must implement this method.")
