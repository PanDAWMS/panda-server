import dataclasses

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

# === Dataclasses of return objects of step handler methods =====


@dataclasses.dataclass
class SubmitResult:
    """
    Result of submitting a target for processing a step.

    Fields:
        success (bool | None): Indicates if the submission was successful.
        target_id (str | None): The ID of the submitted target (e.g., task ID).
        message (str): A message providing additional information about the submission result.
    """

    success: bool | None = None
    target_id: str | None = None
    message: str = ""


@dataclasses.dataclass
class CheckResult:
    """
    Result of checking the status of a submitted target.

    Fields:
        success (bool | None): Indicates if the status check was successful.
        status (WFStepStatus | None): The status of the step to move to.
        native_status (str | None): The native status string from the target system.
        message (str): A message providing additional information about the status check result.
    """

    success: bool | None = None
    status: WFStepStatus | None = None
    native_status: str | None = None
    message: str = ""


# =================================================================


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

    def submit_target(self, step_spec: WFStepSpec, **kwargs) -> SubmitResult:
        """
        Submit a target for processing the step.
        This method should be implemented by subclasses to handle the specifics of target submission.
        This method should NOT modify step_spec. Any update information should be stored in the SubmitResult returned instead.

        Args:
            step_spec (WFStepSpec): Specifications of the workflow step to be submitted.

        Returns:
            SubmitResult: An object containing the result of the submission, including success status, target ID, and message.

        """
        raise NotImplementedError("Subclasses must implement this method.")

    # def check_status(self, target_id: str, **kwargs) -> tuple[bool | None, str | None, str]:
