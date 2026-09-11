from typing import Any

from pandaserver.workflow.workflow_base import (
    WFStepSpec,
    WFStepTargetCancelResult,
    WFStepTargetCheckResult,
    WFStepTargetSubmitResult,
)


class BaseStepHandler:
    """
    Base class for step handlers in the workflow.
    This class provides a common interface and some utility methods for step handlers.
    """

    def __init__(self, task_buffer: Any, *args: Any, **kwargs: Any) -> None:
        """
        Initialize the step handler with necessary parameters.

        Args:
            task_buffer: The task buffer interface to interact with the task database.
            *args: Additional positional arguments.
            **kwargs: Additional keyword arguments.
        """
        # A TaskBuffer when the API server builds the workflow interface, or JEDI's
        # JediTaskBufferInterface, which forwards every method to JediTaskBuffer through
        # CommandSendInterface. panda-server cannot name the JEDI class and __getattr__ is
        # invisible to a type checker, so Any is as close as this gets.
        self.tbif = task_buffer

    def submit_target(self, step_spec: WFStepSpec, **kwargs: Any) -> WFStepTargetSubmitResult:
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

    def check_target(self, step_spec: WFStepSpec, **kwargs: Any) -> WFStepTargetCheckResult:
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

    def on_all_inputs_done(self, step_spec: WFStepSpec, **kwargs: Any) -> None:
        """
        Hook method called when all inputs for the step are done.
        This method can be overridden by subclasses to perform actions when all inputs are ready.

        Args:
            step_spec (WFStepSpec): Specifications of the workflow step whose inputs are done.
            **kwargs: Additional keyword arguments.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def cancel_target(self, step_spec: WFStepSpec, **kwargs: Any) -> WFStepTargetCancelResult:
        """
        Cancel the submitted target.
        This method can be overridden by subclasses to handle target cancellation.

        Args:
            step_spec (WFStepSpec): Specifications of the workflow step whose target is to be cancelled.
            **kwargs: Additional keyword arguments.

        Returns:
            WFStepTargetCancelResult: An object containing the result of the cancellation, including success status and message.
        """
        raise NotImplementedError("Subclasses must implement this method.")
