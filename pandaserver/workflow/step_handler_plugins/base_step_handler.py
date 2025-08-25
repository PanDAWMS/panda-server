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

    def submit_target(self, step_specs: WFStepSpec, **kwargs):
        """
        Submit a target for processing the step.
        This method should be implemented by subclasses to handle the specifics of target submission.

        Args:
            step_specs (WFStepSpec): Specifications of the workflow step to be submitted.
        """
        raise NotImplementedError("Subclasses must implement this method.")
