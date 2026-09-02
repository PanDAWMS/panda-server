import copy
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.workflow.step_handler_plugins.base_step_handler import BaseStepHandler
from pandaserver.workflow.workflow_base import (
    TASKID_PLACEHOLDER,
    WFStepSpec,
    WFStepStatus,
    WFStepTargetCancelResult,
    WFStepTargetCheckResult,
    WFStepTargetSubmitResult,
    WFStepType,
    has_placeholder,
    substitute_placeholder,
)
from pandaserver.workflow.workflow_native_utils import (
    DATA_INPUT_PARAM_TYPES,
    extract_dataset_reference,
)

# Task source labels which may only be used by a submitter holding a production role.
# Note that PanDA has two separate prodSourceLabel taxonomies and only the task-level one matters
# here: JobUtils.prod_sources ("managed", "prod_test") describes *job* labels, used for pilot job
# dispatch and brokerage, and "prod_test" never appears as a task label. Which task labels a JEDI
# instance accepts is set by its own procConfig, so gating anything beyond "managed" would bake one
# deployment's configuration into the server.
PRODUCTION_SOURCE_LABELS = ("managed",)

# main logger
logger = PandaLogger().getLogger(__name__.split(".")[-1])


class PandaTaskStepHandler(BaseStepHandler):
    """
    Handler for PanDA task steps in the workflow.
    This class is responsible for managing the execution of PanDA tasks within a workflow.
    """

    def __init__(self, *args, **kwargs):
        """
        Initialize the step handler with necessary parameters.
        """
        # Initialize base class or any required modules here
        super().__init__(*args, **kwargs)
        # plugin flavor
        self.plugin_flavor = "panda_task"

    def resolve_input_references(self, step_spec: WFStepSpec, task_param_map: dict, tmp_log: LogWrapper) -> tuple[bool, str]:
        """
        Replace dataset references in the task parameters with the datasets actually produced.

        A step's input job parameters may name their dataset as a reference ("{step/output}" or
        "{workflow_input}") rather than a literal name, because the producing step's output dataset
        name embeds its task ID and so is not known when the description is written. By the time this
        step is submitted the producer has run, and its resolved name is recorded on the workflow
        data, so the reference is replaced from there.

        Args:
            step_spec (WFStepSpec): The step being submitted
            task_param_map (dict): Task parameters to resolve in place
            tmp_log (LogWrapper): Logger

        Returns:
            bool: Whether every reference was resolved
            str: An error message when a reference could not be resolved, otherwise empty
        """
        job_params = task_param_map.get("jobParameters")
        if not isinstance(job_params, list):
            return True, ""
        for job_param in job_params:
            if not isinstance(job_param, dict) or job_param.get("param_type") not in DATA_INPUT_PARAM_TYPES:
                continue
            reference = extract_dataset_reference(job_param.get("dataset"))
            if reference is None:
                # a literal dataset name, external to this workflow
                continue
            data_spec = self.tbif.get_workflow_data_by_name(reference, step_spec.workflow_id)
            if data_spec is None or not data_spec.target_id:
                message = f"input reference {{{reference}}} has no workflow data to resolve from"
                tmp_log.error(message)
                return False, message
            if has_placeholder(data_spec.target_id, TASKID_PLACEHOLDER):
                # the producing task has not been submitted yet, so its dataset name is not final
                message = f"input reference {{{reference}}} is not resolved yet: {data_spec.target_id}"
                tmp_log.error(message)
                return False, message
            job_param["dataset"] = data_spec.target_id
            tmp_log.debug(f"resolved input reference {{{reference}}} to {data_spec.target_id}")
        return True, ""

    def update_output_data_names(self, step_spec: WFStepSpec, task_id: int, tmp_log: LogWrapper) -> None:
        """
        Record the resolved output dataset names once the step's task ID is known.

        Output dataset names may embed the task ID, which only exists after the task is queued. The
        workflow data registered for this step still holds the placeholder, so it is rewritten here;
        downstream steps read these names when resolving their own input references.

        Args:
            step_spec (WFStepSpec): The step whose outputs are being recorded
            task_id (int): The task ID assigned to the step
            tmp_log (LogWrapper): Logger
        """
        for output_data_name in step_spec.definition_json_map.get("output_data_list", []):
            data_spec = self.tbif.get_workflow_data_by_name(output_data_name, step_spec.workflow_id)
            if data_spec is None:
                tmp_log.warning(f"output data {output_data_name} not found; skipped")
                continue
            if not has_placeholder(data_spec.target_id, TASKID_PLACEHOLDER):
                continue
            data_spec.target_id = substitute_placeholder(data_spec.target_id, TASKID_PLACEHOLDER, task_id)
            self.tbif.update_workflow_data(data_spec)
            tmp_log.info(f"resolved output data {output_data_name} to {data_spec.target_id}")

    def submit_target(self, step_spec: WFStepSpec, **kwargs) -> WFStepTargetSubmitResult:
        """
        Submit a target for processing the PanDA task step.

        Args:
            step_spec (WFStepSpec): The workflow step specification containing details about the step to be processed.
            **kwargs: Additional keyword arguments that may be required for submission.

        Returns:
            WFStepTargetSubmitResult: An object containing the result of the submission, including success status, target ID (task ID), and message.
        """
        tmp_log = LogWrapper(logger, f"submit_target workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        # Initialize
        submit_result = WFStepTargetSubmitResult()
        # Check step flavor
        if step_spec.flavor != self.plugin_flavor:
            # A flavor mismatch means the plugin map routed this step to the wrong handler, which is
            # a configuration or programming error rather than any runtime condition, so it is
            # reported loudly and as a failure instead of being skipped quietly.
            submit_result.success = False
            submit_result.message = f"flavor={step_spec.flavor} is not {self.plugin_flavor}; wrong step handler for this step"
            tmp_log.error(f"{submit_result.message}")
            return submit_result
        try:
            # Get step definition
            step_definition = step_spec.definition_json_map
            user_dn = step_definition.get("user_dn")
            prod_role = step_definition.get("prod_role", False)
            task_param_map = copy.deepcopy(step_definition.get("task_params", {}))
            if not task_param_map:
                submit_result.message = "step definition has no task_params"
                tmp_log.error(submit_result.message)
                return submit_result
            # A step whose task claims a production label must have been submitted by someone holding
            # the role. Without this the task would still be queued, but under a label its submitter
            # is not entitled to, so refuse rather than silently downgrade it.
            if task_param_map.get("prodSourceLabel") in PRODUCTION_SOURCE_LABELS and not prod_role:
                submit_result.message = f"prodSourceLabel={task_param_map.get('prodSourceLabel')} requires a production role, which the submitter does not hold"
                tmp_log.error(submit_result.message)
                return submit_result
            # Resolve input dataset references against the datasets actually produced upstream
            is_resolved, message = self.resolve_input_references(step_spec, task_param_map, tmp_log)
            if not is_resolved:
                submit_result.message = message
                return submit_result
            if not step_spec.get_parameter("all_inputs_complete"):
                # Some inputs are not complete, set workflowHoldup to True to hold up the workflow until released by workflow processor
                task_param_map["workflowHoldup"] = True
            # A task queued by a previous attempt must not be queued twice. Production tasks are not
            # duplicate-checked on insert, so record the attempt before submitting and refuse to
            # retry a step whose outcome is unknown.
            previous_attempt = step_spec.get_parameter("submit_attempt_task_name")
            if previous_attempt == task_param_map.get("taskName"):
                submit_result.message = f"a previous attempt already submitted taskName={previous_attempt}; not submitting again"
                tmp_log.error(submit_result.message)
                return submit_result
            step_spec.set_parameter("submit_attempt_task_name", task_param_map.get("taskName"))
            self.tbif.update_workflow_step(step_spec)
            # Queue the task, which also resolves the late-bound task ID in the task parameters
            task_id, message = self.tbif.insert_step_task(task_param_map, user_dn)
            if task_id is None:
                submit_result.message = message
                tmp_log.error(f"Failed to submit task: {message}")
                return submit_result
            submit_result.success = True
            submit_result.target_id = str(task_id)
            tmp_log.info(f"Submitted task target_id={submit_result.target_id}")
            # Record the resolved output dataset names for downstream steps to consume
            self.update_output_data_names(step_spec, task_id, tmp_log)
        except Exception as e:
            submit_result.message = f"exception {str(e)}"
            tmp_log.error(f"Failed to submit task: {traceback.format_exc()}")
        return submit_result

    def check_target(self, step_spec: WFStepSpec, **kwargs) -> WFStepTargetCheckResult:
        """
        Check the status of a submitted target for the given step.
        This method should be implemented to handle the specifics of status checking.

        Args:
            step_spec (WFStepSpec): The workflow step specification containing details about the step to be processed.
            **kwargs: Additional keyword arguments that may be required for status checking.

        Returns:
            WFStepTargetCheckResult: An object containing the result of the status check, including success status, step status, native status, and message.
        """
        tmp_log = LogWrapper(logger, f"check_target workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        allowed_step_statuses = [WFStepStatus.starting, WFStepStatus.running]
        try:
            # Initialize
            check_result = WFStepTargetCheckResult()
            # Check preconditions
            if step_spec.status not in allowed_step_statuses:
                check_result.message = f"not in status to check; skipped"
                tmp_log.warning(f"status={step_spec.status} not in status to check; skipped")
                return check_result
            if step_spec.flavor != self.plugin_flavor:
                # A flavor mismatch means the plugin map routed this step to the wrong handler, which is a
                # configuration or programming error rather than any runtime condition, so it is reported
                # loudly and as a failure instead of being skipped quietly.
                check_result.success = False
                check_result.message = f"flavor={step_spec.flavor} is not {self.plugin_flavor}; wrong step handler for this step"
                tmp_log.error(f"{check_result.message}")
                return check_result
            if step_spec.target_id is None:
                check_result.message = f"target_id is None; skipped"
                tmp_log.warning(f"target_id is None; skipped")
                return check_result
            # Get task ID and status
            task_id = int(step_spec.target_id)
            res = self.tbif.getTaskStatusSuperstatus(task_id)
            if not res:
                # A submitted task is queued in DEFT immediately but only appears in JEDI once
                # TaskRefiner picks it up, which lags by up to a refiner cycle. That window is
                # expected, so consult DEFT to tell it apart from a task that has really gone
                # missing, and report it as a warning rather than an error.
                deft_status = self.tbif.get_deft_task_status(task_id)
                if deft_status is not None:
                    # success stays None: not checkable yet, so the caller keeps the step and retries
                    check_result.message = f"task_id={task_id} is queued in DEFT table (status={deft_status}) but not yet refined into JEDI; will retry"
                    tmp_log.warning(f"{check_result.message}")
                else:
                    # gone from both: a real failure, so the caller reports it
                    check_result.success = False
                    check_result.message = f"task_id={task_id} not found in JEDI or DEFT table"
                    tmp_log.error(f"{check_result.message}")
                return check_result
            # Interpret status
            task_status = res[0]
            task_superstatus = res[1]
            check_result.success = True
            check_result.native_status = task_status
            # Production tasks routinely pass through states an analysis task rarely reaches
            # (staging from tape, preprocessing, retrying); leaving them out made check_target fail
            # the step with "unknown task_status" mid-flight.
            if task_status in ["running", "scouting", "scouted", "throttled", "prepared", "finishing", "passed", "merging", "toretry", "toincexec", "paused"]:
                check_result.step_status = WFStepStatus.running
            elif task_status in [
                "registered",
                "defined",
                "assigned",
                "activated",
                "starting",
                "ready",
                "topreprocess",
                "preprocessing",
                "staging",
                "staged",
                "rerefine",
            ]:
                check_result.step_status = WFStepStatus.starting
            elif task_status in ["pending"]:
                # Check superstatus for repetitive status (e.g. pending) to distinguish between starting and running
                if task_superstatus in ["running"]:
                    check_result.step_status = WFStepStatus.running
                else:
                    check_result.step_status = WFStepStatus.starting
            elif task_status in ["done", "finished"]:
                check_result.step_status = WFStepStatus.done
            elif task_status in ["failed", "exhausted", "aborted", "toabort", "aborting", "broken", "tobroken"]:
                check_result.step_status = WFStepStatus.failed
            else:
                check_result.success = False
                check_result.message = f"unknown task_status {task_status}"
                tmp_log.error(f"{check_result.message}")
                return check_result
            tmp_log.info(f"Got task_id={task_id} task_status={task_status}")
        except Exception as e:
            check_result.success = False
            check_result.message = f"exception {str(e)}"
            tmp_log.error(f"Failed to check status: {traceback.format_exc()}")
        return check_result

    def on_all_inputs_done(self, step_spec: WFStepSpec, **kwargs) -> None:
        """
        Hook method called when all inputs for the step are done.
        For PanDA task steps, unset workflowHoldup of the target task to allow it to proceed.

        Args:
            step_spec (WFStepSpec): The workflow step specification containing details about the step.
            **kwargs: Additional keyword arguments.
        """
        tmp_log = LogWrapper(logger, f"on_all_inputs_done workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        try:
            # Check step flavor
            if step_spec.flavor != self.plugin_flavor:
                # A flavor mismatch means the plugin map routed this step to the wrong handler, which is a
                # configuration or programming error rather than any runtime condition, so it is reported
                # loudly and as a failure instead of being skipped quietly.
                tmp_log.error(f"flavor={step_spec.flavor} is not {self.plugin_flavor}; wrong step handler for this step")
                return
            if step_spec.target_id is None:
                tmp_log.warning(f"target_id is None; skipped")
                return
            # Get task ID
            task_id = int(step_spec.target_id)
            # Get task spec
            _, task_spec = self.tbif.getTaskWithID_JEDI(task_id)
            if task_spec is None:
                # Same not-yet-refined window as in check_target: there is nothing to release until
                # the task exists in JEDI, and the next cycle will retry.
                deft_status = self.tbif.get_deft_task_status(task_id)
                if deft_status is not None:
                    tmp_log.warning(f"task_id={task_id} is queued in DEFT (status={deft_status}) but not yet refined into JEDI; nothing to release yet")
                else:
                    tmp_log.error(f"task_id={task_id} not found in JEDI or DEFT; skipped")
                return
            # Unset workflowHoldup, release the task if in pending, and trigger jedi_contents_feeder
            if task_spec.is_workflow_holdup():
                task_spec.set_workflow_holdup(False)
                self.tbif.updateTask_JEDI(task_spec, {"jediTaskID": task_spec.jediTaskID})
                tmp_log.info(f"task_id={task_id} unset workflowHoldup")
                if task_spec.status == "pending":
                    tmp_ret = self.tbif.release_task_on_hold(task_id)
                    if not tmp_ret:
                        tmp_log.error(f"task_id={task_id} failed to release from pending")
                    else:
                        tmp_log.info(f"task_id={task_id} released from pending")
                self.tbif.push_task_trigger_message("jedi_contents_feeder", task_id)
                tmp_log.info(f"task_id={task_id} triggered jedi_contents_feeder")
            # Done
            tmp_log.debug(f"Done")
        except Exception as e:
            tmp_log.error(f"Failed with: {traceback.format_exc()}")

    def cancel_target(self, step_spec, **kwargs) -> WFStepTargetCancelResult:
        """
        Cancel the target task for the given step.
        This method should be implemented to handle the specifics of task cancellation.

        Args:
            step_spec (WFStepSpec): The workflow step specification containing details about the step to be processed.
            **kwargs: Additional keyword arguments that may be required for cancellation.

        Returns:
            WFStepTargetCancelResult: An object containing the result of the cancellation, including success status and message.
        """
        tmp_log = LogWrapper(logger, f"cancel_target workflow_id={step_spec.workflow_id} step_id={step_spec.step_id}")
        cancel_result = WFStepTargetCancelResult()
        try:
            # Check step flavor
            if step_spec.flavor != self.plugin_flavor:
                # A flavor mismatch means the plugin map routed this step to the wrong handler, which is a
                # configuration or programming error rather than any runtime condition, so it is reported
                # loudly and as a failure instead of being skipped quietly.
                cancel_result.success = False
                cancel_result.message = f"flavor={step_spec.flavor} is not {self.plugin_flavor}; wrong step handler for this step"
                tmp_log.error(f"{cancel_result.message}")
                return cancel_result
            if step_spec.target_id is None:
                # If target_id is None, consider it as already cancelled since there is no task to cancel
                cancel_result.success = True
                cancel_result.message = f"target_id is None so considered already cancelled; skipped"
                tmp_log.debug(f"{cancel_result.message}")
                return cancel_result
            # Get task ID
            task_id = int(step_spec.target_id)
            # Cancel task
            ret_val, ret_str = self.tbif.sendCommandTaskPanda(task_id, "PanDA Task Step Handler cancel_target", True, "kill", properErrorCode=True)
            # check if ok
            if ret_val == 0:
                cancel_result.success = True
                tmp_log.info(f"target_id={step_spec.target_id} cancelled")
            else:
                cancel_result.success = False
                cancel_result.message = f"failed to cancel the task: error_code={ret_val} {ret_str}"
                tmp_log.warning(f"{cancel_result.message}")
        except Exception as e:
            cancel_result.message = f"exception {str(e)}"
            tmp_log.error(f"Failed to cancel task: {traceback.format_exc()}")
        return cancel_result
