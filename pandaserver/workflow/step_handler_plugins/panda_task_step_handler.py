import json
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.workflow.step_handler_plugins.base_step_handler import BaseStepHandler
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

    def submit_target(self, step_spec: WFStepSpec, **kwargs) -> WFStepTargetSubmitResult:
        """
        Submit a target for processing the PanDA task step.
        This method should be implemented to handle the specifics of PanDA task submission.

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
            tmp_log.warning(f"flavor={step_spec.flavor} not {self.plugin_flavor}; skipped")
            submit_result.message = f"flavor not {self.plugin_flavor}; skipped"
            return submit_result
        ...
        # task_param_map = {}
        # task_param_map["taskName"] = step_spec.name
        # task_param_map["userName"] = workflow_spec.username
        # task_param_map["vo"] = "atlas"
        # task_param_map["taskPriority"] = 1000
        # # task_param_map["architecture"] = "i686-slc5-gcc43-opt"
        # # task_param_map["transUses"] = "Atlas-17.2.7"
        # task_param_map["transUses"] = None
        # # task_param_map["transHome"] = "AtlasProduction-17.2.8.10"
        # task_param_map["transHome"] = None
        # task_param_map["transPath"] = "runGen-00-00-02"
        # task_param_map["processingType"] = "reco"
        # task_param_map["prodSourceLabel"] = "user"
        # # task_param_map["prodSourceLabel"] = "managed"
        # task_param_map["taskType"] = "anal"
        # # task_param_map["taskType"] = "prod"
        # task_param_map["inputPreStaging"] = True
        # # task_param_map["panda_data_carousel"] = True
        # task_param_map["remove_rule_when_done"] = True
        # # task_param_map["workingGroup"] = "AP_Higgs"
        # task_param_map["coreCount"] = 1
        # task_param_map["nFiles"] = 1
        # # task_param_map["cloud"] = "US"
        # logDatasetName = f"panda.jeditest.log.{uuid.uuid4()}"
        # task_param_map["log"] = {
        #     "dataset": logDatasetName,
        #     "type": "template",
        #     "param_type": "log",
        #     "token": "ATLASDATADISK",
        #     "value": f"{logDatasetName}.${{SN}}.log.tgz",
        # }
        # outDatasetName = f"panda.jeditest.NTUP_EMBLLDN.{uuid.uuid4()}"
        # task_param_map["jobParameters"] = [
        #     {
        #         "type": "template",
        #         "param_type": "input",
        #         "value": "inputAODFile=${IN}",
        #         "dataset": "mc23_13p6TeV:mc23_13p6TeV.602027.PhH7EG_NLO_LQ_S43_ResProd_lam22_5000_3p5.merge.AOD.e8531_e8528_s4162_s4114_r14622_r14663_tid34033945_00",
        #         "expand": True,
        #     },
        #     {"type": "template", "param_type": "pseudo_input", "value": "dummy_value", "dataset": "pseudo_dataset"},
        #     {"type": "constant", "value": "AMITag=p1462"},
        #     {
        #         "type": "template",
        #         "param_type": "output",
        #         "token": "ATLASDATADISK",
        #         "value": f"outputNTUP_EMBLLDNFile={outDatasetName}.${{SN}}.pool.root",
        #         "dataset": outDatasetName,
        #     },
        # ]
        try:
            # Get step definition
            step_definition = step_spec.definition_json_map
            user_name = step_definition.get("user_name")
            user_dn = step_definition.get("user_dn")
            task_param_map = step_definition.get("task_params", {})
            # task_param_map["userName"] = user_name
            # Always set workflowHoldup to True to hold up the workflow until released by workflow processor
            task_param_map["workflowHoldup"] = True
            # Submit task
            tmp_ret_flag, temp_ret_val = self.tbif.insertTaskParamsPanda(task_param_map, user_dn, False, decode=False)
            if tmp_ret_flag:
                submit_result.success = True
                submit_result.target_id = str(temp_ret_val)
                tmp_log.info(f"Submitted task target_id={submit_result.target_id}")
            else:
                submit_result.message = temp_ret_val
                tmp_log.error(f"Failed to submit task: {submit_result.message}")
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
                check_result.message = f"flavor not {self.plugin_flavor}; skipped"
                tmp_log.warning(f"flavor={step_spec.flavor} not {self.plugin_flavor}; skipped")
                return check_result
            if step_spec.target_id is None:
                check_result.message = f"target_id is None; skipped"
                tmp_log.warning(f"target_id is None; skipped")
                return check_result
            # Get task ID and status
            task_id = int(step_spec.target_id)
            res = self.tbif.getTaskStatus(task_id)
            if not res:
                check_result.message = f"task_id={task_id} not found"
                tmp_log.error(f"{check_result.message}")
                return check_result
            # Interpret status
            task_status = res[0]
            check_result.success = True
            check_result.native_status = task_status
            if task_status in ["running", "scouting", "scouted", "throttled", "prepared", "finishing", "passed"]:
                check_result.step_status = WFStepStatus.running
            elif task_status in ["defined", "assigned", "activated", "starting", "ready", "pending"]:
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
