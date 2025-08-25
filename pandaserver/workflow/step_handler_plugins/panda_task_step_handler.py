import json

from pandaserver.workflow.step_handler_plugins.base_step_handler import BaseStepHandler
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

    def submit_target(self, step_spec: WFStepSpec, workflow_spec: WorkflowSpec, **kwargs):
        """
        Submit a target for processing the PanDA task step.
        This method should be implemented to handle the specifics of PanDA task submission.
        """

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

        # task_param_json = json.dumps(task_param_map)

        # self.tbif.insertTaskParams_JEDI(
        #     task_param_map["vo"], task_param_map["prodSourceLabel"], task_param_map["userName"], task_param_map["taskName"], task_param_json
        # )
