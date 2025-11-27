import json

from pandacommon.pandalogger import logger_utils

from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandaserver.workflow.workflow_core import WorkflowInterface

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# Workflow manager message processor plugin
class WorkflowManagerMsgProcPlugin(BaseMsgProcPlugin):
    """
    Message-driven workflow manager
    """

    def initialize(self):
        """
        Initialize the plugin
        """
        BaseMsgProcPlugin.initialize(self)
        the_pid = self.get_pid()
        self.workflow_interface = WorkflowInterface(self.tbIF)

    def process(self, msg_obj):
        """
        Process the message
        Typical message data looks like:
            {"msg_type":"workflow", "workflow_id": 123, "timestamp": 987654321}
            {"msg_type":"wfstep", "step_id": 456, "timestamp": 987654321}
            {"msg_type":"wfdata", "data_id": 789, "timestamp": 987654321}

        Args:
            msg_obj: message object
        """
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="process")
        # start
        tmp_log.info("start")
        tmp_log.debug(f"sub_id={msg_obj.sub_id} ; msg_id={msg_obj.msg_id}")
        # parse json
        try:
            msg_dict = json.loads(msg_obj.data)
        except Exception as e:
            err_str = f"failed to parse message json {msg_obj.data} , skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # sanity check
        try:
            msg_type = msg_dict["msg_type"]
        except Exception as e:
            err_str = f"failed to parse message object dict {msg_dict} , skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        if msg_type not in ("workflow", "wfstep", "wfdata"):
            err_str = f"got unknown msg_type {msg_type} , skipped "
            tmp_log.error(err_str)
            raise
        # run
        try:
            tmp_log.info(f"got message {msg_dict}")
            if msg_type == "workflow":
                workflow_id = msg_dict["workflow_id"]
                workflow_spec = self.tbIF.get_workflow(workflow_id)
                stats = self.workflow_interface.process_workflow(workflow_spec, by="msgproc")
                tmp_log.info(f"processed workflow_id={workflow_id}")
            elif msg_type == "wfstep":
                step_id = msg_dict["step_id"]
                step_spec = self.tbIF.get_workflow_step(step_id)
                stats = self.workflow_interface.process_steps([step_spec], by="msgproc")
                tmp_log.info(f"processed step_id={step_id}")
            elif msg_type == "wfdata":
                data_id = msg_dict["data_id"]
                data_spec = self.tbIF.get_workflow_data(data_id)
                stats = self.workflow_interface.process_datas([data_spec], by="msgproc")
                tmp_log.info(f"processed data_id={data_id}")
        except Exception as e:
            err_str = f"failed to run, skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info("done")
