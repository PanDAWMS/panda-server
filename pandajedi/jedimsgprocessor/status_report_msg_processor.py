import copy
import json
import re
import traceback

from pandacommon.pandalogger import logger_utils

from pandajedi.jediconfig import jedi_config
from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# list of task statuses to return to iDDS
to_return_task_status_list = [
    "defined",
    "ready",
    "scouting",
    "pending",
    "paused",
    "running",
    "prepared",
    "done",
    "finished",
    "failed",
    "broken",
    "aborted",
]

to_return_job_status_list = [
    "pending",
    "defined",
    "assigned",
    "activated",
    "starting",
    "running",
    "throttled",
    "finished",
    "failed",
    "cancelled",
    "closed",
]


# status report message processing plugin
class StatusReportMsgProcPlugin(BaseMsgProcPlugin):
    """
    Messaging status of jobs and tasks
    Forward the incoming message to other msg_processors plugin (e.g. Kafka) if configured in params
    Return the processed message to send to iDDS via MQ
    """

    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        # forwarding plugins: incoming message will be forwarded to process method of these plugins
        self.forwarding_plugins = []
        forwarding_plugin_names = self.params.get("forwarding_plugins", [])
        if "kafka" in forwarding_plugin_names:
            # Kafka
            from pandajedi.jedimsgprocessor.kafka_msg_processor import (
                KafkaMsgProcPlugin,
            )

            plugin_inst = KafkaMsgProcPlugin()
            plugin_inst.initialize()
            self.forwarding_plugins.append(plugin_inst)

    def process(self, msg_obj):
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="process")
        # start
        tmp_log.info("start")
        tmp_log.debug(f"sub_id={msg_obj.sub_id} ; msg_id={msg_obj.msg_id}")
        # parse json
        try:
            msg_dict = json.loads(msg_obj.data)
        except Exception as exc:
            err_str = f"failed to parse message json {msg_obj.data} , skipped. {exc.__class__.__name__} : {exc} ; {traceback.format_exc()}"
            tmp_log.error(err_str)
            raise
        # sanity check
        try:
            msg_type = msg_dict["msg_type"]
        except Exception as e:
            err_str = f"failed to parse message object dict {msg_dict} , skipped. {exc.__class__.__name__} : {exc} ; {traceback.format_exc()}"
            tmp_log.error(err_str)
            raise
        # whether to return the message
        to_return_message = False
        # run different plugins according to message type
        try:
            # DB source name from DB schema
            db_source_name = re.sub("_PANDA", "", jedi_config.db.schemaJEDI).lower()
            # process according to msg_type
            if msg_type == "task_status":
                tmp_log.debug("task_status")
                # forwarding
                for plugin_inst in self.forwarding_plugins:
                    try:
                        tmp_msg_dict = copy.deepcopy(msg_dict)
                        tmp_msg_dict["db_source"] = db_source_name
                        plugin_inst.process(msg_obj, decoded_data=tmp_msg_dict)
                    except Exception as exc:
                        tmp_log.error(f"failed to process message object dict {tmp_msg_dict}; {exc.__class__.__name__} : {exc} ; {traceback.format_exc()}")
                # only return certain statuses
                if msg_dict.get("status") in to_return_task_status_list:
                    to_return_message = True
            elif msg_type == "job_status":
                tmp_log.debug("job_status")
                # forwarding
                for plugin_inst in self.forwarding_plugins:
                    try:
                        tmp_msg_dict = copy.deepcopy(msg_dict)
                        tmp_msg_dict["db_source"] = db_source_name
                        plugin_inst.process(msg_obj, decoded_data=tmp_msg_dict)
                    except Exception as exc:
                        tmp_log.error(f"failed to process message object dict {tmp_msg_dict}; {exc.__class__.__name__} : {exc} ; {traceback.format_exc()}")
                # only return certain statuses
                if msg_dict.get("status") in to_return_job_status_list:
                    to_return_message = True
            else:
                warn_str = f"unknown msg_type : {msg_type}"
                tmp_log.warning(warn_str)
        except Exception:
            raise
        # done
        tmp_log.debug(f"{msg_dict}; to_return={to_return_message}")
        tmp_log.info("done")
        if to_return_message:
            return msg_obj.data
