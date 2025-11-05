import re
import traceback

import yaml
from pandacommon.pandalogger import LogWrapper, logger_utils

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandaserver.dataservice.ddm_handler import DDMHandler

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# panda dataset callback message processing plugin
class PandaCallbackMsgProcPlugin(BaseMsgProcPlugin):

    def __init__(self, **params):
        super().__init__(**params)
        self.activities_with_file_callback = []
        self.component_action_map = []
        self.site_mapper = None
        self.verbose = False

    def initialize(self, **params):
        BaseMsgProcPlugin.initialize(self, **params)
        # activity list to use file callback
        self.activities_with_file_callback = self.params.get("activities_with_file_callback", [])
        # component action map
        # [{"event_type": "<event_type>", "component": "<component_name>", "criteria": {"key": <value>,}, "to_id": "<how_to_get_task_id>}, ...]
        self.component_action_map = self.params.get("component_action_map", [])
        # site mapper
        self.site_mapper = self.tbIF.get_site_mapper()
        # verbose logging
        self.verbose = self.params.get("verbose", False)

    def process(self, msg_obj):
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="process")
        # start
        # tmp_log.info('start')
        # tmp_log.debug('sub_id={0} ; msg_id={1}'.format(msg_obj.sub_id, msg_obj.msg_id))
        # parse yaml
        try:
            message_dict = yaml.safe_load(msg_obj.data)
        except Exception as e:
            err_str = f"failed to parse message yaml {msg_obj.data} , skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # run
        try:
            to_continue = True
            dsn = "UNKNOWN"
            # check event type
            if not isinstance(message_dict, dict):
                err_str = f"skip due to invalid message format:{type(message_dict).__name__}. msg:{str(message_dict)}"
                tmp_log.warning(err_str)
                return
            event_type = message_dict["event_type"]
            message_ids = f"sub_id={msg_obj.sub_id} ; msg_id={msg_obj.msg_id}"
            if event_type in ["datasetlock_ok"]:
                self.process_dataset_callback(event_type, message_ids, message_dict, tmp_log)
            elif self.activities_with_file_callback and event_type in ["transfer-done"]:
                self.process_file_callback(event_type, message_ids, message_dict, tmp_log)
            else:
                if self.verbose:
                    tmp_log.debug(f"skip event_type={event_type}")
            # trigger component actions
            if self.component_action_map:
                self.trigger_component_action(event_type, message_ids, message_dict, tmp_log)
            # end
        except Exception as e:
            err_str = f"failed to run, skipped. {e.__class__.__name__} : {e}\n{traceback.format_exc()}"
            tmp_log.error(err_str)
            raise

    def process_dataset_callback(self, event_type: str, message_ids: str, message_dict: dict, tmp_log: LogWrapper.LogWrapper) -> None:
        """
        Process a dataset callback

        Args:
            event_type: message event type
            message_ids: subscription and message IDs
            message_dict: message dictionary
            tmp_log: logger instance
        """
        message_payload = message_dict["payload"]
        # only for _dis or _sub
        dsn = message_payload["name"]
        if (re.search(r"_dis\d+$", dsn) is None) and (re.search(r"_sub\d+$", dsn) is None):
            return
        tmp_log.debug(message_ids)
        tmp_log.debug(f"{event_type} start")
        # take action
        scope = message_payload["scope"]
        site = message_payload["rse"]
        tmp_log.debug(f"{dsn} site={site} type={event_type}")
        thr = DDMHandler(task_buffer=self.tbIF, vuid=None, site=site, dataset=dsn, scope=scope)
        # just call run rather than start+join, to run it in main thread instead of spawning new thread
        thr.run()
        del thr
        tmp_log.debug(f"done {dsn}")
        return

    def process_file_callback(self, event_type: str, message_ids: str, message_dict: dict, tmp_log: LogWrapper.LogWrapper) -> None:
        """
        Process a file callback

        Args:
            event_type: message event type
            message_ids: subscription and message IDs
            message_dict: message dictionary
            tmp_log: logger instance
        """
        message_payload = message_dict["payload"]
        # only for activities with file callback
        activity = message_payload["activity"]
        if activity not in self.activities_with_file_callback:
            return
        # update file status and get corresponding PandaIDs
        filename = message_payload["name"]
        endpoint = message_payload["dst-rse"]
        tmp_log.debug(message_ids)
        tmp_log.debug(f"{event_type} start for lfn={filename} activity={activity}")
        sites = self.site_mapper.get_sites_for_endpoint(endpoint, "input")
        panda_ids = self.tbIF.update_input_files_at_sites_and_get_panda_ids(filename, sites)
        jobs = self.tbIF.peekJobs(panda_ids, fromActive=False, fromArchived=False, fromWaiting=False)
        # activate jobs
        self.tbIF.activateJobs(jobs)
        tmp_log.debug(f"done")
        return

    def trigger_component_action(self, event_type: str, message_ids: str, message_dict: dict, tmp_log: LogWrapper.LogWrapper) -> None:
        """
        Trigger component action based on the event type
        Args:
            event_type: message event type
            message_ids: subscription and message IDs
            message_dict: message dictionary
            tmp_log: logger instance
        """
        message_payload = message_dict["payload"]
        for action_item in self.component_action_map:
            # check event type
            if action_item["event_type"] != event_type:
                continue
            # check criteria
            criteria_matched = True
            for key, value in action_item.get("criteria", {}).items():
                dict_value = message_payload.get(key)
                if dict_value is None:
                    criteria_matched = False
                    break
                if dict_value != value and not re.match(value, str(dict_value)):
                    criteria_matched = False
                    break
            if not criteria_matched:
                continue
            component_name = action_item["component"]
            to_id = action_item["to_id"]
            # extract task ID based on to_id
            jedi_task_ids = None
            if to_id == "from_input_dataset":
                dataset_name = message_payload["name"].split(":")[-1]
                jedi_task_ids = self.tbIF.get_task_ids_with_dataset_attributes({"datasetName": dataset_name, "type": "input"})
            else:
                tmp_log.warning(f"unknown to_id={to_id} for action_item={action_item} ; skipped")
                continue
            if jedi_task_ids is None:
                tmp_log.warning(f"failed to extract jediTaskID for action_item={action_item} ; skipped")
                continue
            if not jedi_task_ids:
                tmp_log.debug(f"no jediTaskID found for action_item={action_item} ; skipped")
                continue
            # loop over task IDs
            for jedi_task_id in jedi_task_ids:
                # release task just in case
                self.tbIF.release_task_on_hold(jedi_task_id)
                # push trigger message
                push_ret = self.tbIF.push_task_trigger_message(component_name, jedi_task_ids)
                if push_ret:
                    tmp_log.debug(f"pushed trigger message to {component_name} for jediTaskID={jedi_task_ids}")
                else:
                    tmp_log.warning(f"failed to push trigger to {component_name} for jediTaskID={jedi_task_ids}")
        return
