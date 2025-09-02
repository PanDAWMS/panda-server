import json

from pandacommon.pandalogger import logger_utils

from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jediorder.ContentsFeeder import ContentsFeederThread

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# Jedi Contents Feeder message processor plugin
class JediContentsFeederMsgProcPlugin(BaseMsgProcPlugin):
    """
    Message-driven Contents Feeder
    """

    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        ddmIF = DDMInterface()
        ddmIF.setupInterface()
        the_pid = self.get_pid()
        self.contents_feeder_thread_obj = ContentsFeederThread(taskDsList=None, threadPool=None, taskbufferIF=self.tbIF, ddmIF=ddmIF, pid=the_pid)

    def process(self, msg_obj):
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
        if msg_type != "jedi_contents_feeder":
            # FIXME
            err_str = f"got unknown msg_type {msg_type} , skipped "
            tmp_log.error(err_str)
            raise
        # run
        try:
            task_id = msg_dict["taskid"]
            task_ds_list = self.tbIF.getDatasetsToFeedContents_JEDI(vo=None, prodSourceLabel=None, task_id=task_id)
            if task_ds_list:
                self.contents_feeder_thread_obj.feed_contents_to_tasks(task_ds_list)
                tmp_log.info(f"fed datasets to task {task_id}")
            else:
                tmp_log.debug(f"got empty list of datasets to feed to task {task_id}; skipped ")
        except Exception as e:
            err_str = f"failed to run, skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info("done")
