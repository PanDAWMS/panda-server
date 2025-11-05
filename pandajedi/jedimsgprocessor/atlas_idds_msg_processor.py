import json

from pandacommon.pandalogger import logger_utils

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jedimsgprocessor.hpo_msg_processor import HPOMsgProcPlugin
from pandajedi.jedimsgprocessor.processing_msg_processor import ProcessingMsgProcPlugin
from pandajedi.jedimsgprocessor.tape_carousel_msg_processor import (
    TapeCarouselMsgProcPlugin,
)

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# Atlas iDDS message processing plugin, a bridge connect to other idds related message processing plugins
class AtlasIddsMsgProcPlugin(BaseMsgProcPlugin):
    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        self.plugin_TapeCarousel = TapeCarouselMsgProcPlugin()
        self.plugin_HPO = HPOMsgProcPlugin()
        self.plugin_Processing = ProcessingMsgProcPlugin()
        # for each plugin
        for _plugin in [self.plugin_TapeCarousel, self.plugin_HPO, self.plugin_Processing]:
            # initialize each
            _plugin.initialize(in_collective=True)
            # use the same taskBuffer interface
            _plugin.tbIF = self.tbIF

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
            msg_type = msg_dict.get("msg_type")
        except Exception as e:
            err_str = f"failed to parse message object dict {msg_dict} , skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # run different plugins according to message type
        try:
            if msg_type in ("file_stagein", "collection_stagein", "work_stagein"):
                tmp_log.debug("to tape_carousel")
                self.plugin_TapeCarousel.process(msg_obj, decoded_data=msg_dict)
            elif msg_type in ("file_hyperparameteropt", "collection_hyperparameteropt", "work_hyperparameteropt"):
                tmp_log.debug("to hpo")
                self.plugin_HPO.process(msg_obj, decoded_data=msg_dict)
            elif msg_type in ("file_processing", "collection_processing", "work_processing"):
                tmp_log.debug("to processing")
                self.plugin_Processing.process(msg_obj, decoded_data=msg_dict)
            else:
                # Asked by iDDS and message broker guys, JEDI needs to consume unknown types of messages and do nothing...
                warn_str = f"unknown msg_type: {msg_type} msg_dict: {str(msg_dict)}"
                tmp_log.warning(warn_str)
        except Exception:
            raise
        # done
        tmp_log.info("done")
