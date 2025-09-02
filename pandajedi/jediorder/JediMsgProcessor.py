from pandacommon.pandalogger import logger_utils
from pandacommon.pandamsgbkr import msg_processor

from pandajedi.jediconfig import jedi_config

msg_processor.base_logger = logger_utils.setup_logger("JediMsgProcessor")


# Main message processing agent
class MsgProcAgent(msg_processor.MsgProcAgentBase):
    pass


# launch
def launcher(stop_event):
    tmp_log = logger_utils.make_logger(msg_processor.base_logger, method_name="launcher")
    tmp_log.debug("start")
    try:
        config_file = jedi_config.msgprocessor.configFile
    except Exception as e:
        tmp_log.error(f"failed to read config json file; should not happen... {e.__class__.__name__}: {e}")
        raise e
    # start
    agent = MsgProcAgent(config_file)
    agent.start()
    tmp_log.debug("started")
    # wait for stop event
    stop_event.wait()
    # stop
    tmp_log.debug("stopping")
    agent.stop()
    tmp_log.debug("stopped")
