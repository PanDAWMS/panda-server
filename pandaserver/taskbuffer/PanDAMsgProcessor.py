
from pandacommon.pandamsgbkr import msg_processor
from pandacommon.pandalogger import logger_utils

from pandaserver.config import panda_config


# logger
msg_processor.base_logger = logger_utils.setup_logger('PanDAMsgProcessor')


# Main message processing agent
class MsgProcAgent(msg_processor.MsgProcAgentBase):
    pass
