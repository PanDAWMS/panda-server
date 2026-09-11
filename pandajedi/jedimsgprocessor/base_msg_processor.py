from typing import Any

from pandacommon.pandamsgbkr.msg_bkr_utils import MsgObj
from pandacommon.pandamsgbkr.msg_processor import SimpleMsgProcPluginBase

from pandajedi.jedicore.JediTaskBuffer import JediTaskBuffer


# Base simple message processing plugin
class BaseMsgProcPlugin(SimpleMsgProcPluginBase):
    def initialize(self, in_collective: bool = False) -> None:
        """
        initialize plugin instance, run once before loop in thread
        """
        if in_collective:
            # run in collective msg_proc plugin
            pass
        else:
            # run as individual msg_proc plugin
            self.set_tbIF()

    def set_tbIF(self) -> None:
        """
        set up JEDI TaskBuffer interface
        """
        # set nDBConnection = n_threads to avoid DBProxy blocking amongs threads
        n_db_conns = getattr(self, "n_threads", 1)
        self.tbIF = JediTaskBuffer(None, nDBConnection=n_db_conns)

    def process(self, msg_obj: MsgObj) -> Any:
        """
        process the message
        Get msg_obj from the incoming MQ (if any; otherwise msg_obj is None)
        Returned value will be sent to the outgoing MQ (if any)
        """
        pass
