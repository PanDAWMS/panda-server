from pandacommon.pandamsgbkr.msg_processor import SimpleMsgProcPluginBase

from pandajedi.jedicore.JediTaskBuffer import JediTaskBuffer


# Base simple message processing plugin
class BaseMsgProcPlugin(SimpleMsgProcPluginBase):
    def initialize(self, in_collective=False):
        """
        initialize plugin instance, run once before loop in thread
        """
        if in_collective:
            # run in collective msg_proc plugin
            pass
        else:
            # run as individual msg_proc plugin
            self.set_tbIF()

    def set_tbIF(self):
        """
        set up JEDI TaskBuffer interface
        """
        # set nDBConnection = n_threads to avoid DBProxy blocking amongs threads
        n_db_conns = getattr(self, "n_threads", 1)
        self.tbIF = JediTaskBuffer(None, nDBConnection=n_db_conns)

    def process(self, msg_obj):
        """
        process the message
        Get msg_obj from the incoming MQ (if any; otherwise msg_obj is None)
        Returned value will be sent to the outgoing MQ (if any)
        """
        pass
