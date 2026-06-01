from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.asyncprocess import processor

from .WatchDogBase import WatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# watchdog that processes async requests on JEDI machines
class AsyncRequestWatchDog(WatchDogBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)

    # main
    def doAction(self):
        tmpLog = MsgWrapper(logger)
        tmpLog.debug("start")
        try:
            processor.run(service_name="jedi", tbuf=self.taskBufferIF)
        except Exception as e:
            tmpLog.error(f"failed to process async requests with {e}")
            return self.SC_FAILED
        tmpLog.debug("done")
        return self.SC_SUCCEEDED
