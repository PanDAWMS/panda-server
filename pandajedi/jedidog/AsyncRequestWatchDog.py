from typing import TYPE_CHECKING

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.asyncprocess import processor
from pandaserver.taskbuffer.db_proxy_mods.async_request_module import SERVICE_JEDI

from .WatchDogBase import WatchDogBase

if TYPE_CHECKING:
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jediddm.DDMInterface import DDMInterface

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# watchdog that processes async requests on JEDI machines
class AsyncRequestWatchDog(WatchDogBase):
    # constructor
    def __init__(self, taskBufferIF: "JediTaskBufferInterface", ddmIF: "DDMInterface") -> None:
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)

    # main
    def doAction(self) -> Interaction.StatusCode:
        tmpLog = MsgWrapper(logger)
        tmpLog.debug("start")
        try:
            processor.run(service_name=SERVICE_JEDI, tbuf=self.taskBufferIF)
        except Exception as e:
            tmpLog.error(f"failed to process async requests with {e}")
            return self.SC_FAILED
        tmpLog.debug("done")
        return self.SC_SUCCEEDED
