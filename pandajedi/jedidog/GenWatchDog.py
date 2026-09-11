from typing import TYPE_CHECKING

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.MsgWrapper import MsgWrapper

from .TypicalWatchDogBase import TypicalWatchDogBase

if TYPE_CHECKING:
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jediddm.DDMInterface import DDMInterface

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# watchdog for general purpose
class GenWatchDog(TypicalWatchDogBase):
    # constructor
    def __init__(self, taskBufferIF: "JediTaskBufferInterface", ddmIF: "DDMInterface") -> None:
        TypicalWatchDogBase.__init__(self, taskBufferIF, ddmIF)

    # main
    def doAction(self) -> Interaction.StatusCode:
        tmpLog = MsgWrapper(logger)
        tmpLog.debug("start")
        tmpLog.debug("done")
        return self.SC_SUCCEEDED
