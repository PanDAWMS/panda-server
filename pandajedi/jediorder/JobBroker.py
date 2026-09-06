from typing import Any

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jedicore.ThreadUtils import MapWithLock
from pandaserver.taskbuffer.InputChunk import InputChunk
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# factory class for job brokerage
class JobBroker(FactoryBase):
    # constructor
    def __init__(self, vo: str | list[str] | None, sourceLabel: str | list[str] | None) -> None:
        FactoryBase.__init__(self, vo, sourceLabel, logger, jedi_config.jobbroker.modConfig)

    # main
    def doBrokerage(
        self, taskSpec: JediTaskSpec, cloudName: str | None, inputChunk: InputChunk, taskParamMap: dict[str, Any] | None
    ) -> tuple[Interaction.StatusCode, InputChunk]:
        return self.getImpl(taskSpec.vo, taskSpec.prodSourceLabel).doBrokerage(taskSpec, cloudName, inputChunk, taskParamMap)

    # set live counter
    def setLiveCounter(self, vo: str, sourceLabel: str, liveCounter: MapWithLock) -> None:
        self.getImpl(vo, sourceLabel).setLiveCounter(liveCounter)

    # set lock ID
    def setLockID(self, vo: str, sourceLabel: str, pid: str | int, tid: int) -> None:
        self.getImpl(vo, sourceLabel).setLockID(pid, tid)

    # get base lock ID
    def getBaseLockID(self, vo: str, sourceLabel: str) -> str | None:
        return self.getImpl(vo, sourceLabel).getBaseLockID()

    # set test mode
    def setTestMode(self, vo: str, sourceLabel: str) -> None:
        self.getImpl(vo, sourceLabel).setTestMode()
