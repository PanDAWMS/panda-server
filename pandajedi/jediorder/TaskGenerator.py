from typing import Any

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# factory class for task generator
class TaskGenerator(FactoryBase):
    # constructor
    def __init__(self, vo: str | list[str] | None, sourceLabel: str | list[str] | None) -> None:
        FactoryBase.__init__(self, vo, sourceLabel, logger, jedi_config.taskgen.modConfig)

    # main
    def doGenerate(self, taskSpec: JediTaskSpec, taskParamMap: dict[str, Any], **varMap: Any) -> Interaction.StatusCode:
        # the plugin is whichever class the configuration named, so its answer is untyped
        ret: Interaction.StatusCode = self.getImpl(taskSpec.vo, taskSpec.prodSourceLabel).doGenerate(taskSpec, taskParamMap, **varMap)
        return ret
