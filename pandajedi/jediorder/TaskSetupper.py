from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.spec_column import Null

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# factory class for task setup
class TaskSetupper(FactoryBase):
    # constructor
    def __init__(self, vo: str | list[str] | None, sourceLabel: str | list[str] | None) -> None:
        FactoryBase.__init__(self, vo, sourceLabel, logger, jedi_config.tasksetup.modConfig)

    # main
    def doSetup(self, taskSpec: JediTaskSpec, datasetToRegister: list[int | Null], pandaJobs: list[JobSpec]) -> Interaction.StatusCode:
        # the plugin is whichever class the configuration named, so its answer is untyped
        ret: Interaction.StatusCode = self.getImpl(taskSpec.vo, taskSpec.prodSourceLabel).doSetup(taskSpec, datasetToRegister, pandaJobs)
        return ret
