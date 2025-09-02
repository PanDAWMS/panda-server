from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.FactoryBase import FactoryBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# factory class for task generator
class TaskGenerator(FactoryBase):
    # constructor
    def __init__(self, vo, sourceLabel):
        FactoryBase.__init__(self, vo, sourceLabel, logger, jedi_config.taskgen.modConfig)

    # main
    def doGenerate(self, taskSpec, taskParamMap, **varMap):
        return self.getImpl(taskSpec.vo, taskSpec.prodSourceLabel).doGenerate(taskSpec, taskParamMap, **varMap)
