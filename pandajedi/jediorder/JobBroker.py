from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.FactoryBase import FactoryBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# factory class for job brokerage
class JobBroker(FactoryBase):
    # constructor
    def __init__(self, vo, sourceLabel):
        FactoryBase.__init__(self, vo, sourceLabel, logger, jedi_config.jobbroker.modConfig)

    # main
    def doBrokerage(self, taskSpec, cloudName, inputChunk, taskParamMap):
        return self.getImpl(taskSpec.vo, taskSpec.prodSourceLabel).doBrokerage(taskSpec, cloudName, inputChunk, taskParamMap)

    # set live counter
    def setLiveCounter(self, vo, sourceLabel, liveCounter):
        self.getImpl(vo, sourceLabel).setLiveCounter(liveCounter)

    # set lock ID
    def setLockID(self, vo, sourceLabel, pid, tid):
        self.getImpl(vo, sourceLabel).setLockID(pid, tid)

    # get base lock ID
    def getBaseLockID(self, vo, sourceLabel):
        return self.getImpl(vo, sourceLabel).getBaseLockID()

    # set test mode
    def setTestMode(self, vo, sourceLabel):
        self.getImpl(vo, sourceLabel).setTestMode()
