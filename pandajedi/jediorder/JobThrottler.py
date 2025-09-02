from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.FactoryBase import FactoryBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# factory class for throttling
class JobThrottler(FactoryBase):
    # constructor
    def __init__(self, vo, sourceLabel):
        FactoryBase.__init__(self, vo, sourceLabel, logger, jedi_config.jobthrottle.modConfig)

    # main
    def toBeThrottled(self, vo, sourceLabel, cloudName, workQueue, resourceType):
        impl = self.getImpl(vo, sourceLabel)
        retVal = impl.toBeThrottled(vo, sourceLabel, cloudName, workQueue, resourceType)
        # retrieve min priority and max number of jobs from concrete class
        self.minPriority = impl.minPriority
        self.maxNumJobs = impl.maxNumJobs
        self.lackOfJobs = impl.underNqLimit
        return retVal

    # check throttle level
    def mergeThrottled(self, vo, sourceLabel, thrLevel):
        impl = self.getImpl(vo, sourceLabel)
        return impl.mergeThrottled(thrLevel)
