from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandaserver.taskbuffer.WorkQueue import WorkQueue

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# factory class for throttling
class JobThrottler(FactoryBase):
    # constructor
    def __init__(self, vo: str | list[str] | None, sourceLabel: str | list[str] | None) -> None:
        FactoryBase.__init__(self, vo, sourceLabel, logger, jedi_config.jobthrottle.modConfig)

    # main
    def toBeThrottled(
        self, vo: str, sourceLabel: str, cloudName: str | None, workQueue: WorkQueue, resourceType: str
    ) -> tuple[Interaction.StatusCode, bool | int]:
        impl = self.getImpl(vo, sourceLabel)
        retVal = impl.toBeThrottled(vo, sourceLabel, cloudName, workQueue, resourceType)
        # retrieve min priority and max number of jobs from concrete class
        self.minPriority = impl.minPriority
        self.maxNumJobs = impl.maxNumJobs
        self.lackOfJobs = impl.underNqLimit
        return retVal

    # check throttle level
    def mergeThrottled(self, vo: str, sourceLabel: str, thrLevel: bool | int) -> bool:
        impl = self.getImpl(vo, sourceLabel)
        return impl.mergeThrottled(thrLevel)
