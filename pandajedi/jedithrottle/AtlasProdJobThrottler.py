from pandacommon.pandalogger.PandaLogger import PandaLogger

from .JobThrottlerBase import JobThrottlerBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class to throttle ATLAS production jobs
class AtlasProdJobThrottler(JobThrottlerBase):
    # constructor
    def __init__(self, taskBufferIF):
        JobThrottlerBase.__init__(self, taskBufferIF)
        self.comp_name = "prod_job_throttler"
        self.app = "jedi"

    # check if throttled
    def toBeThrottled(self, vo, prodSourceLabel, cloudName, workQueue, resource_name):
        return self.toBeThrottledBase(vo, prodSourceLabel, cloudName, workQueue, resource_name, logger)
