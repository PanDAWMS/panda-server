from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandaserver.taskbuffer.WorkQueue import WorkQueue

from .JobThrottlerBase import JobThrottlerBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class to throttle ATLAS production jobs
class AtlasProdJobThrottler(JobThrottlerBase):
    # constructor
    def __init__(self, taskBufferIF: JediTaskBufferInterface) -> None:
        JobThrottlerBase.__init__(self, taskBufferIF)
        self.comp_name = "prod_job_throttler"
        self.app = "jedi"

    # check if throttled
    def toBeThrottled(
        self, vo: str, prodSourceLabel: str, cloudName: str | None, workQueue: WorkQueue, resource_name: str
    ) -> tuple[Interaction.StatusCode, bool | int]:
        return self.toBeThrottledBase(vo, prodSourceLabel, cloudName, workQueue, resource_name, logger)
