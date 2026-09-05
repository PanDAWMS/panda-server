from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.taskbuffer.WorkQueue import WorkQueue

from .JobThrottlerBase import JobThrottlerBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class to throttle general jobs
class GenJobThrottler(JobThrottlerBase):
    # constructor
    def __init__(self, taskBufferIF: JediTaskBufferInterface) -> None:
        JobThrottlerBase.__init__(self, taskBufferIF)

    # check if throttled
    def toBeThrottled(
        self, vo: str, prodSourceLabel: str, cloudName: str | None, workQueue: WorkQueue, resourceType: str
    ) -> tuple[Interaction.StatusCode, bool | int]:
        # make logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug(f"start vo={vo} label={prodSourceLabel} cloud={cloudName} workQueue={workQueue.queue_name}")
        # check if unthrottled
        if workQueue.queue_share is None:
            tmpLog.debug("  done : unthrottled since share=None")
            return self.retUnThrottled
        tmpLog.debug("  done : SKIP")
        return self.retThrottled
