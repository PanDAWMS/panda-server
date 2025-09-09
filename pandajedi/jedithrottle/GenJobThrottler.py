from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore.MsgWrapper import MsgWrapper

from .JobThrottlerBase import JobThrottlerBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class to throttle general jobs
class GenJobThrottler(JobThrottlerBase):
    # constructor
    def __init__(self, taskBufferIF):
        JobThrottlerBase.__init__(self, taskBufferIF)

    # check if throttled
    def toBeThrottled(self, vo, prodSourceLabel, cloudName, workQueue, resourceType):
        # make logger
        tmpLog = MsgWrapper(logger)
        tmpLog.debug(f"start vo={vo} label={prodSourceLabel} cloud={cloudName} workQueue={workQueue.queue_name}")
        # check if unthrottled
        if workQueue.queue_share is None:
            tmpLog.debug("  done : unthrottled since share=None")
            return self.retUnThrottled
        tmpLog.debug("  done : SKIP")
        return self.retThrottled
