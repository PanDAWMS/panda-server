import datetime
import threading

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.srvcore.CoreUtils import commands_get_status_output

# logger
_logger = PandaLogger().getLogger("ProcessLimiter")


# limit the number of processes
class ProcessLimiter:
    # constructor
    def __init__(self, maxProcess=3):
        self.processLock = threading.Semaphore(maxProcess)
        self.dataLock = threading.Lock()
        self.summary = {"nQueued": 0, "nRunning": 0}

    # update summary
    def updateSummary(self, dataName, change):
        # lock
        self.dataLock.acquire()
        # update
        if dataName in self.summary:
            self.summary[dataName] += change
        # release
        self.dataLock.release()
        _logger.debug(f"Summary : {str(self.summary)}")

    # execute command
    def getstatusoutput(self, commandStr):
        # time stamp
        timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(" ")
        _logger.debug(f'{timestamp} start for "{commandStr}"')
        self.updateSummary("nQueued", 1)
        _logger.debug(f"{timestamp} getting lock")
        # get semaphore
        self.processLock.acquire()
        _logger.debug(f"{timestamp} got lock")
        # execute
        self.updateSummary("nRunning", 1)
        status, output = commands_get_status_output(commandStr)
        _logger.debug(f"{timestamp} executed")
        self.updateSummary("nRunning", -1)
        # release queue
        self.processLock.release()
        _logger.debug(f"{timestamp} end")
        self.updateSummary("nQueued", -1)
        # return
        return status, output
