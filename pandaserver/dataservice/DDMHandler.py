import re
import threading

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.dataservice.activator import Activator
from pandaserver.dataservice.finisher import Finisher

# logger
_logger = PandaLogger().getLogger("DDMHandler")


class DDMHandler(threading.Thread):
    # constructor
    def __init__(self, taskBuffer, vuid, site=None, dataset=None, scope=None):
        threading.Thread.__init__(self)
        self.vuid = vuid
        self.taskBuffer = taskBuffer
        self.site = site
        self.scope = scope
        self.dataset = dataset

    # main
    def run(self):
        # get logger
        tmpLog = LogWrapper(
            _logger,
            f"<vuid={self.vuid} site={self.site} name={self.dataset}>",
        )
        # query dataset
        tmpLog.debug("start")
        if self.vuid is not None:
            dataset = self.taskBuffer.queryDatasetWithMap({"vuid": self.vuid})
        else:
            dataset = self.taskBuffer.queryDatasetWithMap({"name": self.dataset})
        if dataset is None:
            tmpLog.error("Not found")
            tmpLog.debug("end")
            return
        tmpLog.debug(f"type:{dataset.type} name:{dataset.name}")
        if dataset.type == "dispatch":
            # activate jobs in jobsDefined
            Activator(self.taskBuffer, dataset).run()
        if dataset.type == "output":
            if dataset.name is not None and re.search("^panda\..*_zip$", dataset.name) is not None:
                # start unmerge jobs
                Activator(self.taskBuffer, dataset, enforce=True).run()
            else:
                # finish transferring jobs
                Finisher(self.taskBuffer, dataset, site=self.site).run()
        tmpLog.debug("end")
