import re
import sys

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.dataservice.DDM import rucioAPI


# plugin for ATLAS closer
class CloserAtlasPlugin:
    # constructor
    def __init__(self, job, datasets, log):
        self.jobSpec = job
        self.datasets = datasets
        self.tmpLog = LogWrapper(log, f"{self.jobSpec.PandaID} CloserAtlasPlugin")

    # execute
    def execute(self):
        try:
            # only for production
            if self.jobSpec.prodSourceLabel not in ["managed", "test"]:
                return True
            # only for urgent or high prio
            if self.jobSpec.processingType not in ["urgent"] and self.jobSpec.currentPriority <= 1000:
                return True
            # close datasets
            for datasetSpec in self.datasets:
                if re.search("_sub\d+$", datasetSpec.name) is None:
                    continue
                if datasetSpec.status != "tobeclosed":
                    continue
                try:
                    self.tmpLog.debug(f"immediate close {datasetSpec.name}")
                    rucioAPI.closeDataset(datasetSpec.name)
                except Exception:
                    errtype, errvalue = sys.exc_info()[:2]
                    self.tmpLog.warning(f"failed to close : {errtype} {errvalue}")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            self.tmpLog.warning(f"failed to execute : {errtype} {errvalue}")
        return True
