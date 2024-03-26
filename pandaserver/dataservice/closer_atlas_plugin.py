"""
ATLAS plugin for closer
"""
import sys

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.dataservice import DataServiceUtils


# plugin for ATLAS closer
class CloserAtlasPlugin:
    """
    A class used to represent the ATLAS closer plugin.

    ...

    Attributes
    ----------
    jobSpec : object
        an object representing the job specification
    datasets : list
        a list of datasets to be processed
    tmp_log : LogWrapper
        a LogWrapper object for logging

    Methods
    -------
    __init__(self, job, datasets, log)
        Constructs all the necessary attributes for the CloserAtlasPlugin object.
    execute(self)
        Executes the plugin's main functionality.
    """

    # constructor
    def __init__(self, job, datasets, log):
        """
        Constructs all the necessary attributes for the CloserAtlasPlugin object.

        Parameters
        ----------
            job : object
                an object representing the job specification
            datasets : list
                a list of datasets to be processed
            log : LogWrapper
                a LogWrapper object for logging
        """
        self.jobSpec = job
        self.datasets = datasets
        self.tmp_log = LogWrapper(log, f"{self.jobSpec.PandaID} CloserAtlasPlugin")

    # execute
    def execute(self):
        """
        Executes the main functionality of the CloserAtlasPlugin.

        This method is responsible for closing datasets that meet certain criteria.
        It only operates on production jobs that are either 'urgent' or have a high priority.
        For each dataset in the job, if the dataset's name ends with '_sub' followed by a number and its status is 'tobeclosed',
        it attempts to close the dataset using the rucioAPI. If an error occurs during this process, it logs a warning message.

        Returns
        -------
        bool
            Always returns True regardless of whether the datasets were successfully closed or not.
        """
        try:
            # only for production
            if self.jobSpec.prodSourceLabel not in ["managed", "test"]:
                return True
            # only for urgent or high prio
            if self.jobSpec.processingType not in ["urgent"] and self.jobSpec.currentPriority <= 1000:
                return True
            # close datasets
            for datasetSpec in self.datasets:
                if not DataServiceUtils.is_sub_dataset(datasetSpec.name):
                    continue
                if datasetSpec.status != "tobeclosed":
                    continue
                try:
                    self.tmp_log.debug(f"immediate close {datasetSpec.name}")
                    rucioAPI.close_dataset(datasetSpec.name)
                except Exception:
                    errtype, errvalue = sys.exc_info()[:2]
                    self.tmp_log.warning(f"failed to close : {errtype} {errvalue}")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            self.tmp_log.warning(f"failed to execute : {errtype} {errvalue}")
        return True
