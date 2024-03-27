"""
A class used to handle DDM (Distributed Data Management) operations.
"""
import re
import threading

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.dataservice.activator import Activator
from pandaserver.dataservice.finisher import Finisher

# logger
_logger = PandaLogger().getLogger("ddm_handler")


class DDMHandler(threading.Thread):
    """
    A class used to handle DDM (Distributed Data Management) operations.

    Attributes
    ----------
    vuid : str
        The vuid of the dataset.
    task_buffer : TaskBuffer
        The task buffer that contains the jobs.
    site : str, optional
        The site where the jobs are running.
    dataset : str, optional
        The name of the dataset.
    scope : str, optional
        The scope of the dataset.

    Methods
    -------
    run():
        Starts the thread to handle DDM operations.
    """
    # constructor
    def __init__(self, task_buffer, vuid: str, site: str = None, dataset: str = None,
                     scope: str = None):
        """
        Constructs all the necessary attributes for the DDMHandler object.

        Parameters
        ----------
            task_buffer : TaskBuffer
                The task buffer that contains the jobs.
            vuid : str
                The vuid of the dataset.
            site : str
                The site where the jobs are running (default is None).
            dataset : str
                The name of the dataset (default is None).
            scope : str
                The scope of the dataset (default is None).
        """
        threading.Thread.__init__(self)
        self.vuid = vuid
        self.task_buffer = task_buffer
        self.site = site
        self.scope = scope
        self.dataset = dataset

    # main
    def run(self):
        """
        Starts the thread to handle DDM operations.
        """
        # get logger
        tmp_log = LogWrapper(
            _logger,
            f"<vuid={self.vuid} site={self.site} name={self.dataset}>",
        )
        # query dataset
        tmp_log.debug("start")
        if self.vuid is not None:
            dataset = self.task_buffer.queryDatasetWithMap({"vuid": self.vuid})
        else:
            dataset = self.task_buffer.queryDatasetWithMap({"name": self.dataset})
        if dataset is None:
            tmp_log.error("Not found")
            tmp_log.debug("end")
            return
        tmp_log.debug(f"type:{dataset.type} name:{dataset.name}")
        if dataset.type == "dispatch":
            # activate jobs in jobsDefined
            Activator(self.task_buffer, dataset).run()
        if dataset.type == "output":
            if dataset.name is not None and re.search("^panda\..*_zip$", dataset.name) is not None:
                # start unmerge jobs
                Activator(self.task_buffer, dataset, enforce=True).run()
            else:
                # finish transferring jobs
                Finisher(self.task_buffer, dataset, site=self.site).run()
        tmp_log.debug("end")
