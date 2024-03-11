"""
activate job

"""

import datetime

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper


# logger
_logger = PandaLogger().getLogger("activator")


class Activator:
    """
    A class used to activate jobs.

    Attributes
    ----------
    task_buffer : TaskBuffer
        The task buffer that contains the jobs.
    dataset : DatasetSpec
        The dataset to be activated.
    enforce : bool
        A flag to enforce activation.

    Methods
    -------
    run():
        Starts the thread to activate jobs.
    """
    # constructor
    def __init__(self, taskBuffer, dataset, enforce: bool = False):
        """
        Constructs all the necessary attributes for the Activator object.

        Parameters
        ----------
            taskBuffer : TaskBuffer
                The task buffer that contains the jobs.
            dataset : DatasetSpec
                The dataset to be activated.
            enforce : bool, optional
                A flag to enforce activation (default is False).
        """
        self.dataset = dataset
        self.task_buffer = taskBuffer
        self.enforce = enforce

    # main
    def run(self):
        """
        Starts the thread to activate jobs.
        """
        tmp_log = LogWrapper(_logger,
                             f"run-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}-{self.dataset.name}")
        if self.dataset.status in ["completed", "deleting", "deleted"] and not self.enforce:
            tmp_log.debug(f"skip: {self.dataset.name}")
        else:
            # update input files
            panda_ids = self.task_buffer.updateInFilesReturnPandaIDs(self.dataset.name, "ready")
            tmp_log.debug(f"IDs: {panda_ids}")
            if len(panda_ids) != 0:
                # get job
                jobs = self.task_buffer.peekJobs(panda_ids, fromActive=False, fromArchived=False, fromWaiting=False)
                # remove None and unknown
                activate_jobs = []
                for job in jobs:
                    if job is not None and job.jobStatus != "unknown":
                        activate_jobs.append(job)
                # activate
                self.task_buffer.activateJobs(activate_jobs)
            # update dataset in DB
            if self.dataset.type == "dispatch":
                self.dataset.status = "completed"
                self.task_buffer.updateDatasets([self.dataset])
        tmp_log.debug(f"end: {self.dataset.name}")
