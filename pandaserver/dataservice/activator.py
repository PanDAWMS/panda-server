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
    def __init__(self, taskBuffer, dataset, enforce=False):
        """
        Constructs all the necessary attributes for the Activator object.

        Parameters
        ----------
            task_buffer : TaskBuffer
                The task buffer that contains the jobs.
            dataset : DatasetSpec
                The dataset to be activated.
            enforce : bool, optional
                A flag to enforce activation (default is False).
        """
        self.dataset = dataset
        self.taskBuffer = taskBuffer
        self.enforce = enforce

    def start(self):
        """
        To keep backward compatibility
        """
        self.run()

    def join(self):
        """
        To keep backward compatibility
        """
        pass

    # main
    def run(self):
        """
        Starts the thread to activate jobs.
        """
        tmp_log = LogWrapper(_logger,
                             f"run-{datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
        tmp_log.debug(f"start: {self.dataset.name}")
        if self.dataset.status in ["completed", "deleting", "deleted"] and not self.enforce:
            tmp_log.debug(f"   skip: {self.dataset.name}")
        else:
            # update input files
            ids = self.taskBuffer.updateInFilesReturnPandaIDs(self.dataset.name, "ready")
            tmp_log.debug(f"IDs: {ids}")
            if len(ids) != 0:
                # get job
                jobs = self.taskBuffer.peekJobs(ids, fromActive=False, fromArchived=False, fromWaiting=False)
                # remove None and unknown
                acJobs = []
                for job in jobs:
                    if job is None or job.jobStatus == "unknown":
                        continue
                    acJobs.append(job)
                # activate
                self.taskBuffer.activateJobs(acJobs)
            # update dataset in DB
            if self.dataset.type == "dispatch":
                self.dataset.status = "completed"
                self.taskBuffer.updateDatasets([self.dataset])
        tmp_log.debug(f"end: {self.dataset.name}")
