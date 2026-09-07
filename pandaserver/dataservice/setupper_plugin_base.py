"""
Base class for setupper plugins. It separates normal and jumbo jobs and sets parameters.
"""

from typing import TYPE_CHECKING, Any, Dict, List

from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.taskbuffer.JobSpec import JobSpec

if TYPE_CHECKING:
    # LogWrapper reads a configuration file at import time and TaskBuffer imports this
    # package, so naming either for real here would cost this module its standalone
    # import. Annotations are evaluated at runtime in this tree, so the uses below are
    # quoted.
    from pandacommon.pandalogger.LogWrapper import LogWrapper

    from pandaserver.taskbuffer.TaskBuffer import TaskBuffer


class SetupperPluginBase(object):
    """
    Base class for setupper plugins. It separates normal and jumbo jobs and sets parameters.
    """

    # Installed by the params loop in __init__. setupper.py, the only place that builds a
    # plugin, always passes first_submission, and every plugin puts resubmit in its
    # default_map, so both are set by the time run() is called.
    first_submission: bool
    resubmit: bool

    def __init__(self, taskBuffer: "TaskBuffer", jobs: List[JobSpec], logger: "LogWrapper", params: Dict[str, Any], default_map: Dict[str, Any]) -> None:
        """
        Constructor for the SetupperPluginBase class.

        :param task_buffer: The buffer for tasks.
        :param jobs: The jobs to be processed.
        :param logger: The logger to be used for logging.
        :param params: Additional parameters.
        :param default_map: Default parameters.
        """
        self.jobs: List[JobSpec] = []
        self.jumbo_jobs: List[JobSpec] = []
        # separate normal and jumbo jobs
        for job in jobs:
            if EventServiceUtils.isJumboJob(job):
                self.jumbo_jobs.append(job)
            else:
                self.jobs.append(job)
        self.task_buffer = taskBuffer
        self.logger = logger
        # set named parameters
        for key, value in params.items():
            setattr(self, key, value)
        # set defaults
        for key, value in default_map.items():
            if not hasattr(self, key):
                setattr(self, key, value)

    # abstracts
    def run(self) -> None:
        """
        Abstract method for running the plugin. To be implemented in subclasses.
        """
        pass

    def post_run(self) -> None:
        """
        Abstract method to be called after the run method. To be implemented in subclasses.
        """
        pass

    # update failed jobs
    def update_failed_jobs(self, jobs: List[JobSpec]) -> None:
        """
        Updates the status of failed jobs.

        :param jobs: The jobs to be updated.
        """
        for job in jobs:
            # set file status
            for file in job.Files:
                if file.type in ["output", "log"]:
                    if file.status not in ["missing"]:
                        file.status = "failed"
        self.task_buffer.updateJobs(jobs, True)
