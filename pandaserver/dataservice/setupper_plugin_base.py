"""
Base class for setupper plugins. It separates normal and jumbo jobs and sets parameters.
"""
from typing import List, Dict
from pandaserver.taskbuffer import EventServiceUtils


class SetupperPluginBase(object):
    """
    Base class for setupper plugins. It separates normal and jumbo jobs and sets parameters.
    """

    def __init__(self, taskBuffer, jobs: List, logger, params: Dict, default_map: Dict) -> None:
        """
        Constructor for the SetupperPluginBase class.

        :param task_buffer: The buffer for tasks.
        :param jobs: The jobs to be processed.
        :param logger: The logger to be used for logging.
        :param params: Additional parameters.
        :param default_map: Default parameters.
        """
        self.jobs = []
        self.jumbo_jobs = []
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
    def update_failed_jobs(self, jobs: List[object]) -> None:
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
