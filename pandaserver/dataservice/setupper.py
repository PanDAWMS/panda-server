"""
This module is responsible for setting up the dataset for the PanDA server.
This module contains the Setupper class, which is a thread that sets up the dataset for a list of jobs. The jobs are processed according to various parameters, such as whether the job is a resubmission and whether it's the first submission.
The Setupper class also contains methods for running the setup process and updating the status of jobs.
This module uses the PandaLogger for logging and the panda_config for configuration. It also imports several other modules from the pandaserver package.
"""

import sys
import threading
import traceback
from typing import List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.taskbuffer.PickleJobSpec import PickleJobSpec

_logger = PandaLogger().getLogger("setupper")

panda_config.setupPlugin()


# main class
class Setupper(threading.Thread):
    """
    The Setupper class is the main class responsible for setting up the dataset in the PanDA server.
    The Setupper class is initialized with a list of jobs that need to be processed, along with several other parameters such as whether the job is a resubmission and whether it's the first submission.
    The class contains a run method which is the main method for running the setup process. This method groups jobs per VO, gets the appropriate plugin for each VO, and runs the plugin. It also updates the jobs and executes the post process of the plugin.
    The class also contains an update_jobs method which updates the status of jobs. This method sorts jobs by status, updates the jobs in the task buffer, and updates the database.
    This class uses the PandaLogger for logging and the panda_config for configuration. It also imports several other modules from the pandaserver package.
    """

    # constructor
    def __init__(
        self,
        taskBuffer,
        jobs: List[object],
        resubmit: bool = False,
        first_submission: bool = True,
    ):
        """
        Constructor for the Setupper class.

        :param taskBuffer: The buffer for tasks.
        :param jobs: The jobs to be processed.
        :param resubmit: A flag to indicate if the job is a resubmission. Defaults to False.
        :param first_submission: A flag to indicate if it's the first submission. Defaults to True.
        """
        threading.Thread.__init__(self)
        self.jobs = jobs
        self.task_buffer = taskBuffer
        # resubmission or not
        self.resubmit = resubmit
        # first submission
        self.first_submission = first_submission

    # main
    def run(self) -> None:
        """
        This is the main method for running the setup process. It is responsible for the following:
        1. Creating a message instance for logging.
        2. Making the job specifications pickleable for serialization.
        3. Grouping jobs per Virtual Organization (VO).
        4. Getting the appropriate plugin for each VO and running it.
        5. Updating the jobs and executing the post process of the plugin.
        This method handles any exceptions that occur during the setup process and logs the error message.

        :return: None
        """
        try:
            # prefix: None will trigger to use a timestamp as prefix, which is used to group runs
            tmp_log = LogWrapper(_logger, None)
            # run main procedure in the same process
            tmp_log.debug("start")
            tmp_log.debug(f"first_submission={self.first_submission}")
            # make Specs pickleable
            p_job_list = []
            for job_spec in self.jobs:
                p_job = PickleJobSpec()
                p_job.update(job_spec)
                p_job_list.append(p_job)
            self.jobs = p_job_list
            # group jobs per VO
            vo_jobs_map = {}
            tmp_log.debug(f"{len(self.jobs)} jobs in total")
            for tmp_job in self.jobs:
                # set VO=local for DDM free
                if tmp_job.destinationSE == "local":
                    tmp_vo = "local"
                else:
                    tmp_vo = tmp_job.VO
                # make map
                vo_jobs_map.setdefault(tmp_vo, [])
                vo_jobs_map[tmp_vo].append(tmp_job)
            # loop over all VOs
            for tmp_vo in vo_jobs_map:
                tmp_job_list = vo_jobs_map[tmp_vo]
                tmp_log.debug(f"vo={tmp_vo} has {len(tmp_job_list)} jobs")
                # get plugin
                setupper_plugin_class = panda_config.getPlugin("setupper_plugins", tmp_vo)
                if setupper_plugin_class is None:
                    # use ATLAS plug-in by default
                    from pandaserver.dataservice.setupper_atlas_plugin import (
                        SetupperAtlasPlugin,
                    )

                    setupper_plugin_class = SetupperAtlasPlugin
                tmp_log.debug(f"plugin name -> {setupper_plugin_class.__name__}")
                try:
                    # make plugin
                    setupper_plugin = setupper_plugin_class(
                        self.task_buffer,
                        self.jobs,
                        tmp_log,
                        resubmit=self.resubmit,
                        first_submission=self.first_submission,
                    )
                    # run plugin
                    tmp_log.debug("run plugin")
                    setupper_plugin.run()
                    # update jobs
                    tmp_log.debug("update jobs")
                    self.update_jobs(setupper_plugin.jobs + setupper_plugin.jumbo_jobs, tmp_log)
                    # execute post process
                    tmp_log.debug("post execute plugin")
                    setupper_plugin.post_run()
                    tmp_log.debug("done plugin")
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    tmp_log.error(f"plugin failed with {error_type}:{error_value}")
            tmp_log.debug("end")
        except Exception as error:
            tmp_log.error(f"failed with {str(error)} {traceback.format_exc()}")

    #  update jobs
    def update_jobs(self, job_list: List[object], tmp_log: LogWrapper) -> None:
        """
        This method is responsible for updating the status of jobs in the PanDA server.
        It sorts the jobs by their status into different categories: failed, waiting, no input, and normal jobs.
        For each category, it performs the appropriate actions. For example, it changes their status to "assigned".
        It also handles the activation of jobs. If a job should not discard any events, all events in the job are okay, and the job is not an Event Service Merge job, then the job's status is updated to "finished".
        This method also updates the database with the new job statuses.

        :param job_list: The list of jobs to be updated.
        :param tmp_log: The logger to be used for logging.
        :return: None
        """
        update_jobs = []
        failed_jobs = []
        activate_jobs = []
        waiting_jobs = []
        # sort jobs by status
        for job in job_list:
            # failed jobs
            if job.jobStatus in ["failed", "cancelled"]:
                failed_jobs.append(job)
            # waiting
            elif job.jobStatus == "waiting":
                waiting_jobs.append(job)
            # no input jobs
            elif job.dispatchDBlock == "NULL":
                activate_jobs.append(job)
            # normal jobs
            else:
                # change status
                job.jobStatus = "assigned"
                update_jobs.append(job)

        # trigger merge generation if all events are done
        new_activate_jobs = []
        n_finished = 0
        # Iterate over each job in the activate_jobs list
        for job in activate_jobs:
            # Check if the job should not discard events, if all events are okay, and if the job is not an Event Service Merge job
            # notDiscardEvents() returns True if the job should not discard any events
            # allOkEvents() returns True if all events in the job are okay (no errors or issues)
            if job.notDiscardEvents() and job.allOkEvents() and not EventServiceUtils.isEventServiceMerge(job):
                # If all conditions are met, update the job in the task buffer
                self.task_buffer.updateJobs([job])
                # Change the job status to "finished"
                job.jobStatus = "finished"
                # Update the job in the task buffer again with the new status
                # The second parameter False indicates that the job status should not be checked before the update
                self.task_buffer.updateJobs([job], False)
                # Increment the counter for finished jobs
                n_finished += 1
            else:
                # If any of the conditions are not met, add the job to a new list new_activate_jobs
                new_activate_jobs.append(job)
        # Replace the activate_jobs list with new_activate_jobs
        activate_jobs = new_activate_jobs
        tmp_log.debug(f"# of finished jobs in activated : {n_finished}")
        new_update_jobs = []
        n_finished = 0
        # Repeat a similar process for the update_jobs list
        for job in update_jobs:
            if job.notDiscardEvents() and job.allOkEvents() and not EventServiceUtils.isEventServiceMerge(job):
                # The second parameter True indicates that the job status should be checked before the update
                self.task_buffer.updateJobs([job], True)
                # change status
                job.jobStatus = "finished"
                self.task_buffer.updateJobs([job], True)
                n_finished += 1
            else:
                new_update_jobs.append(job)
        update_jobs = new_update_jobs
        tmp_log.debug(f"# of finished jobs in defined : {n_finished}")
        # Update the database
        tmp_log.debug(f"# of activated jobs : {len(activate_jobs)}")
        self.task_buffer.activateJobs(activate_jobs)
        tmp_log.debug(f"# of updated jobs : {len(update_jobs)}")
        # Update the jobs in the 'update_jobs' list. The 'True' argument indicates that the jobs are to be updated immediately
        self.task_buffer.updateJobs(update_jobs, True)
        tmp_log.debug(f"# of failed jobs : {len(failed_jobs)}")
        # Update the failed jobs in the 'failed_jobs' list. The 'True' argument indicates that the jobs are to be updated immediately
        self.task_buffer.updateJobs(failed_jobs, True)
        tmp_log.debug(f"# of waiting jobs : {len(waiting_jobs)}")
        # Keep the jobs in the 'waiting_jobs' list in the waiting state
        self.task_buffer.keepJobs(waiting_jobs)
        # delete local values
        del update_jobs
        del failed_jobs
        del activate_jobs
        del waiting_jobs
