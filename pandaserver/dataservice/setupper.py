"""
setup dataset

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

# logger
_logger = PandaLogger().getLogger("setupper")

panda_config.setupPlugin()


# main class
class Setupper(threading.Thread):
    """
    Main class for setting up the dataset.
    """
    # constructor
    def __init__(
        self,
        taskBuffer,
        jobs: List[object],
        resubmit: bool = False,
        panda_ddm: bool = False,
        ddm_attempt: int = 0,
        only_ta: bool = False,
        first_submission: bool = True,
    ):
        """
        Constructor for the Setupper class.

        :param taskBuffer: The buffer for tasks.
        :param jobs: The jobs to be processed.
        :param resubmit: A flag to indicate if the job is a resubmission. Defaults to False.
        :param panda_ddm: A flag to indicate if PandaDDM is used. Defaults to False.
        :param ddm_attempt: The number of attempts for DDM job. Defaults to 0.
        :param only_ta: A flag to indicate if only task assignment should be run. Defaults to False.
        :param first_submission: A flag to indicate if it's the first submission. Defaults to True.
        """
        threading.Thread.__init__(self)
        self.jobs = jobs
        self.task_buffer = taskBuffer
        # resubmission or not
        self.resubmit = resubmit
        # use PandaDDM
        self.panda_ddm = panda_ddm
        # priority for ddm job
        self.ddm_attempt = ddm_attempt
        # run task assignment only
        self.only_ta = only_ta
        # first submission
        self.first_submission = first_submission

    # main
    def run(self) -> None:
        """
        Main method for running the setup process.
        """
        try:
            # make a message instance
            tmp_log = LogWrapper(_logger, None)
            # run main procedure in the same process
            tmp_log.debug("main start")
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
                    from pandaserver.dataservice.setupper_atlas_plugin import SetupperAtlasPlugin
                    setupper_plugin_class = SetupperAtlasPlugin
                tmp_log.debug(f"plugin name -> {setupper_plugin_class.__name__}")
                try:
                    # make plugin
                    setupper_plugin = setupper_plugin_class(
                        self.task_buffer,
                        self.jobs,
                        tmp_log,
                        resubmit=self.resubmit,
                        panda_ddm=self.panda_ddm,
                        ddm_attempt=self.ddm_attempt,
                        only_ta=self.only_ta,
                        first_submission=self.first_submission,
                    )
                    # run plugin
                    tmp_log.debug("run plugin")
                    setupper_plugin.run()
                    # go forward if not Task Assignment
                    if not self.only_ta:
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
            tmp_log.debug("main end")
        except Exception as error:
            tmp_log.error(f"master failed with {str(error)} {traceback.format_exc()}")

    #  update jobs
    def update_jobs(self, job_list: List[object], tmp_log: LogWrapper) -> None:
        """
        Updates the status of jobs.

        :param job_list: The list of jobs to be updated.
        :param log: The logger to be used for logging.
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
        for job in activate_jobs:
            if job.notDiscardEvents() and job.allOkEvents() and not EventServiceUtils.isEventServiceMerge(job):
                self.task_buffer.updateJobs([job])
                # change status
                job.jobStatus = "finished"
                self.task_buffer.updateJobs([job], False)
                n_finished += 1
            else:
                new_activate_jobs.append(job)
        activate_jobs = new_activate_jobs
        tmp_log.debug(f"# of finished jobs in activated : {n_finished}")
        new_update_jobs = []
        n_finished = 0
        for job in update_jobs:
            if job.notDiscardEvents() and job.allOkEvents() and not EventServiceUtils.isEventServiceMerge(job):
                self.task_buffer.updateJobs([job], True)
                # change status
                job.jobStatus = "finished"
                self.task_buffer.updateJobs([job], True)
                n_finished += 1
            else:
                new_update_jobs.append(job)
        update_jobs = new_update_jobs
        tmp_log.debug(f"# of finished jobs in defined : {n_finished}")
        # update DB
        tmp_log.debug(f"# of activated jobs : {len(activate_jobs)}")
        self.task_buffer.activateJobs(activate_jobs)
        tmp_log.debug(f"# of updated jobs : {len(update_jobs)}")
        self.task_buffer.updateJobs(update_jobs, True)
        tmp_log.debug(f"# of failed jobs : {len(failed_jobs)}")
        self.task_buffer.updateJobs(failed_jobs, True)
        tmp_log.debug(f"# of waiting jobs : {len(waiting_jobs)}")
        self.task_buffer.keepJobs(waiting_jobs)
        # delete local values
        del update_jobs
        del failed_jobs
        del activate_jobs
        del waiting_jobs
