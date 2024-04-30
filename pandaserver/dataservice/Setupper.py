"""
setup dataset

"""

import sys
import threading
import traceback
import uuid

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.taskbuffer.PickleJobSpec import PickleJobSpec

# logger
_logger = PandaLogger().getLogger("Setupper")

panda_config.setupPlugin()


# main class
class Setupper(threading.Thread):
    # constructor
    def __init__(
        self,
        taskBuffer,
        jobs,
        resubmit=False,
        pandaDDM=False,
        ddmAttempt=0,
        forkRun=False,
        onlyTA=False,
        resetLocation=False,
        firstSubmission=True,
    ):
        threading.Thread.__init__(self)
        self.jobs = jobs
        self.taskBuffer = taskBuffer
        # resubmission or not
        self.resubmit = resubmit
        # use PandaDDM
        self.pandaDDM = pandaDDM
        # priority for ddm job
        self.ddmAttempt = ddmAttempt
        # fork another process because python doesn't release memory
        self.forkRun = forkRun
        # run task assignment only
        self.onlyTA = onlyTA
        # reset locations
        self.resetLocation = resetLocation
        # first submission
        self.firstSubmission = firstSubmission

    # main
    def run(self):
        try:
            # make a message instance
            tmpLog = LogWrapper(_logger, None)
            # run main procedure in the same process
            if not self.forkRun:
                tmpLog.debug("main start")
                tmpLog.debug(f"firstSubmission={self.firstSubmission}")
                # make Specs pickleable
                p_job_list = []
                for job_spec in self.jobs:
                    p_job = PickleJobSpec()
                    p_job.update(job_spec)
                    p_job_list.append(p_job)
                self.jobs = p_job_list
                # group jobs per VO
                voJobsMap = {}
                ddmFreeJobs = []
                tmpLog.debug(f"{len(self.jobs)} jobs in total")
                for tmpJob in self.jobs:
                    # set VO=local for DDM free
                    if tmpJob.destinationSE == "local":
                        tmpVO = "local"
                    else:
                        tmpVO = tmpJob.VO
                    # make map
                    voJobsMap.setdefault(tmpVO, [])
                    voJobsMap[tmpVO].append(tmpJob)
                # loop over all VOs
                for tmpVO in voJobsMap:
                    tmpJobList = voJobsMap[tmpVO]
                    tmpLog.debug(f"vo={tmpVO} has {len(tmpJobList)} jobs")
                    # get plugin
                    setupperPluginClass = panda_config.getPlugin("setupper_plugins", tmpVO)
                    if setupperPluginClass is None:
                        # use ATLAS plug-in by default
                        from pandaserver.dataservice.SetupperAtlasPlugin import (
                            SetupperAtlasPlugin,
                        )

                        setupperPluginClass = SetupperAtlasPlugin
                    tmpLog.debug(f"plugin name -> {setupperPluginClass.__name__}")
                    try:
                        # make plugin
                        setupperPlugin = setupperPluginClass(
                            self.taskBuffer,
                            self.jobs,
                            tmpLog,
                            resubmit=self.resubmit,
                            pandaDDM=self.pandaDDM,
                            ddmAttempt=self.ddmAttempt,
                            onlyTA=self.onlyTA,
                            firstSubmission=self.firstSubmission,
                        )
                        # run plugin
                        tmpLog.debug("run plugin")
                        setupperPlugin.run()
                        # go forward if not TA
                        if not self.onlyTA:
                            # update jobs
                            tmpLog.debug("update jobs")
                            self.updateJobs(setupperPlugin.jobs + setupperPlugin.jumboJobs, tmpLog)
                            # execute post process
                            tmpLog.debug("post execute plugin")
                            setupperPlugin.postRun()
                        tmpLog.debug("done plugin")
                    except Exception:
                        errtype, errvalue = sys.exc_info()[:2]
                        tmpLog.error(f"plugin failed with {errtype}:{errvalue}")
                tmpLog.debug("main end")
            else:
                tmpLog.debug("fork start")
                # write jobs to file
                import os

                try:
                    import cPickle as pickle
                except ImportError:
                    import pickle
                outFileName = f"{panda_config.logdir}/set.{self.jobs[0].PandaID}_{str(uuid.uuid4())}"
                outFile = open(outFileName, "wb")
                pickle.dump(self.jobs, outFile, protocol=0)
                outFile.close()
                # run main procedure in another process because python doesn't release memory
                com = f"cd {panda_config.home_dir_cwd} > /dev/null 2>&1; export HOME={panda_config.home_dir_cwd}; "
                com += "env PYTHONPATH=%s:%s %s/python -Wignore %s/dataservice/forkSetupper.py -i %s" % (
                    panda_config.pandaCommon_dir,
                    panda_config.pandaPython_dir,
                    panda_config.native_python,
                    panda_config.pandaPython_dir,
                    outFileName,
                )
                if self.onlyTA:
                    com += " -t"
                if not self.firstSubmission:
                    com += " -f"
                tmpLog.debug(com)
                # execute
                status, output = self.taskBuffer.processLimiter.getstatusoutput(com)
                tmpLog.debug(f"return from main process: {status} {output}")
                tmpLog.debug("fork end")
        except Exception as e:
            tmpLog.error(f"master failed with {str(e)} {traceback.format_exc()}")

    #  update jobs
    def updateJobs(self, jobList, tmpLog):
        updateJobs = []
        failedJobs = []
        activateJobs = []
        waitingJobs = []
        closeJobs = []
        # sort out jobs
        for job in jobList:
            # failed jobs
            if job.jobStatus in ["failed", "cancelled"]:
                failedJobs.append(job)
            # waiting
            elif job.jobStatus == "waiting":
                waitingJobs.append(job)
            # no input jobs
            elif job.dispatchDBlock == "NULL":
                activateJobs.append(job)
            # normal jobs
            else:
                # change status
                job.jobStatus = "assigned"
                updateJobs.append(job)
        # trigger merge generation if all events are done
        newActivateJobs = []
        nFinished = 0
        for job in activateJobs:
            if job.notDiscardEvents() and job.allOkEvents() and not EventServiceUtils.isEventServiceMerge(job):
                self.taskBuffer.activateJobs([job])
                # change status
                job.jobStatus = "finished"
                self.taskBuffer.updateJobs([job], False)
                nFinished += 1
            else:
                newActivateJobs.append(job)
        activateJobs = newActivateJobs
        tmpLog.debug(f"# of finished jobs in activated : {nFinished}")
        newUpdateJobs = []
        nFinished = 0
        for job in updateJobs:
            if job.notDiscardEvents() and job.allOkEvents() and not EventServiceUtils.isEventServiceMerge(job):
                self.taskBuffer.updateJobs([job], True)
                # change status
                job.jobStatus = "finished"
                self.taskBuffer.updateJobs([job], True)
                nFinished += 1
            else:
                newUpdateJobs.append(job)
        updateJobs = newUpdateJobs
        tmpLog.debug(f"# of finished jobs in defined : {nFinished}")
        # update DB
        tmpLog.debug(f"# of activated jobs : {len(activateJobs)}")
        self.taskBuffer.activateJobs(activateJobs)
        tmpLog.debug(f"# of updated jobs : {len(updateJobs)}")
        self.taskBuffer.updateJobs(updateJobs, True)
        tmpLog.debug(f"# of failed jobs : {len(failedJobs)}")
        self.taskBuffer.updateJobs(failedJobs, True)
        tmpLog.debug(f"# of waiting jobs : {len(waitingJobs)}")
        self.taskBuffer.keepJobs(waitingJobs)
        # delete local values
        del updateJobs
        del failedJobs
        del activateJobs
        del waitingJobs
