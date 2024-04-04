"""
watch job

"""

import datetime
import threading
import time
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.dataservice.closer import Closer
from pandaserver.jobdispatcher import ErrorCode
from pandaserver.taskbuffer import EventServiceUtils, retryModule
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.SupErrors import SupErrors

# logger
_logger = PandaLogger().getLogger("Watcher")


class Watcher(threading.Thread):
    # constructor
    def __init__(self, taskBuffer, pandaID, single=False, sleepTime=360, sitemapper=None):
        threading.Thread.__init__(self)
        self.pandaID = pandaID
        self.taskBuffer = taskBuffer
        self.sleepTime = sleepTime
        self.single = single
        self.siteMapper = sitemapper
        self.logger = LogWrapper(_logger, str(pandaID))

    # main
    def run(self):
        try:
            while True:
                self.logger.debug("start")
                # query job
                job = self.taskBuffer.peekJobs(
                    [self.pandaID],
                    fromDefined=False,
                    fromArchived=False,
                    fromWaiting=False,
                )[0]
                # check job status
                if job is None:
                    self.logger.debug("escape : not found")
                    return
                self.logger.debug(f"in {job.jobStatus}")
                if job.jobStatus not in [
                    "running",
                    "sent",
                    "starting",
                    "holding",
                    "stagein",
                    "stageout",
                ]:
                    if job.jobStatus == "transferring" and (job.prodSourceLabel in ["user", "panda"] or job.jobSubStatus not in [None, "NULL", ""]):
                        pass
                    else:
                        self.logger.debug(f"escape : wrong status {job.jobStatus}")
                        return
                # time limit
                timeLimit = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - datetime.timedelta(minutes=self.sleepTime)
                if job.modificationTime < timeLimit or (job.endTime != "NULL" and job.endTime < timeLimit):
                    self.logger.debug(f"{job.jobStatus} lastmod:{str(job.modificationTime)} endtime:{str(job.endTime)}")
                    destDBList = []
                    if job.jobStatus == "sent":
                        # sent job didn't receive reply from pilot within 30 min
                        job.jobDispatcherErrorCode = ErrorCode.EC_SendError
                        job.jobDispatcherErrorDiag = "Sent job didn't receive reply from pilot within 30 min"
                    elif job.exeErrorDiag == "NULL" and job.pilotErrorDiag == "NULL":
                        # lost heartbeat
                        if job.jobDispatcherErrorDiag == "NULL":
                            if job.endTime == "NULL":
                                # normal lost heartbeat
                                job.jobDispatcherErrorCode = ErrorCode.EC_Watcher
                                job.jobDispatcherErrorDiag = f"lost heartbeat : {str(job.modificationTime)}"
                            else:
                                if job.jobStatus == "holding":
                                    job.jobDispatcherErrorCode = ErrorCode.EC_Holding
                                elif job.jobStatus == "transferring":
                                    job.jobDispatcherErrorCode = ErrorCode.EC_Transferring
                                else:
                                    job.jobDispatcherErrorCode = ErrorCode.EC_Timeout
                                job.jobDispatcherErrorDiag = f"timeout in {job.jobStatus} : last heartbeat at {str(job.endTime)}"
                            # get worker
                            workerSpecs = self.taskBuffer.getWorkersForJob(job.PandaID)
                            if len(workerSpecs) > 0:
                                workerSpec = workerSpecs[0]
                                if workerSpec.status in [
                                    "finished",
                                    "failed",
                                    "cancelled",
                                    "missed",
                                ]:
                                    job.supErrorCode = SupErrors.error_codes["WORKER_ALREADY_DONE"]
                                    job.supErrorDiag = f"worker already {workerSpec.status} at {str(workerSpec.endTime)} with {workerSpec.diagMessage}"
                                    job.supErrorDiag = JobSpec.truncateStringAttr("supErrorDiag", job.supErrorDiag)
                    else:
                        # job recovery failed
                        job.jobDispatcherErrorCode = ErrorCode.EC_Recovery
                        job.jobDispatcherErrorDiag = f"job recovery failed for {self.sleepTime / 60} hours"
                    # set job status
                    job.jobStatus = "failed"
                    # set endTime for lost heartbeat
                    if job.endTime == "NULL":
                        # normal lost heartbeat
                        job.endTime = job.modificationTime
                    # set files status
                    for file in job.Files:
                        if file.type == "output" or file.type == "log":
                            file.status = "failed"
                            if file.destinationDBlock not in destDBList:
                                destDBList.append(file.destinationDBlock)
                    # event service
                    if EventServiceUtils.isEventServiceJob(job) and not EventServiceUtils.isJobCloningJob(job):
                        eventStat = self.taskBuffer.getEventStat(job.jediTaskID, job.PandaID)
                        # set sub status when no sucessful events
                        if EventServiceUtils.ST_finished not in eventStat:
                            job.jobSubStatus = "es_heartbeat"
                    # update job
                    self.taskBuffer.updateJobs([job], False)
                    # start closer
                    if job.jobStatus == "failed":
                        source = "jobDispatcherErrorCode"
                        error_code = job.jobDispatcherErrorCode
                        error_diag = job.jobDispatcherErrorDiag
                        errors = [
                            {
                                "source": source,
                                "error_code": error_code,
                                "error_diag": error_diag,
                            }
                        ]

                        try:
                            self.logger.debug("Watcher will call apply_retrial_rules")
                            retryModule.apply_retrial_rules(self.taskBuffer, job.PandaID, errors, job.attemptNr)
                            self.logger.debug("apply_retrial_rules is back")
                        except Exception as e:
                            self.logger.debug(f"apply_retrial_rules excepted and needs to be investigated ({e}): {traceback.format_exc()}")

                        # updateJobs was successful and it failed a job with taskBufferErrorCode
                        try:
                            self.logger.debug("Watcher.run will peek the job")
                            job_tmp = self.taskBuffer.peekJobs(
                                [job.PandaID],
                                fromDefined=False,
                                fromArchived=True,
                                fromWaiting=False,
                            )[0]
                            if job_tmp.taskBufferErrorCode:
                                source = "taskBufferErrorCode"
                                error_code = job_tmp.taskBufferErrorCode
                                error_diag = job_tmp.taskBufferErrorDiag
                                self.logger.debug("Watcher.run 2 will call apply_retrial_rules")
                                retryModule.apply_retrial_rules(
                                    self.taskBuffer,
                                    job_tmp.PandaID,
                                    source,
                                    error_code,
                                    error_diag,
                                    job_tmp.attemptNr,
                                )
                                self.logger.debug("apply_retrial_rules 2 is back")
                        except IndexError:
                            pass
                        except Exception as e:
                            self.logger.error(f"apply_retrial_rules 2 excepted and needs to be investigated ({e}): {traceback.format_exc()}")

                        cThr = Closer(self.taskBuffer, destDBList, job)
                        cThr.run()
                    self.logger.debug("done")
                    return
                # single action
                if self.single:
                    return
                # sleep
                time.sleep(60 * self.sleepTime)
        except Exception as e:
            self.logger.error(f"run() : {str(e)} {traceback.format_exc()}")
            return
