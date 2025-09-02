import datetime
import smtplib
import time
import uuid

from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore import Interaction
from pandaserver.config import panda_config
from pandaserver.taskbuffer import EventServiceUtils

# port for SMTP server
smtpPortList = [25, 587]


# wrapper to patch smtplib.stderr to send debug info to logger
class StderrLogger(object):
    def __init__(self, tmpLog):
        self.tmpLog = tmpLog

    def write(self, message):
        message = message.strip()
        if message != "":
            self.tmpLog.debug(message)


# wrapper of SMTP to redirect messages
class MySMTP(smtplib.SMTP):
    def set_log(self, tmp_log):
        self.tmpLog = tmp_log
        try:
            self.org_stderr = getattr(smtplib, "stderr")
            setattr(smtplib, "stderr", tmp_log)
        except Exception:
            self.org_stderr = None

    def _print_debug(self, *args):
        self.tmpLog.write(" ".join(map(str, args)))

    def reset_log(self):
        if self.org_stderr is not None:
            setattr(smtplib, "stderr", self.org_stderr)


# base class for post process
class PostProcessorBase(object):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.msgType = "postprocessor"
        self.failOnZeroOkFile = False
        self.refresh()

    # refresh
    def refresh(self):
        self.siteMapper = self.taskBufferIF.get_site_mapper()

    # basic post procedure
    def doBasicPostProcess(self, taskSpec, tmpLog):
        # update task status
        taskSpec.lockedBy = None
        taskSpec.status = self.getFinalTaskStatus(taskSpec)
        if taskSpec.status == "failed":
            # set dialog for preprocess
            if taskSpec.usePrePro() and not taskSpec.checkPreProcessed():
                taskSpec.setErrDiag("Preprocessing step failed", True)
        tmpMsg = f"set task_status={taskSpec.status}"
        tmpLog.info(tmpMsg)
        tmpLog.sendMsg(f"set task_status={taskSpec.status}", self.msgType)
        # update dataset
        for datasetSpec in taskSpec.datasetSpecList:
            if taskSpec.status in ["failed", "broken", "aborted"]:
                datasetSpec.status = "failed"
            else:
                # set dataset status
                if datasetSpec.type in ["output", "log", "lib"]:
                    # normal output datasets
                    if datasetSpec.nFiles and datasetSpec.nFilesFinished and datasetSpec.nFiles > datasetSpec.nFilesFinished:
                        datasetSpec.status = "finished"
                    else:
                        datasetSpec.status = "done"
                elif datasetSpec.type.startswith("trn_") or datasetSpec.type.startswith("tmpl_"):
                    # set done for template or transient datasets
                    datasetSpec.status = "done"
                else:
                    # not for input
                    continue
            # set nFiles
            if datasetSpec.type in ["output", "log", "lib"]:
                datasetSpec.nFiles = datasetSpec.nFilesFinished
            self.taskBufferIF.updateDataset_JEDI(datasetSpec, {"datasetID": datasetSpec.datasetID, "jediTaskID": datasetSpec.jediTaskID})
        # trigger internal dataset cleanup
        self.taskBufferIF.trigger_cleanup_internal_datasets(taskSpec.jediTaskID)
        # end time
        taskSpec.endTime = naive_utcnow()
        # update task
        self.taskBufferIF.updateTask_JEDI(taskSpec, {"jediTaskID": taskSpec.jediTaskID}, updateDEFT=True)
        # kill or kick child tasks
        if taskSpec.status in ["failed", "broken", "aborted"]:
            self.taskBufferIF.killChildTasks_JEDI(taskSpec.jediTaskID, taskSpec.status)
        else:
            self.taskBufferIF.kickChildTasks_JEDI(taskSpec.jediTaskID)
        tmpLog.debug(f"doBasicPostProcess done with taskStatus={taskSpec.status}")
        return

    # final procedure
    def doFinalProcedure(self, taskSpec, tmpLog):
        return self.SC_SUCCEEDED

    # send mail
    def sendMail(self, jediTaskID, fromAdd, toAdd, msgBody, nTry, fileBackUp, tmpLog):
        tmpLog.debug(f"sending notification to {toAdd}\n{msgBody}")
        for iTry in range(nTry):
            try:
                stderrLog = StderrLogger(tmpLog)
                smtpPort = smtpPortList[iTry % len(smtpPortList)]
                server = MySMTP(panda_config.emailSMTPsrv, smtpPort)
                server.set_debuglevel(1)
                server.set_log(stderrLog)
                server.ehlo()
                server.starttls()
                out = server.sendmail(fromAdd, toAdd, msgBody)
                tmpLog.debug(str(out))
                server.quit()
                break
            except Exception as e:
                if iTry + 1 < nTry:
                    # sleep for retry
                    tmpLog.debug(f"sleep {iTry} due to {str(e)}")
                    time.sleep(30)
                else:
                    tmpLog.error(f"failed to send notification with {str(e)}")
                    if fileBackUp:
                        # write to file which is processed in add.py
                        mailFile = "{0}/jmail_{1}_{2}" % (panda_config.logdir, jediTaskID, uuid.uuid4())
                        oMail = open(mailFile, "w")
                        oMail.write(str(jediTaskID) + "\n" + toAdd + "\n" + msgBody)
                        oMail.close()
                break
        try:
            server.reset_log()
        except Exception:
            pass

    # return email sender
    def senderAddress(self):
        return panda_config.emailSender

    # get task completeness
    def getTaskCompleteness(self, taskSpec):
        nFiles = 0
        nFilesFinished = 0
        totalInputEvents = 0
        totalOkEvents = 0
        for datasetSpec in taskSpec.datasetSpecList:
            if datasetSpec.isMasterInput():
                if datasetSpec.status == "removed":
                    continue
                nFiles += datasetSpec.nFiles
                nFilesFinished += datasetSpec.nFilesFinished
                try:
                    totalInputEvents += datasetSpec.nEvents
                except Exception:
                    pass
                try:
                    totalOkEvents += datasetSpec.nEventsUsed
                except Exception:
                    pass
        # completeness
        if totalInputEvents != 0:
            taskCompleteness = float(totalOkEvents) / float(totalInputEvents) * 1000.0
        elif nFiles != 0:
            taskCompleteness = float(nFilesFinished) / float(nFiles) * 1000.0
        else:
            taskCompleteness = 0
        return nFiles, nFilesFinished, totalInputEvents, totalOkEvents, taskCompleteness

    # get final task status
    def getFinalTaskStatus(self, taskSpec, checkParent=True, checkGoal=False):
        # count nFiles and nEvents
        nFiles, nFilesFinished, totalInputEvents, totalOkEvents, taskCompleteness = self.getTaskCompleteness(taskSpec)
        # set new task status
        if taskSpec.status == "tobroken":
            status = "broken"
        elif taskSpec.status == "toabort":
            status = "aborted"
        elif taskSpec.status == "paused":
            status = "paused"
        elif self.failOnZeroOkFile and nFiles == nFilesFinished == 0:
            status = "failed"
        elif nFiles == nFilesFinished:
            # check parent status
            if checkParent and taskSpec.parent_tid not in [None, taskSpec.jediTaskID]:
                parent_status = self.taskBufferIF.getTaskStatus_JEDI(taskSpec.parent_tid)
                if parent_status in ["failed", "broken", "aborted"]:
                    status = "failed"
                elif parent_status != "done":
                    status = "finished"
                else:
                    # check if input is mutable
                    inputMutable = False
                    for datasetSpec in taskSpec.datasetSpecList:
                        if datasetSpec.isMasterInput() and datasetSpec.state == "mutable":
                            inputMutable = True
                            break
                    if inputMutable:
                        status = "finished"
                    else:
                        status = "done"
            else:
                status = "done"
        elif nFilesFinished == 0:
            status = "failed"
        else:
            status = "finished"
        # task goal
        if taskSpec.goal is None:
            taskGoal = 1000
        else:
            taskGoal = taskSpec.goal
        # fail if goal is not reached
        if (
            taskSpec.failGoalUnreached()
            and status == "finished"
            and (not taskSpec.useExhausted() or (taskSpec.useExhausted() and taskSpec.status in ["passed"]))
        ):
            if taskCompleteness < taskGoal:
                status = "failed"
        # HPO tasks always go to finished
        if taskSpec.is_hpo_workflow():
            event_stat = self.taskBufferIF.get_event_statistics(taskSpec.jediTaskID)
            if event_stat is not None and event_stat.get(EventServiceUtils.ST_finished):
                status = "finished"
        # check goal only
        if checkGoal:
            # no goal
            if taskSpec.goal is not None and taskCompleteness >= taskGoal:
                return True
            return False
        # return status
        return status

    # pre-check
    def doPreCheck(self, taskSpec, tmpLog):
        # send task to exhausted
        if (
            taskSpec.useExhausted()
            and taskSpec.status not in ["passed"]
            and self.getFinalTaskStatus(taskSpec) in ["finished"]
            and not self.getFinalTaskStatus(taskSpec, checkGoal=True)
        ):
            taskSpec.status = "exhausted"
            if self.getFinalTaskStatus(taskSpec, checkParent=False) == "done":
                taskSpec.errorDialog = "exhausted since the parent task was incomplete and didn't reach the goal"
            else:
                taskSpec.errorDialog = "exhausted since the task was incomplete and didn't reach the goal"
            taskSpec.lockedBy = None
            taskSpec.lockedTime = None
            # update task
            tmpLog.info(f"set task_status={taskSpec.status}")
            self.taskBufferIF.updateTask_JEDI(taskSpec, {"jediTaskID": taskSpec.jediTaskID}, updateDEFT=True)
            # kick child tasks
            self.taskBufferIF.kickChildTasks_JEDI(taskSpec.jediTaskID)
            return True
        return False


Interaction.installSC(PostProcessorBase)
