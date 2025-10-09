import os
import socket

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandaserver.proxycache.token_cache import TokenCache
from pandaserver.srvcore import CoreUtils

from .WatchDogBase import WatchDogBase


# base class for typical watchdog (for production and analysis, etc.)
class TypicalWatchDogBase(WatchDogBase):
    # pre-action
    def pre_action(self, tmpLog, vo, prodSourceLabel, pid, *args, **kwargs):
        # rescue picked files
        tmpLog.info(f"rescue tasks with picked files for vo={vo} label={prodSourceLabel}")
        tmpRet = self.taskBufferIF.rescuePickedFiles_JEDI(vo, prodSourceLabel, jedi_config.watchdog.waitForPicked)
        if tmpRet is None:
            # failed
            tmpLog.error("failed to rescue")
        else:
            tmpLog.info(f"rescued {tmpRet} tasks")
        # reactivate pending tasks
        tmpLog.info(f"reactivate pending tasks for vo={vo} label={prodSourceLabel}")
        timeoutForPending = self.taskBufferIF.getConfigValue("watchdog", f"PENDING_TIMEOUT_{prodSourceLabel}", "jedi", vo)
        if timeoutForPending is None:
            if hasattr(jedi_config.watchdog, "timeoutForPendingVoLabel"):
                timeoutForPending = CoreUtils.getConfigParam(jedi_config.watchdog.timeoutForPendingVoLabel, vo, prodSourceLabel)
            if timeoutForPending is None:
                timeoutForPending = jedi_config.watchdog.timeoutForPending
            timeoutForPending = int(timeoutForPending) * 24
        tmpRet, msg_driven_taskid_set = self.taskBufferIF.reactivatePendingTasks_JEDI(
            vo, prodSourceLabel, jedi_config.watchdog.waitForPending, timeoutForPending
        )
        if tmpRet is None:
            # failed
            tmpLog.error("failed to reactivate")
        else:
            tmpLog.info(f"reactivated {tmpRet} tasks")
            for jeditaskid in msg_driven_taskid_set:
                push_ret = self.taskBufferIF.push_task_trigger_message("jedi_job_generator", jeditaskid)
                if push_ret:
                    tmpLog.debug(f"pushed trigger message to jedi_job_generator for jeditaskid={jeditaskid}")
                else:
                    tmpLog.warning(f"failed to push trigger message to jedi_job_generator for jeditaskid={jeditaskid}")
        # unlock tasks
        tmpLog.info(f"unlock tasks for vo={vo} label={prodSourceLabel} host={socket.getfqdn().split('.')[0]} pgid={os.getpgrp()}")
        tmpRet = self.taskBufferIF.unlockTasks_JEDI(vo, prodSourceLabel, 10, socket.getfqdn().split(".")[0], os.getpgrp())
        if tmpRet is None:
            # failed
            tmpLog.error("failed to unlock")
        else:
            tmpLog.info(f"unlock {tmpRet} tasks")
        # unlock tasks
        tmpLog.info(f"unlock tasks for vo={vo} label={prodSourceLabel}")
        tmpRet = self.taskBufferIF.unlockTasks_JEDI(vo, prodSourceLabel, jedi_config.watchdog.waitForLocked)
        if tmpRet is None:
            # failed
            tmpLog.error("failed to unlock")
        else:
            tmpLog.info(f"unlock {tmpRet} tasks")
        # restart contents update
        tmpLog.info(f"restart contents update for vo={vo} label={prodSourceLabel}")
        tmpRet, msg_driven_taskid_set = self.taskBufferIF.restartTasksForContentsUpdate_JEDI(vo, prodSourceLabel)
        if tmpRet is None:
            # failed
            tmpLog.error("failed to restart")
        else:
            tmpLog.info(f"restarted {tmpRet} tasks")
            for jeditaskid in msg_driven_taskid_set:
                push_ret = self.taskBufferIF.push_task_trigger_message("jedi_contents_feeder", jeditaskid)
                if push_ret:
                    tmpLog.debug(f"pushed trigger message to jedi_contents_feeder for jeditaskid={jeditaskid}")
                else:
                    tmpLog.warning(f"failed to push trigger message to jedi_contents_feeder for jeditaskid={jeditaskid}")
        # kick exhausted tasks
        tmpLog.info(f"kick exhausted tasks for vo={vo} label={prodSourceLabel}")
        tmpRet = self.taskBufferIF.kickExhaustedTasks_JEDI(vo, prodSourceLabel, jedi_config.watchdog.waitForExhausted)
        if tmpRet is None:
            # failed
            tmpLog.error("failed to kick")
        else:
            tmpLog.info(f"kicked {tmpRet} tasks")
        # finish tasks when goal is reached
        tmpLog.info(f"finish achieved tasks for vo={vo} label={prodSourceLabel}")
        tmpRet = self.taskBufferIF.getAchievedTasks_JEDI(vo, prodSourceLabel, jedi_config.watchdog.waitForAchieved)
        if tmpRet is None:
            # failed
            tmpLog.error("failed to finish")
        else:
            for jediTaskID in tmpRet:
                self.taskBufferIF.sendCommandTaskPanda(jediTaskID, "JEDI. Goal reached", True, "finish", comQualifier="soft")
            tmpLog.info(f"finished {tmpRet} tasks")
        # rescue unlocked tasks with picked files
        tmpLog.info(f"rescue unlocked tasks with picked files for vo={vo} label={prodSourceLabel}")
        tmpRet = self.taskBufferIF.rescueUnLockedTasksWithPicked_JEDI(vo, prodSourceLabel, 60, pid)
        if tmpRet is None:
            # failed
            tmpLog.error("failed to rescue unlocked tasks")
        else:
            tmpLog.info(f"rescue unlocked {tmpRet} tasks")
        # cache tokens
        got_lock = self.get_process_lock("TypicalWatchDogBase.cache_tokens", timeLimit=5)
        if not got_lock:
            tmpLog.debug("locked by another watchdog process. Skipped to cache tokens")
        else:
            tmpLog.info(f"cache tokens")
            cacher = TokenCache()
            cacher.run()

    # action to set scout job data w/o scouts
    def doActionToSetScoutJobData(self, gTmpLog):
        tmpRet = self.taskBufferIF.setScoutJobDataToTasks_JEDI(self.vo, self.prodSourceLabel)
        if tmpRet is None:
            # failed
            gTmpLog.error("failed to set scout job data")
        else:
            gTmpLog.info("set scout job data successfully")


Interaction.installSC(TypicalWatchDogBase)
