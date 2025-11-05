import datetime
import os
import re
import socket
import sys
import traceback

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread
from pandaserver.workflow.workflow_core import WorkflowInterface

from .WatchDogBase import WatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


class AtlasWorkflowProcessorWatchDog(WatchDogBase):
    """
    Workflow processor watchdog for ATLAS
    """

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.vo = "atlas"
        self.workflow_interface = WorkflowInterface(taskBufferIF)

    def doProcessWorkflows(self):
        """
        Action to process active workflows
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doProcessWorkflows")
        tmpLog.debug("start")
        try:
            # watchdog lock
            got_lock = self.get_process_lock("AtlasWFProcDog.doProcessWorkflows", timeLimit=1)
            if not got_lock:
                tmpLog.debug("locked by another watchdog process. Skipped")
                return
            tmpLog.debug("got watchdog lock")
            # process active workflows
            stats = self.workflow_interface.process_active_workflows()
            tmpLog.info(f"processed workflows: {stats}")
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug("start")
            # clean up
            # check
            # process workflows
            self.doProcessWorkflows()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            origTmpLog.error(f"failed with {errtype} {errvalue}")
        # return
        origTmpLog.debug("done")
        return self.SC_SUCCEEDED
