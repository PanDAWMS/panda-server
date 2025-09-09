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
from pandaserver.taskbuffer.DataCarousel import (
    DataCarouselInterface,
    DataCarouselRequestSpec,
    DataCarouselRequestStatus,
)

from .WatchDogBase import WatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


class AtlasDataCarouselWatchDog(WatchDogBase):
    """
    Data Carousel watchdog for ATLAS
    """

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.vo = "atlas"
        self.ddmIF = ddmIF.getInterface(self.vo)
        self.data_carousel_interface = DataCarouselInterface(taskBufferIF, self.ddmIF)

    def doStageDCRequests(self):
        """
        Action to get queued DC requests and start staging
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doStageDCRequests")
        tmpLog.debug("start")
        try:
            # watchdog lock
            got_lock = self.get_process_lock("AtlasDataCarousDog.doStageDCReq", timeLimit=5)
            if not got_lock:
                tmpLog.debug("locked by another watchdog process. Skipped")
                return
            tmpLog.debug("got watchdog lock")
            # get DC requests to stage
            to_stage_list = self.data_carousel_interface.get_requests_to_stage()
            # stage the requests
            for dc_req_spec, extra_params in to_stage_list:
                self.data_carousel_interface.stage_request(dc_req_spec, extra_params=extra_params)
                tmpLog.debug(f"stage request_id={dc_req_spec.request_id} dataset={dc_req_spec.dataset}")
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    def doCheckDCRequests(self):
        """
        Action to check active DC requests
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doCheckDCRequests")
        tmpLog.debug("start")
        try:
            # watchdog lock
            got_lock = self.get_process_lock("AtlasDataCarousDog.doCheckDCRequests", timeLimit=5)
            if not got_lock:
                tmpLog.debug("locked by another watchdog process. Skipped")
                return
            tmpLog.debug("got watchdog lock")
            # check for staging
            self.data_carousel_interface.check_staging_requests()
            # resume tasks with requests in staging
            self.data_carousel_interface.resume_tasks_from_staging()
            # done
            tmpLog.debug(f"done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    def doKeepRulesAlive(self):
        """
        Action to keep DDM staging rules alive when tasks running
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doKeepRulesAlive")
        tmpLog.debug("start")
        try:
            # watchdog lock
            got_lock = self.get_process_lock("AtlasDataCarousDog.doKeepRulesAlive", timeLimit=60)
            if not got_lock:
                tmpLog.debug("locked by another watchdog process. Skipped")
                return
            tmpLog.debug("got watchdog lock")
            # get requests of active tasks
            self.data_carousel_interface.keep_alive_ddm_rules()
            # done
            tmpLog.debug(f"done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    def doCleanDCRequests(self):
        """
        Action to clean up old DC requests in DB table
        """
        tmpLog = MsgWrapper(logger, " #ATM #KV doCleanDCRequests")
        tmpLog.debug("start")
        try:
            # watchdog lock
            got_lock = self.get_process_lock("AtlasDataCarousDog.doCleanDCReq", timeLimit=1440)
            if not got_lock:
                tmpLog.debug("locked by another watchdog process. Skipped")
                return
            tmpLog.debug("got watchdog lock")
            # clean up
            self.data_carousel_interface.clean_up_requests()
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
            # clean up old DC requests
            self.doCleanDCRequests()
            # keep staging rules alive
            self.doKeepRulesAlive()
            # check staging requests
            self.doCheckDCRequests()
            # stage queued requests
            self.doStageDCRequests()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            origTmpLog.error(f"failed with {errtype} {errvalue}")
        # return
        origTmpLog.debug("done")
        return self.SC_SUCCEEDED
