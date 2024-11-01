"""
provide web interface to users

"""

import datetime
import json
import re
import sys
import time
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

import pandaserver.jobdispatcher.Protocol as Protocol
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.CoreUtils import clean_user_id, resolve_bool
from pandaserver.taskbuffer import JobUtils, PrioUtil
from pandaserver.taskbuffer.WrappedPickle import WrappedPickle

_logger = PandaLogger().getLogger("harvester_api")


class HarvesterAPI:
    def __init__(self):
        self.task_buffer = None

    # initialize
    def init(self, task_buffer):
        self.task_buffer = task_buffer

    # update workers
    def updateWorkers(self, user, host, harvesterID, data):
        ret = self.task_buffer.updateWorkers(harvesterID, data)
        if ret is None:
            return_value = (False, MESSAGE_DATABASE)
        else:
            return_value = (True, ret)
        return json.dumps(return_value)

    # update workers
    def updateServiceMetrics(self, user, host, harvesterID, data):
        ret = self.task_buffer.updateServiceMetrics(harvesterID, data)
        if ret is None:
            return_value = (False, MESSAGE_DATABASE)
        else:
            return_value = (True, ret)
        return json.dumps(return_value)

    # add harvester dialog messages
    def addHarvesterDialogs(self, user, harvesterID, dialogs):
        ret = self.task_buffer.addHarvesterDialogs(harvesterID, dialogs)
        if not ret:
            return_value = (False, MESSAGE_DATABASE)
        else:
            return_value = (True, "")
        return json.dumps(return_value)

    # heartbeat for harvester
    def harvesterIsAlive(self, user, host, harvesterID, data):
        ret = self.task_buffer.harvesterIsAlive(user, host, harvesterID, data)
        if ret is None:
            return_value = (False, MESSAGE_DATABASE)
        else:
            return_value = (True, ret)
        return json.dumps(return_value)

    # get stats of workers
    def getWorkerStats(self):
        return self.task_buffer.getWorkerStats()

    # report stat of workers
    def reportWorkerStats(self, harvesterID, siteName, paramsList):
        return self.task_buffer.reportWorkerStats(harvesterID, siteName, paramsList)

    # report stat of workers
    def reportWorkerStats_jobtype(self, harvesterID, siteName, paramsList):
        return self.task_buffer.reportWorkerStats_jobtype(harvesterID, siteName, paramsList)

    # sweep panda queue
    def sweepPQ(self, panda_queue, status_list, ce_list, submission_host_list):
        try:
            panda_queue_des = json.loads(panda_queue)
            status_list_des = json.loads(status_list)
            ce_list_des = json.loads(ce_list)
            submission_host_list_des = json.loads(submission_host_list)
        except Exception:
            _logger.error("Problem deserializing variables")

        ret = self.taskBuffer.sweepPQ(panda_queue_des, status_list_des, ce_list_des, submission_host_list_des)
        return WrappedPickle.dumps(ret)


# Singleton
harvester_api = HarvesterAPI()
del harvester_api


# update workers
def updateWorkers(req, harvesterID, workers):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # get DN
    user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    return_value = None
    tStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    # convert
    try:
        data = json.loads(workers)
    except Exception:
        return_value = json.dumps((False, MESSAGE_JSON))
    # update
    if return_value is None:
        return_value = userIF.updateWorkers(user, host, harvesterID, data)
    tDelta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - tStart
    _logger.debug(f"updateWorkers {harvesterID} took {tDelta.seconds}.{tDelta.microseconds // 1000:03d} sec")

    return return_value


# update workers
def updateServiceMetrics(req, harvesterID, metrics):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))

    user = _getDN(req)

    host = req.get_remote_host()
    return_value = None
    tStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    # convert
    try:
        data = json.loads(metrics)
    except Exception:
        return_value = json.dumps((False, MESSAGE_JSON))

    # update
    if return_value is None:
        return_value = userIF.updateServiceMetrics(user, host, harvesterID, data)

    tDelta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - tStart
    _logger.debug(f"updateServiceMetrics {harvesterID} took {tDelta.seconds}.{tDelta.microseconds // 1000:03d} sec")

    return return_value


# add harvester dialog messages
def addHarvesterDialogs(req, harvesterID, dialogs):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # get DN
    user = _getDN(req)
    # convert
    try:
        data = json.loads(dialogs)
    except Exception:
        return json.dumps((False, MESSAGE_JSON))
    # update
    return userIF.addHarvesterDialogs(user, harvesterID, data)


# heartbeat for harvester
def harvesterIsAlive(req, harvesterID, data=None):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # get DN
    user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    # convert
    try:
        if data is not None:
            data = json.loads(data)
        else:
            data = dict()
    except Exception:
        return json.dumps((False, MESSAGE_JSON))
    # update
    return userIF.harvesterIsAlive(user, host, harvesterID, data)


# get stats of workers
def getWorkerStats(req):
    # get
    ret = userIF.getWorkerStats()
    return json.dumps(ret)


# report stat of workers
def reportWorkerStats(req, harvesterID, siteName, paramsList):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # update
    ret = userIF.reportWorkerStats(harvesterID, siteName, paramsList)
    return json.dumps(ret)


# report stat of workers
def reportWorkerStats_jobtype(req, harvesterID, siteName, paramsList):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # update
    ret = userIF.reportWorkerStats_jobtype(harvesterID, siteName, paramsList)
    return json.dumps(ret)


# send Harvester the command to clean up the workers for a panda queue
def sweepPQ(req, panda_queue, status_list, ce_list, submission_host_list):
    # check security
    if not isSecure(req):
        return json.dumps((False, MESSAGE_SSL))
    # check role
    prod_role = _has_production_role(req)
    if not prod_role:
        return json.dumps((False, MESSAGE_PROD_ROLE))

    return json.dumps((True, userIF.sweepPQ(panda_queue, status_list, ce_list, submission_host_list)))
