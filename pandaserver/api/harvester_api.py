"""
provide web interface to users

"""

import datetime
import json

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.common_api import (
    MESSAGE_DATABASE,
    MESSAGE_JSON,
    get_dn,
    json_loader,
    require_production_role,
    require_secure,
)
from pandaserver.userinterface.UserIF import MESSAGE_JSON

_logger = PandaLogger().getLogger("harvester_api")


class HarvesterAPI:
    def __init__(self):
        self.task_buffer = None

    # initialize
    def init(self, task_buffer):
        self.task_buffer = task_buffer

    # update workers
    def update_workers(self, harvester_id, workers):
        ret = self.task_buffer.updateWorkers(harvester_id, workers)
        if not ret:
            return json.dumps((False, MESSAGE_DATABASE))

        return json.dumps((True, ret))

    # update workers
    def update_harvester_service_metrics(self, harvester_id, data):
        ret = self.task_buffer.updateServiceMetrics(harvester_id, data)
        if not ret:
            return json.dumps((False, MESSAGE_DATABASE))

        return json.dumps((True, ret))

    # add harvester dialog messages
    def add_harvester_dialogs(self, harvester_id, dialogs):
        ret = self.task_buffer.addHarvesterDialogs(harvester_id, dialogs)
        if not ret:
            return json.dumps((False, MESSAGE_DATABASE))

        return json.dumps((True, ""))

    # heartbeat for harvester
    def harvester_heartbeat(self, user, host, harvester_id, data):
        ret = self.task_buffer.harvesterIsAlive(user, host, harvester_id, data)
        if not ret:
            return json.dumps((False, MESSAGE_DATABASE))

        return json.dumps((True, ret))

    # get stats of workers
    def get_worker_statistics(self):
        return self.task_buffer.getWorkerStats()

    # report stat of workers
    def report_worker_statistics(self, harvester_id, site_name, parameter_list):
        return self.task_buffer.reportWorkerStats_jobtype(harvester_id, site_name, parameter_list)

    # sweep panda queue
    def sweep_panda_queue(self, panda_queue, status_list, ce_list, submission_host_list):
        try:
            panda_queue_des = json.loads(panda_queue)
            status_list_des = json.loads(status_list)
            ce_list_des = json.loads(ce_list)
            submission_host_list_des = json.loads(submission_host_list)
        except Exception:
            _logger.error("Problem deserializing variables")
            return json.dumps((False, MESSAGE_JSON))

        ret = self.task_buffer.sweepPQ(panda_queue_des, status_list_des, ce_list_des, submission_host_list_des)
        return json.dumps(ret)


# Singleton
harvester_api = HarvesterAPI()
del harvester_api


# update workers
@require_secure(_logger)
@json_loader("workers", logger=_logger)
def update_workers(req, harvester_id, workers):
    time_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    return_value = harvester_api.update_workers(harvester_id, workers)
    time_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - time_start
    _logger.debug(f"update_workers {harvester_id} took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return return_value


# update workers
@require_secure(_logger)
@json_loader("metrics", logger=_logger)
def update_harvester_service_metrics(req, harvester_id, metrics):
    time_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    return_value = harvester_api.update_harvester_service_metrics(harvester_id, metrics)
    time_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - time_start
    _logger.debug(f"update_harvester_service_metrics {harvester_id} took {time_delta.seconds}.{time_delta.microseconds // 1000:03d} sec")

    return return_value


# add harvester dialog messages
@require_secure(_logger)
@json_loader("dialogs", logger=_logger)
def add_harvester_dialogs(req, harvester_id, dialogs):
    return harvester_api.add_harvester_dialogs(harvester_id, dialogs)


# heartbeat for harvester
@require_secure(_logger)
@json_loader("data", default_value={}, logger=_logger)
def harvester_heartbeat(req, harvester_id, data=None):
    # get user and hostname to record in harvester metadata
    user = get_dn(req)
    host = req.get_remote_host()

    return harvester_api.harvester_heartbeat(user, host, harvester_id, data)


# get stats of workers
def get_worker_statistics(req):
    ret = harvester_api.get_worker_statistics()
    return json.dumps(ret)


# report stat of workers
@require_secure(_logger)
def report_worker_statistics(req, harvester_id, site_name, parameter_list):
    ret = harvester_api.report_worker_statistics(harvester_id, site_name, parameter_list)
    return json.dumps(ret)


# send Harvester the command to clean up the workers for a panda queue
@require_secure(_logger)
@require_production_role
def sweep_panda_queue(req, panda_queue, status_list, ce_list, submission_host_list):
    return json.dumps((True, harvester_api.sweep_panda_queue(panda_queue, status_list, ce_list, submission_host_list)))
