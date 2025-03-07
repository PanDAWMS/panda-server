import datetime
import os
import sys
import time
import traceback
from typing import List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.api.v1.common import (
    TimedMethod,
    generate_response,
    get_dn,
    has_production_role,
    request_validation,
)
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.adder_gen import AdderGen
from pandaserver.jobdispatcher import Protocol
from pandaserver.srvcore import CoreUtils
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("pilot_api")
pilot_logger = PandaLogger().getLogger("PilotRequests")


global_task_buffer = None
global_site_mapper_cache = None

VALID_JOB_STATES = ["running", "failed", "finished", "holding", "starting", "transferring"]


def init_task_buffer(task_buffer: TaskBuffer) -> None:
    """
    Initialize the task buffer. This method needs to be called before any other method in this module.
    """
    global global_task_buffer
    global_task_buffer = task_buffer

    global global_site_mapper_cache
    global_site_mapper_cache = CoreUtils.CachedObject("site_mapper", 60 * 10, get_site_mapper, _logger)


def get_site_mapper():
    return True, SiteMapper(global_task_buffer)


@request_validation(_logger, secure=True, request_method="POST")
def acquire_jobs(
    req: PandaRequest,
    site_name: str,
    timeout: int = 60,
    memory: int = None,
    disk_space: int = None,
    prod_source_label: str = None,
    node: str = None,
    computing_element: str = None,
    prod_user_id: str = None,
    get_proxy_key: str = None,
    task_id: int = None,
    n_jobs: int = None,
    background: bool = None,
    resource_type: str = None,
    harvester_id: str = None,
    worker_id: int = None,
    scheduler_id: str = None,
    job_type: str = None,
    via_topic: bool = None,
) -> dict:

    tmp_logger = LogWrapper(_logger, f"acquire_jobs {datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    tmp_logger.debug(f"Start for {site_name}")

    # get DN and FQANs
    real_dn = get_dn(req)

    # check production role
    # TODO: this is a bit tricky, there are multiple versions of checking for production role
    #       and the logic is not clear.
    is_production_manager = has_production_role(req)

    # production jobs should only be retrieved with production role
    if not is_production_manager and prod_source_label not in ["user"]:
        tmp_logger.warning("invalid role")
        tmp_msg = "no production/pilot role in VOMS FQANs or non pilot owner"
        return generate_response(False, message=tmp_msg)

    # set DN for non-production user, the user can only get their own jobs
    if not is_production_manager:
        prod_user_id = real_dn

    # allow get_proxy_key for production role
    if get_proxy_key == "True" and is_production_manager:
        get_proxy_key = True
    else:
        get_proxy_key = False

    # convert memory
    try:
        memory = max(0, memory)
    except (ValueError, TypeError):
        memory = 0

    # convert disk_space
    try:
        disk_space = max(0, disk_space)
    except (ValueError, TypeError):
        disk_space = 0

    tmp_logger.debug(
        f"{site_name}, n_jobs={n_jobs}, memory={memory}, disk={disk_space}, source_label={prod_source_label}, "
        f"node={node}, ce={computing_element}, user={prod_user_id}, proxy={get_proxy_key}, "
        f"task_id={task_id}, DN={real_dn}, role={is_production_manager}, "
        f"bg={background}, rt={resource_type}, harvester_id={harvester_id}, worker_id={worker_id}, "
        f"scheduler_id={scheduler_id}, job_type={job_type}, via_topic={via_topic}"
    )

    # log the acquire_jobs as it's used for site activity metrics
    for i in range(n_jobs):
        slot_suffix = f"_{i}" if n_jobs > 1 else ""
        pilot_logger.info(f"method=acquire_jobs, site={site_name}, node={node}{slot_suffix}, type={prod_source_label}")

    t_start = time.time()

    global_site_mapper_cache.update()
    is_grandly_unified = global_site_mapper_cache.cachedObj.getSite(site_name).is_grandly_unified()
    in_test_status = global_site_mapper_cache.cachedObj.getSite(site_name).status == "test"

    # change label
    if in_test_status and prod_source_label in ["user", "managed", "unified"]:
        new_label = "test"
        tmp_logger.debug(f"prod_source_label changed from {prod_source_label} to {new_label}")
        prod_source_label = new_label

    # wrapper function for timeout
    timed_method = TimedMethod(global_task_buffer.getJobs, timeout)
    timed_method.run(
        n_jobs,
        site_name,
        prod_source_label,
        memory,
        disk_space,
        node,
        timeout,
        computing_element,
        prod_user_id,
        task_id,
        background,
        resource_type,
        harvester_id,
        worker_id,
        scheduler_id,
        job_type,
        is_grandly_unified,
        via_topic,
    )

    jobs = []
    if isinstance(timed_method.result, list):
        result = timed_method.result
        secrets_map = result.pop()
        proxy_key = result[-1]
        n_sent = result[-2]
        jobs = result[:-2]

    # we retrieved jobs
    if len(jobs) > 0:

        # we will append each job as a response to the response list
        response_list = []

        for tmp_job in jobs:
            try:
                response = Protocol.Response(Protocol.SC_Success)
                response.appendJob(tmp_job, global_site_mapper_cache)
            except Exception as e:
                tmp_msg = f"failed to get jobs with {str(e)}"
                tmp_logger.error(f"{tmp_msg}\n{traceback.format_exc()}")
                raise

            # append n_sent
            # TODO: ask Tadashi/Paul why we need to send n_sent?
            response.appendNode("nSent", n_sent)

            # set proxy key
            if get_proxy_key:
                response.setProxyKey(proxy_key)

            # set user secrets
            if tmp_job.use_secrets() and tmp_job.prodUserName in secrets_map and secrets_map[tmp_job.prodUserName]:
                response.appendNode("secrets", secrets_map[tmp_job.prodUserName])

            # set pilot secrets
            pilot_secrets = secrets_map.get(panda_config.pilot_secrets, None)
            if pilot_secrets:
                response.appendNode("pilotSecrets", secrets_map[panda_config.pilot_secrets])

            response_list.append(response.data)

        # make response for bulk
        if n_jobs > 1:
            try:
                response = Protocol.Response(Protocol.SC_Success)
                response.appendNode("jobs", response_list)
            except Exception as e:
                tmp_msg = f"failed to make response with {str(e)}"
                tmp_logger.error(f"{tmp_msg}\n{traceback.format_exc()}")
                raise

    # we didn't retrieve jobs, either because of time out or because there were no jobs
    elif timed_method.result == Protocol.TimeOutToken:
        response = Protocol.Response(Protocol.SC_TimeOut, "database timeout")
    else:
        response = Protocol.Response(Protocol.SC_NoJobs, "no jobs in PanDA")
        pilot_logger.info(f"method=noJob, site={site_name}, node={node}, type={prod_source_label}")

    tmp_logger.debug(f"{site_name} {node} ret -> {response.encode(acceptJson=True)}")

    t_end = time.time()
    t_delta = t_end - t_start
    tmp_logger.info(f"site_name={site_name} took timing={t_delta}s in_test={in_test_status}")
    return response.encode(acceptJson=True)


@request_validation(_logger, secure=True, request_method="GET")
def get_job_status(req: PandaRequest, job_ids: str, timeout: int = 60) -> dict:
    """
    Get the status of a list of jobs

    Args:
        req(PandaRequest): Internally generated request object containing the environment variables.
        job_ids(str): space separated jobs
        timeout(int, optional): The timeout value. Defaults to 60.

    Returns:

    """
    tmp_logger = LogWrapper(_logger, f"get_job_status {job_ids}")

    # split the list of jobs
    job_ids = job_ids.split()

    # peek jobs
    timed_method = TimedMethod(global_task_buffer.peekJobs, timeout)
    timed_method.run(job_ids, fromDefined=False, fromActive=True, fromArchived=True, forAnal=False, use_json=False)

    # make response
    if timed_method.result == Protocol.TimeOutToken:
        response = Protocol.Response(Protocol.SC_TimeOut)
        tmp_logger.debug(f"ret -> {response.encode(acceptJson=True)}")
        return response.encode(acceptJson=True)

    if not isinstance(timed_method.result, list):
        response = Protocol.Response(Protocol.SC_Failed)
        tmp_logger.debug(f"ret -> {response.encode(acceptJson=True)}")
        return response.encode(acceptJson=True)

    response = Protocol.Response(Protocol.SC_Success)
    job_status_list = []
    for job, job_id_requested in zip(timed_method.result, job_ids):
        if not job:
            job_status_list.append({"job_id": job_id_requested, "status": "not found", "attempt_number": 0})
        else:
            job_status_list.append({"job_id": job.PandaID, "status": job.jobStatus, "attempt_number": job.attemptNr})

    response.appendNode("results", job_status_list)

    tmp_logger.debug(f"ret -> {response.encode(acceptJson=True)}")

    return response.encode(acceptJson=True)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_job(
    req: PandaRequest,
    job_id: int,
    job_status: str,
    trans_exit_code: str = None,
    pilot_error_code: str = None,
    pilot_error_diag: str = None,
    timeout: int = 60,
    job_output_report: str = "",
    node: str = None,
    cpu_consumption_time: int = None,
    cpu_consumption_unit: str = None,
    scheduler_id: str = None,
    pilot_id: str = None,
    site_name: str = None,
    pilot_log: str = "",
    meta_data: str = "",
    cpu_conversion_factor: float = None,
    exe_error_code: int = None,
    exe_error_diag: str = None,
    pilot_timing: str = None,
    start_time: str = None,
    end_time: str = None,
    n_events: int = None,
    n_input_files: int = None,
    batch_id: str = None,
    attempt_nr: int = None,
    job_metrics: str = None,
    stdout: str = "",
    job_sub_status: str = None,
    core_count: int = None,
    max_rss: int = None,
    max_vmem: int = None,
    max_swap: int = None,
    max_pss: int = None,
    avg_rss: int = None,
    avg_vmem: int = None,
    avg_swap: int = None,
    avg_pss: int = None,
    tot_rchar: int = None,
    tot_wchar: int = None,
    tot_rbytes: int = None,
    tot_wbytes: int = None,
    rate_rchar: int = None,
    rate_wchar: int = None,
    rate_rbytes: int = None,
    rate_wbytes: int = None,
    corrupted_files: str = None,
    mean_core_count: int = None,
    cpu_architecture_level: int = None,
):
    tmp_logger = LogWrapper(_logger, f"update_job PandaID={job_id} PID={os.getpid()}")
    tmp_logger.debug("Start")

    _logger.debug(
        f"update_job({job_id}, {job_status}, {trans_exit_code}, {pilot_error_code}, {pilot_error_diag}, {node},"
        f"cpu_consumption_time={cpu_consumption_time}, {cpu_consumption_unit}, {cpu_architecture_level}, "
        f"{scheduler_id}, {pilot_id}, {site_name}, {n_events}, {n_input_files}, {cpu_conversion_factor}, "
        f"{exe_error_code}, {exe_error_diag}, {pilot_timing}, {start_time}, {end_time}, {batch_id}, "
        f"attempt_nr:{attempt_nr}, job_sub_status:{job_sub_status}, core_count:{core_count}, "
        f"max_rss={max_rss}, max_vmem={max_vmem}, max_swap={max_swap}, "
        f"max_pss={max_pss}, avg_rss={avg_rss}, avg_vmem={avg_vmem}, avg_swap={avg_swap}, avg_pss={avg_pss}, "
        f"tot_rchar={tot_rchar}, tot_wchar={tot_wchar}, tot_rbytes={tot_rbytes}, tot_wbytes={tot_wbytes}, rate_rchar={rate_rchar}, "
        f"rate_wchar={rate_wchar}, rate_rbytes={rate_rbytes}, rate_wbytes={rate_wbytes}, mean_core_count={mean_core_count}, "
        f"corrupted_files={corrupted_files}\n==job_output_report==\n{job_output_report}\n==LOG==\n{pilot_log[:1024]}\n==Meta==\n{meta_data[:1024]}\n"
        f"==Metrics==\n{job_metrics}\n==stdout==\n{stdout})"
    )

    pilot_logger.debug(f"method=update_job, site={site_name}, node={node}, type=None")

    # aborting message
    if job_id == "NULL":
        return Protocol.Response(Protocol.SC_Success).encode(acceptJson=True)

    # check status
    if job_status not in VALID_JOB_STATES:
        tmp_logger.warning(f"invalid job_status={job_status} for updateJob")
        return Protocol.Response(Protocol.SC_Success).encode(acceptJson=True)

    # create the job parameter map
    param = {}
    fields = [
        ("cpuConsumptionTime", cpu_consumption_time, int),
        ("cpuConsumptionUnit", cpu_consumption_unit, str),
        ("cpu_architecture_level", cpu_architecture_level, lambda x: str(x)[:20]),
        ("modificationHost", node, lambda x: str(x)[:128]),
        ("transExitCode", trans_exit_code, int),
        ("pilotErrorCode", pilot_error_code, int),
        ("pilotErrorDiag", pilot_error_diag, lambda x: str(x)[:500]),
        ("jobMetrics", job_metrics, lambda x: str(x)[:500]),
        ("schedulerID", scheduler_id, str),
        ("pilotID", pilot_id, lambda x: str(x)[:200]),
        ("batchID", batch_id, lambda x: str(x)[:80]),
        ("exeErrorCode", exe_error_code, int),
        ("exeErrorDiag", exe_error_diag, lambda x: str(x)[:500]),
        ("cpuConversion", cpu_conversion_factor, float),
        ("pilotTiming", pilot_timing, str),
        ("nEvents", n_events, int),
        ("nInputFiles", n_input_files, int),
        ("jobSubStatus", job_sub_status, str),
        ("actualCoreCount", core_count, int),
        ("meanCoreCount", mean_core_count, float),
        ("maxRSS", max_rss, int),
        ("maxVMEM", max_vmem, int),
        ("maxSWAP", max_swap, int),
        ("maxPSS", max_pss, int),
        ("avgRSS", avg_rss, lambda x: int(float(x))),
        ("avgVMEM", avg_vmem, lambda x: int(float(x))),
        ("avgSWAP", avg_swap, lambda x: int(float(x))),
        ("avgPSS", avg_pss, lambda x: int(float(x))),
        ("corruptedFiles", corrupted_files, str),
    ]

    # Iterate through fields, apply transformations and add to `param`
    for key, value, cast in fields:
        if value not in [None, ""]:
            try:
                param[key] = cast(value)
            except Exception:
                tmp_logger.error(f"Invalid {key}={value} for updateJob")

    # Special handling for file size metrics
    file_metrics = [
        ("totRCHAR", tot_rchar),
        ("totWCHAR", tot_wchar),
        ("totRBYTES", tot_rbytes),
        ("totWBYTES", tot_wbytes),
        ("rateRCHAR", rate_rchar),
        ("rateWCHAR", rate_wchar),
        ("rateRBYTES", rate_rbytes),
        ("rateWBYTES", rate_wbytes),
    ]

    for key, value in file_metrics:
        if value is not None:
            try:
                value = int(value) / 1024  # Convert to kB
                param[key] = min(10**10 - 1, value)  # Limit to 10 digits
            except Exception:
                tmp_logger.error(f"Invalid {key}={value} for updateJob")

    # Convert timestamps
    for key, value in [("startTime", start_time), ("endTime", end_time)]:
        if value is not None:
            try:
                param[key] = datetime.datetime(*time.strptime(value, "%Y-%m-%d %H:%M:%S")[:6])
            except Exception:
                tmp_logger.error(f"Invalid {key}={value} for updateJob")

    # Handle attempt_nr separately
    if attempt_nr is not None:
        try:
            attempt_nr = int(attempt_nr)
        except Exception:
            attempt_nr = None

    tmp_logger.debug("executing")

    # store the pilot log
    if pilot_log != "":
        tmp_logger.debug("saving pilot log")
        try:
            global_task_buffer.storePilotLog(int(job_id), pilot_log)
            tmp_logger.debug("saving pilot log DONE")
        except Exception:
            tmp_logger.debug("saving pilot log FAILED")

    # add meta_data
    if meta_data != "":
        ret = global_task_buffer.addMetadata([job_id], [meta_data], [job_status])
        if len(ret) > 0 and not ret[0]:
            tmp_logger.debug(f"failed to add meta_data")
            response = Protocol.Response(Protocol.SC_Success)
            return response.encode(acceptJson=True)

    # add stdout
    if stdout != "":
        global_task_buffer.addStdOut(job_id, stdout)

    # update the job
    tmp_status = job_status
    update_state_change = False
    if job_status in ("failed", "finished"):
        tmp_status = "holding"
        update_state_change = True  # update stateChangeTime to prevent Watcher from finding this job
        param["jobDispatcherErrorDiag"] = None
    elif job_status in ("holding", "transferring"):
        param["jobDispatcherErrorDiag"] = (
            f"set to {job_status} by the pilot at {datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')}"
        )

    # update the job status in the database
    timeout = None if job_status == "holding" else timeout
    timed_method = TimedMethod(global_task_buffer.updateJobStatus, timeout)
    timed_method.run(job_id, tmp_status, param, update_state_change, attempt_nr)

    # time-out
    if timed_method.result == Protocol.TimeOutToken:
        response = Protocol.Response(Protocol.SC_TimeOut)
        tmp_logger.debug(f"ret -> {response.encode(acceptJson=True)}")
        return response.encode(acceptJson=True)

    # no result
    if not timed_method.result:
        response = Protocol.Response(Protocol.SC_Failed)
        tmp_logger.debug(f"ret -> {response.encode(acceptJson=True)}")
        return response.encode(acceptJson=True)

    # generate the response with the result
    response = Protocol.Response(Protocol.SC_Success)
    result = timed_method.result

    # set the secrets
    secrets = result.get("secrets") if isinstance(result, dict) else None
    if secrets:
        response.appendNode("pilotSecrets", secrets)

    # set the command to the pilot
    command = result.get("command") if isinstance(result, dict) else result
    response.appendNode("command", command if isinstance(command, str) else "NULL")

    # add output to dataset for failed/finished jobs with correct results
    if job_status in ("failed", "finished") and result not in ("badattemptnr", "alreadydone"):
        adder_gen = AdderGen(global_task_buffer, job_id, job_status, attempt_nr)
        adder_gen.dump_file_report(job_output_report, attempt_nr)
        del adder_gen

    tmp_logger.debug(f"ret -> {response.encode(acceptJson=True)}")
    return response.encode(acceptJson=True)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_jobs_bulk(req, job_list: List, harvester_id: str = None):
    tmp_logger = LogWrapper(_logger, f"update_jobs_bulk harvester_id={harvester_id}")
    tmp_logger.debug("Start")
    t_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    success = False
    message = ""
    data = []
    try:
        for job_dict in job_list:
            job_id = job_dict["jobId"]
            del job_dict["jobId"]
            status = job_dict["state"]
            del job_dict["state"]
            if "metaData" in job_dict:
                job_dict["metaData"] = str(job_dict["metaData"])
            tmp_ret = update_job(req, job_id, status, **job_dict)
            data.append(tmp_ret)
        success = True
    except Exception:
        err_type, err_value = sys.exc_info()[:2]
        message = f"failed with {err_type.__name__} {err_value}"
        data = []
        tmp_logger.error(f"{message}\n{traceback.format_exc()}")

    t_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - t_start
    tmp_logger.debug(f"Done. Took {t_delta.seconds}.{t_delta.microseconds // 1000:03d} sec")
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_worker_status(req: PandaRequest, worker_id, harvester_id, status, timeout=60, node_id=None):
    """
    Update the status of a worker according to the pilot.

    This function validates the pilot permissions and the state passed by the pilot, then updates the worker status.

    API details:
        HTTP Method: POST
        Path: /pilot/v1/update_worker_status

    Args:
        req(PandaRequest): Internally generated request object containing the environment variables.
        worker_id (str): The worker ID.
        harvester_id (str): The harvester ID.
        status (str): The status of the worker. Must be either 'started' or 'finished'.
        timeout (int, optional): The timeout value. Defaults to 60.
        node_id (str, optional): The node ID. Defaults to None.

    Returns:
        str: The result of the status update or an error message.
    """
    tmp_logger = LogWrapper(
        _logger,
        f"updateWorkerPilotStatus worker_id={worker_id} harvester_id={harvester_id} status={status} node_id={node_id} PID={os.getpid()}",
    )
    tmp_logger.debug("Start")

    # validate the state passed by the pilot
    valid_worker_states = ("started", "finished")
    if status not in valid_worker_states:
        message = f"Invalid worker state. The worker state has to be in {valid_worker_states}"
        tmp_logger.debug(message)
        return generate_response(False, message)

    timed_method = TimedMethod(global_task_buffer.updateWorkerPilotStatus, timeout)
    timed_method.run(worker_id, harvester_id, status, node_id)

    # generate the response
    if not timed_method.result:  # failure
        message = "Failed to update worker status"
        tmp_logger.error(message)
        return generate_response(False, message)

    if timed_method.result == Protocol.TimeOutToken:  # timeout
        message = "Updating worker status timed out"
        tmp_logger.error(message)
        return generate_response(False, message)

    tmp_logger.debug(f"Done")
    return generate_response(True)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_worker_node(
    req: PandaRequest,
    site: str,
    host_name: str,
    cpu_model: str,
    n_logical_cpus: int,
    n_sockets: int,
    cores_per_socket: int,
    threads_per_core: int,
    cpu_architecture: str,
    cpu_architecture_level: str,
    clock_speed: float,
    total_memory: int,
    timeout: int = 60,
):
    """
    Update worker node

    Updates a worker node in the worker node map. When already found, it updates the `last_seen` time. When not found, it adds the worker node.

    API details:
        HTTP Method: POST
        Path: /pilot/v1/update_worker_node

    Args:
        req(PandaRequest): Internally generated request object containing the environment variables.
        site(str): Site name (e.g. ATLAS site name, not PanDA queue).
        host_name(str): Host name. In the case of reporting in format `slot@worker_node.example.com`, the slot ID will be parsed out.
        cpu_model(str): CPU model, e.g. `AMD EPYC 7351`
        n_logical_cpus(int): Number of logical CPUs: n_sockets * cores_per_socket * threads_per_core.
                             When SMT is enabled, this is the number of threads. Otherwise it is the number of cores.
        n_sockets(int): Number of sockets.
        cores_per_socket(int): Number of cores per socket.
        threads_per_core(int): Number of threads per core. When SMT is disabled, this is 1. Otherwise a number > 1.
        cpu_architecture(str): CPU architecture, e.g. `x86_64`
        cpu_architecture_level(str): CPU architecture level, e.g. `x86-64-v3`
        clock_speed(float): Clock speed in GHz.
        total_memory(int): Total memory in MB.
        timeout: int = 60: The timeout value. Defaults to 60.

    Returns:
        dict: The system response  `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message.
    """
    tmp_logger = LogWrapper(_logger, f"update_worker_node site={site} host_name={host_name} cpu_model={cpu_model}")
    tmp_logger.debug("Start")

    timed_method = TimedMethod(global_task_buffer.update_worker_node, timeout)
    timed_method.run(
        site,
        host_name,
        cpu_model,
        n_logical_cpus,
        n_sockets,
        cores_per_socket,
        threads_per_core,
        cpu_architecture,
        cpu_architecture_level,
        clock_speed,
        total_memory,
    )

    if timed_method.result == Protocol.TimeOutToken:  # timeout
        message = "Updating worker node timed out"
        tmp_logger.error(message)
        return generate_response(False, message)

    success, message = timed_method.result

    tmp_logger.debug(f"Done")
    return generate_response(success, message)
