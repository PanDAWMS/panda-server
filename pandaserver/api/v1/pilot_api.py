import datetime
import os
import sys
import time
import traceback
from typing import List

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

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
from pandaserver.srvcore.CoreUtils import normalize_cpu_model
from pandaserver.srvcore.panda_request import PandaRequest
from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

_logger = PandaLogger().getLogger("api_pilot")
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
    global_site_mapper_cache = CoreUtils.CachedObject("site_mapper", 60 * 10, _get_site_mapper, _logger)


def _get_site_mapper():
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
    remaining_time=None,
) -> dict:
    """
    Acquire jobs

    Acquire jobs for the pilot. The jobs are reserved, the job status is updated and the jobs are returned. Requires a secure connection.

    API details:
        HTTP Method: POST
        Path: /v1/pilot/acquire_jobs

    Args:
        req(PandaRequest): Internally generated request object containing the environment variables.
        site_name(str): The PanDA queue name
        timeout(int, optional): Request timeout in seconds. Optional and defaults to 60.
        memory(int, optional): Memory limit for the job. Optional and defaults to `None`.
        disk_space(int, optional): Disk space limit for the job. Optional and defaults to `None`.
        prod_source_label(str, optional): Prodsourcelabel, e.g. `user`, `managed`, `unified`... Optional and defaults to `None`.
        node(str, optional): Identifier of the worker node/slot. Optional and defaults to `None`.
        computing_element(str, optional): Computing element. Optional and defaults to `None`.
        prod_user_id(str, optional): User ID of the job. Optional and defaults to `None`.
        get_proxy_key(bool, optional): Flag to request a proxy key.Optional and defaults to `None`.
        task_id(int, optional): JEDI task ID of the job. Optional and defaults to `None`.
        n_jobs(int, optional): Number of jobs for bulk requests. Optional and defaults to `None`.
        background(bool, optional): Background flag. Optional and defaults to `None`.
        resource_type(str, optional): Resource type of the job, e.g. `SCORE`, `MCORE`,.... Optional and defaults to `None`.
        harvester_id(str, optional): Harvester ID, used to update the worker entry in the DB. Optional and defaults to `None`.
        worker_id(int, optional): Worker ID, used to update the worker entry in the DB. Optional and defaults to `None`.
        scheduler_id(str, optional): Scheduler, e.g. harvester ID. Optional and defaults to `None`.
        job_type(str, optional): Job type, e.g. `user`, `unified`, ... This is necessary on top of the `prodsourcelabel`
                                 to disambiguate the cases of test jobs that can be production or analysis. Optional and defaults to `None`.
        via_topic(bool, optional): Topic for message broker. Optional and defaults to `None`.
        remaining_time(int, optional): Remaining walltime. Optional and defaults to `None`.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. The data is a list of job dictionaries.
              When failed, the message contains the error message.
    """

    tmp_logger = LogWrapper(_logger, f"acquire_jobs {naive_utcnow().isoformat('/')}")
    tmp_logger.debug(f"Start for {site_name}")

    # get DN and FQANs
    real_dn = get_dn(req)

    # check production role
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
    if get_proxy_key and is_production_manager:
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

    # convert remaining time
    try:
        remaining_time = max(0, remaining_time)
    except (ValueError, TypeError):
        remaining_time = 0

    tmp_logger.debug(
        f"{site_name}, n_jobs={n_jobs}, memory={memory}, disk={disk_space}, source_label={prod_source_label}, "
        f"node={node}, ce={computing_element}, user={prod_user_id}, proxy={get_proxy_key}, "
        f"task_id={task_id}, DN={real_dn}, role={is_production_manager}, "
        f"bg={background}, rt={resource_type}, harvester_id={harvester_id}, worker_id={worker_id}, "
        f"scheduler_id={scheduler_id}, job_type={job_type}, via_topic={via_topic} remaining_time={remaining_time}"
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
        remaining_time,
    )

    # Time-out
    if timed_method.result == Protocol.TimeOutToken:
        message = "Timed out"
        tmp_logger.debug(message)
        return generate_response(False, message=message)

    # Try to get the jobs
    jobs = []
    if isinstance(timed_method.result, list):
        result = timed_method.result
        secrets_map = result.pop()
        proxy_key = result[-1]
        n_sent = result[-2]
        jobs = result[:-2]

    if not jobs:
        message = "No jobs in PanDA"
        tmp_logger.debug(message)
        pilot_logger.info(f"method=noJob, site={site_name}, node={node}, type={prod_source_label}")
        return generate_response(False, message=message)

    # add each job to the list
    response_list = []
    for tmp_job in jobs:
        try:
            # The response is nothing but a dictionary with the job information
            response = Protocol.Response(Protocol.SC_Success)
            response.appendJob(tmp_job, global_site_mapper_cache)
        except Exception as e:
            tmp_msg = f"failed to get jobs with {str(e)}"
            tmp_logger.error(f"{tmp_msg}\n{traceback.format_exc()}")
            raise

        # append n_sent
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

    # if the jobs were requested in bulk, we make a bulk response
    if n_jobs is not None:
        try:
            response = Protocol.Response(Protocol.SC_Success)
            response.appendNode("jobs", response_list)
        except Exception as e:
            tmp_msg = f"Failed to make response with {str(e)}"
            tmp_logger.error(f"{tmp_msg}\n{traceback.format_exc()}")
            raise

    tmp_logger.debug(f"Done for {site_name} {node}")

    t_end = time.time()
    t_delta = t_end - t_start
    tmp_logger.info(f"site_name={site_name} took timing={t_delta}s in_test={in_test_status}")
    return generate_response(True, data=response.data)


@request_validation(_logger, secure=True, request_method="GET")
def get_job_status(req: PandaRequest, job_ids: List[int], timeout: int = 60) -> dict:
    """
    Get job status

    Gets the status for a list of jobs. Requires a secure connection.

    API details:
        HTTP Method: GET
        Path: /v1/pilot/get_job_status

    Args:
        req(PandaRequest): Internally generated request object containing the environment variables.
        job_ids(list): list of job IDs.
        timeout(int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. The data is a list of job status dictionaries, in the format
            ```
            [{"job_id": <job_id_requested>, "status": "not found", "attempt_number": 0}), {"job_id": <job_id>, "status": <status>, "attempt_number": <attempt_nr>}]
            ```
    """

    tmp_logger = LogWrapper(_logger, f"get_job_status {job_ids}")
    tmp_logger.debug("Start")

    # peek jobs
    timed_method = TimedMethod(global_task_buffer.peekJobs, timeout)
    timed_method.run(job_ids, fromDefined=False, fromActive=True, fromArchived=True, forAnal=False, use_json=False)

    # make response
    if timed_method.result == Protocol.TimeOutToken:
        message = "Timed out"
        tmp_logger.debug(message)
        return generate_response(False, message=message)

    if not isinstance(timed_method.result, list):
        message = "Database error"
        tmp_logger.debug(message)
        return generate_response(False, message=message)

    job_status_list = []
    for job, job_id_requested in zip(timed_method.result, job_ids):
        if not job:
            job_status_list.append({"job_id": job_id_requested, "status": "not found", "attempt_number": 0})
        else:
            job_status_list.append({"job_id": job.PandaID, "status": job.jobStatus, "attempt_number": job.attemptNr})

    tmp_logger.debug(f"Done: job_status_list={job_status_list}")

    return generate_response(True, data=job_status_list)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_job(
    req: PandaRequest,
    job_id: int,
    job_status: str,
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
    trans_exit_code: int = None,
    pilot_error_code: int = None,
    pilot_error_diag: str = None,
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
    max_rss: int | float = None,
    max_vmem: int | float = None,
    max_swap: int | float = None,
    max_pss: int | float = None,
    avg_rss: int | float = None,
    avg_vmem: int | float = None,
    avg_swap: int | float = None,
    avg_pss: int | float = None,
    tot_rchar: int | float = None,
    tot_wchar: int | float = None,
    tot_rbytes: int | float = None,
    tot_wbytes: int | float = None,
    rate_rchar: int | float = None,
    rate_wchar: int | float = None,
    rate_rbytes: int | float = None,
    rate_wbytes: int | float = None,
    corrupted_files: str = None,
    mean_core_count: int = None,
    cpu_architecture_level: str = None,
    grid: str = None,
    source_site: str = None,
    destination_site: str = None,
    timeout: int = 60,
):
    """
    Update job

    Updates the details for a job, stores the metadata and excerpts from the pilot log. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/pilot/update_job

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_id (int): PanDA job ID
        job_status(str, optional): Job status
        job_sub_status(str, optional): Job sub status. Optional, defaults to `None`
        start_time(str, optional): Job start time in format `"%Y-%m-%d %H:%M:%S"`. Optional, defaults to `None`
        end_time(str, optional): Job end time in format `"%Y-%m-%d %H:%M:%S"`. Optional, defaults to `None`
        pilot_timing(str, optional): String with pilot timings. Optional, defaults to `None`
        site_name(str, optional): PanDA queue name. Optional, defaults to `None`
        node(str, optional): Identifier for worker node/slot. Optional, defaults to `None`
        scheduler_id(str, optional): Scheduler ID, such as harvester instance. Optional, defaults to `None`
        pilot_id(str, optional): Pilot ID. Optional, defaults to `None`
        batch_id(str, optional): Batch ID. Optional, defaults to `None`
        trans_exit_code(int, optional): Transformation exit code. Optional, defaults to `None`
        pilot_error_code(int, optional): Pilot error code. Optional, defaults to `None`
        pilot_error_diag(str, optional): Pilot error message. Optional, defaults to `None`
        exe_error_code(int, optional): Execution error code. Optional, defaults to `None`
        exe_error_diag(str, optional): Execution error message. Optional, defaults to `None`
        n_events(int, optional): Number of events. Optional, defaults to `None`
        n_input_files(int, optional): Number of input files. Optional, defaults to `None`
        attempt_nr(int, optional): Job attempt number. Optional, defaults to `None`
        cpu_consumption_time(int, optional): CPU consumption time. Optional, defaults to `None`
        cpu_consumption_unit(str, optional): CPU consumption unit, being used for updating some CPU details. Optional, defaults to `None`
        cpu_conversion_factor(float, optional): CPU conversion factor. Optional defaults to `None`
        core_count(int, optional): Number of cores of the job. Optional, defaults to `None`
        mean_core_count(int, optional): Mean core count. Optional, defaults to `None`
        max_rss(int, optional): Measured max RSS memory. Optional, defaults to `None`
        max_vmem(int, optional): Measured max Virtual memory. Optional, defaults to `None`
        max_swap(int, optional): Measured max swap memory. Optional, defaults to `None`
        max_pss(int, optional): Measured max PSS memory. Optional, defaults to `None`
        avg_rss(int, optional): Measured average RSS. Optional, defaults to `None`
        avg_vmem(int, optional): Measured average Virtual memory.Optional, defaults to `None`
        avg_swap(int, optional): Measured average swap memory. Optional, defaults to `None`
        avg_pss(int, optional): Measured average PSS. Optional, defaults to `None`
        tot_rchar(int, optional): Measured total read characters. Optional, defaults to `None`
        tot_wchar(int, optional): Measured total written characters. Optional, defaults to `None`
        tot_rbytes(int, optional): Measured total read bytes. Optional, defaults to `None`
        tot_wbytes(int, optional): Measured total written bytes. Optional, defaults to `None`
        rate_rchar(int, optional): Measured rate for read characters. Optional, defaults to `None`
        rate_wchar(int, optional): Measured rate for written characters. Optional, defaults to `None`
        rate_rbytes(int, optional): Measured rate for read bytes. Optional, defaults to `None`
        rate_wbytes(int, optional): Measured rate for written bytes. Optional, defaults to `None`
        corrupted_files(str, optional): List of corrupted files in comma separated format. Optional, defaults to `None`
        cpu_architecture_level(str, optional): CPU architecture level (e.g. `x86_64-v3`). Optional, defaults to `None`
        grid(str, optional): Grid type. Optional, defaults to `None`
        source_site(str, optional): Source site name. Optional, defaults to `None`
        destination_site(str, optional): Destination site name. Optional, defaults to `None`
        job_metrics(str, optional): Job metrics. Optional, defaults to `None`
        job_output_report(str, optional): Job output report. Optional, defaults to `""`
        pilot_log(str, optional): Pilot log excerpt. Optional, defaults to `""`
        meta_data(str, optional): Job metadata. Optional, defaults to `""`
        stdout(str, optional): Standard output. Optional, defaults to `""`
        timeout(int, optional): Timeout for the operation in seconds. Optional, defaults to 60

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. Data will contain a dictionary with the pilot secrets and the command to the pilot.
              ```
                {"pilotSecrets": <pilot_secrets>, "command": <command>}
              ```
    """
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
        f"grid={grid}, source_site={source_site}, destination_site={destination_site}, "
        f"corrupted_files={corrupted_files}\n==job_output_report==\n{job_output_report}\n==LOG==\n{pilot_log[:1024]}\n==Meta==\n{meta_data[:1024]}\n"
        f"==Metrics==\n{job_metrics}\n==stdout==\n{stdout})"
    )

    pilot_logger.debug(f"method=update_job, site={site_name}, node={node}, type=None")

    # aborting message
    if job_id == "NULL":
        response = Protocol.Response(Protocol.SC_Invalid)
        return generate_response(success=False, message="job_id is NULL", data=response.data)

    # check the job status is valid
    if job_status not in VALID_JOB_STATES:
        message = f"Invalid job status: {job_status}"
        tmp_logger.warning(message)
        response = Protocol.Response(Protocol.SC_Invalid)
        return generate_response(success=False, message=message, data=response.data)

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
        ("grid", grid, str),
        ("sourceSite", source_site, str),
        ("destinationSite", destination_site, str),
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
        tmp_logger.debug("Saving pilot log")
        try:
            global_task_buffer.storePilotLog(int(job_id), pilot_log)
            tmp_logger.debug("Saving pilot log DONE")
        except Exception:
            tmp_logger.debug("Saving pilot log FAILED")

    # add meta_data
    if meta_data != "":
        ret = global_task_buffer.addMetadata([job_id], [meta_data], [job_status])
        if len(ret) > 0 and not ret[0]:
            message = "Failed to add meta_data"
            tmp_logger.debug(message)
            return generate_response(True, message, data={"StatusCode": Protocol.SC_Failed, "ErrorDiag": message})

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
        param["jobDispatcherErrorDiag"] = f"set to {job_status} by the pilot at {naive_utcnow().strftime('%Y-%m-%d %H:%M:%S')}"

    # update the job status in the database
    timeout = None if job_status == "holding" else timeout
    timed_method = TimedMethod(global_task_buffer.updateJobStatus, timeout)
    timed_method.run(job_id, tmp_status, param, update_state_change, attempt_nr)

    # time-out
    if timed_method.result == Protocol.TimeOutToken:
        message = "Timed out"
        tmp_logger.error(message)
        response = Protocol.Response(Protocol.SC_TimeOut)
        return generate_response(True, message=message, data=response.data)

    # no result
    if not timed_method.result:
        message = "Database error"
        tmp_logger.error(message)
        response = Protocol.Response(Protocol.SC_Failed)
        return generate_response(True, message=message, data=response.data)

    # generate the response with the result
    data = {"StatusCode": Protocol.SC_Success}
    result = timed_method.result

    # set the secrets
    secrets = result.get("secrets") if isinstance(result, dict) else None
    if secrets:
        data["pilotSecrets"] = secrets

    # set the command to the pilot
    command = result.get("command") if isinstance(result, dict) else result
    data["command"] = command if isinstance(command, str) else None

    # add output to dataset for failed/finished jobs with correct results
    if job_status in ("failed", "finished") and result not in ("badattemptnr", "alreadydone"):
        adder_gen = AdderGen(global_task_buffer, job_id, job_status, attempt_nr)
        adder_gen.dump_file_report(job_output_report, attempt_nr)
        del adder_gen

    tmp_logger.debug(f"Done. data={data}")
    return generate_response(True, data=data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_jobs_bulk(req, job_list: List, harvester_id: str = None):
    """
    Update jobs in bulk

    Bulk method to update the details for jobs, store the metadata and excerpt from the pilot log. Internally, this method loops over
    the jobs and calls `update_job` for each job. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/pilot/update_jobs_bulk

    Args:
        req(PandaRequest): internally generated request object containing the env variables
        job_list(list): list of job dictionaries to update. The mandatory and optional keys for each job dictionary are the same as the arguments for `update_job`.
        harvester_id (str, optional): Harvester ID. Optional, defaults to `None`.

    Returns:
        dict: The system response `{"success": success, "message": message, "data": data}`. Data will contain a dictionary with the pilot secrets and the command to the pilot.
              ```
                {"pilotSecrets": <pilot_secrets>, "command": <command>}
              ```
    """
    tmp_logger = LogWrapper(_logger, f"update_jobs_bulk harvester_id={harvester_id}")
    tmp_logger.debug("Start")
    t_start = naive_utcnow()

    success = False
    message = ""
    data = []

    try:
        for job_dict in job_list:
            job_id = job_dict["job_id"]
            del job_dict["job_id"]

            status = job_dict["job_status"]
            del job_dict["job_status"]

            if "meta_data" in job_dict:
                job_dict["meta_data"] = str(job_dict["meta_data"])

            tmp_ret = update_job(req, job_id, status, **job_dict)
            data.append(tmp_ret)
        success = True
    except Exception:
        err_type, err_value = sys.exc_info()[:2]
        message = f"failed with {err_type.__name__} {err_value}"
        data = []
        tmp_logger.error(f"{message}\n{traceback.format_exc()}")

    t_delta = naive_utcnow() - t_start
    tmp_logger.debug(f"Done. Took {t_delta.seconds}.{t_delta.microseconds // 1000:03d} sec")
    return generate_response(success, message, data)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_worker_status(req: PandaRequest, worker_id, harvester_id, status, timeout=60, node_id=None):
    """
    Update worker status

    Updates the status of a worker with the information seen by the pilot. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/pilot/update_worker_status

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
    panda_queue: str = None,
    n_logical_cpus: int = None,
    n_sockets: int = None,
    cores_per_socket: int = None,
    threads_per_core: int = None,
    cpu_architecture: str = None,
    cpu_architecture_level: str = None,
    clock_speed: float = None,
    total_memory: int = None,
    total_local_disk: int = None,
    timeout: int = 60,
):
    """
    Update worker node

    Updates a worker node in the worker node map. When already found, it updates the `last_seen` time. When not found, it adds the worker node. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/pilot/update_worker_node

    Args:
        req(PandaRequest): Internally generated request object containing the environment variables.
        site(str): Site name (e.g. ATLAS site name, not PanDA queue).
        host_name(str): Host name. In the case of reporting in format `slot@worker_node.example.com`, the slot ID will be parsed out.
        cpu_model(str): CPU model, e.g. `AMD EPYC 7351`.
        panda_queue(str, optional): PanDA queue the worker node is associated to. Optional, defaults to `None`.
        n_logical_cpus(int, optional): Number of logical CPUs: n_sockets * cores_per_socket * threads_per_core.
                             When SMT is enabled, this is the number of threads. Otherwise it is the number of cores. Optional, defaults to `None`.
        n_sockets(int, optional): Number of sockets. Optional, defaults to `None`.
        cores_per_socket(int, optional): Number of cores per socket. Optional, defaults to `None`.
        threads_per_core(int, optional): Number of threads per core. When SMT is disabled, this is 1. Otherwise a number > 1. Optional, defaults to `None`.
        cpu_architecture(str, optional): CPU architecture, e.g. `x86_64`. Optional, defaults to `None`.
        cpu_architecture_level(str, optional): CPU architecture level, e.g. `x86-64-v3`. Optional, defaults to `None`.
        clock_speed(float, optional): Clock speed in MHz. Optional, defaults to `None`.
        total_memory(int, optional): Total memory in MB. Optional, defaults to `None`.
        total_local_disk(int, optional): Total disk space in GB. Optional, defaults to `None`.
        timeout(int, optional): The timeout value. Defaults to 60.

    Returns:
        dict: The system response  `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message.
    """
    tmp_logger = LogWrapper(_logger, f"update_worker_node site={site} panda_queue={panda_queue} host_name={host_name} cpu_model={cpu_model}")
    tmp_logger.debug("Start")

    cpu_model_normalized = normalize_cpu_model(cpu_model)

    timed_method = TimedMethod(global_task_buffer.update_worker_node, timeout)
    timed_method.run(
        site,
        panda_queue,
        host_name,
        cpu_model,
        cpu_model_normalized,
        n_logical_cpus,
        n_sockets,
        cores_per_socket,
        threads_per_core,
        cpu_architecture,
        cpu_architecture_level,
        clock_speed,
        total_memory,
        total_local_disk,
    )

    if timed_method.result == Protocol.TimeOutToken:  # timeout
        message = "Updating worker node timed out"
        tmp_logger.error(message)
        return generate_response(False, message)

    success, message = timed_method.result

    tmp_logger.debug(f"Done")
    return generate_response(success, message)


@request_validation(_logger, secure=True, production=True, request_method="POST")
def update_worker_node_gpu(
    req: PandaRequest,
    site: str,
    host_name: str,
    vendor: str,
    model: str,
    count: int,
    vram: int = None,
    architecture: str = None,
    framework: str = None,
    framework_version: str = None,
    driver_version: str = None,
    timeout: int = 60,
):
    """
    Update GPUs for a worker node

    Updates the GPUs associated to a worker node in the worker node map. When already found, it updates the `last_seen` time. Requires a secure connection and production role.

    API details:
        HTTP Method: POST
        Path: /v1/pilot/update_worker_node_gpu

    Args:
        req(PandaRequest): Internally generated request object containing the environment variables.
        site(str): Site name (e.g. ATLAS site name, not PanDA queue).
        host_name(str): Host name. In the case of reporting in format `slot@worker_node.example.com`, the slot ID will be parsed out.
        vendor(str): GPU vendor, e.g. `NVIDIA`.
        model(str): GPU model, e.g. `A100 80GB`.
        count(int): Number of GPUs of this type in the worker node.
        vram(int, optional): VRAM memory in MB. Defaults to `None`.
        architecture(str, optional): GPU architecture, e.g. `Tesla`, `Ampere`... Defaults to `None`.
        framework(str, optional): Driver framework available, e.g. `CUDA`. Defaults to `None`.
        framework_version(str, optional): Version of the driver framework, e.g. `12.2`. Defaults to `None`.
        driver_version(str, optional): Version of the driver, e.g. `575.51.03`. Defaults to `None`
        timeout(int, optional): The timeout value. Defaults to `60`.

    Returns:
        dict: The system response  `{"success": success, "message": message, "data": data}`. True for success, False for failure, and an error message.
    """
    tmp_logger = LogWrapper(_logger, f"update_worker_node_gpu site={site} host_name={host_name} vendor={vendor} model={model}")
    tmp_logger.debug("Start")

    timed_method = TimedMethod(global_task_buffer.update_worker_node_gpu, timeout)
    timed_method.run(
        site,
        host_name,
        vendor,
        model,
        count,
        vram,
        architecture,
        framework,
        framework_version,
        driver_version,
    )

    if timed_method.result == Protocol.TimeOutToken:  # timeout
        message = "Updating worker node GPU timed out"
        tmp_logger.error(message)
        return generate_response(False, message)

    success, message = timed_method.result

    tmp_logger.debug(f"Done")
    return generate_response(success, message)
