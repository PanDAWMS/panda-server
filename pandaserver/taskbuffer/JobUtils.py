import datetime
import json
import re
from typing import TYPE_CHECKING, Any

from pandaserver.srvcore.CoreUtils import NonJsonObjectEncoder, as_python_object
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.spec_column import Null

if TYPE_CHECKING:
    # imported for the annotation only: ResourceSpec imports this module, so a runtime
    # import here would close the cycle
    from pandaserver.taskbuffer.InputChunk import InputChunk
    from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
    from pandaserver.taskbuffer.ResourceSpec import ResourceSpec
    from pandaserver.taskbuffer.SiteSpec import SiteSpec

# list of prod source label for pilot tests
list_ptest_prod_sources = ["ptest", "rc_test", "rc_test2", "rc_alrb"]

# mapping with prodsourcelabels that belong to analysis and production
analy_sources = ["user", "panda"]
prod_sources = ["managed", "prod_test"]
neutral_sources = ["install"] + list_ptest_prod_sources

ANALY_PS = "user"
PROD_PS = "managed"

ANALY_TASKTYPE = "anal"
PROD_TASKTYPE = "prod"

MEMORY_COMPENSATION = 0.9

job_labels = [ANALY_PS, PROD_PS]

# priority of tasks to jumbo over others
priorityTasksToJumpOver = 1500


def translate_prodsourcelabel_to_jobtype(queue_type: str | None, prodsourcelabel: str | None) -> str | None:
    if prodsourcelabel in analy_sources:
        return ANALY_PS

    if prodsourcelabel in prod_sources:
        return PROD_PS

    if prodsourcelabel in neutral_sources:
        if queue_type == "unified" or queue_type == "production":
            return PROD_PS
        if queue_type == "analysis":
            return ANALY_PS

    # currently unmapped
    return prodsourcelabel


def translate_tasktype_to_jobtype(task_type: str | None) -> str:
    # any unrecognized tasktype will be defaulted to production
    if task_type == ANALY_TASKTYPE:
        return ANALY_PS
    else:
        return PROD_PS


# get core count
def getCoreCount(actualCoreCount: int | None, defCoreCount: int | None, jobMetrics: str | None) -> int:
    coreCount = 1
    try:
        if actualCoreCount is not None:
            coreCount = actualCoreCount
        else:
            tmpMatch = None
            if jobMetrics is not None:
                # extract coreCount
                tmpMatch = re.search("coreCount=(\d+)", jobMetrics)
            if tmpMatch is not None:
                coreCount = int(tmpMatch.group(1))
            else:
                # use jobdef
                if defCoreCount not in [None, 0]:
                    coreCount = defCoreCount
    except Exception:
        pass
    return coreCount


# get HS06sec
def getHS06sec(
    startTime: datetime.datetime,
    endTime: datetime.datetime,
    corePower: float,
    coreCount: int,
    baseWalltime: int = 0,
    cpuEfficiency: int = 100,
) -> float | None:
    try:
        # no scaling
        if cpuEfficiency == 0:
            return 0
        # get execution time
        tmpTimeDelta = endTime - startTime
        tmpVal = tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600
        if tmpVal <= baseWalltime:
            return 0
        hs06sec = float(tmpVal - baseWalltime) * corePower * coreCount * float(cpuEfficiency) / 100.0
        return hs06sec
    except Exception:
        return None


def get_job_co2(
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    core_count: int,
    energy_emissions: list[tuple[Any, ...]],
    watts_per_core: float,
) -> float | None:
    energy_emissions_by_ts = {}
    for entry in energy_emissions:
        aux_timestamp, region, value = entry
        energy_emissions_by_ts[aux_timestamp] = {"value": value}

    try:
        timestamps = sorted([entry[0] for entry in energy_emissions])

        started = False
        ended = False
        i = 0

        g_co2_job = 0

        for timestamp in timestamps:
            try:
                if start_time < timestamps[i + 1] and not started:
                    started = True
            except IndexError:
                pass

            if end_time < timestamp and not ended:
                ended = True

            if started and not ended or i == len(timestamps) - 1:
                bottom = max(start_time, timestamp)
                try:
                    top = min(end_time, timestamps[i + 1])
                except IndexError:
                    top = end_time

                g_co2_perkWh = energy_emissions_by_ts[timestamp]["value"]

                duration = max((top - bottom).total_seconds(), 0)
                g_co2_job = g_co2_job + (duration * g_co2_perkWh * core_count * watts_per_core / 3600 / 1000)

            if ended:
                break

            i = i + 1

        return g_co2_job

    except Exception:
        return None


# parse string for number of standby jobs
def parseNumStandby(catchall: str | None) -> dict[int | str, dict[str, int]]:
    retMap: dict[int | str, dict[str, int]] = {}
    if catchall is not None:
        for tmpItem in catchall.split(","):
            tmpMatch = re.search("^nStandby=(.+)", tmpItem)
            if tmpMatch is None:
                continue
            for tmpSubStr in tmpMatch.group(1).split("|"):
                if len(tmpSubStr.split(":")) != 3:
                    continue
                sw_id_str, resource_type, num_str = tmpSubStr.split(":")
                # a work queue is identified by a number and a global share by its name
                sw_id: int | str
                try:
                    sw_id = int(sw_id_str)
                except Exception:
                    sw_id = sw_id_str
                if sw_id not in retMap:
                    retMap[sw_id] = {}
                if num_str == "":
                    num = 0
                else:
                    num = int(num_str)
                retMap[sw_id][resource_type] = num
            break
    return retMap


# compensate memory count to prevent jobs with ramCount close to the HIMEM border from going to HIMEM PQs
def compensate_ram_count(ram_count: float | Null | None) -> int | None:
    if ram_count is None or isinstance(ram_count, str):
        return None
    ram_count = int(ram_count * MEMORY_COMPENSATION)
    return ram_count


# undo the memory count compensation
def decompensate_ram_count(ram_count: float | Null | None) -> int | None:
    if ram_count is None or isinstance(ram_count, str):
        return None
    ram_count = int(ram_count / MEMORY_COMPENSATION)
    return ram_count


# dump jobs to serialized json
def dump_jobs_json(jobs: list[JobSpec]) -> str:
    state_objects = []
    for job_spec in jobs:
        state_objects.append(job_spec.dump_to_json_serializable())
    return json.dumps(state_objects, cls=NonJsonObjectEncoder)


# load serialized json to jobs
def load_jobs_json(state: str) -> list[JobSpec]:
    state_objects = json.loads(state, object_hook=as_python_object)
    jobs = []
    for job_state in state_objects:
        job_spec = JobSpec()
        job_spec.load_from_json_serializable(job_state)
        jobs.append(job_spec)
    return jobs


# get resource type for a job
def get_resource_type_job(resource_map: list["ResourceSpec"], job_spec: JobSpec) -> str | None:
    """
    Get the resource type for a job based on the job's resource type and the list of resource types.
    :param resource_map: The list of resource types.
    :param job_spec: The job.
    :return: The resource type, or None when the matching spec has no resource_name --
        that column is nullable, and every caller assigns the result to a nullable column.
    """
    for resource_spec in resource_map:
        if resource_spec.match_job(job_spec):
            return resource_spec.resource_name
    return "Undefined"


# get min ram count for job
def getJobMinRamCount(taskSpec: "JediTaskSpec", inputChunk: "InputChunk", siteSpec: "SiteSpec", coreCount: int) -> tuple[int | None, str]:
    minRamCount = inputChunk.getMaxRamCount()
    if inputChunk.isMerging:
        minRamUnit = "MB"
    else:
        minRamUnit = taskSpec.ramUnit or "MB"
        if minRamUnit == "NULL":
            minRamUnit = "MB"
        if taskSpec.ramPerCore():
            minRamCount *= coreCount
            # an unset base adds nothing, which is the reading CoreUtils.getJobMaxWalltime takes
            if taskSpec.baseRamCount is not None:
                minRamCount += taskSpec.baseRamCount
            minRamUnit = re.sub("PerCore.*$", "", minRamUnit)
    # round up with chunks
    return compensate_ram_count(minRamCount), minRamUnit
