import json
import re

from pandaserver.srvcore.CoreUtils import NonJsonObjectEncoder, as_python_object
from pandaserver.taskbuffer.JobSpec import JobSpec

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


def translate_prodsourcelabel_to_jobtype(queue_type, prodsourcelabel):
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


def translate_tasktype_to_jobtype(task_type):
    # any unrecognized tasktype will be defaulted to production
    if task_type == ANALY_TASKTYPE:
        return ANALY_PS
    else:
        return PROD_PS


# get core count
def getCoreCount(actualCoreCount, defCoreCount, jobMetrics):
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
def getHS06sec(startTime, endTime, corePower, coreCount, baseWalltime=0, cpuEfficiency=100):
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


def get_job_co2(start_time, end_time, core_count, energy_emissions, watts_per_core):
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
def parseNumStandby(catchall):
    retMap = {}
    if catchall is not None:
        for tmpItem in catchall.split(","):
            tmpMatch = re.search("^nStandby=(.+)", tmpItem)
            if tmpMatch is None:
                continue
            for tmpSubStr in tmpMatch.group(1).split("|"):
                if len(tmpSubStr.split(":")) != 3:
                    continue
                sw_id, resource_type, num = tmpSubStr.split(":")
                try:
                    sw_id = int(sw_id)
                except Exception:
                    pass
                if sw_id not in retMap:
                    retMap[sw_id] = {}
                if num == "":
                    num = 0
                else:
                    num = int(num)
                retMap[sw_id][resource_type] = num
            break
    return retMap


# compensate memory count to prevent jobs with ramCount close to the HIMEM border from going to HIMEM PQs
def compensate_ram_count(ram_count):
    if ram_count in ("NULL", None):
        return None
    ram_count = int(ram_count * MEMORY_COMPENSATION)
    return ram_count


# undo the memory count compensation
def decompensate_ram_count(ram_count):
    if ram_count in ("NULL", None):
        return None
    ram_count = int(ram_count / MEMORY_COMPENSATION)
    return ram_count


# dump jobs to serialized json
def dump_jobs_json(jobs):
    state_objects = []
    for job_spec in jobs:
        state_objects.append(job_spec.dump_to_json_serializable())
    return json.dumps(state_objects, cls=NonJsonObjectEncoder)


# load serialized json to jobs
def load_jobs_json(state):
    state_objects = json.loads(state, object_hook=as_python_object)
    jobs = []
    for job_state in state_objects:
        job_spec = JobSpec()
        job_spec.load_from_json_serializable(job_state)
        jobs.append(job_spec)
    return jobs


# get resource type for a job
def get_resource_type_job(resource_map: list, job_spec: JobSpec) -> str:
    """
    Get the resource type for a job based on the job's resource type and the list of resource types.
    :param resource_map: The list of resource types.
    :param job_spec: The job.
    :return: The resource type.
    """
    for resource_spec in resource_map:
        if resource_spec.match_job(job_spec):
            return resource_spec.resource_name
    return "Undefined"
