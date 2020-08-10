import re

try:
    long
except NameError:
    long = int

# list of prod source label for pilot tests
list_ptest_prod_sources = ['ptest', 'rc_test', 'rc_test2', 'rc_alrb']

# mapping with prodsourcelabels that belong to analysis and production
analy_sources = ['user', 'panda']
prod_sources = ['managed', 'prod_test']
neutral_sources = ['install'] + list_ptest_prod_sources

ANALY_PS = 'user'
PROD_PS = 'managed'

ANALY_TASKTYPE = 'anal'
PROD_TASKTYPE = 'prod'

job_labels = [ANALY_PS, PROD_PS]

# priority of tasks to jumbo over others
priorityTasksToJumpOver = 1500


def translate_resourcetype_to_cores(resource_type, cores_queue):
    # resolve the multiplying core factor
    if 'MCORE' in resource_type:
        return cores_queue
    else:
        return 1


def translate_prodsourcelabel_to_jobtype(queue_type, prodsourcelabel):
    if prodsourcelabel in analy_sources:
        return ANALY_PS

    if prodsourcelabel in prod_sources:
        return PROD_PS

    if prodsourcelabel in neutral_sources:
        if queue_type == 'unified' or queue_type == 'production':
            return PROD_PS
        if queue_type == 'analysis':
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
                tmpMatch = re.search('coreCount=(\d+)',jobMetrics)
            if tmpMatch is not None:
                coreCount = long(tmpMatch.group(1))
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
        tmpTimeDelta = endTime-startTime
        tmpVal = tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600
        if tmpVal <= baseWalltime:
            return 0
        hs06sec = float(tmpVal-baseWalltime) * corePower * coreCount * float(cpuEfficiency) / 100.0
        return hs06sec
    except Exception:
        return None


# parse string for number of standby jobs
def parseNumStandby(catchall):
    retMap = {}
    if catchall is not None:
        for tmpItem in catchall.split(','):
            tmpMatch = re.search('^nStandby=(.+)', tmpItem)
            if tmpMatch is None:
                continue
            for tmpSubStr in tmpMatch.group(1).split('|'):
                if len(tmpSubStr.split(':')) != 3:
                    continue
                sw_id, resource_type, num = tmpSubStr.split(':')
                try:
                    sw_id = int(sw_id)
                except Exception:
                    pass
                if sw_id not in retMap:
                    retMap[sw_id] = {}
                if num == '':
                    num = 0
                else:
                    num = int(num)
                retMap[sw_id][resource_type] = num
            break
    return retMap


# compensate memory count to prevent jobs with ramCount close to the HIMEM border from going to HIMEM PQs
def compensate_ram_count(ram_count):
    if ram_count == 'NULL':
        ram_count = None
    if ram_count is not None:
        ram_count = int(ram_count * 0.90)
    return ram_count
