import re

try:
    long
except NameError:
    long = int

# list of prod source label for pilot tests
list_ptest_prod_sources = ['ptest', 'rc_test', 'rc_test2', 'rc_alrb']

# priority of tasks to jumbo over others
priorityTasksToJumpOver = 1500



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
                if not defCoreCount in [None, 0]:
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
