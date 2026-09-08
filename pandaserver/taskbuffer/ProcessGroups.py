from typing import Any

from pandaserver.taskbuffer import JobUtils

processGroups = [
    ("others", []),
    ("evgen", ["evgen"]),
    ("simul", ["simul"]),
    ("reprocessing", ["reprocessing"]),
    ("test", ["prod_test", "validation"] + JobUtils.list_ptest_prod_sources),
    ("mcore", ["mcore"]),
    ("group", ["group"]),
    ("deriv", ["deriv"]),
    ("pile", ["pile"]),
    ("merge", ["merge"]),
]

# ('evgensimul',   ['evgen','simul']),

# maximum number of debug jobs per user
maxDebugJobs = 3

# maximum number of debug jobs for prod role
maxDebugProdJobs = 30

# maximum number of debug jobs for working group
maxDebugWgJobs = 10

# extension level for GP
extensionLevel_1 = 1


# get corresponding group
def getProcessGroup(valGroup: str | None) -> str:
    # the first entry is the default. Its own list is empty, so letting the loop below see it
    # cannot match; the original spelling skipped it explicitly to install it as the default
    tmpGroup = processGroups[0][0]
    for tmpKey, tmpList in processGroups:
        if valGroup in tmpList:
            tmpGroup = tmpKey
            break
    # return
    return tmpGroup


# convert cloud and processingType for extended PG
def converCPTforEPG(cloud: str, processingType: str, coreCount: int | None, workingGroup: str | None = None) -> tuple[str, str]:
    if coreCount in [0, 1, None]:
        # use group queue for GP jobs
        if workingGroup is not None and workingGroup.startswith("GP_"):
            return cloud, "group"
        return cloud, processingType
    else:
        # use MCORE queue for MPC jobs in all clouds
        return cloud, "mcore"


# count the number of jobs per group
def countJobsPerGroup(valMap: dict[str, dict[str, dict[str, dict[str, int]]]]) -> dict[str, dict[str, dict[str, dict[str, int]]]]:
    # cloud -> site -> process group -> job status -> count. The process group replaces the
    # processing type the input is keyed by, which is why the two shapes are the same but not
    # interchangeable
    ret: dict[str, dict[str, dict[str, dict[str, int]]]] = {}
    # loop over all clouds
    for cloud in valMap:
        cloudVal = valMap[cloud]
        # add cloud
        ret.setdefault(cloud, {})
        # loop over all sites
        for site in cloudVal:
            siteVal = cloudVal[site]
            # add site
            ret[cloud].setdefault(site, {})
            # loop over all types
            for pType in siteVal:
                typeVal = siteVal[pType]
                # get process group
                tmpGroup = getProcessGroup(pType)
                # add group
                ret[cloud][site].setdefault(tmpGroup, {})
                # loop over all status
                for jobStatus in typeVal:
                    statVal = typeVal[jobStatus]
                    ret[cloud][site][tmpGroup].setdefault(jobStatus, 0)
                    # add
                    ret[cloud][site][tmpGroup][jobStatus] += statVal
    # return
    return ret


# count the number of jobs per group for analysis
def countJobsPerGroupForAnal(valMap: dict[str, dict[str, dict[str, int]]]) -> dict[str, dict[str, dict[str, int]]]:
    # as countJobsPerGroup, without the cloud level: site -> process group -> job status -> count
    ret: dict[str, dict[str, dict[str, int]]] = {}
    # loop over all sites
    for site in valMap:
        siteVal = valMap[site]
        # add site
        ret.setdefault(site, {})
        # loop over all types
        for pType in siteVal:
            typeVal = siteVal[pType]
            # get process group
            tmpGroup = getProcessGroup(pType)
            # add group
            if tmpGroup not in ret[site]:
                ret[site][tmpGroup] = {}
            # loop over all status
            for jobStatus in typeVal:
                statVal = typeVal[jobStatus]
                ret[site][tmpGroup].setdefault(jobStatus, 0)
                # add
                ret[site][tmpGroup][jobStatus] += statVal
    # return
    return ret
