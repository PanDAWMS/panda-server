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
def getProcessGroup(valGroup):
    tmpGroup = None
    for tmpKey, tmpList in processGroups:
        # set default
        if tmpGroup is None:
            tmpGroup = tmpKey
            continue
        if valGroup in tmpList:
            tmpGroup = tmpKey
            break
    # return
    return tmpGroup


# convert cloud and processingType for extended PG
def converCPTforEPG(cloud, processingType, coreCount, workingGroup=None):
    if coreCount in [0, 1, None]:
        # use group queue for GP jobs
        if workingGroup is not None and workingGroup.startswith("GP_"):
            return cloud, "group"
        return cloud, processingType
    else:
        # use MCORE queue for MPC jobs in all clouds
        return cloud, "mcore"


# count the number of jobs per group
def countJobsPerGroup(valMap):
    ret = {}
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
def countJobsPerGroupForAnal(valMap):
    ret = {}
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
