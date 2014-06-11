processGroups = [('others',       []),
                 ('evgensimul',   ['evgen','simul']),
                 ('reprocessing', ['reprocessing']),
                 ('test',         ['prod_test','rc_test','validation']),
                 ('mcore',        ['mcore']),
                 ('group',        ['group']),                 
                 ]

# source labels used for panda internal purpose
internalSourceLabels = ['ddm']

# maximum number of debug jobs per user
maxDebugJobs   = 3

# maximum number of debug jobs for prod role
maxDebugProdJobs  = 30

# maximum number of debug jobs for working group
maxDebugWgJobs = 10

# extension level for GP
extensionLevel_1 = 1


# get corresponding group
def getProcessGroup(valGroup):
    tmpGroup = None
    for tmpKey,tmpList in processGroups:
        # set default
        if tmpGroup == None:
            tmpGroup = tmpKey
            continue
        if valGroup in tmpList:
            tmpGroup = tmpKey
            break
    # return
    return tmpGroup
    

# convert cloud and processingType for extended PG
def converCPTforEPG(cloud,processingType,coreCount,workingGroup=None):
    if coreCount in [0,1,None]:
        # use group queue for GP jobs
        if workingGroup != None and workingGroup.startswith('GP_'):
            return cloud,'group'
        return cloud,processingType
    else:
        # use MCORE queue for MPC jobs in all clouds
        return cloud,"mcore"

    
# count the number of jobs per group
def countJobsPerGroup(valMap):
    ret = {}
    # loop over all clouds
    for cloud,cloudVal in valMap.iteritems():
        # add cloud
        if not ret.has_key(cloud):
            ret[cloud] = {}
        # loop over all sites
        for site,siteVal in cloudVal.iteritems():
            # add site
            if not ret[cloud].has_key(site):
                ret[cloud][site] = {}
            # loop over all types
            for pType,typeVal in siteVal.iteritems():
                # get process group
                tmpGroup = getProcessGroup(pType)
                # add group
                if not ret[cloud][site].has_key(tmpGroup):
                    ret[cloud][site][tmpGroup] = {}
                # loop over all status
                for jobStatus,statVal in typeVal.iteritems():
                    if not ret[cloud][site][tmpGroup].has_key(jobStatus):
                        ret[cloud][site][tmpGroup][jobStatus] = 0
                    # add
                    ret[cloud][site][tmpGroup][jobStatus] += statVal
    # return
    return ret


# count the number of jobs per group for analysis
def countJobsPerGroupForAnal(valMap):
    ret = {}
    # loop over all sites
    for site,siteVal in valMap.iteritems():
        # add site
        if not ret.has_key(site):
            ret[site] = {}
        # loop over all types
        for pType,typeVal in siteVal.iteritems():
            # get process group
            tmpGroup = getProcessGroup(pType)
            # add group
            if not ret[site].has_key(tmpGroup):
                ret[site][tmpGroup] = {}
            # loop over all status
            for jobStatus,statVal in typeVal.iteritems():
                if not ret[site][tmpGroup].has_key(jobStatus):
                    ret[site][tmpGroup][jobStatus] = 0
                # add
                ret[site][tmpGroup][jobStatus] += statVal
    # return
    return ret
