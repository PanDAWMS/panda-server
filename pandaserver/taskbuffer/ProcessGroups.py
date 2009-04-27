processGroups = [('others',       []),
                 ('evgensimul',   ['evgen','simul']),
                 ('reprocessing', ['reprocessing']),
                 ('test',         ['prod_test','rc_test','validation']),
                 ]


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
    

# count the number of jobs per group
def countJobsPerGroup(valMap):
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
