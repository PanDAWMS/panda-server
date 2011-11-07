import re

# get the list of sites where dataset is available
def getSitesWithDataset(tmpDsName,siteMapper,replicaMap,cloudKey,useHomeCloud=False):
    retList = []
    # no replica map
    if not replicaMap.has_key(tmpDsName):
        return retList
    # use valid cloud
    if not siteMapper.checkCloud(cloudKey):
        return retList
    # check sites in the cloud
    for tmpSiteName in siteMapper.getCloud(cloudKey)['sites']:
        # skip T1
        if tmpSiteName == siteMapper.getCloud(cloudKey)['source']:
            continue
        # use home cloud
        if useHomeCloud:
            if siteMapper.getSite(tmpSiteName).cloud != cloudKey:
                continue
        # prefix of DQ2 ID
        tmpDQ2IDPrefix = re.sub('_[A-Z,0-9]+DISK$','',siteMapper.getSite(tmpSiteName).ddm)
        # ignore empty
        tmpDQ2IDPrefix = tmpDQ2IDPrefix.strip()
        if tmpDQ2IDPrefix == '':
            continue
        # loop over all DQ2 IDs
        tmpFoundFlag = False
        for tmpDQ2ID in replicaMap[tmpDsName].keys():
            # use DATADISK or GROUPDISK 
            if '_SCRATCHDISK'        in tmpDQ2ID or \
                   '_USERDISK'       in tmpDQ2ID or \
                   '_PRODDISK'       in tmpDQ2ID or \
                   '_LOCALGROUPDISK' in tmpDQ2ID or \
                   'TAPE'            in tmpDQ2ID or \
                   '_DAQ'            in tmpDQ2ID or \
                   '_TZERO'          in tmpDQ2ID:
                continue
            # check DQ2 prefix
            if tmpDQ2ID.startswith(tmpDQ2IDPrefix):
                tmpFoundFlag = True
                break
        # append
        if tmpFoundFlag:
            retList.append(tmpSiteName)
    # retrun
    return retList

