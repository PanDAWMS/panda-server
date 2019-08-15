import re
import sys
from OpenSSL import crypto

# get prefix for DQ2
def getDQ2Prefix(dq2SiteID):
    # prefix of DQ2 ID
    tmpDQ2IDPrefix = re.sub('_[A-Z,0-9]+DISK$','',dq2SiteID)
    # remove whitespace 
    tmpDQ2IDPrefix = tmpDQ2IDPrefix.strip()
    # patchfor MWT2
    if tmpDQ2IDPrefix == 'MWT2_UC':
        tmpDQ2IDPrefix = 'MWT2'
    return tmpDQ2IDPrefix


# check if the file is cached
def isCachedFile(datasetName,siteSpec):
    # using CVMFS
    if siteSpec.iscvmfs != True:
        return False
    # look for DBR
    if not datasetName.startswith('ddo'):
        return False
    # look for three digits
    if re.search('v\d{6}$',datasetName) == None:
        return False
    return True


# get the list of sites where dataset is available
def getSitesWithDataset(tmpDsName, siteMapper, replicaMap, cloudKey, prodSourceLabel, useHomeCloud=False, getDQ2ID=False,
                        useOnlineSite=False, includeT1=False):
    retList = []
    retDQ2Map = {}
    # no replica map
    if not replicaMap.has_key(tmpDsName):
        if getDQ2ID:
            return retDQ2Map
        return retList
    # use valid cloud
    if not siteMapper.checkCloud(cloudKey):
        if getDQ2ID:
            return retDQ2Map
        return retList
    # check sites in the cloud
    for tmpSiteName in siteMapper.getCloud(cloudKey)['sites']:
        tmpSiteSpec = siteMapper.getSite(tmpSiteName)
        scopeSiteSpec = select_scope(tmpSiteSpec, prodSourceLabel)
        # skip T1
        if not includeT1:
            # T1
            if tmpSiteName == siteMapper.getCloud(cloudKey)['source']:
                continue
            # hospital queue
            tmpSrcSpec = siteMapper.getSite(siteMapper.getCloud(cloudKey)['source'])
            scopeSrcSpec = select_scope(tmpSrcSpec, prodSourceLabel)
            if tmpSiteSpec.ddm_input[scopeSiteSpec] == siteMapper.getSite(siteMapper.getCloud(cloudKey)['source']).ddm_input[scopeSrcSpec]:
                continue
        # use home cloud
        if useHomeCloud:
            if siteMapper.getSite(tmpSiteName).cloud != cloudKey:
                continue
        # online
        if siteMapper.getSite(tmpSiteName).status != 'online':
            continue
        # check all associated DQ2 IDs
        tmpFoundFlag = False

        # skip misconfigured sites
        if not tmpSiteSpec.ddm_inputp[scopeSiteSpec] and not tmpSiteSpec.setokens_input[scopeSiteSpec].values():
            continue

        for tmpSiteDQ2ID in [tmpSiteSpec.ddm_input[scopeSiteSpec]]+tmpSiteSpec.setokens_input[scopeSiteSpec].values():
            # prefix of DQ2 ID
            tmpDQ2IDPrefix = getDQ2Prefix(tmpSiteDQ2ID)
            # ignore empty
            if tmpDQ2IDPrefix == '':
                continue
            # loop over all replica DQ2 IDs 
            for tmpDQ2ID in replicaMap[tmpDsName].keys():
                # use DATADISK or GROUPDISK 
                if '_SCRATCHDISK'        in tmpDQ2ID or \
                       '_USERDISK'       in tmpDQ2ID or \
                       '_PRODDISK'       in tmpDQ2ID or \
                       '_LOCALGROUPDISK' in tmpDQ2ID or \
                       '_LOCALGROUPTAPE' in tmpDQ2ID or \
                       'TAPE'            in tmpDQ2ID or \
                       '_DAQ'            in tmpDQ2ID or \
                       '_TMPDISK'        in tmpDQ2ID or \
                       '_TZERO'          in tmpDQ2ID:
                    continue
                # check DQ2 prefix
                if tmpDQ2ID.startswith(tmpDQ2IDPrefix):
                    tmpFoundFlag = True
                    if not getDQ2ID:
                        break
                    # append map
                    if not retDQ2Map.has_key(tmpSiteName):
                        retDQ2Map[tmpSiteName] = []
                    if not tmpDQ2ID in retDQ2Map[tmpSiteName]:    
                        retDQ2Map[tmpSiteName].append(tmpDQ2ID)    
        # append
        if tmpFoundFlag:
            retList.append(tmpSiteName)
    # return map
    if getDQ2ID:
        return retDQ2Map
    # retrun
    return retList


# get the list of sites where dataset is available
def getEndpointsAtT1(tmpRepMap,siteMapper,cloudName):
    retList = []
    # get cloud SEs
    tmpCloud = siteMapper.getCloud(cloudName)
    cloudSEs = tmpCloud['tier1SE']
    # check T1 endpoints
    for tmpSePat in cloudSEs:
        # ignore empty
        if tmpSePat == '':
            continue
        # make regexp pattern
        if '*' in tmpSePat:
            tmpSePat = tmpSePat.replace('*','.*')
        tmpSePat = '^' + tmpSePat +'$'
        # loop over all sites
        for tmpSE in tmpRepMap.keys():
            # check match
            if re.search(tmpSePat,tmpSE) == None:
                continue
            # append
            if not tmpSE in retList:
                retList.append(tmpSE)
    # return
    return retList


# check DDM response
def isDQ2ok(out):
    if out.find("DQ2 internal server exception") != -1 \
           or out.find("An error occurred on the central catalogs") != -1 \
           or out.find("MySQL server has gone away") != -1 \
           or out == '()':
        return False
    return True


# check if DBR
def isDBR(datasetName):
    if datasetName.startswith('ddo'):
        return True
    return False


# check invalid characters in dataset name
def checkInvalidCharacters(datasetName):
    if re.match("^[A-Za-z0-9][A-Za-z0-9\.\-\_/]{1,255}$",datasetName) != None:
        return True
    return False


# get dataset type
def getDatasetType(dataset):
    datasetType = None
    try:
        datasetType = dataset.split('.')[4]
    except:
        pass
    return datasetType


# check certificate
def checkCertificate(certName):
    try:
        cert = crypto.load_certificate(crypto.FILETYPE_PEM,file(certName).read())
        if cert.has_expired() is True:
            return False,"{0} expired".format(certName)
        else:
            return True,None
    except:
        errtype,errvalue = sys.exc_info()[:2]
        return False,'{0}:{1}'.format(errtype.__name__,errvalue)


# get sites which share DDM endpoint
def getSitesShareDDM(siteMapper, siteName, prodSourceLabel):

    # nonexistent site
    if not siteMapper.checkSite(siteName):
        return []
    # get siteSpec
    siteSpec = siteMapper.getSite(siteName)
    scope_site = select_scope(siteSpec, prodSourceLabel)
    # loop over all sites
    retSites = []
    for tmpSiteName,tmpSiteSpec in siteMapper.siteSpecList.iteritems():
        scope_tmpSite = select_scope(tmpSiteSpec, prodSourceLabel)
        # only same type
        if siteSpec.type != tmpSiteSpec.type:
            continue
        # only online sites
        if tmpSiteSpec.status != 'online':
            continue
        # same endpoint
        try:
            if siteSpec.ddm_input[scope_site] != tmpSiteSpec.ddm_input[scope_tmpSite] \
                    and siteSpec.ddm_output[scope_site] not in tmpSiteSpec.ddm_endpoints_input[scope_tmpSite].all.keys():
                continue
        except:
            continue
        # skip itself
        if siteName == tmpSiteSpec.sitename:
            continue
        # append
        if not tmpSiteSpec.sitename in retSites:
            retSites.append(tmpSiteSpec.sitename)
    # return
    return retSites


 # check if destination is specified
def getDestinationSE(destinationDBlockToken):
    if destinationDBlockToken != None:
        for tmpToken in destinationDBlockToken.split(','):
            tmpMatch = re.search('^dst:([^/]*)(/.*)*$',tmpToken)
            if tmpMatch != None:
                return tmpMatch.group(1)
    return None


# check if job sets destination
def checkJobDestinationSE(tmpJob):
    for tmpFile in tmpJob.Files:
        if getDestinationSE(tmpFile.destinationDBlockToken) != None:
            return tmpFile.destinationSE
    return None



 # check if destination is distributed
def getDistributedDestination(destinationDBlockToken):
    if destinationDBlockToken != None:
        for tmpToken in destinationDBlockToken.split(','):
            tmpMatch = re.search('^ddd:([^/]*)(/.*)*$',tmpToken)
            if tmpMatch != None:
                return tmpMatch.group(1)
    return None



# change output of listDatasets to include dataset info
def changeListDatasetsOut(out,datasetName=None):
    try:
        exec 'origMap = '+out.split('\n')[0]
        newMap = {}
        for tmpkey,tmpval in origMap.iteritems():
            # rucio doesn't put / for container
            rucioConvention = False
            if datasetName != None and datasetName.endswith('/') and not tmpkey.endswith('/'):
                rucioConvention = True
            # original key-value
            newMap[tmpkey] = tmpval
            if rucioConvention:
                # add /
                newMap[tmpkey+'/'] = tmpval
            # remove scope
            if ':' in tmpkey:
                newMap[tmpkey.split(':')[-1]] = tmpval
                if rucioConvention:
                    # add /
                    newMap[tmpkey.split(':')[-1]+'/'] = tmpval
        return str(newMap)
    except:
        pass
    return out
    


# extract importand error string
def extractImportantError(out):
    retStr = ''
    try:
        strList = ['InvalidRSEExpression','Details:']
        for line in out.split('\n'):
            for tmpStr in strList:
                if tmpStr in line:
                    retStr += line
                    retStr += ' '
        retStr = retStr[:-1]
    except:
        pass
    return retStr



# get activity for output
def getActivityForOut(prodSourceLabel):
    if prodSourceLabel in ['managed']:
        activity = "Production Output"
    elif prodSourceLabel in ['user','panda']:
        activity = "User Subscriptions"
    else:
        activity = "Functional Test" 
    return activity


# cleanup DN
def cleanupDN(realDN):
    tmpRealDN = re.sub('/CN=limited proxy','',realDN)
    tmpRealDN = re.sub('/CN=proxy','',tmpRealDN)
    return tmpRealDN


def select_scope(site_spec, prodsourcelabel):
    """
    Select the scope of the activity. The scope was introduced for prod-analy queues where you might want
    to associate different RSEs depending on production or analysis.
    """
    scopes = site_spec.ddm_endpoints_input.keys()
    if prodsourcelabel == 'user' and 'analysis' in scopes:
        return 'analysis'
    else:
        return 'default'