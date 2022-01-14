import re
import sys
from OpenSSL import crypto
from pandaserver.taskbuffer import JobUtils

# get prefix for DQ2
def getDQ2Prefix(dq2SiteID):
    try:
        # prefix of DQ2 ID
        tmpDQ2IDPrefix = re.sub('_[A-Z,0-9]+DISK$','',dq2SiteID)
        # remove whitespace
        tmpDQ2IDPrefix = tmpDQ2IDPrefix.strip()
        # patchfor MWT2
        if tmpDQ2IDPrefix == 'MWT2_UC':
            tmpDQ2IDPrefix = 'MWT2'
        return tmpDQ2IDPrefix
    except Exception:
        return ''

# check if the file is cached
def isCachedFile(datasetName,siteSpec):
    # using CVMFS
    if siteSpec.iscvmfs is not True:
        return False
    # look for DBR
    if not datasetName.startswith('ddo'):
        return False
    # look for three digits
    if re.search('v\d{6}$',datasetName) is None:
        return False
    return True


# get the list of sites where dataset is available
def getSitesWithDataset(tmpDsName, siteMapper, replicaMap, cloudKey, prodSourceLabel, job_label, useHomeCloud=False,
                        getDQ2ID=False, useOnlineSite=False, includeT1=False):
    retList = []
    retDQ2Map = {}
    # no replica map
    if tmpDsName not in replicaMap:
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
        scopeSiteSpec_input, scopeSiteSpec_output = select_scope(tmpSiteSpec, prodSourceLabel, job_label)
        # skip T1
        if not includeT1:
            # T1
            if tmpSiteName == siteMapper.getCloud(cloudKey)['source']:
                continue
            # hospital queue
            tmpSrcSpec = siteMapper.getSite(siteMapper.getCloud(cloudKey)['source'])
            scopeSrcSpec_input, scopeSrcSpec_output = select_scope(tmpSrcSpec, prodSourceLabel, job_label)
            if tmpSiteSpec.ddm_input.get(scopeSiteSpec_input) == tmpSrcSpec.ddm_input.get(scopeSrcSpec_input):
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

        ddm_endpoints = []
        if scopeSiteSpec_input in tmpSiteSpec.ddm_input and tmpSiteSpec.ddm_input[scopeSiteSpec_input]:
            ddm_endpoints.append(tmpSiteSpec.ddm_input[scopeSiteSpec_input])
        if scopeSiteSpec_input in tmpSiteSpec.setokens_input and tmpSiteSpec.setokens_input[scopeSiteSpec_input].values():
            ddm_endpoints = ddm_endpoints + list(tmpSiteSpec.setokens_input[scopeSiteSpec_input].values())

        if not ddm_endpoints:
            # skip misconfigured sites
            continue

        for tmpSiteDQ2ID in ddm_endpoints:
            # prefix of DQ2 ID
            tmpDQ2IDPrefix = getDQ2Prefix(tmpSiteDQ2ID)
            # ignore empty
            if tmpDQ2IDPrefix == '':
                continue
            # loop over all replica DQ2 IDs
            for tmpDQ2ID in replicaMap[tmpDsName]:
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
                    if tmpSiteName not in retDQ2Map:
                        retDQ2Map[tmpSiteName] = []
                    if tmpDQ2ID not in retDQ2Map[tmpSiteName]:
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
        for tmpSE in tmpRepMap:
            # check match
            if re.search(tmpSePat,tmpSE) is None:
                continue
            # append
            if tmpSE not in retList:
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
    if re.match("^[A-Za-z0-9][A-Za-z0-9\.\-\_/]{1,255}$",datasetName) is not None:
        return True
    return False


# get dataset type
def getDatasetType(dataset):
    datasetType = None
    try:
        datasetType = dataset.split('.')[4]
    except Exception:
        pass
    return datasetType


# check certificate
def checkCertificate(certName):
    try:
        cert = crypto.load_certificate(crypto.FILETYPE_PEM, open(certName).read())
        if cert.has_expired() is True:
            return False,"{0} expired".format(certName)
        else:
            return True,None
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        return False,'{0}:{1}'.format(errtype.__name__,errvalue)


# get sites which share DDM endpoint
def getSitesShareDDM(siteMapper, siteName, prodSourceLabel, job_label, output_share=False):
    # output_share: False to get sites which use the output RSE as input, True to get sites which use
    #               the input RSEs as output

    # nonexistent site
    if not siteMapper.checkSite(siteName):
        return []
    # get siteSpec
    siteSpec = siteMapper.getSite(siteName)
    scope_site_input, scope_site_output = select_scope(siteSpec, prodSourceLabel, job_label)
    # loop over all sites
    retSites = []
    for tmpSiteName in siteMapper.siteSpecList:
        tmpSiteSpec = siteMapper.siteSpecList[tmpSiteName]
        scope_tmpSite_input, scope_tmpSite_output = select_scope(tmpSiteSpec, prodSourceLabel, job_label)
        # only same type
        if siteSpec.type != tmpSiteSpec.type:
            continue
        # only online sites
        if tmpSiteSpec.status != 'online':
            continue
        # same endpoint
        try:
            if not output_share and \
                siteSpec.ddm_output[scope_site_output] \
                    not in tmpSiteSpec.ddm_endpoints_input[scope_tmpSite_input].all:
                continue
            if output_share and \
                tmpSiteSpec.ddm_output[scope_site_output]\
                    not in siteSpec.ddm_endpoints_input[scope_tmpSite_input].all:
                continue
        except Exception:
            continue
        # skip itself
        if siteName == tmpSiteSpec.sitename:
            continue
        # append
        if tmpSiteSpec.sitename not in retSites:
            retSites.append(tmpSiteSpec.sitename)
    # return
    return retSites


 # check if destination is specified
def getDestinationSE(destinationDBlockToken):
    if destinationDBlockToken is not None:
        for tmpToken in destinationDBlockToken.split(','):
            tmpMatch = re.search('^dst:([^/]*)(/.*)*$',tmpToken)
            if tmpMatch is not None:
                return tmpMatch.group(1)
    return None


# check if job sets destination
def checkJobDestinationSE(tmpJob):
    for tmpFile in tmpJob.Files:
        if getDestinationSE(tmpFile.destinationDBlockToken) is not None:
            return tmpFile.destinationSE
    return None



 # check if destination is distributed
def getDistributedDestination(destinationDBlockToken):
    if destinationDBlockToken is not None:
        for tmpToken in destinationDBlockToken.split(','):
            tmpMatch = re.search('^ddd:([^/]*)(/.*)*$',tmpToken)
            if tmpMatch is not None:
                return tmpMatch.group(1)
    return None


# extract importand error string
def extractImportantError(out):
    retStr = ''
    try:
        strList = ['InvalidRSEExpression', 'Details:']
        for line in out.split('\n'):
            for tmpStr in strList:
                if tmpStr in line:
                    retStr += line
                    retStr += ' '
        retStr = retStr[:-1]
    except Exception:
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


def select_scope(site_spec, prodsourcelabel, job_label):
    """
    Select the scopes of the activity for input and output. The scope was introduced for prod-analy queues
    where you might want to associate different RSEs depending on production or analysis.
    """
    scope_input = 'default'
    aux_scopes_input = site_spec.ddm_endpoints_input.keys()
    if (job_label == JobUtils.ANALY_PS or prodsourcelabel in JobUtils.analy_sources) and 'analysis' in aux_scopes_input:
        scope_input = 'analysis'

    scope_output = 'default'
    aux_scopes_output = site_spec.ddm_endpoints_output.keys()
    if (job_label == JobUtils.ANALY_PS or prodsourcelabel in JobUtils.analy_sources) and 'analysis' in aux_scopes_output:
        scope_output = 'analysis'

    return scope_input, scope_output
