import re
import sys
from OpenSSL import crypto


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
    if siteSpec.iscvmfs != True:
        return False
    # look for DBR
    if not datasetName.startswith('ddo'):
        return False
    # look for three digits
    if re.search('v\d{6}$',datasetName) is None:
        return False
    return True


# get the list of sites where dataset is available
def getSitesWithDataset(tmpDsName,siteMapper,replicaMap,cloudKey,useHomeCloud=False,getDQ2ID=False,
                        useOnlineSite=False,includeT1=False):
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
        # skip T1
        if not includeT1:
            # T1
            if tmpSiteName == siteMapper.getCloud(cloudKey)['source']:
                continue
            # hospital queue
            if siteMapper.getSite(tmpSiteName).ddm_input == siteMapper.getSite(siteMapper.getCloud(cloudKey)['source']).ddm_input:
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
        tmpSiteSpec = siteMapper.getSite(tmpSiteName)

        # skip misconfigured sites
        if not tmpSiteSpec.ddm_input and not tmpSiteSpec.setokens_input.values():
            continue

        for tmpSiteDQ2ID in [tmpSiteSpec.ddm_input]+list(tmpSiteSpec.setokens_input.values()):
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


# get the number of files available at the site
def getNumAvailableFilesSite(siteName,siteMapper,replicaMap,badMetaMap,additionalSEs=[],
                             noCheck=[],fileCounts=None):
    try:
        # get DQ2 endpoints 
        tmpSiteSpec = siteMapper.getSite(siteName)
        prefixList = []
        for tmpSiteDQ2ID in [tmpSiteSpec.ddm_input]+list(tmpSiteSpec.setokens_input.values()):
            # prefix of DQ2 ID
            tmpDQ2IDPrefix = getDQ2Prefix(tmpSiteDQ2ID)
            # ignore empty
            if tmpDQ2IDPrefix != '':
                prefixList.append(tmpDQ2IDPrefix)
        # loop over datasets
        totalNum = 0
        for tmpDsName in replicaMap:
            tmpSitesData = replicaMap[tmpDsName]
            # cached files
            if isCachedFile(tmpDsName,tmpSiteSpec) and fileCounts is not None and \
               tmpDsName in fileCounts:
                # add with no check
                totalNum += fileCounts[tmpDsName]
                continue
            # dataset type
            datasetType = getDatasetType(tmpDsName)
            # use total num to effectively skip file availability check
            if datasetType in noCheck:
                columnName = 'total'
            else:
                columnName = 'found'
            # get num of files
            maxNumFile = 0
            # for T1 or T2
            if additionalSEs != []:
                # check T1 endpoints
                for tmpSePat in additionalSEs:
                    # ignore empty
                    if tmpSePat == '':
                        continue
                    # make regexp pattern
                    if '*' in tmpSePat:
                        tmpSePat = tmpSePat.replace('*','.*')
                    tmpSePat = '^' + tmpSePat +'$'
                    # loop over all sites
                    for tmpSE in tmpSitesData:
                        # skip bad metadata
                        if tmpDsName in badMetaMap and tmpSE in badMetaMap[tmpDsName]:
                            continue
                        # check match
                        if re.search(tmpSePat,tmpSE) is None:
                            continue
                        # get max num of files
                        tmpN = tmpSitesData[tmpSE][0][columnName]                            
                        if tmpN is not None and tmpN > maxNumFile:
                            maxNumFile = tmpN
            else:
                # check explicit endpoint name
                for tmpSiteDQ2ID in [tmpSiteSpec.ddm_input]+list(tmpSiteSpec.setokens_input.values()):
                    # skip bad metadata
                    if tmpDsName in badMetaMap and tmpSiteDQ2ID in badMetaMap[tmpDsName]:
                        continue
                    # ignore empty
                    if tmpSiteDQ2ID == '':
                        continue
                    # get max num of files
                    if tmpSiteDQ2ID in tmpSitesData:
                        tmpN = tmpSitesData[tmpSiteDQ2ID][0][columnName]
                        if tmpN is not None and tmpN > maxNumFile:
                            maxNumFile = tmpN
                # check prefix
                for tmpDQ2IDPrefix in prefixList:
                    for tmpDQ2ID in tmpSitesData:
                        # skip bad metadata
                        if tmpDsName in badMetaMap and tmpDQ2ID in badMetaMap[tmpDsName]:
                            continue
                        # ignore NG
                        if     '_SCRATCHDISK'    in tmpDQ2ID or \
                               '_USERDISK'       in tmpDQ2ID or \
                               '_PRODDISK'       in tmpDQ2ID or \
                               '_LOCALGROUPDISK' in tmpDQ2ID or \
                               '_LOCALGROUPTAPE' in tmpDQ2ID or \
                               '_DAQ'            in tmpDQ2ID or \
                               '_TMPDISK'        in tmpDQ2ID or \
                               '_TZERO'          in tmpDQ2ID:
                            continue
                        # check prefix
                        if tmpDQ2ID.startswith(tmpDQ2IDPrefix):
                            tmpN = tmpSitesData[tmpDQ2ID][0][columnName]
                            if tmpN is not None and tmpN > maxNumFile:
                                maxNumFile = tmpN
            # sum
            totalNum += maxNumFile
        # return
        return True,totalNum
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        return False,'%s:%s' % (errtype,errvalue) 


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
    if re.match("^[A-Za-z0-9][A-Za-z0-9\.\-\_/]{1,255}$",datasetName) is not None:
        return True
    return False


# get the list of sites in a cloud which cache a dataset
def getSitesWithCacheDS(cloudKey,excludedSites,siteMapper,datasetName):
    retList = []
    # check sites in the cloud
    for tmpSiteName in siteMapper.getCloud(cloudKey)['sites']:
        # excluded
        if tmpSiteName in excludedSites:
            continue
        # skip T1
        if tmpSiteName == siteMapper.getCloud(cloudKey)['source']:
            continue
        # hospital queue
        if siteMapper.getSite(tmpSiteName).ddm_input == siteMapper.getSite(siteMapper.getCloud(cloudKey)['source']).ddm_input:
            continue
        # not home cloud
        if siteMapper.getSite(tmpSiteName).cloud != cloudKey:
            continue
        # online
        if siteMapper.getSite(tmpSiteName).status != 'online':
            continue
        # check CVMFS
        if isCachedFile(datasetName,siteMapper.getSite(tmpSiteName)):
            retList.append(tmpSiteName)
    # return
    return retList


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
def getSitesShareDDM(siteMapper,siteName):
    # nonexistent site
    if not siteMapper.checkSite(siteName):
        return []
    # get siteSpec
    siteSpec = siteMapper.getSite(siteName)
    # loop over all sites
    retSites = []
    for tmpSiteName in siteMapper.siteSpecList:
        tmpSiteSpec = siteMapper.siteSpecList[tmpSiteName]
        # only same type
        if siteSpec.type != tmpSiteSpec.type:
            continue
        # only online sites
        if tmpSiteSpec.status != 'online':
            continue
        # same endpoint
        try:
            if siteSpec.ddm_input != tmpSiteSpec.ddm_input and \
                    siteSpec.ddm_output not in tmpSiteSpec.ddm_endpoints_input.all:
                continue
        except Exception:
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
        strList = ['InvalidRSEExpression','Details:']
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


# cleanup DN
def cleanupDN(realDN):
    tmpRealDN = re.sub('/CN=limited proxy','',realDN)
    tmpRealDN = re.sub('/CN=proxy','',tmpRealDN)
    return tmpRealDN
