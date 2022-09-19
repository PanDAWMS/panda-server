import re
import sys
import traceback
import time
import random
import datetime
import uuid
import functools
from pandaserver.brokerage import ErrorCode
from pandaserver.taskbuffer import ProcessGroups
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.DDM import rucioAPI
from pandaserver.config import panda_config

from pandacommon.pandalogger.PandaLogger import PandaLogger
_log = PandaLogger().getLogger('broker')

try:
    long
except NameError:
    long = int

# all known sites
_allSites = []


# non LRC checking
_disableLRCcheck = []


# processingType to skip brokerage
skipBrokerageProTypes = ['prod_test']

# comparison function for sort
def _compFunc(jobA,jobB):
    # append site if not in list
    if jobA.computingSite not in _allSites:
        _allSites.append(jobA.computingSite)
    if jobB.computingSite not in _allSites:
        _allSites.append(jobB.computingSite)
    # compare
    indexA = _allSites.index(jobA.computingSite)
    indexB = _allSites.index(jobB.computingSite)
    if indexA > indexB:
        return 1
    elif indexA < indexB:
        return -1
    else:
        return 0


# release checker
def _checkRelease(jobRels,siteRels):
    # all on/off
    if "True" in siteRels:
        return True
    if "False" in siteRels:
        return False
    # loop over all releases
    for tmpRel in jobRels.split('\n'):
        relVer = re.sub('^Atlas-','',tmpRel)
        # not available releases
        if relVer not in siteRels:
            return False
    return True


# get list of files which already exist at the site
def _getOkFiles(v_ce, v_files, allLFNs, allOkFilesMap, prodsourcelabel, job_label, tmpLog=None, allScopeList=None):

    scope_association_input, scope_association_output = select_scope(v_ce, prodsourcelabel, job_label)
    rucio_sites = list(v_ce.setokens_input[scope_association_input].values())
    try:
        rucio_sites.remove('')
    except Exception:
        pass
    rucio_sites.sort()
    if rucio_sites == []:
        rucio_site = v_ce.ddm_input[scope_association_input]
    else:
        rucio_site = ''
        for tmpID in rucio_sites:
            rucio_site += '%s,' % tmpID
        rucio_site = rucio_site[:-1]
    # set LFC and SE name
    rucio_url = 'rucio://atlas-rucio.cern.ch:/grid/atlas'
    tmpSE = v_ce.ddm_endpoints_input[scope_association_input].getAllEndPoints()
    if tmpLog is not None:
        tmpLog.debug('getOkFiles for %s with rucio_site:%s, rucio_url:%s, SE:%s' % (v_ce.sitename, rucio_site, rucio_url, str(tmpSE)))
    anyID = 'any'
    # use bulk lookup
    if allLFNs != []:
        # get all replicas
        if rucio_url not in allOkFilesMap:
            allOkFilesMap[rucio_url] = {}
            tmpStat,tmpAvaFiles = rucioAPI.listFileReplicas(allScopeList, allLFNs, tmpSE)
            if not tmpStat and tmpLog is not None:
                tmpLog.debug('getOkFile failed to get file replicas')
                tmpAvaFiles = {}
            allOkFilesMap[rucio_url][anyID] = tmpAvaFiles
        # get files for each rucio_site
        if rucio_site not in allOkFilesMap[rucio_url]:
            allOkFilesMap[rucio_url][rucio_site] = allOkFilesMap[rucio_url][anyID]
        # make return map
        retMap = {}
        for tmpLFN in v_files:
            if tmpLFN in allOkFilesMap[rucio_url][rucio_site]:
                retMap[tmpLFN] = allOkFilesMap[rucio_url][rucio_site][tmpLFN]
        tmpLog.debug('getOkFiles done')
        # return
        return retMap
    else:
        # old style
        tmpLog.debug('getOkFiles old')
        return {}


# check reprocessing or not
def _isReproJob(tmpJob):
    if tmpJob is not None:
        if tmpJob.processingType in ['reprocessing']:
            return True
        if tmpJob.transformation in ['csc_cosmics_trf.py','csc_BSreco_trf.py','BStoESDAODDPD_trf.py']:
            return True
    return False


# set 'ready' if files are already there
def _setReadyToFiles(tmpJob, okFiles, siteMapper, tmpLog):
    tmpLog.debug(str(okFiles))
    allOK = True
    tmpSiteSpec = siteMapper.getSite(tmpJob.computingSite)
    tmpSrcSpec  = siteMapper.getSite(siteMapper.getCloud(tmpJob.getCloud())['source'])
    scope_association_site_input, scope_association_site_output = select_scope(tmpSiteSpec, tmpJob.prodSourceLabel,
                                                                               tmpJob.job_label)
    scope_association_src_input, scope_association_src_output = select_scope(tmpSrcSpec, tmpJob.prodSourceLabel,
                                                                             tmpJob.job_label)
    tmpTapeEndPoints = tmpSiteSpec.ddm_endpoints_input[scope_association_site_input].getTapeEndPoints()
    # direct usage of remote SE
    if tmpSiteSpec.ddm_input[scope_association_site_input] != tmpSrcSpec.ddm_input[scope_association_src_input] \
            and tmpSrcSpec.ddm_input[scope_association_src_input] in tmpSiteSpec.setokens_input[scope_association_site_input].values():
        tmpSiteSpec = tmpSrcSpec
        tmpLog.debug('%s uses remote SiteSpec of %s for %s' % (tmpJob.PandaID,tmpSrcSpec.sitename,tmpJob.computingSite))
    for tmpFile in tmpJob.Files:
        if tmpFile.type == 'input':
            if tmpFile.status == 'ready':
                tmpFile.dispatchDBlock = 'NULL'
            elif DataServiceUtils.isCachedFile(tmpFile.dataset,tmpSiteSpec):
                # cached file
                tmpFile.status = 'cached'
                tmpFile.dispatchDBlock = 'NULL'
            elif tmpJob.computingSite == siteMapper.getCloud(tmpJob.getCloud())['source'] or \
                    tmpSiteSpec.ddm_input[scope_association_site_input] == tmpSrcSpec.ddm_input[scope_association_src_input]:
                # use DDM prestage only for on-tape files
                if len(tmpTapeEndPoints) > 0 and tmpFile.lfn in okFiles:
                    tapeOnly = True
                    tapeCopy = False
                    for tmpSE in okFiles[tmpFile.lfn]:
                        if tmpSE not in tmpTapeEndPoints:
                            tapeOnly = False
                        else:
                            # there is a tape copy
                            tapeCopy = True
                    # trigger prestage when disk copy doesn't exist or token is TAPE
                    if tapeOnly or (tapeCopy and tmpFile.dispatchDBlockToken in ['ATLASDATATAPE','ATLASMCTAPE']):
                        allOK = False
                    else:
                        # set ready
                        tmpFile.status = 'ready'
                        tmpFile.dispatchDBlock = 'NULL'
                else:
                    # set ready anyway even if LFC is down. i.e. okFiles doesn't contain the file
                    tmpFile.status = 'ready'
                    tmpFile.dispatchDBlock = 'NULL'
            else:
                # set ready if the file exists and the site doesn't use prestage
                tmpFile.status = 'ready'
                tmpFile.dispatchDBlock = 'NULL'
    # unset disp dataset
    if allOK:
        tmpJob.dispatchDBlock = 'NULL'



# check number/size of inputs
def _isTooManyInput(nFilesPerJob,inputSizePerJob):
    # the number of inputs is larger than 5 or
    # size of inputs is larger than 500MB
    if nFilesPerJob > 5 or inputSizePerJob > 500*1024*1024:
        return True
    return False


# send analysis brokerage info to logger
def sendMsgToLogger(message):
    _log.debug(message)


# send analysis brokerage info to logger with HTTP
def sendMsgToLoggerHTTP(msgList,job):
    try:
        # logging
        iMsg = 0
        # message type
        msgType = 'analy_brokerage'
        # make header
        if job.jobsetID not in [None,'NULL']:
            msgHead = "dn='%s' : jobset=%s jobdef=%s" % (job.prodUserName,job.jobsetID,job.jobDefinitionID)
        else:
            msgHead = "dn='%s' : jobdef=%s" % (job.prodUserName,job.jobDefinitionID)
        for msgBody in msgList:
            # make message
            message = msgHead + ' : ' + msgBody
            # dump locally
            _log.debug(message)
            # get logger
            _pandaLogger = PandaLogger()
            _pandaLogger.lock()
            _pandaLogger.setParams({'Type':msgType})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            logger.info(message)
            # release HTTP handler
            _pandaLogger.release()
            # sleep
            iMsg += 1
            if iMsg % 5 == 0:
                time.sleep(1)
    except Exception:
        errType,errValue = sys.exc_info()[:2]
        _log.error("sendMsgToLoggerHTTP : %s %s" % (errType,errValue))


# get T2 candidates when files are missing at T2
def getT2CandList(tmpJob,siteMapper,t2FilesMap):
    if tmpJob is None:
        return []
    # no cloud info
    if tmpJob.getCloud() not in t2FilesMap:
        return []
    # loop over all files
    tmpCandT2s = None
    for tmpFile in tmpJob.Files:
        if tmpFile.type == 'input' and tmpFile.status == 'missing':
            # no dataset info
            if tmpFile.dataset not in t2FilesMap[tmpJob.getCloud()]:
                return []
            # initial candidates
            if tmpCandT2s is None:
                tmpCandT2s = t2FilesMap[tmpJob.getCloud()][tmpFile.dataset]['sites']
            # check all candidates
            newCandT2s = []
            for tmpCandT2 in tmpCandT2s:
                # site doesn't have the dataset
                if tmpCandT2 not in t2FilesMap[tmpJob.getCloud()][tmpFile.dataset]['sites']:
                    continue
                # site has the file
                if tmpFile.lfn in t2FilesMap[tmpJob.getCloud()][tmpFile.dataset]['sites'][tmpCandT2]:
                    if tmpCandT2 not in newCandT2s:
                        newCandT2s.append(tmpCandT2)
            # set new candidates
            tmpCandT2s = newCandT2s
            if tmpCandT2s == []:
                break
    # return [] if no missing files
    if tmpCandT2s is None:
        return []
    # return
    tmpCandT2s.sort()
    return tmpCandT2s


# make compact dialog message
def makeCompactDiagMessage(header,results):
    # limit
    maxSiteList  = 5
    # types for compact format
    compactTypeList = ['status','cpucore']
    # message mapping
    messageMap = {'rel'          : 'missing rel/cache',
                  'pilot'        : 'no pilot',
                  'status'       : 'not online',
                  'disk'         : 'SE full',
                  'memory'       : 'RAM shortage',
                  'transferring' : 'many transferring',
                  'share'        : 'zero share',
                  'maxtime'      : 'short walltime',
                  'cpucore'      : 'CPU core mismatch',
                  'scratch'      : 'small scratch disk'
                  }
    # put header
    if header in ['',None]:
        retStr = 'No candidate - '
    else:
        retStr = 'special brokerage for %s - ' % header
    # count number of sites per type
    numTypeMap = {}
    for resultType in results:
        resultList = results[resultType]
        # ignore empty
        if len(resultList) == 0:
            continue
        # add
        nSites = len(resultList)
        if nSites not in numTypeMap:
            numTypeMap[nSites] = []
        numTypeMap[nSites].append(resultType)
    # sort
    numTypeKeys = list(numTypeMap)
    numTypeKeys.sort()
    # use compact format for largest one
    largeTypes = None
    if len(numTypeKeys) > 0:
        largeTypes = numTypeMap[numTypeKeys[-1]]
    # loop over all types
    for numTypeKey in numTypeKeys:
        for resultType in numTypeMap[numTypeKey]:
            # label
            if resultType in messageMap:
                retStr += '%s at ' % messageMap[resultType]
            else:
                retStr += '%s at' % resultType
            # use comact format or not
            if (resultType in compactTypeList+largeTypes \
               or len(results[resultType]) >= maxSiteList) \
               and header in ['',None,'reprocessing'] :
                if len(results[resultType]) == 1:
                    retStr += '%s site' % len(results[resultType])
                else:
                    retStr += '%s sites' % len(results[resultType])
            else:
                for tmpSite in results[resultType]:
                    retStr += '%s,' % tmpSite
                retStr = retStr[:-1]
            retStr += '. '
    retStr = retStr[:-2]
    # return
    return retStr


# message class
class MsgWrapper:
    def __init__(self):
        self.timestamp = datetime.datetime.utcnow().isoformat('/')

    def info(self,msg):
        _log.info(self.timestamp + ' ' + msg)

    def debug(self,msg):
        _log.debug(self.timestamp + ' ' + msg)

    def error(self,msg):
        _log.error(self.timestamp + ' ' + msg)

    def warning(self,msg):
        _log.warning(self.timestamp + ' ' + msg)



# schedule
def schedule(jobs,taskBuffer,siteMapper,forAnalysis=False,setScanSiteList=[],trustIS=False,
             distinguishedName=None,specialWeight={},getWeight=False,sizeMapForCheck={},
             datasetSize=0,replicaMap={},pd2pT1=False,reportLog=False,minPriority=None,
             t2FilesMap={},preferredCountries=[],siteReliability=None):
    # make a message instance
    tmpLog = MsgWrapper()
    try:
        tmpLog.debug('start %s %s %s %s minPrio=%s pref=%s siteRel=%s' % (forAnalysis,str(setScanSiteList),trustIS,
                                                                          distinguishedName,minPriority,
                                                                          str(preferredCountries),
                                                                          siteReliability))
        if specialWeight != {}:
            tmpLog.debug('PD2P weight : %s' % str(specialWeight))
        tmpLog.debug('replicaMap : %s' % str(replicaMap))
        # no jobs
        if len(jobs) == 0:
            tmpLog.debug('finished : no jobs')
            return
        allOkFilesMap = {}

        nJob  = 20
        iJob  = 0
        nFile = 20
        fileList  = []
        scopeList = []
        okFiles   = {}
        prioInterval = 50
        totalNumInputs = 0
        totalInputSize = 0
        chosen_ce      = None
        prodDBlock     = None
        computingSite  = None
        dispatchDBlock = None
        previousCloud  = None
        prevRelease    = None
        prevMemory     = None
        prevCmtConfig  = None
        prevProType    = None
        prevSourceLabel= None
        prevDiskCount  = None
        prevDirectAcc  = None
        prevCoreCount  = None
        prevIsJEDI     = None
        prevDDM        = None
        prevBrokergageSiteList = None
        prevManualPreset = None
        prevGoToT2Flag   = None
        prevWorkingGroup = None
        prevMaxCpuCount  = None
        prevBrokerageNote = None
        prevPriority      = None

        nWNmap = {}
        indexJob = 0

        diskThresholdT1   = 20 * 1024
        diskThresholdT2   = 200
        diskThresholdAna  = 200
        diskThresholdPD2P = 1024 * 3
        manyInputsThr     = 20
        weightUsedByBrokerage = {}
        prestageSites = []

        # check if only JEDI
        onlyJEDI = True
        for tmpJob in jobs:
            if tmpJob.lockedby != 'jedi':
                onlyJEDI = False
                break

        # get statistics
        fairsharePolicy = {}
        newJobStatWithPrio = {}
        jobStatBrokerClouds = {}
        jobStatBrokerCloudsWithPrio = {}
        hospitalQueueMap = {}
        if len(jobs) > 0 and (jobs[0].processingType.startswith('gangarobot') or \
                              jobs[0].processingType.startswith('hammercloud') or \
                              jobs[0].processingType in ['pandamover','usermerge'] or \
                              onlyJEDI):
            # disable redundant counting for HC
            jobStatistics = {}
            jobStatBroker = {}
            jobStatBrokerClouds = {}
            nRunningMap = {}

        else:
            jobStatistics = taskBuffer.getJobStatistics(forAnal=forAnalysis)
            if not forAnalysis:
                jobStatBroker = {}
                jobStatBrokerClouds = taskBuffer.getJobStatisticsBrokerage()
                fairsharePolicy = taskBuffer.getFairsharePolicy()
            else:
                if minPriority is None:
                    jobStatBroker = taskBuffer.getJobStatisticsAnalBrokerage()
                else:
                    jobStatBroker = taskBuffer.getJobStatisticsAnalBrokerage(minPriority=minPriority)
                nRunningMap   = taskBuffer.getnRunningInSiteData()
        # sort jobs by siteID. Some jobs may already define computingSite
        jobs = sorted(jobs, key=functools.cmp_to_key(_compFunc))
        # brokerage for analysis
        candidateForAnal = True
        relCloudMap      = {}
        loggerMessages   = []
        # get all input files for bulk LFC lookup
        allLFNs   = []
        allGUIDs  = []
        allScopes = []
        for tmpJob in jobs:
            if tmpJob.prodSourceLabel in ('test','managed') or tmpJob.prodUserName in ['gangarbt']:
                for tmpFile in tmpJob.Files:
                    if tmpFile.type == 'input' and tmpFile.lfn not in allLFNs:
                        allLFNs.append(tmpFile.lfn)
                        allGUIDs.append(tmpFile.GUID)
                        allScopes.append(tmpFile.scope)
        # loop over all jobs + terminator(None)
        for job in jobs+[None]:
            indexJob += 1
            # ignore failed jobs
            if job is None:
                pass
            elif job.jobStatus == 'failed':
                continue
            # list of sites for special brokerage
            specialBrokergageSiteList = []
            # note for brokerage
            brokerageNote = ''
            # send jobs to T2 when files are missing at T1
            goToT2Flag = False
            if job is not None and job.computingSite == 'NULL' and job.prodSourceLabel in ('test','managed') \
                   and specialBrokergageSiteList == []:
                currentT2CandList = getT2CandList(job,siteMapper,t2FilesMap)
                if currentT2CandList != []:
                    goToT2Flag = True
                    specialBrokergageSiteList = currentT2CandList
                    tmpLog.debug('PandaID:%s -> set SiteList=%s to use T2 for missing files at T1' % (job.PandaID,specialBrokergageSiteList))
                    brokerageNote = 'useT2'
            # set computingSite to T1 for high priority jobs
            if job is not None and job.currentPriority not in [None,'NULL'] and job.currentPriority >= 950 and job.computingSite == 'NULL' \
                   and job.prodSourceLabel in ('test','managed') and specialBrokergageSiteList == []:
                specialBrokergageSiteList = [siteMapper.getCloud(job.getCloud())['source']]
                # set site list to use T1 and T1_VL
                if job.getCloud() in hospitalQueueMap:
                    specialBrokergageSiteList += hospitalQueueMap[job.getCloud()]
                tmpLog.debug('PandaID:%s -> set SiteList=%s for high prio' % (job.PandaID,specialBrokergageSiteList))
                brokerageNote = 'highPrio'
            # use limited sites for MP jobs
            if job is not None and job.computingSite == 'NULL' and job.prodSourceLabel in ('test','managed') \
                   and job.coreCount not in [None,'NULL'] and job.coreCount > 1 and specialBrokergageSiteList == []:
                for tmpSiteName in siteMapper.getCloud(job.getCloud())['sites']:
                    if siteMapper.checkSite(tmpSiteName):
                        tmpSiteSpec = siteMapper.getSite(tmpSiteName)
                        if tmpSiteSpec.coreCount > 1:
                            specialBrokergageSiteList.append(tmpSiteName)
                tmpLog.debug('PandaID:%s -> set SiteList=%s for MP=%scores' % (job.PandaID,specialBrokergageSiteList,job.coreCount))
                brokerageNote = 'MP=%score' % job.coreCount
            # use limited sites for reprocessing
            if job is not None and job.computingSite == 'NULL' and job.prodSourceLabel in ('test','managed') \
                   and job.processingType in ['reprocessing'] and specialBrokergageSiteList == []:
                for tmpSiteName in siteMapper.getCloud(job.getCloud())['sites']:
                    if siteMapper.checkSite(tmpSiteName):
                        tmpSiteSpec = siteMapper.getSite(tmpSiteName)
                        if _checkRelease(job.AtlasRelease,tmpSiteSpec.validatedreleases):
                            specialBrokergageSiteList.append(tmpSiteName)
                tmpLog.debug('PandaID:%s -> set SiteList=%s for processingType=%s' % (job.PandaID,specialBrokergageSiteList,job.processingType))
                brokerageNote = '%s' % job.processingType
            # manually set site
            manualPreset = False
            if job is not None and job.computingSite != 'NULL' and job.prodSourceLabel in ('test','managed') \
                   and specialBrokergageSiteList == []:
                specialBrokergageSiteList = [job.computingSite]
                manualPreset = True
                brokerageNote = 'presetSite'
            overwriteSite = False
            # check JEDI
            isJEDI = False
            if job is not None and job.lockedby == 'jedi' and job.processingType != 'evtest':
                isJEDI = True
            # new bunch or terminator
            if job is None or len(fileList) >= nFile \
                   or (dispatchDBlock is None and job.homepackage.startswith('AnalysisTransforms')) \
                   or prodDBlock != job.prodDBlock or job.computingSite != computingSite or iJob > nJob \
                   or previousCloud != job.getCloud() or prevRelease != job.AtlasRelease \
                   or prevCmtConfig != job.cmtConfig \
                   or (prevProType in skipBrokerageProTypes and iJob > 0) \
                   or prevDirectAcc != job.transferType \
                   or (prevMemory != job.minRamCount and not isJEDI) \
                   or (prevDiskCount != job.maxDiskCount and not isJEDI) \
                   or prevCoreCount != job.coreCount \
                   or prevWorkingGroup != job.workingGroup \
                   or prevProType != job.processingType \
                   or (prevMaxCpuCount != job.maxCpuCount and not isJEDI) \
                   or prevBrokergageSiteList != specialBrokergageSiteList \
                   or prevIsJEDI != isJEDI \
                   or prevDDM != job.getDdmBackEnd():
                if indexJob > 1:
                    tmpLog.debug('new bunch')
                    tmpLog.debug('  iJob           %s'    % iJob)
                    tmpLog.debug('  cloud          %s' % previousCloud)
                    tmpLog.debug('  rel            %s' % prevRelease)
                    tmpLog.debug('  sourceLabel    %s' % prevSourceLabel)
                    tmpLog.debug('  cmtConfig      %s' % prevCmtConfig)
                    tmpLog.debug('  memory         %s' % prevMemory)
                    tmpLog.debug('  priority       %s' % prevPriority)
                    tmpLog.debug('  prodDBlock     %s' % prodDBlock)
                    tmpLog.debug('  computingSite  %s' % computingSite)
                    tmpLog.debug('  processingType %s' % prevProType)
                    tmpLog.debug('  workingGroup   %s' % prevWorkingGroup)
                    tmpLog.debug('  coreCount      %s' % prevCoreCount)
                    tmpLog.debug('  maxCpuCount    %s' % prevMaxCpuCount)
                    tmpLog.debug('  transferType   %s' % prevDirectAcc)
                    tmpLog.debug('  goToT2         %s' % prevGoToT2Flag)
                    tmpLog.debug('  DDM            %s' % prevDDM)
                # brokerage decisions
                resultsForAnal = {'rel': [], 'pilot': [], 'disk': [], 'status': [], 'weight': [], 'memory': [],
                                  'share': [], 'transferring': [], 'cpucore': [],
                                  'reliability': [], 'maxtime': [], 'scratch': []}
                # determine site
                if (iJob == 0 or chosen_ce != 'TOBEDONE') and prevBrokergageSiteList in [None,[]]:
                     # file scan for pre-assigned jobs
                     jobsInBunch = jobs[indexJob-iJob-1:indexJob-1]
                     if jobsInBunch != [] and fileList != [] and (computingSite not in prestageSites) \
                            and (jobsInBunch[0].prodSourceLabel in ['managed','software'] or \
                                 re.search('test',jobsInBunch[0].prodSourceLabel) is not None):
                         # get site spec
                         tmp_chosen_ce = siteMapper.getSite(computingSite)
                         # get files from LRC
                         okFiles = _getOkFiles(tmp_chosen_ce, fileList, allLFNs, allOkFilesMap,
                                               jobsInBunch[0].prodSourceLabel, jobsInBunch[0].job_label,
                                               tmpLog, allScopes)

                         nOkFiles = len(okFiles)
                         tmpLog.debug('site:%s - nFiles:%s/%s %s %s' % (computingSite,nOkFiles,len(fileList),str(fileList),str(okFiles)))
                         # loop over all jobs
                         for tmpJob in jobsInBunch:
                             # set 'ready' if files are already there
                             _setReadyToFiles(tmpJob,okFiles,siteMapper,tmpLog)
                else:
                    # load balancing
                    minSites = {}
                    nMinSites = 2
                    if prevBrokergageSiteList != []:
                        # special brokerage
                        scanSiteList = prevBrokergageSiteList
                    elif setScanSiteList == []:
                        if siteMapper.checkCloud(previousCloud):
                            # use cloud sites
                            scanSiteList = siteMapper.getCloud(previousCloud)['sites']
                        else:
                            # use default sites
                            scanSiteList = siteMapper.getCloud('default')['sites']
                    else:
                        # use given sites
                        scanSiteList = setScanSiteList

                    # the number/size of inputs per job
                    nFilesPerJob    = float(totalNumInputs)/float(iJob)
                    inputSizePerJob = float(totalInputSize)/float(iJob)
                    # use T1 for jobs with many inputs when weight is negative
                    if (not forAnalysis) and _isTooManyInput(nFilesPerJob,inputSizePerJob) and \
                           siteMapper.getCloud(previousCloud)['weight'] < 0 and prevManualPreset is False and \
                           (prevCoreCount not in ['NULL',None] and prevCoreCount > 1):
                        scanSiteList = [siteMapper.getCloud(previousCloud)['source']]
                        # set site list to use T1 and T1_VL
                        if previousCloud in hospitalQueueMap:
                            scanSiteList += hospitalQueueMap[previousCloud]

                    # found candidate
                    foundOneCandidate = False
                    # randomize the order
                    if forAnalysis:
                        random.shuffle(scanSiteList)
                    # get cnadidates
                    if True:
                        # loop over all sites
                        for site in scanSiteList:
                            tmpLog.debug('calculate weight for site:%s' % site)
                            # _allSites may conain NULL after sort()
                            if site == 'NULL':
                                continue
                            if prevIsJEDI:
                                winv = 1
                            else:
                                # get SiteSpec
                                if siteMapper.checkSite(site):
                                    tmpSiteSpec = siteMapper.getSite(site)
                                else:
                                    tmpLog.debug(" skip: %s doesn't exist in DB" % site)
                                    continue
                                # ignore test sites
                                if (prevManualPreset is False) and (site.endswith('test') or \
                                                                    site.endswith('Test') or site.startswith('Test')):
                                    continue
                                # ignore analysis queues
                                if (not forAnalysis) and (not tmpSiteSpec.runs_production()):
                                    continue
                                # check status
                                if tmpSiteSpec.status in ['offline', 'brokeroff'] and computingSite in ['NULL', None, '']:
                                    if forAnalysis and prevProType in ['hammercloud', 'gangarobot', 'gangarobot-squid']:
                                        # ignore site status for HC
                                        pass
                                    else:
                                        tmpLog.debug(' skip: status %s' % tmpSiteSpec.status)
                                        resultsForAnal['status'].append(site)
                                        continue
                                if tmpSiteSpec.status == 'test' and (prevProType not in ['prod_test','hammercloud','gangarobot','gangarobot-squid']) \
                                       and prevSourceLabel not in ['test','prod_test']:
                                    tmpLog.debug(' skip: status %s for %s' % (tmpSiteSpec.status,prevProType))
                                    resultsForAnal['status'].append(site)
                                    continue
                                tmpLog.debug('   status=%s' % tmpSiteSpec.status)
                                # check core count
                                if tmpSiteSpec.coreCount > 1:
                                    # use multi-core queue for MP jobs
                                    if prevCoreCount not in [None,'NULL'] and prevCoreCount > 1:
                                        pass
                                    else:
                                        tmpLog.debug('  skip: MP site (%s core) for job.coreCount=%s' % (tmpSiteSpec.coreCount,
                                                                                                       prevCoreCount))
                                        resultsForAnal['cpucore'].append(site)
                                        continue
                                else:
                                    # use single core for non-MP jobs
                                    if prevCoreCount not in [None,'NULL'] and prevCoreCount > 1:
                                        tmpLog.debug('  skip: single core site (%s core) for job.coreCount=%s' % (tmpSiteSpec.coreCount,
                                                                                                                prevCoreCount))
                                        resultsForAnal['cpucore'].append(site)
                                        continue
                                # check max memory
                                if tmpSiteSpec.memory != 0 and prevMemory not in [None,0,'NULL']:
                                    try:
                                        if int(tmpSiteSpec.memory) < int(prevMemory):
                                            tmpLog.debug('  skip: site memory shortage %s<%s' % (tmpSiteSpec.memory,prevMemory))
                                            resultsForAnal['memory'].append(site)
                                            continue
                                    except Exception:
                                        errtype,errvalue = sys.exc_info()[:2]
                                        tmpLog.error("max memory check : %s %s" % (errtype,errvalue))
                                # check maxcpucount
                                if tmpSiteSpec.maxtime != 0 and prevMaxCpuCount not in [None,0,'NULL']:
                                    try:
                                        if int(tmpSiteSpec.maxtime) < int(prevMaxCpuCount):
                                            tmpLog.debug('  skip: insufficient maxtime %s<%s' % (tmpSiteSpec.maxtime,prevMaxCpuCount))
                                            resultsForAnal['maxtime'].append(site)
                                            continue
                                    except Exception:
                                        errtype,errvalue = sys.exc_info()[:2]
                                        tmpLog.error("maxtime check : %s %s" % (errtype,errvalue))
                                if tmpSiteSpec.mintime != 0 and prevMaxCpuCount not in [None,0,'NULL']:
                                    try:
                                        if int(tmpSiteSpec.mintime) > int(prevMaxCpuCount):
                                            tmpLog.debug('  skip: insufficient job maxtime %s<%s' % (prevMaxCpuCount,tmpSiteSpec.mintime))
                                            resultsForAnal['maxtime'].append(site)
                                            continue
                                    except Exception:
                                        errtype,errvalue = sys.exc_info()[:2]
                                        tmpLog.error("mintime check : %s %s" % (errtype,errvalue))
                                # check max work dir size
                                if tmpSiteSpec.maxwdir != 0 and (prevDiskCount not in [None,0,'NULL']):
                                    try:
                                        if int(tmpSiteSpec.maxwdir) < int(prevDiskCount):
                                            tmpLog.debug('  skip: not enough disk %s<%s' % (tmpSiteSpec.maxwdir, prevDiskCount))
                                            resultsForAnal['scratch'].append(site)
                                            continue
                                    except Exception:
                                        errtype,errvalue = sys.exc_info()[:2]
                                        tmpLog.error("disk check : %s %s" % (errtype,errvalue))
                                tmpLog.debug('   maxwdir=%s' % tmpSiteSpec.maxwdir)
                                # reliability
                                if forAnalysis and isinstance(siteReliability, (int, long)):
                                    if tmpSiteSpec.reliabilityLevel is not None and tmpSiteSpec.reliabilityLevel > siteReliability:
                                        tmpLog.debug(' skip: insufficient reliability %s > %s' % (tmpSiteSpec.reliabilityLevel,siteReliability))
                                        resultsForAnal['reliability'].append(site)
                                        continue
                                # change NULL cmtconfig to slc3/4
                                if prevCmtConfig in ['NULL','',None]:
                                    if forAnalysis:
                                        tmpCmtConfig = 'i686-slc4-gcc34-opt'
                                    else:
                                        tmpCmtConfig = 'i686-slc3-gcc323-opt'
                                else:
                                    tmpCmtConfig = prevCmtConfig

                                # get pilot statistics
                                nPilotsGet = 0
                                nPilotsUpdate = 0
                                if nWNmap == {}:
                                    nWNmap = taskBuffer.getCurrentSiteData()
                                if site in nWNmap:
                                    nPilots = nWNmap[site]['getJob'] + nWNmap[site]['updateJob']
                                    nPilotsGet = nWNmap[site]['getJob']
                                    nPilotsUpdate = nWNmap[site]['updateJob']
                                elif site.split('/')[0] in nWNmap:
                                    tmpID = site.split('/')[0]
                                    nPilots = nWNmap[tmpID]['getJob'] + nWNmap[tmpID]['updateJob']
                                    nPilotsGet = nWNmap[tmpID]['getJob']
                                    nPilotsUpdate = nWNmap[tmpID]['updateJob']
                                else:
                                    nPilots = 0
                                tmpLog.debug(' original nPilots:%s get:%s update:%s' % (nPilots,nPilotsGet,nPilotsUpdate))
                                # limit on (G+1)/(U+1)
                                limitOnGUmax = 1.1
                                limitOnGUmin = 0.9
                                guRatio = float(1+nPilotsGet)/float(1+nPilotsUpdate)
                                if guRatio > limitOnGUmax:
                                    nPilotsGet = limitOnGUmax * float(1+nPilotsUpdate) - 1.0
                                elif guRatio < limitOnGUmin:
                                    nPilotsGet = limitOnGUmin * float(1+nPilotsUpdate) - 1.0
                                tmpLog.debug(' limited nPilots:%s get:%s update:%s' % (nPilots,nPilotsGet,nPilotsUpdate))
                                # if no pilots
                                if nPilots == 0 and nWNmap != {}:
                                    tmpLog.debug(" skip: %s no pilot" % site)
                                    resultsForAnal['pilot'].append(site)
                                    continue
                                # if no jobs in jobsActive/jobsDefined
                                jobStatistics.setdefault(site,
                                                         {'assigned':0,'activated':0,'running':0,'transferring':0})
                                # set nRunning
                                if forAnalysis:
                                    nRunningMap.setdefault(site, 0)
                                # check space
                                if specialWeight != {}:
                                    # for PD2P
                                    if site in sizeMapForCheck:
                                        # threshold for PD2P max(5%,3TB)
                                        thrForThisSite = long(sizeMapForCheck[site]['total'] * 5 / 100)
                                        if thrForThisSite < diskThresholdPD2P:
                                            thrForThisSite = diskThresholdPD2P
                                        remSpace = sizeMapForCheck[site]['total'] - sizeMapForCheck[site]['used']
                                        tmpLog.debug('   space available=%s remain=%s thr=%s' % (sizeMapForCheck[site]['total'],
                                                                                               remSpace,thrForThisSite))
                                        if remSpace-datasetSize < thrForThisSite:
                                            tmpLog.debug('  skip: disk shortage %s-%s< %s' % (remSpace,datasetSize,thrForThisSite))
                                            if getWeight:
                                                weightUsedByBrokerage[site] = "NA : disk shortage"
                                            continue
                                else:
                                    if tmpSiteSpec.space:
                                        # production
                                        if not forAnalysis:
                                            # take assigned/activated/running jobs into account for production
                                            nJobsIn  = float(jobStatistics[site]['assigned'])
                                            nJobsOut = float(jobStatistics[site]['activated']+jobStatistics[site]['running'])
                                            # get remaining space and threshold
                                            if site == siteMapper.getCloud(previousCloud)['source']:
                                                # T1
                                                remSpace = float(tmpSiteSpec.space) - 0.2 * nJobsOut
                                                remSpace = int(remSpace)
                                                diskThreshold = diskThresholdT1
                                            else:
                                                # T2
                                                remSpace = float(tmpSiteSpec.space) - 0.2 * nJobsOut - 2.0 * nJobsIn
                                                remSpace = int(remSpace)
                                                diskThreshold = diskThresholdT2
                                        else:
                                            # analysis
                                            remSpace = tmpSiteSpec.space
                                            diskThreshold = diskThresholdAna
                                        tmpLog.debug('   space available=%s remain=%s' % (tmpSiteSpec.space,remSpace))
                                        if remSpace < diskThreshold:
                                            tmpLog.debug('  skip: disk shortage < %s' % diskThreshold)
                                            resultsForAnal['disk'].append(site)
                                            # keep message to logger
                                            try:
                                                if prevSourceLabel in ['managed','test']:
                                                    # make message
                                                    message = '%s - disk %s < %s' % (site,remSpace,diskThreshold)
                                                    if message not in loggerMessages:
                                                        loggerMessages.append(message)
                                            except Exception:
                                                pass
                                            continue
                                # get the process group
                                tmpProGroup = ProcessGroups.getProcessGroup(prevProType)
                                if prevProType in skipBrokerageProTypes:
                                    # use original processingType since prod_test is in the test category and thus is interfered by validations
                                    tmpProGroup = prevProType
                                # production share
                                skipDueToShare = False
                                try:
                                    if not forAnalysis and prevSourceLabel in ['managed'] and site in fairsharePolicy:
                                        for tmpPolicy in fairsharePolicy[site]['policyList']:
                                            # ignore priority policy
                                            if tmpPolicy['priority'] is not None:
                                                continue
                                            # only zero share
                                            if tmpPolicy['share'] != '0%':
                                                continue
                                            # check group
                                            if tmpPolicy['group'] is not None:
                                                if '*' in tmpPolicy['group']:
                                                    # wildcard
                                                    tmpPatt = '^' + tmpPolicy['group'].replace('*','.*') + '$'
                                                    if re.search(tmpPatt,prevWorkingGroup) is None:
                                                        continue
                                                else:
                                                    # normal definition
                                                    if prevWorkingGroup != tmpPolicy['group']:
                                                        continue
                                            else:
                                                # catch all except WGs used by other policies
                                                groupInDefList = fairsharePolicy[site]['groupList']
                                                usedByAnother = False
                                                # loop over all groups
                                                for groupInDefItem in groupInDefList:
                                                    if '*' in groupInDefItem:
                                                        # wildcard
                                                        tmpPatt = '^' + groupInDefItem.replace('*','.*') + '$'
                                                        if re.search(tmpPatt,prevWorkingGroup) is not None:
                                                            usedByAnother = True
                                                            break
                                                    else:
                                                        # normal definition
                                                        if prevWorkingGroup == groupInDefItem:
                                                            usedByAnother = True
                                                            break
                                                if usedByAnother:
                                                    continue
                                            # check type
                                            if tmpPolicy['type'] is not None:
                                                if tmpPolicy['type'] == tmpProGroup:
                                                    skipDueToShare = True
                                                    break
                                            else:
                                                # catch all except PGs used by other policies
                                                typeInDefList  = fairsharePolicy[site]['typeList'][tmpPolicy['group']]
                                                usedByAnother = False
                                                for typeInDefItem in typeInDefList:
                                                    if typeInDefItem == tmpProGroup:
                                                        usedByAnother = True
                                                        break
                                                if not usedByAnother:
                                                    skipDueToShare = True
                                                    break
                                        # skip
                                        if skipDueToShare:
                                            tmpLog.debug(" skip: %s zero share" % site)
                                            resultsForAnal['share'].append(site)
                                            continue
                                except Exception:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error("share check : %s %s" % (errtype,errvalue))
                                # the number of assigned and activated
                                if not forAnalysis:
                                    jobStatBrokerClouds.setdefault(previousCloud, {})
                                    # use number of jobs in the cloud
                                    jobStatBroker = jobStatBrokerClouds[previousCloud]
                                if site not in jobStatBroker:
                                    jobStatBroker[site] = {}
                                if tmpProGroup not in jobStatBroker[site]:
                                    jobStatBroker[site][tmpProGroup] = {'assigned':0,'activated':0,'running':0,'transferring':0}
                                # count # of assigned and activated jobs for prod by taking priorities in to account
                                nRunJobsPerGroup = None
                                if not forAnalysis and prevSourceLabel in ['managed','test']:
                                    jobStatBrokerCloudsWithPrio.setdefault(prevPriority,
                                                                           taskBuffer.getJobStatisticsBrokerage(
                                                                               prevPriority,
                                                                               prevPriority+prioInterval))
                                    jobStatBrokerCloudsWithPrio[prevPriority].setdefault(previousCloud, {})
                                    jobStatBrokerCloudsWithPrio[prevPriority][previousCloud].setdefault(site, {})
                                    jobStatBrokerCloudsWithPrio[prevPriority][previousCloud][site].setdefault(
                                        tmpProGroup, {'assigned':0,'activated':0,'running':0,'transferring':0})
                                    nAssJobs = jobStatBrokerCloudsWithPrio[prevPriority][previousCloud][site][tmpProGroup]['assigned']
                                    nActJobs = jobStatBrokerCloudsWithPrio[prevPriority][previousCloud][site][tmpProGroup]['activated']
                                    nRunJobsPerGroup = jobStatBrokerCloudsWithPrio[prevPriority][previousCloud][site][tmpProGroup]['running']
                                    # add newly assigned jobs
                                    for tmpNewPriority in newJobStatWithPrio:
                                        if tmpNewPriority < prevPriority:
                                            continue
                                        if previousCloud not in newJobStatWithPrio[tmpNewPriority]:
                                            continue
                                        if site not in newJobStatWithPrio[tmpNewPriority][previousCloud]:
                                            continue
                                        if tmpProGroup not in newJobStatWithPrio[tmpNewPriority][previousCloud][site]:
                                            continue
                                        nAssJobs += newJobStatWithPrio[tmpNewPriority][previousCloud][site][tmpProGroup]
                                else:
                                    nAssJobs = jobStatBroker[site][tmpProGroup]['assigned']
                                    if forAnalysis and 'defined' in jobStatBroker[site][tmpProGroup]:
                                        nAssJobs += jobStatBroker[site][tmpProGroup]['defined']
                                    nActJobs = jobStatBroker[site][tmpProGroup]['activated']

                                # limit of the number of transferring jobs
                                if tmpSiteSpec.transferringlimit == 0:
                                    maxTransferring   = 2000
                                else:
                                    maxTransferring = tmpSiteSpec.transferringlimit
                                # get ration of transferring to running
                                if not forAnalysis and tmpSiteSpec.cloud not in ['ND']:
                                    nTraJobs = 0
                                    nRunJobs = 0
                                    for tmpGroupForTra in jobStatBroker[site]:
                                        tmpCountsForTra = jobStatBroker[site][tmpGroupForTra]
                                        if 'running' in tmpCountsForTra:
                                            nRunJobs += tmpCountsForTra['running']
                                        if 'transferring' in tmpCountsForTra:
                                            nTraJobs += tmpCountsForTra['transferring']
                                    tmpLog.debug('   running=%s transferring=%s max=%s' % (nRunJobs,nTraJobs,maxTransferring))
                                    if max(maxTransferring,2*nRunJobs) < nTraJobs:
                                        tmpLog.debug(" skip: %s many transferring=%s > max(%s,2*running=%s)" % (site,nTraJobs,maxTransferring,nRunJobs))
                                        resultsForAnal['transferring'].append(site)
                                        if prevSourceLabel in ['managed','test']:
                                            # make message
                                            message = '%s - too many transferring' % site
                                            if message not in loggerMessages:
                                                loggerMessages.append(message)
                                        continue
                                # get ratio of running jobs = run(cloud)/run(all) for multi cloud (disabled)
                                multiCloudFactor = 1

                                # calculate weight
                                if specialWeight != {}:
                                    if not pd2pT1:
                                        # weight for T2 PD2P
                                        nSubs = 1
                                        if site in specialWeight:
                                            nSubs = specialWeight[site]
                                        tmpLog.debug('   %s nSubs:%s assigned:%s activated:%s running:%s nWNsG:%s nWNsU:%s' % \
                                                   (site,nSubs,nAssJobs,nActJobs,nRunningMap[site],nPilotsGet,nPilotsUpdate))
                                        winv = float(nSubs) * float(nAssJobs+nActJobs) / float(1+nRunningMap[site]) / (1.0+float(nPilotsGet)/float(1+nPilotsUpdate))
                                        if getWeight:
                                            weightUsedByBrokerage[site] = "(1+%s/%s)*%s/%s/%s" % (nPilotsGet,1+nPilotsUpdate,1+nRunningMap[site],nAssJobs+nActJobs,nSubs)
                                    else:
                                        # weight for T1 PD2P
                                        tmpLog.debug('   %s MoU:%s' % (site,specialWeight[site]))
                                        winv = 1.0 / float(specialWeight[site])
                                        if getWeight:
                                            weightUsedByBrokerage[site] = "%s" % specialWeight[site]
                                else:
                                    if not forAnalysis:
                                        if nRunJobsPerGroup is None:
                                            tmpLog.debug('   %s assigned:%s activated:%s running:%s nPilotsGet:%s nPilotsUpdate:%s multiCloud:%s' %
                                                         (site,nAssJobs,nActJobs,jobStatistics[site]['running'],nPilotsGet,nPilotsUpdate,multiCloudFactor))
                                        else:
                                            tmpLog.debug('   %s assigned:%s activated:%s runningGroup:%s nPilotsGet:%s nPilotsUpdate:%s multiCloud:%s' %
                                                         (site,nAssJobs,nActJobs,nRunJobsPerGroup,nPilotsGet,nPilotsUpdate,multiCloudFactor))
                                    else:
                                        tmpLog.debug('   %s assigned:%s activated:%s running:%s nWNsG:%s nWNsU:%s' %
                                                   (site,nAssJobs,nActJobs,nRunningMap[site],nPilotsGet,nPilotsUpdate))
                                    if forAnalysis:
                                        winv = float(nAssJobs+nActJobs) / float(1+nRunningMap[site]) / (1.0+float(nPilotsGet)/float(1+nPilotsUpdate))
                                    else:
                                        if nRunJobsPerGroup is None:
                                            winv = float(nAssJobs+nActJobs) / float(1+jobStatistics[site]['running']) / (float(1+nPilotsGet)/float(1+nPilotsUpdate))
                                        else:
                                            winv = float(nAssJobs+nActJobs) / float(1+nRunJobsPerGroup) / (float(1+nPilotsGet)/float(1+nPilotsUpdate))
                                    winv *= float(multiCloudFactor)
                                    # send jobs to T1 when they require many or large inputs
                                    if _isTooManyInput(nFilesPerJob,inputSizePerJob):
                                        if site == siteMapper.getCloud(previousCloud)['source'] or \
                                           (previousCloud in hospitalQueueMap and site in hospitalQueueMap[previousCloud]):
                                            cloudT1Weight = 2.0
                                            # use weight in cloudconfig
                                            try:
                                                tmpCloudT1Weight = float(siteMapper.getCloud(previousCloud)['weight'])
                                                if tmpCloudT1Weight != 0.0:
                                                    cloudT1Weight = tmpCloudT1Weight
                                            except Exception:
                                                pass
                                            winv /= cloudT1Weight
                                            tmpLog.debug('   special weight for %s : nInputs/Job=%s inputSize/Job=%s weight=%s' %
                                                       (site,nFilesPerJob,inputSizePerJob,cloudT1Weight))
                            # found at least one candidate
                            foundOneCandidate = True
                            tmpLog.debug('Site:%s 1/Weight:%s' % (site, winv))
                            if forAnalysis and trustIS and reportLog:
                                resultsForAnal['weight'].append((site, '(1+%s/%s)*%s/%s' % (nPilotsGet, 1+nPilotsUpdate,
                                                                                            1+nRunningMap[site],
                                                                                            nAssJobs+nActJobs)))
                            # choose largest nMinSites weights
                            minSites[site] = winv
                            if len(minSites) > nMinSites:
                                maxSite = site
                                maxWinv = winv
                                for tmpSite in minSites:
                                    tmpWinv = minSites[tmpSite]
                                    if tmpWinv > maxWinv:
                                        maxSite = tmpSite
                                        maxWinv = tmpWinv
                                # delte max one
                                del minSites[maxSite]
                            # remove too different weights
                            if len(minSites) >= 2:
                                # look for minimum
                                minSite = list(minSites)[0]
                                minWinv = minSites[minSite]
                                for tmpSite in minSites:
                                    tmpWinv = minSites[tmpSite]
                                    if tmpWinv < minWinv:
                                        minSite = tmpSite
                                        minWinv = tmpWinv
                                # look for too different weights
                                difference = 2
                                removeSites = []
                                for tmpSite in minSites:
                                    tmpWinv = minSites[tmpSite]
                                    if tmpWinv > minWinv*difference:
                                        removeSites.append(tmpSite)
                                # remove
                                for tmpSite in removeSites:
                                    del minSites[tmpSite]
                    # set default
                    if len(minSites) == 0:
                        # cloud's list
                        if forAnalysis or siteMapper.checkCloud(previousCloud):
                            minSites[scanSiteList[0]] = 0
                        else:
                            minSites[panda_config.def_sitename] = 0
                        # release not found
                        if forAnalysis and trustIS:
                            candidateForAnal = False
                    # use only one site for prod_test to skip LFC scan
                    if prevProType in skipBrokerageProTypes:
                        if len(minSites) > 1:
                            minSites = {list(minSites)[0]:0}
                    # choose site
                    tmpLog.debug('Min Sites:%s' % minSites)
                    if len(fileList) ==0 or prevIsJEDI is True:
                        # choose min 1/weight
                        minSite = list(minSites)[0]
                        minWinv = minSites[minSite]
                        for tmpSite in minSites:
                            tmpWinv = minSites[tmpSite]
                            if tmpWinv < minWinv:
                                minSite = tmpSite
                                minWinv = tmpWinv
                        chosenCE = siteMapper.getSite(minSite)
                    else:
                        # compare # of files in LRC
                        maxNfiles = -1
                        for site in minSites:
                            tmp_chosen_ce = siteMapper.getSite(site)
                            # search LRC
                            if site in _disableLRCcheck:
                                tmpOKFiles = {}
                            else:
                                # get files from LRC
                                tmpOKFiles = _getOkFiles(tmp_chosen_ce, fileList, allLFNs, allOkFilesMap,
                                                         job.proSourceLabel, job.job_label, tmpLog, allScopes)
                            nFiles = len(tmpOKFiles)
                            tmpLog.debug('site:%s - nFiles:%s/%s %s' % (site,nFiles,len(fileList),str(tmpOKFiles)))
                            # choose site holding max # of files
                            if nFiles > maxNfiles:
                                chosenCE = tmp_chosen_ce
                                maxNfiles = nFiles
                                okFiles = tmpOKFiles
                    # set job spec
                    tmpLog.debug('indexJob      : %s' % indexJob)
                    tmpLog.debug('nInputs/Job   : %s' % nFilesPerJob)
                    tmpLog.debug('inputSize/Job : %s' % inputSizePerJob)
                    for tmpJob in jobs[indexJob-iJob-1:indexJob-1]:
                        # set computingSite
                        if (not candidateForAnal) and forAnalysis and trustIS:
                            resultsForAnalStr = 'ERROR : No candidate. '
                            if resultsForAnal['rel'] != []:
                                if prevCmtConfig in ['','NULL',None]:
                                    resultsForAnalStr += 'Release:%s was not found at %s. ' % (prevRelease,str(resultsForAnal['rel']))
                                else:
                                    resultsForAnalStr += 'Release:%s/%s was not found at %s. ' % (prevRelease,prevCmtConfig,str(resultsForAnal['rel']))
                            if resultsForAnal['pilot'] != []:
                                resultsForAnalStr += '%s are inactive (no pilots for last 3 hours). ' % str(resultsForAnal['pilot'])
                            if resultsForAnal['disk'] != []:
                                resultsForAnalStr += 'Disk shortage < %sGB at %s. ' % (diskThresholdAna,str(resultsForAnal['disk']))
                            if resultsForAnal['memory'] != []:
                                resultsForAnalStr += 'Insufficient RAM at %s. ' % str(resultsForAnal['memory'])
                            if resultsForAnal['maxtime'] != []:
                                resultsForAnalStr += 'Shorter walltime limit than maxCpuCount:%s at ' % prevMaxCpuCount
                                for tmpItem in resultsForAnal['maxtime']:
                                    if siteMapper.checkSite(tmpItem):
                                        resultsForAnalStr += '%s:%s,' % (tmpItem,siteMapper.getSite(tmpItem).maxtime)
                                resultsForAnalStr = resultsForAnalStr[:-1]
                                resultsForAnalStr += '. '
                            if resultsForAnal['status'] != []:
                                resultsForAnalStr += '%s are not online. ' % str(resultsForAnal['status'])
                            if resultsForAnal['reliability'] != []:
                                resultsForAnalStr += 'Insufficient reliability at %s. ' % str(resultsForAnal['reliability'])
                            resultsForAnalStr = resultsForAnalStr[:-1]
                            tmpJob.computingSite = resultsForAnalStr
                        else:
                            tmpJob.computingSite = chosenCE.sitename
                        tmpLog.debug('PandaID:%s -> site:%s' % (tmpJob.PandaID,tmpJob.computingSite))

                        # fail jobs if no sites have the release
                        if ((tmpJob.relocationFlag != 1 and not foundOneCandidate)) and (tmpJob.prodSourceLabel in ['managed','test']):
                            # reset
                            if tmpJob.relocationFlag not in [1,2]:
                                tmpJob.computingSite = None
                                tmpJob.computingElement = None
                            # go to waiting
                            tmpJob.jobStatus          = 'waiting'
                            tmpJob.brokerageErrorCode = ErrorCode.EC_Release
                            if tmpJob.relocationFlag in [1,2]:
                                try:
                                    if resultsForAnal['pilot'] != []:
                                        tmpJob.brokerageErrorDiag = '%s no pilots' % tmpJob.computingSite
                                    elif resultsForAnal['disk'] != []:
                                        tmpJob.brokerageErrorDiag = 'SE full at %s' % tmpJob.computingSite
                                    elif resultsForAnal['memory'] != []:
                                        tmpJob.brokerageErrorDiag = 'RAM shortage at %s' % tmpJob.computingSite
                                    elif resultsForAnal['status'] != []:
                                        tmpJob.brokerageErrorDiag = '%s not online' % tmpJob.computingSite
                                    elif resultsForAnal['share'] != []:
                                        tmpJob.brokerageErrorDiag = '%s zero share' % tmpJob.computingSite
                                    elif resultsForAnal['cpucore'] != []:
                                        tmpJob.brokerageErrorDiag = "CPU core mismatch at %s" % tmpJob.computingSite
                                    elif resultsForAnal['maxtime'] != []:
                                        tmpJob.brokerageErrorDiag = "short walltime at %s" % tmpJob.computingSite
                                    elif resultsForAnal['transferring'] != []:
                                        tmpJob.brokerageErrorDiag = 'too many transferring at %s' % tmpJob.computingSite
                                    elif resultsForAnal['scratch'] != []:
                                        tmpJob.brokerageErrorDiag = 'small scratch disk at %s' % tmpJob.computingSite
                                    else:
                                        tmpJob.brokerageErrorDiag = '%s/%s not found at %s' % (tmpJob.AtlasRelease,tmpJob.cmtConfig,tmpJob.computingSite)
                                except Exception:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error("failed to set diag for %s: %s %s" % (tmpJob.PandaID,errtype,errvalue))
                                    tmpJob.brokerageErrorDiag = 'failed to set diag. see brokerage log in the panda server'
                            elif prevBrokergageSiteList not in [[],None]:
                                try:
                                    # make message
                                    tmpJob.brokerageErrorDiag = makeCompactDiagMessage(prevBrokerageNote,resultsForAnal)
                                except Exception:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error("failed to set special diag for %s: %s %s" % (tmpJob.PandaID,errtype,errvalue))
                                    tmpJob.brokerageErrorDiag = 'failed to set diag. see brokerage log in the panda server'
                            elif prevProType in ['reprocessing']:
                                tmpJob.brokerageErrorDiag = '%s/%s not found at reprocessing sites' % (tmpJob.homepackage,tmpJob.cmtConfig)
                            else:
                                try:
                                    tmpJob.brokerageErrorDiag = makeCompactDiagMessage('',resultsForAnal)
                                except Exception:
                                    errtype,errvalue = sys.exc_info()[:2]
                                    tmpLog.error("failed to set compact diag for %s: %s %s" % (tmpJob.PandaID,errtype,errvalue))
                                    tmpJob.brokerageErrorDiag = 'failed to set diag. see brokerage log in the panda server'
                            tmpLog.debug('PandaID:%s %s' % (tmpJob.PandaID,tmpJob.brokerageErrorDiag))
                            continue
                        # set ready if files are already there
                        if prevIsJEDI is False:
                            _setReadyToFiles(tmpJob,okFiles,siteMapper,tmpLog)
                        # update statistics
                        tmpProGroup = ProcessGroups.getProcessGroup(tmpJob.processingType)
                        if tmpJob.processingType in skipBrokerageProTypes:
                            # use original processingType since prod_test is in the test category and thus is interfered by validations
                            tmpProGroup = tmpJob.processingType
                        jobStatistics.setdefault(tmpJob.computingSite, {'assigned':0,'activated':0,'running':0})
                        jobStatBroker.setdefault(tmpJob.computingSite, {})
                        jobStatBroker[tmpJob.computingSite].setdefault(tmpProGroup,
                                                                       {'assigned':0,'activated':0,'running':0})
                        jobStatistics[tmpJob.computingSite]['assigned'] += 1
                        jobStatBroker[tmpJob.computingSite][tmpProGroup]['assigned'] += 1
                        # update statistics by taking priorities into account
                        if not forAnalysis and prevSourceLabel in ['managed','test']:
                            newJobStatWithPrio.setdefault(prevPriority, {})
                            newJobStatWithPrio[prevPriority].setdefault(tmpJob.getCloud(), {})
                            newJobStatWithPrio[prevPriority][tmpJob.getCloud()].setdefault(tmpJob.computingSite, {})
                            newJobStatWithPrio[prevPriority][tmpJob.getCloud()][tmpJob.computingSite].setdefault(
                                tmpProGroup, 0)
                            newJobStatWithPrio[prevPriority][tmpJob.getCloud()][tmpJob.computingSite][tmpProGroup] += 1
                # terminate
                if job is None:
                    break
                # reset iJob
                iJob = 0
                # reset file list
                fileList  = []
                scopeList = []
                okFiles   = {}
                totalNumInputs = 0
                totalInputSize = 0
                # create new dispDBlock
                if job.prodDBlock != 'NULL':
                    # get datatype
                    try:
                        tmpDataType = job.prodDBlock.split('.')[-2]
                    except Exception:
                        # default
                        tmpDataType = 'GEN'
                    if len(tmpDataType) > 20:
                        # avoid too long name
                        tmpDataType = 'GEN'
                    transferType = 'transfer'
                    if job.useInputPrestaging():
                        transferType = 'prestaging'
                    dispatchDBlock = "panda.%s.%s.%s.%s.%s_dis%s" % (job.taskID, time.strftime('%m.%d'),
                                                                     tmpDataType, transferType,
                                                                     str(uuid.uuid4()), job.PandaID)
                    tmpLog.debug('New dispatchDBlock: %s' % dispatchDBlock)
                prodDBlock = job.prodDBlock
                # already define computingSite
                if job.computingSite != 'NULL':
                    # instantiate KnownSite
                    chosen_ce = siteMapper.getSite(job.computingSite)
                    # if site doesn't exist, use the default site
                    if job.homepackage.startswith('AnalysisTransforms'):
                        if chosen_ce.sitename == panda_config.def_sitename:
                            chosen_ce = siteMapper.getSite(panda_config.def_queue)
                            overwriteSite = True
                else:
                    # default for Analysis jobs
                    if job.homepackage.startswith('AnalysisTransforms'):
                        chosen_ce = siteMapper.getSite(panda_config.def_queue)
                        overwriteSite = True
                    else:
                        # set chosen_ce
                        chosen_ce = 'TOBEDONE'
            # increment iJob
            iJob += 1
            # reserve computingSite and cloud
            computingSite = job.computingSite
            previousCloud = job.getCloud()
            prevRelease = job.AtlasRelease
            prevMemory = job.minRamCount
            prevCmtConfig = job.cmtConfig
            prevProType = job.processingType
            prevSourceLabel = job.prodSourceLabel
            prevDiskCount = job.maxDiskCount
            prevDirectAcc = job.transferType
            prevCoreCount = job.coreCount
            prevMaxCpuCount = job.maxCpuCount
            prevBrokergageSiteList = specialBrokergageSiteList
            prevManualPreset = manualPreset
            prevGoToT2Flag = goToT2Flag
            prevWorkingGroup = job.workingGroup
            prevBrokerageNote = brokerageNote
            prevIsJEDI = isJEDI
            prevDDM = job.getDdmBackEnd()
            # truncate prio to avoid too many lookups
            if job.currentPriority not in [None,'NULL']:
                prevPriority = (job.currentPriority / prioInterval) * prioInterval
            # assign site
            if chosen_ce != 'TOBEDONE':
                job.computingSite = chosen_ce.sitename
                if job.computingElement == 'NULL':
                    if job.prodSourceLabel == 'ddm':
                        # use nickname for ddm jobs
                        job.computingElement = chosen_ce.nickname

                # update statistics
                jobStatistics.setdefault(job.computingSite, {'assigned':0,'activated':0,'running':0})
                jobStatistics[job.computingSite]['assigned'] += 1
                tmpLog.debug('PandaID:%s -> preset site:%s' % (job.PandaID,chosen_ce.sitename))
                # set cloud
                if job.cloud in ['NULL',None,'']:
                    job.cloud = chosen_ce.cloud
            # set destinationSE
            destSE = job.destinationSE
            if siteMapper.checkCloud(job.getCloud()):
                # use cloud dest for non-exsiting sites
                if job.prodSourceLabel != 'user' and job.destinationSE not in siteMapper.siteSpecList \
                       and job.destinationSE != 'local':
                    if DataServiceUtils.checkJobDestinationSE(job) is not None:
                        destSE = DataServiceUtils.checkJobDestinationSE(job)
                    else:
                        destSE = siteMapper.getCloud(job.getCloud())['dest']
                    job.destinationSE = destSE

            if overwriteSite:
                # overwrite SE for analysis jobs which set non-existing sites
                destSE = job.computingSite
                job.destinationSE = destSE
            # set dispatchDBlock and destinationSE
            first = True
            for file in job.Files:
                # dispatchDBlock. Set dispDB for prestaging jobs too
                if file.type == 'input' and file.dispatchDBlock == 'NULL' and \
                   ((file.status not in ['ready', 'missing', 'cached']) or job.computingSite in prestageSites):
                    if first:
                        first = False
                        job.dispatchDBlock = dispatchDBlock
                    file.dispatchDBlock = dispatchDBlock
                    file.status = 'pending'
                    if file.lfn not in fileList:
                        fileList.append(file.lfn)
                        scopeList.append(file.scope)
                        try:
                            # get total number/size of inputs except DBRelease
                            # tgz inputs for evgen may be negligible
                            if re.search('\.tar\.gz', file.lfn) is None:
                                totalNumInputs += 1
                                totalInputSize += file.fsize
                        except Exception:
                            pass
                # destinationSE
                if file.type in ['output','log'] and destSE != '':
                    if job.prodSourceLabel == 'user' and job.computingSite == file.destinationSE:
                        pass
                    elif job.prodSourceLabel == 'user' and prevIsJEDI is True and file.destinationSE not in ['','NULL']:
                        pass
                    elif destSE == 'local':
                        pass
                    elif DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is not None:
                        pass
                    else:
                        file.destinationSE = destSE
                # pre-assign GUID to log
                if file.type == 'log':
                    # generate GUID
                    file.GUID = str(uuid.uuid4())
        # send log messages
        try:
            for  message in loggerMessages:
                # get logger
                _pandaLogger = PandaLogger()
                _pandaLogger.lock()
                _pandaLogger.setParams({'Type':'brokerage'})
                logger = _pandaLogger.getHttpLogger(panda_config.loggername)
                # add message
                logger.warning(message)
                # release HTTP handler
                _pandaLogger.release()
                time.sleep(1)
        except Exception:
            pass
        # send analysis brokerage info when jobs are submitted
        if len(jobs) > 0 and jobs[0] is not None and not forAnalysis and not pd2pT1 and specialWeight=={}:
            # for analysis job. FIXME once ganga is updated to send analy brokerage info
            if jobs[0].prodSourceLabel in ['user','panda'] and jobs[0].processingType in ['pathena','prun']:
                # send countryGroup
                tmpMsgList = []
                tmpNumJobs = len(jobs)
                if jobs[0].prodSourceLabel == 'panda':
                    tmpNumJobs -= 1
                tmpMsg = 'nJobs=%s ' % tmpNumJobs
                if jobs[0].countryGroup in ['NULL', '', None]:
                    tmpMsg += 'countryGroup=None'
                else:
                    tmpMsg += 'countryGroup=%s' % jobs[0].countryGroup
                tmpMsgList.append(tmpMsg)
                # send log
                sendMsgToLoggerHTTP(tmpMsgList, jobs[0])
        # finished
        tmpLog.debug('N lookup for prio : {0}'.format(len(jobStatBrokerCloudsWithPrio)))
        tmpLog.debug('finished')
        if getWeight:
            return weightUsedByBrokerage
    except Exception as e:
        tmpLog.error("schedule : %s %s" % (str(e), traceback.format_exc()))
        if getWeight:
            return {}
