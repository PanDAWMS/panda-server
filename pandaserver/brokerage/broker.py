import re
import sys
import time
import fcntl
import commands
import ErrorCode
import broker_util
import PandaSiteIDs
from taskbuffer import ProcessGroups
from config import panda_config

from pandalogger.PandaLogger import PandaLogger
_log = PandaLogger().getLogger('broker')

# all known sites
_allSites = PandaSiteIDs.PandaSiteIDs.keys()
        
# sites for prestaging
prestageSites = ['BNL_ATLAS_test','BNL_ATLAS_1','BNL_ATLAS_2']

# non LRC checking
_disableLRCcheck = []

# lock for uuidgen
_lockGetUU   = open(panda_config.lockfile_getUU, 'w')


# comparison function for sort
def _compFunc(jobA,jobB):
    # append site if not in list
    if not jobA.computingSite in _allSites:
        _allSites.append(jobA.computingSite)
    if not jobB.computingSite in _allSites:
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
    # loop over all releases
    for tmpRel in jobRels.split('\n'):
        relVer = re.sub('^Atlas-','',tmpRel)
        # not available releases
        if not relVer in siteRels:
            return False
    return True


# get list of files which already exist at the site
def _getOkFiles(v_ce,v_files,v_guids):
    # DQ2 URL
    dq2URL = v_ce.dq2url
    dq2ID  = v_ce.ddm
    # set LFC and SE name 
    tmpSE = []
    if not v_ce.lfchost in [None,'']:
        dq2URL = 'lfc://'+v_ce.lfchost+':/grid/atlas/'
        tmpSE  = broker_util.getSEfromSched(v_ce.se)
    # get files from LRC 
    return broker_util.getFilesFromLRC(v_files,dq2URL,guids=v_guids,
                                       storageName=tmpSE,getPFN=True)


# check reprocessing or not
def _isReproJob(tmpJob):
    if tmpJob != None:
        if tmpJob.processingType in ['reprocessing']:
            return True
        if tmpJob.transformation in ['csc_cosmics_trf.py','csc_BSreco_trf.py','BStoESDAODDPD_trf.py']:
            return True
    return False

    
# set 'ready' if files are already there
def _setReadyToFiles(tmpJob,okFiles,siteMapper):
    allOK = True
    tmpSiteSpec = siteMapper.getSite(tmpJob.computingSite)
    tmpSrcSpec  = siteMapper.getSite(siteMapper.getCloud(tmpJob.cloud)['source'])
    _log.debug(tmpSiteSpec.seprodpath)
    for tmpFile in tmpJob.Files:
        if tmpFile.type == 'input':
            if (tmpJob.computingSite.endswith('_REPRO') or tmpJob.computingSite == siteMapper.getCloud(tmpJob.cloud)['source'] \
                or tmpSiteSpec.ddm == tmpSrcSpec.ddm) \
                   and (not tmpJob.computingSite in prestageSites):
                # EGEE T1. use DQ2 prestage only for on-tape files
                if tmpSiteSpec.seprodpath.has_key('ATLASDATATAPE') and tmpSiteSpec.seprodpath.has_key('ATLASMCTAPE') and \
                       okFiles.has_key(tmpFile.lfn):
                    tapeOnly = True
                    tapeCopy = False
                    for okPFN in okFiles[tmpFile.lfn]:
                        if re.search(tmpSiteSpec.seprodpath['ATLASDATATAPE'],okPFN) == None and \
                               re.search(tmpSiteSpec.seprodpath['ATLASMCTAPE'],okPFN) == None:
                            # there is a disk copy
                            if tmpJob.cloud == 'US':
                                # check for BNLPANDA
                                if re.search(tmpSiteSpec.seprodpath['ATLASMCDISK'],okPFN) != None or \
                                       re.search(tmpSiteSpec.seprodpath['ATLASDATADISK'],okPFN) != None:
                                    tapeOnly = False
                            else:
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
            elif ((tmpFile.lfn in okFiles) or (tmpJob.computingSite == tmpJob.destinationSE)) \
                     and (not tmpJob.computingSite in prestageSites):
                # set ready if the file exists and the site doesn't use prestage
                tmpFile.status = 'ready'
                tmpFile.dispatchDBlock = 'NULL'                                
            else:
                # prestage with PandaMover
                allOK = False
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


# schedule
def schedule(jobs,taskBuffer,siteMapper,forAnalysis=False,setScanSiteList=[],trustIS=False):
    _log.debug('start %s %s %s' % (forAnalysis,str(setScanSiteList),trustIS))
    # no jobs
    if len(jobs) == 0:
        _log.debug('finished : no jobs')        
        return
    nJob  = 20
    iJob  = 0
    nFile = 20
    fileList = []
    guidList = []
    okFiles = {}
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
    
    nWNmap = {}
    indexJob = 0
    vomsOK = None

    diskThreshold = 200
    manyInputsThr = 20
    
    try:
        # get statistics
        jobStatistics = taskBuffer.getJobStatistics()
        jobStatBroker = taskBuffer.getJobStatisticsBrokerage()        
        # sort jobs by siteID. Some jobs may already define computingSite
        jobs.sort(_compFunc)
        # brokerage for analysis 
        candidateForAnal = True
        resultsForAnal   = {'rel':[],'pilot':[],'disk':[]}
        relCloudMap      = {}
        loggerMessages   = []
        # loop over all jobs + terminator(None)
        for job in jobs+[None]:
            indexJob += 1
            # ignore failed jobs
            if job == None:
                pass
            elif job.jobStatus == 'failed':
                continue
            # set computingSite to T1 for high priority jobs
            if job != None and job.currentPriority >= 950 and job.computingSite == 'NULL' \
                   and job.prodSourceLabel in ('test','managed'):
                # FIXME : just for slc5-gcc43 validation
                #job.computingSite = siteMapper.getCloud(job.cloud)['source']                
                if job.cmtConfig != 'i686-slc5-gcc43-opt': 
                    job.computingSite = siteMapper.getCloud(job.cloud)['source']
            # set computingSite to T1 when too many inputs are required
            if job != None and job.computingSite == 'NULL' and job.prodSourceLabel in ('test','managed'):
                # counts # of inputs
                tmpTotalInput = 0
                for tmpFile in job.Files:
                    if tmpFile.type == 'input':
                        tmpTotalInput += 1
                if tmpTotalInput >= manyInputsThr:
                    # FIXME : just for slc5-gcc43 validation
                    #job.computingSite = siteMapper.getCloud(job.cloud)['source']
                    if job.cmtConfig != 'i686-slc5-gcc43-opt': 
                        job.computingSite = siteMapper.getCloud(job.cloud)['source']
            # FIXME : send UAT jobs to long
            try:
                if job != None and job.prodSourceLabel in ['user','panda']:
                    if jobs[-1].prodDBlock != None and jobs[-1].prodDBlock.startswith('uat09.') and job.computingSite == 'ANALY_BNL_ATLAS_1':
                        job.computingSite = 'ANALY_LONG_BNL_ATLAS'
            except:
                pass
            overwriteSite = False
            # new bunch or terminator
            if job == None or len(fileList) >= nFile \
                   or (dispatchDBlock == None and job.homepackage.startswith('AnalysisTransforms')) \
                   or prodDBlock != job.prodDBlock or job.computingSite != computingSite or iJob > nJob \
                   or previousCloud != job.cloud or prevRelease != job.AtlasRelease \
                   or prevCmtConfig != job.cmtConfig \
                   or (computingSite in ['RAL_REPRO','INFN-T1_REPRO'] and len(fileList)>=2):
                if indexJob > 1:
                    _log.debug('new bunch')
                    _log.debug('  iJob           %s'    % iJob)
                    _log.debug('  cloud          %s' % previousCloud)
                    _log.debug('  rel            %s' % prevRelease)
                    _log.debug('  sourceLabel    %s' % prevSourceLabel)
                    _log.debug('  cmtConfig      %s' % prevCmtConfig)
                    _log.debug('  prodDBlock     %s' % prodDBlock)
                    _log.debug('  computingSite  %s' % computingSite)
                    _log.debug('  processingType %s' % prevProType)
                # determine site
                if iJob == 0 or chosen_ce != 'TOBEDONE':
                     # file scan for pre-assigned jobs
                     jobsInBunch = jobs[indexJob-iJob-1:indexJob-1]
                     if jobsInBunch != [] and fileList != [] and (not computingSite in prestageSites) \
                            and (jobsInBunch[0].prodSourceLabel in ['managed','software'] or \
                                 re.search('test',jobsInBunch[0].prodSourceLabel) != None):
                         # get site spec
                         tmp_chosen_ce = siteMapper.getSite(computingSite)
                         # get files from LRC 
                         okFiles = _getOkFiles(tmp_chosen_ce,fileList,guidList)
                         # loop over all jobs
                         for tmpJob in jobsInBunch:
                             # set 'ready' if files are already there
                             _setReadyToFiles(tmpJob,okFiles,siteMapper)
                else:
                    # load balancing
                    minSites = {}
                    nMinSites = 2
                    if setScanSiteList == []:
                        if siteMapper.checkCloud(previousCloud):
                            # use cloud sites                    
                            scanSiteList = siteMapper.getCloud(previousCloud)['sites']
                        else:
                            # use default sites
                            scanSiteList = siteMapper.getCloud('default')['sites']
                    else:
                        # use given sites
                        scanSiteList = setScanSiteList
                    # get availabe sites with cache
                    useCacheVersion = False
                    siteListWithCache = []
                    if forAnalysis and re.search('-\d+\.\d+\.\d+\.\d+',prevRelease) != None:
                        useCacheVersion = True
                        siteListWithCache = taskBuffer.checkSitesWithRelease(scanSiteList,caches=prevRelease)
                        _log.debug('  cacheSites     %s' % str(siteListWithCache))
                    # release/cmtconfig check
                    foundRelease   = False
                    # the number/size of inputs per job 
                    nFilesPerJob    = float(totalNumInputs)/float(iJob)
                    inputSizePerJob = float(totalInputSize)/float(iJob)
                    # use T1 for jobs with many inputs when weight is negative
                    if (not forAnalysis) and _isTooManyInput(nFilesPerJob,inputSizePerJob) and \
                           siteMapper.getCloud(previousCloud)['weight'] < 0:
                        minSites[siteMapper.getCloud(previousCloud)['source']] = 0
                        foundRelease = True
                    else:
                        # loop over all sites    
                        for site in scanSiteList:
                            _log.debug('calculate weight for site:%s' % site)                    
                            # _allSites may conain NULL after sort()
                            if site == 'NULL':
                                continue
                            # ignore test sites
                            if site.endswith('test') or site.endswith('Test') or site.startswith('Test'):
                                continue
                            # ignore analysis queues
                            if (not forAnalysis) and site.startswith('ANALY'):
                                continue
                            # get SiteSpec
                            if siteMapper.checkSite(site):
                                tmpSiteSpec = siteMapper.getSite(site)
                            else:
                                _log.debug(" skip: %s doesn't exist in DB" % site)
                                continue
                            # check status
                            if tmpSiteSpec.status in ['offline','brokeroff']:
                                if forAnalysis and tmpSiteSpec.status == 'brokeroff' and tmpSiteSpec.accesscontrol == 'grouplist':
                                    # ignore brokeroff for grouplist site
                                    pass
                                else:
                                    _log.debug(' skip: status %s' % tmpSiteSpec.status)
                                    if forAnalysis and trustIS:
                                        resultsForAnal['pilot'].append(site)
                                    continue
                            if tmpSiteSpec.status == 'test' and (not prevProType in ['prod_test']):
                                _log.debug(' skip: status %s for %s' % (tmpSiteSpec.status,prevProType))
                                if forAnalysis and trustIS:
                                    resultsForAnal['pilot'].append(site)
                                continue
                            _log.debug('   status=%s' % tmpSiteSpec.status)
                            # change NULL cmtconfig to slc3
                            if prevCmtConfig in ['NULL','',None]:
                                tmpCmtConfig = 'i686-slc3-gcc323-opt'
                            else:
                                tmpCmtConfig = prevCmtConfig
                            # set release
                            releases = tmpSiteSpec.releases
                            if prevProType in ['reprocessing']:
                                # use validated releases for reprocessing
                                releases = tmpSiteSpec.validatedreleases
                            _log.debug('   %s' % str(releases))
                            _log.debug('   %s' % str(tmpSiteSpec.cmtconfig))
                            if forAnalysis and (tmpSiteSpec.cloud in ['US'] or prevRelease==''):
                                # doesn't check releases for US analysis
                                _log.debug(' skip release check')
                                pass
                            elif forAnalysis and useCacheVersion:
                                # cache matching
                                if not site in siteListWithCache:
                                    _log.debug(' skip: cache %s/%s not found' % (prevRelease.replace('\n',' '),prevCmtConfig))                            
                                    if trustIS:
                                        resultsForAnal['rel'].append(site)
                                    continue    
                            elif (prevRelease != None and ((releases != [] and previousCloud != 'US') or \
                                                           prevProType in ['reprocessing']) and \
                                  (not _checkRelease(prevRelease,releases))) or \
                                  (tmpCmtConfig != None and tmpSiteSpec.cmtconfig != [] and \
                                   (not tmpCmtConfig in tmpSiteSpec.cmtconfig)):
                                _log.debug(' skip: release %s/%s not found' % (prevRelease.replace('\n',' '),prevCmtConfig))
                                if forAnalysis and trustIS:
                                    resultsForAnal['rel'].append(site)
                                # keep message to logger
                                try:
                                    if prevSourceLabel in ['managed','test']:
                                        # make message
                                        message = '%s - release %s/%s not found' % (site,prevRelease.replace('\n',' '),prevCmtConfig)
                                        if not message in loggerMessages:
                                            loggerMessages.append(message)
                                except:
                                    pass
                                continue
                            elif not foundRelease:
                                # found at least one site has the release
                                foundRelease = True
                            # check memory
                            if tmpSiteSpec.memory != 0 and (not prevMemory in [None,0,'NULL']):
                                try:
                                    if int(tmpSiteSpec.memory) < int(prevMemory):
                                        _log.debug('  skip: memory shortage %s<%s' % (tmpSiteSpec.memory,prevMemory))
                                        continue
                                except:
                                    type, value, traceBack = sys.exc_info()
                                    _log.error("memory check : %s %s" % (type,value))
                            # get pilot statistics
                            if nWNmap == {}:
                                nWNmap = taskBuffer.getCurrentSiteData()
                            if nWNmap.has_key(site):    
                                nPilots = nWNmap[site]['getJob'] + nWNmap[site]['updateJob']
                            else:
                                nPilots = 0
                            # if no pilots
                            if nPilots == 0 and nWNmap != {}:
                                _log.debug(" skip: %s no pilot" % site)
                                if forAnalysis and trustIS:
                                    resultsForAnal['pilot'].append(site)
                                continue
                            # if no jobs in jobsActive/jobsDefined
                            if not jobStatistics.has_key(site):
                                jobStatistics[site] = {'assigned':0,'activated':0,'running':0}
                            # check space for T2
                            if site != siteMapper.getCloud(previousCloud)['source']:
                                if tmpSiteSpec.space != 0:
                                    nRemJobs = jobStatistics[site]['assigned']+jobStatistics[site]['activated']+jobStatistics[site]['running']
                                    if not forAnalysis:
                                        # take assigned/activated/running jobs into account for production
                                        remSpace = tmpSiteSpec.space - 0.250*nRemJobs
                                    else:
                                        remSpace = tmpSiteSpec.space
                                    _log.debug('   space available=%s remain=%s' % (tmpSiteSpec.space,remSpace))
                                    if remSpace < diskThreshold:
                                        _log.debug('  skip: disk shortage < %s' % diskThreshold)
                                        if forAnalysis and trustIS:
                                            resultsForAnal['disk'].append(site)
                                        # keep message to logger
                                        try:
                                            if prevSourceLabel in ['managed','test']:
                                                # make message
                                                message = '%s - disk %s < %s' % (site,remSpace,diskThreshold)
                                                if not message in loggerMessages:
                                                    loggerMessages.append(message)
                                        except:
                                            pass
                                        continue
                            # number of jobs per node
                            if not nWNmap.has_key(site):
                                nJobsPerNode = 1
                            elif jobStatistics[site]['running']==0 or nWNmap[site]['updateJob']==0:
                                nJobsPerNode = 1
                            else:
                                nJobsPerNode = float(jobStatistics[site]['running'])/float(nWNmap[site]['updateJob'])
                            # get the number of activated and assigned for the process group
                            tmpProGroup = ProcessGroups.getProcessGroup(prevProType)
                            if not jobStatBroker.has_key(site):
                                jobStatBroker[site] = {}
                            if not jobStatBroker[site].has_key(tmpProGroup):
                                jobStatBroker[site][tmpProGroup] = {'assigned':0,'activated':0,'running':0}
                            nAssJobs = jobStatBroker[site][tmpProGroup]['assigned']
                            nActJobs = jobStatBroker[site][tmpProGroup]['activated']
                            # calculate weight
                            _log.debug('   %s assigned:%s activated:%s running:%s nPilots:%s nJobsPerNode:%s' %
                                       (site,nAssJobs,nActJobs,jobStatistics[site]['running'],nPilots,nJobsPerNode))
                            if nPilots != 0:
                                winv = (float(nAssJobs+nActJobs)) / float(nPilots) / nJobsPerNode
                            else:
                                winv = (float(nAssJobs+nActJobs)) / nJobsPerNode
                            # send jobs to T1 when they require many or large inputs
                            if _isTooManyInput(nFilesPerJob,inputSizePerJob):
                                if site == siteMapper.getCloud(previousCloud)['source']:
                                    cloudT1Weight = 2.0
                                    # use weight in cloudconfig
                                    try:
                                        tmpCloudT1Weight = float(siteMapper.getCloud(previousCloud)['weight'])
                                        if tmpCloudT1Weight != 0.0:
                                            cloudT1Weight = tmpCloudT1Weight
                                    except:
                                        pass
                                    winv /= cloudT1Weight
                                    _log.debug('   special weight for %s : nInputs/Job=%s inputSize/Job=%s weight=%s' % 
                                               (site,nFilesPerJob,inputSizePerJob,cloudT1Weight))
                            _log.debug('Site:%s 1/Weight:%s' % (site,winv))
                            # choose largest nMinSites weights
                            minSites[site] = winv
                            if len(minSites) > nMinSites:
                                maxSite = site
                                maxWinv = winv
                                for tmpSite,tmpWinv in minSites.iteritems():
                                    if tmpWinv > maxWinv:
                                        maxSite = tmpSite
                                        maxWinv = tmpWinv
                                # delte max one
                                del minSites[maxSite]
                            # remove too different weights
                            if len(minSites) >= 2:
                                # look for minimum
                                minSite = minSites.keys()[0]
                                minWinv = minSites[minSite]
                                for tmpSite,tmpWinv in minSites.iteritems():
                                    if tmpWinv < minWinv:
                                        minSite = tmpSite
                                        minWinv = tmpWinv
                                # look for too different weights
                                difference = 2
                                removeSites = []
                                for tmpSite,tmpWinv in minSites.iteritems():
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
                            minSites['BNL_ATLAS_1'] = 0
                        # release not found
                        if forAnalysis and trustIS:
                            candidateForAnal = False
                    # choose site
                    _log.debug('Min Sites:%s' % minSites)
                    if len(fileList) ==0:
                        # choose min 1/weight
                        minSite = minSites.keys()[0]
                        minWinv = minSites[minSite]
                        for tmpSite,tmpWinv in minSites.iteritems():
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
                                tmpOKFiles = _getOkFiles(tmp_chosen_ce,fileList,guidList)
                            nFiles = len(tmpOKFiles)
                            _log.debug('site:%s - nFiles:%s' % (site,nFiles))
                            # choose site holding max # of files
                            if nFiles > maxNfiles:
                                chosenCE = tmp_chosen_ce
                                maxNfiles = nFiles
                                okFiles = tmpOKFiles
                    # set job spec
                    _log.debug('indexJob      : %s' % indexJob)
                    _log.debug('nInputs/Job   : %s' % nFilesPerJob)
                    _log.debug('inputSize/Job : %s' % inputSizePerJob)
                    for tmpJob in jobs[indexJob-iJob-1:indexJob-1]:
                        # set computingSite
                        if (not candidateForAnal) and forAnalysis and trustIS:
                            resultsForAnalStr = 'ERROR : No candidate. '
                            if resultsForAnal['rel'] != []:
                                resultsForAnalStr += 'Release:%s was not found in %s. ' % (prevRelease,str(resultsForAnal['rel']))
                            if resultsForAnal['pilot'] != []:
                                resultsForAnalStr += '%s are inactive (no pilots for last 3 hours). ' % str(resultsForAnal['pilot'])
                            if resultsForAnal['disk'] != []:
                                resultsForAnalStr += 'Disk shortage < %sGB at %s. ' % (diskThreshold,str(resultsForAnal['disk']))
                            resultsForAnalStr = resultsForAnalStr[:-1]
                            tmpJob.computingSite = resultsForAnalStr
                        else:
                            tmpJob.computingSite = chosenCE.sitename
                        _log.debug('PandaID:%s -> site:%s' % (tmpJob.PandaID,tmpJob.computingSite))
                        if tmpJob.computingElement == 'NULL':
                            if tmpJob.prodSourceLabel == 'ddm':
                                # use nickname for ddm jobs
                                tmpJob.computingElement = chosenCE.nickname
                            else:
                                tmpJob.computingElement = chosenCE.gatekeeper
                        # fail jobs if no sites have the release
                        if (not foundRelease) and (tmpJob.prodSourceLabel in ['managed','test']):
                            tmpJob.jobStatus          = 'failed'
                            tmpJob.brokerageErrorCode = ErrorCode.EC_Release
                            if prevProType in ['reprocessing']:
                                tmpJob.brokerageErrorDiag = '%s/%s not validated for reprocessing in this cloud' % (tmpJob.AtlasRelease,tmpJob.cmtConfig)
                            else:
                                tmpJob.brokerageErrorDiag = '%s/%s not found in this cloud' % (tmpJob.AtlasRelease,tmpJob.cmtConfig)
                            _log.debug(tmpJob.brokerageErrorDiag)
                            continue
                        # set 'ready' if files are already there
                        _setReadyToFiles(tmpJob,okFiles,siteMapper)                        
                        # update statistics
                        tmpProGroup = ProcessGroups.getProcessGroup(tmpJob.processingType)
                        if not jobStatistics.has_key(tmpJob.computingSite):
                            jobStatistics[tmpJob.computingSite] = {'assigned':0,'activated':0,'running':0}
                        if not jobStatBroker.has_key(tmpJob.computingSite):
                            jobStatBroker[tmpJob.computingSite] = {}
                        if not jobStatBroker[tmpJob.computingSite].has_key(tmpProGroup):
                            jobStatBroker[tmpJob.computingSite][tmpProGroup] = {'assigned':0,'activated':0,'running':0}
                        jobStatistics[tmpJob.computingSite]['assigned'] += 1
                        jobStatBroker[tmpJob.computingSite][tmpProGroup]['assigned'] += 1
                # terminate
                if job == None:
                    break
                # reset iJob
                iJob = 0
                # reset file list
                fileList = []
                guidList = []            
                okFiles  = {}
                totalNumInputs = 0
                totalInputSize = 0
                # create new dispDBlock
                if job.prodDBlock != 'NULL':
                    # get datatype
                    try:
                        tmpDataType = job.prodDBlock.split('.')[-2]
                    except:
                        # default
                        tmpDataType = 'Unknown'                        
                    if len(tmpDataType) > 10:
                        # avoid too long name
                        tmpDataType = 'Unknown'
                    dispatchDBlock = "panda.%s.%s.%s.%s_dis%s" % (tmpDataType,job.taskID,time.strftime('%m.%d'),
                                                                  commands.getoutput('uuidgen'),job.PandaID)
                    _log.debug('New dispatchDBlock: %s' % dispatchDBlock)                    
                prodDBlock = job.prodDBlock
                # already define computingSite
                if job.computingSite != 'NULL':
                    # instantiate KnownSite
                    chosen_ce = siteMapper.getSite(job.computingSite)
                    # if site doesn't exist, use ANALY_BNL_ATLAS_1
                    if job.homepackage.startswith('AnalysisTransforms'):
                        if chosen_ce.sitename == 'BNL_ATLAS_1':
                            chosen_ce = siteMapper.getSite('ANALY_BNL_ATLAS_1')
                            overwriteSite = True
                else:
                    # default for Analysis jobs
                    if job.homepackage.startswith('AnalysisTransforms'):
                        chosen_ce = siteMapper.getSite('ANALY_BNL_ATLAS_1')
                        overwriteSite = True                        
                    else:
                        # set chosen_ce
                        chosen_ce = 'TOBEDONE'
            # increment iJob
            iJob += 1
            # reserve computingSite and cloud
            computingSite   = job.computingSite
            previousCloud   = job.cloud
            prevRelease     = job.AtlasRelease
            prevMemory      = job.minRamCount
            prevCmtConfig   = job.cmtConfig
            prevProType     = job.processingType
            prevSourceLabel = job.prodSourceLabel
            # assign site
            if chosen_ce != 'TOBEDONE':
                job.computingSite = chosen_ce.sitename
                if job.computingElement == 'NULL':
                    if job.prodSourceLabel == 'ddm':
                        # use nickname for ddm jobs
                        job.computingElement = chosen_ce.nickname
                    else:
                        job.computingElement = chosen_ce.gatekeeper
                # update statistics
                if not jobStatistics.has_key(job.computingSite):
                    jobStatistics[job.computingSite] = {'assigned':0,'activated':0,'running':0}
                jobStatistics[job.computingSite]['assigned'] += 1
                _log.debug('PandaID:%s -> preset site:%s' % (job.PandaID,chosen_ce.sitename))
                # set cloud
                if job.cloud in ['NULL',None,'']:
                    job.cloud = chosen_ce.cloud
            # set destinationSE
            destSE = job.destinationSE
            if siteMapper.checkCloud(job.cloud):
                # use cloud dest for non-exsiting sites
                if job.prodSourceLabel != 'user' and not job.destinationSE in siteMapper.siteSpecList.keys():
                    destSE = siteMapper.getCloud(job.cloud)['dest'] 
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
                   (file.status != 'ready' or job.computingSite in prestageSites):
                    if first:
                        first = False
                        job.dispatchDBlock = dispatchDBlock
                    file.dispatchDBlock = dispatchDBlock
                    file.status = 'pending'
                    if not file.lfn in fileList:
                        fileList.append(file.lfn)
                        guidList.append(file.GUID)
                        try:
                            # get total number/size of inputs except DBRelease
                            # tgz inputs for evgen may be negligible
                            if re.search('\.tar\.gz',file.lfn) == None:
                                totalNumInputs += 1
                                totalInputSize += file.fsize
                        except:
                            pass
                # destinationSE
                if file.type in ['output','log'] and destSE != '':
                    file.destinationSE = destSE
                # pre-assign GUID to log
                if file.type == 'log':
                    # get lock
                    fcntl.flock(_lockGetUU.fileno(), fcntl.LOCK_EX)                
                    # generate GUID
                    file.GUID = commands.getoutput('uuidgen')
                    # release lock
                    fcntl.flock(_lockGetUU.fileno(), fcntl.LOCK_UN)
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
        except:
            pass
        _log.debug('finished')
    except:
        type, value, traceBack = sys.exc_info()
        _log.error("schedule : %s %s" % (type,value))

