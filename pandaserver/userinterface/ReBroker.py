'''
find another candidate site for analysis

'''

import re
import sys
import time
import random
import datetime
import threading

from dataservice.DDM import ddm
from taskbuffer.JobSpec import JobSpec
from taskbuffer.OraDBProxy import DBProxy
from dataservice.Setupper import Setupper
from brokerage.SiteMapper import SiteMapper
import brokerage.broker

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('ReBroker')


class ReBroker (threading.Thread):

    # constructor
    def __init__(self,taskBuffer,cloud=None,excludedSite=None,overrideSite=True,
                 simulation=False):
        threading.Thread.__init__(self)
        self.job           = None
        self.jobID         = None
        self.pandaID       = None
        self.cloud         = cloud
        self.libDS         = ''
        self.pandaJobList  = []
        self.buildStatus   = None
        self.taskBuffer    = taskBuffer
        self.token         = None
        self.newDatasetMap = {}
        self.simulation    = simulation
        self.excludedSite  = excludedSite
        self.overrideSite = overrideSite
        

    # main
    def run(self):
        try:
            # get job
            tmpJobs = self.taskBuffer.getFullJobStatus([self.rPandaID])
            if tmpJobs == [] or tmpJobs[0] == None:
                _logger.debug("cannot find job for PandaID=%s" % self.rPandaID)
                return
            self.job = tmpJobs[0]
            _logger.debug("%s start" % self.token)
            # using output container
            if not self.job.destinationDBlock.endswith('/'):
                _logger.debug("%s ouput dataset container is required" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check jobsetID
            if self.job.jobsetID in [0,'NULL',None]:
                _logger.debug("%s jobsetID is undefined" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check metadata 
            if self.job.metadata in [None,'NULL']:
                _logger.debug("%s metadata is unavailable" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check --disableRebrokerage
            match = re.search("--disableRebrokerage",self.job.metadata)
            if match != None:
                _logger.debug("%s diabled rebrokerage" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check --workingGroup since it is site-specific 
            match = re.search("--workingGroup",self.job.metadata)
            if match != None:
                _logger.debug("%s workingGroup is specified" % self.token)
                _logger.debug("%s end" % self.token)
                return
            # check excludedSite
            if self.excludedSite == None:
                self.excludedSite = []
                match = re.search("--excludedSite( +|=)([^ \"\';$]+)",self.job.metadata)
                if match != None:
                    self.excludedSite = match.group(2).split(',')
            _logger.debug("%s excludedSite=%s" % (self.token,str(self.excludedSite)))
            # get inDS/LFNs
            status,tmpMapInDS = self.taskBuffer.getInDatasetsForReBrokerage(self.jobID,self.userName)
            if not status:
                # failed
                _logger.error("%s failed to get inDS/LFN from DB" % self.token)
                return
            status,inputDS = self.getListDatasetsUsedByJob(tmpMapInDS)
            if not status:
                # failed
                _logger.error("%s failed" % self.token)
                return 
            # get relicas
            replicaMap = {}
            unknownSites = {} 
            for tmpDS in inputDS:
                if tmpDS.endswith('/'):
                    # container
                    status,tmpRepMaps = self.getListDatasetReplicasInContainer(tmpDS)
                else:
                    # normal dataset
                    status,tmpRepMap = self.getListDatasetReplicas(tmpDS)
                    tmpRepMaps = {tmpDS:tmpRepMap}
                if not status:
                    # failed
                    _logger.debug("%s failed" % self.token)
                    return 
                # make map per site
                for tmpDS,tmpRepMap in tmpRepMaps.iteritems():
                    for tmpSite,tmpStat in tmpRepMap.iteritems():
                        # ignore special sites
                        if tmpSite in ['CERN-PROD_TZERO','CERN-PROD_DAQ']:
                            continue
                        # ignore tape sites
                        if tmpSite.endswith('TAPE'):
                            continue
                        # keep sites with unknown replica info 
                        if tmpStat[-1]['found'] == None:
                            if not unknownSites.has_key(tmpDS):
                                unknownSites[tmpDS] = []
                            unknownSites[tmpDS].append(tmpSite)
                        # change DISK to SCRATCHDISK
                        tmpSite = re.sub('_[^_-]+DISK$','_SCRATCHDISK',tmpSite)
                        # change PERF-XYZ to SCRATCHDISK
                        tmpSite = re.sub('_PERF-[^_-]+$','_SCRATCHDISK',tmpSite)
                        # patch for BNLPANDA
                        if tmpSite in ['BNLPANDA','BNL-OSG2_SCRATCHDISK']:
                            tmpSite = 'BNL-OSG2_USERDISK'
                        # add to map    
                        if not replicaMap.has_key(tmpSite):
                            replicaMap[tmpSite] = {}
                        replicaMap[tmpSite][tmpDS] = tmpStat[-1]
            _logger.debug("%s replica map -> %s" % (self.token,str(replicaMap)))
            # refresh replica info in needed
            self.refreshReplicaInfo(unknownSites)
            # instantiate SiteMapper
            siteMapper = SiteMapper(self.taskBuffer)
            # get original DDM
            origSiteDDM = siteMapper.getSite(self.job.computingSite).ddm
            maxDQ2Sites = []
            if inputDS != []:
                # check original is there
                if not replicaMap.has_key(origSiteDDM):
                    _logger.error("%s original site %s was not found in replica map" % \
                                  (self.token,origSiteDDM))
                    _logger.debug("%s failed" % self.token)
                    return 
                # look for DQ2 IDs where datasets are available in the same distribution as original site
                firstLoop = True
                for tmpOrigDS,tmpOrigVal in replicaMap[origSiteDDM].iteritems():
                    # loop over all sites
                    for tmpSite,tmpDsVal in replicaMap.iteritems():
                        if tmpDsVal.has_key(tmpOrigDS) and tmpDsVal[tmpOrigDS]['found'] != None and \
                               tmpOrigVal['found'] != None and tmpDsVal[tmpOrigDS]['found'] >= tmpOrigVal['found']:
                            # add in the first loop
                            if firstLoop:
                                maxDQ2Sites.append(tmpSite)
                        else:
                            # delete
                            if tmpSite in maxDQ2Sites:
                                maxDQ2Sites.remove(tmpSite)
                    # first loop is over
                    if firstLoop:
                        firstLoop = False
            _logger.debug("%s candidate DQ2s -> %s" % (self.token,str(maxDQ2Sites)))
            if inputDS != [] and maxDQ2Sites == []:
                _logger.debug("%s no DQ2 candidate" % self.token)
            else:
                maxPandaSites = []
                # look for Panda siteIDs
                for tmpSiteID,tmpSiteSpec in siteMapper.siteSpecList.iteritems():
                    # use ANALY_ only
                    if not tmpSiteID.startswith('ANALY_'):
                        continue
                    # remove test and local
                    if re.search('_test',tmpSiteID,re.I) != None:
                        continue
                    if re.search('_local',tmpSiteID,re.I) != None:
                        continue
                    # use online only
                    if tmpSiteSpec.status != 'online':
                        continue
                    # excluded sites
                    excludedFlag = False
                    for tmpExcSite in self.excludedSite:
                        if re.search(tmpExcSite,tmpSiteID) != None:
                            excludedFlag = True
                            break
                    if excludedFlag:    
                        continue
                    # check DQ2 ID
                    if self.cloud in [None,tmpSiteSpec.cloud] \
                           and (tmpSiteSpec.ddm in maxDQ2Sites or inputDS == []):
                        # append
                        if not tmpSiteID in maxPandaSites:
                            maxPandaSites.append(tmpSiteID)
                # choose at most 20 sites randomly to avoid too many lookup            
                random.shuffle(maxPandaSites)
                maxPandaSites = maxPandaSites[:20]
                _logger.debug("%s candidate PandaIDs -> %s" % (self.token,str(maxPandaSites)))
                # no Panda siteIDs            
                if maxPandaSites == []:            
                    _logger.debug("%s no Panda site candidate" % self.token)
                else:
                    # set AtlasRelease and cmtConfig to dummy job
                    tmpJobForBrokerage = JobSpec()
                    if self.job.AtlasRelease in ['NULL',None]:
                        tmpJobForBrokerage.AtlasRelease = ''
                    else:
                        tmpJobForBrokerage.AtlasRelease = self.job.AtlasRelease
                    # run brokerage
                    if not self.job.cmtConfig in ['NULL',None]:    
                        tmpJobForBrokerage.cmtConfig = self.job.cmtConfig
                    brokerage.broker.schedule([tmpJobForBrokerage],self.taskBuffer,siteMapper,forAnalysis=True,
                                              setScanSiteList=maxPandaSites,trustIS=True)
                    newSiteID = tmpJobForBrokerage.computingSite
                    _logger.debug("%s runBrokerage - > %s" % (self.token,newSiteID))
                    # unknown site
                    if not siteMapper.checkSite(newSiteID):
                        _logger.error("%s unknown site" % self.token)
                        _logger.debug("%s failed" % self.token)
                        return 
                    # get new site spec
                    newSiteSpec = siteMapper.getSite(newSiteID)
                    # avoid repetition
                    if newSiteSpec.ddm == origSiteDDM:
                        _logger.debug("%s assigned to the same site %s " % (self.token,newSiteID))
                        _logger.debug("%s end" % self.token)                        
                        return
                    # simulation mode
                    if self.simulation:
                        _logger.debug("%s end simulation" % self.token)                        
                        return
                    # prepare jobs
                    status = self.prepareJob(newSiteID,newSiteSpec.cloud)
                    if status:
                        # run SetUpper
                        statusSetUp = self.runSetUpper()
                        if not statusSetUp:
                            _logger.debug("%s runSetUpper failed" % self.token)
                        else:
                            _logger.debug("%s successfully assigned to %s" % (self.token,newSiteID))
            _logger.debug("%s end" % self.token)
        except:
            errType,errValue,errTraceBack = sys.exc_info()
            _logger.error("%s run() : %s %s" % (self.token,errType,errValue))


    # lock job to disable multiple broker running in parallel
    def lockJob(self,dn,jobID,libDS):
        # make token
        tmpProxy = DBProxy()
        self.token = "%s:%s:" % (tmpProxy.cleanUserID(dn),jobID)
        _logger.debug("%s lockJob" % self.token)        
        # lock
        resST,resVal = self.taskBuffer.lockJobForReBrokerage(dn,jobID,libDS,self.simulation)
        # failed
        if not resST:
            return False,resVal['err']
        # keep jobID and libDS
        self.jobID = jobID
        self.libDS = libDS
        # set PandaID,buildStatus,userName
        self.rPandaID    = resVal['rPandaID']        
        self.bPandaID    = resVal['bPandaID']
        self.userName    = resVal['userName']
        self.buildStatus = resVal['bStatus']
        self.buildJobID  = resVal['bJobID']
        _logger.debug("%s run PandaID=%s / build PandaID=%s Status=%s JobID=%s" % \
                      (self.token,self.rPandaID,self.bPandaID,self.buildStatus,self.buildJobID))
        # return
        return True,''


    # move build job to jobsDefined4
    def prepareJob(self,site,cloud):
        _logger.debug("%s prepareJob" % self.token)
        # reuse buildJob + all runJobs
        if self.jobID == self.buildJobID and self.buildStatus in ['defined','activated']:
            if self.buildStatus == 'activated':
                # move build job to jobsDefined4                
                ret = self.taskBuffer.resetBuildJobForReBrokerage(self.bPandaID)
                if not ret:
                    _logger.error("%s failed to move build job %s to jobsDefined" % (self.token,self.bPandaID))
                    return False
            # get PandaIDs from jobsDefined4    
            tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.userName,self.jobID,False)
            if tmpPandaIDs == []:
                _logger.error("%s cannot find PandaDSs" % self.token)
                return False
            # get jobSpecs
            iBunchJobs = 0
            nBunchJobs = 500
            tmpJobsMap = {}
            while iBunchJobs < len(tmpPandaIDs):
                # get IDs
                tmpJobs = self.taskBuffer.peekJobs(tmpPandaIDs[iBunchJobs:iBunchJobs+nBunchJobs],True,False,False,False)
                for tmpJob in tmpJobs:
                    if tmpJob != None and tmpJob.jobStatus in ['defined','assigned']: 
                        self.pandaJobList.append(tmpJob)
                # increment index
                iBunchJobs += nBunchJobs
        # make new bunch
        else:
            # make new buildJob
            if self.bPandaID != None:            
                tmpJobs = self.taskBuffer.getFullJobStatus([self.bPandaID])
                if tmpJobs == [] or tmpJobs[0] == None:
                    _logger.debug("cannot find build job for PandaID=%s" % self.bPandaID)
                    return False
                # make
                tmpBuildJob,oldLibDS,newLibDS = self.taskBuffer.makeNewBuildJobForRebrokerage(tmpJobs[0])
                # set parameters
                tmpBuildJob.jobExecutionID = self.jobID
                # regisger
                status = self.registerNewDataset(newLibDS)
                if not status:
                    _logger.debug("%s failed to register new libDS" % self.token)
                    return False
                # append
                self.pandaJobList = [tmpBuildJob]
            # prepare outputDS
            status = self.prepareDS()
            if not status:
                _logger.error("%s failed to prepare outputDS" % self.token)
                return False
            # get PandaIDs
            if self.buildStatus in ['finished',None]:
                # from jobsActivated when buildJob already finished or noBuild
                tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.userName,self.jobID,True)                
            else:
                # from jobsDefined
                tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.userName,self.jobID,False)
            if tmpPandaIDs == []:
                _logger.error("%s cannot find PandaDSs" % self.token)
                return False
            # get jobSpecs
            iBunchJobs = 0
            nBunchJobs = 500
            tmpJobsMap = {}
            while iBunchJobs < len(tmpPandaIDs):
                # get jobs
                tmpJobs = self.taskBuffer.peekJobs(tmpPandaIDs[iBunchJobs:iBunchJobs+nBunchJobs],True,True,False,False)
                for tmpJob in tmpJobs:
                    # check job status. activated jobs were changed to holding by getPandaIDsForReBrokerage
                    if tmpJob != None and tmpJob.jobStatus in ['defined','assigned','holding']:
                        # reset parameter
                        tmpJob.parentID = tmpJob.PandaID
                        tmpJob.PandaID = None
                        tmpJob.jobExecutionID = tmpJob.jobDefinitionID
                        tmpJob.jobParameters = re.sub(oldLibDS,newLibDS,tmpJob.jobParameters)
                        for tmpFile in tmpJob.Files:
                            tmpFile.row_ID = None
                            tmpFile.PandaID = None
                            if tmpFile.type == 'input':
                                if tmpFile.dataset == oldLibDS:
                                    tmpFile.status  = 'unknown'
                                    tmpFile.GUID    = None
                                    tmpFile.dataset = newLibDS
                                    tmpFile.dispatchDBlock = newLibDS
                                    tmpFile.lfn = re.sub(oldLibDS,newLibDS,tmpFile.lfn)
                            else:
                                # use new dataset
                                tmpFile.destinationDBlock = re.sub('_sub\d+$','',tmpFile.destinationDBlock)
                                if not self.newDatasetMap.has_key(tmpFile.destinationDBlock):
                                    _logger.error("%s cannot find new dataset for %s:%s" % (self.token,tmpFile.PandaID,tmpFile.destinationDBlock))
                                    return False
                                tmpFile.destinationDBlock = self.newDatasetMap[tmpFile.destinationDBlock] 
                        # append
                        self.pandaJobList.append(tmpJob)
                # increment index
                iBunchJobs += nBunchJobs
        # no jobs
        if self.pandaJobList == []:
            _logger.error("%s no jobs" % self.token)
            return False
        # set cloud and site
        for tmpJob in self.pandaJobList:
            # check if --destSE is used
            newDestSE = False
            if tmpJob.destinationSE == tmpJob.computingSite:
                newDestSE = True
                tmpJob.destinationSE = site
            # set site and cloud    
            tmpJob.computingSite = site
            tmpJob.cloud = cloud
            # reset destinationDBlock
            for tmpFile in tmpJob.Files:
                if tmpFile.type in ['output','log']:
                    # set destSE
                    if newDestSE:
                        tmpFile.destinationSE = tmpJob.destinationSE
                    else:
                        tmpFile.destinationSE = site
        # return
        return True

        
    # prepare libDS 
    def prepareDS(self):
        _logger.debug("%s prepareDS" % self.token)        
        # get all outDSs
        for tmpFile in self.job.Files:
            if tmpFile.type in ['output','log']:
                tmpDS = re.sub('_sub\d+$','',tmpFile.destinationDBlock)
                if not tmpDS in self.newDatasetMap:
                    # get new rev number
                    sn,dummyFlag = self.taskBuffer.getSerialNumber(tmpDS)
                    # append new rev number
                    match = re.search('_rev(\d+)$',tmpDS)
                    if match == None:
                        newDS = tmpDS + '_rev%s' % sn
                    else:
                        newDS = re.sub('_rev(\d+)$','_rev%s' % sn,tmpDS)
                    # register
                    status = self.registerNewDataset(newDS,tmpFile.dataset)
                    if not status:
                        _logger.debug("%s prepareDS failed" % self.token)                                
                        return False
                    # append
                    self.newDatasetMap[tmpDS] = newDS
        return True            


    # run SetUpper
    def runSetUpper(self):
        # reuse buildJob + all runJobs
        if self.jobID == self.buildJobID and self.buildStatus in ['defined','activated']:
            _logger.debug("%s start Setupper for JobID=%s" % (self.token,self.jobID))
            thr = Setupper(self.taskBuffer,self.pandaJobList,resetLocation=True)
            thr.start()
            thr.join()
        # new bunch    
        else:
            # fake FQANs
            fqans = []
            if not self.job.countryGroup in ['','NULL',None]:
                fqans.append('/atlas/%s/Role=NULL' % self.job.countryGroup)
            if self.job.destinationDBlock.startswith('group') and not self.job.workingGroup in ['','NULL',None]:
                fqans.append('/atlas/%s/Role=production' % self.job.workingGroup)
            # insert jobs
            _logger.debug("%s start storeJobs for JobID=%s" % (self.token,self.jobID))            
            ret = self.taskBuffer.storeJobs(self.pandaJobList,self.job.prodUserID,True,False,fqans,self.job.creationHost,True)
            if ret == []:
                _logger.error("%s storeJobs failed with [] for JobID=%s" % (self.token,self.jobID))
                return False
            # get PandaIDs to be killed
            pandaIDsTobeKilled = []
            for tmpIndex,tmpItem in enumerate(ret):
                if not tmpItem[0] in ['NULL',None]:
                    tmpJob = self.pandaJobList[tmpIndex]
                    if not tmpJob.parentID in [0,None,'NULL']:                    
                        pandaIDsTobeKilled.append(tmpJob.parentID)
            if pandaIDsTobeKilled != []:
                _logger.debug("%s kill jobs for JobID=%s : %s" % (self.token,self.jobID,str(pandaIDsTobeKilled)))
                self.taskBuffer.killJobs(pandaIDsTobeKilled,'rebrokerage','9',True)
        # succeeded
        _logger.debug("%s completed for JobID=%s" % (self.token,self.jobID))
        return True

    
    # check DDM response
    def isDQ2ok(self,out):
        if out.find("DQ2 internal server exception") != -1 \
               or out.find("An error occurred on the central catalogs") != -1 \
               or out.find("MySQL server has gone away") != -1 \
               or out == '()':
            return False
        return True
    

    # get list of datasets
    def getListDatasets(self,dataset):
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s listDatasets %s" % (self.token,iDDMTry,nTry,dataset))
            status,out =  ddm.DQ2.main('listDatasets',dataset,0,True)            
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result    
        if status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))            
            return False,{}
        try:
            # convert res to map
            exec "tmpDatasets = %s" % out
            # remove _sub/_dis
            resList = []
            for tmpDS in tmpDatasets.keys():
                if re.search('(_sub|_dis)\d+$',tmpDS) == None and re.search('(_shadow$',tmpDS) == None:
                    resList.append(tmpDS)
            _logger.debug('%s getListDatasets->%s' % (self.token,str(resList)))
            return True,resList
        except:
            _logger.error(self.token+' '+out)            
            _logger.error('%s could not convert HTTP-res to datasets for %s' % (self.token,dataset))
            return False,{}

            
    # get list of replicas for a dataset
    def getListDatasetReplicas(self,dataset):
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s listDatasetReplicas %s" % (self.token,iDDMTry,nTry,dataset))
            status,out = ddm.DQ2.main('listDatasetReplicas',dataset,0,None,False)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result    
        if status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))            
            return False,{}
        try:
            # convert res to map
            exec "tmpRepSites = %s" % out
            _logger.debug('%s getListDatasetReplicas->%s' % (self.token,str(tmpRepSites)))
            return True,tmpRepSites
        except:
            _logger.error(self.token+' '+out)            
            _logger.error('%s could not convert HTTP-res to replica map for %s' % (self.token,dataset))
            return False,{}
        
    
    # get replicas for a container 
    def getListDatasetReplicasInContainer(self,container):
        # response for failure
        resForFailure = False,{}
        # get datasets in container
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug('%s %s/%s listDatasetsInContainer %s' % (self.token,iDDMTry,nTry,container))
            status,out = ddm.DQ2.main('listDatasetsInContainer',container)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s bad DQ2 response for %s' % (self.token,container))
            return resForFailure
        datasets = []
        try:
            # convert to list
            exec "datasets = %s" % out
        except:
            _logger.error('%s could not convert HTTP-res to dataset list for %s' % (self.token,container))
            return resForFailure
        # loop over all datasets
        allRepMap = {}
        for dataset in datasets:
            # get replicas
            status,tmpRepSites = self.getListDatasetReplicas(dataset)
            if not status:
                return resForFailure
            # append
            allRepMap[dataset] = tmpRepSites
        # return
        _logger.debug('%s getListDatasetReplicasInContainer done')
        return True,allRepMap            


    # delete original locations
    def deleteDatasetReplicas(self,datasets):
        # loop over all datasets
        for dataset in datasets:
            # get locations
            status,tmpRepSites = self.getListDatasetReplicas(dataset)
            if not status:
                return False
            # no replicas
            if len(tmpRepSites.keys()) == 0:
                continue
            # delete
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s deleteDatasetReplicas %s" % (self.token,iDDMTry,nTry,dataset))
                status,out = ddm.DQ2.main('deleteDatasetReplicas',dataset,tmpRepSites.keys())
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            # result
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))            
                return False
            _logger.debug(self.token+' '+out)
        # return
        _logger.debug('%s deleted replicas for %s' % (self.token,str(datasets)))
        return True


    # check if datasets are empty
    def checkDatasetContents(self,datasets):
        # loop over all datasets
        for dataset in datasets:
            # check
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s getNumberOfFiles %s" % (self.token,iDDMTry,nTry,dataset))
                status,out = ddm.DQ2.main('getNumberOfFiles',dataset)
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            # result
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))            
                return False
            # convert to int
            _logger.debug(self.token+' '+out)            
            try:
                nFile = int(out)
                # not empty
                if nFile != 0:
                    _logger.error('%s %s is not empty' % (self.token,dataset))            
                    return False
            except:
                _logger.error("%s could not convert HTTP-res to nFiles" % (self.token,dataset))
                return False
        # all OK
        return True
                

    # register dataset
    def registerNewDataset(self,dataset,container=''):
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s registerNewDataset %s" % (self.token,iDDMTry,nTry,dataset))
            status,out = ddm.DQ2.main('registerNewDataset',dataset)
            if out.find('DQDatasetExistsException') != -1:
                break
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result
        if out.find('DQDatasetExistsException') != -1:
            # ignore DQDatasetExistsException
            pass
        elif status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s failed to register new dataset %s' % (self.token,dataset))            
            return False
        # remove /CN=proxy and /CN=limited from DN
        tmpRealDN = self.job.prodUserID
        tmpRealDN = re.sub('/CN=limited proxy','',tmpRealDN)
        tmpRealDN = re.sub('/CN=proxy','',tmpRealDN)
        # set owner
        for iDDMTry in range(nTry):
            _logger.debug("%s %s/%s setMetaDataAttribute %s" % (self.token,iDDMTry,nTry,dataset))
            status,out = ddm.DQ2.main('setMetaDataAttribute',dataset,'owner',tmpRealDN)            
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error(self.token+' '+out)
            _logger.error('%s failed to set owner to dataset %s' % (self.token,dataset))            
            return False
        # add to contaner
        if container != '' and container.endswith('/'):
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s registerDatasetsInContainer %s to %s" % (self.token,iDDMTry,nTry,dataset,container))
                status,out = ddm.DQ2.main('registerDatasetsInContainer',container,[dataset])
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s add %s to container:%s' % (self.token,dataset,container))            
                return False
        # return
        return True
                    

    # get list of dataset used by the job             
    def getListDatasetsUsedByJob(self,mapDsLFN):
        # response for failure
        resForFailure = False,[]
        # loop over all datasets
        retList = []
        for tmpDsContainer,tmpLFNs in mapDsLFN.iteritems():
            # not a container
            if not tmpDsContainer.endswith('/'):
                if not tmpDsContainer in retList:
                    retList.append(tmpDsContainer)
                continue
            # get datasets in container
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug('%s %s/%s listDatasetsInContainer %s' % (self.token,iDDMTry,nTry,tmpDsContainer))
                status,out = ddm.DQ2.main('listDatasetsInContainer',tmpDsContainer)
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.token,tmpDsContainer))
                return resForFailure
            tmpDatasets = []
            try:
                # convert to list
                exec "tmpDatasets = %s" % out
            except:
                _logger.error('%s could not convert HTTP-res to dataset list for %s' % (self.token,tmpDsContainer))
                return resForFailure
            # get files in dataset
            for tmpDS in tmpDatasets:
                if tmpDS in retList:
                    continue
                nTry = 3
                for iDDMTry in range(nTry):
                    _logger.debug('%s %s/%s listFilesInDataset %s' % (self.token,iDDMTry,nTry,tmpDS))
                    status,out = ddm.DQ2.main('listFilesInDataset',tmpDS)
                    if status != 0 or (not self.isDQ2ok(out)):
                        time.sleep(60)
                    else:
                        break
                if status != 0 or out.startswith('Error'):
                    _logger.error(self.token+' '+out)
                    _logger.error('%s bad DQ2 response for %s' % (self.token,tmpDS))
                    return resForFailure
                # get LFN map
                tmpMapDQ2 = {}
                try:
                    # convert to list
                    exec "tmpMapDQ2 = %s[0]" % out
                    for tmpGUID,tmpVal in tmpMapDQ2.iteritems():
                        # check if a file in DS is used by the job
                        if tmpVal['lfn'] in tmpLFNs:
                            # append
                            if not tmpDS in retList:
                                retList.append(tmpDS)
                            break
                except:
                    _logger.error('%s could not convert HTTP-res to LFN map for %s' % (self.token,tmpDS))
                    return resForFailure
        # return
        _logger.debug('%s getListDatasetsUsedByJob done %s' % (self.token,str(retList)))
        return True,retList
            

    # refresh replica info in needed
    def refreshReplicaInfo(self,unknownSites):
        for tmpDS,sites in unknownSites.iteritems():
            nTry = 3
            for iDDMTry in range(nTry):
                _logger.debug("%s %s/%s listFileReplicasBySites %s %s" % (self.token,iDDMTry,nTry,tmpDS,str(sites)))
                status,out =  ddm.DQ2_iter.listFileReplicasBySites(tmpDS,0,sites,0,300)
                if status != 0 or (not self.isDQ2ok(out)):
                    time.sleep(60)
                else:
                    break
            # result    
            if status != 0 or out.startswith('Error'):
                _logger.error(self.token+' '+out)
                _logger.error('%s bad DQ2 response for %s' % (self.token,dataset))
        # return
        return True
