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
        self.job          = None
        self.jobID        = None
        self.pandaID      = None
        self.cloud        = cloud
        self.libDS        = ''
        self.nJobs        = 0
        self.buildStatus  = None
        self.taskBuffer   = taskBuffer
        self.token        = None
        self.pandaJobsMap = {}
        self.simulation   = simulation
        self.excludedSite = excludedSite
        self.overrideSite = overrideSite
        

    # main
    def run(self):
        try:
            # get job
            tmpJobs = self.taskBuffer.getFullJobStatus([self.pandaID])
            if tmpJobs == [] or tmpJobs[0] == None:
                _logger.debug("cannot find job for PandaID=%s" % self.pandaID)
                return
            self.job = tmpJobs[0]
            _logger.debug("%s start" % self.token)
            # check metadata 
            if self.job.metadata in [None,'NULL']:
                _logger.debug("%s metadata is unavailable" % self.token)
                _logger.debug("%s failed" % self.token)
                return
            # check --site
            if (not self.simulation) and (not self.overrideSite):
                match = re.search("--site( +|=)([^ \"\';$]+)",self.job.metadata)
                _logger.debug("%s --site was being used" % self.token)
                _logger.debug("%s failed" % self.token)
            # check excludedSite
            if self.excludedSite == None:
                self.excludedSite = []
                match = re.search("--excludedSite( +|=)([^ \"\';$]+)",self.job.metadata)
                if match != None:
                    self.excludedSite = match.group(2).split(',')
            _logger.debug("%s excludedSite=%s" % (self.token,str(self.excludedSite)))
            # extract inDS from metadata
            inputDS = []
            match = re.search("--inDS( +|=)([^ \"\';$]+)",self.job.metadata)
            if match != None:
                inputDS = match.group(2).split(',')
            # look for parentDS
            match = re.search("--parentDS( +|=)([^ \"\';$]+)",self.job.metadata)
            if match != None:
                inputDS = match.group(2).split(',')
            _logger.debug("%s inDS=%s" % (self.token,str(inputDS)))
            # expand *
            newInDS = []
            for tmpDS in inputDS:
                if re.search('\*',tmpDS) == None:
                    newInDS.append(tmpDS)
                else:
                    # get list using wild-card
                    status,tmpList = self.getListDatasets(tmpDS)
                    if not status:
                        # failed
                        _logger.debug("%s failed" % self.token)
                        return 
                    # append
                    newInDS += tmpList
            # replace
            inputDS = newInDS
            # get relicas
            replicaMap = {} 
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
                        if tmpDsVal.has_key(tmpOrigDS) and tmpDsVal[tmpOrigDS]['found'] == tmpOrigVal['found']:
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
                    # check repetition
                    if newSiteSpec.ddm == origSiteDDM:
                        _logger.debug("%s assigned to the same site %s " % (self.token,newSiteID))
                        return
                    # prepare outputDS
                    status = self.prepareDS()
                    if not status:
                        _logger.error("%s failed to prepare outputDSs" % self.token)
                    else:
                        # simulation mode
                        if self.simulation:
                            _logger.debug("%s end simulation" % self.token)                        
                            return
                        # move jobs to jobsDefined
                        status = self.updateJob()
                        if not status:
                            _logger.error("%s failed to move jobs to jobsDefined" % self.token)
                        else:
                            # run SetUpper
                            statusSetUp = self.runSetUpper(newSiteID,newSiteSpec.cloud)
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
        # set PandaID,nJobs,buildStatus,userName
        self.pandaID     = resVal['PandaID']
        self.nJobs       = resVal['nJobs']
        self.userName    = resVal['userName']
        self.buildStatus = resVal['status']
        _logger.debug("%s nJobs=%s buildPandaID=%s buildStatus=%s" % (self.token,self.nJobs,
                                                                      self.pandaID,self.buildStatus))
        # return
        return True,''


    # move build job to jobsDefined4
    def updateJob(self):
        _logger.debug("%s updateJob" % self.token)
        if self.buildStatus in ['activated']:
            # buildJob is in activated state
            ret = self.taskBuffer.resetJobForReBrokerage(self.userName,self.jobID,1,self.job)
        elif self.buildStatus in ['finished']:
            # buildJob finished
            if self.jobID == self.job.jobDefinitionID:
                # normal build+run
                ret = self.taskBuffer.resetJobForReBrokerage(self.userName,self.jobID,self.nJobs-1,self.job)
            else:
                # libDS
                ret = self.taskBuffer.resetJobForReBrokerage(self.userName,self.jobID,self.nJobs,self.job)                
        elif self.buildStatus in [None]:
            # noBuild
            ret = self.taskBuffer.resetJobForReBrokerage(self.userName,self.jobID,self.nJobs,None)
        else:
            # do nothing for others
            ret = True
        return ret


    # get and sort jobs
    def getSortJobs(self,tmpPandaIDs):
        # get jobs        
        iBunchJobs = 0
        nBunchJobs = 500
        tmpJobsMap = {}
        while iBunchJobs < len(tmpPandaIDs):
            # get IDs
            tmpJobs = self.taskBuffer.peekJobs(tmpPandaIDs[iBunchJobs:iBunchJobs+nBunchJobs],True,True,False,False)
            # split by jobID
            for idxjob,tmpJob in enumerate(tmpJobs):
                # not found in DB
                if tmpJob == None:
                    _logger.error("%s cannot find PandaID=%s in DB" % (self.token,tmpPandaIDs[iBunchJobs+idxjob]))
                    return False,{}
                # check jobID
                if not tmpJobsMap.has_key(tmpJob.jobDefinitionID):
                    tmpJobsMap[tmpJob.jobDefinitionID] = []
                # append
                tmpJobsMap[tmpJob.jobDefinitionID].append(tmpJob)
            # increment index
            iBunchJobs += nBunchJobs
        # return
        return True,tmpJobsMap

        
    # prepare libDS and check outDS
    def prepareDS(self):
        _logger.debug("%s prepareDS" % self.token)        
        # get all outDSs
        if self.libDS != '' and self.buildStatus in ['activated']:
            # get jobs when buildJob is in activated
            tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.libDS,self.userName,self.jobID)
            if tmpPandaIDs == []:
                _logger.error("%s cannot find PandaDSs" % self.token)
                return False
            # get jobs
            tmpStat,tmpJobsMap = self.getSortJobs(tmpPandaIDs)
            # failed
            if not tmpStat:
                return False
            # keep map
            self.pandaJobsMap = tmpJobsMap
            dsWithNewLoc = []
            tmpJobIDList = tmpJobsMap.keys()
            # loop over all jobIDs to get outDS
            for tmpJobID in tmpJobIDList:
                tmpJobs = tmpJobsMap[tmpJobID]
                # loop over all jobs to get outDS
                for tmpJob in tmpJobs:
                    # set destinationSE when --destSE is not used
                    newDestSE = False
                    if tmpJob.destinationSE == tmpJob.computingSite:
                        newDestSE = True
                    # loop over all files
                    for tmpFile in tmpJob.Files:
                        if tmpFile.type in ['output','log']:
                            # get datasets with new location 
                            if newDestSE and not tmpFile.dataset in dsWithNewLoc:
                                dsWithNewLoc.append(tmpFile.dataset)
        else:
            # get list of outDSs
            tmpStat,tmpOutDSs,computingSite,destinationSE = self.taskBuffer.getOutDSsForReBrokerage(self.userName,self.jobID)
            if not tmpStat:
                _logger.error("%s cannot find outDSs" % self.token)
                return False
            # check only when --destSE is unused
            dsWithNewLoc = []
            if computingSite == destinationSE:
                dsWithNewLoc = tmpOutDSs
        # check active jobs in Panda
        status,jobInfo = self.taskBuffer.getNumWaitingJobsWithOutDS(dsWithNewLoc)
        if not status:
            _logger.error("%s failed to get job info with %s" % (self.token,str(dsWithNewLoc)))
            return False
        # other job are using this outDS 
        if len(jobInfo) != 1:
            _logger.error("%s JobID=%s are using %s" % (self.token,str(jobInfo.keys()),str(dsWithNewLoc)))
            return False
        # check if output datasets are empty
        if not self.checkDatasetContents(dsWithNewLoc):
            return False
        # succeeded
        return True


    # run SetUpper
    def runSetUpper(self,site,cloud):
        # get Panda jobs if not yet
        if self.pandaJobsMap == {}:
            # get PandaIDs
            if self.libDS != '' and self.buildStatus in ['finished']:
                # use new libDS
                tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.job.destinationDBlock,self.userName,self.jobID)
            else:
                tmpPandaIDs = self.taskBuffer.getPandaIDsForReBrokerage(self.libDS,self.userName,self.jobID)
            # get jobs
            tmpStat,self.pandaJobsMap = self.getSortJobs(tmpPandaIDs)
            # failed
            if not tmpStat:
                return False
        # loop over all jobID
        newJobsMap = {}
        dsWithNewLoc = []
        tmpJobIDList = self.pandaJobsMap.keys()
        tmpJobIDList.sort()
        for tmpJobID in tmpJobIDList:
            _logger.debug("%s preparation for JobID=%s" % (self.token,tmpJobID))
            tmpJobs = self.pandaJobsMap[tmpJobID]
            if tmpJobs[0].prodSourceLabel == 'panda':
                tmpNumJobs = 1 + ((len(tmpJobs)-1) << 1)
            else:
                tmpNumJobs = (len(tmpJobs)-1) << 1
            # set new parameters
            for tmpJob in tmpJobs:
                # set nJobs (=taskID)
                tmpJob.taskID = tmpNumJobs
                # set destinationSE when --destSE is not used
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
                        # reset destinationDBlock
                        tmpFile.destinationDBlock = tmpFile.dataset
                        # set destSE
                        if newDestSE:
                            tmpFile.destinationSE = tmpJob.destinationSE
                            # get datasets with new location 
                            if not tmpFile.dataset in dsWithNewLoc:
                                dsWithNewLoc.append(tmpFile.dataset)
            # append
            newJobsMap[tmpJobID] = tmpJobs
        # register newLibDS
        retNew = self.registerNewDataset(self.job.destinationDBlock)
        if not retNew:
            return False
        # delete datasets and locations
        if dsWithNewLoc != []:
            # delete original DQ2 locations            
            self.deleteDatasetReplicas(dsWithNewLoc)
            # delete from Panda DB to trigger location registration in following Setupper
            self.taskBuffer.deleteDatasets(dsWithNewLoc)
        # run setupper at this stage for following jobIDs not to delete locations
        tmpJobIDList = newJobsMap.keys()
        tmpJobIDList.sort()
        for tmpJobID in tmpJobIDList:
            tmpJobs = newJobsMap[tmpJobID]
            if tmpJobs != []:
                _logger.debug("%s start Setupper for JobID=%s" % (self.token,tmpJobID))
                # FIXME once DQ2 support changing replica owner
                #thr = Setupper(self.taskBuffer,tmpJobs,resetLocation=True)
                thr = Setupper(self.taskBuffer,tmpJobs)
                thr.start()
                thr.join()
                _logger.debug("%s end Setupper for JobID=%s" % (self.token,tmpJobID))
        # succeeded
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
    def registerNewDataset(self,dataset):
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
        # return
        return True
                    
                
            
