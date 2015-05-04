'''
setup cloud

'''

import re
import sys
import time
import types
import random
import commands
import datetime
import brokerage.broker_util
from DDM import ddm
from DDM import dq2Common
from DDM import toa
from DDM import rucioAPI
from config import panda_config
from taskbuffer import ProcessGroups
from pandalogger.PandaLogger import PandaLogger
import DataServiceUtils


# logger
_logger = PandaLogger().getLogger('TaskAssigner')

# cutoff for RW
thr_RW_low  = 400
thr_RW_high = 8000
thr_RW_sub  = 600

# cutoff for disk in TB
thr_space_low = 5

# special reduction for TAPE
reductionForTape = 0.5

# special weight for T1 data
specialWeightT1Data = 1

# task types using MC share
taskTypesMcShare = ['evgen']

# task types for subscriptions
taskTypesSub = ['simul']

# task types for aggregation
taskTypesAgg = ['evgen','simul']

# dataset type to ignore file availability check
datasetTypeToSkipCheck = ['log']

class TaskAssigner:
    # constructor
    def __init__(self,taskBuffer,siteMapper,taskID,prodSourceLabel,job):
        self.taskBuffer = taskBuffer
        self.siteMapper = siteMapper
        self.taskID     = taskID
        self.cloudTask  = None
        self.prodSourceLabel = prodSourceLabel
        self.cloudForSubs = []
        self.job = job
        self.metadataMap = {}
        self.contDsMap = {}
        

    # check cloud
    def checkCloud(self):
        try:
            _logger.info('%s checkCloud' % self.taskID)
            # get CloudTask from DB
            self.cloudTask = self.taskBuffer.getCloudTask(self.taskID)
            if self.cloudTask == None:
                _logger.error('%s cannot get CloudTask' % self.taskID)
                return None
            # if already assigned
            if self.cloudTask.status == 'assigned':
                _logger.info('%s checked Cloud -> %s' % (self.taskID,self.cloudTask.cloud))
                return self.cloudTask.cloud
            # return "" to set cloud later
            _logger.info('%s return Cloud=""' % self.taskID)
            return ""
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("%s checkCloud : %s %s" % (self.taskID,type,value))
            return None


    # set cloud
    def setCloud(self,lfns,guids,locations={},metadata=None,fileCounts=None,
                 dsSizeMap=None):
        try:
            _logger.info('%s setCloud' % self.taskID)
            _logger.info('%s metadata="%s"' % (self.taskID,metadata))
            _logger.info('%s fileCounts="%s"' % (self.taskID,fileCounts))            
            _logger.info('%s dsSizeMap="%s"' % (self.taskID,dsSizeMap))
            taskType = None
            RWs      = {}
            expRWs   = {}
            highRWs  = {}
            prioMap  = {}
            fullRWs  = {}
            tt2Map   = {}
            diskCount = 0
            usingOpenDS = False
            try:
                # parse metadata
                if not metadata in (None,'NULL'):
                    # task type
                    taskType = metadata.split(';')[0]
                    # RWs
                    exec "RWs = %s"     % metadata.split(';')[1]
                    # expected RWs
                    exec "expRWs = %s"  % metadata.split(';')[2]
                    # RWs for high priority tasks
                    exec "prioMap = %s" % metadata.split(';')[3]
                    # full RWs for space calcuration
                    exec "fullRWs = %s" % metadata.split(';')[4]
                    # tasktype2 map
                    exec "tt2Map = %s"  % metadata.split(';')[5]
            except:
                pass
            try:
                diskCount = int(self.job.maxDiskCount)
            except:
                pass
            message = '%s taskType==%s prio==%s RW==%s DiskCount==%s' % (self.taskID,taskType,prioMap[self.taskID],
                                                                         expRWs[self.taskID],diskCount)
            _logger.info(message)
            self.sendMesg(message)
            _logger.info('%s RWs     = %s' % (self.taskID,str(RWs)))
            _logger.info('%s expRWs  = %s' % (self.taskID,str(expRWs)))
            _logger.info('%s prioMap = %s' % (self.taskID,str(prioMap)))            
            _logger.info('%s fullRWs = %s' % (self.taskID,str(fullRWs)))
            _logger.info('%s tt2Map  = %s' % (self.taskID,str(tt2Map)))
            # get total input size
            totalInputSize = 0
            if dsSizeMap != None:
                for tmpDatasetName,tmpDatasetSize in dsSizeMap.iteritems():
                    if not DataServiceUtils.isDBR(tmpDatasetName):
                        totalInputSize += tmpDatasetSize
                # in GB
                totalInputSize = totalInputSize / 1024 / 1024 / 1024
            # get cloud list
            cloudList = self.siteMapper.getCloudList()
            # get pilot statistics
            nWNmap = self.taskBuffer.getCurrentSiteData()
            # get process group
            myTaskGroup = ProcessGroups.getProcessGroup(tt2Map[self.taskID])
            # recalculate RWs
            for tmpTaskID,tmpExpRW in expRWs.iteritems():
                # skip myself
                if tmpTaskID == self.taskID:
                    continue
                # get cloud from DB
                tmpCloudInDB = self.taskBuffer.seeCloudTask(tmpTaskID)
                # not assigned
                if tmpCloudInDB == '':
                    continue
                # increase full RW
                if not fullRWs.has_key(tmpCloudInDB):
                    fullRWs[tmpCloudInDB] = 0
                fullRWs[tmpCloudInDB] += tmpExpRW
                # no priority info
                if not prioMap.has_key(tmpTaskID):
                    continue
                # lower priority
                if prioMap[tmpTaskID] < prioMap[self.taskID]:
                    continue
                # check tasktype2
                tmpTaskGroup = ProcessGroups.getProcessGroup(tt2Map[tmpTaskID])
                # check tasktype2
                if tmpTaskGroup != myTaskGroup:
                    continue
                # increase RW
                if not RWs.has_key(tmpCloudInDB):
                    RWs[tmpCloudInDB] = 0
                RWs[tmpCloudInDB] += tmpExpRW
            _logger.info('%s newRWs  =%s' % (self.taskID,str(RWs)))
            _logger.info('%s fullRWs =%s' % (self.taskID,str(fullRWs)))            
            # remove offline clouds and check validation/fasttrack
            tmpCloudList = []
            badClouds = []
            for tmpCloudName in cloudList:
                # get cloud
                tmpCloud = self.siteMapper.getCloud(tmpCloudName)
                # skip offline clouds
                if not tmpCloud['status'] in ['online']:
                    message = '%s    %s skip : status==%s' % (self.taskID,tmpCloudName,tmpCloud['status'])
                    _logger.info(message)
                    self.sendMesg(message)
                    badClouds.append(tmpCloudName)
                    continue
                # skip non-validation cloud if validation
                if self.prodSourceLabel in ['validation'] and tmpCloud['validation'] != 'true':
                    message = "%s    %s skip : validation=='%s'" % (self.taskID,tmpCloudName,tmpCloud['validation'])
                    _logger.info(message)
                    self.sendMesg(message)
                    continue
                # check fast track
                if ((taskType in ['evgen'] and prioMap[self.taskID] >= 700) or
                    (taskType in ['simul'] and prioMap[self.taskID] >= 800)) and tmpCloud['fasttrack'] != 'true':
                    message = "%s    %s skip : fasttrack=='%s'" % (self.taskID,tmpCloudName,tmpCloud['fasttrack'])
                    _logger.info(message)
                    self.sendMesg(message)
                    continue
                # check disk count
                if diskCount != 0: 
                    enoughSpace = self.checkDiskCount(diskCount,tmpCloudName)
                    if not enoughSpace:
                        message = "%s    %s skip : no online sites have enough space for DiskCount==%s" % (self.taskID,tmpCloudName,diskCount)
                        _logger.info(message)
                        self.sendMesg(message,msgType='warning')
                        continue
                # append
                tmpCloudList.append(tmpCloudName)
                self.cloudForSubs.append(tmpCloudName)
            goodClouds = tmpCloudList
            cloudList = goodClouds + badClouds
            # DQ2 location info
            _logger.info('%s DQ2 locations %s' % (self.taskID,str(locations)))
            # check immutable datasets
            for tmpDataset,tmpSites in locations.iteritems():
                sitesForRefresh = []
                for tmpSite in tmpSites.keys():
                    tmpStat = tmpSites[tmpSite][-1]
                    if tmpStat['total'] == -1 or tmpStat['found'] ==  None:
                        sitesForRefresh.append(tmpSite)
                    elif tmpStat['immutable'] == 0:
                        # using open datasets
                        usingOpenDS = True
                        _logger.info('%s open dataset : %s' % (self.taskID,tmpDataset))
                # refresh replica info
                if sitesForRefresh != []:
                    # invoke listFileReplicasBySites to refresh replica info
                    _logger.info('%s listFileReplicasBySites %s:%s' % (self.taskID,tmpDataset,str(sitesForRefresh)))
                    tmpStat,tmpOut = ddm.DQ2_iter.listFileReplicasBySites(tmpDataset,0,sitesForRefresh,0,300)
                    _logger.info('%s listFileReplicasBySites end with %s:%s' % (self.taskID,tmpStat,tmpOut))
                    # reset tmod to shorten retry interval
                    self.taskBuffer.resetTmodCloudTask(self.taskID)
            removedDQ2Map = {}
            t2ListForMissing = {}
            diskCopyCloud = None
            badMetaMap = {}
            weightWithData = {}
            if locations != {}:
                # sort datasets by the number of sites
                numSitesDatasetMap = {}
                for dataset,sites in locations.iteritems():
                    numSites = len(sites)
                    if not numSitesDatasetMap.has_key(numSites):
                        numSitesDatasetMap[numSites] = []
                    numSitesDatasetMap[numSites].append(dataset)    
                numSitesList = numSitesDatasetMap.keys()
                numSitesList.sort()
                sortedDatasetList = []
                for numSites in numSitesList:
                    sortedDatasetList += numSitesDatasetMap[numSites]
                # loop over datasets starting with fewer replicas
                removedCloud = []
                for dataset in sortedDatasetList:
                    sites = locations[dataset]
                    tmpDiskCopyCloud = []
                    removedDQ2Map[dataset] = []
                    _logger.info('%s DS:%s' % (self.taskID,dataset))
                    datasetType = DataServiceUtils.getDatasetType(dataset)
                    for tmpCloudName in cloudList:
                        useCacheT1 = False
                        tmpCloud = self.siteMapper.getCloud(tmpCloudName)
                        if DataServiceUtils.isCachedFile(dataset,self.siteMapper.getSite(tmpCloud['source'])):
                            # use site's endpoint for CVMFS cache 
                            foundSE  = self.siteMapper.getSite(tmpCloud['source']).ddm
                            tmpDiskCopyCloud.append(tmpCloudName)
                            # using cached files at T1
                            useCacheT1 = True
                        else:    
                            # look for T1 SE which holds the max number of files
                            minFound = -1
                            foundSE  = ''
                            for tmpSePat in tmpCloud['tier1SE']:
                                # make regexp pattern 
                                if '*' in tmpSePat:
                                    tmpSePat = tmpSePat.replace('*','.*')
                                tmpSePat = '^' + tmpSePat +'$'
                                for tmpSE in sites.keys():
                                    # check name with regexp pattern
                                    if re.search(tmpSePat,tmpSE) == None:
                                        continue
                                    # check metadata
                                    metaOK = self.checkMetadata(dataset,tmpSE)
                                    if not metaOK:
                                        if not badMetaMap.has_key(dataset):
                                            badMetaMap[dataset] = []
                                        badMetaMap[dataset].append(tmpSE)    
                                        _logger.info('%s skip %s due to ToBeDeleted' % (self.taskID,tmpSE))
                                        continue
                                    # check the number of available files
                                    tmpStat = sites[tmpSE][-1]
                                    if tmpStat['found'] == None:
                                        if minFound == -1:
                                            foundSE  = tmpSE
                                    elif minFound < tmpStat['found']:
                                        minFound = tmpStat['found']
                                        foundSE  = tmpSE
                                    # check if disk copy is available
                                    tmpStatusSE,tmpRetSE = toa.getSiteProperty(tmpSE,'tape')
                                    if tmpRetSE != 'True':
                                        if tmpStat['found'] != None and tmpStat['found'] == tmpStat['total']:
                                            tmpDiskCopyCloud.append(tmpCloudName)
                                    else:
                                        _logger.info('%s %s is on tape : %s' % (self.taskID,tmpSE,tmpRetSE))
                        # get list of T2s where dataset is available
                        tmpT2List = []
                        tmpT2Map = DataServiceUtils.getSitesWithDataset(dataset,self.siteMapper,locations,
                                                                         tmpCloudName,True,getDQ2ID=True,
                                                                         useOnlineSite=True)
                        for tmpT2Name,tmpT2DQ2List in tmpT2Map.iteritems():
                            # skip redundant lookup
                            if t2ListForMissing.has_key(tmpCloudName) and \
                                   not tmpT2Name in t2ListForMissing[tmpCloudName]:
                                continue
                            # loop over all DQ2 IDs
                            for tmpT2DQ2 in tmpT2DQ2List:
                                # check metadata
                                metaOK = self.checkMetadata(dataset,tmpT2DQ2)
                                if metaOK:
                                    tmpT2List.append(tmpT2Name)
                                    break
                                else:
                                    if not badMetaMap.has_key(dataset):
                                        badMetaMap[dataset] = []
                                    badMetaMap[dataset].append(tmpT2DQ2)
                                    _logger.info('%s skip %s due to ToBeDeleted' % (self.taskID,tmpT2DQ2))
                        # take CVMFS cache into account            
                        tmpT2CacheList = DataServiceUtils.getSitesWithCacheDS(tmpCloudName,tmpT2List,self.siteMapper,dataset)
                        tmpT2List += tmpT2CacheList
                        # remove cloud if T1SE or T2 is not a location
                        if foundSE == '':
                            # keep if T2 has the dataset
                            if tmpT2List == []:
                                if not tmpCloudName in removedCloud:
                                    _logger.info('%s   removed %s' % (self.taskID,tmpCloudName))
                                    removedCloud.append(tmpCloudName)
                            # add dataset to map for subscription when T2 has non-cached replica       
                            if (tmpT2List != [] and len(tmpT2CacheList) != len(tmpT2List)) and not tmpCloudName in removedDQ2Map[dataset]:
                                removedDQ2Map[dataset].append(tmpCloudName)
                        else:
                            if not DataServiceUtils.isDBR(dataset):
                                if not tmpCloudName in weightWithData:
                                    weightWithData[tmpCloudName] = 0
                                if dsSizeMap != None and dataset in dsSizeMap:
                                    weightWithData[tmpCloudName] += dsSizeMap[dataset]
                            if not useCacheT1:
                                # check incomplete or not
                                tmpStat = sites[foundSE][-1]
                                if tmpStat['found'] == None or \
                                       (not datasetType in datasetTypeToSkipCheck and tmpStat['found'] < tmpStat['total']):
                                    # add dataset to map which is subscribed when the task is used due to T2 files
                                    if not tmpCloudName in removedDQ2Map[dataset]:
                                        removedDQ2Map[dataset].append(tmpCloudName)
                        # aggregate T2 list
                        if not t2ListForMissing.has_key(tmpCloudName):
                            t2ListForMissing[tmpCloudName] = tmpT2List
                        else:
                            # use sites where all datasets are available
                            newTmpT2List = []
                            for tmpT2 in t2ListForMissing[tmpCloudName]:
                                if tmpT2 in tmpT2List:
                                    newTmpT2List.append(tmpT2)
                            t2ListForMissing[tmpCloudName] = newTmpT2List
                    # disk copy cloud
                    if diskCopyCloud == None:
                        diskCopyCloud = tmpDiskCopyCloud
                    else:
                        newDiskCopyCloud = []
                        for tmpCloudName in diskCopyCloud:
                            if tmpCloudName in tmpDiskCopyCloud:
                                newDiskCopyCloud.append(tmpCloudName)
                        diskCopyCloud = newDiskCopyCloud        
                # remove clouds
                for tmpCloudName in removedCloud:
                    if tmpCloudName in cloudList:
                        #cloudList.remove(tmpCloudName)
                        pass
            #_logger.info('%s new locations after DQ2 filter %s' % (self.taskID,str(cloudList)))
            #_logger.info('%s clouds where complete disk copies are available %s' % (self.taskID,str(diskCopyCloud)))
            #_logger.info('%s removed DQ2 map %s' % (self.taskID,str(removedDQ2Map)))
            if cloudList == []:
                # make subscription to empty cloud
                if taskType in taskTypesSub:
                    _logger.info('%s makeSubscription start' % self.taskID)                    
                    retSub = self.makeSubscription(removedDQ2Map,RWs,fullRWs,expRWs)
                    _logger.info('%s makeSubscription end with %s' % (self.taskID,retSub))
                # make subscription for aggregation
                if taskType in taskTypesAgg:
                    # check if input is container
                    inputIsContainer = False
                    for tmpDS in removedDQ2Map.keys():
                        if tmpDS.endswith('/'):
                            inputIsContainer = True
                            break
                    # only for container
                    if inputIsContainer:
                        _logger.info('%s Aggregation start' % self.taskID)
                        retSub = self.makeSubscription(removedDQ2Map,RWs,fullRWs,expRWs,aggregation=True)
                        _logger.info('%s Aggregation end with %s' % (self.taskID,retSub))
                message = '%s no input data locations' % self.taskID
                self.sendMesg(message,msgType='warning')
                raise RuntimeError, '%s cloud list is empty after DQ2 filter' % self.taskID
            message = '%s input data size per cloud %s' % (self.taskID,str(weightWithData))
            _logger.info(message)
            self.sendMesg(message)
            if weightWithData == {}:
                # use all good clouds if no T1 has data
                cloudList = goodClouds
                _logger.info('%s use all good clouds since no T1 has data' % self.taskID)
            else:
                cloudList = []
                # use only clouds that have complete/incomplete data
                for tmpCloud in weightWithData.keys():
                    if tmpCloud in goodClouds:
                        cloudList.append(tmpCloud)
                _logger.info('%s use clouds where data is fullly or partially available' % self.taskID)
            # loop over all cloud
            weightParams = {}
            foundCandidateWithT1 = []
            candidatesUsingT2 = []
            for tmpCloudName in cloudList:
                _logger.info('%s calculate weight for %s' % (self.taskID,tmpCloudName))
                # add missing cloud in RWs
                if not RWs.has_key(tmpCloudName):
                    RWs[tmpCloudName] = 0
                if not fullRWs.has_key(tmpCloudName):
                    fullRWs[tmpCloudName] = 0
                # get cloud
                tmpCloud = self.siteMapper.getCloud(tmpCloudName)
                # patch for TW
                if cloudList == ['TW']:
                    tmpCloud['mcshare'] = 1
                weightParams[tmpCloudName] = {}            
                # get T1 site
                tmpT1Site = self.siteMapper.getSite(tmpCloud['source'])
                # get number of running jobs. Initially set 1 to avoid zero dividing
                nPilot = 1
                for siteName in tmpCloud['sites']:
                    if nWNmap.has_key(siteName):
                        nPilot += (nWNmap[siteName]['getJob'] + nWNmap[siteName]['updateJob'])
                weightParams[tmpCloudName]['nPilot'] = nPilot
                _logger.info('%s  # of pilots %s' % (self.taskID,nPilot))
                # available space
                tmpMap = rucioAPI.getRseUsage(tmpT1Site.ddm)
                weightParams[tmpCloudName]['freeSpace'] = tmpMap['free']
                # take volume of secondary data into account
                tmpMap = rucioAPI.getRseUsage(tmpT1Site.ddm, 'expired')
                weightParams[tmpCloudName]['secSpace'] = tmpMap['used']
                _logger.info('{0}  T1 space    free:{1}GB secondary:{2}GB'.format(self.taskID,weightParams[tmpCloudName]['freeSpace'],
                                                                                  weightParams[tmpCloudName]['secSpace']))
                # MC share
                weightParams[tmpCloudName]['mcshare'] = tmpCloud['mcshare']
                _logger.info('%s  MC share    %s' % (self.taskID,tmpCloud['mcshare']))
                # calculate available space = totalT1space - ((RW(cloud)+RW(thistask))*GBperSI2kday))
                totalSpace = weightParams[tmpCloudName]['freeSpace'] + weightParams[tmpCloudName]['secSpace']
                aveSpace,sizeCloud,sizeThis = self.getAvailableSpace(totalSpace,
                                                                     fullRWs[tmpCloudName],
                                                                     expRWs[self.taskID])
                # no task is assigned if available space is less than the limit
                aveNG = aveSpace < (thr_space_low * 1024 * tmpCloud['mcshare'])
                freeNG = weightParams[tmpCloudName]['freeSpace'] < (thr_space_low * 1024 * tmpCloud['mcshare'])
                if aveNG or freeNG:
                    if aveNG:
                        message = '%s    %s skip : space:%sGB (free+secondary:%s - assigned:%s - this:%s) < %s(mcshare) x %sTB' % \
                            (self.taskID,tmpCloudName,aveSpace,totalSpace,
                             sizeCloud,sizeThis,tmpCloud['mcshare'],thr_space_low)
                    else:
                        message = '%s    %s skip : free space:%sGB  < %s(mcshare) x %sTB' % \
                            (self.taskID,tmpCloudName,weightParams[tmpCloudName]['freeSpace'],tmpCloud['mcshare'],thr_space_low)
                    _logger.info(message)
                    self.sendMesg(message,msgType='warning')
                    del weightParams[tmpCloudName]                    
                    continue
                else:
                    _logger.info('%s    %s pass : space:%sGB (free+secondary:%s - assigned:%s - this:%s)' % \
                                  (self.taskID,tmpCloudName,aveSpace,totalSpace,
                                   sizeCloud,sizeThis))
                # not assign tasks when RW is too high
                if RWs.has_key(tmpCloudName) and RWs[tmpCloudName] > thr_RW_high*weightParams[tmpCloudName]['mcshare']:
                    message = '%s    %s skip : too high RW==%s > %s' % \
                              (self.taskID,tmpCloudName,RWs[tmpCloudName],thr_RW_high*weightParams[tmpCloudName]['mcshare'])
                    _logger.info(message)
                    self.sendMesg(message,msgType='warning')
                    del weightParams[tmpCloudName]
                    continue
                # set
                if tmpCloudName in weightWithData:
                    weightParams[tmpCloudName]['dsSize'] = int(weightWithData[tmpCloudName] / 1024 / 1024 / 1024)
                else:
                    weightParams[tmpCloudName]['dsSize'] = 0
                _logger.info('%s  data size at T1 %sGB' % (self.taskID,weightParams[tmpCloudName]['dsSize']))
                foundCandidateWithT1.append(tmpCloudName)
            # compare parameters
            maxClouds = []
            useMcShare = False
            # use clouds where T1 have the data
            maxClouds += foundCandidateWithT1
            # check RW
            _logger.info('%s check RW' % self.taskID)                
            tmpInfClouds = []
            for cloudName in maxClouds:
                # set weight to infinite when RW is too low
                if not taskType in taskTypesMcShare:
                    if RWs[cloudName] < thr_RW_low*weightParams[cloudName]['mcshare']:
                        message = '%s    %s infinite weight : RW==%s < %s' % \
                                  (self.taskID,cloudName,RWs[cloudName],thr_RW_low*weightParams[cloudName]['mcshare'])
                        _logger.info(message)
                        self.sendMesg(message)
                        tmpInfClouds.append(cloudName)
            # use new list
            if tmpInfClouds != []:
                _logger.info('%s use infinite clouds after RW checking' % self.taskID)
                maxClouds  = tmpInfClouds
                useMcShare = True
            elif maxClouds == []:
                messageEnd = '%s no candidates left' % self.taskID
                self.sendMesg(messageEnd)
                # make subscription to empty cloud
                if taskType in taskTypesSub:
                    _logger.info('%s makeSubscription start' % self.taskID)                    
                    retSub = self.makeSubscription(removedDQ2Map,RWs,fullRWs,expRWs)
                    _logger.info('%s makeSubscription end with %s' % (self.taskID,retSub))
                    if retSub:
                        message = '%s made subscription' % self.taskID
                        self.sendMesg(message,msgType='info')                        
                    else:
                        message = "%s didn't make subscription" % self.taskID
                        self.sendMesg(message,msgType='warning')
                # make subscription for aggregation
                """
                if taskType in taskTypesAgg:
                    # check if input is container
                    inputIsContainer = False
                    for tmpDS in removedDQ2Map.keys():
                        if tmpDS.endswith('/'):
                            inputIsContainer = True
                            break
                    # only for container
                    if inputIsContainer:
                        _logger.info('%s Aggregation start' % self.taskID)
                        retSub = self.makeSubscription(removedDQ2Map,RWs,fullRWs,expRWs,aggregation=True)
                        _logger.info('%s Aggregation end with %s' % (self.taskID,retSub))
                """        
                # return
                _logger.info(messageEnd)
                _logger.info("%s end" % self.taskID) 
                return None
            # choose one
            message = '%s candidates %s' % (self.taskID,str(maxClouds))
            _logger.info(message)
            self.sendMesg(message)
            if len(maxClouds) == 1:
                definedCloud = maxClouds[0]
            elif len(maxClouds) > 1:
                # choose cloud according to weight
                nWeightList = []
                totalWeight = 0
                _logger.info('%s weight list' % self.taskID)
                for cloudName in maxClouds:
                    if (taskType in taskTypesMcShare):
                        # use MC share for evgen
                        tmpWeight = float(weightParams[cloudName]['mcshare'])
                        message = "%s %s weight==%s" % (self.taskID,cloudName,weightParams[cloudName]['mcshare'])
                    else:
                        # use nPilot/RW*MCshare
                        tmpWeight = float(weightParams[cloudName]['nPilot']) / float(1+RWs[cloudName])
                        message = "%s %s weight==%s/%s" % (self.taskID,cloudName,
                                                          weightParams[cloudName]['nPilot'],
                                                          1+RWs[cloudName])
                    # use different weight if DISK is available
                    if diskCopyCloud != None and diskCopyCloud != [] and cloudName not in diskCopyCloud:
                        tmpWeight *= float(reductionForTape)
                        message += '*%s' % reductionForTape
                    # special weight for T1 data
                    tmpWeight /= (int((totalInputSize-weightParams[cloudName]['dsSize']) / specialWeightT1Data) + 1)
                    message += '/(({0}-{1})/{2}GB+1)'.format(totalInputSize,
                                                             weightParams[cloudName]['dsSize'],
                                                             specialWeightT1Data)
                    self.sendMesg(message)
                    nWeightList.append(tmpWeight)
                    totalWeight += tmpWeight
                    _logger.info(message)
                # check total weight
                if totalWeight == 0:
                    raise RuntimeError, 'totalWeight=0'
                # determin cloud using random number
                rNumber = random.random() * totalWeight
                _logger.info('%s    totalW   %s' % (self.taskID,totalWeight))                
                _logger.info('%s    rNumber  %s' % (self.taskID,rNumber))
                for index,tmpWeight in enumerate(nWeightList):
                    rNumber -= tmpWeight
                    _logger.info('%s    rNumber  %s : Cloud=%s weight=%s' % 
                                  (self.taskID,rNumber,maxClouds[index],tmpWeight))
                    if rNumber <= 0:
                        definedCloud = maxClouds[index]
                        break
            # make subscription when T2 candidate is chosen
            if definedCloud in candidatesUsingT2:
                newT2DQ2Map = {}
                for tmpDS,tmpT2CloudList in removedDQ2Map.iteritems():
                    if definedCloud in tmpT2CloudList:
                        newT2DQ2Map[tmpDS] = [definedCloud]
                if newT2DQ2Map == {}:
                    _logger.error('%s no subscription map to use T2 datasets cloud=%s map=%s' % (self.taskID,definedCloud,removedDQ2Map))
                    return None
                _logger.info('%s makeSubscription to use T2 start' % self.taskID)
                retSub = self.makeSubscription(newT2DQ2Map,RWs,fullRWs,expRWs,noEmptyCheck=True,acceptInProcess=True)
                if not retSub:
                    _logger.error('%s makeSubscription to use T2 failed with %s' % (self.taskID,retSub))
                    return None
                _logger.info('%s makeSubscription to use T2 end with %s' % (self.taskID,retSub))
            # set CloudTask in DB
            self.cloudTask.cloud = definedCloud
            retCloudTask = self.taskBuffer.setCloudTask(self.cloudTask)
            if retCloudTask == None:
                _logger.error('%s cannot set CloudTask' % self.taskID)
                return None
            # pin input dataset
            pinSiteList = []
            if definedCloud in candidatesUsingT2:
                # pin T2 replicas
                if t2ListForMissing.has_key(definedCloud):
                    pinSiteList = t2ListForMissing[definedCloud]
            else:
                # pin T1 replica
                pinSiteList = [self.siteMapper.getCloud(definedCloud)['tier1']]
            if pinSiteList != []:
                self.pinDataset(locations,pinSiteList,definedCloud)
            message = '%s set Cloud -> %s' % (self.taskID,retCloudTask.cloud)
            _logger.info(message)
            self.sendMesg(message)
            # return
            return retCloudTask.cloud
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("%s setCloud : %s %s" % (self.taskID,type,value))
            return None
            

    # send message to logger
    def sendMesg(self,message,msgType=None):
        try:
            # get logger
            tmpPandaLogger = PandaLogger()
            # lock HTTP handler
            tmpPandaLogger.lock()
            tmpPandaLogger.setParams({'Type':'taskbrokerage'})
            # use bamboo for loggername
            if panda_config.loggername == 'prod':
                tmpLogger = tmpPandaLogger.getHttpLogger('bamboo')
            else:
                # for dev
                tmpLogger = tmpPandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            if msgType=='error':
                tmpLogger.error(message)
            elif msgType=='warning':
                tmpLogger.warning(message)
            elif msgType=='info':
                tmpLogger.info(message)
            else:
                tmpLogger.debug(message)                
            # release HTTP handler
            tmpPandaLogger.release()
        except:
            pass
        time.sleep(1)


    # check disk count
    def checkDiskCount(self,diskCount,cloud):
        scanSiteList = self.siteMapper.getCloud(cloud)['sites']
        # loop over all sites
        for tmpSiteName in scanSiteList:
            if 'test' in tmpSiteName.lower():
                continue
            # get sitespec
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # use online only
            if not tmpSiteSpec.status in ['online']:
                continue
            # no size limit
            if tmpSiteSpec.maxinputsize in [0,None,'']:
                return True
            # enough space for input
            if int(tmpSiteSpec.maxinputsize) >= int(diskCount):
                return True
        # no sites have enough space
        return False


    # get available space
    def getAvailableSpace(self,space,fullRW,expRW):
        # calculate available space = totalT1space - ((RW(cloud)+RW(thistask))*GBperSI2kday))
        sizeCloud = fullRW * 0.2
        sizeThis  = expRW * 0.2
        aveSpace = space - (sizeCloud + sizeThis)
        return aveSpace,sizeCloud,sizeThis


    # make subscription
    def makeSubscription(self,dsCloudMap,RWs,fullRWs,expRWs,noEmptyCheck=False,acceptInProcess=False,
                         aggregation=False):
        nDDMtry = 3
        cloudList = []
        # collect clouds which don't hold datasets
        message = '%s possible clouds : %s' % (self.taskID,str(self.cloudForSubs))
        _logger.info(message)
        for tmpDS,tmpClouds in dsCloudMap.iteritems():
            for tmpCloud in tmpClouds:
                if (not tmpCloud in cloudList) and tmpCloud in self.cloudForSubs:
                    cloudList.append(tmpCloud)
        if aggregation:
            cloudList = self.cloudForSubs
        message = '%s candidates for subscription : %s' % (self.taskID,str(cloudList))
        _logger.info(message)
        self.sendMesg(message)
        if cloudList == []:
            _logger.info('%s no candidates for subscription' % self.taskID)            
            return False
        # get DN
        com = 'unset LD_LIBRARY_PATH; unset PYTHONPATH; export PATH=/usr/local/bin:/bin:/usr/bin; '
        com+= 'source %s; grid-proxy-info -subject' % panda_config.glite_source
        status,DN = commands.getstatusoutput(com)
        _logger.info('%s %s' % (self.taskID,DN))
        # ignore AC issuer
        if re.search('WARNING: Unable to verify signature!',DN) != None:
            status = 0
        if status != 0:
            _logger.error('%s could not get DN %s:%s' % (self.taskID,status,DN))
            return False
        # check if there is in-process subscription
        if not acceptInProcess:
            # remove /CN=proxy and /CN=limited from DN
            DN = DN.split('\n')[-1]
            DN = re.sub('(/CN=proxy)+$','',DN)
            DN = re.sub('/CN=limited proxy','',DN)
            status,out = dq2Common.parse_dn(DN)
            if status != 0:
                _logger.error('%s could not truncate DN %s:%s' % (self.taskID,status,DN))
                return False
            DN = out
            # loop over all datasets
            runningSub = {}
            for datasetName,tmpClouds in dsCloudMap.iteritems():
                if datasetName.endswith('/'):
                    tmpStat,repMap = self.getListDatasetReplicasInContainer(datasetName)
                else:
                    tmpStat,repMap = True,{datasetName:[]}
                if not tmpStat:
                    _logger.info('%s failed to get datasets in %s ' % (self.taskID,tmpDsName))
                    continue
                # loop over all constituents
                for tmpDS in repMap.keys():
                    # get running subscriptions
                    runningSub[tmpDS] = []
                    _logger.info('%s listSubscriptions(%s)' % (self.taskID,tmpDS))
                    iTry = 0
                    while True:
                        status,outLoc = ddm.DQ2.listSubscriptions(tmpDS)
                        # succeed
                        if status == 0:
                            break
                        # failed
                        iTry += 1
                        if iTry < nDDMtry:
                            time.sleep(30)
                        else:
                            _logger.error('%s %s' % (self.taskID,outLoc))
                            return False
                    _logger.info('%s %s %s' % (self.taskID,status,outLoc))                
                    time.sleep(1)
                    # get subscription metadata
                    exec "outLoc = %s" % outLoc
                    for tmpLocation in outLoc:
                        t1Flag = False
                        # check T1 or not
                        for tmpCloudName4T1 in self.siteMapper.getCloudList():
                            if tmpLocation in self.siteMapper.getCloud(tmpCloudName4T1)['tier1SE']:
                                t1Flag = True
                                break
                        # skip non-T1
                        if not t1Flag:
                            continue
                        _logger.info('%s listSubscriptionInfo(%s,%s)' % (self.taskID,tmpDS,tmpLocation))
                        iTry = 0
                        while True:
                            status,outMeta = ddm.DQ2.listSubscriptionInfo(tmpDS,tmpLocation,0)
                            # succeed
                            if status == 0:
                                break
                            # skip non-existing ID
                            if re.search('not a Tiers of Atlas Destination',outMeta) != None:
                                _logger.info('%s ignore %s' % (self.taskID,outMeta.split('\n')[-1]))
                                status = 0
                                outMeta = "()"
                                break
                            # failed
                            iTry += 1
                            if iTry < nDDMtry:
                                time.sleep(30)
                            else:
                                _logger.error('%s %s' % (self.taskID,outMeta))
                                return False
                        _logger.info('%s %s %s' % (self.taskID,status,outMeta))
                        time.sleep(1)                
                        # look for DN in metadata
                        exec "outMeta = %s" % outMeta
                        if DN in outMeta:
                            # get corrosponding cloud
                            for tmpCloudName in self.siteMapper.getCloudList():
                                tmpCloudSpec = self.siteMapper.getCloud(tmpCloudName)
                                if tmpLocation in tmpCloudSpec['tier1SE']:
                                    # append
                                    if not tmpCloudName in runningSub[tmpDS]:
                                        runningSub[tmpDS].append(tmpCloudName)
                                    break
            _logger.info('%s runningSub=%s' % (self.taskID,runningSub))
            # doesn't make subscriptions when another subscriptions is in process
            subThr = 1
            for tmpDS,tmpClouds in runningSub.iteritems():
                if len(tmpClouds) > 0:
                    message = '%s subscription:%s to %s in process' % (self.taskID,tmpDS,str(tmpClouds))
                    _logger.info(message)
                    self.sendMesg(message)
                    return False
        # get size of datasets
        dsSizeMap = {}
        for tmpDS in dsCloudMap.keys():
            _logger.debug('%s listFilesInDataset(%s)' % (self.taskID,tmpDS))
            iTry = 0
            while True:
                status,outList = ddm.DQ2.listFilesInDataset(tmpDS)
                # succeed
                if status == 0:
                    break
                # failed
                iTry += 1
                if iTry < nDDMtry:
                    time.sleep(30)
                else:
                    _logger.error('%s %s %s' % (self.taskID,status,outList))
                    return False
            # get total size
            dsSizeMap[tmpDS] = 0
            exec "outList = %s" % outList
            for guid,vals in outList[0].iteritems():
                try:
                    dsSizeMap[tmpDS] += long(vals['filesize'])
                except:
                    pass
            # GB
            _logger.info('%s %s %sB' % (self.taskID,tmpDS,dsSizeMap[tmpDS]))
            dsSizeMap[tmpDS] /= (1024*1024*1024)
        _logger.info('%s dsSize=%s' % (self.taskID,dsSizeMap))
        # check space and RW
        minRW    = None
        minCloud = None
        for tmpCloudName in cloudList:
            # get cloud spec
            tmpCloudSpec = self.siteMapper.getCloud(tmpCloudName)
            # get T1 site
            tmpT1Site = self.siteMapper.getSite(tmpCloudSpec['source'])
            # calculate available space
            if not fullRWs.has_key(tmpCloudName):
                fullRWs[tmpCloudName] = 0
            aveSpace,sizeCloud,sizeThis = self.getAvailableSpace(tmpT1Site.space,
                                                                 fullRWs[tmpCloudName],
                                                                 expRWs[self.taskID])
            # reduce requred space
            for tmpDS,tmpClouds in dsCloudMap.iteritems():
                if tmpCloudName in tmpClouds:
                    aveSpace -= dsSizeMap[tmpDS]
            # check space
            if aveSpace < thr_space_low:
                message = '%s    %s skip : space==%s total==%s' % (self.taskID,tmpCloudName,aveSpace,
                                                                   tmpT1Site.space)
                _logger.info(message)
                self.sendMesg(message,msgType='warning')                
                continue
            _logger.info('%s    %s pass : space==%s total==%s' % (self.taskID,tmpCloudName,aveSpace,
                                                                   tmpT1Site.space))
            # get cloud spec
            tmpCloudSpec = self.siteMapper.getCloud(tmpCloudName)
            # check MC share
            if tmpCloudSpec['mcshare'] == 0:
                message = '%s    %s skip : mcshare==%s' % (self.taskID,tmpCloudName,tmpCloudSpec['mcshare'])
                _logger.info(message)
                continue
            # get minimum RW
            if not RWs.has_key(tmpCloudName):
                RWs[tmpCloudName] = 0
            tmpRwThr = tmpCloudSpec['mcshare']*thr_RW_sub    
            _logger.info('%s    %s RW==%s Thr==%s' % (self.taskID,tmpCloudName,RWs[tmpCloudName],
                                                       tmpRwThr))
            tmpRwRatio = float(RWs[tmpCloudName])/float(tmpRwThr)
            if minRW == None or minRW > tmpRwRatio:
                minRW    = tmpRwRatio
                minCloud = tmpCloudName
        # check RW
        if minCloud == None:
            message = '%s no candidates left for subscription' % self.taskID
            _logger.info(message)
            self.sendMesg(message)
            return False
        # get cloud spec
        tmpCloudSpec = self.siteMapper.getCloud(minCloud)
        # check threshold
        if minRW > 1.0 and not noEmptyCheck and not aggregation:
            message = '%s no empty cloud : %s minRW==%s>%s' % \
                      (self.taskID,minCloud,RWs[minCloud],thr_RW_sub*tmpCloudSpec['mcshare'])
            _logger.info(message)
            self.sendMesg(message)
            return False
        message = '%s %s for subscription : minRW==%s' % (self.taskID,minCloud,minRW)
        _logger.info(message)
        self.sendMesg(message)
        # get cloud spec for subscription
        tmpCloudSpec = self.siteMapper.getCloud(minCloud)
        # get T1 site
        tmpT1Site = self.siteMapper.getSite(tmpCloudSpec['source'])
        # dest DQ2 ID
        dq2ID = tmpT1Site.ddm
        # make subscription
        for tmpDsName,tmpClouds in dsCloudMap.iteritems():
            # skip if the dataset already exists in the cloud
            if not aggregation and not minCloud in tmpClouds:
                _logger.info('%s %s already exists in %s' % (self.taskID,tmpDS,minCloud))
                continue
            # get constituents
            if tmpDsName.endswith('/'):
                tmpStat,repMap = self.getListDatasetReplicasInContainer(tmpDsName)
                if not tmpStat:
                    _logger.info('%s failed to get datasets in %s ' % (self.taskID,tmpDsName))
                    continue
            else:
                repMap = {tmpDsName:{dq2ID:[]}}
            # loop over all constituents
            for tmpDS in repMap.keys():
                # register subscription
                optSrcPolicy = 001000 | 010000
                _logger.debug("%s %s %s" % ('registerDatasetSubscription',(tmpDS,dq2ID),
                                            {'version':0,'archived':0,'callbacks':{},'sources':{},
                                             'sources_policy':optSrcPolicy,'wait_for_sources':0,
                                             'destination':None,'query_more_sources':0,'sshare':"secondary",
                                             'group':None,'activity':"Production",'acl_alias':'secondary'}))
                iTry = 0
                while True:
                    # execute
                    status,out = ddm.DQ2.main('registerDatasetSubscription',tmpDS,dq2ID,version=0,archived=0,callbacks={},
                                              sources={},sources_policy=optSrcPolicy,wait_for_sources=0,destination=None,
                                              query_more_sources=0,sshare="secondary",group=None,activity="Production",
                                              acl_alias='secondary')
                    # succeed
                    if status == 0 or 'DQSubscriptionExistsException' in out:
                        break
                    # failed
                    iTry += 1
                    if iTry < nDDMtry:
                        time.sleep(30)
                    else:
                        _logger.error('%s %s %s' % (self.taskID,status,out))
                        return False
                if 'DQSubscriptionExistsException' in out:    
                    _logger.info('%s %s %s' % (self.taskID,status,'DQSubscriptionExistsException'))
                else:
                    _logger.info('%s %s %s' % (self.taskID,status,out))                
                message = '%s registered subscription %s %s:%s' % (self.taskID,tmpDS,minCloud,dq2ID)
                _logger.info(message)
                self.sendMesg(message)
                time.sleep(1)
        # completed
        return True


    # pin dataset
    def pinDataset(self,locationMap,siteList,cloudName):
        _logger.info('%s start pin input datasets' % self.taskID)
        pinLifeTime = 7        
        # loop over all datasets
        for tmpDsName,tmpDQ2Map in locationMap.iteritems():
            # skip DBR
            if DataServiceUtils.isDBR(tmpDsName):
                continue
            # get DQ2 IDs in the cloud where dataset is available
            tmpDq2Map = DataServiceUtils.getSitesWithDataset(tmpDsName,self.siteMapper,locationMap,
                                                             cloudName,useHomeCloud=True,
                                                             getDQ2ID=True,
                                                             useOnlineSite=True,
                                                             includeT1=True)
            # loop over all sites
            for tmpSiteName in siteList:
                # pin dataset when the site has replicas
                if tmpDq2Map.has_key(tmpSiteName):
                    # loop over all DQ2 IDs
                    for tmpRepSite in tmpDq2Map[tmpSiteName]:
                        # get constituents
                        if tmpDsName.endswith('/'):
                            tmpStat,repMap = self.getListDatasetReplicasInContainer(tmpDsName)
                            if not tmpStat:
                                _logger.info('%s failed to get datasets in %s ' % (self.taskID,tmpDsName))
                                continue
                        else:
                            repMap = {tmpDsName:{tmpRepSite:[]}}
                        # loop over all datasets
                        for datasetName,locVal in repMap.iteritems():
                            # check missing
                            if not repMap[datasetName].has_key(tmpRepSite):
                                _logger.info('%s skip pinning for %s at %s due to missing replica' % \
                                              (self.taskID,datasetName,tmpRepSite))
                                continue
                            # get metadata
                            status,tmpMetadata = self.getReplicaMetadata(datasetName,tmpRepSite)
                            if not status:
                                continue
                            # check pin lifetime                            
                            if tmpMetadata.has_key('pin_expirationdate'):
                                if isinstance(tmpMetadata['pin_expirationdate'],types.StringType) and tmpMetadata['pin_expirationdate'] != 'None':
                                    # keep original pin lifetime if it is longer 
                                    origPinLifetime = datetime.datetime.strptime(tmpMetadata['pin_expirationdate'],'%Y-%m-%d %H:%M:%S')
                                    if origPinLifetime > datetime.datetime.utcnow()+datetime.timedelta(days=pinLifeTime):
                                        _logger.info('%s skip pinning for %s:%s due to longer lifetime %s' % (self.taskID,
                                                                                                               datasetName,tmpRepSite,
                                                                                                               tmpMetadata['pin_expirationdate']))
                                        continue
                            # set pin lifetime
                            status = self.setReplicaMetadata(datasetName,tmpRepSite,'pin_lifetime','%s days' % pinLifeTime)
        # return                
        _logger.info('%s end pin input datasets' % self.taskID)
        return


    # get replica metadata
    def getReplicaMetadata(self,datasetName,locationName):
        # use cached data
        if self.metadataMap.has_key(datasetName) and self.metadataMap[datasetName].has_key(locationName):
            return True,self.metadataMap[datasetName][locationName]
        # response for failure
        resForFailure = False,{}
        # get metadata
        nTry = 3
        for iDDMTry in range(nTry):
            status,out = ddm.DQ2.main('listMetaDataReplica',locationName,datasetName)
            if status != 0 or (not DataServiceUtils.isDQ2ok(out)):
                if 'rucio.common.exception.ReplicaNotFound' in out:
                    break
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            if not 'rucio.common.exception.ReplicaNotFound' in out:
                _logger.debug('%s %s/%s listMetaDataReplica %s %s' % (self.taskID,iDDMTry,nTry,datasetName,locationName))
                _logger.error("%s %s" % (self.taskID,out))
            return resForFailure
        metadata = {}
        try:
            # convert to map
            exec "metadata = %s" % out
        except:
            _logger.error('%s could not convert HTTP-res to replica metadata for %s:%s' % \
                          (self.taskID,datasetName,locationName))
            return resForFailure
        # append
        if not self.metadataMap.has_key(datasetName):
            self.metadataMap[datasetName] = {}
        self.metadataMap[datasetName][locationName] = metadata    
        # return
        return True,metadata


    # check metadata
    def checkMetadata(self,datasetName,tmpSE):
        try:
            # skip checking for DBR
            if DataServiceUtils.isDBR(datasetName):
                return True
            # get constituents
            if datasetName.endswith('/'):
                tmpStat,repMap = self.getListDatasetReplicasInContainer(datasetName)
                if not tmpStat:
                    raise RuntimeError, 'failed to get datasets in %s when checkMetadata' % datasetName
            else:
                repMap = {datasetName:{tmpSE:[]}}
            # loop over all datasets
            for dataset,locVal in repMap.iteritems():
                # check missing
                if not locVal.has_key(tmpSE):
                    _logger.info('%s skip %s at %s due to missing replica when checkMetadata' % (self.taskID,dataset,tmpSE))
                    # NG
                    return False
                # get metadata
                status,metaItem = self.getReplicaMetadata(dataset,tmpSE)
                if not status:
                    raise RuntimeError, 'failed to get metadata at %s for %s when checkMetadata' % (tmpSE,dataset)
                # check     
                if metaItem.has_key('archived') and isinstance(metaItem['archived'],types.StringType) \
                       and metaItem['archived'].lower() in ['tobedeleted',]:
                    _logger.info('%s skip %s due to ToBeDeleted when checkMetadata' % (self.taskID,tmpSE))
                    # NG
                    return False
        except:
            errtype,errvalue = sys.exc_info()[:2]
            if not 'failed to get metadata' in str(errvalue):
                _logger.error("%s checkMetadata : %s %s" % (self.taskID,errtype,errvalue))
            # FIXME
            #return False
        # OK
        return True


    # set replica metadata
    def setReplicaMetadata(self,datasetName,locationName,attrname,attrvalue):
        # response for failure
        resForFailure = False
        # get metadata
        nTry = 3
        for iDDMTry in range(nTry):
            _logger.debug('%s %s/%s setReplicaMetaDataAttribute %s %s %s=%s' % (self.taskID,iDDMTry,nTry,datasetName,
                                                                                locationName,attrname,attrvalue))
            status,out = ddm.DQ2.main('setReplicaMetaDataAttribute',datasetName,locationName,attrname,attrvalue)
            if status != 0 or (not DataServiceUtils.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.error("%s %s" % (self.taskID,out))
            return resForFailure
        # return
        _logger.info('%s setReplicaMetadata done for %s:%s' % (self.taskID,datasetName,locationName))
        return True


    # get list of replicas in container 
    def getListDatasetReplicasInContainer(self,container):
        # use cache
        if self.contDsMap.has_key(container):
            return True,self.contDsMap[container]
        # get datasets in container
        for iDDMTry in range(3):
            status,out = ddm.DQ2.main('listDatasetsInContainer',container)
            if status != 0 or (not DataServiceUtils.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            _logger.debug((self.taskID,'listDatasetsInContainer',container))
            _logger.error('%s %s' % (self.taskID,out))
            return False,out
        datasets = []
        try:
            # convert to list
            exec "datasets = %s" % out
        except:
            return False,out
        # loop over all datasets
        allRepMap = {}
        for dataset in datasets:
            allFileList = []
            for iDDMTry in range(3):
                status,outList = ddm.DQ2.listFilesInDataset(dataset)
                if status == 0:
                    exec "allFileList = %s[0]" % outList
                    status,out = rucioAPI.listDatasetReplicas(dataset)
                    if status != 0:
                        time.sleep(10)
                    else:
                        break
            if status != 0:
                _logger.debug((self.taskID,'listDatasetReplicas',dataset))
                _logger.error('%s %s' % (self.taskID,out))
                return False,out
            # get map
            allRepMap[dataset] = out
        # return
        self.contDsMap[container] = allRepMap
        return True,allRepMap


    
