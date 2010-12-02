'''
find candidate site to distribute input datasets

'''

import re
import sys
import time
import random
import datetime

from dataservice.DDM import ddm
from taskbuffer.JobSpec import JobSpec
import brokerage.broker

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('DynDataDistributer')

# NG datasets
ngDataTypes = ['RAW','HITS','RDO']

# excluded provenance
ngProvenance = ['GP',]

# threshold for hot dataset
nUsedForHotDS = 10


class DynDataDistributer:

    # constructor
    def __init__(self,jobs,taskBuffer,siteMapper):
        self.jobs = jobs
        self.taskBuffer = taskBuffer
        self.siteMapper = siteMapper
        self.token = datetime.datetime.utcnow().isoformat(' ')
        self.pd2pClouds = []


    # main
    def run(self):
        try:
            # get a list of PD2P clouds
            for tmpSiteName,tmpSiteSpec in self.siteMapper.siteSpecList.iteritems():
                # ignore test sites
                if 'test' in tmpSiteName.lower():
                    continue
                # analysis only
                if not tmpSiteName.startswith('ANALY'):
                    continue
                # using PD2P
                if tmpSiteSpec.cachedse == 1:
                    if not tmpSiteSpec.cloud in self.pd2pClouds:
                        self.pd2pClouds.append(tmpSiteSpec.cloud)
            self.putLog("start for %s" % self.jobs[0].PandaID)
            # check cloud
            if not self.jobs[0].cloud in self.pd2pClouds:
                self.putLog("skip cloud=%s not one of PD2P clouds %s" % (self.jobs[0].cloud,str(self.pd2pClouds)))
                self.putLog("end for %s" % self.jobs[0].PandaID)
                return
            # ignore HC and group production
            if self.jobs[0].processingType in ['hammercloud','gangarobot'] or self.jobs[0].processingType.startswith('gangarobot'):
                self.putLog("skip due to processingType=%s" % self.jobs[0].processingType)
                self.putLog("end for %s" % self.jobs[0].PandaID)
                return
            # ignore HC and group production
            if not self.jobs[0].workingGroup in ['NULL',None,'']:
                self.putLog("skip due to workingGroup=%s" % self.jobs[0].workingGroup)
                self.putLog("end for %s" % self.jobs[0].PandaID)
                return
            # get input datasets
            inputDatasets = []
            for tmpJob in self.jobs:
                if tmpJob.prodSourceLabel == 'user':
                    for tmpFile in tmpJob.Files:
                        if tmpFile.type == 'input' and not tmpFile.lfn.endswith('.lib.tgz'):
                            if not tmpFile.dataset in inputDatasets:
                                inputDatasets.append(tmpFile.dataset)
            # loop over all input datasets
            for inputDS in inputDatasets:
                # only mc or data datasets
                moveFlag = False
                for projectName in ['mc','data']:
                    if inputDS.startswith(projectName):
                        moveFlag = True
                if not moveFlag:
                    self.putLog("skip non official dataset %s" % inputDS)
                    continue
                if re.search('_sub\d+$',inputDS) != None or re.search('_dis\d+$',inputDS) != None:
                    self.putLog("skip dis/sub dataset %s" % inputDS)
                    continue
                # check type
                tmpItems = inputDS.split('.')
                if len(tmpItems) < 5:
                    self.putLog("cannot get type from %s" % inputDS)
                    continue
                if tmpItems[4] in ngDataTypes:
                    self.putLog("don't move %s : %s" % (tmpItems[4],inputDS))
                    continue
                # get candidate sites
                self.putLog("get candidates for %s" % inputDS)
                status,sitesMaps = self.getCandidates(inputDS)
                if not status:
                    self.putLog("failed to get candidates")
                    continue
                # loop over all datasets
                usedSites = []
                for tmpDS,tmpVal in sitesMaps.iteritems():
                    self.putLog("triggered for %s" % tmpDS,sendLog=True)
                    # increment used counter
                    nUsed = self.taskBuffer.incrementUsedCounterSubscription(tmpDS)
                    # collect candidates
                    allCandidates = []
                    totalUserSub = 0
                    allCompPd2pSites = []
                    allOKClouds = []
                    for tmpCloud,(candSites,sitesComDS,sitesPd2pDS,nUserSub,t1HasReplica) in tmpVal.iteritems():
                        self.putLog("%s sites with comp DS %s - compPD2P %s - candidates %s - nSub %s - T1 %s" % \
                                    (tmpCloud,str(sitesComDS),str(sitesPd2pDS),str(candSites),nUserSub,t1HasReplica))
                        # add
                        totalUserSub += nUserSub 
                        allCompPd2pSites += sitesPd2pDS
                        # no replica in the cloud
                        if sitesComDS == [] and not t1HasReplica:
                            self.putLog("unused since no replica in the cloud")
                            continue
                        # add candidates
                        for tmpCandSite in candSites:
                            if not tmpCandSite in usedSites:
                                allCandidates.append(tmpCandSite)
                        # add clouds
                        if not tmpCloud in allOKClouds:
                            allOKClouds.append(tmpCloud)
                    self.putLog("PD2P sites with comp replicas : %s" % str(allCompPd2pSites))
                    self.putLog("PD2P candidates : %s" % str(allCandidates))
                    self.putLog("PD2P # of subscriptions : %s" % totalUserSub)
                    self.putLog("PD2P nUsed : %s" % nUsed)
                    # make any data subscriptions to EOS
                    if allOKClouds != [] and inputDS.startswith('data'):
                        self.makeSubscriptionToEOS(inputDS)
                    # check number of replicas
                    maxSitesHaveDS = 1
                    if nUsed > nUsedForHotDS:
                        maxSitesHaveDS = 2
                    if len(allCompPd2pSites) >= maxSitesHaveDS:
                        self.putLog("skip since many PD2P sites (%s>=%s) have the replica" % (len(allCompPd2pSites),maxSitesHaveDS),
                                    sendLog=True)
                        continue
                    # check the number of subscriptions
                    maxNumSubInAllCloud = 1
                    if nUsed > nUsedForHotDS:
                        maxNumSubInAllCloud = 2                    
                    if totalUserSub >= maxNumSubInAllCloud:
                        self.putLog("skip since enough subscriptions (%s>=%s) were already made" % \
                                    (totalUserSub,maxNumSubInAllCloud),
                                    sendLog=True)
                        continue
                    # no candidates
                    if len(allCandidates) == 0:
                        self.putLog("skip since no candidates",sendLog=True)
                        continue
                    # get weight for brokerage
                    weightForBrokerage = self.getWeightForBrokerage(allCandidates,tmpDS)
                    self.putLog("weight %s" % str(weightForBrokerage))
                    # get free disk size
                    retFreeSizeMap,freeSizeMap = self.getFreeDiskSize(tmpDS,allCandidates)
                    if not retFreeSizeMap:
                        self.putLog("failed to get free disk size",type='error',sendLog=True)
                        continue
                    # get dataset size
                    retDsSize,dsSize = self.getDatasetSize(tmpDS)
                    if not retDsSize:
                        self.putLog("failed to get dataset size of %s" % tmpDS,type='error',sendLog=True)
                        continue
                    # run brokerage
                    tmpJob = JobSpec()
                    tmpJob.AtlasRelease = ''
                    self.putLog("run brokerage for %s" % tmpDS)
                    usedWeight = brokerage.broker.schedule([tmpJob],self.taskBuffer,self.siteMapper,True,allCandidates,
                                                           True,specialWeight=weightForBrokerage,getWeight=True,
                                                           sizeMapForCheck=freeSizeMap,datasetSize=dsSize)
                    for tmpWeightSite,tmpWeightStr in usedWeight.iteritems():
                        self.putLog("weight %s %s" % (tmpWeightSite,tmpWeightStr),sendLog=True)
                    self.putLog("site -> %s" % tmpJob.computingSite)
                    # make subscription
                    subRet,dq2ID = self.makeSubscription(tmpDS,tmpJob.computingSite)
                    self.putLog("made subscription to %s:%s" % (tmpJob.computingSite,dq2ID),sendLog=True)
                    usedSites.append(tmpJob.computingSite)
                    # update database
                    if subRet:
                        self.taskBuffer.addUserSubscription(tmpDS,[dq2ID])
            self.putLog("end for %s" % self.jobs[0].PandaID)
        except:
            errType,errValue = sys.exc_info()[:2]
            self.putLog("%s %s" % (errType,errValue),'error')


    # get candidate sites for subscription
    def getCandidates(self,inputDS):
        # return for failure
        failedRet = False,{'':{'':([],[],[],0,False)}}
        # get replica locations
        if inputDS.endswith('/'):
            # container
            status,tmpRepMaps = self.getListDatasetReplicasInContainer(inputDS)
        else:
            # normal dataset
            status,tmpRepMap = self.getListDatasetReplicas(inputDS)
            tmpRepMaps = {inputDS:tmpRepMap}
        if not status:
            # failed
            self.putLog("failed to get replica locations for %s" % inputDS,'error')
            return failedRet
        # get all sites
        allSiteMap = {}
        for tmpSiteName,tmpSiteSpec in self.siteMapper.siteSpecList.iteritems():
            # check cloud
            if not tmpSiteSpec.cloud in self.pd2pClouds:
                continue
            # ignore test sites
            if 'test' in tmpSiteName.lower():
                continue
            # analysis only
            if not tmpSiteName.startswith('ANALY'):
                continue
            # online
            if not tmpSiteSpec.status in ['online']:
                self.putLog("skip %s due to status=%s" % (tmpSiteName,tmpSiteSpec.status))
                continue
            if not allSiteMap.has_key(tmpSiteSpec.cloud):
                allSiteMap[tmpSiteSpec.cloud] = []
            allSiteMap[tmpSiteSpec.cloud].append(tmpSiteSpec)
        # loop over all datasets
        returnMap = {}
        checkedMetaMap = {}
        userSubscriptionsMap = {}
        for cloud in self.pd2pClouds:
            # DQ2 prefix of T1
            tmpT1SiteID = self.siteMapper.getCloud(cloud)['source']
            tmpT1DQ2ID  = self.siteMapper.getSite(tmpT1SiteID).ddm
            prefixDQ2T1 = re.sub('[^_]+DISK$','',tmpT1DQ2ID)
            # loop over all datasets     
            for tmpDS,tmpRepMap in tmpRepMaps.iteritems():
                candSites     = []
                sitesComDS    = []
                sitesCompPD2P = []
                # check metadata
                if not checkedMetaMap.has_key(tmpDS):
                    checkedMetaMap[tmpDS] = self.getDatasetMetadata(tmpDS)
                retMeta,tmpMetadata = checkedMetaMap[tmpDS]    
                if not retMeta:
                    self.putLog("failed to get metadata for %s" % tmpDS,'error')
                    continue
                if tmpMetadata['provenance'] in ngProvenance:
                    self.putLog("provenance=%s of %s is excluded" % (tmpMetadata['provenance'],tmpDS))
                    continue
                if tmpMetadata['hidden'] in [True,'True']:
                    self.putLog("%s is hidden" % tmpDS)
                    continue
                # check T1 has a replica
                t1HasReplica = False
                for tmpDQ2ID,tmpStatMap in tmpRepMap.iteritems():
                    if tmpDQ2ID.startswith(prefixDQ2T1):
                        if tmpStatMap[0]['total'] == tmpStatMap[0]['found']:
                            t1HasReplica = True
                            break
                # get on-going subscriptions
                timeRangeSub = 7
                if not userSubscriptionsMap.has_key(tmpDS):
                    userSubscriptionsMap[tmpDS] = self.taskBuffer.getUserSubscriptions(tmpDS,timeRangeSub)
                userSubscriptions = userSubscriptionsMap[tmpDS]
                # unused cloud
                if not allSiteMap.has_key(cloud):
                    continue
                # check sites
                nUserSub = 0
                for tmpSiteSpec in allSiteMap[cloud]:
                    # check cloud
                    if tmpSiteSpec.cloud != cloud:
                        continue
                    self.putLog(tmpSiteSpec.sitename)
                    # prefix of DQ2 ID
                    prefixDQ2 = re.sub('[^_]+DISK$','',tmpSiteSpec.ddm)
                    # skip T1
                    if prefixDQ2 == prefixDQ2T1:
                        continue
                    # check if corresponding DQ2 ID is a replica location
                    hasReplica = False
                    for tmpDQ2ID,tmpStatMap in tmpRepMap.iteritems():
                        if tmpDQ2ID.startswith(prefixDQ2):
                            if tmpStatMap[0]['total'] == tmpStatMap[0]['found']:
                                # complete
                                sitesComDS.append(tmpSiteSpec.sitename)
                                if tmpSiteSpec.cachedse == 1:
                                    sitesCompPD2P.append(tmpSiteSpec.sitename)                                    
                            hasReplica = True
                            break
                    # site doesn't have a replica
                    if (not hasReplica) and tmpSiteSpec.cachedse == 1:
                        candSites.append(tmpSiteSpec.sitename)
                    # the number of subscriptions
                    for tmpUserSub in userSubscriptions:
                        if tmpUserSub.startswith(prefixDQ2):
                            nUserSub += 1
                            break
                # append
                if not returnMap.has_key(tmpDS):
                    returnMap[tmpDS] = {}
                returnMap[tmpDS][cloud] = (candSites,sitesComDS,sitesCompPD2P,nUserSub,t1HasReplica)
        # return
        return True,returnMap

    
    # check DDM response
    def isDQ2ok(self,out):
        if out.find("DQ2 internal server exception") != -1 \
               or out.find("An error occurred on the central catalogs") != -1 \
               or out.find("MySQL server has gone away") != -1 \
               or out == '()':
            return False
        return True
    

    # get map of DQ2 IDs
    def getDQ2ID(self,sitename,dataset):
        # get DQ2 ID
        if not self.siteMapper.checkSite(sitename):
            self.putLog("cannot find SiteSpec for %s" % sitename)
            return ''
        dq2ID = self.siteMapper.getSite(sitename).ddm
        if dataset.startswith('data'):
            # data
            matchEOS = re.search('_EOS[^_]+DISK$',dq2ID)
            if matchEOS != None:
                dq2ID = re.sub('_EOS[^_]+DISK','_EOSDATADISK',dq2ID)
            else:
                dq2ID = re.sub('_[^_]+DISK','_DATADISK',dq2ID)
        elif dataset.startswith('mc'):
            # mc
            matchEOS = re.search('_EOS[^_]+DISK$',dq2ID)
            if matchEOS!= None:
                dq2ID = re.sub('_EOS[^_]+DISK','_EOSMCDISK',dq2ID)
            else:
                dq2ID = re.sub('_[^_]+DISK','_MCDISK',dq2ID)
        else:
            # unsupported prefix for subscription
            self.putLog('%s has unsupported prefix for subscription' % dataset,'error')
            return ''
        # patch for MWT2_UC
        if dq2ID == 'MWT2_UC_DATADISK':
            dq2ID = 'MWT2_DATADISK'
        # return
        return dq2ID
        

    # get list of datasets
    def makeSubscription(self,dataset,sitename,givenDQ2ID=None):
        # return for failuer
        retFailed = False,''
        # get DQ2 IDs
        if givenDQ2ID == None:
            dq2ID = self.getDQ2ID(sitename,dataset)
        else:
            dq2ID = givenDQ2ID
        if dq2ID == '':
            self.putLog("cannot find DQ2 ID for %s:%s" % (sitename,dataset))
            return retFailed
        # make subscription    
        optSrcPolicy = 001000 | 010000
        nTry = 3
        for iDDMTry in range(nTry):
            # register subscription
            self.putLog('%s/%s registerDatasetSubscription %s %s' % (iDDMTry,nTry,dataset,dq2ID))
            status,out = ddm.DQ2.main('registerDatasetSubscription',dataset,dq2ID,0,0,{},{},optSrcPolicy,
                                      0,None,0,"production",None,'Data Brokering','secondary')
            if out.find('DQSubscriptionExistsException') != -1:
                break
            elif status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result
        if out.find('DQSubscriptionExistsException') != -1:
            pass
        elif status != 0 or out.startswith('Error'):
            self.putLog(out,'error')
            self.putLog('bad DQ2 response for %s' % dataset,'error')
            return retFailed
        # update 
        self.putLog('%s %s' % (status,out))
        return True,dq2ID

            
    # get weight for brokerage
    def getWeightForBrokerage(self,sitenames,dataset):
        # return for failuer
        retFailed = False,{}
        retMap = {}
        # get the number of subscriptions for last 24 hours
        numUserSubs = self.taskBuffer.getNumUserSubscriptions()
        # loop over all sites
        for sitename in sitenames:
            # get DQ2 ID
            dq2ID = self.getDQ2ID(sitename,dataset)
            if dq2ID == '':
                self.putLog("cannot find DQ2 ID for %s:%s" % (sitename,dataset))
                return retFailed
            # append
            if numUserSubs.has_key(dq2ID):
                retMap[sitename] = numUserSubs[dq2ID]
            else:
                retMap[sitename] = 0
        # return
        return retMap


    # get free disk size
    def getFreeDiskSize(self,dataset,siteList):
        # return for failuer
        retFailed = False,{}
        # loop over all sites
        sizeMap = {}
        for sitename in siteList:
            # get DQ2 IDs
            dq2ID = self.getDQ2ID(sitename,dataset)
            if dq2ID == '':
                self.putLog("cannot find DQ2 ID for %s:%s" % (sitename,dataset))
                return retFailed
            for valueItem in ['used','total']:
                nTry = 3
                for iDDMTry in range(nTry):
                    self.putLog("%s/%s queryStorageUsage key=%s value=%s site=%s" % (iDDMTry,nTry,'srm',valueItem,dq2ID))
                    status,out = ddm.DQ2.main('queryStorageUsage','srm',valueItem,dq2ID)
                    if status != 0 or (not self.isDQ2ok(out)):
                        time.sleep(60)
                    else:
                        break
                # result    
                if status != 0 or out.startswith('Error'):
                    self.putLog(out,'error')
                    self.putLog('bad DQ2 response for %s:%s' % (dq2ID,valueItem), 'error')            
                    return retFailed
                try:
                    # convert res to map
                    exec "tmpGigaVal = %s[0]['giga']" % out
                    if not sizeMap.has_key(sitename):
                        sizeMap[sitename] = {}
                    # append
                    sizeMap[sitename][valueItem] = tmpGigaVal
                    self.putLog(out)
                except:
                    self.putLog(out,'error')            
                    self.putLog('could not convert HTTP-res to free size map for %s%s' % (dq2ID,valueItem), 'error')
                    return retFailed
        # return
        self.putLog('getFreeDiskSize done->%s' % str(sizeMap))
        return True,sizeMap
            

        
    # get list of replicas for a dataset
    def getListDatasetReplicas(self,dataset):
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog("%s/%s listDatasetReplicas %s" % (iDDMTry,nTry,dataset))
            status,out = ddm.DQ2.main('listDatasetReplicas',dataset,0,None,False)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        # result    
        if status != 0 or out.startswith('Error'):
            self.putLog(out,'error')
            self.putLog('bad DQ2 response for %s' % dataset, 'error')            
            return False,{}
        try:
            # convert res to map
            exec "tmpRepSites = %s" % out
            self.putLog('getListDatasetReplicas->%s' % str(tmpRepSites))
            return True,tmpRepSites
        except:
            self.putLog(out,'error')            
            self.putLog('could not convert HTTP-res to replica map for %s' % dataset, 'error')
            return False,{}
        
    
    # get replicas for a container 
    def getListDatasetReplicasInContainer(self,container):
        # response for failure
        resForFailure = False,{}
        # get datasets in container
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog('%s/%s listDatasetsInContainer %s' % (iDDMTry,nTry,container))
            status,out = ddm.DQ2.main('listDatasetsInContainer',container)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            self.putLog(out,'error')
            self.putLog('bad DQ2 response for %s' % container, 'error')
            return resForFailure
        datasets = []
        try:
            # convert to list
            exec "datasets = %s" % out
        except:
            self.putLog('could not convert HTTP-res to dataset list for %s' % container, 'error')
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
        self.putLog('getListDatasetReplicasInContainer done')
        return True,allRepMap            


    # get dataset metadata
    def getDatasetMetadata(self,datasetName):
        # response for failure
        resForFailure = False,{}
        metaDataAttrs = ['provenance','hidden']
        # get datasets in container
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog('%s/%s getMetaDataAttribute %s' % (iDDMTry,nTry,datasetName))
            status,out = ddm.DQ2.main('getMetaDataAttribute',datasetName,metaDataAttrs)
            if status != 0 or (not self.isDQ2ok(out)):
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            self.putLog(out,'error')
            self.putLog('bad DQ2 response for %s' % datasetName, 'error')
            return resForFailure
        metadata = {}
        try:
            # convert to map
            exec "metadata = %s" % out
        except:
            self.putLog('could not convert HTTP-res to dataset list for %s' % datasetName, 'error')
            return resForFailure
        # check whether all attributes are available
        for tmpAttr in metaDataAttrs:
            if not metadata.has_key(tmpAttr):
                self.putLog('%s is missing in %s' % (tmpAttr,str(metadata)), 'error')
                return resForFailure
        # return
        self.putLog('getDatasetMetadata -> %s' % str(metadata))
        return True,metadata


    # check subscription info
    def checkSubscriptionInfo(self,destDQ2ID,datasetName):
        resForFailure = (False,False)
        # get datasets in container
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog('%s/%s listSubscriptionInfo %s %s' % (iDDMTry,nTry,destDQ2ID,datasetName))
            status,out = ddm.DQ2.main('listSubscriptionInfo',datasetName,destDQ2ID,0)
            if status != 0:
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            self.putLog(out,'error')
            self.putLog('bad DQ2 response for %s' % datasetName, 'error')
            return resForFailure
        self.putLog(out)
        if out == '()':
            # no subscription
            retVal = False
        else:
            # already exists
            retVal = True
        self.putLog('checkSubscriptionInfo -> %s' % retVal)
        return True,retVal


    # get size of dataset
    def getDatasetSize(self,datasetName):
        self.putLog("get size of %s" % datasetName)
        resForFailure = (False,0)
        # get size of datasets
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog('%s/%s listFilesInDataset %s' % (iDDMTry,nTry,datasetName))
            status,out = ddm.DQ2.listFilesInDataset(datasetName)
            if status != 0:
                time.sleep(60)
            else:
                break
        if status != 0 or out.startswith('Error'):
            self.putLog(out,'error')
            self.putLog('bad DQ2 response to get size of %s' % datasetName, 'error')
            return resForFailure
        self.putLog(out)
        # get total size
        dsSize = 0
        try:
            exec "outList = %s" % out
            for guid,vals in outList[0].iteritems():
                dsSize += long(vals['filesize'])
        except:
            self.putLog('failed to get size from DQ2 response for %s' % datasetName, 'error')
            return resForFailure
        # GB
        dsSize /= (1024*1024*1024)
        self.putLog("dataset size = %s" % dsSize)
        return True,dsSize
        
        
    # make subscriptions to EOS 
    def makeSubscriptionToEOS(self,datasetName):
        self.putLog("start making EOS subscription for %s" % datasetName)
        destDQ2IDs = ['CERN-PROD_EOSDATADISK']
        # get dataset replica locations
        if datasetName.endswith('/'):
            statRep,replicaMaps = self.getListDatasetReplicasInContainer(datasetName)
        else:
            statRep,replicaMap = self.getListDatasetReplicas(datasetName)
            replicaMaps = {datasetName:replicaMap}
        if not statRep:
            self.putLog("failed to get replica map for EOS",type='error')
            return False
        # loop over all datasets
        for tmpDsName,replicaMap in replicaMaps.iteritems():
            # check if replica is already there
            for destDQ2ID in destDQ2IDs:
                if replicaMap.has_key(destDQ2ID):
                    self.putLog("skip EOS sub for %s:%s since replica is already there" % (destDQ2ID,tmpDsName))
                else:
                    statSubEx,subExist = self.checkSubscriptionInfo(destDQ2ID,tmpDsName)
                    if not statSubEx:
                        self.putLog("failed to check subscription for %s:%s" % (destDQ2ID,tmpDsName),type='error')
                        continue
                    # make subscription
                    if subExist:
                        self.putLog("skip EOS sub for %s:%s since subscription is already there" % (destDQ2ID,tmpDsName))                    
                    else:
                        statMkSub,retMkSub = self.makeSubscription(tmpDsName,'',destDQ2ID)
                        if statMkSub:
                            self.putLog("made subscription to %s for %s" % (destDQ2ID,tmpDsName))
                        else:
                            self.putLog("failed to make subscription to %s for %s" % (destDQ2ID,tmpDsName),type='error')
        # return
        self.putLog("end making EOS subscription for %s" % datasetName)        
        return True


    # put log
    def putLog(self,msg,type='debug',sendLog=False):
        tmpMsg = self.token+' '+msg
        if type == 'error':
            _logger.error(tmpMsg)
        else:
            _logger.debug(tmpMsg)
        # send to logger
        if sendLog:
            tmpPandaLogger = PandaLogger()
            tmpPandaLogger.lock()
            tmpPandaLogger.setParams({'Type':'pd2p'})
            tmpLog = tmpPandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            if type == 'error':
                tmpLog.error(tmpMsg)
            else:
                tmpLog.info(tmpMsg)                
            # release HTTP handler
            tmpPandaLogger.release()
            time.sleep(1)
                                                                                                                            
            
                
            
