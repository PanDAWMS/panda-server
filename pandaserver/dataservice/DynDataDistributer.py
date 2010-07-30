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
            if self.jobs[0].processingType in ['hammercloud','gangarobot']:
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
                for tmpDS,tmpVal in sitesMaps.iteritems():
                    self.putLog("constituent DS %s" % tmpDS)
                    allCandidates = []
                    totalUserSub = 0
                    allCompPd2pSites = []
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
                        # use candidates
                        allCandidates += candSites
                    self.putLog("PD2P sites with comp replicas : %s" % str(allCompPd2pSites))
                    self.putLog("PD2P candidates : %s" % str(allCandidates))
                    self.putLog("PD2P subscriptions : %s" % totalUserSub)
                    # check number of replicas
                    maxSitesHaveDS = 1
                    if len(allCompPd2pSites) >= maxSitesHaveDS:
                        self.putLog("skip since many PD2P sites (%s>=%s) have the replica" % (len(allCompPd2pSites),maxSitesHaveDS))
                        continue
                    # check the number of subscriptions
                    maxNumSubInAllCloud = 1
                    if totalUserSub >= maxNumSubInAllCloud:
                        self.putLog("skip since enough subscriptions (%s>=%s) were already made" % \
                                    (totalUserSub,maxNumSubInAllCloud))
                        continue
                    # no candidates
                    if len(allCandidates) == 0:
                        self.putLog("skip since no candidates")
                        continue
                    # run brokerage
                    tmpJob = JobSpec()
                    tmpJob.AtlasRelease = ''
                    self.putLog("run brokerage for %s" % tmpDS)
                    brokerage.broker.schedule([tmpJob],self.taskBuffer,self.siteMapper,True,allCandidates,True)
                    self.putLog("site -> %s" % tmpJob.computingSite)
                    # make subscription
                    subRet,dq2ID = self.makeSubscription(tmpDS,tmpJob.computingSite)
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
                # check T1 has a replica
                t1HasReplica = False
                for tmpDQ2ID,tmpStatMap in tmpRepMap.iteritems():
                    if tmpDQ2ID.startswith(prefixDQ2T1):
                        if tmpStatMap[0]['total'] == tmpStatMap[0]['found']:
                            t1HasReplica = True
                            break
                # get on-going subscriptions
                timeRangeSub = 7
                userSubscriptions = self.taskBuffer.getUserSubscriptions(tmpDS,timeRangeSub)
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
    

    # get list of datasets
    def makeSubscription(self,dataset,sitename):
        # return for failuer
        retFailed = False,''
        # get DQ2 ID
        if not self.siteMapper.checkSite(sitename):
            self.putLog("cannot find SiteSpec for %s" % sitename)
            return retFailed
        dq2ID = self.siteMapper.getSite(sitename).ddm
        if dataset.startswith('data'):
            dq2ID = re.sub('_[^_]+DISK','_DATADISK',dq2ID)
        elif dataset.startswith('mc'):
            dq2ID = re.sub('_[^_]+DISK','_MCDISK',dq2ID)
        # patch for MWT2_UC
        if dq2ID == 'MWT2_UC_DATADISK':
            dq2ID = 'MWT2_DATADISK'
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


    # put log
    def putLog(self,msg,type='debug'):
        tmpMsg = self.token+' '+msg
        if type == 'error':
            _logger.error(tmpMsg)
        else:
            _logger.debug(tmpMsg)
            
                
            
