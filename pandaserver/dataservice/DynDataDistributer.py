'''
find candidate site to distribute input datasets

'''

import re
import fnmatch
import sys
import time
import math
import random
import datetime

from pandaserver.dataservice.DDM import rucioAPI
from pandaserver.taskbuffer.JobSpec import JobSpec
import pandaserver.brokerage.broker

from pandaserver.config import panda_config
from pandacommon.pandalogger.PandaLogger import PandaLogger

try:
    long
except NameError:
    long = int

# logger
_logger = PandaLogger().getLogger('DynDataDistributer')

def initLogger(pLogger):
    # redirect logging to parent
    global _logger
    _logger = pLogger
                

# NG datasets
ngDataTypes = ['RAW','HITS','RDO','ESD','EVNT']

# excluded provenance
ngProvenance = []

# protection for max number of replicas
protectionMaxNumReplicas  = 10

# max number of waiting jobs
maxWaitingJobs = 200

# max number of waiting jobsets
maxWaitingJobsets = 2

# clouds with small T1 to make replica at T2
cloudsWithSmallT1 = ['IT']

# files in datasets
g_filesInDsMap = {}


class DynDataDistributer:

    # constructor
    def __init__(self,jobs,taskBuffer,siteMapper,simul=False,token=None,logger=None):
        self.jobs = jobs
        self.taskBuffer = taskBuffer
        self.siteMapper = siteMapper
        if token is None:
            self.token = datetime.datetime.utcnow().isoformat(' ')
        else:
            self.token = token
        # use a fixed list since some clouds don't have active T2s
        self.pd2pClouds = ['CA','DE','ES','FR','IT','ND','NL','TW','UK','US']
        self.simul = simul
        self.lastMessage = ''
        self.cachedSizeMap = {}
        self.shareMoUForT2 = None
        self.mapTAGandParentGUIDs = {}
        self.tagParentInfo = {}
        self.parentLfnToTagMap = {}
        self.logger = logger


    # main
    def run(self):
        try:
            self.putLog("start for %s" % self.jobs[0].PandaID)
            # check cloud
            if not self.jobs[0].cloud in self.pd2pClouds+['CERN',]:
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
                # only mc/data datasets
                moveFlag = False
                for projectName in ['mc','data']:
                    if inputDS.startswith(projectName):
                        moveFlag = True
                if not moveFlag:
                    self.putLog("skip non official dataset %s" % inputDS)
                    continue
                if re.search('_sub\d+$',inputDS) is not None or re.search('_dis\d+$',inputDS) is not None:
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
                status,sitesMaps = self.getCandidates(inputDS,useCloseSites=True)
                if not status:
                    self.putLog("failed to get candidates")
                    continue
                # get size of input container
                totalInputSize = 0
                if inputDS.endswith('/'):
                    status,totalInputSize = rucioAPI.getDatasetSize(inputDS)
                    if not status:
                        self.putLog("failed to get size of %s" % inputDS)
                        continue
                # get number of waiting jobs and jobsets
                nWaitingJobsAll = self.taskBuffer.getNumWaitingJobsForPD2P(inputDS)
                nWaitingJobsets = self.taskBuffer.getNumWaitingJobsetsForPD2P(inputDS)
                # loop over all datasets
                usedSites = []
                for tmpDS in sitesMaps:
                    tmpVal = sitesMaps[tmpDS]
                    self.putLog("triggered for %s" % tmpDS,sendLog=True)
                    # increment used counter
                    if not self.simul:
                        nUsed = self.taskBuffer.incrementUsedCounterSubscription(tmpDS)
                    else:
                        nUsed = 5
                    # insert dummy for new dataset which is used to keep track of usage even if subscription is not made
                    if nUsed == 0:
                        retAddUserSub = self.taskBuffer.addUserSubscription(tmpDS,['DUMMY'])
                        if not retAddUserSub:
                            self.putLog("failed to add dummy subscription to database for %s " % tmpDS,type='error',sendLog=True)
                            continue
                    # collect candidates
                    allCandidates = []
                    totalUserSub = 0
                    allCompPd2pSites = []
                    allOKClouds = []
                    totalSecReplicas = 0
                    allT1Candidates = []
                    totalT1Sub = 0
                    cloudCandMap = {}
                    nReplicasInCloud = {}
                    allCandidatesMoU = []
                    nTier1Copies = 0
                    for tmpCloud in tmpVal:
                        (candSites, sitesComDS, sitesPd2pDS, nUserSub, t1HasReplica,
                         t1HasPrimary, nSecReplicas, nT1Sub, candForMoU) = tmpVal[tmpCloud]
                        self.putLog("%s sites with comp DS:%s compPD2P:%s candidates:%s nSub:%s T1:%s Pri:%s nSec:%s nT1Sub:%s candMoU:%s" % \
                                    (tmpCloud,str(sitesComDS),str(sitesPd2pDS),str(candSites),nUserSub,t1HasReplica,t1HasPrimary,
                                     nSecReplicas,nT1Sub,str(candForMoU)))
                        # add
                        totalUserSub += nUserSub
                        totalT1Sub += nT1Sub
                        allCompPd2pSites += sitesPd2pDS
                        totalSecReplicas += nSecReplicas
                        cloudCandMap[tmpCloud] = candSites
                        nReplicasInCloud[tmpCloud] = len(sitesComDS) + len(sitesPd2pDS)
                        # cloud is candidate for T1-T1 when T1 doesn't have primary or secondary replicas or old subscriptions
                        if not t1HasPrimary and nSecReplicas == 0 and nT1Sub == 0:
                            allT1Candidates.append(tmpCloud)
                        # the number of T1s with replica
                        if t1HasPrimary or nSecReplicas > 0:
                            nTier1Copies += 1
                        # add candidates
                        for tmpCandSite in candSites:
                            if not tmpCandSite in usedSites:
                                allCandidates.append(tmpCandSite)
                        # add candidates for MoU
                        for tmpCandSite in candForMoU:
                            if not tmpCandSite in usedSites:
                                allCandidatesMoU.append(tmpCandSite)
                        # add clouds
                        if not tmpCloud in allOKClouds:
                            allOKClouds.append(tmpCloud)
                    self.putLog("PD2P sites with comp replicas : %s" % str(allCompPd2pSites))
                    self.putLog("PD2P T2 candidates : %s" % str(allCandidates))
                    self.putLog("PD2P T2 MoU candidates : %s" % str(allCandidatesMoU))                    
                    self.putLog("PD2P # of T2 subscriptions : %s" % totalUserSub)
                    self.putLog("PD2P # of T1 secondaries   : %s" % totalSecReplicas)
                    self.putLog("PD2P # of T1 subscriptions : %s" % nT1Sub)
                    self.putLog("PD2P # of T1 replicas : %s" % nTier1Copies)                    
                    self.putLog("PD2P T1 candidates : %s" % str(allT1Candidates))
                    self.putLog("PD2P nUsed : %s" % nUsed)
                    # get dataset size
                    retDsSize,dsSize = rucioAPI.getDatasetSize(tmpDS)
                    if not retDsSize:
                        self.putLog("failed to get dataset size of %s" % tmpDS,type='error',sendLog=True)
                        continue
                    self.putLog("PD2P nWaitingJobsets : %s" % nWaitingJobsets)
                    if totalInputSize != 0:
                        self.putLog("PD2P nWaitingJobs    : %s = %s(all)*%s(dsSize)/%s(contSize)" % \
                                    (int((float(nWaitingJobsAll * dsSize) / float(totalInputSize))),
                                     nWaitingJobsAll,dsSize,totalInputSize))
                    else:
                        self.putLog("PD2P nWaitingJobs    : %s = %s(all)" % \
                                    (nWaitingJobsAll,nWaitingJobsAll))
                    # make T1-T1
                    triggeredT1PD2P = False
                    if nUsed > 0:
                        # extract integer part. log10(nUsed) and log10(nUsed)+1 are used to avoid round-off error
                        intLog10nUsed = int(math.log10(nUsed))
                        if self.simul or (int(math.log10(nUsed)) > totalSecReplicas and \
                                          (nUsed == 10**intLog10nUsed or nUsed == 10**(intLog10nUsed+1)) and \
                                          nT1Sub == 0 and allT1Candidates != []):
                            self.putLog("making T1-T1",sendLog=True)
                            # make subscription
                            retT1Sub,useSmallT1 = self.makeT1Subscription(allT1Candidates,tmpDS,dsSize,nUsed)
                            self.putLog("done for T1-T1")
                            triggeredT1PD2P = True
                    # make a T2 copy when T1 PD2P was triggered
                    if triggeredT1PD2P:
                        # TODO
                        retT2MoU,selectedSite = self.makeT2SubscriptionMoU(allCandidatesMoU,tmpDS,dsSize,'T1MOU',nUsed)
                        if retT2MoU and selectedSite is not None:
                            # remove from candidate list
                            if selectedSite in allCandidates:
                                allCandidates.remove(selectedSite)
                            if selectedSite in allCandidatesMoU:
                                allCandidatesMoU.remove(selectedSite)
                            # increment the number of T2 subscriptions
                            totalUserSub += 1
                    # set the number of T2 PD2P replicas
                    maxSitesHaveDS = 1
                    # additional replicas
                    if nWaitingJobsets > maxWaitingJobsets:
                        # the number of waiting jobs for this dataset
                        if totalInputSize != 0:
                            # dataset in container
                            tmpN = float(nWaitingJobsAll * dsSize) / float(totalInputSize)
                        else:
                            # dataset
                            tmpN = float(nWaitingJobsAll)
                        tmpN = int(math.log10(tmpN/float(maxWaitingJobs))) + nTier1Copies
                        maxSitesHaveDS = max(maxSitesHaveDS,tmpN)
                    # protection against too many replications
                    maxSitesHaveDS = min(maxSitesHaveDS,protectionMaxNumReplicas)
                    self.putLog("PD2P maxSitesHaveDS : %s" % maxSitesHaveDS)
                    # ignore the first job
                    if nUsed == 0:
                        self.putLog("skip the first job",
                                    sendLog=True,actionTag='SKIPPED',tagsMap={'reason':'FIRSTJOB','dataset':tmpDS})
                        if not self.simul:
                            continue
                    # check number of replicas                        
                    if len(allCompPd2pSites) >= maxSitesHaveDS and nUsed != 1:
                        self.putLog("skip since many T2 PD2P sites (%s>=%s) have the replica" % (len(allCompPd2pSites),maxSitesHaveDS),
                                    sendLog=True,actionTag='SKIPPED',tagsMap={'reason':'TOO_MANY_T2_REPLICAS','dataset':tmpDS})
                        if not self.simul:
                            continue
                    # check the number of subscriptions
                    maxNumSubInAllCloud = max(0,maxSitesHaveDS-len(allCompPd2pSites))
                    maxNumSubInAllCloud = min(2,maxNumSubInAllCloud)
                    self.putLog("PD2P maxNumSubInAllCloud : %s" % maxNumSubInAllCloud)
                    if totalUserSub >= maxNumSubInAllCloud:
                        self.putLog("skip since enough subscriptions (%s>=%s) were already made for T2 PD2P" % \
                                    (totalUserSub,maxNumSubInAllCloud),
                                    sendLog=True,actionTag='SKIPPED',tagsMap={'reason':'TOO_MANY_T2_SUBSCRIPTIONS','dataset':tmpDS})
                        if not self.simul:                        
                            continue
                    # no candidates
                    if len(allCandidates) == 0:
                        self.putLog("skip since no candidates",sendLog=True,actionTag='SKIPPED',tagsMap={'reason':'NO_T2_CANDIDATE','dataset':tmpDS})
                        continue
                    # get inverse weight for brokerage
                    weightForBrokerage = self.getWeightForBrokerage(allCandidates,tmpDS,nReplicasInCloud)
                    self.putLog("inverse weight %s" % str(weightForBrokerage))
                    # get free disk size
                    self.putLog("getting free disk size for T2 PD2P")
                    retFreeSizeMap,freeSizeMap = self.getFreeDiskSize(tmpDS,allCandidates)
                    if not retFreeSizeMap:
                        self.putLog("failed to get free disk size",type='error',sendLog=True)
                        continue
                    # run brokerage
                    tmpJob = JobSpec()
                    tmpJob.AtlasRelease = ''
                    self.putLog("run brokerage for %s" % tmpDS)
                    usedWeight = pandaserver.brokerage.broker.schedule([tmpJob],self.taskBuffer,self.siteMapper,True,allCandidates,
                                                           True,specialWeight=weightForBrokerage,getWeight=True,
                                                           sizeMapForCheck=freeSizeMap,datasetSize=dsSize)
                    selectedSite = tmpJob.computingSite
                    for tmpWeightSite in usedWeight:
                        tmpWeightStr = usedWeight[tmpWeightSite]
                        tmpTagsMap = {'site':tmpWeightSite,'weight':tmpWeightStr,'dataset':tmpDS}                        
                        if tmpWeightSite == selectedSite:
                            if nUsed == 1:
                                tmpActionTag = 'SELECTEDT2_JOB'
                            elif len(allCompPd2pSites) == 0:
                                tmpActionTag = 'SELECTEDT2_NOREP'
                            else:
                                tmpActionTag = 'SELECTEDT2_WAIT'
                            tmpTagsMap['nused']           = nUsed
                            tmpTagsMap['nwaitingjobs']    = nWaitingJobsAll
                            tmpTagsMap['nwaitingjobsets'] = nWaitingJobsets
                            tmpTagsMap['nsiteshaveds']    = len(allCompPd2pSites)
                        else:
                            tmpActionTag = 'UNSELECTEDT2'
                        self.putLog("weight %s %s" % (tmpWeightSite,tmpWeightStr),sendLog=True,
                                    actionTag=tmpActionTag,tagsMap=tmpTagsMap)
                    self.putLog("site for T2 PD2P -> %s" % selectedSite)
                    # remove from candidate list
                    if selectedSite in allCandidates:
                        allCandidates.remove(selectedSite)
                    if selectedSite in allCandidatesMoU:
                        allCandidatesMoU.remove(selectedSite)
                    # make subscription
                    if not self.simul:
                        subRet,dq2ID = self.makeSubscription(tmpDS,selectedSite,ddmShare='secondary')
                        self.putLog("made subscription to %s:%s" % (selectedSite,dq2ID),sendLog=True)
                        usedSites.append(selectedSite)
                        # update database
                        if subRet:
                            self.taskBuffer.addUserSubscription(tmpDS,[dq2ID])
                    # additional T2 copy with MoU share when it is the second submission
                    if nUsed == 1 or self.simul:
                        retT2MoU,selectedSite = self.makeT2SubscriptionMoU(allCandidatesMoU,tmpDS,dsSize,'T2MOU',nUsed)
            self.putLog("end for %s" % self.jobs[0].PandaID)
        except Exception:
            errType,errValue = sys.exc_info()[:2]
            self.putLog("%s %s" % (errType,errValue),'error')


    # get candidate sites for subscription
    def getCandidates(self,inputDS,checkUsedFile=True,useHidden=False,useCloseSites=False):
        # return for failure
        failedRet = False,{'':{'':([],[],[],0,False,False,0,0,[])}}
        # get replica locations
        if inputDS.endswith('/'):
            # container
            status,tmpRepMaps = self.getListDatasetReplicasInContainer(inputDS)
            # get used datasets
            if status and checkUsedFile:
                status,tmpUsedDsList = self.getUsedDatasets(tmpRepMaps)
                # remove unused datasets
                newRepMaps = {}
                for tmpKey in tmpRepMaps:
                    tmpVal = tmpRepMaps[tmpKey]
                    if tmpKey in tmpUsedDsList:
                        newRepMaps[tmpKey] = tmpVal
                tmpRepMaps = newRepMaps        
        else:
            # normal dataset
            status,tmpRepMap = self.getListDatasetReplicas(inputDS)
            tmpRepMaps = {inputDS:tmpRepMap}
        if not status:
            # failed
            self.putLog("failed to get replica locations for %s" % inputDS,'error')
            return failedRet
        # get close sites
        closeSitesMap = {}
        # get all sites
        allSiteMap = {}
        for tmpSiteName in self.siteMapper.siteSpecList:
            tmpSiteSpec = self.siteMapper.siteSpecList[tmpSiteName]
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
            allSiteMap.setdefault(tmpSiteSpec.cloud, [])
            allSiteMap[tmpSiteSpec.cloud].append(tmpSiteSpec)
        # NG DQ2 IDs
        ngDQ2SuffixList = ['LOCALGROUPDISK','STAGING']
        # loop over all clouds
        returnMap = {}
        checkedMetaMap = {}
        userSubscriptionsMap = {}
        for cloud in self.pd2pClouds:
            # DQ2 prefix of T1
            tmpT1SiteID = self.siteMapper.getCloud(cloud)['source']
            tmpT1DQ2ID  = self.siteMapper.getSite(tmpT1SiteID).ddm_input # TODO: check with Tadashi
            prefixDQ2T1 = re.sub('[^_]+DISK$','',tmpT1DQ2ID)
            # loop over all datasets     
            for tmpDS in tmpRepMaps:
                tmpRepMap = tmpRepMaps[tmpDS]
                candSites     = []
                sitesComDS    = []
                sitesCompPD2P = []
                # check T1 has a replica and get close sites
                t1HasReplica = False
                t1HasPrimary = False
                nSecReplicas = 0
                closeSiteList = []
                candForMoU = []
                for tmpDQ2ID in tmpRepMap:
                    # check NG suffix
                    ngSuffixFlag = False
                    for tmpNGSuffix in ngDQ2SuffixList:
                        if tmpDQ2ID.endswith(tmpNGSuffix):
                            ngSuffixFlag = True
                            break
                    if ngSuffixFlag:
                        continue
                    # get close sites
                    if tmpDQ2ID in closeSitesMap:
                        for tmpCloseSiteID in closeSitesMap[tmpDQ2ID]:
                            if not tmpCloseSiteID in closeSiteList:
                                closeSiteList.append(tmpCloseSiteID)
                self.putLog("close sites : %s" % str(closeSiteList))
                # get on-going subscriptions
                timeRangeSub = 7
                userSubscriptionsMap.setdefault(tmpDS, self.taskBuffer.getUserSubscriptions(tmpDS,timeRangeSub))
                userSubscriptions = userSubscriptionsMap[tmpDS]
                # unused cloud
                if cloud not in allSiteMap:
                    continue
                # count the number of T1 subscriptions
                nT1Sub = 0
                for tmpUserSub in userSubscriptions:
                    if tmpUserSub.startswith(prefixDQ2T1):
                        nT1Sub += 1
                # check sites
                nUserSub = 0
                for tmpSiteSpec in allSiteMap[cloud]:
                    # check cloud
                    if tmpSiteSpec.cloud != cloud:
                        continue
                    # prefix of DQ2 ID
                    if tmpSiteSpec.ddm_input is None:
                        continue
                    prefixDQ2 = re.sub('[^_]+DISK$','',tmpSiteSpec.ddm_input) # TODO: Check with Tadashi
                    # skip T1
                    if prefixDQ2 == prefixDQ2T1:
                        continue
                    # check if corresponding DQ2 ID is a replica location
                    hasReplica = False
                    for tmpDQ2ID in tmpRepMap:
                        tmpStatMap = tmpRepMap[tmpDQ2ID]
                        # check NG suffix
                        ngSuffixFlag = False
                        for tmpNGSuffix in ngDQ2SuffixList:
                            if tmpDQ2ID.endswith(tmpNGSuffix):
                                ngSuffixFlag = True
                                break
                        if ngSuffixFlag:
                            continue
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
                        candForMoU.append(tmpSiteSpec.sitename)
                        if not useCloseSites:
                            candSites.append(tmpSiteSpec.sitename)
                        else:
                            # use close sites only
                            if self.getDQ2ID(tmpSiteSpec.sitename,tmpDS) in closeSiteList:
                                candSites.append(tmpSiteSpec.sitename)
                    # the number of subscriptions
                    for tmpUserSub in userSubscriptions:
                        if tmpUserSub.startswith(prefixDQ2):
                            nUserSub += 1
                            break
                # append
                returnMap.setdefault(tmpDS, {})
                returnMap[tmpDS][cloud] = (candSites,sitesComDS,sitesCompPD2P,nUserSub,t1HasReplica,t1HasPrimary,
                                           nSecReplicas,nT1Sub,candForMoU)
        # return
        return True,returnMap

    
    # get map of DQ2 IDs
    def getDQ2ID(self,sitename,dataset):
        # get DQ2 ID
        if not self.siteMapper.checkSite(sitename):
            self.putLog("cannot find SiteSpec for %s" % sitename)
            return ''
        dq2ID = self.siteMapper.getSite(sitename).ddm_input # TODO: check with Tadashi
        if True:
            # data
            matchEOS = re.search('_EOS[^_]+DISK$',dq2ID)
            if matchEOS is not None:
                dq2ID = re.sub('_EOS[^_]+DISK','_EOSDATADISK',dq2ID)
            else:
                dq2ID = re.sub('_[^_]+DISK','_DATADISK',dq2ID)
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
    def makeSubscription(self,dataset,sitename,givenDQ2ID=None,ddmShare='secondary'):
        # return for failuer
        retFailed = False,''
        # get DQ2 IDs
        if givenDQ2ID is None:
            dq2ID = self.getDQ2ID(sitename,dataset)
        else:
            dq2ID = givenDQ2ID
        if dq2ID == '':
            self.putLog("cannot find DQ2 ID for %s:%s" % (sitename,dataset))
            return retFailed
        # register subscription
        self.putLog('registerDatasetSubscription %s %s' % (dataset,dq2ID))
        nTry = 3
        for iDDMTry in range(nTry):
            try:
                status = rucioAPI.registerDatasetSubscription(dataset,[dq2ID],
                                                              activity='Data Brokering')
                out = 'OK'
                break
            except Exception:
                status = False
                errType,errValue = sys.exc_info()[:2]
                out = "%s %s" % (errType,errValue)
                time.sleep(30)
        # result
        if not status:
            self.putLog(out,'error')
            self.putLog('bad DDM response for %s' % dataset,'error')
            return retFailed
        # update 
        self.putLog('%s %s' % (status,out))
        return True,dq2ID

            
    # get weight for brokerage
    def getWeightForBrokerage(self,sitenames,dataset,nReplicasInCloud):
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
            if dq2ID in numUserSubs:
                retMap[sitename] = 1 + numUserSubs[dq2ID]
            else:
                retMap[sitename] = 1
            # negative weight if a cloud already has replicas
            tmpCloud = self.siteMapper.getSite(sitename).cloud
            retMap[sitename] *= (1 + nReplicasInCloud[tmpCloud])
        # return
        return retMap


    # get free disk size
    def getFreeDiskSize(self,dataset,siteList):
        # return for failuer
        retFailed = False,{}
        # loop over all sites
        sizeMap = {}
        for sitename in siteList:
            # reuse cached value
            if sitename in self.cachedSizeMap:
                sizeMap[sitename] = self.cachedSizeMap[sitename]
                continue
            # get DQ2 IDs
            dq2ID = self.getDQ2ID(sitename,dataset)
            if dq2ID == '':
                self.putLog("cannot find DQ2 ID for %s:%s" % (sitename,dataset))
                return retFailed
            tmpMap = rucioAPI.getRseUsage(dq2ID)
            if tmpMap == {}:
                self.putLog('getRseUsage failed for {0}'.format(sitename))
            # append
            sizeMap[sitename] = tmpMap
            # cache
            self.cachedSizeMap[sitename] = sizeMap[sitename]
        # return
        self.putLog('getFreeDiskSize done->%s' % str(sizeMap))
        return True,sizeMap
            

        
    # get list of replicas for a dataset
    def getListDatasetReplicas(self,dataset):
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog("%s/%s listDatasetReplicas %s" % (iDDMTry,nTry,dataset))
            status,out = rucioAPI.listDatasetReplicas(dataset)
            if status != 0:
                time.sleep(10)
            else:
                break
        # result    
        if status != 0:
            self.putLog(out,'error')
            self.putLog('bad response for %s' % dataset, 'error')            
            return False,{}
        self.putLog('getListDatasetReplicas->%s' % str(out))
        return True,out

        
    
    # get replicas for a container 
    def getListDatasetReplicasInContainer(self,container):
        # response for failure
        resForFailure = False,{}
        # get datasets in container
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog('%s/%s listDatasetsInContainer %s' % (iDDMTry,nTry,container))
            datasets,out = rucioAPI.listDatasetsInContainer(container)
            if datasets is None:
                time.sleep(60)
            else:
                break
        if datasets is None:
            self.putLog(out,'error')
            self.putLog('bad DDM response for %s' % container, 'error')
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


    # get datasets used by jobs
    def getUsedDatasets(self,datasetMap):
        resForFailure = (False,[])
        # loop over all datasets
        usedDsList = []
        for datasetName in datasetMap:
            # get file list
            nTry = 3
            for iDDMTry in range(nTry):
                try:
                    self.putLog('%s/%s listFilesInDataset %s' % (iDDMTry,nTry,datasetName))
                    fileItems,out = rucioAPI.listFilesInDataset(datasetName)
                    status = True
                    break
                except Exception:
                    status = False
                    errType,errValue = sys.exc_info()[:2]
                    out = '{0} {1}'.format(errType,errValue)
                    time.sleep(60)
            if not status:
                self.putLog(out,'error')
                self.putLog('bad DDM response to get size of %s' % datasetName, 'error')
                return resForFailure
            # get 
            # check if jobs use the dataset
            usedFlag = False
            for tmpJob in self.jobs:
                for tmpFile in tmpJob.Files:
                    if tmpFile.type == 'input' and tmpFile.lfn in fileItems:
                        usedFlag = True
                        break
                # escape    
                if usedFlag:
                    break
            # used
            if usedFlag:
                usedDsList.append(datasetName)
        # return
        self.putLog("used datasets = %s" % str(usedDsList))
        return True,usedDsList


    # get file from dataset
    def getFileFromDataset(self,datasetName,guid,randomMode=False,nSamples=1):
        resForFailure = (False,None)
        # get files in datasets
        global g_filesInDsMap
        if datasetName not in g_filesInDsMap:
            nTry = 3
            for iDDMTry in range(nTry):
                try:
                    self.putLog('%s/%s listFilesInDataset %s' % (iDDMTry,nTry,datasetName))
                    fileItems,out = rucioAPI.listFilesInDataset(datasetName)
                    status = True
                    break
                except Exception:
                    status = False
                    errType,errValue = sys.exc_info()[:2]
                    out = '{0} {1}'.format(errType,errValue)
                    time.sleep(60)
            if not status:
                self.putLog(out,'error')
                self.putLog('bad DDM response to get size of %s' % datasetName, 'error')
                return resForFailure
            # append
            g_filesInDsMap[datasetName] = fileItems
        # random mode
        if randomMode:
            tmpList = list(g_filesInDsMap[datasetName])
            random.shuffle(tmpList)
            retList = []
            for iSamples in range(nSamples):
                if iSamples < len(tmpList):
                    tmpLFN = tmpList[iSamples]
                    retMap = g_filesInDsMap[datasetName][tmpLFN]
                    retMap['lfn'] = tmpLFN
                    retMap['dataset'] = datasetName
                    retList.append(retMap)
            return True,retList        
        # return
        for tmpLFN in g_filesInDsMap[datasetName]:
            tmpVal = g_filesInDsMap[datasetName][tmpLFN]
            if tmpVal['guid'] == guid:
                retMap = tmpVal
                retMap['lfn'] = tmpLFN
                retMap['dataset'] = datasetName            
                return True,retMap
        return resForFailure
        
        
    # register new dataset container with datasets
    def registerDatasetContainerWithDatasets(self,containerName,files,replicaMap,nSites=1,owner=None):
        # parse DN
        if owner is not None:
            out = rucioAPI.parse_dn(owner)
            status,userInfo = rucioAPI.finger(out)
            if not status:
                self.putLog('failed to finger: {0}'.format(userInfo))
            else:
                owner = userInfo['nickname']
            self.putLog('parsed DN={0}'.format(owner))
        # sort by locations
        filesMap = {}
        for tmpFile in files:
            tmpLocations = replicaMap[tmpFile['dataset']]
            tmpLocations.sort()
            newLocations = []
            # skip STAGING
            for tmpLocation in tmpLocations:
                if not tmpLocation.endswith('STAGING'):
                    newLocations.append(tmpLocation)
            if newLocations == []:
                continue
            tmpLocations = newLocations
            tmpKey = tuple(tmpLocations)
            filesMap.setdefault(tmpKey, [])
            # append file
            filesMap[tmpKey].append(tmpFile)
        # get nfiles per dataset
        nFilesPerDataset,tmpR = divmod(len(files),nSites)
        if nFilesPerDataset == 0:
            nFilesPerDataset = 1
        maxFilesPerDataset = 1000
        if nFilesPerDataset >= maxFilesPerDataset:
            nFilesPerDataset = maxFilesPerDataset
        # register new datasets
        datasetNames = []
        tmpIndex = 1
        for tmpLocations in filesMap:
            tmpFiles = filesMap[tmpLocations]
            tmpSubIndex = 0
            while tmpSubIndex < len(tmpFiles):
                tmpDsName = containerName[:-1] + '_%04d' % tmpIndex
                tmpRet = self.registerDatasetWithLocation(tmpDsName,tmpFiles[tmpSubIndex:tmpSubIndex+nFilesPerDataset],
                                                          #tmpLocations,owner=owner)
                                                          tmpLocations,owner=None)
                # failed
                if not tmpRet:
                    self.putLog('failed to register %s' % tmpDsName, 'error')
                    return False
                # append dataset
                datasetNames.append(tmpDsName)
                tmpIndex += 1
                tmpSubIndex += nFilesPerDataset
        # register container
        nTry = 3
        for iDDMTry in range(nTry):
            try:
                self.putLog('%s/%s registerContainer %s' % (iDDMTry,nTry,containerName))
                status = rucioAPI.registerContainer(containerName,datasetNames)
                out = 'OK'
                break
            except Exception:
                status = False
                errType,errValue = sys.exc_info()[:2]
                out = '{0} {1}'.format(errType,errValue)
                time.sleep(10)
        if not status:
            self.putLog(out,'error')
            self.putLog('bad DDM response to register %s' % containerName, 'error')
            return False
        # return
        self.putLog(out)
        return True
        
            

    # register new dataset with locations
    def registerDatasetWithLocation(self,datasetName,files,locations,owner=None):
        resForFailure = False
        # get file info
        guids   = []
        lfns    = []
        fsizes  = []
        chksums = []
        for tmpFile in files:
            guids.append(tmpFile['guid'])
            lfns.append(tmpFile['scope']+':'+tmpFile['lfn'])
            fsizes.append(long(tmpFile['filesize']))
            chksums.append(tmpFile['checksum'])
        # register new dataset    
        nTry = 3
        for iDDMTry in range(nTry):
            try:
                self.putLog('%s/%s registerNewDataset %s len=%s' % (iDDMTry,nTry,datasetName,
                                                                    len(files)))
                out = rucioAPI.registerDataset(datasetName,lfns,guids,fsizes,chksums,
                                               lifetime=14)
                self.putLog(out)
                break
            except Exception:
                errType,errValue = sys.exc_info()[:2]
                self.putLog("%s %s" % (errType,errValue),'error')
                if iDDMTry+1 == nTry:
                    self.putLog('failed to register {0} in rucio'.format(datasetName))
                    return resForFailure
                time.sleep(10)
        # freeze dataset    
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog('%s/%s freezeDataset %s' % (iDDMTry,nTry,datasetName))
            try:
                rucioAPI.closeDataset(datasetName)
                status = True
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                out = 'failed to freeze : {0} {1}'.format(errtype,errvalue)
                status = False
            if not status:
                time.sleep(10)
            else:
                break
        if not status:
            self.putLog(out,'error')
            self.putLog('bad DDM response to freeze %s' % datasetName, 'error')
            return resForFailure
        # register locations
        for tmpLocation in locations:
            nTry = 3
            for iDDMTry in range(nTry):
                try:
                    self.putLog('%s/%s registerDatasetLocation %s %s' % (iDDMTry,nTry,datasetName,tmpLocation))
                    out = rucioAPI.registerDatasetLocation(datasetName,[tmpLocation],14,owner)
                    self.putLog(out)
                    status = True
                    break
                except Exception:
                    status = False
                    errType,errValue = sys.exc_info()[:2]
                    self.putLog("%s %s" % (errType,errValue),'error')
                    if iDDMTry+1 == nTry:
                        self.putLog('failed to register {0} in rucio'.format(datasetName))
                        return resForFailure
                    time.sleep(10)
            if not status:
                self.putLog(out,'error')
                self.putLog('bad DDM response to set owner %s' % datasetName, 'error')
                return resForFailure
        return True


    # list datasets by file GUIDs
    def listDatasetsByGUIDs(self,guids,dsFilters):
        resForFailure = (False,{})
        resForFatal = (False,{'isFatal':True})
        # get size of datasets
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog('%s/%s listDatasetsByGUIDs GUIDs=%s' % (iDDMTry, nTry, str(guids)))
            try:
                out = rucioAPI.listDatasetsByGUIDs(guids)
                status = True
                break
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                out = 'failed to get datasets with GUIDs : {0} {1}'.format(errtype,errvalue)
                status = False
                time.sleep(10)
        if not status:
            self.putLog(out,'error')
            self.putLog('bad response to list datasets by GUIDs','error')
            if 'DataIdentifierNotFound' in out:
                return resForFatal
            return resForFailure
        self.putLog(out)
        # get map
        retMap = {}
        try:
            outMap = out
            for guid in guids:
                tmpDsNames = []
                # GUID not found
                if guid not in outMap:
                    self.putLog('GUID=%s not found' % guid,'error')
                    return resForFatal
                # ignore junk datasets
                for tmpDsName in outMap[guid]:
                    if tmpDsName.startswith('panda') or \
                           tmpDsName.startswith('user') or \
                           tmpDsName.startswith('group') or \
                           tmpDsName.startswith('archive') or \
                           re.search('_sub\d+$',tmpDsName) is not None or \
                           re.search('_dis\d+$',tmpDsName) is not None or \
                           re.search('_shadow$',tmpDsName) is not None:
                        continue
                    # check with filters
                    if dsFilters != []:
                        flagMatch = False
                        for tmpFilter in dsFilters:
                            if fnmatch.fnmatchcase(tmpDsName, tmpFilter):
                                flagMatch = True
                                break
                        # not match
                        if not flagMatch:
                            continue
                    # append
                    tmpDsNames.append(tmpDsName)
                # empty
                if tmpDsNames == []:
                    self.putLog('no datasets found for GUID=%s' % guid)
                    continue
                # duplicated
                if len(tmpDsNames) != 1:
                    self.putLog('there are multiple datasets %s for GUID:%s' % (str(tmpDsNames),guid),'error')
                    return resForFatal
                # append
                retMap[guid] = tmpDsNames[0]
        except Exception:
            self.putLog('failed to list datasets by GUIDs','error')
            return resForFailure
        return True,retMap


    # conver event/run list to datasets
    def convertEvtRunToDatasets(self,runEvtList,dsType,streamName,dsFilters,amiTag,user,runEvtGuidMap,ei_api):
        self.putLog('convertEvtRunToDatasets type=%s stream=%s dsPatt=%s amitag=%s' % \
                    (dsType,streamName,str(dsFilters),amiTag))
        # check data type
        failedRet = False,{},[]
        fatalRet  = False,{'isFatal':True},[]
        streamRef = 'Stream' + dsType
        # import event lookup client
        if runEvtGuidMap == {}:
            if len(runEvtList) == 0:
                self.putLog("Empty list for run and events was provided",type='error')
                return failedRet
            # Hadoop EI
            from eventLookupClientEI import eventLookupClientEI
            elssiIF = eventLookupClientEI()
            # Oracle EI
            from pandaserver.taskbuffer.EiTaskBuffer import eiTaskBuffer
            eiTaskBuffer.init()
            # loop over all events
            nEventsPerLoop = 500
            iEventsTotal = 0
            while iEventsTotal < len(runEvtList):
                tmpRunEvtList = runEvtList[iEventsTotal:iEventsTotal+nEventsPerLoop]
                iEventsTotal += nEventsPerLoop
                regStart = datetime.datetime.utcnow()
                guidListELSSI,tmpCom,tmpOut,tmpErr = elssiIF.doLookup(tmpRunEvtList,stream=streamName,tokens=streamRef,
                                                                      amitag=amiTag,user=user,ei_api=ei_api)
                regTime = datetime.datetime.utcnow()-regStart
                self.putLog("Hadoop EI command: {0}".format(tmpCom))
                self.putLog("Hadoop EI took {0}.{1:03d} sec for {2} events" .format(regTime.seconds,
                                                                                    regTime.microseconds/1000,
                                                                                    len(tmpRunEvtList)))
                regStart = datetime.datetime.utcnow()
                """
                statOra,guidListOraEI = eiTaskBuffer.getGUIDsFromEventIndex(tmpRunEvtList,streamName,amiTag,dsType)
                regTime = datetime.datetime.utcnow()-regStart
                self.putLog("Oracle EI took {0}.{1:03d} sec for {2} events" .format(regTime.seconds,
                                                                                    regTime.microseconds/1000,
                                                                                    len(tmpRunEvtList)))
                """
                # failed
                if not tmpErr in [None,''] or len(guidListELSSI) == 0:
                    self.putLog(tmpCom)
                    self.putLog(tmpOut)
                    self.putLog(tmpErr)
                    self.putLog("invalid retrun from EventIndex",type='error')
                    return failedRet
                # check events
                for runNr,evtNr in tmpRunEvtList:
                    paramStr = 'Run:%s Evt:%s Stream:%s' % (runNr,evtNr,streamName)
                    self.putLog(paramStr)
                    tmpRunEvtKey = (long(runNr),long(evtNr))
                    """
                    # check in Oracle EI
                    if not tmpRunEvtKey in guidListOraEI:
                        errStr = "no GUIDs were found in Oracle EI for %s" % paramStr
                        self.putLog(errStr)
                    """
                    # not found
                    if not tmpRunEvtKey in guidListELSSI or len(guidListELSSI[tmpRunEvtKey]) == 0:
                        self.putLog(tmpCom)
                        self.putLog(tmpOut)
                        self.putLog(tmpErr)
                        errStr = "no GUIDs were found in EventIndex for %s" % paramStr
                        self.putLog(errStr,type='error')
                        return fatalRet
                    # append
                    runEvtGuidMap[tmpRunEvtKey] = guidListELSSI[tmpRunEvtKey]
        # convert to datasets
        allDatasets  = []
        allFiles     = []
        allLocations = {}
        for tmpIdx in runEvtGuidMap:
            tmpguids = runEvtGuidMap[tmpIdx]
            runNr,evtNr = tmpIdx
            tmpDsRet,tmpDsMap = self.listDatasetsByGUIDs(tmpguids,dsFilters)
            # failed
            if not tmpDsRet:
                self.putLog("failed to convert GUIDs to datasets",type='error')
                if 'isFatal' in tmpDsMap and tmpDsMap['isFatal'] == True:
                    return fatalRet
                return failedRet
            # empty
            if tmpDsMap == {}:
                self.putLog("there is no dataset for Run:%s Evt:%s GUIDs:%s" % (runNr,evtNr,str(tmpguids)),type='error')
                return fatalRet
            if len(tmpDsMap) != 1:
                self.putLog("there are multiple datasets %s for Run:%s Evt:%s GUIDs:%s" % (str(tmpDsMap),runNr,evtNr,
                                                                                           str(tmpguids)),
                            type='error')
                return fatalRet
            # append
            for tmpGUID in tmpDsMap:
                tmpDsName = tmpDsMap[tmpGUID]
                # collect dataset names
                if not tmpDsName in allDatasets:
                    allDatasets.append(tmpDsName)
                    # get location
                    statRep,replicaMap = self.getListDatasetReplicas(tmpDsName)
                    # failed
                    if not statRep:
                        self.putLog("failed to get locations for DS:%s" % tmpDsName,type='error')
                        return failedRet
                    # collect locations
                    tmpLocationList = []
                    for tmpLocation in replicaMap:
                        # use only complete replicas
                        dsStatDict = replicaMap[tmpLocation][0]
                        if dsStatDict['total'] is not None and dsStatDict['total'] == dsStatDict['found']:
                            if not tmpLocation in tmpLocationList:
                                tmpLocationList.append(tmpLocation)
                    allLocations[tmpDsName] = tmpLocationList
                # get file info
                tmpFileRet,tmpFileInfo = self.getFileFromDataset(tmpDsName,tmpGUID)
                # failed
                if not tmpFileRet:
                    self.putLog("failed to get fileinfo for GUID:%s DS:%s" % (tmpGUID,tmpDsName),type='error')
                    return failedRet
                # collect files
                allFiles.append(tmpFileInfo)
        # return
        self.putLog('converted to %s, %s, %s' % (str(allDatasets),str(allLocations),str(allFiles)))
        return True,allLocations,allFiles

    # put log
    def putLog(self,msg,type='debug',sendLog=False,actionTag='',tagsMap={}):
        if self.logger is None:
            tmpMsg = self.token+' '+str(msg)
        else:
            tmpMsg = str(msg)
        if type == 'error':
            if self.logger is None:
                _logger.error(tmpMsg)
            else:
                self.logger.error(tmpMsg)
            # keep last error message
            self.lastMessage = tmpMsg   
        else:
            if self.logger is None:
                _logger.debug(tmpMsg)
            else:
                self.logger.debug(tmpMsg)
        # send to logger
        if sendLog:
            tmpMsg = self.token + ' - '
            if actionTag != '':
                tmpMsg += 'action=%s ' % actionTag
                for tmpTag in tagsMap:
                    tmpTagVal = tagsMap[tmpTag]
                    tmpMsg += '%s=%s ' % (tmpTag,tmpTagVal)
            tmpMsg += '- ' + msg    
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
                                                                                                                            

    # peek log
    def peekLog(self):
        return self.lastMessage
    
                
    # make T1 subscription
    def makeT1Subscription(self,allCloudCandidates,tmpDS,dsSize,
                           nUsed=None,nWaitingJobs=None,nWaitingJobsets=None):
        useSmallT1 = None
        # no candidate
        if allCloudCandidates == []:
            return True,useSmallT1
        # convert to siteIDs
        t1Candidates = []
        t1Weights    = {}
        siteToCloud  = {}
        for tmpCloud in allCloudCandidates:
            tmpCloudSpec = self.siteMapper.getCloud(tmpCloud)
            tmpT1SiteID = tmpCloudSpec['source']
            t1Candidates.append(tmpT1SiteID)
            # use MoU share
            t1Weights[tmpT1SiteID] = tmpCloudSpec['mcshare']
            # reverse lookup
            siteToCloud[tmpT1SiteID] = tmpCloud
        # get free disk size
        self.putLog("getting free disk size for T1 PD2P")        
        retFreeSizeMap,freeSizeMap = self.getFreeDiskSize(tmpDS,t1Candidates)
        if not retFreeSizeMap:
            self.putLog("failed to get free disk size",type='error',sendLog=True)
            return False,useSmallT1
        # run brokerage
        tmpJob = JobSpec()
        tmpJob.AtlasRelease = ''
        self.putLog("run brokerage for T1-T1 for %s" % tmpDS)
        selectedSite = self.chooseSite(t1Weights,freeSizeMap,dsSize)
        self.putLog("site for T1 PD2P -> %s" % selectedSite)
        # simulation
        if self.simul:
            return True,useSmallT1
        # no candidate
        if selectedSite is None:
            self.putLog("no candidate for T1-T1")
            return False,useSmallT1
        # make subscription
        tmpJob.computingSite = selectedSite
        subRet,dq2ID = self.makeSubscription(tmpDS,tmpJob.computingSite)
        tmpTagsMap = {'site':tmpJob.computingSite,'dataset':tmpDS}
        if nUsed is not None:
            tmpTagsMap['nused'] = nUsed
        if nWaitingJobs is not None:
            tmpTagsMap['nwaitingjobs'] = nWaitingJobs
        if nWaitingJobsets is not None:
            tmpTagsMap['nwaitingjobsets'] = nWaitingJobsets
        self.putLog("made subscription for T1-T1 to %s:%s" % (tmpJob.computingSite,dq2ID),sendLog=True,
                    actionTag='SELECTEDT1',tagsMap=tmpTagsMap)
        # check if small cloud is used
        if siteToCloud[tmpJob.computingSite] in cloudsWithSmallT1:
            useSmallT1 = siteToCloud[tmpJob.computingSite]
        # update database
        if subRet:
            self.taskBuffer.addUserSubscription(tmpDS,[dq2ID])
            return True,useSmallT1
        else:
            return False,useSmallT1


    # make T2 subscription with MoU share
    def makeT2SubscriptionMoU(self,allCandidates,tmpDS,dsSize,pd2pType,
                              nUsed=None,nWaitingJobs=None,nWaitingJobsets=None):
        # no candidate
        if allCandidates == []:
            return True,None
        # get MoU share
        if self.shareMoUForT2 is None:
            self.shareMoUForT2 = self.taskBuffer.getMouShareForT2PD2P()
        # convert to DQ2 ID
        t2Candidates = []
        t2Weights    = {}
        dq2List = []
        for tmpCandidate in allCandidates:
            tmpDQ2ID = self.getDQ2ID(tmpCandidate,tmpDS)
            if not tmpDQ2ID in dq2List:
                # append
                dq2List.append(tmpDQ2ID)
                # get MoU share
                if tmpDQ2ID not in self.shareMoUForT2:
                    # site is undefined in t_regions_replication 
                    self.putLog("%s is not in MoU table" % tmpDQ2ID,type='error')
                    continue
                if not self.shareMoUForT2[tmpDQ2ID]['status'] in ['ready']:
                    # site is not ready
                    self.putLog("%s is not ready in MoU table" % tmpDQ2ID)
                    continue
                tmpWeight = self.shareMoUForT2[tmpDQ2ID]['weight']
                # skip if the weight is 0
                if tmpWeight == 0:
                    self.putLog("%s has 0 weight in MoU table" % tmpDQ2ID)
                    continue
                # collect siteIDs and weights for brokerage
                t2Candidates.append(tmpCandidate)
                t2Weights[tmpCandidate] = tmpWeight
        # sort for reproducibility
        t2Candidates.sort()
        # get free disk size
        self.putLog("getting free disk size for T2 %s PD2P" % pd2pType)        
        retFreeSizeMap,freeSizeMap = self.getFreeDiskSize(tmpDS,t2Candidates)
        if not retFreeSizeMap:
            self.putLog("failed to get free disk size",type='error',sendLog=True)
            return False,None
        # run brokerage
        tmpJob = JobSpec()
        tmpJob.AtlasRelease = ''
        self.putLog("run brokerage for T2 with %s for %s" % (pd2pType,tmpDS))
        selectedSite = self.chooseSite(t2Weights,freeSizeMap,dsSize)
        self.putLog("site for T2 %s PD2P -> %s" % (pd2pType,selectedSite))
        # simulation
        if self.simul:
            return True,selectedSite
        # no candidate
        if selectedSite is None:
            self.putLog("no candidate for T2 with %s" % pd2pType)
            return False,None
        # make subscription
        subRet,dq2ID = self.makeSubscription(tmpDS,selectedSite)
        tmpTagsMap = {'site':selectedSite,'dataset':tmpDS}
        if nUsed is not None:
            tmpTagsMap['nused'] = nUsed
        if nWaitingJobs is not None:
            tmpTagsMap['nwaitingjobs'] = nWaitingJobs
        if nWaitingJobsets is not None:
            tmpTagsMap['nwaitingjobsets'] = nWaitingJobsets
        self.putLog("made subscription for T2 with %s to %s:%s" % (pd2pType,selectedSite,dq2ID),sendLog=True,
                    actionTag='SELECTEDT2_%s' % pd2pType,tagsMap=tmpTagsMap)
        # update database
        if subRet:
            self.taskBuffer.addUserSubscription(tmpDS,[dq2ID])
            return True,selectedSite
        else:
            return False,None


    # choose site
    def chooseSite(self,canWeights,freeSizeMap,datasetSize):
        # loop over all candidates
        totalW = 0
        allCandidates = []
        for tmpCan in canWeights:
            tmpW = canWeights[tmpCan]
            # size check
            if tmpCan in freeSizeMap:
                # disk threshold for PD2P max(5%,3TB)
                diskThresholdPD2P = 1024 * 3
                thrForThisSite = long(freeSizeMap[tmpCan]['total'] * 5 / 100)
                if thrForThisSite < diskThresholdPD2P:
                    thrForThisSite = diskThresholdPD2P
                remSpace = freeSizeMap[tmpCan]['total'] - freeSizeMap[tmpCan]['used']
                if remSpace-datasetSize < thrForThisSite:
                    self.putLog('  skip: disk shortage %s-%s< %s' % (remSpace,datasetSize,thrForThisSite))
                    continue
            self.putLog('weight %s %s' % (tmpCan,tmpW))
            # get total weight    
            totalW += tmpW
            # append candidate
            allCandidates.append(tmpCan)
        # no candidate
        if allCandidates == []:
            return None
        # sort for reproducibility
        allCandidates.sort()
        # choose site    
        rNumber = random.random() * totalW
        for tmpCan in allCandidates:
            rNumber -= canWeights[tmpCan]
            if rNumber <= 0:
                return tmpCan
        return allCandidates[-1]
