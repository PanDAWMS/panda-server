"""
find candidate site to distribute input datasets

"""

import datetime
import fnmatch
import math
import random
import re
import sys
import time
import uuid

import pandaserver.brokerage.broker
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.dataservice.DDM import rucioAPI
from pandaserver.taskbuffer import JobUtils
from pandaserver.taskbuffer.JobSpec import JobSpec

# NG datasets
ngDataTypes = ["RAW", "HITS", "RDO", "ESD", "EVNT"]

# excluded provenance
ngProvenance = []

# protection for max number of replicas
protectionMaxNumReplicas = 10

# max number of waiting jobs
maxWaitingJobs = 200

# max number of waiting jobsets
maxWaitingJobsets = 2

# clouds with small T1 to make replica at T2
cloudsWithSmallT1 = ["IT"]

# files in datasets
g_filesInDsMap = {}


class DynDataDistributer:
    # constructor
    def __init__(self, jobs, taskBuffer, siteMapper, simul=False, token=None, logger=None):
        self.jobs = jobs
        self.taskBuffer = taskBuffer
        self.siteMapper = siteMapper
        if token is None:
            self.token = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(" ")
        else:
            self.token = token
        # use a fixed list since some clouds don't have active T2s
        self.pd2pClouds = ["CA", "DE", "ES", "FR", "IT", "ND", "NL", "TW", "UK", "US"]
        self.simul = simul
        self.lastMessage = ""
        self.cachedSizeMap = {}
        self.shareMoUForT2 = None
        self.mapTAGandParentGUIDs = {}
        self.tagParentInfo = {}
        self.parentLfnToTagMap = {}
        self.logger = logger if logger else PandaLogger().getLogger("DynDataDistributer")

    # main
    def run(self):
        try:
            self.putLog(f"start for {self.jobs[0].PandaID}")
            # check cloud
            if self.jobs[0].cloud not in self.pd2pClouds + [
                "CERN",
            ]:
                self.putLog(f"skip cloud={self.jobs[0].cloud} not one of PD2P clouds {str(self.pd2pClouds)}")
                self.putLog(f"end for {self.jobs[0].PandaID}")
                return
            # ignore HC and group production
            if self.jobs[0].processingType in [
                "hammercloud",
                "gangarobot",
            ] or self.jobs[
                0
            ].processingType.startswith("gangarobot"):
                self.putLog(f"skip due to processingType={self.jobs[0].processingType}")
                self.putLog(f"end for {self.jobs[0].PandaID}")
                return
            # ignore HC and group production
            if self.jobs[0].workingGroup not in ["NULL", None, ""]:
                self.putLog(f"skip due to workingGroup={self.jobs[0].workingGroup}")
                self.putLog(f"end for {self.jobs[0].PandaID}")
                return
            # get input datasets
            inputDatasets = []
            prodsourcelabels = []
            job_labels = []
            for tmpJob in self.jobs:
                if tmpJob.prodSourceLabel == "user":
                    for tmpFile in tmpJob.Files:
                        if tmpFile.type == "input" and not tmpFile.lfn.endswith(".lib.tgz"):
                            if tmpFile.dataset not in inputDatasets:
                                inputDatasets.append(tmpFile.dataset)
                                prodsourcelabels.append(tmpJob.prodSourceLabel)
                                job_labels.append(tmpJob.job_label)
            # loop over all input datasets
            for inputDS, prodsourcelabel, job_label in zip(inputDatasets, prodsourcelabels, job_labels):
                # only mc/data datasets
                moveFlag = False
                for projectName in ["mc", "data"]:
                    if inputDS.startswith(projectName):
                        moveFlag = True
                if not moveFlag:
                    self.putLog(f"skip non official dataset {inputDS}")
                    continue
                if re.search("_sub\d+$", inputDS) is not None or re.search("_dis\d+$", inputDS) is not None:
                    self.putLog(f"skip dis/sub dataset {inputDS}")
                    continue
                # check type
                tmpItems = inputDS.split(".")
                if len(tmpItems) < 5:
                    self.putLog(f"cannot get type from {inputDS}")
                    continue
                if tmpItems[4] in ngDataTypes:
                    self.putLog(f"don't move {tmpItems[4]} : {inputDS}")
                    continue
                # get candidate sites
                self.putLog(f"get candidates for {inputDS}")
                status, sitesMaps = self.getCandidates(inputDS, prodsourcelabel, job_label, useCloseSites=True)
                if not status:
                    self.putLog("failed to get candidates")
                    continue
                # get size of input container
                totalInputSize = 0
                if inputDS.endswith("/"):
                    status, totalInputSize = rucioAPI.getDatasetSize(inputDS)
                    if not status:
                        self.putLog(f"failed to get size of {inputDS}")
                        continue
                # get number of waiting jobs and jobsets
                nWaitingJobsAll = self.taskBuffer.getNumWaitingJobsForPD2P(inputDS)
                nWaitingJobsets = self.taskBuffer.getNumWaitingJobsetsForPD2P(inputDS)
                # loop over all datasets
                usedSites = []
                for tmpDS in sitesMaps:
                    tmpVal = sitesMaps[tmpDS]
                    self.putLog(f"triggered for {tmpDS}", sendLog=True)
                    # increment used counter
                    if not self.simul:
                        nUsed = self.taskBuffer.incrementUsedCounterSubscription(tmpDS)
                    else:
                        nUsed = 5
                    # insert dummy for new dataset which is used to keep track of usage even if subscription is not made
                    if nUsed == 0:
                        retAddUserSub = self.taskBuffer.addUserSubscription(tmpDS, ["DUMMY"])
                        if not retAddUserSub:
                            self.putLog(
                                f"failed to add dummy subscription to database for {tmpDS} ",
                                type="error",
                                sendLog=True,
                            )
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
                        (
                            candSites,
                            sitesComDS,
                            sitesPd2pDS,
                            nUserSub,
                            t1HasReplica,
                            t1HasPrimary,
                            nSecReplicas,
                            nT1Sub,
                            candForMoU,
                        ) = tmpVal[tmpCloud]
                        self.putLog(
                            "%s sites with comp DS:%s compPD2P:%s candidates:%s nSub:%s T1:%s Pri:%s nSec:%s nT1Sub:%s candMoU:%s"
                            % (
                                tmpCloud,
                                str(sitesComDS),
                                str(sitesPd2pDS),
                                str(candSites),
                                nUserSub,
                                t1HasReplica,
                                t1HasPrimary,
                                nSecReplicas,
                                nT1Sub,
                                str(candForMoU),
                            )
                        )
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
                            if tmpCandSite not in usedSites:
                                allCandidates.append(tmpCandSite)
                        # add candidates for MoU
                        for tmpCandSite in candForMoU:
                            if tmpCandSite not in usedSites:
                                allCandidatesMoU.append(tmpCandSite)
                        # add clouds
                        if tmpCloud not in allOKClouds:
                            allOKClouds.append(tmpCloud)
                    self.putLog(f"PD2P sites with comp replicas : {str(allCompPd2pSites)}")
                    self.putLog(f"PD2P T2 candidates : {str(allCandidates)}")
                    self.putLog(f"PD2P T2 MoU candidates : {str(allCandidatesMoU)}")
                    self.putLog(f"PD2P # of T2 subscriptions : {totalUserSub}")
                    self.putLog(f"PD2P # of T1 secondaries   : {totalSecReplicas}")
                    self.putLog(f"PD2P # of T1 subscriptions : {nT1Sub}")
                    self.putLog(f"PD2P # of T1 replicas : {nTier1Copies}")
                    self.putLog(f"PD2P T1 candidates : {str(allT1Candidates)}")
                    self.putLog(f"PD2P nUsed : {nUsed}")
                    # get dataset size
                    retDsSize, dsSize = rucioAPI.getDatasetSize(tmpDS)
                    if not retDsSize:
                        self.putLog(
                            f"failed to get dataset size of {tmpDS}",
                            type="error",
                            sendLog=True,
                        )
                        continue
                    self.putLog(f"PD2P nWaitingJobsets : {nWaitingJobsets}")
                    if totalInputSize != 0:
                        self.putLog(
                            "PD2P nWaitingJobs    : %s = %s(all)*%s(dsSize)/%s(contSize)"
                            % (
                                int((float(nWaitingJobsAll * dsSize) / float(totalInputSize))),
                                nWaitingJobsAll,
                                dsSize,
                                totalInputSize,
                            )
                        )
                    else:
                        self.putLog(f"PD2P nWaitingJobs    : {nWaitingJobsAll} = {nWaitingJobsAll}(all)")
                    # make T1-T1
                    triggeredT1PD2P = False
                    if nUsed > 0:
                        # extract integer part. log10(nUsed) and log10(nUsed)+1 are used to avoid round-off error
                        intLog10nUsed = int(math.log10(nUsed))
                        if self.simul or (
                            int(math.log10(nUsed)) > totalSecReplicas
                            and (nUsed == 10**intLog10nUsed or nUsed == 10 ** (intLog10nUsed + 1))
                            and nT1Sub == 0
                            and allT1Candidates != []
                        ):
                            self.putLog("making T1-T1", sendLog=True)
                            # make subscription
                            retT1Sub, useSmallT1 = self.makeT1Subscription(
                                allT1Candidates,
                                tmpDS,
                                dsSize,
                                prodsourcelabel,
                                job_label,
                                nUsed,
                            )
                            self.putLog("done for T1-T1")
                            triggeredT1PD2P = True
                    # make a T2 copy when T1 PD2P was triggered
                    if triggeredT1PD2P:
                        # TODO
                        retT2MoU, selectedSite = self.makeT2SubscriptionMoU(
                            allCandidatesMoU,
                            tmpDS,
                            dsSize,
                            " T1MOU",
                            prodsourcelabel,
                            job_label,
                            nUsed,
                        )
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
                        tmpN = int(math.log10(tmpN / float(maxWaitingJobs))) + nTier1Copies
                        maxSitesHaveDS = max(maxSitesHaveDS, tmpN)
                    # protection against too many replications
                    maxSitesHaveDS = min(maxSitesHaveDS, protectionMaxNumReplicas)
                    self.putLog(f"PD2P maxSitesHaveDS : {maxSitesHaveDS}")
                    # ignore the first job
                    if nUsed == 0:
                        self.putLog(
                            "skip the first job",
                            sendLog=True,
                            actionTag="SKIPPED",
                            tagsMap={"reason": "FIRSTJOB", "dataset": tmpDS},
                        )
                        if not self.simul:
                            continue
                    # check number of replicas
                    if len(allCompPd2pSites) >= maxSitesHaveDS and nUsed != 1:
                        self.putLog(
                            f"skip since many T2 PD2P sites ({len(allCompPd2pSites)}>={maxSitesHaveDS}) have the replica",
                            sendLog=True,
                            actionTag="SKIPPED",
                            tagsMap={
                                "reason": "TOO_MANY_T2_REPLICAS",
                                "dataset": tmpDS,
                            },
                        )
                        if not self.simul:
                            continue
                    # check the number of subscriptions
                    maxNumSubInAllCloud = max(0, maxSitesHaveDS - len(allCompPd2pSites))
                    maxNumSubInAllCloud = min(2, maxNumSubInAllCloud)
                    self.putLog(f"PD2P maxNumSubInAllCloud : {maxNumSubInAllCloud}")
                    if totalUserSub >= maxNumSubInAllCloud:
                        self.putLog(
                            f"skip since enough subscriptions ({totalUserSub}>={maxNumSubInAllCloud}) were already made for T2 PD2P",
                            sendLog=True,
                            actionTag="SKIPPED",
                            tagsMap={
                                "reason": "TOO_MANY_T2_SUBSCRIPTIONS",
                                "dataset": tmpDS,
                            },
                        )
                        if not self.simul:
                            continue
                    # no candidates
                    if len(allCandidates) == 0:
                        self.putLog(
                            "skip since no candidates",
                            sendLog=True,
                            actionTag="SKIPPED",
                            tagsMap={"reason": "NO_T2_CANDIDATE", "dataset": tmpDS},
                        )
                        continue
                    # get inverse weight for brokerage
                    weightForBrokerage = self.getWeightForBrokerage(
                        allCandidates,
                        tmpDS,
                        nReplicasInCloud,
                        prodsourcelabel,
                        job_label,
                    )
                    self.putLog(f"inverse weight {str(weightForBrokerage)}")
                    # get free disk size
                    self.putLog("getting free disk size for T2 PD2P")
                    retFreeSizeMap, freeSizeMap = self.getFreeDiskSize(tmpDS, allCandidates, prodsourcelabel, job_label)
                    if not retFreeSizeMap:
                        self.putLog("failed to get free disk size", type="error", sendLog=True)
                        continue
                    # run brokerage
                    tmpJob = JobSpec()
                    tmpJob.AtlasRelease = ""
                    self.putLog(f"run brokerage for {tmpDS}")
                    usedWeight = pandaserver.brokerage.broker.schedule(
                        [tmpJob],
                        self.taskBuffer,
                        self.siteMapper,
                        True,
                        allCandidates,
                        True,
                        specialWeight=weightForBrokerage,
                        getWeight=True,
                        sizeMapForCheck=freeSizeMap,
                        datasetSize=dsSize,
                    )
                    selectedSite = tmpJob.computingSite
                    for tmpWeightSite in usedWeight:
                        tmpWeightStr = usedWeight[tmpWeightSite]
                        tmpTagsMap = {
                            "site": tmpWeightSite,
                            "weight": tmpWeightStr,
                            "dataset": tmpDS,
                        }
                        if tmpWeightSite == selectedSite:
                            if nUsed == 1:
                                tmpActionTag = "SELECTEDT2_JOB"
                            elif len(allCompPd2pSites) == 0:
                                tmpActionTag = "SELECTEDT2_NOREP"
                            else:
                                tmpActionTag = "SELECTEDT2_WAIT"
                            tmpTagsMap["nused"] = nUsed
                            tmpTagsMap["nwaitingjobs"] = nWaitingJobsAll
                            tmpTagsMap["nwaitingjobsets"] = nWaitingJobsets
                            tmpTagsMap["nsiteshaveds"] = len(allCompPd2pSites)
                        else:
                            tmpActionTag = "UNSELECTEDT2"
                        self.putLog(
                            f"weight {tmpWeightSite} {tmpWeightStr}",
                            sendLog=True,
                            actionTag=tmpActionTag,
                            tagsMap=tmpTagsMap,
                        )
                    self.putLog(f"site for T2 PD2P -> {selectedSite}")
                    # remove from candidate list
                    if selectedSite in allCandidates:
                        allCandidates.remove(selectedSite)
                    if selectedSite in allCandidatesMoU:
                        allCandidatesMoU.remove(selectedSite)
                    # make subscription
                    if not self.simul:
                        selectedSiteSpec = self.siteMapper.getSite(selectedSite)
                        scope_input, scope_output = select_scope(selectedSiteSpec, prodsourcelabel, job_label)
                        subRet, dq2ID = self.makeSubscription(tmpDS, selectedSite, scope_input, ddmShare="secondary")
                        self.putLog(
                            f"made subscription to {selectedSite}:{dq2ID}",
                            sendLog=True,
                        )
                        usedSites.append(selectedSite)
                        # update database
                        if subRet:
                            self.taskBuffer.addUserSubscription(tmpDS, [dq2ID])
                    # additional T2 copy with MoU share when it is the second submission
                    if nUsed == 1 or self.simul:
                        retT2MoU, selectedSite = self.makeT2SubscriptionMoU(
                            allCandidatesMoU,
                            tmpDS,
                            dsSize,
                            "T2MOU",
                            prodsourcelabel,
                            job_label,
                            nUsed,
                        )
            self.putLog(f"end for {self.jobs[0].PandaID}")
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            self.putLog(f"{errType} {errValue}", "error")

    # get candidate sites for subscription
    def getCandidates(
        self,
        inputDS,
        prodsourcelabel,
        job_label,
        checkUsedFile=True,
        useHidden=False,
        useCloseSites=False,
    ):
        # return for failure
        failedRet = False, {"": {"": ([], [], [], 0, False, False, 0, 0, [])}}
        # get replica locations
        if inputDS.endswith("/"):
            # container
            status, tmpRepMaps = self.getListDatasetReplicasInContainer(inputDS)
            # get used datasets
            if status and checkUsedFile:
                status, tmpUsedDsList = self.getUsedDatasets(tmpRepMaps)
                # remove unused datasets
                newRepMaps = {}
                for tmpKey in tmpRepMaps:
                    tmpVal = tmpRepMaps[tmpKey]
                    if tmpKey in tmpUsedDsList:
                        newRepMaps[tmpKey] = tmpVal
                tmpRepMaps = newRepMaps
        else:
            # normal dataset
            status, tmpRepMap = self.getListDatasetReplicas(inputDS)
            tmpRepMaps = {inputDS: tmpRepMap}
        if not status:
            # failed
            self.putLog(f"failed to get replica locations for {inputDS}", "error")
            return failedRet
        # get close sites
        closeSitesMap = {}
        # get all sites
        allSiteMap = []
        for tmpSiteName in self.siteMapper.siteSpecList:
            tmpSiteSpec = self.siteMapper.siteSpecList[tmpSiteName]
            # check cloud
            if tmpSiteSpec.cloud not in self.pd2pClouds:
                continue
            # ignore test sites
            if "test" in tmpSiteName.lower():
                continue
            # analysis only
            if not tmpSiteSpec.runs_analysis():
                continue
            # skip GPU
            if tmpSiteSpec.isGPU():
                continue
            # skip VP
            if tmpSiteSpec.use_vp(JobUtils.ANALY_PS):
                continue
            # online
            if tmpSiteSpec.status not in ["online"]:
                continue
            allSiteMap.append(tmpSiteSpec)
        # NG DQ2 IDs
        ngDQ2SuffixList = ["LOCALGROUPDISK", "STAGING"]
        # loop over all clouds
        returnMap = {}
        cloud = "WORLD"
        # loop over all datasets
        for tmpDS in tmpRepMaps:
            tmpRepMap = tmpRepMaps[tmpDS]
            candSites = []
            sitesComDS = []
            sitesCompPD2P = []
            t1HasReplica = False
            t1HasPrimary = False
            nSecReplicas = 0
            candForMoU = []
            # check sites
            nUserSub = 0
            for tmpSiteSpec in allSiteMap:
                tmp_scope_input, tmp_scope_output = select_scope(tmpSiteSpec, prodsourcelabel, job_label)
                if tmp_scope_input not in tmpSiteSpec.ddm_endpoints_input:
                    continue
                rses = tmpSiteSpec.ddm_endpoints_input[tmp_scope_input].getLocalEndPoints()
                hasReplica = False
                for tmpDQ2ID in tmpRepMap:
                    tmpStatMap = tmpRepMap[tmpDQ2ID]
                    if tmpDQ2ID in rses and tmpStatMap[0]["total"] == tmpStatMap[0]["found"] and tmpDQ2ID.endswith("DATADISK"):
                        # complete
                        sitesComDS.append(tmpSiteSpec.sitename)
                        hasReplica = True
                        break
                # site doesn't have a replica
                if hasReplica or not useCloseSites:
                    candSites.append(tmpSiteSpec.sitename)
            # append
            returnMap.setdefault(tmpDS, {})
            if sitesComDS:
                candSites = sitesComDS
            returnMap[tmpDS][cloud] = (
                candSites,
                sitesComDS,
                sitesCompPD2P,
                nUserSub,
                t1HasReplica,
                t1HasPrimary,
                nSecReplicas,
                0,
                candForMoU,
            )
        # return
        return True, returnMap

    def getDQ2ID(self, sitename: str, dataset: str, scope: str) -> str:
        """
        Get the DQ2 ID for the given site and dataset.

        Args:
            sitename: The name of the site.
            dataset: The name of the dataset.
            scope: The scope of the dataset.

        Returns:
            The DQ2 ID for the site and dataset.
        """
        if not self.siteMapper.checkSite(sitename):
            self.putLog(f"cannot find SiteSpec for {sitename}")
            return ""

        dq2_id = self.siteMapper.getSite(sitename).ddm_input[scope]

        match_eos = re.search("_EOS[^_]+DISK$", dq2_id)
        if match_eos is not None:
            dq2_id = re.sub("_EOS[^_]+DISK", "_EOSDATADISK", dq2_id)
        else:
            dq2_id = re.sub("_[^_]+DISK", "_DATADISK", dq2_id)

        # Patch for MWT2_UC
        if dq2_id == "MWT2_UC_DATADISK":
            dq2_id = "MWT2_DATADISK"

        return dq2_id

    # get list of datasets
    def makeSubscription(self, dataset: str, sitename: str, scope: str, givenDQ2ID: Optional[str] = None, ddmShare: str = "secondary") -> Tuple[bool, str]:
        """
        Register a new dataset subscription for the given site and dataset.

        Args:
            dataset: The name of the dataset.
            sitename: The name of the site.
            scope: The scope of the dataset.
            givenDQ2ID: The DQ2 ID. If not provided, the function will attempt to retrieve it.
            ddmShare: The DDM share. Default is "secondary".

        Returns:
            A tuple where the first element is a boolean indicating success or failure, and the second element is the DQ2 ID.
        """
        if givenDQ2ID is None:
            dq2ID = self.getDQ2ID(sitename, dataset, scope)
        else:
            dq2ID = givenDQ2ID

        if dq2ID == "":
            self.putLog(f"cannot find DQ2 ID for {sitename}:{dataset}")
            return False, ""

        # register subscription
        self.putLog(f"registerDatasetSubscription {dataset} {dq2ID}")

        for _ in range(3):
            try:
                status = rucioAPI.registerDatasetSubscription(dataset, [dq2ID], activity="Data Brokering")
                if status:
                    self.putLog(f"{status} OK")
                    return True, dq2ID
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                out = f"{errType} {errValue}"
                self.putLog(out, "error")
                time.sleep(30)

        self.putLog(f"bad DDM response for {dataset}", "error")
        return False, ""

    def getWeightForBrokerage(self, sitenames: List[str], dataset: str, nReplicasInCloud: Dict[str, int],
                              prodsourcelabel: str, job_label: str) -> Dict[str, int]:
        """
        Calculate the weight for each site that is a candidate for data distribution.

        Args:
            sitenames: A list of site names that are candidates for data distribution.
            dataset: The name of the dataset that is to be distributed.
            nReplicasInCloud: A dictionary where the keys are the names of the clouds and the values are the number of replicas in each cloud.
            prodsourcelabel: The production source label.
            job_label: The job label.

        Returns:
            A dictionary where the keys are the site names and the values are the weights.
        """
        # return for failure
        retFailed = False, {}
        retMap = {}

        # get the number of subscriptions for last 24 hours
        numUserSubs = self.taskBuffer.getNumUserSubscriptions()

        # loop over all sites
        for sitename in sitenames:
            # get DQ2 ID
            siteSpec = self.siteMapper.getSite(sitename)
            scope_input, scope_output = select_scope(siteSpec, prodsourcelabel, job_label)
            dq2ID = self.getDQ2ID(sitename, dataset, scope_input)

            if dq2ID == "":
                self.putLog(f"cannot find DQ2 ID for {sitename}:{dataset}")
                return retFailed

            # append
            if dq2ID in numUserSubs:
                retMap[sitename] = 1 + numUserSubs[dq2ID]
            else:
                retMap[sitename] = 1

            # negative weight if a cloud already has replicas
            tmpCloud = self.siteMapper.getSite(sitename).cloud
            retMap[sitename] *= 1 + nReplicasInCloud[tmpCloud]

        # return
        return retMap

    def getFreeDiskSize(self, dataset: str, siteList: List[str], prodsourcelabel: str, job_label: str) -> Tuple[
        bool, Dict[str, Any]]:
        """
        Get the free disk size for a given dataset and a list of sites.

        Args:
            dataset: The name of the dataset.
            siteList: A list of site names.
            prodsourcelabel: The production source label.
            job_label: The job label.

        Returns:
            A tuple where the first element is a boolean indicating success or failure, and the second element is a dictionary containing the free disk size for each site.
        """
        # return for failure
        retFailed = False, {}
        # loop over all sites
        sizeMap = {}
        for sitename in siteList:
            # reuse cached value
            if sitename in self.cachedSizeMap:
                sizeMap[sitename] = self.cachedSizeMap[sitename]
                continue

            # get DQ2 IDs
            siteSpec = self.siteMapper.getSite(sitename)
            scope_input, scope_output = select_scope(siteSpec, prodsourcelabel, job_label)
            dq2ID = self.getDQ2ID(sitename, dataset, scope_input)

            if dq2ID == "":
                self.putLog(f"cannot find DQ2 ID for {sitename}:{dataset}")
                return retFailed

            tmpMap = rucioAPI.getRseUsage(dq2ID)
            if tmpMap == {}:
                self.putLog(f"getRseUsage failed for {sitename}")

            # append
            sizeMap[sitename] = tmpMap
            # cache
            self.cachedSizeMap[sitename] = sizeMap[sitename]

        # return
        self.putLog(f"getFreeDiskSize done->{str(sizeMap)}")
        return True, sizeMap

    def getListDatasetReplicas(self, dataset: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Get the list of replicas for a given dataset.

        Args:
            dataset: The name of the dataset.

        Returns:
            A tuple where the first element is a boolean indicating success or failure, and the second element is a dictionary containing the replicas.
        """
        for attempt in range(3):
            self.putLog(f"{attempt}/3 listDatasetReplicas {dataset}")
            status, out = rucioAPI.listDatasetReplicas(dataset)
            if status == 0:
                break
            time.sleep(10)

        if status != 0:
            self.putLog(out, "error")
            self.putLog(f"bad response for {dataset}", "error")
            return False, {}

        self.putLog(f"getListDatasetReplicas->{str(out)}")
        return True, out

    def getListDatasetReplicasInContainer(self, container: str) -> Tuple[bool, Dict[str, Any]]:
        """
        Get the list of replicas for all datasets in a given container.

        Args:
            container: The name of the container.

        Returns:
            A tuple where the first element is a boolean indicating success or failure, and the second element is a dictionary containing the replicas for all datasets.
        """
        for attempt in range(3):
            self.putLog(f"{attempt}/3 listDatasetsInContainer {container}")
            datasets, out = rucioAPI.listDatasetsInContainer(container)
            if datasets is not None:
                break
            time.sleep(60)

        if datasets is None:
            self.putLog(out, "error")
            self.putLog(f"bad response for {container}", "error")
            return False, {}

        allRepMap = {}
        for dataset in datasets:
            status, tmpRepSites = self.getListDatasetReplicas(dataset)
            if not status:
                return False, {}
            allRepMap[dataset] = tmpRepSites

        self.putLog("getListDatasetReplicasInContainer done")
        return True, allRepMap

    def getUsedDatasets(self, datasetMap: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Get the list of datasets that are used by jobs.

        Args:
            datasetMap: A dictionary where the keys are dataset names and the values are replica maps.

        Returns:
            A tuple where the first element is a boolean indicating success or failure, and the second element is a list of used datasets.
        """
        # loop over all datasets
        usedDsList = []
        for datasetName in datasetMap:
            # get file list
            for attempt in range(3):
                try:
                    self.putLog(f"{attempt}/3 listFilesInDataset {datasetName}")
                    fileItems, out = rucioAPI.listFilesInDataset(datasetName)
                    status = True
                    break
                except Exception:
                    status = False
                    errType, errValue = sys.exc_info()[:2]
                    out = f"{errType} {errValue}"
                    time.sleep(60)
            if not status:
                self.putLog(out, "error")
                self.putLog(f"bad DDM response to get size of {datasetName}", "error")
                return False, []

            # check if jobs use the dataset
            usedFlag = False
            for tmpJob in self.jobs:
                for tmpFile in tmpJob.Files:
                    if tmpFile.type == "input" and tmpFile.lfn in fileItems:
                        usedFlag = True
                        break
                # escape
                if usedFlag:
                    break
            # used
            if usedFlag:
                usedDsList.append(datasetName)
        # return
        self.putLog(f"used datasets = {str(usedDsList)}")
        return True, usedDsList

    def getFileFromDataset(self, datasetName: str, guid: str, randomMode: bool = False, nSamples: int = 1) -> Tuple[
        bool, Union[Dict[str, Any], List[Dict[str, Any]]]]:
        """
        Get a file from a given dataset.

        Args:
            datasetName: The name of the dataset.
            guid: The GUID of the file.
            randomMode: If True, select a file randomly. Default is False.
            nSamples: The number of samples to select if randomMode is True. Default is 1.

        Returns:
            A tuple where the first element is a boolean indicating success or failure, and the second element is a dictionary containing the file information or a list of such dictionaries if randomMode is True.
        """
        global g_filesInDsMap
        if datasetName not in g_filesInDsMap:
            for attempt in range(3):
                try:
                    self.putLog(f"{attempt}/3 listFilesInDataset {datasetName}")
                    fileItems, out = rucioAPI.listFilesInDataset(datasetName)
                    status = True
                    break
                except Exception:
                    status = False
                    errType, errValue = sys.exc_info()[:2]
                    out = f"{errType} {errValue}"
                    time.sleep(60)
            if not status:
                self.putLog(out, "error")
                self.putLog(f"bad DDM response to get size of {datasetName}", "error")
                return False, None
            g_filesInDsMap[datasetName] = fileItems

        if randomMode:
            tmpList = list(g_filesInDsMap[datasetName])
            random.shuffle(tmpList)
            retList = []
            for iSamples in range(nSamples):
                if iSamples < len(tmpList):
                    tmpLFN = tmpList[iSamples]
                    retMap = g_filesInDsMap[datasetName][tmpLFN]
                    retMap["lfn"] = tmpLFN
                    retMap["dataset"] = datasetName
                    retList.append(retMap)
            return True, retList

        for tmpLFN in g_filesInDsMap[datasetName]:
            tmpVal = g_filesInDsMap[datasetName][tmpLFN]
            if uuid.UUID(tmpVal["guid"]) == uuid.UUID(guid):
                retMap = tmpVal
                retMap["lfn"] = tmpLFN
                retMap["dataset"] = datasetName
                return True, retMap
        return False, None

    def parse_dn(self, owner):
        """
        Parse the DN of the owner.

        Args:
            owner: The owner whose DN is to be parsed.

        Returns:
            The parsed DN of the owner.
        """
        if owner is not None:
            status, userInfo = rucioAPI.finger(owner)
            if not status:
                self.putLog(f"failed to finger: {userInfo}")
            else:
                owner = userInfo["nickname"]
            self.putLog(f"parsed DN={owner}")
        return owner

    def registerDatasetContainerWithDatasets(self, containerName, files, replicaMap, nSites=1, owner=None):
        """
        Register a new dataset container with datasets.

        Args:
            containerName: The name of the container.
            files: The files to be included in the datasets.
            replicaMap: The map of replicas.
            nSites: The number of sites.
            owner: The owner of the container.

        Returns:
            A boolean indicating success or failure.
        """
        owner = self.parse_dn(owner)

        # sort by locations
        filesMap = {}
        for tmpFile in files:
            tmpLocations = sorted(replicaMap[tmpFile["dataset"]])
            newLocations = []
            # skip STAGING
            for tmpLocation in tmpLocations:
                if not tmpLocation.endswith("STAGING"):
                    newLocations.append(tmpLocation)
            if newLocations == []:
                continue
            tmpLocations = newLocations
            tmpKey = tuple(tmpLocations)
            filesMap.setdefault(tmpKey, [])
            # append file
            filesMap[tmpKey].append(tmpFile)

        # get nfiles per dataset
        nFilesPerDataset, tmpR = divmod(len(files), nSites)
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
                tmpDsName = containerName[:-1] + "_%04d" % tmpIndex
                status, out = self.registerDatasetWithLocation(tmpDsName,
                                                               tmpFiles[tmpSubIndex: tmpSubIndex + nFilesPerDataset],
                                                               tmpLocations, owner)
                # failed
                if not status:
                    self.putLog(f"failed to register {tmpDsName}", "error")
                    return False
                # append dataset
                datasetNames.append(tmpDsName)
                tmpIndex += 1
                tmpSubIndex += nFilesPerDataset

        # register container
        for attempt in range(3):
            self.putLog(f"{attempt}/3 registerContainer {containerName}")
            try:
                status = rucioAPI.registerContainer(containerName, datasetNames)
                out = "OK"
                break
            except Exception:
                status = False
                errType, errValue = sys.exc_info()[:2]
                out = f"{errType} {errValue}"
                time.sleep(10)
        if not status:
            self.putLog(out, "error")
            self.putLog(f"bad DDM response to register {containerName}", "error")
            return False

        # return
        self.putLog(out)
        return True

    def registerNewDataset(self, datasetName, files):
        guids = []
        lfns = []
        fsizes = []
        chksums = []
        for tmpFile in files:
            guids.append(tmpFile["guid"])
            lfns.append(tmpFile["scope"] + ":" + tmpFile["lfn"])
            fsizes.append(int(tmpFile["filesize"]))
            chksums.append(tmpFile["checksum"])
        for attempt in range(3):
            try:
                self.putLog(f"{attempt}/3 registerNewDataset {datasetName} len={len(files)}")
                out = rucioAPI.registerDataset(datasetName, lfns, guids, fsizes, chksums, lifetime=14)
                self.putLog(out)
                return True
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                self.putLog(f"{errType} {errValue}", "error")
                if attempt + 1 == 3:
                    self.putLog(f"failed to register {datasetName} in rucio")
                    return False
                time.sleep(10)

    def freezeDataset(self, datasetName):
        for attempt in range(3):
            self.putLog(f"{attempt}/3 freezeDataset {datasetName}")
            try:
                rucioAPI.closeDataset(datasetName)
                return True
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                out = f"failed to freeze : {errtype} {errvalue}"
                self.putLog(out, "error")
                time.sleep(10)
        self.putLog(f"bad DDM response to freeze {datasetName}", "error")
        return False

    def registerDatasetLocations(self, datasetName, locations, owner=None):
        for tmpLocation in locations:
            for attempt in range(3):
                try:
                    self.putLog(f"{attempt}/3 registerDatasetLocation {datasetName} {tmpLocation}")
                    out = rucioAPI.registerDatasetLocation(datasetName, [tmpLocation], 14, owner)
                    self.putLog(out)
                    return True
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    self.putLog(f"{errType} {errValue}", "error")
                    if attempt + 1 == 3:
                        return False
                    time.sleep(10)
        return False
    def registerDatasetWithLocation(self, datasetName, files, locations, owner=None):
        """
        Register a new dataset with specific locations.

        Args:
            datasetName: The name of the dataset to be registered.
            files: The files to be included in the dataset.
            locations: The locations where the dataset will be registered.
            owner: The owner of the dataset.

        Returns:
            A boolean indicating success or failure.
        """
        # Register new dataset
        if not self.registerNewDataset(datasetName, files):
            self.putLog(f"failed to register {datasetName} in rucio")
            return False

        # Freeze dataset
        if not self.freezeDataset(datasetName):
            self.putLog(f"bad DDM response to freeze {datasetName}", "error")
            return False

        # Register locations
        for attempt in range(3):
            try:
                self.putLog(f"{attempt}/3 registerDatasetLocation {datasetName} {locations}")
                if not self.registerDatasetLocations(datasetName, locations, owner):
                    self.putLog(f"failed to register location for {datasetName} in rucio")
                    time.sleep(10)
                else:
                    break
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                self.putLog(f"{errType} {errValue}", "error")
                if attempt + 1 == 3:
                    return False
        return True

    # list datasets by file GUIDs
    def listDatasetsByGUIDs(self, guids, dsFilters):
        resForFailure = (False, {})
        resForFatal = (False, {"isFatal": True})
        # get size of datasets
        nTry = 3
        for iDDMTry in range(nTry):
            self.putLog(f"{iDDMTry}/{nTry} listDatasetsByGUIDs GUIDs={str(guids)}")
            try:
                out = rucioAPI.listDatasetsByGUIDs(guids)
                status = True
                break
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                out = f"failed to get datasets with GUIDs : {errtype} {errvalue}"
                status = False
                time.sleep(10)
        if not status:
            self.putLog(out, "error")
            self.putLog("bad response to list datasets by GUIDs", "error")
            if "DataIdentifierNotFound" in out:
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
                    self.putLog(f"GUID={guid} not found", "error")
                    return resForFatal
                # ignore junk datasets
                for tmpDsName in outMap[guid]:
                    if (
                        tmpDsName.startswith("panda")
                        or tmpDsName.startswith("user")
                        or tmpDsName.startswith("group")
                        or tmpDsName.startswith("archive")
                        or re.search("_sub\d+$", tmpDsName) is not None
                        or re.search("_dis\d+$", tmpDsName) is not None
                        or re.search("_shadow$", tmpDsName) is not None
                    ):
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
                    self.putLog(f"no datasets found for GUID={guid}")
                    continue
                # duplicated
                if len(tmpDsNames) != 1:
                    self.putLog(f"use the first dataset in {str(tmpDsNames)} for GUID:{guid}")
                # append
                retMap[guid] = tmpDsNames[0]
        except Exception:
            self.putLog("failed to list datasets by GUIDs", "error")
            return resForFailure
        return True, retMap

    # conver event/run list to datasets
    def convertEvtRunToDatasets(
        self,
        runEvtList,
        dsType,
        streamName,
        dsFilters,
        amiTag,
        user,
        runEvtGuidMap,
        ei_api,
    ):
        self.putLog(f"convertEvtRunToDatasets type={dsType} stream={streamName} dsPatt={str(dsFilters)} amitag={amiTag}")
        # check data type
        failedRet = False, {}, []
        fatalRet = False, {"isFatal": True}, []
        streamRef = "Stream" + dsType
        # import event lookup client
        if runEvtGuidMap == {}:
            if len(runEvtList) == 0:
                self.putLog("Empty list for run and events was provided", type="error")
                return failedRet
            # Hadoop EI
            from .eventLookupClientEI import eventLookupClientEI

            elssiIF = eventLookupClientEI()
            # loop over all events
            nEventsPerLoop = 500
            iEventsTotal = 0
            while iEventsTotal < len(runEvtList):
                tmpRunEvtList = runEvtList[iEventsTotal : iEventsTotal + nEventsPerLoop]
                self.putLog(f"EI lookup for {iEventsTotal}/{len(runEvtList)}")
                iEventsTotal += nEventsPerLoop
                regStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                guidListELSSI, tmpCom, tmpOut, tmpErr = elssiIF.doLookup(
                    tmpRunEvtList,
                    stream=streamName,
                    tokens=streamRef,
                    amitag=amiTag,
                    user=user,
                    ei_api=ei_api,
                )
                regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - regStart
                self.putLog(f"EI command: {tmpCom}")
                self.putLog(f"took {regTime.seconds}.{regTime.microseconds / 1000:03f} sec for {len(tmpRunEvtList)} events")
                # failed
                if tmpErr not in [None, ""] or len(guidListELSSI) == 0:
                    self.putLog(tmpCom)
                    self.putLog(tmpOut)
                    self.putLog(tmpErr)
                    self.putLog("invalid return from EventIndex", type="error")
                    return failedRet
                # check events
                for runNr, evtNr in tmpRunEvtList:
                    paramStr = f"Run:{runNr} Evt:{evtNr} Stream:{streamName}"
                    self.putLog(paramStr)
                    tmpRunEvtKey = (int(runNr), int(evtNr))
                    # not found
                    if tmpRunEvtKey not in guidListELSSI or len(guidListELSSI[tmpRunEvtKey]) == 0:
                        self.putLog(tmpCom)
                        self.putLog(tmpOut)
                        self.putLog(tmpErr)
                        errStr = f"no GUIDs were found in EventIndex for {paramStr}"
                        self.putLog(errStr, type="error")
                        return fatalRet
                    # append
                    runEvtGuidMap[tmpRunEvtKey] = guidListELSSI[tmpRunEvtKey]
        # convert to datasets
        allDatasets = []
        allFiles = []
        allLocations = {}
        for tmpIdx in runEvtGuidMap:
            tmpguids = runEvtGuidMap[tmpIdx]
            runNr, evtNr = tmpIdx
            tmpDsRet, tmpDsMap = self.listDatasetsByGUIDs(tmpguids, dsFilters)
            # failed
            if not tmpDsRet:
                self.putLog("failed to convert GUIDs to datasets", type="error")
                if "isFatal" in tmpDsMap and tmpDsMap["isFatal"] is True:
                    return fatalRet
                return failedRet
            # empty
            if tmpDsMap == {}:
                self.putLog(
                    f"there is no dataset for Run:{runNr} Evt:{evtNr} GUIDs:{str(tmpguids)}",
                    type="error",
                )
                return fatalRet
            if len(tmpDsMap) != 1:
                self.putLog(
                    f"there are multiple datasets {str(tmpDsMap)} for Run:{runNr} Evt:{evtNr} GUIDs:{str(tmpguids)}",
                    type="error",
                )
                return fatalRet
            # append
            for tmpGUID in tmpDsMap:
                tmpDsName = tmpDsMap[tmpGUID]
                # collect dataset names
                if tmpDsName not in allDatasets:
                    allDatasets.append(tmpDsName)
                    # get location
                    statRep, replicaMap = self.getListDatasetReplicas(tmpDsName)
                    # failed
                    if not statRep:
                        self.putLog(
                            f"failed to get locations for DS:{tmpDsName}",
                            type="error",
                        )
                        return failedRet
                    # collect locations
                    tmpLocationList = []
                    for tmpLocation in replicaMap:
                        # use only complete replicas
                        dsStatDict = replicaMap[tmpLocation][0]
                        if dsStatDict["total"] is not None and dsStatDict["total"] == dsStatDict["found"]:
                            if tmpLocation not in tmpLocationList:
                                tmpLocationList.append(tmpLocation)
                    allLocations[tmpDsName] = tmpLocationList
                # get file info
                tmpFileRet, tmpFileInfo = self.getFileFromDataset(tmpDsName, tmpGUID)
                # failed
                if not tmpFileRet:
                    self.putLog(
                        f"failed to get fileinfo for GUID:{tmpGUID} DS:{tmpDsName}",
                        type="error",
                    )
                    return failedRet
                # collect files
                allFiles.append(tmpFileInfo)
        # return
        self.putLog(f"converted to {str(allDatasets)}, {str(allLocations)}, {str(allFiles)}")
        return True, allLocations, allFiles

    # put log
    def putLog(self, msg, type="debug", sendLog=False, actionTag="", tagsMap={}):
        if self.logger is None:
            tmpMsg = self.token + " " + str(msg)
        else:
            tmpMsg = str(msg)
        if type == "error":
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
            tmpMsg = self.token + " - "
            if actionTag != "":
                tmpMsg += f"action={actionTag} "
                for tmpTag in tagsMap:
                    tmpTagVal = tagsMap[tmpTag]
                    tmpMsg += f"{tmpTag}={tmpTagVal} "
            tmpMsg += "- " + msg
            tmpPandaLogger = PandaLogger()
            tmpPandaLogger.lock()
            tmpPandaLogger.setParams({"Type": "pd2p"})
            tmpLog = tmpPandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            if type == "error":
                tmpLog.error(tmpMsg)
            else:
                tmpLog.info(tmpMsg)
            # release HTTP handler
            tmpPandaLogger.release()
            time.sleep(1)

    # make T1 subscription
    def makeT1Subscription(
        self,
        allCloudCandidates,
        tmpDS,
        dsSize,
        prodsourcelabel,
        job_label,
        nUsed=None,
        nWaitingJobs=None,
        nWaitingJobsets=None,
    ):
        useSmallT1 = None
        # no candidate
        if allCloudCandidates == []:
            return True, useSmallT1

        # convert to siteIDs
        t1Candidates = []
        t1Weights = {}
        siteToCloud = {}
        for tmpCloud in allCloudCandidates:
            tmpCloudSpec = self.siteMapper.getCloud(tmpCloud)
            tmpT1SiteID = tmpCloudSpec["source"]
            t1Candidates.append(tmpT1SiteID)
            # use MoU share
            t1Weights[tmpT1SiteID] = tmpCloudSpec["mcshare"]
            # reverse lookup
            siteToCloud[tmpT1SiteID] = tmpCloud
        # get free disk size
        self.putLog("getting free disk size for T1 PD2P")
        retFreeSizeMap, freeSizeMap = self.getFreeDiskSize(tmpDS, t1Candidates, prodsourcelabel, job_label)
        if not retFreeSizeMap:
            self.putLog("failed to get free disk size", type="error", sendLog=True)
            return False, useSmallT1
        # run brokerage
        tmpJob = JobSpec()
        tmpJob.AtlasRelease = ""
        self.putLog(f"run brokerage for T1-T1 for {tmpDS}")
        selectedSite = self.chooseSite(t1Weights, freeSizeMap, dsSize)
        self.putLog(f"site for T1 PD2P -> {selectedSite}")
        # simulation
        if self.simul:
            return True, useSmallT1
        # no candidate
        if selectedSite is None:
            self.putLog("no candidate for T1-T1")
            return False, useSmallT1
        # make subscription
        tmpJob.computingSite = selectedSite
        tmpSiteSpec = self.siteMapper.getSite(tmpJob.computingSite)
        scope_input, scope_output = select_scope(tmpSiteSpec, prodsourcelabel, job_label)
        subRet, dq2ID = self.makeSubscription(tmpDS, tmpJob.computingSite, scope_input)
        tmpTagsMap = {"site": tmpJob.computingSite, "dataset": tmpDS}
        if nUsed is not None:
            tmpTagsMap["nused"] = nUsed
        if nWaitingJobs is not None:
            tmpTagsMap["nwaitingjobs"] = nWaitingJobs
        if nWaitingJobsets is not None:
            tmpTagsMap["nwaitingjobsets"] = nWaitingJobsets
        self.putLog(
            f"made subscription for T1-T1 to {tmpJob.computingSite}:{dq2ID}",
            sendLog=True,
            actionTag="SELECTEDT1",
            tagsMap=tmpTagsMap,
        )
        # check if small cloud is used
        if siteToCloud[tmpJob.computingSite] in cloudsWithSmallT1:
            useSmallT1 = siteToCloud[tmpJob.computingSite]
        # update database
        if subRet:
            self.taskBuffer.addUserSubscription(tmpDS, [dq2ID])
            return True, useSmallT1
        else:
            return False, useSmallT1

    # make T2 subscription with MoU share
    def makeT2SubscriptionMoU(
        self,
        allCandidates,
        tmpDS,
        dsSize,
        pd2pType,
        prodsourcelabel,
        job_label,
        nUsed=None,
        nWaitingJobs=None,
        nWaitingJobsets=None,
    ):
        # no candidate
        if allCandidates == []:
            return True, None
        # get MoU share
        if self.shareMoUForT2 is None:
            self.shareMoUForT2 = self.taskBuffer.getMouShareForT2PD2P()
        # convert to DQ2 ID
        t2Candidates = []
        t2Weights = {}
        dq2List = []
        for tmpCandidate in allCandidates:
            tmpCandidateSpec = self.siteMapper.getSite(tmpCandidate)
            scope_input, scope_output = select_scope(tmpCandidateSpec, prodsourcelabel, job_label)
            tmpDQ2ID = self.getDQ2ID(tmpCandidate, tmpDS, scope_input)
            if tmpDQ2ID not in dq2List:
                # append
                dq2List.append(tmpDQ2ID)
                # get MoU share
                if tmpDQ2ID not in self.shareMoUForT2:
                    # site is undefined in t_regions_replication
                    self.putLog(f"{tmpDQ2ID} is not in MoU table", type="error")
                    continue
                if self.shareMoUForT2[tmpDQ2ID]["status"] not in ["ready"]:
                    # site is not ready
                    self.putLog(f"{tmpDQ2ID} is not ready in MoU table")
                    continue
                tmpWeight = self.shareMoUForT2[tmpDQ2ID]["weight"]
                # skip if the weight is 0
                if tmpWeight == 0:
                    self.putLog(f"{tmpDQ2ID} has 0 weight in MoU table")
                    continue
                # collect siteIDs and weights for brokerage
                t2Candidates.append(tmpCandidate)
                t2Weights[tmpCandidate] = tmpWeight
        # sort for reproducibility
        t2Candidates.sort()
        # get free disk size
        self.putLog(f"getting free disk size for T2 {pd2pType} PD2P")
        retFreeSizeMap, freeSizeMap = self.getFreeDiskSize(tmpDS, t2Candidates, prodsourcelabel, job_label)
        if not retFreeSizeMap:
            self.putLog("failed to get free disk size", type="error", sendLog=True)
            return False, None
        # run brokerage
        tmpJob = JobSpec()
        tmpJob.AtlasRelease = ""
        self.putLog(f"run brokerage for T2 with {pd2pType} for {tmpDS}")
        selectedSite = self.chooseSite(t2Weights, freeSizeMap, dsSize)
        self.putLog(f"site for T2 {pd2pType} PD2P -> {selectedSite}")
        # simulation
        if self.simul:
            return True, selectedSite
        # no candidate
        if selectedSite is None:
            self.putLog(f"no candidate for T2 with {pd2pType}")
            return False, None
        # make subscription
        selectedSiteSpec = self.siteMapper.getSite(selectedSite)
        scope_input, scope_output = select_scope(selectedSiteSpec, prodsourcelabel, job_label)
        subRet, dq2ID = self.makeSubscription(tmpDS, selectedSite, scope_input)
        tmpTagsMap = {"site": selectedSite, "dataset": tmpDS}
        if nUsed is not None:
            tmpTagsMap["nused"] = nUsed
        if nWaitingJobs is not None:
            tmpTagsMap["nwaitingjobs"] = nWaitingJobs
        if nWaitingJobsets is not None:
            tmpTagsMap["nwaitingjobsets"] = nWaitingJobsets
        self.putLog(
            f"made subscription for T2 with {pd2pType} to {selectedSite}:{dq2ID}",
            sendLog=True,
            actionTag=f"SELECTEDT2_{pd2pType}",
            tagsMap=tmpTagsMap,
        )
        # update database
        if subRet:
            self.taskBuffer.addUserSubscription(tmpDS, [dq2ID])
            return True, selectedSite
        else:
            return False, None

    # choose site
    def chooseSite(self, canWeights, freeSizeMap, datasetSize):
        # loop over all candidates
        totalW = 0
        allCandidates = []
        for tmpCan in canWeights:
            tmpW = canWeights[tmpCan]
            # size check
            if tmpCan in freeSizeMap:
                # disk threshold for PD2P max(5%,3TB)
                diskThresholdPD2P = 1024 * 3
                thrForThisSite = int(freeSizeMap[tmpCan]["total"] * 5 / 100)
                if thrForThisSite < diskThresholdPD2P:
                    thrForThisSite = diskThresholdPD2P
                remSpace = freeSizeMap[tmpCan]["total"] - freeSizeMap[tmpCan]["used"]
                if remSpace - datasetSize < thrForThisSite:
                    self.putLog(f"  skip: disk shortage {remSpace}-{datasetSize}< {thrForThisSite}")
                    continue
            self.putLog(f"weight {tmpCan} {tmpW}")
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
