"""
update dataset DB, and then close dataset and start Activator if needed

"""

import re
import sys

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.dataservice import Notifier
from pandaserver.dataservice.Activator import Activator
from pandaserver.taskbuffer import EventServiceUtils

# logger
_logger = PandaLogger().getLogger("Closer")


def initLogger(pLogger):
    # redirect logging to parent as it doesn't work in nested threads
    global _logger
    _logger = pLogger
    Notifier.initLogger(_logger)


class Closer:
    # constructor
    def __init__(self, taskBuffer, destinationDBlocks, job, pandaDDM=False, datasetMap={}):
        self.taskBuffer = taskBuffer
        self.destinationDBlocks = destinationDBlocks
        self.job = job
        self.pandaID = job.PandaID
        self.pandaDDM = pandaDDM
        self.siteMapper = None
        self.datasetMap = datasetMap
        self.allSubFinished = None

    # to keep backward compatibility
    def start(self):
        self.run()

    def join(self):
        pass

    # main
    def run(self):
        try:
            _logger.debug(f"{self.pandaID} Start {self.job.jobStatus}")
            flagComplete = True
            topUserDsList = []
            usingMerger = False
            disableNotifier = False
            firstIndvDS = True
            finalStatusDS = []
            for destinationDBlock in self.destinationDBlocks:
                dsList = []
                _logger.debug(f"{self.pandaID} start {destinationDBlock}")
                # ignore tid datasets
                if re.search("_tid[\d_]+$", destinationDBlock):
                    _logger.debug(f"{self.pandaID} skip {destinationDBlock}")
                    continue
                # ignore HC datasets
                if re.search("^hc_test\.", destinationDBlock) is not None or re.search("^user\.gangarbt\.", destinationDBlock) is not None:
                    if re.search("_sub\d+$", destinationDBlock) is None and re.search("\.lib$", destinationDBlock) is None:
                        _logger.debug(f"{self.pandaID} skip HC {destinationDBlock}")
                        continue
                # query dataset
                if destinationDBlock in self.datasetMap:
                    dataset = self.datasetMap[destinationDBlock]
                else:
                    dataset = self.taskBuffer.queryDatasetWithMap({"name": destinationDBlock})
                if dataset is None:
                    _logger.error(f"{self.pandaID} Not found : {destinationDBlock}")
                    flagComplete = False
                    continue
                # skip tobedeleted/tobeclosed
                if dataset.status in ["cleanup", "tobeclosed", "completed", "deleted"]:
                    _logger.debug(f"{self.pandaID} skip {destinationDBlock} due to {dataset.status}")
                    continue
                dsList.append(dataset)
                # sort
                dsList.sort()
                # count number of completed files
                notFinish = self.taskBuffer.countFilesWithMap({"destinationDBlock": destinationDBlock, "status": "unknown"})
                if notFinish < 0:
                    _logger.error(f"{self.pandaID} Invalid DB return : {notFinish}")
                    flagComplete = False
                    continue
                # check if completed
                _logger.debug(f"{self.pandaID} notFinish:{notFinish}")
                if self.job.destinationSE == "local" and self.job.prodSourceLabel in [
                    "user",
                    "panda",
                ]:
                    # close non-DQ2 destinationDBlock immediately
                    finalStatus = "closed"
                elif self.job.lockedby == "jedi" and self.isTopLevelDS(destinationDBlock):
                    # set it closed in order not to trigger DDM cleanup. It will be closed by JEDI
                    finalStatus = "closed"
                elif self.job.prodSourceLabel in ["user"] and "--mergeOutput" in self.job.jobParameters and self.job.processingType != "usermerge":
                    # merge output files
                    if firstIndvDS:
                        # set 'tobemerged' to only the first dataset to avoid triggering many Mergers for --individualOutDS
                        finalStatus = "tobemerged"
                        firstIndvDS = False
                    else:
                        finalStatus = "tobeclosed"
                    # set merging to top dataset
                    usingMerger = True
                    # disable Notifier
                    disableNotifier = True
                elif self.job.produceUnMerge():
                    finalStatus = "doing"
                else:
                    # set status to 'tobeclosed' to trigger DQ2 closing
                    finalStatus = "tobeclosed"
                if notFinish == 0 and EventServiceUtils.isEventServiceMerge(self.job):
                    allInJobsetFinished = self.checkSubDatasetsInJobset()
                else:
                    allInJobsetFinished = True
                if notFinish == 0 and allInJobsetFinished:
                    _logger.debug(f"{self.pandaID} set {finalStatus} to dataset : {destinationDBlock}")
                    # set status
                    dataset.status = finalStatus
                    # update dataset in DB
                    retT = self.taskBuffer.updateDatasets(
                        dsList,
                        withLock=True,
                        withCriteria="status<>:crStatus AND status<>:lockStatus ",
                        criteriaMap={":crStatus": finalStatus, ":lockStatus": "locked"},
                    )
                    if len(retT) > 0 and retT[0] == 1:
                        finalStatusDS += dsList
                        # close user datasets
                        if (
                            self.job.prodSourceLabel in ["user"]
                            and self.job.destinationDBlock.endswith("/")
                            and (dataset.name.startswith("user") or dataset.name.startswith("group"))
                        ):
                            # get top-level user dataset
                            topUserDsName = re.sub("_sub\d+$", "", dataset.name)
                            # update if it is the first attempt
                            if topUserDsName != dataset.name and topUserDsName not in topUserDsList and self.job.lockedby != "jedi":
                                topUserDs = self.taskBuffer.queryDatasetWithMap({"name": topUserDsName})
                                if topUserDs is not None:
                                    # check status
                                    if topUserDs.status in [
                                        "completed",
                                        "cleanup",
                                        "tobeclosed",
                                        "deleted",
                                        "tobemerged",
                                        "merging",
                                    ]:
                                        _logger.debug(f"{self.pandaID} skip {topUserDsName} due to status={topUserDs.status}")
                                    else:
                                        # set status
                                        if self.job.processingType.startswith("gangarobot") or self.job.processingType.startswith("hammercloud"):
                                            # not trigger freezing for HC datasets so that files can be appended
                                            topUserDs.status = "completed"
                                        elif not usingMerger:
                                            topUserDs.status = finalStatus
                                        else:
                                            topUserDs.status = "merging"
                                        # append to avoid repetition
                                        topUserDsList.append(topUserDsName)
                                        # update DB
                                        retTopT = self.taskBuffer.updateDatasets(
                                            [topUserDs],
                                            withLock=True,
                                            withCriteria="status<>:crStatus",
                                            criteriaMap={":crStatus": topUserDs.status},
                                        )
                                        if len(retTopT) > 0 and retTopT[0] == 1:
                                            _logger.debug(f"{self.pandaID} set {topUserDs.status} to top dataset : {topUserDsName}")
                                        else:
                                            _logger.debug(f"{self.pandaID} failed to update top dataset : {topUserDsName}")
                            # get parent dataset for merge job
                            if self.job.processingType == "usermerge":
                                tmpMatch = re.search("--parentDS ([^ '\"]+)", self.job.jobParameters)
                                if tmpMatch is None:
                                    _logger.error(f"{self.pandaID} failed to extract parentDS")
                                else:
                                    unmergedDsName = tmpMatch.group(1)
                                    # update if it is the first attempt
                                    if unmergedDsName not in topUserDsList:
                                        unmergedDs = self.taskBuffer.queryDatasetWithMap({"name": unmergedDsName})
                                        if unmergedDs is None:
                                            _logger.error(f"{self.pandaID} failed to get parentDS={unmergedDsName} from DB")
                                        else:
                                            # check status
                                            if unmergedDs.status in [
                                                "completed",
                                                "cleanup",
                                                "tobeclosed",
                                            ]:
                                                _logger.debug(f"{self.pandaID} skip {unmergedDsName} due to status={unmergedDs.status}")
                                            else:
                                                # set status
                                                unmergedDs.status = finalStatus
                                                # append to avoid repetition
                                                topUserDsList.append(unmergedDsName)
                                                # update DB
                                                retTopT = self.taskBuffer.updateDatasets(
                                                    [unmergedDs],
                                                    withLock=True,
                                                    withCriteria="status<>:crStatus",
                                                    criteriaMap={":crStatus": unmergedDs.status},
                                                )
                                                if len(retTopT) > 0 and retTopT[0] == 1:
                                                    _logger.debug(f"{self.pandaID} set {unmergedDs.status} to parent dataset : {unmergedDsName}")
                                                else:
                                                    _logger.debug(f"{self.pandaID} failed to update parent dataset : {unmergedDsName}")
                        # start Activator
                        if re.search("_sub\d+$", dataset.name) is None:
                            if self.job.prodSourceLabel == "panda" and self.job.processingType in ["merge", "unmerge"]:
                                # don't trigger Activator for merge jobs
                                pass
                            else:
                                if self.job.jobStatus == "finished":
                                    aThr = Activator(self.taskBuffer, dataset)
                                    aThr.start()
                                    aThr.join()
                    else:
                        # unset flag since another thread already updated
                        # flagComplete = False
                        pass
                else:
                    # update dataset in DB
                    self.taskBuffer.updateDatasets(
                        dsList,
                        withLock=True,
                        withCriteria="status<>:crStatus AND status<>:lockStatus ",
                        criteriaMap={":crStatus": finalStatus, ":lockStatus": "locked"},
                    )
                    # unset flag
                    flagComplete = False
                # end
                _logger.debug(f"{self.pandaID} end {destinationDBlock}")
            # special actions for vo
            if flagComplete:
                closerPluginClass = panda_config.getPlugin("closer_plugins", self.job.VO)
                if closerPluginClass is None and self.job.VO == "atlas":
                    # use ATLAS plugin for ATLAS
                    from pandaserver.dataservice.CloserAtlasPlugin import (
                        CloserAtlasPlugin,
                    )

                    closerPluginClass = CloserAtlasPlugin
                if closerPluginClass is not None:
                    closerPlugin = closerPluginClass(self.job, finalStatusDS, _logger)
                    closerPlugin.execute()
            # change pending jobs to failed
            finalizedFlag = True
            if flagComplete and self.job.prodSourceLabel == "user":
                _logger.debug(f"{self.pandaID} finalize {self.job.prodUserName} {self.job.jobDefinitionID}")
                finalizedFlag = self.taskBuffer.finalizePendingJobs(self.job.prodUserName, self.job.jobDefinitionID)
                _logger.debug(f"{self.pandaID} finalized with {finalizedFlag}")
            # update unmerged datasets in JEDI to trigger merging
            if flagComplete and self.job.produceUnMerge() and finalStatusDS != []:
                if finalizedFlag:
                    tmpStat = self.taskBuffer.updateUnmergedDatasets(self.job, finalStatusDS)
                    _logger.debug(f"{self.pandaID} updated unmerged datasets with {tmpStat}")
            # start notifier
            _logger.debug(f"{self.pandaID} source:{self.job.prodSourceLabel} complete:{flagComplete}")
            if (
                (self.job.jobStatus != "transferring")
                and ((flagComplete and self.job.prodSourceLabel == "user") or (self.job.jobStatus == "failed" and self.job.prodSourceLabel == "panda"))
                and self.job.lockedby != "jedi"
            ):
                # don't send email for merge jobs
                if (not disableNotifier) and self.job.processingType not in [
                    "merge",
                    "unmerge",
                ]:
                    useNotifier = True
                    summaryInfo = {}
                    # check all jobDefIDs in jobsetID
                    if self.job.jobsetID not in [0, None, "NULL"]:
                        (
                            useNotifier,
                            summaryInfo,
                        ) = self.taskBuffer.checkDatasetStatusForNotifier(
                            self.job.jobsetID,
                            self.job.jobDefinitionID,
                            self.job.prodUserName,
                        )
                        _logger.debug(f"{self.pandaID} useNotifier:{useNotifier}")
                    if useNotifier:
                        _logger.debug(f"{self.pandaID} start Notifier")
                        nThr = Notifier.Notifier(
                            self.taskBuffer,
                            self.job,
                            self.destinationDBlocks,
                            summaryInfo,
                        )
                        nThr.run()
                        _logger.debug(f"{self.pandaID} end Notifier")
            _logger.debug(f"{self.pandaID} End")
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            _logger.error(f"{errType} {errValue}")

    # check if top dataset
    def isTopLevelDS(self, datasetName):
        topDS = re.sub("_sub\d+$", "", datasetName)
        if topDS == datasetName:
            return True
        return False

    # check sub datasets with the same jobset
    def checkSubDatasetsInJobset(self):
        # skip already checked
        if self.allSubFinished is not None:
            return self.allSubFinished
        # get consumers in the jobset
        jobs = self.taskBuffer.getOriginalConsumers(self.job.jediTaskID, self.job.jobsetID, self.job.PandaID)
        checkedDS = set()
        for jobSpec in jobs:
            # collect all sub datasets
            subDatasets = set()
            for fileSpec in jobSpec.Files:
                if fileSpec.type == "output":
                    subDatasets.add(fileSpec.destinationDBlock)
            subDatasets = sorted(subDatasets)
            if len(subDatasets) > 0:
                # use the first sub dataset
                subDataset = subDatasets[0]
                # skip if already checked
                if subDataset in checkedDS:
                    continue
                checkedDS.add(subDataset)
                # count the number of unfinished
                notFinish = self.taskBuffer.countFilesWithMap({"destinationDBlock": subDataset, "status": "unknown"})
                if notFinish != 0:
                    _logger.debug(f"{self.pandaID} related sub dataset {subDataset} from {jobSpec.PandaID} has {notFinish} unfinished files")
                    self.allSubFinished = False
                    break
        if self.allSubFinished is None:
            _logger.debug(f"{self.pandaID} all related sub datasets are done")
            self.allSubFinished = True
        return self.allSubFinished
