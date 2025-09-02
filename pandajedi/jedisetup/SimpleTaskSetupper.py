import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore.MsgWrapper import MsgWrapper

from .TaskSetupperBase import TaskSetupperBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# task setup for general purpose
class SimpleTaskSetupper(TaskSetupperBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TaskSetupperBase.__init__(self, taskBufferIF, ddmIF)

    # main to setup task
    def doSetup(self, taskSpec, datasetToRegister, pandaJobs):
        # make logger
        tmpLog = MsgWrapper(logger, f"< jediTaskID={taskSpec.jediTaskID} >")
        tmpLog.info(f"start label={taskSpec.prodSourceLabel} taskType={taskSpec.taskType}")
        # returns
        retFatal = self.SC_FATAL
        retOK = self.SC_SUCCEEDED
        try:
            # get DDM I/F
            ddmIF = self.ddmIF.getInterface(taskSpec.vo, taskSpec.cloud)
            # skip if DDM I/F is inactive
            if not ddmIF:
                tmpLog.info("skip due to inactive DDM I/F")
                return retOK
            # collect datasetID to register datasets/containers just in case
            for tmpPandaJob in pandaJobs:
                if not tmpPandaJob.produceUnMerge():
                    for tmpFileSpec in tmpPandaJob.Files:
                        if tmpFileSpec.type in ["output", "log"]:
                            if tmpFileSpec.datasetID not in datasetToRegister:
                                datasetToRegister.append(tmpFileSpec.datasetID)
            # register datasets
            if datasetToRegister:
                tmpLog.info(f"datasetToRegister={str(datasetToRegister)}")
                # get site mapper
                siteMapper = self.taskBufferIF.get_site_mapper()

                # loop over all datasets
                avDatasetList = []
                cnDatasetMap = {}
                ddmBackEnd = "rucio"
                for datasetID in datasetToRegister:
                    # get output and log datasets
                    tmpLog.info(f"getting datasetSpec with datasetID={datasetID}")
                    tmpStat, datasetSpec = self.taskBufferIF.getDatasetWithID_JEDI(taskSpec.jediTaskID, datasetID)
                    if not tmpStat:
                        tmpLog.error("failed to get output and log datasets")
                        return retFatal
                    if datasetSpec.isPseudo():
                        tmpLog.info("skip pseudo dataset")
                        continue

                    # set lifetime
                    key_name = f"DATASET_LIFETIME_{datasetSpec.type}"
                    lifetime = self.taskBufferIF.getConfigValue("task_setup", key_name, "jedi", taskSpec.cloud)
                    if not lifetime:
                        lifetime = self.taskBufferIF.getConfigValue("task_setup", key_name, "jedi", taskSpec.vo)

                    tmpLog.info(f"checking {datasetSpec.datasetName}")
                    # check if dataset and container are available in DDM
                    for targetName in [datasetSpec.datasetName, datasetSpec.containerName]:
                        if not targetName:
                            continue
                        if targetName in avDatasetList:
                            tmpLog.info(f"{targetName} already registered")
                            continue
                        # check dataset/container in DDM
                        tmpList = ddmIF.listDatasets(targetName)
                        if not tmpList:
                            # get location
                            location = None
                            locForRule = None
                            if targetName == datasetSpec.datasetName:
                                # dataset
                                tmpLog.info(f"dest={datasetSpec.destination}")
                                if datasetSpec.destination:
                                    if siteMapper.checkSite(datasetSpec.destination):
                                        location = siteMapper.getSite(datasetSpec.destination).ddm_output["default"]
                                    else:
                                        location = datasetSpec.destination
                            if locForRule is None:
                                locForRule = location
                            # set metadata
                            if targetName == datasetSpec.datasetName:
                                metaData = {}
                                metaData["task_id"] = taskSpec.jediTaskID
                                if taskSpec.campaign:
                                    metaData["campaign"] = taskSpec.campaign
                            else:
                                metaData = None
                            # register dataset/container
                            tmpLog.info(f"registering {targetName} with location={location} backend={ddmBackEnd} lifetime={lifetime} meta={str(metaData)}")
                            tmpStat = ddmIF.registerNewDataset(targetName, backEnd=ddmBackEnd, location=location, lifetime=lifetime, metaData=metaData)
                            if not tmpStat:
                                tmpLog.error(f"failed to register {targetName}")
                                return retFatal
                            # register location
                            if locForRule:
                                """
                                if taskSpec.workingGroup:
                                    userName = taskSpec.workingGroup
                                else:
                                    userName = taskSpec.userName
                                """
                                userName = None
                                activity = None
                                grouping = None
                                tmpLog.info(
                                    f"registering location={locForRule} lifetime={lifetime} days activity={activity} grouping={grouping} owner={userName}"
                                )
                                tmpStat = ddmIF.registerDatasetLocation(
                                    targetName, locForRule, owner=userName, lifetime=lifetime, backEnd=ddmBackEnd, activity=activity, grouping=grouping
                                )
                                if not tmpStat:
                                    tmpLog.error(f"failed to register location {locForRule} for {targetName}")
                                    return retFatal
                            avDatasetList.append(targetName)

                    # check if dataset is in the container
                    if datasetSpec.containerName and datasetSpec.containerName != datasetSpec.datasetName:
                        # get list of constituent datasets in the container
                        if datasetSpec.containerName not in cnDatasetMap:
                            cnDatasetMap[datasetSpec.containerName] = ddmIF.listDatasetsInContainer(datasetSpec.containerName)
                        # add dataset
                        if datasetSpec.datasetName not in cnDatasetMap[datasetSpec.containerName]:
                            tmpLog.info(f"adding {datasetSpec.datasetName} to {datasetSpec.containerName}")
                            tmpStat = ddmIF.addDatasetsToContainer(datasetSpec.containerName, [datasetSpec.datasetName], backEnd=ddmBackEnd)
                            if not tmpStat:
                                tmpLog.error(f"failed to add {datasetSpec.datasetName} to {datasetSpec.containerName}")
                                return retFatal
                            cnDatasetMap[datasetSpec.containerName].append(datasetSpec.datasetName)
                        else:
                            tmpLog.info(f"{datasetSpec.datasetName} already in {datasetSpec.containerName}")
                    # update dataset
                    datasetSpec.status = "registered"
                    self.taskBufferIF.updateDataset_JEDI(datasetSpec, {"jediTaskID": taskSpec.jediTaskID, "datasetID": datasetID})
            # return
            tmpLog.info("done")
            return retOK
        except Exception as e:
            errStr = f"doSetup failed with {str(e)}"
            tmpLog.error(errStr + traceback.format_exc())
            taskSpec.setErrDiag(errStr)
            return retFatal
