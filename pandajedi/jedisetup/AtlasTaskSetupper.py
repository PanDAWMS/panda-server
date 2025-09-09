import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandaserver.dataservice import DataServiceUtils
from pandaserver.taskbuffer import EventServiceUtils, JobUtils

from .TaskSetupperBase import TaskSetupperBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# task setup for ATLAS
class AtlasTaskSetupper(TaskSetupperBase):
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
            ddmIF = self.ddmIF.getInterface(taskSpec.vo)
            # register datasets
            if datasetToRegister != [] or taskSpec.prodSourceLabel in ["user"]:
                # prod vs anal
                userSetup = False
                if taskSpec.prodSourceLabel in ["user"]:
                    userSetup = True
                    # collect datasetID to register datasets/containers just in case
                    for tmpPandaJob in pandaJobs:
                        if not tmpPandaJob.produceUnMerge():
                            for tmpFileSpec in tmpPandaJob.Files:
                                if tmpFileSpec.type in ["output", "log"]:
                                    if tmpFileSpec.datasetID not in datasetToRegister:
                                        datasetToRegister.append(tmpFileSpec.datasetID)
                tmpLog.info(f"datasetToRegister={str(datasetToRegister)}")
                # get site mapper
                siteMapper = self.taskBufferIF.get_site_mapper()

                # loop over all datasets
                avDatasetList = []
                cnDatasetMap = {}
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
                    # DDM backend
                    ddmBackEnd = taskSpec.getDdmBackEnd()
                    tmpLog.info(f"checking {datasetSpec.datasetName}")
                    # secondary nucleus
                    nucleusSpec = siteMapper.getNucleus(taskSpec.nucleus)
                    secNucleusSpecBase = None
                    if nucleusSpec and not taskSpec.is_intermediate_task():
                        secondaryNucleus = nucleusSpec.get_secondary_nucleus()
                        if secondaryNucleus:
                            secNucleusSpecBase = siteMapper.getNucleus(secondaryNucleus)
                    # check if dataset and container are available in DDM
                    for targetName in [datasetSpec.datasetName, datasetSpec.containerName]:
                        if targetName is None:
                            continue
                        if targetName not in avDatasetList:
                            # set lifetime
                            if targetName.startswith("panda"):
                                if datasetSpec.type == "trn_log" and taskSpec.prodSourceLabel == "managed":
                                    lifetime = 365
                                else:
                                    lifetime = 14
                            else:
                                lifetime = None
                            # check dataset/container in DDM
                            tmpList = ddmIF.listDatasets(targetName)
                            if tmpList == []:
                                # get location
                                location = None
                                locForRule = None
                                if targetName == datasetSpec.datasetName:
                                    # dataset
                                    if datasetSpec.site in ["", None]:
                                        if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None:
                                            locForRule = datasetSpec.destination
                                        elif DataServiceUtils.getDestinationSE(datasetSpec.storageToken) is not None:
                                            location = DataServiceUtils.getDestinationSE(datasetSpec.storageToken)
                                    else:
                                        tmpLog.info(f"site={datasetSpec.site} token={datasetSpec.storageToken}")
                                        location = siteMapper.getDdmEndpoint(
                                            datasetSpec.site,
                                            datasetSpec.storageToken,
                                            taskSpec.prodSourceLabel,
                                            JobUtils.translate_tasktype_to_jobtype(taskSpec.taskType),
                                        )
                                if locForRule is None:
                                    locForRule = location
                                # set metadata
                                if taskSpec.prodSourceLabel in ["managed", "test"] and targetName == datasetSpec.datasetName:
                                    metaData = {}
                                    metaData["task_id"] = taskSpec.jediTaskID
                                    if taskSpec.campaign not in [None, ""]:
                                        metaData["campaign"] = taskSpec.campaign
                                    if datasetSpec.getTransient() is not None:
                                        metaData["transient"] = datasetSpec.getTransient()
                                else:
                                    metaData = None
                                # use secondary nucleus only for production output
                                if (
                                    secNucleusSpecBase
                                    and taskSpec.prodSourceLabel in ["managed", "test"]
                                    and targetName == datasetSpec.datasetName
                                    and datasetSpec.type == "output"
                                ):
                                    secNucleusSpec = secNucleusSpecBase
                                else:
                                    secNucleusSpec = None
                                # register dataset/container
                                tmpLog.info(f"registering {targetName} with location={location} backend={ddmBackEnd} lifetime={lifetime} meta={str(metaData)}")
                                tmpStat = ddmIF.registerNewDataset(targetName, backEnd=ddmBackEnd, location=location, lifetime=lifetime, metaData=metaData)
                                if not tmpStat:
                                    tmpLog.error(f"failed to register {targetName}")
                                    return retFatal
                                # procedures for user
                                if userSetup or DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None or secNucleusSpec:
                                    # register location
                                    tmpToRegister = False
                                    if userSetup and targetName == datasetSpec.datasetName and datasetSpec.site not in ["", None]:
                                        if taskSpec.workingGroup:
                                            userName = taskSpec.workingGroup
                                        else:
                                            userName = taskSpec.userName
                                        grouping = None
                                        tmpToRegister = True
                                    elif DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None:
                                        userName = None
                                        grouping = "NONE"
                                        tmpToRegister = True
                                    elif secNucleusSpec:
                                        userName = None
                                        grouping = None
                                        tmpToRegister = True
                                        locForRule = siteMapper.getDdmEndpoint(
                                            secNucleusSpec.getOnePandaSite(),
                                            datasetSpec.storageToken,
                                            taskSpec.prodSourceLabel,
                                            JobUtils.translate_tasktype_to_jobtype(taskSpec.taskType),
                                        )

                                    if tmpToRegister:
                                        activity = DataServiceUtils.getActivityForOut(taskSpec.prodSourceLabel)
                                        tmpLog.info(
                                            "registering location={} lifetime={} days activity={} grouping={} "
                                            "owner={}".format(locForRule, lifetime, activity, grouping, userName)
                                        )
                                        tmpStat = ddmIF.registerDatasetLocation(
                                            targetName, locForRule, owner=userName, lifetime=lifetime, backEnd=ddmBackEnd, activity=activity, grouping=grouping
                                        )
                                        if not tmpStat:
                                            tmpLog.error(f"failed to register location {locForRule} for {targetName}")
                                            return retFatal
                                        # double copy
                                        if userSetup and datasetSpec.type == "output":
                                            if datasetSpec.destination != datasetSpec.site:
                                                tmpLog.info(f"skip making double copy as destination={datasetSpec.destination} is not site={datasetSpec.site}")
                                            else:
                                                second_copy = True
                                                try:
                                                    if taskSpec.site:
                                                        panda_site = siteMapper.getSite(taskSpec.site)
                                                        if panda_site.catchall and "skip_2nd_copy" in panda_site.catchall:
                                                            tmpLog.info(f"skip making double copy as specified in {panda_site} catchall")
                                                            second_copy = False
                                                except Exception:
                                                    second_copy = True

                                                if second_copy:
                                                    locForDouble = f"(type=SCRATCHDISK)\\notforextracopy=True\\{locForRule}"
                                                    tmpMsg = "registering double copy "
                                                    tmpMsg += f'location="{locForDouble}" lifetime={lifetime}days activity={activity} for dataset={targetName}'
                                                    tmpLog.info(tmpMsg)
                                                    tmpStat = ddmIF.registerDatasetLocation(
                                                        targetName,
                                                        locForDouble,
                                                        copies=1,
                                                        owner=userName,
                                                        lifetime=lifetime,
                                                        activity=activity,
                                                        grouping="NONE",
                                                        weight="freespace",
                                                        ignore_availability=False,
                                                    )
                                                    if not tmpStat:
                                                        tmpLog.error(f"failed to register double copylocation {locForDouble} for {targetName}")
                                                        return retFatal
                                avDatasetList.append(targetName)
                            else:
                                tmpLog.info(f"{targetName} already registered")
                    # check if dataset is in the container
                    if datasetSpec.containerName is not None and datasetSpec.containerName != datasetSpec.datasetName:
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
            # register ES datasets
            if taskSpec.registerEsFiles():
                targetName = EventServiceUtils.getEsDatasetName(taskSpec.jediTaskID)
                location = None
                metaData = {}
                metaData["task_id"] = taskSpec.jediTaskID
                metaData["hidden"] = True
                tmpLog.info(f"registering ES dataset {targetName} with location={location} meta={str(metaData)}")
                tmpStat = ddmIF.registerNewDataset(targetName, location=location, metaData=metaData, resurrect=True)
                if not tmpStat:
                    tmpLog.error(f"failed to register ES dataset {targetName}")
                    return retFatal
                # register rule
                location = "type=DATADISK"
                activity = DataServiceUtils.getActivityForOut(taskSpec.prodSourceLabel)
                grouping = "NONE"
                tmpLog.info(f"registering location={location} activity={activity} grouping={grouping}")
                tmpStat = ddmIF.registerDatasetLocation(targetName, location, activity=activity, grouping=grouping)
                if not tmpStat:
                    tmpLog.error(f"failed to register location {location} with {activity} for {targetName}")
                    return retFatal
            # open datasets
            if taskSpec.prodSourceLabel in ["managed", "test"]:
                # get the list of output/log datasets
                outDatasetList = []
                for tmpPandaJob in pandaJobs:
                    for tmpFileSpec in tmpPandaJob.Files:
                        if tmpFileSpec.type in ["output", "log"]:
                            if tmpFileSpec.destinationDBlock not in outDatasetList:
                                outDatasetList.append(tmpFileSpec.destinationDBlock)
                # open datasets
                for outDataset in outDatasetList:
                    tmpLog.info(f"open {outDataset}")
                    ddmIF.openDataset(outDataset)
                    # unset lifetime
                    ddmIF.setDatasetMetadata(outDataset, "lifetime", None)
            # return
            tmpLog.info("done")
            return retOK
        except Exception as e:
            tmpLog.error(f"doSetup failed with {str(e)}")
            tb = traceback.format_exc()
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            tmpLog.error(tb)
            return retFatal
