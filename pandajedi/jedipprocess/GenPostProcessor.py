from .PostProcessorBase import PostProcessorBase


# post processor for general purpose
class GenPostProcessor(PostProcessorBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        PostProcessorBase.__init__(self, taskBufferIF, ddmIF)
        self.failOnZeroOkFile = True

    # main
    def doPostProcess(self, taskSpec, tmpLog):
        try:
            # get DDM I/F
            ddmIF = self.ddmIF.getInterface(taskSpec.vo, taskSpec.cloud)
            # skip if DDM I/F is inactive
            if not ddmIF:
                tmpLog.info("skip due to inactive DDM I/F")
            else:
                # loop over all datasets
                for datasetSpec in taskSpec.datasetSpecList:
                    # only output and log datasets
                    if datasetSpec.type not in ["log", "output"]:
                        continue
                    tmpLog.info(f"freezing datasetID={datasetSpec.datasetID}:Name={datasetSpec.datasetName}")
                    ddmIF.freezeDataset(datasetSpec.datasetName, ignoreUnknown=True)
        except Exception as e:
            tmpLog.warning(f"failed to freeze datasets with {str(e)}")
            return self.SC_FAILED
        try:
            self.doBasicPostProcess(taskSpec, tmpLog)
        except Exception as e:
            tmpLog.error(f"doBasicPostProcess failed with {str(e)}")
            return self.SC_FATAL
        return self.SC_SUCCEEDED
