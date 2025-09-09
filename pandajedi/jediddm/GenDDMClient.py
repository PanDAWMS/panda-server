from pandacommon.pandalogger.PandaLogger import PandaLogger

from .DDMClientBase import DDMClientBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class to access to general DDM
class GenDDMClient(DDMClientBase):
    # constructor
    def __init__(self, con):
        # initialize base class
        DDMClientBase.__init__(self, con)

    # get dataset metadata
    def getDatasetMetaData(self, datasetName, ignore_missing=False):
        return self.SC_SUCCEEDED, {"state": "closed"}
