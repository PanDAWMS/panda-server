from multiprocessing.connection import Connection
from typing import Any

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore.Interaction import StatusCode

from .DDMClientBase import DDMClientBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class to access to general DDM
class GenDDMClient(DDMClientBase):
    # constructor
    def __init__(self, con: Connection) -> None:
        # initialize base class
        DDMClientBase.__init__(self, con)

    # get dataset metadata
    def getDatasetMetaData(self, datasetName: str, ignore_missing: bool = False) -> tuple[StatusCode, dict[str, Any]]:
        return self.SC_SUCCEEDED, {"state": "closed"}
