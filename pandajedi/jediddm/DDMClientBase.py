from multiprocessing.connection import Connection

from pandajedi.jedicore.Interaction import CommandReceiveInterface, StatusCode


# base class to interact with DDM
class DDMClientBase(CommandReceiveInterface):
    # constructor
    def __init__(self, con: Connection) -> None:
        CommandReceiveInterface.__init__(self, con)

    # list dataset/container
    def listDatasets(self, datasetName: str, ignorePandaDS: bool = True) -> tuple[StatusCode, list[str]]:
        return self.SC_SUCCEEDED, [datasetName]

    # check endpoint
    def check_endpoint(self, rse: str) -> tuple[StatusCode, tuple[bool | None, str | None]]:
        return self.SC_SUCCEEDED, (True, None)
