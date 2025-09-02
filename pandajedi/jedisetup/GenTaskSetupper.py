from pandacommon.pandalogger.PandaLogger import PandaLogger

from .TaskSetupperBase import TaskSetupperBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# task setup for general purpose
class GenTaskSetupper(TaskSetupperBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        TaskSetupperBase.__init__(self, taskBufferIF, ddmIF)

    # main to setup task
    def doSetup(self, taskSpec, datasetToRegister, pandaJobs):
        return self.SC_SUCCEEDED
