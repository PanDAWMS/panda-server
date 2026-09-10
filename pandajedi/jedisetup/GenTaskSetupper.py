from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.spec_column import Null

from .TaskSetupperBase import TaskSetupperBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# task setup for general purpose
class GenTaskSetupper(TaskSetupperBase):
    # constructor
    def __init__(self, taskBufferIF: JediTaskBufferInterface, ddmIF: DDMInterface) -> None:
        TaskSetupperBase.__init__(self, taskBufferIF, ddmIF)

    # main to setup task
    def doSetup(self, taskSpec: JediTaskSpec, datasetToRegister: list[int | Null], pandaJobs: list[JobSpec]) -> Interaction.StatusCode:
        return self.SC_SUCCEEDED
