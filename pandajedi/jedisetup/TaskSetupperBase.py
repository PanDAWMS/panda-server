from typing import TYPE_CHECKING

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface

if TYPE_CHECKING:
    from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
    from pandaserver.taskbuffer.JobSpec import JobSpec
    from pandaserver.taskbuffer.spec_column import Null


# base class for task setup
class TaskSetupperBase(object):
    # installed on this class by Interaction.installSC() at the bottom of this module
    SC_SUCCEEDED: Interaction.StatusCode
    SC_FAILED: Interaction.StatusCode
    SC_FATAL: Interaction.StatusCode
    SC_WAITING: Interaction.StatusCode

    def __init__(self, taskBufferIF: JediTaskBufferInterface, ddmIF: DDMInterface) -> None:
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.refresh()

    def refresh(self) -> None:
        self.siteMapper = self.taskBufferIF.get_site_mapper()

    # set up a task. Every plugin overrides this; FactoryBase[TaskSetupperBase] lets the knight call it
    def doSetup(self, taskSpec: "JediTaskSpec", datasetToRegister: "list[int | Null]", pandaJobs: "list[JobSpec]") -> Interaction.StatusCode:
        raise NotImplementedError


Interaction.installSC(TaskSetupperBase)
