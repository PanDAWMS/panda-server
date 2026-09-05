from pandajedi.jedicore import Interaction
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
from pandajedi.jediddm.DDMInterface import DDMInterface


# base class for task generator
class TaskGeneratorBase(object):
    # installed on this class by Interaction.installSC() at the bottom of this module
    SC_SUCCEEDED: Interaction.StatusCode
    SC_FAILED: Interaction.StatusCode
    SC_FATAL: Interaction.StatusCode

    def __init__(self, taskBufferIF: JediTaskBufferInterface, ddmIF: DDMInterface) -> None:
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.refresh()

    def refresh(self) -> None:
        self.siteMapper = self.taskBufferIF.get_site_mapper()


Interaction.installSC(TaskGeneratorBase)
