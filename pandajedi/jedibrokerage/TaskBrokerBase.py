from typing import TYPE_CHECKING, Any

from pandajedi.jedicore import Interaction

if TYPE_CHECKING:
    # Importing these for real makes this module read a configuration file at import time,
    # and it has no other reason to need one. Annotations are evaluated at runtime in this
    # tree, so the uses below are quoted.
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jediddm.DDMInterface import DDMInterface
    from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
    from pandaserver.taskbuffer.WorkQueue import WorkQueue


# base class for task brokerge
class TaskBrokerBase(object):
    # installed on this class by Interaction.installSC() at the bottom of this module
    SC_SUCCEEDED: Interaction.StatusCode
    SC_FAILED: Interaction.StatusCode
    SC_FATAL: Interaction.StatusCode
    SC_WAITING: Interaction.StatusCode

    def __init__(self, taskBufferIF: "JediTaskBufferInterface", ddmIF: "DDMInterface") -> None:
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.refresh()

    def refresh(self) -> None:
        self.siteMapper = self.taskBufferIF.get_site_mapper()

    # check tasks. Every plugin overrides this; FactoryBase[TaskBrokerBase] lets the knight call it
    def doCheck(self, taskSpecList: "list[JediTaskSpec]") -> tuple[Interaction.StatusCode, dict[str, Any]]:
        raise NotImplementedError

    # task brokerage. Every plugin overrides this; FactoryBase[TaskBrokerBase] lets the knight call it
    def doBrokerage(
        self, inputList: list[Any], vo: str | None, prodSourceLabel: str | None, workQueue: "WorkQueue", resource_name: str
    ) -> Interaction.StatusCode:
        raise NotImplementedError


Interaction.installSC(TaskBrokerBase)
