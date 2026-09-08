import os
import socket
from typing import TYPE_CHECKING, Any

from pandajedi.jedicore import Interaction

if TYPE_CHECKING:
    # Importing any of these for real makes this module read a configuration file at import
    # time, and it has no other reason to need one. Annotations are evaluated at runtime in
    # this tree, so the uses below are quoted.
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jedicore.MsgWrapper import MsgWrapper
    from pandajedi.jediddm.DDMInterface import DDMInterface


# base class for watchdog
class WatchDogBase(object):
    """
    Base class for watchdog
    """

    # installed on this class by Interaction.installSC() at the bottom of this module
    SC_SUCCEEDED: Interaction.StatusCode
    SC_FAILED: Interaction.StatusCode
    SC_FATAL: Interaction.StatusCode

    # Installed by FactoryBase, which sets both right after the plugin is instantiated --
    # the only way a watchdog is created -- so both carry a real value by the time an
    # action runs. __init__ assigns None to vo solely to create the attribute.
    vo: str
    prodSourceLabel: str

    # constructor
    def __init__(self, taskBufferIF: "JediTaskBufferInterface", ddmIF: "DDMInterface") -> None:
        self.taskBufferIF = taskBufferIF
        self.ddmIF = ddmIF
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        self.vo = None  # type: ignore[assignment]
        self.refresh()

    def get_process_lock(self, component: str, timeLimit: float = 5, **kwargs: Any) -> bool:
        """
        Shortcut of get process lock for watchdog action methods

        Args:
        component (str): spec of the request
        timeLimit (int): lifetime of the lock in minutes
        **kwargs: other arguments for taskBufferIF.lockProcess_JEDI

        Returns:
            bool : True if got lock, False otherwise
        """
        # the interface forwards this over a pipe, so what comes back carries no type
        got_lock: bool = self.taskBufferIF.lockProcess_JEDI(
            vo=self.vo,
            prodSourceLabel=kwargs.get("prodSourceLabel", "default"),
            cloud=kwargs.get("cloud", None),
            workqueue_id=kwargs.get("workqueue_id", None),
            resource_name=kwargs.get("resource_name", None),
            component=component,
            pid=self.pid,
            timeLimit=timeLimit,
        )
        return got_lock

    # refresh
    def refresh(self) -> None:
        self.siteMapper = self.taskBufferIF.get_site_mapper()

    # pre-action
    # vo and prodSourceLabel come from watchdog.procConfig, where an empty field parses as
    # None and reaches the DB calls below as a NULL that simply matches no row
    def pre_action(self, tmpLog: "MsgWrapper", vo: str | None, prodSourceLabel: str | None, pid: str, *args: Any, **kwargs: Any) -> None:
        pass


Interaction.installSC(WatchDogBase)
