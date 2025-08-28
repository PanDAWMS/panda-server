import os
import socket

from pandajedi.jedicore import Interaction


# base class for watchdog
class WatchDogBase(object):
    """
    Base class for watchdog
    """

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        self.taskBufferIF = taskBufferIF
        self.ddmIF = ddmIF
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        self.vo = None
        self.refresh()

    def get_process_lock(self, component, timeLimit=5, **kwargs):
        """
        Shortcut of get process lock for watchdog action methods

        Args:
        component (str): spec of the request
        timeLimit (int): lifetime of the lock in minutes
        **kwargs: other arguments for taskBufferIF.lockProcess_JEDI

        Returns:
            bool : True if got lock, False otherwise
        """
        return self.taskBufferIF.lockProcess_JEDI(
            vo=self.vo,
            prodSourceLabel=kwargs.get("prodSourceLabel", "default"),
            cloud=kwargs.get("cloud", None),
            workqueue_id=kwargs.get("workqueue_id", None),
            resource_name=kwargs.get("resource_name", None),
            component=component,
            pid=self.pid,
            timeLimit=timeLimit,
        )

    # refresh
    def refresh(self):
        self.siteMapper = self.taskBufferIF.get_site_mapper()

    # pre-action
    def pre_action(self, tmpLog, vo, prodSourceLabel, pid, *args, **kwargs):
        pass


Interaction.installSC(WatchDogBase)
