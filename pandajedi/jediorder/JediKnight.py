import logging
import random
import time
from multiprocessing.connection import Connection
from typing import TYPE_CHECKING, Any

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.ThreadUtils import ZombieCleaner

if TYPE_CHECKING:
    # Importing either of these for real makes this module read a configuration file at
    # import time, and it has no other reason to need one. Annotations are evaluated at
    # runtime in this tree, so the two uses below are quoted.
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jediddm.DDMInterface import DDMInterface


class JediKnight(Interaction.CommandReceiveInterface):
    # installed on this class by Interaction.installSC() at the bottom of this module
    SC_SUCCEEDED: Interaction.StatusCode
    SC_FAILED: Interaction.StatusCode
    SC_FATAL: Interaction.StatusCode

    # What this holds depends on the subclass. WatchDog, TaskBroker, TaskRefiner and
    # PostProcessor also inherit FactoryBase, whose __init__() assigns self.logger a
    # MsgWrapper and runs second in all four, so the assignment below is overwritten
    # there. The rest keep the plain logger. Both spell error() the same way, which is
    # all this class asks of it.
    logger: Any

    # constructor. commuChannel is None in the jeditest drivers, which build a knight to
    # call one of its methods directly and never reach start(), where the channel is used
    def __init__(
        self, commuChannel: Connection | None, taskBufferIF: "JediTaskBufferInterface", ddmIF: "DDMInterface", logger: logging.Logger, **kwargs: Any
    ) -> None:
        Interaction.CommandReceiveInterface.__init__(self, commuChannel)
        self.taskBufferIF = taskBufferIF
        self.ddmIF = ddmIF
        self.logger = logger
        # intra-node message broker proxies
        self.mb_proxy_dict = kwargs.get("mb_proxy_dict")
        # start zombie cleaner
        ZombieCleaner().start()

    # start communication channel in a thread
    def start(self) -> None:
        # start communication channel
        import threading

        thr = threading.Thread(target=self.startImpl)
        thr.start()

    # implementation of start()
    def startImpl(self) -> None:
        try:
            Interaction.CommandReceiveInterface.start(self)
        except Exception as e:
            self.logger.error(f"crashed in JediKnight.startImpl() with {type(e).__name__} {e}")

    # parse init params
    # the value comes from JediMaster.convParams(), which turns an empty configuration
    # field into None and a comma-separated one into a list, so this takes whatever that
    # produced -- a string, a list or None -- and always hands back a list
    def parseInit(self, par: Any) -> list[Any]:
        if isinstance(par, list):
            return par
        try:
            return par.split("|")
        except Exception:
            return [par]

    # sleep to avoid synchronization of loop
    def randomSleep(self, min_val: int = 0, default_max_val: int = 30, max_val: int | None = None) -> None:
        if max_val is None:
            max_val = default_max_val
        max_val = min(max_val, default_max_val)
        time.sleep(random.randint(min_val, max_val))


# install SCs
Interaction.installSC(JediKnight)
