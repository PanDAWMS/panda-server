import random
import sys
import time

from pandajedi.jedicore import Interaction
from pandajedi.jedicore.ThreadUtils import ZombieCleaner


class JediKnight(Interaction.CommandReceiveInterface):
    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, logger, **kwargs):
        Interaction.CommandReceiveInterface.__init__(self, commuChannel)
        self.taskBufferIF = taskBufferIF
        self.ddmIF = ddmIF
        self.logger = logger
        # intra-node message broker proxies
        self.mb_proxy_dict = kwargs.get("mb_proxy_dict")
        # start zombie cleaner
        ZombieCleaner().start()

    # start communication channel in a thread
    def start(self):
        # start communication channel
        import threading

        thr = threading.Thread(target=self.startImpl)
        thr.start()

    # implementation of start()
    def startImpl(self):
        try:
            Interaction.CommandReceiveInterface.start(self)
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            self.logger.error(f"crashed in JediKnight.startImpl() with {errtype.__name__} {errvalue}")

    # parse init params
    def parseInit(self, par):
        if isinstance(par, list):
            return par
        try:
            return par.split("|")
        except Exception:
            return [par]

    # sleep to avoid synchronization of loop
    def randomSleep(self, min_val=0, default_max_val=30, max_val=None):
        if max_val is None:
            max_val = default_max_val
        max_val = min(max_val, default_max_val)
        time.sleep(random.randint(min_val, max_val))


# install SCs
Interaction.installSC(JediKnight)
