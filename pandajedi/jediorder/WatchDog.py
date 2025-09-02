import datetime
import os
import socket
import sys
import time

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jedicore.MsgWrapper import MsgWrapper

from .JediKnight import JediKnight

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# worker class for watchdog
class WatchDog(JediKnight, FactoryBase):
    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels, subStr, period):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.subStr = subStr
        self.period = period
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)
        FactoryBase.__init__(self, self.vos, self.prodSourceLabels, logger, jedi_config.watchdog.modConfig)

    # main
    def start(self):
        # start base classes
        JediKnight.start(self)
        FactoryBase.initializeMods(self, self.taskBufferIF, self.ddmIF)
        # go into main loop
        while True:
            startTime = naive_utcnow()
            try:
                # get logger
                tmpLog = MsgWrapper(logger)
                tmpLog.info("start")
                # loop over all vos
                for vo in self.vos:
                    # loop over all sourceLabels
                    for prodSourceLabel in self.prodSourceLabels:
                        # vo/prodSourceLabel specific action
                        impl = self.getImpl(vo, prodSourceLabel, subType=self.subStr)
                        if impl is not None:
                            plugin_name = impl.__class__.__name__
                            tmpLog.info(f"pre-action for vo={vo} label={prodSourceLabel} cls={plugin_name}")
                            impl.pre_action(tmpLog, vo, prodSourceLabel, self.pid)
                            tmpLog.info(f"do action for vo={vo} label={prodSourceLabel} cls={plugin_name}")
                            tmpStat = impl.doAction()
                            if tmpStat != Interaction.SC_SUCCEEDED:
                                tmpLog.error(f"failed to run special action for vo={vo} label={prodSourceLabel} cls={plugin_name}")
                            else:
                                tmpLog.info(f"done for vo={vo} label={prodSourceLabel} cls={plugin_name}")
                tmpLog.info("done")
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                tmpLog.error(f"failed in {self.__class__.__name__}.start() with {errtype.__name__} {errvalue}")
            # sleep if needed
            loopCycle = jedi_config.watchdog.loopCycle if self.period is None else self.period
            timeDelta = naive_utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)


# launch


def launcher(commuChannel, taskBufferIF, ddmIF, vos=None, prodSourceLabels=None, subStr=None, period=None):
    p = WatchDog(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels, subStr, period)
    p.start()
