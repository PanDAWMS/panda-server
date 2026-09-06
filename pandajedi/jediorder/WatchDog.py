import datetime
import os
import socket
import time
from multiprocessing.connection import Connection
from typing import TYPE_CHECKING

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore import Interaction
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jedicore.MsgWrapper import MsgWrapper

from .JediKnight import JediKnight

if TYPE_CHECKING:
    from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface
    from pandajedi.jediddm.DDMInterface import DDMInterface

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# worker class for watchdog
class WatchDog(JediKnight, FactoryBase):
    # constructor
    def __init__(
        self,
        commuChannel: Connection,
        taskBufferIF: "JediTaskBufferInterface",
        ddmIF: "DDMInterface",
        vos: str | list[str] | None,
        prodSourceLabels: str | list[str] | None,
        subStr: str | None,
        period: int | None,
    ) -> None:
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.subStr = subStr
        self.period = period
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)
        FactoryBase.__init__(self, self.vos, self.prodSourceLabels, logger, jedi_config.watchdog.modConfig)

    # main
    def start(self) -> None:
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
                        else:
                            tmpLog.warning(f"no plugin for vo={vo} label={prodSourceLabel} subType={self.subStr}")
                tmpLog.info("done")
            except Exception as e:
                tmpLog.error(f"failed in {self.__class__.__name__}.start() with {type(e).__name__} {e}")
            # sleep if needed
            loopCycle = jedi_config.watchdog.loopCycle if self.period is None else self.period
            timeDelta = naive_utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)
            # randomize cycle
            self.randomSleep(max_val=loopCycle)


# launch


def launcher(
    commuChannel: Connection,
    taskBufferIF: "JediTaskBufferInterface",
    ddmIF: "DDMInterface",
    vos: str | list[str] | None = None,
    prodSourceLabels: str | list[str] | None = None,
    subStr: str | None = None,
    period: int | None = None,
) -> None:
    p = WatchDog(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels, subStr, period)
    p.start()
