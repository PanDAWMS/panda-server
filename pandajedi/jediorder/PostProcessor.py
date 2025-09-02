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
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread

from .JediKnight import JediKnight

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# worker class to do post-processing
class PostProcessor(JediKnight, FactoryBase):
    # constructor
    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-post"
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)
        FactoryBase.__init__(self, self.vos, self.prodSourceLabels, logger, jedi_config.postprocessor.modConfig)

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
                        # prepare tasks to be finished
                        tmpLog.info(f"preparing tasks to be finished for vo={vo} label={prodSourceLabel}")
                        tmp_ret_list = self.taskBufferIF.prepareTasksToBeFinished_JEDI(vo, prodSourceLabel, jedi_config.postprocessor.nTasks, pid=self.pid)
                        if tmp_ret_list is None:
                            # failed
                            tmpLog.error("failed to prepare tasks")
                        # get tasks to be finished
                        tmpLog.info("getting tasks to be finished")
                        tmpList = self.taskBufferIF.getTasksToBeFinished_JEDI(
                            vo, prodSourceLabel, self.pid, jedi_config.postprocessor.nTasks, target_tasks=tmp_ret_list
                        )
                        if tmpList is None:
                            # failed
                            tmpLog.error("failed to get tasks to be finished")
                        else:
                            tmpLog.info(f"got {len(tmpList)} tasks")
                            # put to a locked list
                            taskList = ListWithLock(tmpList)
                            # make thread pool
                            threadPool = ThreadPool()
                            # make workers
                            nWorker = jedi_config.postprocessor.nWorkers
                            for iWorker in range(nWorker):
                                thr = PostProcessorThread(taskList, threadPool, self.taskBufferIF, self.ddmIF, self)
                                thr.start()
                            # join
                            threadPool.join()
                tmpLog.info("done")
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                tmpLog.error(f"failed in {self.__class__.__name__}.start() with {errtype.__name__} {errvalue}")
            # sleep if needed
            loopCycle = 60
            timeDelta = naive_utcnow() - startTime
            sleepPeriod = loopCycle - timeDelta.seconds
            if sleepPeriod > 0:
                time.sleep(sleepPeriod)


# thread for real worker
class PostProcessorThread(WorkerThread):
    # constructor
    def __init__(self, taskList, threadPool, taskbufferIF, ddmIF, implFactory):
        # initialize woker with no semaphore
        WorkerThread.__init__(self, None, threadPool, logger)
        # attributres
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.implFactory = implFactory

    # post process tasks
    def post_process_tasks(self, task_list):
        for taskSpec in task_list:
            # make logger
            tmpLog = MsgWrapper(self.logger, f"<jediTaskID={taskSpec.jediTaskID}>")
            tmpLog.info("start")
            tmpStat = Interaction.SC_SUCCEEDED
            # get impl
            impl = self.implFactory.instantiateImpl(taskSpec.vo, taskSpec.prodSourceLabel, None, self.taskBufferIF, self.ddmIF)
            if impl is None:
                # post processor is undefined
                tmpLog.error(f"post-processor is undefined for vo={taskSpec.vo} sourceLabel={taskSpec.prodSourceLabel}")
                tmpStat = Interaction.SC_FATAL
            # execute
            if tmpStat == Interaction.SC_SUCCEEDED:
                tmpLog.info(f"post-process with {impl.__class__.__name__}")
                try:
                    tmpStat = impl.doPostProcess(taskSpec, tmpLog)
                except Exception as e:
                    tmpLog.error(f"post-process failed with {str(e)}")
                    tmpStat = Interaction.SC_FATAL
            # done
            if tmpStat == Interaction.SC_FATAL or (tmpStat == Interaction.SC_FAILED and taskSpec.status in ["toabort", "tobroken"]):
                # task is broken
                tmpErrStr = "post-process permanently failed"
                tmpLog.error(tmpErrStr)
                taskSpec.status = "broken"
                taskSpec.setErrDiag(tmpErrStr)
                taskSpec.lockedBy = None
                self.taskBufferIF.updateTask_JEDI(taskSpec, {"jediTaskID": taskSpec.jediTaskID})
            elif tmpStat == Interaction.SC_FAILED:
                tmpErrStr = "post-processing temporarily failed"
                taskSpec.setErrDiag(tmpErrStr, True)
                self.taskBufferIF.updateTask_JEDI(taskSpec, {"jediTaskID": taskSpec.jediTaskID})
                tmpLog.info(f"set task_status={taskSpec.status} since {taskSpec.errorDialog}")
                tmpLog.info("done")
                continue
            # final procedure
            try:
                impl.doFinalProcedure(taskSpec, tmpLog)
            except Exception as e:
                tmpLog.error(f"final procedure failed with {str(e)}")
            # done
            tmpLog.info("done")

    # main
    def runImpl(self):
        while True:
            try:
                # get a part of list
                nTasks = 10
                taskList = self.taskList.get(nTasks)
                # no more datasets
                if len(taskList) == 0:
                    self.logger.debug(f"{self.__class__.__name__} terminating since no more items")
                    return
                # post process tasks
                self.post_process_tasks(taskList)
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                logger.error(f"{self.__class__.__name__} failed in runImpl() with {errtype.__name__}:{errvalue}")


# launch


def launcher(commuChannel, taskBufferIF, ddmIF, vos=None, prodSourceLabels=None):
    p = PostProcessor(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels)
    p.start()
