"""
Post-processing worker for JEDI tasks.

PostProcessor is a long-running daemon that periodically picks up tasks ready
to be finished and dispatches them to a pool of PostProcessorThread workers.
Each worker calls the VO/label-specific post-processor implementation
(doPostProcess) and, if successful, the final-procedure hook (doFinalProcedure).
"""

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


class PostProcessor(JediKnight, FactoryBase):
    """
    Daemon that drives post-processing for finished JEDI tasks.

    Inherits scheduling and communication from JediKnight and VO/label-specific
    implementation instantiation from FactoryBase.  On each loop iteration it
    calls prepareTasksToBeFinished_JEDI to lock eligible tasks, then hands them
    off to a pool of PostProcessorThread workers.
    """

    def __init__(self, commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels):
        self.vos = self.parseInit(vos)
        self.prodSourceLabels = self.parseInit(prodSourceLabels)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-post"
        JediKnight.__init__(self, commuChannel, taskBufferIF, ddmIF, logger)
        FactoryBase.__init__(self, self.vos, self.prodSourceLabels, logger, jedi_config.postprocessor.modConfig)

    def start(self):
        """Run the main post-processing loop, cycling every 60 seconds."""
        JediKnight.start(self)
        FactoryBase.initializeMods(self, self.taskBufferIF, self.ddmIF)

        while True:
            start_time = naive_utcnow()
            try:
                tmp_log = MsgWrapper(logger)
                tmp_log.info("start")

                for vo in self.vos:
                    for prod_source_label in self.prodSourceLabels:
                        tmp_log.info(f"preparing tasks to be finished for vo={vo} label={prod_source_label}")
                        target_tasks = self.taskBufferIF.prepareTasksToBeFinished_JEDI(vo, prod_source_label, jedi_config.postprocessor.nTasks, pid=self.pid)
                        if target_tasks is None:
                            tmp_log.error("failed to prepare tasks")

                        tmp_log.info("getting tasks to be finished")
                        task_list = self.taskBufferIF.getTasksToBeFinished_JEDI(
                            vo, prod_source_label, self.pid, jedi_config.postprocessor.nTasks, target_tasks=target_tasks
                        )
                        if task_list is None:
                            tmp_log.error("failed to get tasks to be finished")
                            continue

                        tmp_log.info(f"got {len(task_list)} tasks")
                        locked_list = ListWithLock(task_list)
                        thread_pool = ThreadPool()
                        for _ in range(jedi_config.postprocessor.nWorkers):
                            thr = PostProcessorThread(locked_list, thread_pool, self.taskBufferIF, self.ddmIF, self)
                            thr.start()
                        thread_pool.join()

                tmp_log.info("done")
            except Exception:
                err_type, err_value = sys.exc_info()[:2]
                tmp_log.error(f"failed in {self.__class__.__name__}.start() with {err_type.__name__} {err_value}")

            # sleep for the remainder of the 60-second cycle
            loop_cycle = 60
            elapsed = naive_utcnow() - start_time
            sleep_period = loop_cycle - elapsed.seconds
            if sleep_period > 0:
                time.sleep(sleep_period)


class PostProcessorThread(WorkerThread):
    """
    Worker thread that post-processes a batch of JEDI tasks.

    Instantiated by PostProcessor.start() for each worker slot.  Pulls tasks
    from a shared locked list and calls post_process_tasks() until the list is
    exhausted.
    """

    def __init__(self, taskList, threadPool, taskbufferIF, ddmIF, implFactory):
        WorkerThread.__init__(self, None, threadPool, logger)
        self.taskList = taskList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.implFactory = implFactory

    def post_process_tasks(self, task_list):
        """
        Run post-processing and final-procedure for each task in task_list.

        Outcome per task:
        - SC_FATAL or SC_FAILED on a terminal task status → mark as broken.
        - SC_FAILED on a non-terminal status → record transient error and skip
          the final procedure.
        - SC_SUCCEEDED → call doFinalProcedure.
        """
        for task_spec in task_list:
            tmp_log = MsgWrapper(self.logger, f"<jediTaskID={task_spec.jediTaskID}>")
            tmp_log.info("start")
            tmp_stat = Interaction.SC_SUCCEEDED

            # instantiate the VO/label-specific post-processor
            impl = self.implFactory.instantiateImpl(task_spec.vo, task_spec.prodSourceLabel, None, self.taskBufferIF, self.ddmIF)
            if impl is None:
                tmp_log.error(f"post-processor is undefined for vo={task_spec.vo} sourceLabel={task_spec.prodSourceLabel}")
                tmp_stat = Interaction.SC_FATAL

            # run post-processing
            if tmp_stat == Interaction.SC_SUCCEEDED:
                tmp_log.info(f"post-process with {impl.__class__.__name__}")
                try:
                    tmp_stat = impl.doPostProcess(task_spec, tmp_log)
                except Exception as e:
                    tmp_log.error(f"post-process failed with {str(e)}")
                    tmp_stat = Interaction.SC_FATAL

            # handle permanent failure
            if tmp_stat == Interaction.SC_FATAL or (tmp_stat == Interaction.SC_FAILED and task_spec.status in ("toabort", "tobroken")):
                err_str = "post-process permanently failed"
                tmp_log.error(err_str)
                task_spec.status = "broken"
                task_spec.setErrDiag(err_str)
                task_spec.lockedBy = None
                self.taskBufferIF.updateTask_JEDI(task_spec, {"jediTaskID": task_spec.jediTaskID})

            # handle transient failure — skip final procedure
            elif tmp_stat == Interaction.SC_FAILED:
                err_str = "post-processing temporarily failed"
                task_spec.setErrDiag(err_str, True)
                self.taskBufferIF.updateTask_JEDI(task_spec, {"jediTaskID": task_spec.jediTaskID})
                tmp_log.info(f"set task_status={task_spec.status} since {task_spec.errorDialog}")
                tmp_log.info("done")
                continue

            # run final procedure depending on prodsourcelabel (e.g. email notifications, manage output datasets, etc.)
            try:
                impl.doFinalProcedure(task_spec, tmp_log)
            except Exception as e:
                tmp_log.error(f"final procedure failed with {str(e)}")

            tmp_log.info("done")

    def runImpl(self):
        """Pull batches of tasks from the shared list and post-process them."""
        while True:
            try:
                task_list = self.taskList.get(10)
                if not task_list:
                    self.logger.debug(f"{self.__class__.__name__} terminating since no more items")
                    return
                self.post_process_tasks(task_list)
            except Exception:
                err_type, err_value = sys.exc_info()[:2]
                logger.error(f"{self.__class__.__name__} failed in runImpl() with {err_type.__name__}:{err_value}")


def launcher(commuChannel, taskBufferIF, ddmIF, vos=None, prodSourceLabels=None):
    """Entry point used by the JEDI daemon infrastructure to start the PostProcessor."""
    p = PostProcessor(commuChannel, taskBufferIF, ddmIF, vos, prodSourceLabels)
    p.start()
