import datetime
import os
import socket
import sys
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock, ThreadPool, WorkerThread

from .WatchDogBase import WatchDogBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# data locality updater for ATLAS
class AtlasDataLocalityUpdaterWatchDog(WatchDogBase):
    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}-dog"
        self.vo = "atlas"
        self.ddmIF = ddmIF.getInterface(self.vo)

    # get list-with-lock of datasets to update
    def get_datasets_list(self):
        datasets_list = self.taskBufferIF.get_tasks_inputdatasets_JEDI(self.vo)
        datasets_list = ListWithLock(datasets_list)
        # return
        return datasets_list

    # update data locality records to DB table
    def doUpdateDataLocality(self):
        tmpLog = MsgWrapper(logger, " #ATM #KV doUpdateDataLocality")
        tmpLog.debug("start")
        try:
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(
                vo=self.vo,
                prodSourceLabel="default",
                cloud=None,
                workqueue_id=None,
                resource_name=None,
                component="AtlasDataLocaUpdDog.doUpdateDataLoca",
                pid=self.pid,
                timeLimit=240,
            )
            if not got_lock:
                tmpLog.debug("locked by another process. Skipped")
                return
            tmpLog.debug("got lock")
            # get list of datasets
            datasets_list = self.get_datasets_list()
            tmpLog.debug(f"got {len(datasets_list)} datasets to update")
            # make thread pool
            thread_pool = ThreadPool()
            # make workers
            n_workers = 8
            for _ in range(n_workers):
                thr = DataLocalityUpdaterThread(
                    taskDsList=datasets_list, threadPool=thread_pool, taskbufferIF=self.taskBufferIF, ddmIF=self.ddmIF, pid=self.pid, loggerObj=tmpLog
                )
                thr.start()
            tmpLog.debug(f"started {n_workers} updater workers")
            # join
            thread_pool.join()
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    # clean up old data locality records in DB table
    def doCleanDataLocality(self):
        tmpLog = MsgWrapper(logger, " #ATM #KV doCleanDataLocality")
        tmpLog.debug("start")
        try:
            # lock
            got_lock = self.taskBufferIF.lockProcess_JEDI(
                vo=self.vo,
                prodSourceLabel="default",
                cloud=None,
                workqueue_id=None,
                resource_name=None,
                component="AtlasDataLocaUpdDog.doCleanDataLoca",
                pid=self.pid,
                timeLimit=1440,
            )
            if not got_lock:
                tmpLog.debug("locked by another process. Skipped")
                return
            tmpLog.debug("got lock")
            # lifetime of records
            record_lifetime_hours = 24
            # run
            now_timestamp = naive_utcnow()
            before_timestamp = now_timestamp - datetime.timedelta(hours=record_lifetime_hours)
            n_rows = self.taskBufferIF.deleteOutdatedDatasetLocality_JEDI(before_timestamp)
            tmpLog.info(f"cleaned up {n_rows} records")
            # done
            tmpLog.debug("done")
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            tmpLog.error(f"failed with {errtype} {errvalue} {traceback.format_exc()}")

    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug("start")
            # clean up data locality
            self.doCleanDataLocality()
            # update data locality
            self.doUpdateDataLocality()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            origTmpLog.error(f"failed with {errtype} {errvalue}")
        # return
        origTmpLog.debug("done")
        return self.SC_SUCCEEDED


# thread for data locality update
class DataLocalityUpdaterThread(WorkerThread):
    # constructor
    def __init__(self, taskDsList, threadPool, taskbufferIF, ddmIF, pid, loggerObj):
        # initialize worker with no semaphore
        WorkerThread.__init__(self, None, threadPool, loggerObj)
        # attributes
        self.taskDsList = taskDsList
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.msgType = "datalocalityupdate"
        self.pid = pid
        self.logger = loggerObj

    # main
    def runImpl(self):
        # initialize
        n_updated_ds = 0
        n_skipped_ds = 0
        n_updated_replicas = 0
        n_skipped_replicas = 0
        while True:
            try:
                # get part of datasets
                nDatasets = 5
                taskDsList = self.taskDsList.get(nDatasets)
                if len(taskDsList) == 0:
                    # no more datasets, quit
                    self.logger.debug(
                        f"{self.name} terminating since no more items; updated {n_updated_ds} datasets and {n_updated_replicas} replicas; skipped {n_skipped_ds} datasets and {n_skipped_replicas} replicas"
                    )
                    return
                # loop over these datasets
                for item in taskDsList:
                    if item is None:
                        n_skipped_ds += 1
                        continue
                    jediTaskID, datasetID, datasetName = item
                    _, task_spec = self.taskBufferIF.getTaskWithID_JEDI(jediTaskID)
                    dataset_replicas_map = self.ddmIF.listDatasetReplicas(datasetName)
                    is_distributed_ds = self.ddmIF.isDistributedDataset(datasetName)
                    for tmpRSE, tmpList in dataset_replicas_map.items():
                        # check data locality unless input is distributed or uses data carousel
                        if not is_distributed_ds and not task_spec.inputPreStaging():
                            tmpStatistics = tmpList[-1]
                            # skip unknown and incomplete
                            if tmpStatistics["found"] is None or tmpStatistics["found"] != tmpStatistics["total"]:
                                n_skipped_replicas += 1
                                continue
                        # update dataset locality table
                        self.taskBufferIF.updateDatasetLocality_JEDI(jedi_taskid=jediTaskID, datasetid=datasetID, rse=tmpRSE)
                        n_updated_replicas += 1
                    n_updated_ds += 1
            except Exception as e:
                self.logger.error(f"{self.__class__.__name__} failed in runImpl() with {str(e)}: {traceback.format_exc()}")
                return
