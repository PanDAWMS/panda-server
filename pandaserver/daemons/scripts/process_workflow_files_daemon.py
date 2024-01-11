import datetime
import glob
import os.path
import sys
import threading
import time
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.taskbuffer.workflow_processor import WorkflowProcessor

# logger
_logger = PandaLogger().getLogger("process_workflow_files")


# main
def main(tbuf=None, **kwargs):
    _logger.debug("===================== start =====================")

    # overall timeout value
    overallTimeout = 300
    # prefix of the files
    if "target" in kwargs and kwargs["target"]:
        evpFilePatt = kwargs["target"]
    else:
        prefixEVP = "/workflow."
        # file pattern of evp files
        evpFilePatt = panda_config.cache_dir + "/" + prefixEVP + "*"

    from pandaserver.taskbuffer.TaskBuffer import taskBuffer

    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
    taskBuffer.init(
        panda_config.dbhost,
        panda_config.dbpasswd,
        nDBConnection=1,
        useTimeout=True,
        requester=requester_id,
    )

    test_mode = kwargs.get("test_mode", False)
    dump_workflow = kwargs.get("dump_workflow", False)

    # thread pool
    class ThreadPool:
        def __init__(self):
            self.lock = threading.Lock()
            self.list = []

        def add(self, obj):
            self.lock.acquire()
            self.list.append(obj)
            self.lock.release()

        def remove(self, obj):
            self.lock.acquire()
            self.list.remove(obj)
            self.lock.release()

        def join(self):
            self.lock.acquire()
            thrlist = tuple(self.list)
            self.lock.release()
            for thr in thrlist:
                thr.join()

    # thread
    class EvpThr(threading.Thread):
        def __init__(self, task_buffer, lock, pool, file_name, to_delete, get_log):
            threading.Thread.__init__(self)
            self.lock = lock
            self.pool = pool
            self.fileName = file_name
            self.to_delete = to_delete
            self.get_log = get_log
            self.pool.add(self)
            self.processor = WorkflowProcessor(task_buffer=task_buffer, log_stream=_logger)

        def run(self):
            self.lock.acquire()
            try:
                self.processor.process(
                    self.fileName,
                    self.to_delete,
                    test_mode,
                    self.get_log,
                    dump_workflow,
                )
            except Exception as e:
                _logger.error(f"{str(e)} {traceback.format_exc()}")
            self.pool.remove(self)
            self.lock.release()

    # get files
    timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    timeInt = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    fileList = sorted(glob.glob(evpFilePatt))

    # create thread pool and semaphore
    adderLock = threading.Semaphore(1)
    adderThreadPool = ThreadPool()

    # add
    while len(fileList) != 0:
        # time limit to aviod too many copyArchve running at the sametime
        if (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - timeNow) > datetime.timedelta(minutes=overallTimeout):
            _logger.debug("time over in main session")
            break
        # try to get Semaphore
        adderLock.acquire()
        # get fileList
        if (datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - timeInt) > datetime.timedelta(minutes=15):
            timeInt = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            # get file
            fileList = sorted(glob.glob(evpFilePatt))
        # choose a file
        fileName = fileList.pop(0)
        # release lock
        adderLock.release()
        if not os.path.exists(fileName):
            continue
        try:
            modTime = datetime.datetime(*(time.gmtime(os.path.getmtime(fileName))[:7]))
            to_go = True
            if test_mode:
                _logger.debug(f"Testing : {fileName}")
                to_delete = False
            elif (timeNow - modTime) > datetime.timedelta(hours=2):
                # last chance
                _logger.debug(f"Last attempt : {fileName}")
                to_delete = True
            elif (timeInt - modTime) > datetime.timedelta(seconds=5):
                # try
                _logger.debug(f"Normal attempt : {fileName}")
                to_delete = False
            else:
                _logger.debug(f"Wait {timeInt - modTime} : {fileName}")
                to_go = False
            if to_go:
                thr = EvpThr(taskBuffer, adderLock, adderThreadPool, fileName, to_delete, False)
                thr.start()
        except Exception as e:
            _logger.error(f"{str(e)} {traceback.format_exc()}")

    # join all threads
    adderThreadPool.join()

    # stop taskBuffer if created inside this script
    taskBuffer.cleanup(requester=requester_id)

    _logger.debug("===================== end =====================")


# run
if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        data = {"target": sys.argv[1], "test_mode": True, "dump_workflow": True}
    else:
        data = {}
    main(**data)
