import datetime
import glob
import json
import os.path
import sys
import threading
import time
import traceback

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.dataservice import RecoverLostFilesCore

# logger
_logger = PandaLogger().getLogger("recover_lost_files")


# main
def main(tbuf=None, **kwargs):
    _logger.debug("===================== start =====================")

    # overall timeout value
    overallTimeout = 300
    # prefix of the files
    prefixEVP = "recov."
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

    # thread to ev-pd2p
    class EvpThr(threading.Thread):
        def __init__(self, lock, pool, tb_if, file_name, to_delete):
            threading.Thread.__init__(self)
            self.lock = lock
            self.pool = pool
            self.fileName = file_name
            self.to_delete = to_delete
            self.taskBuffer = tb_if
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            with open(self.fileName) as f:
                ops = json.load(f)
                tmpLog = LogWrapper(_logger, f"< jediTaskID={ops['jediTaskID']} >")
                tmpLog.info(f"start {self.fileName}")
                s, o = RecoverLostFilesCore.main(self.taskBuffer, ops, tmpLog)
                tmpLog.info(f"status={s}. {o}")
                if s is not None or self.to_delete:
                    tmpLog.debug(f"delete {self.fileName}")
                    try:
                        os.remove(self.fileName)
                    except Exception:
                        pass
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
            if (timeNow - modTime) > datetime.timedelta(hours=2):
                # last chance
                _logger.debug(f"Last attempt : {fileName}")
                thr = EvpThr(adderLock, adderThreadPool, taskBuffer, fileName, False)
                thr.start()
            elif (timeInt - modTime) > datetime.timedelta(seconds=5):
                # try
                _logger.debug(f"Normal attempt : {fileName}")
                thr = EvpThr(adderLock, adderThreadPool, taskBuffer, fileName, True)
                thr.start()
            else:
                _logger.debug(f"Wait {timeInt - modTime} : {fileName}")
        except Exception as e:
            _logger.error(f"{str(e)} {traceback.format_exc()}")

    # join all threads
    adderThreadPool.join()

    # stop taskBuffer if created inside this script
    taskBuffer.cleanup(requester=requester_id)

    _logger.debug("===================== end =====================")


# run
if __name__ == "__main__":
    main()
