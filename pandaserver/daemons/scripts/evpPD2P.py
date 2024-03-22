import datetime
import glob
import os.path
import sys
import threading
import time

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.brokerage import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.event_picker import EventPicker

# logger
_logger = PandaLogger().getLogger("evpPD2P")


# main
def main(tbuf=None, **kwargs):
    _logger.debug("===================== start =====================")

    # overall timeout value
    overallTimeout = 300
    # prefix of evp files
    prefixEVP = "evp."
    # file pattern of evp files
    evpFilePatt = panda_config.cache_dir + "/" + prefixEVP + "*"

    # instantiate PD2P

    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    from pandaserver.taskbuffer.TaskBuffer import taskBuffer

    taskBuffer.init(
        panda_config.dbhost,
        panda_config.dbpasswd,
        nDBConnection=1,
        useTimeout=True,
        requester=requester_id,
    )
    siteMapper = SiteMapper.SiteMapper(taskBuffer)

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
        def __init__(self, lock, pool, aTaskBuffer, aSiteMapper, fileName, ignoreError):
            threading.Thread.__init__(self)
            self.lock = lock
            self.pool = pool
            self.fileName = fileName
            self.evp = EventPicker(aTaskBuffer, aSiteMapper, fileName, ignoreError)
            self.pool.add(self)

        def run(self):
            self.lock.acquire()
            retRun = self.evp.run()
            _logger.debug(f"{retRun} : {self.fileName}")
            self.pool.remove(self)
            self.lock.release()

    # get files
    _logger.debug("EVP session")
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
            _logger.debug("time over in EVP session")
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
            if (timeNow - modTime) > datetime.timedelta(hours=24):
                # last chance
                _logger.debug(f"Last event picking : {fileName}")
                thr = EvpThr(adderLock, adderThreadPool, taskBuffer, siteMapper, fileName, False)
                thr.start()
            elif (timeInt - modTime) > datetime.timedelta(minutes=1):
                # try
                _logger.debug(f"event picking : {fileName}")
                thr = EvpThr(adderLock, adderThreadPool, taskBuffer, siteMapper, fileName, True)
                thr.start()
            else:
                _logger.debug(f"{timeInt - modTime} : {fileName}")
        except Exception:
            errType, errValue = sys.exc_info()[:2]
            _logger.error(f"{errType} {errValue}")

    # join all threads
    adderThreadPool.join()

    # stop taskBuffer if created inside this script
    taskBuffer.cleanup(requester=requester_id)

    _logger.debug("===================== end =====================")


# run
if __name__ == "__main__":
    main()
