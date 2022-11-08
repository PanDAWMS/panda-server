import json
import glob
import time
import os.path
import datetime
import threading
import traceback

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.config import panda_config
from pandaserver.dataservice import RecoverLostFilesCore


# logger
_logger = PandaLogger().getLogger('recover_lost_files')


# main
def main(tbuf=None, **kwargs):
    _logger.debug("===================== start =====================")

    # overall timeout value
    overallTimeout = 300
    # prefix of the files
    prefixEVP = 'recov.'
    # file pattern of evp files
    evpFilePatt = panda_config.cache_dir + '/' + prefixEVP + '*'

    from pandaserver.taskbuffer.TaskBuffer import taskBuffer
    taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, useTimeout=True)

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
                tmpLog = LogWrapper(_logger, '< jediTaskID={} >'.format(ops['jediTaskID']))
                tmpLog.info('start {}'.format(self.fileName))
                s, o = RecoverLostFilesCore.main(self.taskBuffer, ops, tmpLog)
                tmpLog.info('status={}. {}'.format(s, o))
                if s is not None or self.to_delete:
                    tmpLog.debug('delete {}'.format(self.fileName))
                    try:
                        os.remove(self.fileName)
                    except Exception:
                        pass
            self.pool.remove(self)
            self.lock.release()

    # get files
    timeNow = datetime.datetime.utcnow()
    timeInt = datetime.datetime.utcnow()
    fileList = glob.glob(evpFilePatt)
    fileList.sort()

    # create thread pool and semaphore
    adderLock = threading.Semaphore(1)
    adderThreadPool = ThreadPool()

    # add
    while len(fileList) != 0:
        # time limit to aviod too many copyArchve running at the sametime
        if (datetime.datetime.utcnow() - timeNow) > datetime.timedelta(minutes=overallTimeout):
            _logger.debug("time over in main session")
            break
        # try to get Semaphore
        adderLock.acquire()
        # get fileList
        if (datetime.datetime.utcnow() - timeInt) > datetime.timedelta(minutes=15):
            timeInt = datetime.datetime.utcnow()
            # get file
            fileList = glob.glob(evpFilePatt)
            fileList.sort()
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
                _logger.debug("Last attempt : %s" % fileName)
                thr = EvpThr(adderLock, adderThreadPool, taskBuffer, fileName, False)
                thr.start()
            elif (timeInt - modTime) > datetime.timedelta(seconds=5):
                # try
                _logger.debug("Normal attempt : %s" % fileName)
                thr = EvpThr(adderLock, adderThreadPool, taskBuffer, fileName, True)
                thr.start()
            else:
                _logger.debug("Wait %s : %s" % ((timeInt - modTime), fileName))
        except Exception as e:
            _logger.error("{} {}".format(str(e), traceback.format_exc()))

    # join all threads
    adderThreadPool.join()

    _logger.debug("===================== end =====================")


# run
if __name__ == '__main__':
    main()
