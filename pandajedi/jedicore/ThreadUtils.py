import multiprocessing
import sys
import threading
import time


# list with lock
class ListWithLock:
    def __init__(self, dataList):
        self.lock = threading.Lock()
        self.dataList = dataList
        self.dataIndex = 0

    def __iter__(self):
        return self

    def __contains__(self, item):
        self.lock.acquire()
        ret = self.dataList.__contains__(item)
        self.lock.release()
        return ret

    def __next__(self):
        if self.dataIndex >= len(self.dataList):
            self.dataIndex = 0
            raise StopIteration
        val = self.dataList[self.dataIndex]
        self.dataIndex += 1
        return val

    def next(self):
        return self.__next__()

    def append(self, item):
        self.lock.acquire()
        appended = False
        if item not in self.dataList:
            self.dataList.append(item)
            appended = True
        self.lock.release()
        return appended

    def get(self, num):
        self.lock.acquire()
        retList = self.dataList[self.dataIndex : self.dataIndex + num]
        self.dataIndex += len(retList)
        self.lock.release()
        return retList

    def stat(self):
        self.lock.acquire()
        total = len(self.dataList)
        nIndx = self.dataIndex
        self.lock.release()
        return total, nIndx

    def __len__(self):
        self.lock.acquire()
        ret = len(self.dataList)
        self.lock.release()
        return ret

    def dump(self):
        self.lock.acquire()
        if len(self.dataList) > self.dataIndex:
            ret = ",".join(map(str, self.dataList[self.dataIndex :]))
        else:
            ret = "None"
        self.lock.release()
        return ret


# map with lock
class MapWithLock:
    def __init__(self, dataMap=None):
        self.lock = threading.Lock()
        if dataMap is None:
            dataMap = {}
        self.dataMap = dataMap

    def __getitem__(self, item):
        ret = self.dataMap.__getitem__(item)
        return ret

    def __setitem__(self, item, value):
        self.dataMap.__setitem__(item, value)

    def __contains__(self, item):
        ret = self.dataMap.__contains__(item)
        return ret

    def acquire(self):
        self.lock.acquire()

    def release(self):
        self.lock.release()

    def add(self, item, value):
        if item not in self.dataMap:
            self.dataMap[item] = 0
        self.dataMap[item] += value

    def get(self, item):
        if item not in self.dataMap:
            return 0
        return self.dataMap[item]

    def items(self):
        return self.dataMap.items()

    def iteritems(self):
        return self.items()


# thread pool
class ThreadPool:
    def __init__(self):
        self.lock = threading.Lock()
        self.list = []

    # add thread
    def add(self, obj):
        self.lock.acquire()
        self.list.append(obj)
        self.lock.release()

    # remove thread
    def remove(self, obj):
        self.lock.acquire()
        try:
            self.list.remove(obj)
        except Exception:
            pass
        self.lock.release()

    # join
    def join(self, timeOut=None):
        thrlist = tuple(self.list)
        for thr in thrlist:
            try:
                thr.join(timeOut)
                if thr.is_alive():
                    break
            except Exception:
                pass

    # remove inactive threads
    def clean(self):
        thrlist = tuple(self.list)
        for thr in thrlist:
            if not thr.is_alive():
                self.remove(thr)

    # dump contents
    def dump(self):
        thrlist = tuple(self.list)
        nActv = 0
        for thr in thrlist:
            if thr.is_alive():
                nActv += 1
        return f"nActive={nActv}"


# thread class working with semaphore and thread pool
class WorkerThread(threading.Thread):
    # constructor
    def __init__(self, workerSemaphore, threadPool, logger):
        threading.Thread.__init__(self)
        self.workerSemaphore = workerSemaphore
        self.threadPool = threadPool
        if self.threadPool is not None:
            self.threadPool.add(self)
        self.logger = logger

    # main loop
    def run(self):
        # get slot
        if self.workerSemaphore is not None:
            self.workerSemaphore.acquire()
        # execute real work
        try:
            self.runImpl()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            self.logger.error(f"{self.__class__.__name__} crashed in WorkerThread.run() with {errtype.__name__}:{errvalue}")
        # remove self from thread pool
        if self.threadPool is not None:
            self.threadPool.remove(self)
        # release slot
        if self.workerSemaphore is not None:
            self.workerSemaphore.release()


# thread class to cleanup zombi processes
class ZombieCleaner(threading.Thread):
    # constructor
    def __init__(self, interval=20):
        threading.Thread.__init__(self)
        self.interval = interval

    # main loop
    def run(self):
        while True:
            x = multiprocessing.active_children()
            time.sleep(self.interval)
