import multiprocessing
import threading
import time
from collections.abc import ItemsView
from typing import Any


# list with lock
class ListWithLock:
    # The element type is whatever the caller put in -- task specs, dataset specs,
    # brokerage lock IDs -- and the class is used bare in annotations across JEDI, so it
    # is deliberately not generic
    def __init__(self, dataList: list[Any]) -> None:
        self.lock = threading.Lock()
        self.dataList = dataList
        self.dataIndex = 0

    def __iter__(self) -> "ListWithLock":
        return self

    def __contains__(self, item: Any) -> bool:
        self.lock.acquire()
        ret = self.dataList.__contains__(item)
        self.lock.release()
        return ret

    def __next__(self) -> Any:
        if self.dataIndex >= len(self.dataList):
            self.dataIndex = 0
            raise StopIteration
        val = self.dataList[self.dataIndex]
        self.dataIndex += 1
        return val

    def next(self) -> Any:
        return self.__next__()

    def append(self, item: Any) -> bool:
        self.lock.acquire()
        appended = False
        if item not in self.dataList:
            self.dataList.append(item)
            appended = True
        self.lock.release()
        return appended

    def get(self, num: int) -> list[Any]:
        self.lock.acquire()
        retList = self.dataList[self.dataIndex : self.dataIndex + num]
        self.dataIndex += len(retList)
        self.lock.release()
        return retList

    def stat(self) -> tuple[int, int]:
        self.lock.acquire()
        total = len(self.dataList)
        nIndx = self.dataIndex
        self.lock.release()
        return total, nIndx

    def __len__(self) -> int:
        self.lock.acquire()
        ret = len(self.dataList)
        self.lock.release()
        return ret

    def dump(self) -> str:
        self.lock.acquire()
        if len(self.dataList) > self.dataIndex:
            ret = ",".join(map(str, self.dataList[self.dataIndex :]))
        else:
            ret = "None"
        self.lock.release()
        return ret


# map with lock
class MapWithLock:
    # Two callers use two different shapes -- the task brokers key priorities to RW maps,
    # the job generator keys site names to counts add() sums -- so the map is left open
    # rather than made generic, which every bare MapWithLock annotation in JEDI relies on
    def __init__(self, dataMap: dict[Any, Any] | None = None) -> None:
        self.lock = threading.Lock()
        if dataMap is None:
            dataMap = {}
        self.dataMap = dataMap

    def __getitem__(self, item: Any) -> Any:
        ret = self.dataMap.__getitem__(item)
        return ret

    def __setitem__(self, item: Any, value: Any) -> None:
        self.dataMap.__setitem__(item, value)

    def __contains__(self, item: Any) -> bool:
        ret = self.dataMap.__contains__(item)
        return ret

    def acquire(self) -> None:
        self.lock.acquire()

    def release(self) -> None:
        self.lock.release()

    def add(self, item: Any, value: int) -> None:
        if item not in self.dataMap:
            self.dataMap[item] = 0
        self.dataMap[item] += value

    def get(self, item: Any) -> Any:
        if item not in self.dataMap:
            return 0
        return self.dataMap[item]

    def items(self) -> ItemsView[Any, Any]:
        return self.dataMap.items()

    def iteritems(self) -> ItemsView[Any, Any]:
        return self.items()


# thread pool
class ThreadPool:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.list: list[threading.Thread] = []

    # add thread
    def add(self, obj: threading.Thread) -> None:
        self.lock.acquire()
        self.list.append(obj)
        self.lock.release()

    # remove thread
    def remove(self, obj: threading.Thread) -> None:
        self.lock.acquire()
        try:
            self.list.remove(obj)
        except Exception:
            pass
        self.lock.release()

    # join
    def join(self, timeOut: float | None = None) -> None:
        thrlist = tuple(self.list)
        for thr in thrlist:
            try:
                thr.join(timeOut)
                if thr.is_alive():
                    break
            except Exception:
                pass

    # remove inactive threads
    def clean(self) -> None:
        thrlist = tuple(self.list)
        for thr in thrlist:
            if not thr.is_alive():
                self.remove(thr)

    # dump contents
    def dump(self) -> str:
        thrlist = tuple(self.list)
        nActv = 0
        for thr in thrlist:
            if thr.is_alive():
                nActv += 1
        return f"nActive={nActv}"


# thread class working with semaphore and thread pool
class WorkerThread(threading.Thread):
    # constructor
    def __init__(self, workerSemaphore: threading.Semaphore | None, threadPool: ThreadPool | None, logger: Any) -> None:
        threading.Thread.__init__(self)
        self.workerSemaphore = workerSemaphore
        self.threadPool = threadPool
        if self.threadPool is not None:
            self.threadPool.add(self)
        self.logger = logger

    # the real work, implemented by every subclass
    def runImpl(self) -> None:
        raise NotImplementedError("runImpl is not implemented")

    # main loop
    def run(self) -> None:
        # get slot
        if self.workerSemaphore is not None:
            self.workerSemaphore.acquire()
        # execute real work
        try:
            self.runImpl()
        except Exception as e:
            self.logger.error(f"{self.__class__.__name__} crashed in WorkerThread.run() with {type(e).__name__}:{e}")
        # remove self from thread pool
        if self.threadPool is not None:
            self.threadPool.remove(self)
        # release slot
        if self.workerSemaphore is not None:
            self.workerSemaphore.release()


# thread class to cleanup zombie processes
class ZombieCleaner(threading.Thread):
    # constructor
    def __init__(self, interval: int = 20) -> None:
        threading.Thread.__init__(self)
        self.interval = interval

    # main loop
    def run(self) -> None:
        while True:
            # the reaping is the side effect of the call, not the list it returns
            multiprocessing.active_children()
            time.sleep(self.interval)
