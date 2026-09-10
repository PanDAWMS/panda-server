import multiprocessing
import os
import pickle
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.taskbuffer import FileSpec, JobSpec

if TYPE_CHECKING:
    from pandaserver.taskbuffer.TaskBuffer import TaskBuffer

JobSpec.reserveChangedState = True
FileSpec.reserveChangedState = True


_logger = PandaLogger().getLogger("TaskBufferInterface")


# method class
class TaskBufferMethod:
    def __init__(
        self, methodName: str, commDict: dict[int, Any], childlock: "multiprocessing.Queue[int]", comLock: dict[int, Any], resLock: dict[int, Any]
    ) -> None:
        self.methodName = methodName
        self.childlock = childlock
        self.commDict = commDict
        self.comLock = comLock
        self.resLock = resLock

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        log = LogWrapper(
            _logger,
            f"pid={os.getpid()} thr={threading.current_thread().ident} {self.methodName}",
        )
        log.debug("start")
        # get lock among children
        i = self.childlock.get()
        # make dict to send it master
        self.commDict[i].update(
            {
                "methodName": self.methodName,
                "args": pickle.dumps(args),
                "kwargs": pickle.dumps(kwargs),
            }
        )
        # send notification to master
        self.comLock[i].release()
        # wait response
        self.resLock[i].acquire()
        res = pickle.loads(self.commDict[i]["res"])
        statusCode = self.commDict[i]["stat"]
        # release lock to children
        self.childlock.put(i)
        log.debug("end")
        # return
        if statusCode == 0:
            return res
        else:
            errtype, errvalue = res
            raise RuntimeError(f"{self.methodName}: {errtype.__name__} {errvalue}")


# child class
class TaskBufferInterfaceChild:
    # constructor
    def __init__(self, commDict: dict[int, Any], childlock: "multiprocessing.Queue[int]", comLock: dict[int, Any], resLock: dict[int, Any]) -> None:
        self.childlock = childlock
        self.commDict = commDict
        self.comLock = comLock
        self.resLock = resLock

    # method emulation
    def __getattr__(self, attrName: str) -> TaskBufferMethod:
        return TaskBufferMethod(attrName, self.commDict, self.childlock, self.comLock, self.resLock)


# master class
class TaskBufferInterface:
    # The TaskBuffer this interface serves, installed by launch() before the child process
    # runs. Its type is Any since launch() takes it untyped from the caller
    taskBuffer: Any

    # constructor
    def __init__(self) -> None:
        # make manager to create shared objects
        self.manager = multiprocessing.Manager()
        self.taskBuffer = None

    # main loop
    def run(self, taskBuffer: "TaskBuffer", commDict: dict[int, Any], comLock: dict[int, Any], resLock: dict[int, Any], to_stop: Any) -> None:
        with ThreadPoolExecutor(max_workers=taskBuffer.get_num_connections()) as pool:
            [
                pool.submit(
                    self.thread_run,
                    taskBuffer,
                    commDict[i],
                    comLock[i],
                    resLock[i],
                    to_stop,
                )
                for i in commDict.keys()
            ]

    # main loop
    def thread_run(self, taskBuffer: "TaskBuffer", commDict: Any, comLock: Any, resLock: Any, to_stop: Any) -> None:
        # main loop
        while True:
            # stop sign
            if to_stop.value:
                break
            # wait for command
            if not comLock.acquire(timeout=0.25):
                continue
            try:
                # get command from child
                methodName = commDict["methodName"]
                args = pickle.loads(commDict["args"])
                kwargs = pickle.loads(commDict["kwargs"])
                # execute
                method = getattr(taskBuffer, methodName)
                res = method(*args, **kwargs)
                commDict["stat"] = 0
                # set response
                commDict["res"] = pickle.dumps(res)
            except Exception:
                res = sys.exc_info()[:2]
                commDict["stat"] = 1
                commDict["res"] = pickle.dumps(res)
            # send response
            resLock.release()

    # launcher
    def launch(self, taskBuffer: "TaskBuffer") -> None:
        # shared objects
        # the queue holds the index of each free child, handed out one at a time
        self.childlock: multiprocessing.Queue[int] = multiprocessing.Queue()
        self.commDict: dict[int, Any] = {}
        self.comLock: dict[int, Any] = {}
        self.resLock: dict[int, Any] = {}
        self.taskBuffer = taskBuffer
        self.to_stop = multiprocessing.Value("i", 0)
        for i in range(taskBuffer.get_num_connections()):
            self.childlock.put(i)
            self.commDict[i] = self.manager.dict()
            self.comLock[i] = multiprocessing.Semaphore(0)
            self.resLock[i] = multiprocessing.Semaphore(0)

        # run
        self.process = multiprocessing.Process(
            target=self.run,
            args=(taskBuffer, self.commDict, self.comLock, self.resLock, self.to_stop),
        )
        self.process.start()

    # get interface for child
    def getInterface(self) -> "TaskBufferInterfaceChild":
        return TaskBufferInterfaceChild(self.commDict, self.childlock, self.comLock, self.resLock)

    # stop the loop
    def stop(self, requester: str | None = None) -> None:
        with self.to_stop.get_lock():
            self.to_stop.value = 1
        while self.process.is_alive():
            time.sleep(1)
        self.taskBuffer.cleanup(requester=requester)

    # kill
    def terminate(self) -> None:
        self.process.terminate()
