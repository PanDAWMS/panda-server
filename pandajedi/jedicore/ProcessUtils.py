import datetime
import multiprocessing
import sys
from collections.abc import Callable
from typing import Any

from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.srvcore import CoreUtils


# wrapper for multiprocessing.Process
class ProcessWrapper(multiprocessing.Process):
    # constructor
    def __init__(self, target: Callable[..., Any], args: tuple[Any, ...]) -> None:
        multiprocessing.Process.__init__(self, target=self.wrappedMain)
        self.target = target
        self.args = args

    # main
    def wrappedMain(self) -> None:
        while True:
            proc = multiprocessing.Process(target=self.target, args=self.args)
            proc.start()
            pid = proc.pid
            if pid is None:
                print(f"{str(naive_utcnow())} {self.__class__.__name__}: ERROR   failed to start the child process")
                break
            while True:
                try:
                    proc.join(20)
                    if not CoreUtils.checkProcess(pid):
                        timeNow = naive_utcnow()
                        print(f"{str(timeNow)} {self.__class__.__name__}: INFO    pid={pid} not exist")
                        break
                except Exception:
                    timeNow = naive_utcnow()
                    errType, errValue = sys.exc_info()[:2]
                    print(f"{str(timeNow)} {self.__class__.__name__}: INFO    failed to check pid={pid} with {errType} {errValue}")
