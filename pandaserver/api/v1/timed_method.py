import threading

TIME_OUT = "TimeOut"


# a wrapper to install timeout into a method
class TimedMethod:
    def __init__(self, method, timeout):
        self.method = method
        self.timeout = timeout
        self.result = TIME_OUT

    # method emulation
    def __call__(self, *var):
        self.result = self.method(*var)

    # run
    def run(self, *var):
        thr = threading.Thread(target=self, args=var)
        thr.start()
        thr.join()
