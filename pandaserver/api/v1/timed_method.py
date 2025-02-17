import threading

TIME_OUT = "TimeOut"


# a wrapper to install timeout into a method
class TimedMethod:
    def __init__(self, method, timeout):
        self.method = method
        self.timeout = timeout
        self.result = TIME_OUT

    # method emulation
    def __call__(self, *var, **kwargs):
        self.result = self.method(*var, **kwargs)

    # run
    def run(self, *var, **kwargs):
        thr = threading.Thread(target=self, args=var, kwargs=kwargs)
        thr.start()
        thr.join()
