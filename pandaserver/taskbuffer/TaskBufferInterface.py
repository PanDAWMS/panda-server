import sys
import pickle
import multiprocessing
from concurrent.futures import  ThreadPoolExecutor

# required to reserve changed attributes

from pandaserver.taskbuffer import JobSpec
from pandaserver.taskbuffer import FileSpec
JobSpec.reserveChangedState = True
FileSpec.reserveChangedState = True


# method class
class TaskBufferMethod:
    def __init__(self,methodName,commDict,childlock,comLock,resLock):
        self.methodName = methodName
        self.childlock = childlock
        self.commDict = commDict
        self.comLock = comLock
        self.resLock = resLock

    def __call__(self,*args,**kwargs):
        # get lock among children
        i = self.childlock.get()
        # make dict to send it master
        self.commDict[i].update({'methodName': self.methodName,
                                 'args': pickle.dumps(args),
                                 'kwargs': pickle.dumps(kwargs)})
        # send notification to master
        self.comLock[i].release()
        # wait response
        self.resLock[i].acquire()
        res = self.commDict[i]['res']
        statusCode = self.commDict[i]['stat']
        # release lock to children 
        self.childlock.put(i)
        # return
        if statusCode == 0:
            return res
        else:
            errtype,errvalue = res
            raise RuntimeError("{0}: {1} {2}".format(self.methodName,errtype.__name__,errvalue))



# child class
class TaskBufferInterfaceChild:
    # constructor
    def __init__(self,commDict,childlock,comLock,resLock):
        self.childlock = childlock
        self.commDict = commDict
        self.comLock = comLock
        self.resLock = resLock


    # method emulation
    def __getattr__(self,attrName):
        return TaskBufferMethod(attrName,self.commDict,self.childlock,
                                self.comLock,self.resLock)
        

# master class
class TaskBufferInterface:
    # constructor
    def __init__(self):
        # make manager to create shared objects
        self.manager = multiprocessing.Manager()

    # main loop
    def run(self, taskBuffer, commDict, comLock, resLock):
        with ThreadPoolExecutor(max_workers=taskBuffer.get_num_connections()) as pool:
            [pool.submit(self.thread_run, taskBuffer, commDict[i], comLock[i], resLock[i]) for i in commDict.keys()]

    # main loop
    def thread_run(self, taskBuffer, commDict, comLock, resLock):
        # main loop
        while True:
            # wait for command
            comLock.acquire()
            # get command from child
            methodName = commDict['methodName']
            args = pickle.loads(commDict['args'])
            kwargs = pickle.loads(commDict['kwargs'])
            # execute
            try:
                method = getattr(taskBuffer,methodName)
                res = method(*args, **kwargs)
                commDict['stat'] = 0
            except Exception:
                res = sys.exc_info()[:2]
                commDict['stat'] = 1
            # set response
            commDict['res'] = res
            # send response
            resLock.release()

    # launcher
    def launch(self, taskBuffer):
        # shared objects
        self.childlock = multiprocessing.Queue()
        self.commDict = dict()
        self.comLock = dict()
        self.resLock = dict()
        for i in range(taskBuffer.get_num_connections()):
            self.childlock.put(i)
            self.commDict[i] = self.manager.dict()
            self.comLock[i] = multiprocessing.Semaphore(0)
            self.resLock[i] = multiprocessing.Semaphore(0)

        # run
        self.process = multiprocessing.Process(target=self.run,
                                               args=(taskBuffer, self.commDict, self.comLock, self.resLock))
        self.process.start()


    # get interface for child
    def getInterface(self):
        return TaskBufferInterfaceChild(self.commDict, self.childlock, self.comLock, self.resLock)


    # kill
    def terminate(self):
        self.process.terminate()
