import datetime
import multiprocessing
import multiprocessing.reduction
import os
import signal
import sys
import time

from pandacommon.pandautils.PandaUtils import naive_utcnow

try:
    from multiprocessing.connection import reduce_connection
except ImportError:
    from multiprocessing.reduction import reduce_connection

# import multiprocessing
# logger = multiprocessing.log_to_stderr()
# logger.setLevel(multiprocessing.SUBDEBUG)

###########################################################
#
# status codes for IPC
#


# class for status code
class StatusCode(object):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return f"{self.value}"

    # comparator
    def __eq__(self, other):
        try:
            return self.value == other.value
        except Exception:
            return False

    def __ne__(self, other):
        try:
            return self.value != other.value
        except Exception:
            return True


# mapping to accessors
statusCodeMap = {
    "SC_SUCCEEDED": StatusCode(0),
    "SC_FAILED": StatusCode(1),
    "SC_FATAL": StatusCode(2),
}


# install the list of status codes to a class
def installSC(cls):
    for sc, val in statusCodeMap.items():
        setattr(cls, sc, val)


# install SCs in this module
installSC(sys.modules[__name__])


###########################################################
#
# classes for IPC
#


# log message with timestamp
def dumpStdOut(sender, message):
    timeNow = naive_utcnow()
    print(f"{str(timeNow)} {sender}: INFO    {message}")


# object class for command
class CommandObject(object):
    # constructor
    def __init__(self, methodName, argList, argMap):
        self.methodName = methodName
        self.argList = argList
        self.argMap = argMap


# object class for response
class ReturnObject(object):
    # constructor
    def __init__(self):
        self.statusCode = None
        self.errorValue = None
        self.returnValue = None


# process class
class ProcessClass(object):
    # constructor
    def __init__(self, pid, connection):
        self.pid = pid
        self.nused = 0
        self.usedMemory = 0
        self.nMemLookup = 20
        # reduce connection to make it picklable
        self.reduced_pipe = reduce_connection(connection)

    # get connection
    def connection(self):
        # rebuild connection
        return self.reduced_pipe[0](*self.reduced_pipe[1])

    # reduce connection
    def reduceConnection(self, connection):
        self.reduced_pipe = reduce_connection(connection)

    # get memory usage
    def getMemUsage(self):
        # update memory info
        if self.nused % self.nMemLookup == 0:
            try:
                # read memory info from /proc
                t = open(f"/proc/{self.pid}/status")
                v = t.read()
                t.close()
                value = 0
                for line in v.split("\n"):
                    if line.startswith("VmRSS"):
                        items = line.split()
                        value = int(items[1])
                        if items[2] in ["kB", "KB"]:
                            value /= 1024
                        elif items[2] in ["mB", "MB"]:
                            pass
                        break
                self.usedMemory = value
            except Exception:
                pass
            return self.usedMemory
        else:
            return None


# method class
class MethodClass(object):
    # constructor
    def __init__(self, className, methodName, vo, connectionQueue, voIF):
        self.className = className
        self.methodName = methodName
        self.vo = vo
        self.connectionQueue = connectionQueue
        self.voIF = voIF
        self.pipeList = []

    # method emulation
    def __call__(self, *args, **kwargs):
        commandObj = CommandObject(self.methodName, args, kwargs)
        nTry = 3
        for iTry in range(nTry):
            # exceptions
            retException = None
            strException = None
            try:
                stepIdx = 0
                # get child process
                child_process = self.connectionQueue.get()
                # get pipe
                stepIdx = 1
                pipe = child_process.connection()
                # get ack
                stepIdx = 2
                timeoutPeriodACK = 30
                if not pipe.poll(timeoutPeriodACK):
                    raise JEDITimeoutError(f"did not get ACK for {timeoutPeriodACK}sec")
                ack = pipe.recv()
                # send command
                stepIdx = 3
                pipe.send(commandObj)
                # wait response
                stepIdx = 4
                timeoutPeriod = 600
                timeNow = naive_utcnow()
                if not pipe.poll(timeoutPeriod):
                    raise JEDITimeoutError(f"did not get response for {timeoutPeriod}sec")
                regTime = naive_utcnow() - timeNow
                if regTime > datetime.timedelta(seconds=60):
                    dumpStdOut(
                        self.className,
                        f"methodName={self.methodName} took {regTime.seconds}.{int(regTime.microseconds / 1000):03d} sec in pid={child_process.pid}",
                    )
                # get response
                stepIdx = 5
                ret = pipe.recv()
                # set exception type based on error
                stepIdx = 6
                if ret.statusCode == SC_FAILED:
                    retException = JEDITemporaryError
                elif ret.statusCode == SC_FATAL:
                    retException = JEDIFatalError
            except Exception:
                errtype, errvalue = sys.exc_info()[:2]
                retException = errtype
                argStr = f"args={str(args)} kargs={str(kwargs)}"
                strException = f"VO={self.vo} type={errtype.__name__} stepIdx={stepIdx} : {self.className}.{self.methodName} {errvalue} {argStr[:200]}"
            # increment nused
            child_process.nused += 1
            # memory check
            largeMemory = False
            memUsed = child_process.getMemUsage()
            if memUsed is not None:
                memStr = f"pid={child_process.pid} memory={memUsed}MB"
                if memUsed > 1.5 * 1024:
                    largeMemory = True
                    memStr += " exceeds memory limit"
                    dumpStdOut(self.className, memStr)
            # kill old or problematic process
            if child_process.nused > 1000 or retException not in [None, JEDITemporaryError, JEDIFatalError] or largeMemory:
                dumpStdOut(
                    self.className,
                    f"methodName={self.methodName} ret={retException} nused={child_process.nused} {strException} in pid={child_process.pid}",
                )
                # close connection
                try:
                    pipe.close()
                except Exception:
                    pass
                # terminate child process
                try:
                    dumpStdOut(self.className, f"killing pid={child_process.pid}")
                    os.kill(child_process.pid, signal.SIGKILL)
                    dumpStdOut(self.className, f"waiting pid={child_process.pid}")
                    os.waitpid(child_process.pid, 0)
                    dumpStdOut(self.className, f"terminated pid={child_process.pid}")
                except Exception:
                    errtype, errvalue = sys.exc_info()[:2]
                    if "No child processes" not in str(errvalue):
                        dumpStdOut(self.className, f"failed to terminate {child_process.pid} with {errtype}:{errvalue}")
                # make new child process
                self.voIF.launchChild()
            else:
                # reduce process object to avoid deadlock due to rebuilding of connection
                child_process.reduceConnection(pipe)
                self.connectionQueue.put(child_process)
            # success, fatal error, or maximally attempted
            if retException in [None, JEDIFatalError] or (iTry + 1 == nTry):
                break
            # sleep
            time.sleep(1)
        # raise exception
        if retException is not None:
            if strException is None:
                strException = f"VO={self.vo} {ret.errorValue}"
            raise retException(strException)
        # return
        if ret.statusCode == SC_SUCCEEDED:
            return ret.returnValue
        else:
            raise retException(f"VO={self.vo} {ret.errorValue}")


# interface class to send command
class CommandSendInterface(object):
    # constructor
    def __init__(self, vo, maxChild, moduleName, className):
        self.vo = vo
        self.maxChild = maxChild
        self.connectionQueue = multiprocessing.Queue(maxChild)
        self.moduleName = moduleName
        self.className = className

    # factory method
    def __getattr__(self, attrName):
        return MethodClass(self.className, attrName, self.vo, self.connectionQueue, self)

    # launcher for child processe
    def launcher(self, channel):
        # import module
        mod = __import__(self.moduleName)
        for subModuleName in self.moduleName.split(".")[1:]:
            mod = getattr(mod, subModuleName)
        # get class
        cls = getattr(mod, self.className)
        # start child process
        msg = f"start {self.className} with pid={os.getpid()}"
        dumpStdOut(self.moduleName, msg)
        timeNow = naive_utcnow()
        try:
            cls(channel).start()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            dumpStdOut(self.className, f"launcher crashed with {errtype}:{errvalue}")

    # launch child processes to interact with DDM
    def launchChild(self):
        # make pipe
        parent_conn, child_conn = multiprocessing.Pipe()
        # make child process
        child_process = multiprocessing.Process(target=self.launcher, args=(child_conn,))
        # start child process
        child_process.daemon = True
        child_process.start()
        # keep process in queue
        processObj = ProcessClass(child_process.pid, parent_conn)
        # sync to wait until object in the child process is instantiated
        pipe = processObj.connection()
        pipe.recv()
        processObj.reduceConnection(pipe)
        # ready
        self.connectionQueue.put(processObj)

    # initialize
    def initialize(self):
        for i in range(self.maxChild):
            self.launchChild()


# interface class to receive command
class CommandReceiveInterface(object):
    # constructor
    def __init__(self, con):
        self.con = con
        self.cacheMap = {}

    # make key for cache
    def makeKey(self, className, methodName, argList, argMap):
        try:
            tmpKey = f"{className}:{methodName}:"
            for argItem in argList:
                tmpKey += f"{str(argItem)}:"
            for argKey, argVal in argMap.items():
                tmpKey += f"{argKey}={str(argVal)}:"
            tmpKey = tmpKey[:-1]
            return tmpKey
        except Exception:
            return None

    # main loop
    def start(self):
        # sync
        self.con.send("ready")
        # main loop
        while True:
            # send ACK
            self.con.send("ack")
            # get command
            commandObj = self.con.recv()
            # make return
            retObj = ReturnObject()
            # get class name
            className = self.__class__.__name__
            # check method name
            if not hasattr(self, commandObj.methodName):
                # method not found
                retObj.statusCode = self.SC_FATAL
                retObj.errorValue = f"type=AttributeError : {className} instance has no attribute {commandObj.methodName}"
            else:
                try:
                    # use cache
                    useCache = False
                    doExec = True
                    if "useResultCache" in commandObj.argMap:
                        # get time range
                        timeRange = commandObj.argMap["useResultCache"]
                        # delete from args map
                        del commandObj.argMap["useResultCache"]
                        # make key for cache
                        tmpCacheKey = self.makeKey(className, commandObj.methodName, commandObj.argList, commandObj.argMap)
                        if tmpCacheKey is not None:
                            useCache = True
                            # cache is fresh
                            if tmpCacheKey in self.cacheMap and self.cacheMap[tmpCacheKey]["utime"] + datetime.timedelta(seconds=timeRange) > naive_utcnow():
                                tmpRet = self.cacheMap[tmpCacheKey]["value"]
                                doExec = False
                    # exec
                    if doExec:
                        # get function
                        functionObj = getattr(self, commandObj.methodName)
                        # exec
                        tmpRet = functionObj(*commandObj.argList, **commandObj.argMap)
                    if isinstance(tmpRet, StatusCode):
                        # only status code was returned
                        retObj.statusCode = tmpRet
                    elif (isinstance(tmpRet, tuple) or isinstance(tmpRet, list)) and len(tmpRet) > 0 and isinstance(tmpRet[0], StatusCode):
                        retObj.statusCode = tmpRet[0]
                        # status code + return values
                        if len(tmpRet) > 1:
                            if retObj.statusCode == self.SC_SUCCEEDED:
                                if len(tmpRet) == 2:
                                    retObj.returnValue = tmpRet[1]
                                else:
                                    retObj.returnValue = tmpRet[1:]
                            else:
                                if len(tmpRet) == 2:
                                    retObj.errorValue = tmpRet[1]
                                else:
                                    retObj.errorValue = tmpRet[1:]
                    else:
                        retObj.statusCode = self.SC_SUCCEEDED
                        retObj.returnValue = tmpRet
                except Exception:
                    errtype, errvalue = sys.exc_info()[:2]
                    # failed
                    retObj.statusCode = self.SC_FATAL
                    retObj.errorValue = f"type={errtype.__name__} : {className}.{commandObj.methodName} : {errvalue}"
                # cache
                if useCache and doExec and retObj.statusCode == self.SC_SUCCEEDED:
                    self.cacheMap[tmpCacheKey] = {"utime": naive_utcnow(), "value": tmpRet}
            # return
            self.con.send(retObj)


# install SCs
installSC(CommandReceiveInterface)


###########################################################
#
# exceptions for IPC
#


# exception for temporary error
class JEDITemporaryError(Exception):
    pass


# exception for fatal error
class JEDIFatalError(Exception):
    pass


# exception for timeout error
class JEDITimeoutError(Exception):
    pass
