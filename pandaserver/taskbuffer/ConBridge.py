"""
ConBridge allows having database interactions in separate processes and killing them independently when interactions are stalled.
This avoids clogged httpd processes due to stalled database accesses.
"""

import os
import pickle
import random
import signal
import socket
import sys
import threading
import time
import traceback
import types

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.taskbuffer import OraDBProxy as DBProxy
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec

# logger
_logger = PandaLogger().getLogger("ConBridge")


# exception for normal termination
class HarmlessEx(Exception):
    pass


# terminate child process by itself when master has gone
class Terminator(threading.Thread):
    # constructor
    def __init__(self, consock):
        threading.Thread.__init__(self)
        self.consock = consock

    # main
    def run(self):
        # watching control socket
        try:
            rcvSize = self.consock.recv(1)
        except Exception:
            pass
        # get PID
        pid = os.getpid()
        _logger.debug(f"child  {pid} received termination")
        # kill
        try:
            os.kill(pid, signal.SIGTERM)
        except Exception:
            pass
        try:
            os.kill(pid, signal.SIGKILL)
        except Exception:
            pass


# connection bridge with timeout
class ConBridge(object):
    # constructor
    def __init__(self):
        self.child_pid = 0
        self.isMaster = False
        self.mysock = None
        self.consock = None
        self.proxy = None
        self.pid = os.getpid()
        # timeout
        if hasattr(panda_config, "dbtimeout"):
            self.timeout = int(panda_config.dbtimeout)
        else:
            self.timeout = 600
        # verbose
        if hasattr(panda_config, "dbbridgeverbose"):
            self.verbose = panda_config.dbbridgeverbose
        else:
            self.verbose = False

    # destructor
    def __del__(self):
        # kill old child process
        self.bridge_killChild()

    # connect
    def connect(
        self,
        dbhost=panda_config.dbhost,
        dbpasswd=panda_config.dbpasswd,
        dbuser=panda_config.dbuser,
        dbname=panda_config.dbname,
        dbtimeout=None,
        reconnect=False,
    ):
        # kill old child process
        self.bridge_killChild()
        _logger.debug(f"master {self.pid} connecting")
        # reset child PID and sockets
        self.child_pid = 0
        self.mysock = None
        self.consock = None
        # create socket
        datpair = socket.socketpair()
        conpair = socket.socketpair()
        # fork
        self.child_pid = os.fork()
        if self.child_pid == 0:
            # child
            self.isMaster = False
            self.pid = os.getpid()
            # keep socket
            self.mysock = datpair[1]
            self.consock = conpair[1]
            datpair[0].close()
            conpair[0].close()
            # connect to database
            _logger.debug(f"child  {self.pid} connecting to database")
            self.proxy = DBProxy.DBProxy()
            if not self.proxy.connect(dbhost=dbhost, dbpasswd=dbpasswd, dbtimeout=60):
                _logger.error(f"child  {self.pid} failed to connect")
                # send error
                self.bridge_sendError((RuntimeError, f"child  {self.pid} connection failed"))
                # exit
                self.bridge_childExit()
            # send OK just for ACK
            _logger.debug(f"child  {self.pid} connection is ready")
            self.bridge_sendResponse(None)
            # start terminator
            Terminator(self.consock).start()
            # go main loop
            _logger.debug(f"child  {self.pid} going into the main loop")
            self.bridge_run()
            # exit
            self.bridge_childExit(0)
        else:
            # master
            self.isMaster = True
            # keep socket
            self.mysock = datpair[0]
            self.consock = conpair[0]
            datpair[1].close()
            conpair[1].close()
            try:
                # get ACK
                _logger.debug(f"master {self.pid} waiting ack from child={self.child_pid}")
                self.bridge_getResponse()
                _logger.debug(f"master {self.pid} got ready from child={self.child_pid}")
                return True
            except Exception as e:
                _logger.error(f"master {self.pid} failed to setup child={self.child_pid} : {str(e)} {traceback.format_exc()}")
                # kill child
                self.bridge_killChild()
                return False

    #######################
    # communication methods

    # send packet
    def bridge_send(self, val):
        try:
            # set timeout
            if self.isMaster:
                self.mysock.settimeout(self.timeout)
            # serialize
            tmpStr = pickle.dumps(val, protocol=0)
            # send size
            sizeStr = "%50s" % len(tmpStr)
            self.mysock.sendall(sizeStr.encode())
            # send body
            self.mysock.sendall(tmpStr)
            # set timeout back
            if self.isMaster:
                self.mysock.settimeout(None)
        except Exception as e:
            if self.isMaster:
                roleType = "master"
            else:
                roleType = "child "
            _logger.error(f"{roleType} {self.pid} send error : val={str(val)[:1024]} - {str(e)} {traceback.format_exc()}")
            # terminate child
            if not self.isMaster:
                self.bridge_childExit()
            raise e

    # receive packet
    def bridge_recv(self):
        try:
            # set timeout
            if self.isMaster:
                self.mysock.settimeout(self.timeout)
            # get size
            strSize = None
            headSize = 50
            while strSize is None or len(strSize) < headSize:
                if strSize is None:
                    tmpSize = headSize
                else:
                    tmpSize = headSize - len(strSize)
                tmpStr = self.mysock.recv(tmpSize)
                if len(tmpStr) == 0:
                    if self.isMaster:
                        raise socket.error("empty packet")
                    else:
                        # master closed socket
                        raise HarmlessEx("empty packet")
                if strSize is None:
                    strSize = tmpStr
                else:
                    strSize += tmpStr
            if strSize is None:
                strSize = ""
            else:
                strSize = strSize.decode()
            # get body
            strBody = None
            bodySize = int(strSize)
            while strBody is None or len(strBody) < bodySize:
                if strBody is None:
                    tmpSize = bodySize
                else:
                    tmpSize = bodySize - len(strBody)
                tmpStr = self.mysock.recv(tmpSize)
                if len(tmpStr) == 0:
                    if self.isMaster:
                        raise socket.error("empty packet")
                    else:
                        # master closed socket
                        raise HarmlessEx("empty packet")
                if strBody is None:
                    strBody = tmpStr
                else:
                    strBody += tmpStr
            if strBody is None:
                strBody = "".encode()
            # set timeout back
            if self.isMaster:
                self.mysock.settimeout(None)
            # deserialize
            retVal = pickle.loads(strBody)
            return True, retVal
        except Exception as e:
            if self.isMaster:
                roleType = "master"
            else:
                roleType = "child "
            if isinstance(e, HarmlessEx):
                _logger.debug(f"{roleType} {self.pid} recv harmless ex : {str(e)}")
            else:
                _logger.error(f"{roleType} {self.pid} recv error : {str(e)} {traceback.format_exc()}")
            # terminate child
            if not self.isMaster:
                self.bridge_childExit()
            raise e

    #######################
    # child's methods

    # send error
    def bridge_sendError(self, val):
        # send status
        self.bridge_send("NG")
        # check if pickle-able
        try:
            pickle.dumps(val, protocol=0)
        except Exception:
            # use RuntimeError
            val = (RuntimeError, str(val[-1]))
        # send exceptions
        self.bridge_send(val)

    # send response
    def bridge_sendResponse(self, val):
        # send status
        self.bridge_send("OK")
        # send response
        self.bridge_send(val)

    # termination of child
    def bridge_childExit(self, exitCode=1):
        if not self.isMaster:
            # close database connection
            _logger.debug(f"child  {self.pid} closing database connection")
            if self.proxy:
                self.proxy.cleanup()
            # close sockets
            _logger.debug(f"child  {self.pid} closing sockets")
            try:
                self.mysock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.consock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            # exit
            _logger.debug(f"child  {self.pid} going to exit")
            os._exit(exitCode)

    # child main
    def bridge_run(self):
        comStr = ""
        while True:
            try:
                # get command
                status, comStr = self.bridge_recv()
                if not status:
                    raise RuntimeError("invalid command")
                # get variables
                status, variables = self.bridge_recv()
                if not status:
                    raise RuntimeError("invalid variables")
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                _logger.error(f"child  {self.pid} died : {errType} {errValue}")
                # exit
                self.bridge_childExit()
            if self.verbose:
                _logger.debug(f"child  {self.pid} method {comStr} executing")
            try:
                # execute
                method = getattr(self.proxy, comStr)
                res = method(*variables[0], **variables[1])
                # FIXME : modify response since oracledb types cannot be picked
                if comStr in ["querySQLS"]:
                    newRes = [True] + list(res[1:])
                    res = newRes
                if self.verbose:
                    _logger.debug(f"child  {self.pid} method {comStr} completed")
                # return
                self.bridge_sendResponse((res, variables[0], variables[1]))
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                _logger.error(f"child  {self.pid} method {comStr} failed : {errType} {errValue}")
                if errType in [socket.error, socket.timeout]:
                    _logger.error(f"child  {self.pid} died : {errType} {errValue}")
                    # exit
                    self.bridge_childExit()
                # send error
                self.bridge_sendError((errType, errValue))

    #######################
    # master's methods

    # kill child
    def bridge_killChild(self):
        # kill old child process
        if self.child_pid != 0:
            # close sockets
            _logger.debug(f"master {self.pid} closing sockets for child={self.child_pid}")
            try:
                if self.mysock is not None:
                    self.mysock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                if self.consock is not None:
                    self.consock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            _logger.debug(f"master {self.pid} killing child={self.child_pid}")
            # send SIGTERM
            try:
                os.kill(self.child_pid, signal.SIGTERM)
            except Exception:
                pass
            time.sleep(2)
            # send SIGKILL
            try:
                os.kill(self.child_pid, signal.SIGKILL)
            except Exception:
                pass
            # wait for completion of child
            _logger.debug(f"master {self.pid} waiting child={self.child_pid}")
            try:
                os.waitpid(self.child_pid, 0)
            except Exception:
                pass
            # sleep to avoid burst reconnection
            time.sleep(random.randint(5, 15))
            _logger.debug(f"master {self.pid} killed child={self.child_pid}")

    # get response
    def bridge_getResponse(self):
        # get status
        status, strStatus = self.bridge_recv()
        if not status:
            raise RuntimeError(f"master {self.pid} got invalid status response from child={self.child_pid}")
        if strStatus == "OK":
            # return res
            status, ret = self.bridge_recv()
            if not status:
                raise RuntimeError(f"master {self.pid} got invalid response body from child={self.child_pid}")
            return ret
        elif strStatus == "NG":
            # raise error
            status, ret = self.bridge_recv()
            if not status:
                raise RuntimeError(f"master {self.pid} got invalid response value from child={self.child_pid}")
            raise ret[0](ret[1])
        else:
            raise RuntimeError(f"master {self.pid} got invalid response from child={self.child_pid} : {str(strStatus)}")

    # method wrapper class
    class bridge_masterMethod:
        # constructor
        def __init__(self, name, parent):
            self.name = name
            self.parent = parent
            self.pid = os.getpid()

        # copy changes in taskbuff objects to master
        def copyTbObjChanges(self, oldPar, newPar):
            # check they have the same type
            if not isinstance(oldPar, type(newPar)):
                return False
            # copy some Specs since they are passed via ref's
            if isinstance(oldPar, JobSpec) or isinstance(oldPar, FileSpec) or isinstance(oldPar, DatasetSpec):
                if hasattr(oldPar, "__getstate__"):
                    tmpStat = newPar.__getstate__()
                    oldPar.__setstate__(tmpStat)
                else:
                    tmpStat = newPar.values()
                    oldPar.pack(tmpStat)
                return True
            # copy Datasets
            return False

        # copy changes in objects to master
        def copyChanges(self, oldPar, newPar):
            if isinstance(oldPar, list):
                # delete all elements first
                while len(oldPar) > 0:
                    oldPar.pop()
                # append
                for tmpItem in newPar:
                    oldPar.append(tmpItem)
            elif isinstance(oldPar, dict):
                # replace
                for tmpKey in newPar:
                    oldPar[tmpKey] = newPar[tmpKey]
            else:
                self.copyTbObjChanges(oldPar, newPar)

        # method emulation
        def __call__(self, *args, **keywords):
            while True:
                try:
                    # send command name
                    self.parent.bridge_send(self.name)
                    # send variables
                    self.parent.bridge_send((args, keywords))
                    # get response
                    retVal, newArgs, newKeywords = self.parent.bridge_getResponse()
                    # propagate child's changes in args to master
                    for idxArg, tmpArg in enumerate(args):
                        self.copyChanges(tmpArg, newArgs[idxArg])
                    # propagate child's changes in keywords to master
                    for tmpKey in keywords:
                        tmpArg = keywords[tmpKey]
                        self.copyChanges(tmpArg, newKeywords[tmpKey])
                    # return
                    return retVal
                except Exception:
                    errType, errValue = sys.exc_info()[:2]
                    _logger.error(f"master {self.pid} method {self.name} failed : {errType} {errValue}")
                    # reconnect when socket has a problem
                    if errType not in [socket.error, socket.timeout]:
                        # kill old child process
                        self.parent.bridge_killChild()
                        _logger.error(f"master {self.pid} killed child")
                        # raise errType,errValue
                    # sleep
                    time.sleep(5)
                    # reconnect
                    try:
                        _logger.debug(f"master {self.pid} trying to reconnect")
                        is_ok = self.parent.connect()
                        if is_ok:
                            _logger.debug(f"master {self.pid} reconnect completed")
                        else:
                            _logger.debug(f"master {self.pid} reconnect failed. sleep")
                            time.sleep(120)
                    except Exception:
                        _logger.error(f"master {self.pid} connect failed. sleep")
                        time.sleep(120)

    # get atter for cursor attributes
    def __getattribute__(self, name):
        if object.__getattribute__(self, "isMaster"):
            try:
                # return original attribute
                return object.__getattribute__(self, name)
            except Exception:
                # append methods
                if (
                    not name.startswith("_")
                    and hasattr(DBProxy.DBProxy, name)
                    and isinstance(
                        getattr(DBProxy.DBProxy, name),
                        (types.MethodType, types.FunctionType),
                    )
                ):
                    # get DBProxy's method wrapper
                    method = ConBridge.bridge_masterMethod(name, self)
                    # set method
                    setattr(self, name, method)
                    # return
                    return method
        # return original attribute for child
        return object.__getattribute__(self, name)
