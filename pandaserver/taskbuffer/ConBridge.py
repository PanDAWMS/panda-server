import os
import sys
import time
import types
import socket
import signal
import random
import threading
import traceback
try:
    import cPickle as pickle
except ImportError:
    import pickle

from pandaserver.taskbuffer import OraDBProxy as DBProxy

from pandaserver.config      import panda_config
from pandaserver.taskbuffer.JobSpec     import JobSpec
from pandaserver.taskbuffer.FileSpec    import FileSpec
from pandaserver.taskbuffer.DatasetSpec import DatasetSpec
from pandacommon.pandalogger.PandaLogger import PandaLogger

try:
    long
except NameError:
    long = int

# logger
_logger = PandaLogger().getLogger('ConBridge')


# exception for normal termination
class HarmlessEx(Exception):
    pass
    

# terminate child process by itself when master has gone
class Terminator (threading.Thread):

    # constructor
    def __init__(self,consock):
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
        _logger.debug("child  %s received termination" % pid)
        # kill 
        try:
            os.kill(pid,signal.SIGTERM)
        except Exception:
            pass
        try:
            os.kill(pid,signal.SIGKILL)
        except Exception:
            pass


    
# connection bridge with with timeout
class ConBridge (object):

    # constructor
    def __init__(self):
        self.child_pid = 0
        self.isMaster  = False
        self.mysock    = None
        self.consock   = None
        self.pid       = os.getpid()
        # timeout
        if hasattr(panda_config,'dbtimeout'):
            self.timeout = int(panda_config.dbtimeout)
        else:
            self.timeout = 600
        # verbose    
        if hasattr(panda_config,'dbbridgeverbose'):
            self.verbose = panda_config.dbbridgeverbose
        else:
            self.verbose = False


    # destructor
    def __del__(self):
        # kill old child process
        self.bridge_killChild()

            
    # connect
    def connect(self,dbhost=panda_config.dbhost,dbpasswd=panda_config.dbpasswd,
                dbuser=panda_config.dbuser,dbname=panda_config.dbname,
                dbtimeout=None,reconnect=False):
        # kill old child process
        self.bridge_killChild()
        _logger.debug('master %s connecting' % self.pid)
        # reset child PID and sockets
        self.child_pid = 0     
        self.mysock    = None
        self.consock   = None
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
            self.mysock  = datpair[1]
            self.consock = conpair[1]
            datpair[0].close()
            conpair[0].close()
            # connect to database
            _logger.debug('child  %s connecting to database' % self.pid)            
            self.proxy = DBProxy.DBProxy()
            if not self.proxy.connect(dbhost=dbhost,dbpasswd=dbpasswd,dbtimeout=60):
                _logger.error('child  %s failed to connect' % self.pid)
                # send error
                self.bridge_sendError((RuntimeError,'child  %s connection failed' % self.pid))
                # exit
                self.bridge_childExit()
            # send OK just for ACK
            _logger.debug('child  %s connection is ready' % self.pid)
            self.bridge_sendResponse(None)
            # start terminator
            Terminator(self.consock).start()
            # go main loop
            _logger.debug('child  %s going into the main loop' % self.pid)
            self.bridge_run()
            # exit
            self.bridge_childExit(0)
        else:
            # master
            self.isMaster = True
            # keep socket
            self.mysock  = datpair[0]
            self.consock = conpair[0]
            datpair[1].close()
            conpair[1].close()
            try:
                # get ACK
                _logger.debug('master %s waiting ack from child=%s' % (self.pid,self.child_pid))
                self.bridge_getResponse()
                _logger.debug('master %s got ready from child=%s' % (self.pid,self.child_pid))
                return True
            except Exception as e:
                _logger.error('master %s failed to setup child=%s : %s %s' % \
                              (self.pid, self.child_pid, str(e), traceback.format_exc()))
                # kill child
                self.bridge_killChild()
                return False

            

    #######################        
    # communication methods
    
    # send packet
    def bridge_send(self,val):
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
                roleType = 'master'
            else:
                roleType = 'child '                
            _logger.error('%s %s send error : val=%s - %s %s' % \
                          (roleType, self.pid, str(val), str(e),
                           traceback.format_exc()))
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
                        raise socket.error('empty packet')
                    else:
                        # master closed socket
                        raise HarmlessEx('empty packet')
                if strSize is None:
                    strSize = tmpStr
                else:
                    strSize += tmpStr
            if strSize is None:
                strSize = ''
            else:
                strSize = strSize.decode()
            # get body
            strBody = None
            bodySize = long(strSize)
            while strBody is None or len(strBody) < bodySize:
                if strBody is None:
                    tmpSize = bodySize
                else:
                    tmpSize = bodySize - len(strBody)
                tmpStr = self.mysock.recv(tmpSize)
                if len(tmpStr) == 0:
                    if self.isMaster:
                        raise socket.error('empty packet')
                    else:
                        # master closed socket
                        raise HarmlessEx('empty packet')
                if strBody is None:
                    strBody = tmpStr
                else:
                    strBody += tmpStr
            if strBody is None:
                strBody = ''.encode()
            # set timeout back
            if self.isMaster:
                self.mysock.settimeout(None)
            # deserialize
            retVal = pickle.loads(strBody)
            return True,retVal
        except Exception as e:
            if self.isMaster:
                roleType = 'master'
            else:
                roleType = 'child '
            if type(e) == HarmlessEx:
                _logger.debug('%s %s recv harmless ex : %s' % \
                              (roleType, self.pid, e.message))
            else:
                _logger.error('%s %s recv error : %s %s' % \
                              (roleType, self.pid, str(e),
                               traceback.format_exc()))
            # terminate child            
            if not self.isMaster:
                self.bridge_childExit()
            raise e
                                    
    

    #######################        
    # child's methods

    # send error
    def bridge_sendError(self,val):
        # send status
        self.bridge_send("NG")
        # check if pickle-able
        try:
            pickle.dumps(val, protocol=0)
        except Exception:
            # use RuntimeError
            val = (RuntimeError,str(val[-1]))
        # send exceptions
        self.bridge_send(val)


    # send response
    def bridge_sendResponse(self,val):
        # send status
        self.bridge_send("OK")
        # send response
        self.bridge_send(val)


    # termination of child
    def bridge_childExit(self,exitCode=1):
        if not self.isMaster:
            _logger.debug("child  %s closing sockets" % self.pid)
            # close sockets
            try:
                self.mysock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.consock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            # exit
            _logger.debug("child  %s going to exit" % self.pid)            
            os._exit(exitCode)
            

    # child main    
    def bridge_run(self):
        comStr = ''
        while True:
            try:
                # get command
                status,comStr = self.bridge_recv()
                if not status:
                    raise RuntimeError('invalid command')
                # get variables
                status,variables = self.bridge_recv()
                if not status:
                    raise RuntimeError('invalid variables')
            except Exception:
                errType,errValue = sys.exc_info()[:2]
                _logger.error('child  %s died : %s %s' % (self.pid,errType,errValue))
                # exit
                self.bridge_childExit()
            if self.verbose:    
                _logger.debug('child  %s method %s executing' % (self.pid,comStr))
            try:    
                # execute
                method = getattr(self.proxy,comStr)
                res = method(*variables[0], **variables[1])
                # FIXME : modify response since cx_Oracle types cannot be picked
                if comStr in ['querySQLS']:
                    newRes = [True]+res[1:]
                    res = newRes
                if self.verbose:    
                    _logger.debug('child  %s method %s completed' % (self.pid,comStr))
                # return
                self.bridge_sendResponse((res,variables[0],variables[1]))
            except Exception:
                errType,errValue = sys.exc_info()[:2]
                _logger.error('child  %s method %s failed : %s %s' % (self.pid,comStr,errType,errValue))
                if errType in [socket.error,socket.timeout]:
                    _logger.error('child  %s died : %s %s' % (self.pid,errType,errValue))
                    # exit
                    self.bridge_childExit()
                # send error
                self.bridge_sendError((errType,errValue))
                        


    #######################        
    # master's methods

    # kill child
    def bridge_killChild(self):
        # kill old child process
        if self.child_pid != 0:
            # close sockets
            _logger.debug('master %s closing sockets for child=%s' % (self.pid,self.child_pid))
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
            _logger.debug('master %s killing child=%s' % (self.pid,self.child_pid))
            # send SIGTERM
            try:
                os.kill(self.child_pid,signal.SIGTERM)
            except Exception:
                pass
            time.sleep(2)
            # send SIGKILL
            try:
                os.kill(self.child_pid,signal.SIGKILL)
            except Exception:
                pass
            # wait for completion of child
            _logger.debug('master %s waiting child=%s' % (self.pid,self.child_pid))
            try:
                os.waitpid(self.child_pid,0)
            except Exception:
                pass
            # sleep to avoid burst reconnection
            time.sleep(random.randint(5,15))
            _logger.debug('master %s killed child=%s' % (self.pid,self.child_pid))

            
    # get responce
    def bridge_getResponse(self):
        # get status
        status,strStatus = self.bridge_recv()
        if not status:
            raise RuntimeError('master %s got invalid status response from child=%s' % \
                  (self.pid,self.child_pid))
        if strStatus == 'OK':
            # return res
            status,ret = self.bridge_recv()
            if not status:
                raise RuntimeError('master %s got invalid response body from child=%s' % \
                      (self.pid,self.child_pid))
            return ret
        elif strStatus == 'NG':
            # raise error
            status,ret = self.bridge_recv()
            if not status:
                raise RuntimeError('master %s got invalid response value from child=%s' % \
                      (self.pid,self.child_pid))
            raise ret[0](ret[1])
        else:
            raise RuntimeError('master %s got invalid response from child=%s : %s' % \
                  (self.pid,self.child_pid,str(strStatus)))


    # method wrapper class
    class bridge_masterMethod:

        # constructor
        def __init__(self,name,parent):
            self.name   = name
            self.parent = parent
            self.pid = os.getpid()


        # copy changes in taskbuff objects to master
        def copyTbObjChanges(self,oldPar,newPar):
            # check they have the same type
            if type(oldPar) != type(newPar):
                return False
            # copy some Specs since they are passed via ref's 
            if isinstance(oldPar,JobSpec) or isinstance(oldPar,FileSpec) \
                   or isinstance(oldPar,DatasetSpec):
                if hasattr(oldPar,'__getstate__'):
                    tmpStat = newPar.__getstate__()
                    oldPar.__setstate__(tmpStat)
                else:
                    tmpStat = newPar.values()
                    oldPar.pack(tmpStat)
                return True
            # copy Datasets
            return False

        
        # copy changes in objects to master
        def copyChanges(self,oldPar,newPar):
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
                self.copyTbObjChanges(oldPar,newPar)


        # method emulation    
        def __call__(self,*args,**keywords):
            while True:
                try:
                    # send command name
                    self.parent.bridge_send(self.name)
                    # send variables
                    self.parent.bridge_send((args,keywords))
                    # get response
                    retVal,newArgs,newKeywords = self.parent.bridge_getResponse()
                    # propagate child's changes in args to master
                    for idxArg,tmpArg in enumerate(args):
                        self.copyChanges(tmpArg,newArgs[idxArg])
                    # propagate child's changes in keywords to master
                    for tmpKey in keywords:
                        tmpArg = keywords[tmpKey]
                        self.copyChanges(tmpArg,newKeywords[tmpKey])
                    # return
                    return retVal
                except Exception:
                    errType,errValue = sys.exc_info()[:2]
                    _logger.error('master %s method %s failed : %s %s' % \
                                  (self.pid,self.name,errType,errValue))
                    # reconnect when socket has a problem
                    if not errType in [socket.error,socket.timeout]:
                        # kill old child process
                        self.parent.bridge_killChild()
                        _logger.error('master %s killed child' % self.pid)
                        #raise errType,errValue
                    # sleep
                    time.sleep(5)
                    # reconnect
                    try:
                        _logger.debug('master %s trying to reconnect' % self.pid)
                        self.parent.connect()
                        _logger.debug('master %s reconnect completed' % self.pid)
                    except Exception:
                        _logger.error('master %s connect failed' % self.pid)

                    
    # get atter for cursor attributes
    def __getattribute__(self,name):
        if object.__getattribute__(self,'isMaster'):
            try:
                # return origianl attribute
                return object.__getattribute__(self,name)
            except Exception:
                # append methods
                if not name.startswith('_') and hasattr(DBProxy.DBProxy,name) and \
                       isinstance(getattr(DBProxy.DBProxy,name),
                                  (types.MethodType, types.FunctionType)):
                    # get DBProxy's method wrapper
                    method = ConBridge.bridge_masterMethod(name,self)
                    # set method
                    setattr(self,name,method)
                    # return
                    return method
        # return origianl attribute for child
        return object.__getattribute__(self,name)
