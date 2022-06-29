"""
pool for DBProxies

"""

try:
    from Queue import Queue
except ImportError:
    from queue import Queue
from pandaserver.taskbuffer import OraDBProxy as DBProxy
import os
import time
import random
from threading import Lock
from pandaserver.config import panda_config
from pandaserver.taskbuffer.ConBridge import ConBridge
from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('DBProxyPool')

class DBProxyPool:
    
    def __init__(self,dbhost,dbpasswd,nConnection,useTimeout=False,dbProxyClass=None):
        # crate lock for callers
        self.lock = Lock()
        self.callers = []
        # create Proxies
        _logger.debug("init")
        self.proxyList = Queue(nConnection)
        self.connList = []
        for i in range(nConnection):
            _logger.debug("connect -> %s " % i)
            if dbProxyClass is not None:
                proxy = dbProxyClass()
            elif useTimeout and hasattr(panda_config,'usedbtimeout') and \
                   panda_config.usedbtimeout is True:
                proxy = ConBridge()
            else:
                proxy = DBProxy.DBProxy()
                self.connList.append(proxy)
            iTry = 0    
            while True:
                if proxy.connect(dbhost,dbpasswd,dbtimeout=60):
                    break
                iTry += 1
                _logger.debug("failed -> %s : try %s" % (i,iTry))
                time.sleep(random.randint(60,90))
            self.proxyList.put(proxy)
            time.sleep(1)
        # get PID    
        self.pid = os.getpid()    
        _logger.debug("ready")

    # return a free proxy. this method blocks until a proxy is available
    def getProxy(self):
        # get proxy
        proxy = self.proxyList.get()
        # wake up connection
        proxy.wakeUp()
        # return
        return proxy

    # put back a proxy
    def putProxy(self,proxy):
        self.proxyList.put(proxy)

    # cleanup
    def cleanup(self):
        _logger.debug("cleanup start")
        [c.cleanup() for c in self.connList]
        _logger.debug("cleanup done")
