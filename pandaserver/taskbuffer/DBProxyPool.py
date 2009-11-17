"""
pool for DBProxies

"""

import Queue
import OraDBProxy as DBProxy
import os
import time
import random
from config import panda_config
from taskbuffer.ConBridge import ConBridge
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('DBProxyPool')

class DBProxyPool:
    
    def __init__(self,dbhost,dbpasswd,nConnection,useTimeout=False):
        # create Proxies
        _logger.debug("init")
        self.proxyList = Queue.Queue(nConnection)
        for i in range(nConnection):
            _logger.debug("connect -> %s " % i)
            if useTimeout and hasattr(panda_config,'usedbtimeout') and \
                   panda_config.usedbtimeout == True:
                proxy = ConBridge()
            else:
                proxy = DBProxy.DBProxy()
            iTry = 0    
            while True:
                if proxy.connect(dbhost,dbpasswd,dbtimeout=60):
                    break
                iTry += 1
                _logger.debug("failed -> %s : try %s" % (i,iTry))
                time.sleep(random.randint(10,20))
            self.proxyList.put(proxy)
            time.sleep(1)
        # get PID    
        self.pid = os.getpid()    
        _logger.debug("ready")            

    # return a free proxy. this method blocks until a proxy is available
    def getProxy(self):
        # get proxy
        _logger.debug("PID=%s getting proxy" % self.pid)
        proxy = self.proxyList.get()
        _logger.debug("PID=%s got proxy" % self.pid)        
        # wake up connection
        proxy.wakeUp()
        # return
        return proxy

    # put back a proxy
    def putProxy(self,proxy):
        self.proxyList.put(proxy)
