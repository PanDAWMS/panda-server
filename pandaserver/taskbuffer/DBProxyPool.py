"""
pool for DBProxies

"""

import Queue
import DBProxy
import time
import random

from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('DBProxyPool')

class DBProxyPool:
    
    def __init__(self,dbhost,dbpasswd,nConnection):
        # create Proxies
        _logger.debug("init")
        self.proxyList = Queue.Queue(nConnection)
        for i in range(nConnection):
            _logger.debug("connect -> %s " % i)            
            proxy = DBProxy.DBProxy()
            nTry = 100
            for iTry in range(nTry):
                if proxy.connect(dbhost,dbpasswd,dbtimeout=60):
                    break
                _logger.debug("failed -> %s : try %s" % (i,iTry))
                if iTry+1 == nTry:
                    raise RuntimeError, 'DBProxyPool.__init__ failed'
                time.sleep(random.randint(10,20))
            self.proxyList.put(proxy)
            time.sleep(1)
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
