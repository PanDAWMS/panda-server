"""
pool for LogDBProxies

"""

import time
import Queue
import random
import OraLogDBProxy as LogDBProxy
from config import panda_config

from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('LogDBProxyPool')

class LogDBProxyPool:
    
    def __init__(self,nConnection=panda_config.nLogDBConnection):
        # create Proxies
        _logger.debug("init")
        self.proxyList = Queue.Queue(nConnection)
        for i in range(nConnection): 
            _logger.debug("connect -> %s " % i)
            proxy = LogDBProxy.LogDBProxy()
            nTry = 10
            for iTry in range(nTry):
                if proxy.connect():
                    break
                _logger.debug("failed -> %s : try %s" % (i,iTry))
                if iTry+1 == nTry:
                    raise RuntimeError, 'LogDBProxyPool.__init__ failed'
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
        # put
        self.proxyList.put(proxy)

