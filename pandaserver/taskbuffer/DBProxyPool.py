"""
pool for DBProxies

"""

try:
    from Queue import Queue
except ImportError:
    from queue import Queue

import os
import random
import time
from threading import Lock

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.taskbuffer import OraDBProxy as DBProxy
from pandaserver.taskbuffer.ConBridge import ConBridge

# logger
_logger = PandaLogger().getLogger("DBProxyPool")


class DBProxyPool:
    def __init__(self, dbhost, dbpasswd, nConnection, useTimeout=False, dbProxyClass=None):
        # crate lock for callers
        self.lock = Lock()
        self.callers = []
        # create Proxies
        _logger.debug("init")
        self.proxyList = Queue(nConnection)
        self.connList = []
        for i in range(nConnection):
            _logger.debug(f"connect -> {i} ")
            if dbProxyClass is not None:
                proxy = dbProxyClass()
            elif useTimeout and hasattr(panda_config, "usedbtimeout") and panda_config.usedbtimeout is True:
                """
                ConBridge allows having database interactions in separate processes and killing them independently when interactions are stalled.
                This avoids clogged httpd processes due to stalled database accesses.
                """
                proxy = ConBridge()
            else:
                proxy = DBProxy.DBProxy()
                self.connList.append(proxy)
            iTry = 0
            while True:
                if proxy.connect(dbhost, dbpasswd, dbtimeout=60):
                    break
                iTry += 1
                _logger.debug(f"failed -> {i} : try {iTry}")
                time.sleep(random.randint(60, 90))
            self.proxyList.put(proxy)
            time.sleep(1)
        # get PID
        self.pid = os.getpid()
        _logger.debug("ready")

    # return a free proxy. this method blocks until a proxy is available
    def getProxy(self):
        # time how long it took to get a proxy
        start_time = time.time()

        # get proxy
        proxy = self.proxyList.get()
        # wake up connection
        proxy.wakeUp()

        end_time = time.time()
        elapsed_time = end_time - start_time
        _logger.debug(f"Getting proxy took: {elapsed_time} seconds")

        return proxy

    # put back a proxy
    def putProxy(self, proxy):
        self.proxyList.put(proxy)

    # cleanup
    def cleanup(self):
        _logger.debug("cleanup start")
        [c.cleanup() for c in self.connList]
        _logger.debug("cleanup done")
