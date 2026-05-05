"""
dispatch jobs

"""

import datetime
import json
import threading
from threading import Lock

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.jobdispatcher import Protocol
from pandaserver.proxycache import panda_proxy_cache, token_cache
from pandaserver.srvcore import CoreUtils

# logger
_logger = PandaLogger().getLogger("JobDispatcher")
_pilotReqLogger = PandaLogger().getLogger("PilotRequests")


# a wrapper to install timeout into a method
class _TimedMethod:
    def __init__(self, method, timeout):
        self.method = method
        self.timeout = timeout
        self.result = Protocol.TimeOutToken

    # method emulation
    def __call__(self, *var):
        self.result = self.method(*var)

    # run
    def run(self, *var):
        thr = threading.Thread(target=self, args=var)
        thr.start()
        thr.join()


# job dispatcher
class JobDispatcher:
    # constructor
    def __init__(self):
        # taskbuffer
        self.taskBuffer = None
        # datetime of last updated
        self.lastUpdated = naive_utcnow()
        # how frequently update DN/token map
        self.timeInterval = datetime.timedelta(seconds=180)
        # special dispatcher parameters
        self.specialDispatchParams = None
        # site mapper cache
        self.siteMapperCache = None
        # lock
        self.lock = Lock()
        # proxy cacher
        self.proxy_cacher = panda_proxy_cache.MyProxyInterface()
        # token cacher
        self.token_cacher = token_cache.TokenCache()
        # config of token cacher
        try:
            with open(panda_config.token_cache_config) as f:
                self.token_cache_config = json.load(f)
        except Exception:
            self.token_cache_config = {}

    # set task buffer
    def init(self, taskBuffer):
        # lock
        self.lock.acquire()
        # set TB
        if self.taskBuffer is None:
            self.taskBuffer = taskBuffer
        # special dispatcher parameters
        if self.specialDispatchParams is None:
            self.specialDispatchParams = CoreUtils.CachedObject("dispatcher_params", 60 * 10, self.get_special_dispatch_params, _logger)
        # site mapper cache
        if self.siteMapperCache is None:
            self.siteMapperCache = CoreUtils.CachedObject("site_mapper", 60 * 10, self.getSiteMapper, _logger)
        # release
        self.lock.release()

    # get site mapper
    def getSiteMapper(self):
        return True, SiteMapper(self.taskBuffer)


# Singleton
jobDispatcher = JobDispatcher()
del JobDispatcher


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmp_key in req.subprocess_env:
        tmp_value = req.subprocess_env[tmp_key]
        # Scan VOMS attributes
        # compact style
        if tmp_key.startswith("GRST_CRED_") and tmp_value.startswith("VOMS"):
            fqan = tmp_value.split()[-1]
            fqans.append(fqan)

        # old style
        elif tmp_key.startswith("GRST_CONN_"):
            tmp_items = tmp_value.split(":")
            if len(tmp_items) == 2 and tmp_items[0] == "fqan":
                fqans.append(tmp_items[-1])

    return fqans


# get DN
def _getDN(req):
    realDN = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        realDN = req.subprocess_env["SSL_CLIENT_S_DN"]
        # remove redundant CN
        realDN = CoreUtils.get_bare_dn(realDN, keep_proxy=True)
    # return
    return realDN
