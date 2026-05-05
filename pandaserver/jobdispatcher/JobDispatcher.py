"""
dispatch jobs

"""

import datetime
import json
import os
import re
import socket
import sys
import threading
import time
import traceback
from threading import Lock

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.adder_gen import AdderGen
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

    # get special parameters for dispatcher
    def get_special_dispatch_params(self):
        """
        Wrapper function around taskBuffer.get_special_dispatch_params to convert list to set since task buffer cannot return set
        """
        param = self.taskBuffer.get_special_dispatch_params()
        for client_name in param["tokenKeys"]:
            param["tokenKeys"][client_name]["fullList"] = set(param["tokenKeys"][client_name]["fullList"])
        return True, param

    # set user proxy
    def set_user_proxy(self, response, distinguished_name=None, role=None, tokenized=False) -> tuple[bool, str]:
        """
        Set user proxy to the response

        :param response: response object
        :param distinguished_name: the distinguished name of the user
        :param role: the role of the user
        :param tokenized: whether the response should contain a token instead of a proxy

        :return: a tuple containing a boolean indicating success and a message
        """
        try:
            if distinguished_name is None:
                distinguished_name = response.data["prodUserID"]
            # remove redundant extensions
            distinguished_name = CoreUtils.get_bare_dn(distinguished_name, keep_digits=False)
            if not tokenized:
                # get proxy
                output = self.proxy_cacher.retrieve(distinguished_name, role=role)
            else:
                # get token
                output = self.token_cacher.get_access_token(distinguished_name)
            # not found
            if output is None:
                tmp_msg = f"""{"token" if tokenized else "proxy"} not found for {distinguished_name}"""
                response.appendNode("errorDialog", tmp_msg)
                return False, tmp_msg
            # set
            response.appendNode("userProxy", output)
            return True, ""
        except Exception as e:
            tmp_msg = f"""{"token" if tokenized else "proxy"} retrieval failed with {str(e)}"""
            response.appendNode("errorDialog", tmp_msg)
            return False, tmp_msg

    # get site mapper
    def getSiteMapper(self):
        return True, SiteMapper(self.taskBuffer)

    # get proxy
    def get_proxy(self, real_distinguished_name: str, role: str | None, target_distinguished_name: str | None, tokenized: bool, token_key: str | None) -> dict:
        """
        Get proxy for a user with a role

        :param real_distinguished_name: actual distinguished name of the user
        :param role: role of the user
        :param target_distinguished_name: target distinguished name if the user wants to get proxy for someone else.
                                          This is one of client_name defined in token_cache_config when getting a token
        :param tokenized: whether the response should contain a token instead of a proxy
        :param token_key: key to get the token from the token cache

        :return: response in dictionary
        """
        if target_distinguished_name is None:
            target_distinguished_name = real_distinguished_name
        tmp_log = LogWrapper(_logger, f"get_proxy PID={os.getpid()}")
        tmp_msg = f"""start DN="{real_distinguished_name}" role={role} target="{target_distinguished_name}" tokenized={tokenized} token_key={token_key}"""
        tmp_log.debug(tmp_msg)
        if real_distinguished_name is None:
            # cannot extract DN
            tmp_msg = "failed since DN cannot be extracted"
            tmp_log.debug(tmp_msg)
            response = Protocol.Response(Protocol.SC_Perms, "Cannot extract DN from proxy. not HTTPS?")
        else:
            # get compact DN
            compact_name = CoreUtils.clean_user_id(real_distinguished_name)
            # check permission
            self.specialDispatchParams.update()
            if "allowProxy" not in self.specialDispatchParams:
                allowed_names = []
            else:
                allowed_names = self.specialDispatchParams["allowProxy"]
            if compact_name not in allowed_names:
                # permission denied
                tmp_msg = f"failed since '{compact_name}' not in the authorized user list who have 'p' in {panda_config.schemaMETA}.USERS.GRIDPREF "
                if not tokenized:
                    tmp_msg += "to get proxy"
                else:
                    tmp_msg += "to get access token"
                tmp_log.debug(tmp_msg)
                response = Protocol.Response(Protocol.SC_Perms, tmp_msg)
            elif (
                tokenized
                and target_distinguished_name in self.token_cache_config
                and self.token_cache_config[target_distinguished_name].get("use_token_key") is True
                and (
                    target_distinguished_name not in self.specialDispatchParams["tokenKeys"]
                    or token_key not in self.specialDispatchParams["tokenKeys"][target_distinguished_name]["fullList"]
                )
            ):
                # invalid token key
                tmp_msg = f"failed since token key is invalid for {target_distinguished_name}"
                tmp_log.debug(tmp_msg)
                response = Protocol.Response(Protocol.SC_Invalid, tmp_msg)
            else:
                # get proxy
                response = Protocol.Response(Protocol.SC_Success, "")
                tmp_status, tmp_msg = self.set_user_proxy(response, target_distinguished_name, role, tokenized)
                if not tmp_status:
                    tmp_log.debug(tmp_msg)
                    response.appendNode("StatusCode", Protocol.SC_ProxyError)
                else:
                    tmp_msg = "successful sent proxy"
                    tmp_log.debug(tmp_msg)
        # return
        return response.encode(True)

    # get active job attribute
    def getActiveJobAttributes(self, pandaID, attrs):
        return self.taskBuffer.getActiveJobAttributes(pandaID, attrs)


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


# check role
def _checkRole(fqans, dn, withVomsPatch=True):
    production_manager = False
    try:
        # VOMS attributes of production and pilot roles
        production_attributes = [
            "/atlas/usatlas/Role=production",
            "/atlas/usatlas/Role=pilot",
            "/atlas/Role=production",
            "/atlas/Role=pilot",
            "/osg/Role=pilot",
            "^/[^/]+/Role=production$",
            "/ams/Role=pilot",
            "/Engage/LBNE/Role=pilot",
        ]
        if withVomsPatch:
            # FIXME once http://savannah.cern.ch/bugs/?47136 is solved
            production_attributes += ["/atlas/", "/osg/", "/cms/", "/ams/", "/Engage/LBNE/"]

        for fqan in fqans:
            # check atlas/usatlas production role
            if any(fqan.startswith(role_pattern) or re.search(role_pattern, fqan) for role_pattern in production_attributes):
                production_manager = True
                break

            # escape
            if production_manager:
                break

        # check DN with pilot owners
        if not production_manager and dn not in [None]:
            for owner in set(panda_config.production_dns).union(panda_config.pilot_owners):
                if owner and re.search(owner, dn) is not None:
                    production_manager = True
                    break

    except Exception:
        pass

    return production_manager


# get DN
def _getDN(req):
    realDN = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        realDN = req.subprocess_env["SSL_CLIENT_S_DN"]
        # remove redundant CN
        realDN = CoreUtils.get_bare_dn(realDN, keep_proxy=True)
    # return
    return realDN


"""
web service interface

"""


def checkPilotPermission(req):
    """
    This function retrieves the distinguished name (DN) and Fully Qualified Attribute Names (FQANs) from the request,
    checks if the user has a production role, and verifies the DN.

    Args:
        req: The request object containing the environment variables.
    Returns:
        tuple: A tuple containing a boolean indicating success and a message.
    """
    # get DN
    real_dn = _getDN(req)
    if real_dn is None:
        return False, "failed to retrieve DN"

    # get FQANs and check production role
    fqans = _getFQAN(req)
    prod_manager = _checkRole(fqans, real_dn, True)
    if not prod_manager:
        return False, "production or pilot role is required"

    return True, None
