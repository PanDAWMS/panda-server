import re

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.proxycache import panda_proxy_cache
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

# logger
_logger = PandaLogger().getLogger("panda_activeusers_query")
tmpLog = LogWrapper(_logger)

if __name__ == "__main__":
    tmpLog.debug("================= start ==================")

    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
    taskBuffer.init(
        panda_config.dbhost,
        panda_config.dbpasswd,
        nDBConnection=1,
        requester=requester_id,
    )

    # instantiate MyProxy I/F
    my_proxy_interface_instance = panda_proxy_cache.MyProxyInterface()

    # roles
    if hasattr(panda_config, "proxy_cache_roles"):
        roles = panda_config.proxy_cache_roles.split(",")
    else:
        roles = ["atlas", "atlas:/atlas/Role=production", "atlas:/atlas/Role=pilot"]
    # get users
    sql = "select distinct DN FROM ATLAS_PANDAMETA.users WHERE GRIDPREF LIKE :patt"
    varMap = {}
    varMap[":patt"] = "%p%"
    tmpStat, tmpRes = taskBuffer.querySQLS(sql, varMap)
    for (realDN,) in tmpRes:
        if realDN is None:
            continue
        realDN = re.sub("/CN=limited proxy", "", realDN)
        realDN = re.sub("(/CN=proxy)+", "", realDN)
        realDN = re.sub("(/CN=\d+)+$", "", realDN)
        # check proxy
        tmpLog.debug(f"check proxy cache for DN={realDN}")
        for role in roles:
            my_proxy_interface_instance.checkProxy(realDN, role=role)
    tmpLog.debug("done")
