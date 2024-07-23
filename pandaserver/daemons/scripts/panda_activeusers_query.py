import sys

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.proxycache import panda_proxy_cache, token_cache
from pandaserver.srvcore import CoreUtils

# logger
_logger = PandaLogger().getLogger("activeusers_query")


# main
def main(tbuf=None, **kwargs):
    # logger
    tmpLog = LogWrapper(_logger)
    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    tmpLog.debug("================= start ==================")
    # instantiate TB
    if tbuf is None:
        tmpLog.debug("Getting new connection")
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer

        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            useTimeout=True,
            requester=requester_id,
        )
        tmpLog.debug("Getting new connection - done")
    else:
        tmpLog.debug("Reusing connection")
        taskBuffer = tbuf

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
    tmpLog.debug("Querying users")
    tmpStat, tmpRes = taskBuffer.querySQLS(sql, varMap)
    tmpLog.debug("Querying done")
    for (realDN,) in tmpRes:
        if realDN is None:
            continue
        realDN = CoreUtils.get_bare_dn(realDN, keep_digits=False)
        name = taskBuffer.cleanUserID(realDN)
        # check proxy
        tmpLog.debug(f"check proxy cache for {name}")
        for role in roles:
            my_proxy_interface_instance.checkProxy(realDN, role=role, name=name)

    # instantiate Token Cache
    tmpLog.debug("Token Cache start")
    token_cacher = token_cache.TokenCache(task_buffer=taskBuffer)
    token_cacher.run()
    tmpLog.debug("Token Cache done")

    # stop taskBuffer if created inside this script
    if tbuf is None:
        taskBuffer.cleanup(requester=requester_id)
        tmpLog.debug("Stopped the new connection")
    tmpLog.debug("done")


# run
if __name__ == "__main__":
    main()
