import re
import sys

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.proxycache import panda_proxy_cache
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

# logger
_logger = PandaLogger().getLogger("activeusers_query")
tmp_log = LogWrapper(_logger)

if __name__ == "__main__":
    tmp_log.debug("================= start ==================")

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
    var_map = {":patt": "%p%"}
    _, results = taskBuffer.querySQLS(sql, var_map)

    # iterate over user DNs, clean them up and check proxy cache
    for (real_dn,) in results:
        if real_dn is None:
            continue
        real_dn = re.sub("/CN=limited proxy", "", real_dn)
        real_dn = re.sub("(/CN=proxy)+", "", real_dn)
        real_dn = re.sub("(/CN=\d+)+$", "", real_dn)

        # check proxy
        tmp_log.debug(f"check proxy cache for DN={real_dn}")
        for role in roles:
            my_proxy_interface_instance.checkProxy(real_dn, role=role)

    tmp_log.debug("done")
