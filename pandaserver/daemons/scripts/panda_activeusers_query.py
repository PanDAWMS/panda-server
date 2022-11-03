import re

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.config import panda_config
from pandaserver.proxycache import panda_proxy_cache
from pandaserver.srvcore import CoreUtils


# logger
_logger = PandaLogger().getLogger('panda_activeusers_query')


# main
def main(tbuf=None, **kwargs):
    # logger
    tmpLog = LogWrapper(_logger)

    tmpLog.debug("================= start ==================")
    # instantiate TB
    if tbuf is None:
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1, useTimeout=True)
    else:
        taskBuffer = tbuf

    # instantiate MyProxy I/F
    my_proxy_interface_instance = panda_proxy_cache.MyProxyInterface()

    # roles
    if hasattr(panda_config,'proxy_cache_roles'):
        roles = panda_config.proxy_cache_roles.split(',')
    else:
        roles = ['atlas','atlas:/atlas/Role=production','atlas:/atlas/Role=pilot']
    # get users
    sql = 'select distinct DN FROM ATLAS_PANDAMETA.users WHERE GRIDPREF LIKE :patt'
    varMap = {}
    varMap[':patt'] = '%p%'
    tmpStat,tmpRes = taskBuffer.querySQLS(sql,varMap)
    for realDN, in tmpRes:
        if realDN is None:
            continue
        realDN = CoreUtils.get_bare_dn(realDN, keep_digits=False)
        name = taskBuffer.cleanUserID(realDN)
        # check proxy
        tmpLog.debug("check proxy cache for {}".format(name))
        for role in roles:
            my_proxy_interface_instance.checkProxy(realDN, role=role, name=name)
    tmpLog.debug("done")


# run
if __name__ == '__main__':
    main()
