"""
checking DB schema version for PanDA Server

"""

from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

from pandaserver.taskbuffer import PandaDBSchemaInfo

from packaging import version

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

sql = "select major || '.' || minor || '.' || patch from ATLAS_PANDA.pandadb_version where component = 'SERVER'"

res = proxyS.querySQL(sql)
dbVersion = res[0][0]

serverDBVersion = PandaDBSchemaInfo.PandaDBSchemaInfo().method()

if version.parse(dbVersion) >= version.parse(serverDBVersion):
    return ("True")
else:
    return ("False")