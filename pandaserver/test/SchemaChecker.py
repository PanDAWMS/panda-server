"""
Checking DB schema version for PanDA Server.
If there is an issue and the pandaEmailNotification var is
defined in panda_config, it will send an email notification.
"""

from packaging import version
from pandaserver.config import panda_config
from pandaserver.srvcore.MailUtils import MailUtils
from pandaserver.taskbuffer import PandaDBSchemaInfo
from pandaserver.taskbuffer.OraDBProxy import DBProxy

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

sql = "select major || '.' || minor || '.' || patch from ATLAS_PANDA.pandadb_version where component = 'SERVER'"

res = proxyS.querySQL(sql)
dbVersion = res[0][0]

serverDBVersion = PandaDBSchemaInfo.PandaDBSchemaInfo().method()

if version.parse(dbVersion) >= version.parse(serverDBVersion):
    print("DB schema check: OK")
else:
    msgBody = (
        "There is an issue with "
        + panda_config.pserveralias
        + ". PanDA DB schema installed is "
        + dbVersion
        + " while PanDA Server requires version "
        + serverDBVersion
        + " to be installed. Please check the official docs for instructions on how to upgrade the schema."
    )

    print(msgBody)
    if "pandaEmailNotification" in panda_config.__dict__:
        MailUtils().send(
            panda_config.pandaEmailNotification,
            "PanDA DB Version installed is not correct for " + panda_config.pserveralias,
            msgBody,
        )
