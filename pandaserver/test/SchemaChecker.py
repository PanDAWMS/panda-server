"""
checking DB schema version for PanDA Server

"""

from pandaserver.config import panda_config
from pandaserver.taskbuffer.OraDBProxy import DBProxy

from pandaserver.taskbuffer import PandaDBSchemaInfo

#panda_config.dbhost = 'panda-postgres'
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost, panda_config.dbpasswd, panda_config.dbuser, panda_config.dbname)

sql = "select major || '.' || minor || '.' || patch from ATLAS_PANDA.pandadb_version where component = 'SCHEMA'"

res = proxyS.querySQL(sql)
dbVersion = res[0][0].split('.')

# covert string to list of ints to compare numbers
dbVersionIntegers = list(map(int, dbVersion))

minimumSchema = PandaDBSchemaInfo.PandaDBSchemaInfo().method()
serverDBVersion = minimumSchema.split('.')

# covert string to list of ints to compare numbers
serverDBVersionIntegers = list(map(int, serverDBVersion))

if dbVersionIntegers[0] == serverDBVersionIntegers[0] and dbVersionIntegers[1] == serverDBVersionIntegers[1]:
    if dbVersionIntegers[2] > serverDBVersionIntegers[2]:
        print ("False")
    else:
        print ("True")
else:
    print ("False")

