import sys
from pandaserver.config import panda_config

# initialize cx_Oracle using dummy connection
from pandaserver.taskbuffer.Initializer import initializer
initializer.init()

from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandacommon.pandalogger.PandaLogger import PandaLogger


# logger
_logger = PandaLogger().getLogger('boostUser')
_logger.debug("================= start ==================")

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

user = sys.stdin.read()
user = user[:-1]

sql = "UPDATE atlas_panda.%s set currentPriority=:prio where prodUserName=:uname and prodSourceLabel IN (:label1,:label2) and currentPriority<:prio"
varMap = {}
varMap[':prio'] = 4000
varMap[':uname'] = user
varMap[':label1'] = 'user'
varMap[':label2'] = 'panda'
for table in ('jobsactive4','jobsdefined4'):
	_logger.debug((sql % table) + str(varMap))
	ret = taskBuffer.querySQLS(sql % table,varMap)
	_logger.debug('ret -> %s' % str(ret))

_logger.debug("================= end ==================")
