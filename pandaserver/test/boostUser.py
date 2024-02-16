import sys

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config
from pandaserver.taskbuffer.Initializer import initializer
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

# initialize DB using dummy connection
initializer.init()

# logger
_logger = PandaLogger().getLogger("boostUser")
_logger.debug("================= start ==================")

# instantiate TB
requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, requester=requester_id)

user = sys.stdin.read()
user = user[:-1]

sql = "UPDATE atlas_panda.%s set currentPriority=:prio where prodUserName=:uname and prodSourceLabel IN (:label1,:label2) and currentPriority<:prio"
varMap = {}
varMap[":prio"] = 4000
varMap[":uname"] = user
varMap[":label1"] = "user"
varMap[":label2"] = "panda"
for table in ("jobsactive4", "jobsdefined4"):
    _logger.debug((sql % table) + str(varMap))
    ret = taskBuffer.querySQLS(sql % table, varMap)
    _logger.debug(f"ret -> {str(ret)}")

_logger.debug("================= end ==================")
