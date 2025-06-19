import sys

from pandaserver.config import panda_config
from pandaserver.dataservice.activator import Activator
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)

d = taskBuffer.queryDatasetWithMap({"name": sys.argv[1]})

Activator(taskBuffer, d, enforce=True).run()
