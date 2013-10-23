import time
import sys
import optparse

import userinterface.Client as Client

aSrvID = None

from taskbuffer.OraDBProxy import DBProxy
# password
from config import panda_config

optP = optparse.OptionParser(conflict_handler="resolve")
options,args = optP.parse_args()

jediTaskID = args[0]

s,o = Client.finishTask(jediTaskID)
print o

