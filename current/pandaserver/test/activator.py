import os
import re
import sys
import time
import datetime
import commands
from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from dataservice.Activator import Activator

# password
from config import panda_config
passwd = panda_config.dbpasswd

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

if len(sys.argv) != 2:
    print "datasetname is required"

dataset = taskBuffer.queryDatasetWithMap({'name':sys.argv[1]})
thr = Activator(taskBuffer,dataset)
thr.start()
thr.join()
