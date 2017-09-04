import os
import re
import sys
import time
import random
import datetime
import commands
from taskbuffer.TaskBuffer import taskBuffer
from brokerage import SiteMapper

# password
from config import panda_config
passwd = panda_config.dbpasswd

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

siteMapper = SiteMapper.SiteMapper(taskBuffer)

#x = siteMapper.getSite('BNL_ATLAS_1')
#print x


