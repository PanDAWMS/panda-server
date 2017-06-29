import os
import re
import sys
import time
import glob
import fcntl
import json
import random
import datetime

from taskbuffer.TaskBuffer import taskBuffer
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper

# password
from config import panda_config
passwd = panda_config.dbpasswd

# logger
_logger = PandaLogger().getLogger('harvesterCtl')

tmpLog = LogWrapper(_logger,None)

tmpLog.debug("===================== start =====================")

# overall timeout value
overallTimeout = 20

# grace period
try:
    gracePeriod = int(sys.argv[1])
except:
    gracePeriod = 3

# kill old process
try:
    # time limit
    timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=overallTimeout)
    # get process list
    scriptName = sys.argv[0]
    out = commands.getoutput('env TZ=UTC ps axo user,pid,lstart,args | grep %s' % scriptName)
    for line in out.split('\n'):
        items = line.split()
        # owned process
        if not items[0] in ['sm','atlpan','pansrv','root']: # ['os.getlogin()']: doesn't work in cron
            continue
        # look for python
        if re.search('python',line) == None:
            continue
        # PID
        pid = items[1]
        # start time
        timeM = re.search('(\S+\s+\d+ \d+:\d+:\d+ \d+)',line)
        startTime = datetime.datetime(*time.strptime(timeM.group(1),'%b %d %H:%M:%S %Y')[:6])
        # kill old process
        if startTime < timeLimit:
            tmpLog.debug("old process : %s %s" % (pid,startTime))
            tmpLog.debug(line)            
            commands.getoutput('kill -9 %s' % pid)
except:
    type, value, traceBack = sys.exc_info()
    tmpLog.error("kill process : %s %s" % (type,value))

    
# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

refreshInterval = 3
lockIntervalForRefresh = 3
lockIntervalForSet = 10

# send command to refresh worker stats
taskBuffer.requestRefreshWorkerStats(refreshInterval, lockIntervalForRefresh)

# get sites to set nWorkers
items = taskBuffer.getSitesToSetNumWorkers(refreshInterval, lockIntervalForSet)
allSites = set()
for harvester_ID, siteName, workerStats in items:
    # only one instance per site in a single cycle
    if siteName in allSites:
        continue
    allSites.add(siteName)
    # get nWorkers from global share
    # FIXME
    nWorkersToSubmit = {'SCORE': 3,
                        'MCORE': 5}
    # update stat
    params = dict()
    for rType, cnt in nWorkersToSubmit.iteritems():
        params[rType] = {'to_submit': cnt}
    taskBuffer.reportWorkerStats(harvester_ID, siteName, json.dumps(params))
    # send command
    com = 'SET_N_WORKERS:{0}'.format(siteName)
    taskBuffer.commandToHarvester(harvester_ID, com, 0, 'new', None, refreshInterval, nWorkersToSubmit)

tmpLog.debug("===================== end =====================")
