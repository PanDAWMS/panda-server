import os
import re
import sys
import time
import math
import fcntl
import json
import socket
import random
import datetime
import commands

from taskbuffer.TaskBuffer import taskBuffer
from taskbuffer.WorkerSpec import WorkerSpec
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
lockInterval = 3
activeInterval = 60
setInterval = 3

pid = '{0}-{1}_{2}'.format(socket.getfqdn().split('.')[0],os.getpid(),os.getpgrp())

# get minimal resource
minResourceName = taskBuffer.getMinimalResource()

# get active harvesters
harvesterIDs = taskBuffer.getActiveHarvesters(activeInterval)
random.shuffle(harvesterIDs)

# loop over all harvesters
for harvesterID in harvesterIDs:
    # get lock to send REPORT_WORKER_STATS command
    com = 'REPORT_WORKER_STATS'
    tmpLog.debug('locking for com={0} id={1}'.format(com, harvesterID))
    locks = taskBuffer.getCommandLocksHarvester(harvesterID, com, pid, lockInterval, refreshInterval)
    # send command to refresh worker stats
    for siteName, resourceTypes in locks.iteritems():
        comSite = '{0}:{1}'.format(com, siteName)
        tmpLog.debug('sending {0} to {1}'.format(comSite, harvesterID))
        taskBuffer.commandToHarvester(harvesterID, comSite, 0, 'new', None, None, None)    
        # release lock
        for resourceType in resourceTypes:
            taskBuffer.releaseCommandLockHarvester(harvesterID, com, siteName, resourceType, pid)

    # get lock to send SET_N_WORKERS command
    com = 'SET_N_WORKERS_JOBTYPE'
    tmpLog.debug('locking for com={0} id={1}'.format(com, harvesterID))
    locks = taskBuffer.getCommandLocksHarvester(harvesterID, com, pid, lockInterval, setInterval)
    # send command to set nWorkers
    for siteName, resourceTypes in locks.iteritems():
        # calcurate the number of new workers
        # FIXME : using nActivated and workerStats for now, to be given by global share
        nActivatedStats = taskBuffer.getActivatedJobStatisticsPerResource(siteName)
        workerStats, nPilot = taskBuffer.getWorkerStats(siteName)
        nWorkersToSubmit = dict()
        for resourceType in resourceTypes:
            # number of incarnated workers
            nIncarnated = 0
            nHarvester = 1
            if resourceType in workerStats:
                nHarvester = workerStats[resourceType]['nInstances']
                for status in ['running', 'submitted']:
                    if status in workerStats[resourceType]['stats']:
                        nIncarnated += workerStats[resourceType]['stats'][status]
            # number of activated jobs
            nActivated = 0
            if resourceType in nActivatedStats:
                nActivated = nActivatedStats[resourceType]
            elif resourceType == WorkerSpec.RT_catchall:
                # sum if catchall
                for tmpVal in nActivatedStats[resourceType].values():
                    nActivated += tmpVal
            # number of new workers per instance
            nWorkers = int(math.ceil(float(nActivated) / float(nHarvester)))
            if nWorkers > 0:
                nWorkersToSubmit[resourceType] = nWorkers
            else:
                # to send dummy getJob with minimal config when the site is inactive
                if nPilot == 0 and nIncarnated == 0 and resourceType == minResourceName:
                    nWorkersToSubmit[resourceType] = 1
            tmpLog.debug('site={0} resource={1} nWorkers={2} nInc={3} nAct={4} nPilot={5}'.format(siteName, resourceType,
                                                                                                  nWorkers, nIncarnated,
                                                                                                  nActivated, nPilot))
        # send command
        if len(nWorkersToSubmit) > 0:
            comSite = '{0}:{1}'.format(com, siteName)
            tmpLog.debug('sending {0} with {1} to {2}'.format(comSite, str(nWorkersToSubmit), harvesterID))
            taskBuffer.commandToHarvester(harvesterID, comSite, 0, 'new', None, None, nWorkersToSubmit)
        # release lock
        for resourceType in resourceTypes:
            taskBuffer.releaseCommandLockHarvester(harvesterID, com, siteName, resourceType, pid)

tmpLog.debug("===================== end =====================")
