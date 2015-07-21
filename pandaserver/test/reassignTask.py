import re
import sys
import time
import datetime
import argparse

from taskbuffer.OraDBProxy import DBProxy
# password
from config import panda_config

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

import userinterface.Client as Client

optP = argparse.ArgumentParser(conflict_handler="resolve",description='Reassign jobs in a task')
optP.add_argument('taskid',action='store',
                  metavar='TASKID',help='taskID of the task')
optP.add_argument('-m',dest='limit',type=int,action='store', default=60,
                    metavar='MIMUTES',help='time limit in minute')
optP.add_option('-9',action='store_const',const=True,dest='forceKill',
                default=False,help='kill jobs before next heartbeat is coming')
options = optP.parse_args()
taskid = options.taskid

print
print 'trying to reassign jobs with modificationTime < CURRENT-{0}min. Change the limit using -m if necessary'.format(options.limit)

codeV = 51
if options.forceKill:
    codeV = 9

jobs = []
jediJobs = []

timeLimit = datetime.datetime.utcnow() - datetime.timedelta(minutes=options.limit)
varMap = {}
varMap[':modificationTime'] = timeLimit
varMap[':taskID']    = taskid
sql = "SELECT PandaID,lockedby FROM ATLAS_PANDA.jobsDefined4 WHERE taskID=:taskID AND modificationTime<:modificationTime "
status,res = proxyS.querySQLS(sql,varMap)
if res != None:
    for (id,lockedby) in res:
        if lockedby == 'jedi':
            jediJobs.append(id)
        else:
            jobs.append(id)
            

varMap = {}
varMap[':js1']        = 'activated'
varMap[':js2']        = 'starting'
varMap[':modificationTime'] = timeLimit
varMap[':taskID']    = taskid
sql = "SELECT PandaID,lockedby FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus IN (:js1,:js2) AND taskID=:taskID AND modificationTime<:modificationTime "
status,res = proxyS.querySQLS(sql,varMap)
if res != None:
    for (id,lockedby) in res:
        if lockedby == 'jedi':
            jediJobs.append(id)
        else:
            jobs.append(id)

varMap = {}
varMap[':jobStatus']        = 'waiting'
varMap[':modificationTime'] = timeLimit
varMap[':taskID']    = taskid
sql = "SELECT PandaID,lockedby FROM ATLAS_PANDA.jobsWaiting4 WHERE jobStatus=:jobStatus AND taskID=:taskID AND modificationTime<:modificationTime "
status,res = proxyS.querySQLS(sql,varMap)
if res != None:
    for (id,lockedby) in res:
        if lockedby == 'jedi':
            jediJobs.append(id)
        else:
            jobs.append(id)

# reassign
jobs.sort()
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        print 'reassign  %s' % str(jobs[iJob:iJob+nJob])
        Client.reassignJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(10)

if len(jediJobs) != 0:
    nJob = 100
    iJob = 0
    while iJob < len(jediJobs):
        print 'kill JEDI jobs %s' % str(jediJobs[iJob:iJob+nJob])
        Client.killJobs(jediJobs[iJob:iJob+nJob],codeV)
        iJob += nJob

print
print 'reassigned {0} jobs'.format(len(jobs+jediJobs))


