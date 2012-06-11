import time
import sys
import optparse

import userinterface.Client as Client

aSrvID = None

from taskbuffer.OraDBProxy import DBProxy
# password
from config import panda_config

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option('-9',action='store_const',const=True,dest='forceKill',
                default=False,help='kill jobs even if they are still running')
optP.add_option('--noRunning',action='store_const',const=True,dest='noRunning',
                default=False,help='kill only activated/assigned/waiting jobs')
options,args = optP.parse_args()

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

jobs = []

varMap = {}
varMap[':prodSourceLabel']  = 'managed'
varMap[':taskID'] = args[0]
if not options.noRunning:
    sql = "SELECT PandaID FROM %s WHERE prodSourceLabel=:prodSourceLabel AND taskID=:taskID ORDER BY PandaID"
else:
    sql = "SELECT PandaID FROM %s WHERE prodSourceLabel=:prodSourceLabel AND taskID=:taskID AND jobStatus<>:jobStatus ORDER BY PandaID"
    varMap[':jobStatus'] = 'running'
for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']:
    status,res = proxyS.querySQLS(sql % table,varMap)
    if res != None:
        for id, in res:
            if not id in jobs:
                jobs.append(id)

print 'The number of jobs to be killed : %s' % len(jobs)            
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        print 'kill %s' % str(jobs[iJob:iJob+nJob])
        if options.forceKill:
            Client.killJobs(jobs[iJob:iJob+nJob],9)
        else:
            Client.killJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(1)
                        

