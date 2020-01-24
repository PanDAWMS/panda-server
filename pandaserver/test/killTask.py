import time
import optparse

import pandaserver.userinterface.Client as Client

from pandaserver.taskbuffer.OraDBProxy import DBProxy
# password
from pandaserver.config import panda_config


aSrvID = None

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option('-9',action='store_const',const=True,dest='forceKill',
                default=False,help='kill jobs even if they are still running')
optP.add_option('--noRunning',action='store_const',const=True,dest='noRunning',
                default=True,help='kill jobs if they are not in running or transferring (ON by default)')
optP.add_option('--killAny',action='store_const',const=True,dest='killAny',
                default=False,help='kill jobs in any status')
optP.add_option('--prodSourceLabel', action='store',dest='prodSourceLabel', default='managed',
                help='prodSourceLabel')

options,args = optP.parse_args()

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

jobs = []

varMap = {}
varMap[':prodSourceLabel'] = options.prodSourceLabel
varMap[':taskID'] = args[0]
if not options.noRunning or options.killAny:
    sql = "SELECT PandaID FROM %s WHERE prodSourceLabel=:prodSourceLabel AND taskID=:taskID ORDER BY PandaID"
else:
    sql = "SELECT PandaID FROM %s WHERE prodSourceLabel=:prodSourceLabel AND taskID=:taskID AND NOT jobStatus IN (:js1,:js2,:js3) ORDER BY PandaID"
    varMap[':js1'] = 'running'
    varMap[':js2'] = 'transferring'
    varMap[':js3'] = 'holding'
for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']:
    status,res = proxyS.querySQLS(sql % table,varMap)
    if res is not None:
        for id, in res:
            if id not in jobs:
                jobs.append(id)

print('The number of jobs to be killed for prodSourceLabel={0} taskID={1}: {2}'.format(options.prodSourceLabel,
                                                                                       args[0],
                                                                                       len(jobs)))
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        print('kill %s' % str(jobs[iJob:iJob+nJob]))
        if options.forceKill:
            Client.killJobs(jobs[iJob:iJob+nJob],9)
        else:
            Client.killJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(1)
