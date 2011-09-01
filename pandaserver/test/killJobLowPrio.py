import time
import sys
import optparse

import userinterface.Client as Client

aSrvID = None

from taskbuffer.OraDBProxy import DBProxy
# password
from config import panda_config

optP = optparse.OptionParser(conflict_handler="resolve",usage="%s [options] <priority>" % sys.argv[0])
optP.add_option('-9',action='store_const',const=True,dest='forceKill',
                default=False,help='kill jobs even if they are still running')
optP.add_option('--site',action='store',dest='site',default=None,help='computingSite')
optP.add_option('--cloud',action='store',dest='cloud',default=None,help='cloud')
options,args = optP.parse_args()

if options.cloud == None and options.site == None:
    optP.error("--site=<computingSite> and/or --cloud=<cloud> is required")
        
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

jobs = []

if len(args) == 0:
    optP.error('priority is required')

varMap = {}
varMap[':prodSourceLabel']  = 'managed'
varMap[':currentPriority']  = args[0]
sql = "SELECT PandaID FROM %s WHERE prodSourceLabel=:prodSourceLabel AND currentPriority<:currentPriority "
if options.cloud != None:
    sql += "AND cloud=:cloud "
    varMap[':cloud'] = options.cloud
if options.site != None:
    sql += "AND computingSite=:site "
    varMap[':site'] = options.site
sql += "ORDER BY PandaID "
for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']:
    status,res = proxyS.querySQLS(sql % table,varMap)
    if res != None:
        for id, in res:
            if not id in jobs:
                jobs.append(id)

print 'The number of jobs with priorities below %s : %s' % (args[0],len(jobs))
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
                        

