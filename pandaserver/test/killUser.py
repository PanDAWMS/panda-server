import sys
import time
import datetime
import optparse

from taskbuffer.OraDBProxy import DBProxy
# password
from config import panda_config

optP = optparse.OptionParser(conflict_handler="resolve")
optP.add_option('--user', action='store',dest='user', default=None,help='prodUserName')
optP.add_option('--jobID',action='store',dest='jobID',default=None,help='jobDefinitionID')

options,args = optP.parse_args()

if options.user == None:
    print "--user=<prodUserName> is required"
    sys.exit(1)

if options.jobID == None:
    print "--jobID=<jobDefinitionID> is required"
    sys.exit(1)
    
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

prodUserName = sys.argv[1]
import userinterface.Client as Client

varMap = {}
varMap[':src1'] = 'user'
varMap[':src2'] = 'panda'
varMap[':prodUserName'] = options.user
varMap[':jobDefinitionID'] = options.jobID

jobs = []
tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']
for table in tables:
    sql = "SELECT PandaID FROM %s WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID AND prodSourceLabel IN (:src1,:src2) ORDER BY PandaID" % table
    status,res = proxyS.querySQLS(sql,varMap)
    if res != None:
        for id, in res:
            jobs.append(id)
if len(jobs):
    print "kill %s" % jobs
    Client.killJobs(jobs,code=9)

