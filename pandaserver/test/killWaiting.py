import sys
import time
import datetime
from taskbuffer.DBProxy import DBProxy
import userinterface.Client as Client

# password
from config import panda_config
passwd = panda_config.dbpasswd

cloud = sys.argv[1]

# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

while True:
    # get PandaIDs
    res = proxyS.querySQL("SELECT PandaID FROM jobsWaiting4 WHERE cloud='%s' ORDER BY PandaID" % cloud)
    # escape
    if len(res) == 0:
        break
    # convert to list
    jobs = []
    for id, in res:
        jobs.append(id)
    # reassign
    nJob = 300
    iJob = 0
    while iJob < len(jobs):
        print 'killJobs(%s)' % jobs[iJob:iJob+nJob]
        Client.killJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(60)

