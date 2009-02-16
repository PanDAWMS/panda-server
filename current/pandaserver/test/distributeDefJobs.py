import datetime
from taskbuffer.DBProxy import DBProxy
import userinterface.Client as Client
import jobscheduler.Site
import random
import time

# password
from config import panda_config
passwd = panda_config.dbpasswd

# time limit
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

# get PandaIDs from jobsDefined
res = proxyS.querySQL("SELECT PandaID,modificationTime from jobsDefined4 ORDER BY modificationTime")

# list of known sites
tmpSites = jobscheduler.Site.KnownSite.getAllSitesID()
allSites = []
for site in tmpSites:
    # _allSites may conain NULL after sort()
    if site == 'NULL':
        continue
    # ignore test sites
    if site.endswith('test') or site.endswith('Test'):
        continue
    # append
    allSites.append(site)

# reassign jobs
jobs=[]
for (id,modTime) in res:
    if modTime < timeLimit:
        jobs.append(id)

# reassign
if len(jobs):
    nJob = 20
    iJob = 0
    while iJob < len(jobs):
        print 'reassignJobs(%s)' % jobs[iJob:iJob+nJob]
        index = random.randint(1,len(allSites))
        site = allSites[int(index)-1]
        print 'site=%s' % site
        Client.reassignJobs(jobs[iJob:iJob+nJob],site)
        iJob += nJob
        time.sleep(10)

