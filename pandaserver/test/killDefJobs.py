import datetime
from taskbuffer.DBProxy import DBProxy
import userinterface.Client as Client

# password
from config import panda_config
passwd = panda_config.dbpasswd

# time limit
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(days=1)

# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect('adbpro.usatlas.bnl.gov',passwd,'panda-developer','PandaDevDB')

# get PandaIDs from jobsDefined
res = proxyS.querySQL("SELECT PandaID,modificationTime from jobsDefined4 ORDER BY modificationTime")

# kill f old
jobs=[]
for (id,modTime) in res:
    if modTime < timeLimit:
        jobs.append(id)

Client.killJobs(jobs)

