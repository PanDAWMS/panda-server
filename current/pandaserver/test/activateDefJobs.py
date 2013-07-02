from taskbuffer.DBProxy import DBProxy
import userinterface.Client as Client
import urllib2,urllib,datetime,time

# password
from config import panda_config
passwd = panda_config.dbpasswd

# time limit
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=2)

# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect('adbpro.usatlas.bnl.gov',passwd,'panda-developer','PandaDevDB')

# get PandaIDs from jobsDefined
res = proxyS.querySQL("SELECT dispatchDBlock from jobsDefined4 GROUP BY dispatchDBlock")

# emulate DDM callbacks
jobs=[]
for dispatchDBlock, in res:
    # get VUID and creationdate
    resvuid = proxyS.querySQL("SELECT vuid,creationdate from Datasets WHERE name='%s'" % dispatchDBlock)
    if len(resvuid) == 1:
        vuid,creationdate = resvuid[0]
        # convert creatindate to datetime
        creation_datetime = datetime.datetime(*time.strptime(creationdate,'%Y-%m-%d %H:%M:%S')[:6])
        if creation_datetime < timeLimit:
            # make HTTP request
            node={'vuid':vuid}
            url=Client.baseURLSSL+'/datasetCompleted'
            rdata=urllib.urlencode(node)
            req=urllib2.Request(url)
            # invoke callback
            fd=urllib2.urlopen(req,rdata)
        
