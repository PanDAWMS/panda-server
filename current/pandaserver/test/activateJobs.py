import sys

from taskbuffer.DBProxy import DBProxy
import userinterface.Client as Client
import urllib2,urllib,datetime,time

# password
from config import panda_config
passwd = panda_config.dbpasswd

if len(sys.argv) == 2:
    startID = int(sys.argv[1])
    endID   = startID
else:
    startID = int(sys.argv[1])
    endID   = int(sys.argv[2])
    if startID > endID:
        print '%d is less than %d' % (endID,startID)
        sys.exit(1)

# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

# get PandaIDs from jobsDefined
res = proxyS.querySQL("SELECT dispatchDBlock from jobsDefined4 WHERE PandaID>=%s AND PandaID<=%s GROUP BY dispatchDBlock" % (startID,endID))

# emulate DDM callbacks
for dispatchDBlock, in res:
    # get VUID and creationdate
    resvuid = proxyS.querySQL("SELECT vuid from Datasets WHERE name='%s'" % dispatchDBlock)
    if len(resvuid) == 1:
        vuid, = resvuid[0]
        # make HTTP request
        node={'vuid':vuid}
        url=Client.baseURLSSL+'/datasetCompleted'
        rdata=urllib.urlencode(node)
        req=urllib2.Request(url)
        # invoke callback
        fd=urllib2.urlopen(req,rdata)
        
