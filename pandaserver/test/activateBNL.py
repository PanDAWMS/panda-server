import sys
import time
from dataservice.DDM import ddm
from taskbuffer.DBProxy import DBProxy
import userinterface.Client as Client
import urllib2,urllib,datetime,time
import jobscheduler.siteinfo
import jobscheduler.Site
import brokerage.broker_util

# password
# A very minor edit.
from config import panda_config
passwd = panda_config.dbpasswd

# instantiate DB proxies
proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

# get PandaIDs from jobsDefined
timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
sql  = "SELECT dispatchDBlock from jobsDefined4 WHERE jobStatus='assigned' AND prodSourceLabel='managed' "
sql += "AND (computingSite='BNL_ATLAS_1' OR computingSite='BNL_ATLAS_2') AND modificationTime<'%s' "
sql += "GROUP BY dispatchDBlock"

res = proxyS.querySQL(sql % timeLimit.strftime('%Y-%m-%d %H:%M:%S'))

# emulate DDM callbacks
for dispatchDBlock, in res:
    print dispatchDBlock
    time.sleep(5)
    # get file list
    status,out = ddm.dq2.main(['listFilesInDataset',dispatchDBlock])
    if status != 0 or out.startswith('Error'):
        print out
        continue
    # make LFN list
    lfns = []
    for line in out.split('\n'):
        items = line.split()
        if len(items) == 2:
            lfns.append(items[1])
    # skip empty datasets
    if len(lfns) == 0:
        print "empty dataset"
        continue
    # get missing file
    missLFNs = brokerage.broker_util.getMissLFNsFromLRC(lfns,jobscheduler.Site.KnownSite('BNL_ATLAS_2').getDQ2URL())
    if len(missLFNs) != 0:
        print "some files are missing"
        continue
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
        
