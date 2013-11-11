import sys
import urllib2,urllib

from taskbuffer.TaskBuffer import taskBuffer
from config import panda_config
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

d = taskBuffer.queryDatasetWithMap({'name':sys.argv[1]})
node={}
node['vuid']=d.vuid
node['site']=sys.argv[2]
url='https://localhost:25443/server/panda/datasetCompleted'
rdata=urllib.urlencode(node)
req=urllib2.Request(url)
fd=urllib2.urlopen(req,rdata)
data = fd.read()

print data
