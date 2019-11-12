import sys

try:
    from urllib import urlencode, urlopen
    from urllib2 import Request
except ImportError:
    from urllib.parse import urlencode
    from urllib.request import urlopen, Request
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.config import panda_config
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

d = taskBuffer.queryDatasetWithMap({'name':sys.argv[1]})
node={}
node['vuid']=d.vuid
node['site']=sys.argv[2]
url='https://localhost:25443/server/panda/datasetCompleted'
rdata=urlencode(node)
req=Request(url)
fd=urlopen(req,rdata)
data = fd.read()

print(data)
