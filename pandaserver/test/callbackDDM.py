import os
import sys
import six

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
node['vuid'] = d.vuid
node['site'] = sys.argv[2]

try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except KeyError:
    baseURLSSL = 'https://localhost:25443/server/panda'

url = '{0}/datasetCompleted'.format(baseURLSSL)
rdata = six.b(urlencode(node))
req = Request(url)
fd = urlopen(req, rdata)
data = fd.read()

print(data)
