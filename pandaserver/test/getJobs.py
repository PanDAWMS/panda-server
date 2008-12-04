import sys
import time
import threading
import urllib2,urllib

import httplib

import re
import os

node={}
node['siteName']=sys.argv[1]
node['mem']=1000
#node['prodSourceLabel']='user'
url='https://localhost:26443/server/panda/getJob'
#url='https://.usatlas.bnl.gov:25443/server/panda/getJob'

match = re.search('[^:/]+://([^/]+)(/.+)',url)
host = match.group(1)
path = match.group(2)

if os.environ.has_key('X509_USER_PROXY'):
    certKey = os.environ['X509_USER_PROXY']
else:
    certKey = '/tmp/x509up_u%s' % os.getuid()

rdata=urllib.urlencode(node)

class Thr(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        conn = httplib.HTTPSConnection(host,key_file=certKey,cert_file=certKey)
        conn.request('POST',path,rdata)
        resp = conn.getresponse()
        print time.localtime()
        data = resp.read()
        conn.close()
        import cgi
        print cgi.parse_qs(data)

nThr = 1
thrs = []
for i in range(nThr):
    thrs.append(Thr())

for thr in thrs:
    thr.start()
