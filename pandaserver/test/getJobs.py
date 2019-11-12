import os
import re
import sys
import socket
import datetime
import threading
try:
    from urllib import urlencode
    from urlparse import parse_qs
except ImportError:
    from urllib.parse import urlencode, parse_qs
try:
    from httplib import HTTPSConnection
except ImportError:
    from http.client import HTTPSConnection

from pandaserver.userinterface.Client import baseURLSSL

node={}
node['siteName']=sys.argv[1]
node['mem']=1000
node['node']=socket.getfqdn()
#node['prodSourceLabel']='user'
url='%s/getJob' % baseURLSSL

match = re.search('[^:/]+://([^/]+)(/.+)',url)
host = match.group(1)
path = match.group(2)

if 'X509_USER_PROXY' in os.environ:
    certKey = os.environ['X509_USER_PROXY']
else:
    certKey = '/tmp/x509up_u%s' % os.getuid()

rdata=urlencode(node)


class Thr(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        print(datetime.datetime.utcnow().isoformat(' '))
        conn = HTTPSConnection(host, key_file=certKey, cert_file=certKey)
        conn.request('POST', path, rdata)
        resp = conn.getresponse()
        data = resp.read()
        print(datetime.datetime.utcnow().isoformat(' '))
        print(parse_qs(data))

nThr = 1
thrs = []
for i in range(nThr):
    thrs.append(Thr())

for thr in thrs:
    thr.start()
