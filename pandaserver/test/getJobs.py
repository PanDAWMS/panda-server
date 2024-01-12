import datetime
import os
import re
import socket
import sys
import threading
from http.client import HTTPSConnection
from urllib.parse import parse_qs, urlencode

from pandaserver.userinterface.Client import baseURLSSL

node = {}
node["siteName"] = sys.argv[1]
node["mem"] = 1000
node["node"] = socket.getfqdn()
# node['prodSourceLabel']='user'
url = f"{baseURLSSL}/getJob"

match = re.search("[^:/]+://([^/]+)(/.+)", url)
host = match.group(1)
path = match.group(2)

if "X509_USER_PROXY" in os.environ:
    certKey = os.environ["X509_USER_PROXY"]
else:
    certKey = f"/tmp/x509up_u{os.getuid()}"

rdata = urlencode(node)


class Thr(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        print(datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(" "))
        conn = HTTPSConnection(host, key_file=certKey, cert_file=certKey)
        conn.request("POST", path, rdata)
        resp = conn.getresponse()
        data = resp.read()
        print(datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat(" "))
        print(parse_qs(data))


nThr = 1
thrs = []
for i in range(nThr):
    thrs.append(Thr())

for thr in thrs:
    thr.start()
