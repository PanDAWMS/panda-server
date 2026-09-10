import os
import re
import socket
import sys
import threading
from http.client import HTTPSConnection
from typing import Any
from urllib.parse import parse_qs, urlencode

from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandaserver.api.v1.http_client import api_url_ssl

node: dict[str, Any] = {}
node["site_name"] = sys.argv[1]
node["memory"] = 1000
node["node"] = socket.getfqdn()
# node['prod_source_label']='user'
url = f"{api_url_ssl}/pilot/acquire_jobs"

match = re.search("[^:/]+://([^/]+)(/.+)", url)
if match is None:
    sys.exit(f"cannot extract the host and the path from the server URL {url}")
host = match.group(1)
path = match.group(2)

if "X509_USER_PROXY" in os.environ:
    certKey = os.environ["X509_USER_PROXY"]
else:
    certKey = f"/tmp/x509up_u{os.getuid()}"

rdata = urlencode(node)


class Thr(threading.Thread):
    def __init__(self) -> None:
        threading.Thread.__init__(self)

    def run(self) -> None:
        print(naive_utcnow().isoformat(" "))
        conn = HTTPSConnection(host, key_file=certKey, cert_file=certKey)
        conn.request("POST", path, rdata)
        resp = conn.getresponse()
        data = resp.read()
        print(naive_utcnow().isoformat(" "))
        print(parse_qs(data))


nThr = 1
thrs = []
for i in range(nThr):
    thrs.append(Thr())

for thr in thrs:
    thr.start()
