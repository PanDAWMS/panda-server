import os
import sys
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer

taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)

d = taskBuffer.queryDatasetWithMap({"name": sys.argv[1]})

node = {"vuid": d.vuid, "site": sys.argv[2]}

try:
    baseURLSSL = os.environ["PANDA_URL_SSL"]
except KeyError:
    baseURLSSL = "https://localhost:25443/server/panda"

url = f"{baseURLSSL}/datasetCompleted"
rdata = (urlencode(node)).encode("utf-8")
req = Request(url)
fd = urlopen(req, rdata)
data = fd.read()

print(data)
