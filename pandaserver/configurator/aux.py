import requests
import time
import os
import json
from urllib.parse import urlparse

from pandaserver.config import panda_config

GB = 1024**3
PROD_INPUT = 'Production Input'
PROD_OUTPUT = 'Production Output'
EXPRESS = 'Express'
FILES = 'files'
MBPS = 'mbps'
LATENCY = 'latency'
PACKETLOSS = 'packetloss'
DONE = 'done'
QUEUED = 'queued'
TOTAL = 'total'
LATEST = 'latest'
H1 = '1h'
H6 = '6h'
D1 = '1d'
W1 = '1w'
TIMESTAMP = 'timestamp'


def get_dump(url):
    """
    Retrieves a json file from the given URL and loads it into memory
    """
    # check if local file
    p = urlparse(url)
    if not p.scheme or p.scheme == 'file':
        try:
            with open(p.path) as f:
                return json.load(f)
        except Exception:
            return None
    if panda_config.configurator_use_cert:
        key_file = os.environ['X509_USER_PROXY']
        cert_file = os.environ['X509_USER_PROXY']
        ca_certs = os.environ['X509_CERT_DIR']
        cert = (cert_file, key_file)
    else:
        cert = None
        ca_certs = False
    for i in range(1, 4):  # 3 retries
        try:
            r = requests.get(url, cert=cert, verify=ca_certs)
            if r.status_code == requests.codes.ok:
                return r.json()
        except Exception:
            time.sleep(1)
    return None

