try:
    from urllib2 import urlopen
except ImportError:
    from urllib.request import urlopen
import json
import time

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
    for i in range(1, 4): # 3 retries
        try:
            response = urlopen(url)
            json_str = response.read()
            dump = json.loads(json_str)
            return dump
        except ValueError:
            time.sleep(1)

    return {}




