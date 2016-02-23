import urllib2
import json

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
TIMESTAMP = 'timestamp'

def get_dump(url):
    """
    Retrieves a json file from the given URL and loads it into memory
    """
    response = urllib2.urlopen(url)
    json_str = response.read()
    dump = json.loads(json_str)
    return dump
