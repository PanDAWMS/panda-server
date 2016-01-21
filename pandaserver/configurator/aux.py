import urllib2
import json


def get_dump(url):
    """
    Retrieves a json file from the given URL and loads it into memory
    """
    response = urllib2.urlopen(url)
    json_str = response.read()
    dump = json.loads(json_str)
    return dump
