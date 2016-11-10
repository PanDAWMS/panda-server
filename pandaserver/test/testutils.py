import urllib
import httplib
import re
import os
import time
import userinterface.Client as Client


def sendCommand(function, node, _logger):
    """
    Send a command to the panda server.
    URL is composed by baseURLSS+function parameter
    The node parameter is url encoded and sent in the request
    The answer from the server is returned without further processing.
    """

    # Prepare certificate
    if os.environ.has_key('X509_USER_PROXY'):
        certKey = os.environ['X509_USER_PROXY']
    else:
        certKey = '/tmp/x509up_u%s' % os.getuid()

    # Prepare the URL (host+path) to connect
    url = '%s/%s' % (Client.baseURLSSL, function)
    match = re.search('[^:/]+://([^/]+)(/.+)', url)
    host = match.group(1)
    path = match.group(2)
    request = urllib.urlencode(node)

    st = time.time()
    conn = httplib.HTTPSConnection(host, key_file=certKey, cert_file=certKey)
    conn.request('POST', path, request)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    elapsed = round(time.time() - st, 2)

    _logger.info("Called URL {0} with request {1}. Took {2}s".format(url, request, elapsed))

    return data