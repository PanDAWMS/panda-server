import re
import os
import time

try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode
try:
    from httplib import HTTPSConnection
except ImportError:
    from http.client import HTTPSConnection

import pandaserver.userinterface.Client as Client


def sendCommand(function, node, _logger):
    """
    Send a command to the panda server.
    URL is composed by baseURLSS+function parameter
    The node parameter is url encoded and sent in the request
    The answer from the server is returned without further processing.
    """

    # Prepare certificate
    if 'X509_USER_PROXY' in os.environ:
        certKey = os.environ['X509_USER_PROXY']
    else:
        certKey = '/tmp/x509up_u%s' % os.getuid()

    # Prepare the URL (host+path) to connect
    url = '%s/%s' % (Client.baseURLSSL, function)
    match = re.search('[^:/]+://([^/]+)(/.+)', url)
    host = match.group(1)
    path = match.group(2)
    request = urlencode(node)

    st = time.time()
    conn = HTTPSConnection(host, key_file=certKey, cert_file=certKey)
    conn.request('POST', path, request)
    resp = conn.getresponse()
    data = resp.read()
    conn.close()
    elapsed = round(time.time() - st, 2)

    _logger.info("Called URL {0} with request {1}. Took {2}s".format(url, request, elapsed))

    return data
