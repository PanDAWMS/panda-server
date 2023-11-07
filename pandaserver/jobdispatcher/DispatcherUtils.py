import os
import sys

import requests
from pandaserver.config import panda_config


# get secret key
def getSecretKey(pandaID):
    try:
        # get parameters for panda proxy
        proxyURLSSL = panda_config.pandaProxy_URLSSL
        ca_certs = panda_config.pandaProxy_ca_certs
        key_file = os.environ["X509_USER_PROXY"]
        cert_file = os.environ["X509_USER_PROXY"]
        # send request
        data = {"pandaID": pandaID}
        res = requests.post(
            proxyURLSSL + "/insertSecretKeyForPandaID",
            data=data,
            verify=ca_certs,
            cert=(cert_file, key_file),
        )
        tmpDict = res.json()
        statusCode = tmpDict["errorCode"]
        secretKey = tmpDict["secretKey"]
        if tmpDict["errorCode"] == 0:
            # succeeded
            return secretKey, ""
        else:
            # failed
            return None, tmpDict["errorDiag"]
    except Exception:
        errType, errValue = sys.exc_info()[:2]
        return None, f"{errType}:{errValue}"
