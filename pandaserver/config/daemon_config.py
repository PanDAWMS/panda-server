import re
import sys
import socket
from pandacommon.liveconfigparser.LiveConfigParser import LiveConfigParser

# get ConfigParser
tmpConf = LiveConfigParser()

# read
tmpConf.read('panda_server.cfg')

# get daemon section
tmpDict = getattr(tmpConf, 'daemon', {})

# expand all values
tmpSelf = sys.modules[ __name__ ]
for tmpKey in tmpDict:
    tmpVal = tmpDict[tmpKey]
    # convert string to bool/int
    if tmpVal == 'True':
        tmpVal = True
    elif tmpVal == 'False':
        tmpVal = False
    elif re.match('^\d+$', tmpVal):
        tmpVal = int(tmpVal)
    # update dict
    tmpSelf.__dict__[tmpKey] = tmpVal

# default values
if 'enable' not in tmpSelf.__dict__:
    tmpSelf.__dict__['enable'] = False
