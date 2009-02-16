
"""
redirect apache log to the logging server

"""

import re
from pandalogger.PandaLogger import PandaLogger

# logger
_loggerMap = {} 
pandaLogger = PandaLogger()
            
while True:
    # read line
    line = raw_input()
    # extract host, request and response
    items = re.findall('(\S+) - - \[[^\]]+\] ("[^"]+") (\d+)',line)
    if len(items) == 1:
        # host
        host     = items[0][0]
        # request
        request  = items[0][1].split()[1].split('/')[-1]
        if request == 'isAlive':
            # somehow isAlive is not recorded
            request = 'IsAlive'
        # set logtype
        if request.startswith('datasetCompleted'):
            logtype = 'datasetCompleted'
        else:
            logtype = request
        # response    
        response = items[0][2]
        # make message
        message = '%s - %s %s' % (host,request,response)
        # get logger
        pandaLogger.setParam('Type',logtype)
        logger = pandaLogger.getHttpLogger('prod')
        # add message
        logger.info(message)
