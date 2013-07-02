'''
launcer for ReBroker

'''

import sys
import time
import commands
import threading

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('RbLauncher')


class RbLauncher (threading.Thread):
    # constructor
    def __init__(self,dn,jobID,cloud=None,excludedSite=None):
        threading.Thread.__init__(self)
        self.dn           = dn
        self.jobID        = jobID
        self.cloud        = cloud
        self.excludedSite = excludedSite
        # time stamp
        self.timestamp = time.asctime()
        

    # main
    def run(self):
        try:
            _logger.debug('%s startRun' % self.timestamp)
            # run 
            com = 'cd %s > /dev/null 2>&1; export HOME=%s; ' % (panda_config.home_dir_cwd,panda_config.home_dir_cwd)
            com += 'source %s; ' % panda_config.glite_source
            com += 'env PYTHONPATH=%s:%s %s/python -Wignore %s/userinterface/runReBroker.py ' % \
                   (panda_config.pandaCommon_dir,panda_config.pandaPython_dir,panda_config.native_python,
                    panda_config.pandaPython_dir)
            com += '-j %s -d "%s" ' % (self.jobID,self.dn)
            if self.cloud != None:
                com += '-c %s ' % self.cloud
            if self.excludedSite != None:
                com += '-e %s ' % self.excludedSite
            # exeute
            _logger.debug('%s com=%s' % (self.timestamp,com))
            status,output = commands.getstatusoutput(com)
            _logger.debug("%s Ret from another process: %s %s" % (self.timestamp,status,output))                
            _logger.debug('%s endRun' % self.timestamp)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("run() : %s %s" % (type,value))
