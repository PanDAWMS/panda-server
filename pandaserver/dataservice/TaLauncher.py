'''
launcer for TaskAssigner

'''

import sys
import time
import commands
import threading
import cPickle as pickle

from config import panda_config
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('TaLauncher')


class TaLauncher (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,jobs):
        threading.Thread.__init__(self)
        self.jobs       = jobs
        self.taskBuffer = taskBuffer
        # time stamp
        self.timestamp = time.asctime()
        

    # main
    def run(self):
        try:
            _logger.debug('%s startRun' % self.timestamp)
            # run setupper sequentially
            for job in self.jobs:
                # write jobs to file
                outFileName = '%s/set.%s_%s' % (panda_config.logdir,job.PandaID,commands.getoutput('uuidgen'))
                outFile = open(outFileName,'w')
                pickle.dump([job],outFile)
                outFile.close()
                # run main procedure in another process because python doesn't release memory
                com = 'env PYTHONPATH=%s:%s %s/python -Wignore %s/dataservice/forkSetupper.py -i %s' % \
                      (panda_config.pandaCommon_dir,panda_config.pandaPython_dir,panda_config.native_python,
                       panda_config.pandaPython_dir,outFileName)
                # add option for TA
                com += " -t"
                _logger.debug(com)
                # exeute
                status,output = commands.getstatusoutput(com)
                _logger.debug("Ret from another process: %s %s" % (status,output))                
            _logger.debug('%s endRun' % self.timestamp)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("run() : %s %s" % (type,value))
