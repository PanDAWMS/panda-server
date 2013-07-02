import sys
import cx_Oracle
from threading import Lock

from config import panda_config

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('Initializer')

# initialize cx_Oracle using dummy connection to avoid "Unable to acquire Oracle environment handle"
class Initializer:
    def __init__(self):
        self.lock = Lock()
        self.first = True

    def init(self):
        _logger.debug("init new=%s" % self.first)
        # do nothing when nDBConnection is 0
        if panda_config.nDBConnection == 0:
            return True
        # lock
        self.lock.acquire()
        if self.first:
            self.first = False
            try:
                _logger.debug("connect")
                # connect
                conn = cx_Oracle.connect(dsn=panda_config.dbhost,user=panda_config.dbuser,
                                         password=panda_config.dbpasswd,threaded=True)
                # close
                conn.close()
                _logger.debug("done")                
            except:
                self.lock.release()                
                type, value, traceBack = sys.exc_info()
                _logger.error("connect : %s %s" % (type,value))
                return False
        # release    
        self.lock.release()                            
        return True        


# singleton
initializer = Initializer()
del Initializer
