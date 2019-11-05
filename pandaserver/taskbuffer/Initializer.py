import sys
from threading import Lock

from pandaserver.config import panda_config

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
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
                if panda_config.backend == 'oracle':
                    import cx_Oracle
                    conn = cx_Oracle.connect(dsn=panda_config.dbhost,user=panda_config.dbuser,
                                             password=panda_config.dbpasswd,threaded=True)
                else:
                    import MySQLdb
                    conn = MySQLdb.connect(host=panda_config.dbhost, db=panda_config.dbname, 
                                           port=panda_config.dbport, connect_timeout=panda_config.dbtimeout, 
                                           user=panda_config.dbuser, passwd=panda_config.dbpasswd)
                # close
                conn.close()
                _logger.debug("done")                
            except Exception:
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
