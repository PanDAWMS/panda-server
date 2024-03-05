import sys
from threading import Lock

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config

_logger = PandaLogger().getLogger("Initializer")


class Initializer:
    """
    Initialize a dummy database connection.

    Returns:
        bool: True if the initialization is successful, False otherwise.
    """

    def __init__(self):
        self.lock = Lock()
        self.first = True

    def init(self):
        _logger.debug(f"init new={self.first}")
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
                if panda_config.backend == "oracle":
                    import oracledb

                    oracledb.init_oracle_client()
                    conn = oracledb.connect(user=panda_config.dbuser, password=panda_config.dbpasswd, dsn=panda_config.dbhost)

                elif panda_config.backend == "postgres":
                    import psycopg2

                    conn = psycopg2.connect(
                        host=panda_config.dbhost,
                        dbname=panda_config.dbname,
                        port=panda_config.dbport,
                        connect_timeout=panda_config.dbtimeout,
                        user=panda_config.dbuser,
                        password=panda_config.dbpasswd,
                    )

                else:
                    import MySQLdb

                    conn = MySQLdb.connect(
                        host=panda_config.dbhost,
                        db=panda_config.dbname,
                        port=panda_config.dbport,
                        connect_timeout=panda_config.dbtimeout,
                        user=panda_config.dbuser,
                        passwd=panda_config.dbpasswd,
                    )

                # close
                conn.close()
                _logger.debug("done")

            except Exception:
                self.lock.release()
                exception_type, exception_value, traceback = sys.exc_info()
                _logger.error(f"connect : {exception_type} {exception_value}")
                return False

        # release the lock
        self.lock.release()
        return True


# singleton
initializer = Initializer()
del Initializer
