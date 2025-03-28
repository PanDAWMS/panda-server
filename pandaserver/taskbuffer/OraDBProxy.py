"""
proxy for database connection

"""

import atexit
import logging
import warnings

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.taskbuffer.db_proxy_mods import (
    entity_module,
    job_complex_module,
    job_standalone_module,
    metrics_module,
    misc_standalone_module,
    task_complex_module,
    task_event_module,
    task_standalone_module,
    task_utils_module,
    worker_module,
)
from pandaserver.taskbuffer.WrappedCursor import WrappedCursor

try:
    import idds.common.constants
    import idds.common.utils
    from idds.client.client import Client as iDDS_Client
except ImportError:
    pass

if panda_config.backend == "oracle":
    import oracledb

    from . import wrapped_oracle_conn

    oracledb.init_oracle_client()

elif panda_config.backend == "postgres":
    import psycopg2 as psycopg

    from . import WrappedPostgresConn

else:
    import MySQLdb

warnings.filterwarnings("ignore")

# logger
_logger = PandaLogger().getLogger("DBProxy")
_loggerFiltered = PandaLogger().getLogger("DBProxyFiltered")

# add handlers
for handler in _loggerFiltered.handlers:
    handler.setLevel(logging.INFO)
    _logger.addHandler(handler)
    _loggerFiltered.removeHandler(handler)


# proxy
class DBProxy(
    entity_module.EntityModule,
    metrics_module.MetricsModule,
    worker_module.WorkerModule,
    task_event_module.TaskEventModule,
    job_complex_module.JobComplexModule,
    job_standalone_module.JobStandaloneModule,
    misc_standalone_module.MiscStandaloneModule,
    task_complex_module.TaskComplexModule,
    task_standalone_module.TaskStandaloneModule,
    task_utils_module.TaskUtilsModule,
):
    # constructor
    def __init__(self, useOtherError=False):
        # init modules
        super().__init__(_logger)
        # connection object
        self.conn = None
        # cursor object
        self.cur = None

        # use special error codes for reconnection in querySQL
        self.useOtherError = useOtherError

        # set composite modules. use self to share a single database connection
        self.add_composite_module("entity", self)
        self.add_composite_module("metrics", self)
        self.add_composite_module("worker", self)
        self.add_composite_module("task_event", self)
        self.add_composite_module("task_utils", self)
        self.add_composite_module("job_complex", self)

    # connect to DB
    def connect(
        self,
        dbhost=panda_config.dbhost,
        dbpasswd=panda_config.dbpasswd,
        dbuser=panda_config.dbuser,
        dbname=panda_config.dbname,
        dbtimeout=panda_config.dbtimeout,
        reconnect=False,
        dbport=panda_config.dbport,
    ):
        _logger.debug(f"connect : re={reconnect}")
        # keep parameters for reconnect
        if not reconnect:
            self.dbhost = dbhost
            self.dbpasswd = dbpasswd
            self.dbuser = dbuser
            self.dbname = dbname
            self.dbtimeout = dbtimeout
            self.dbport = dbport
        # close old connection
        if reconnect:
            _logger.debug("closing old connection")
            try:
                self.conn.close()
            except Exception:
                _logger.debug("failed to close old connection")
        # connect
        try:
            if self.backend == "oracle":
                conn = oracledb.connect(dsn=self.dbhost, user=self.dbuser, password=self.dbpasswd)

                def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
                    if defaultType == oracledb.CLOB:
                        return cursor.var(oracledb.LONG_STRING, arraysize=cursor.arraysize)

                conn.outputtypehandler = OutputTypeHandler
                self.conn = wrapped_oracle_conn.WrappedOracleConn(conn)

            elif self.backend == "postgres":
                dsn = {"dbname": self.dbname, "user": self.dbuser, "keepalives_idle": 30, "keepalives_interval": 30, "keepalives": 1}
                if self.dbpasswd:
                    dsn["password"] = self.dbpasswd
                if self.dbhost:
                    dsn["host"] = self.dbhost
                if self.dbport:
                    dsn["port"] = self.dbport
                conn = psycopg.connect(**dsn)
                self.conn = WrappedPostgresConn.WrappedPostgresConn(conn)
            else:
                self.conn = MySQLdb.connect(
                    host=self.dbhost,
                    db=self.dbname,
                    port=self.dbport,
                    connect_timeout=self.dbtimeout,
                    user=self.dbuser,
                    passwd=self.dbpasswd,
                    charset="utf8",
                )
            self.cur = WrappedCursor(self.conn)
            try:
                # use SQL dumper
                if panda_config.dump_sql:
                    from pandaserver.taskbuffer.SQLDumper import SQLDumper

                    self.cur = SQLDumper(self.cur)
            except Exception:
                pass
            self.hostname = self.cur.initialize()
            if not reconnect:
                atexit.register(self.close_connection)
            _logger.debug(f"connect : re={reconnect} ready")
            return True
        except Exception as e:
            _logger.error(f"connect : {str(e)}")
            return False
