"""
proxy for database connection

"""

import atexit
import logging
import warnings
from typing import Any

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.config import panda_config
from pandaserver.taskbuffer.db_proxy_mods import (
    async_request_module,
    data_carousel_module,
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
    workflow_module,
)
from pandaserver.taskbuffer.WrappedCursor import WrappedCursor

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
    async_request_module.AsyncRequestModule,
    entity_module.EntityModule,
    metrics_module.MetricsModule,
    worker_module.WorkerModule,
    data_carousel_module.DataCarouselModule,
    task_event_module.TaskEventModule,
    job_complex_module.JobComplexModule,
    job_standalone_module.JobStandaloneModule,
    misc_standalone_module.MiscStandaloneModule,
    task_complex_module.TaskComplexModule,
    task_standalone_module.TaskStandaloneModule,
    task_utils_module.TaskUtilsModule,
    workflow_module.WorkflowModule,
):
    # constructor
    def __init__(self, useOtherError: bool = False) -> None:
        # init modules. this also installs the connection and cursor placeholders
        super().__init__(_logger)

        # use special error codes for reconnection in querySQL
        self.useOtherError = useOtherError

        # most milliseconds one round trip may take, given to connect(). None for no limit
        self.call_timeout_ms: int | None = None

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
        dbhost: str = panda_config.dbhost,
        dbpasswd: str = panda_config.dbpasswd,
        dbuser: str = panda_config.dbuser,
        dbname: str = panda_config.dbname,
        dbtimeout: int | None = panda_config.dbtimeout,
        reconnect: bool = False,
        dbport: int = panda_config.dbport,
        call_timeout_ms: int | None = None,
    ) -> bool:
        """
        Connect to the database, or reconnect with the parameters of the first connection.

        Args:
            call_timeout_ms(int): most milliseconds one round trip may take, or None for no limit. It is
                kept like the other parameters, so a reconnect by wakeUp() or _rollback() keeps the limit.
                Not available with MySQL, where the connection fails rather than go without the limit

        Returns:
            bool: True when connected
        """
        _logger.debug(f"connect : re={reconnect}")
        # keep parameters for reconnect
        if not reconnect:
            self.dbhost = dbhost
            self.dbpasswd = dbpasswd
            self.dbuser = dbuser
            self.dbname = dbname
            self.dbtimeout = dbtimeout
            self.dbport = dbport
            self.call_timeout_ms = call_timeout_ms
        if self.call_timeout_ms is not None and self.backend not in ("oracle", "postgres"):
            _logger.error(f"connect : call_timeout_ms is not available with {self.backend}")
            return False
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

                def OutputTypeHandler(cursor: Any, name: str, defaultType: Any, size: int, precision: int, scale: int) -> Any:
                    if defaultType == oracledb.CLOB:
                        return cursor.var(oracledb.LONG_STRING, arraysize=cursor.arraysize)

                conn.outputtypehandler = OutputTypeHandler
                # set on the driver's connection: WrappedOracleConn forwards reads only, so the same
                # assignment through it would set an attribute on the wrapper and limit nothing
                if self.call_timeout_ms is not None:
                    conn.call_timeout = self.call_timeout_ms
                self.conn = wrapped_oracle_conn.WrappedOracleConn(conn)

            elif self.backend == "postgres":
                dsn = {"dbname": self.dbname, "user": self.dbuser, "keepalives_idle": 30, "keepalives_interval": 30, "keepalives": 1}
                if self.dbpasswd:
                    dsn["password"] = self.dbpasswd
                if self.dbhost:
                    dsn["host"] = self.dbhost
                if self.dbport:
                    dsn["port"] = self.dbport
                if self.call_timeout_ms is not None:
                    dsn["options"] = f"-c statement_timeout={int(self.call_timeout_ms)}"
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

                    # SQLDumper delegates every attribute to the cursor it wraps
                    self.cur = SQLDumper(self.cur)  # type: ignore[assignment]
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

    # limit the following round trips to the database
    def set_call_timeout(self, timeout_ms: int | None) -> None:
        """
        Set how long each following round trip to the database may take.

        The limit can only be lowered: a value above the one given to connect() is cut down to it.
        On Oracle this is the driver's call_timeout, kept until it is set again or the connection is
        remade. On Postgres it is statement_timeout set with SET LOCAL, which takes a round trip of
        its own and lasts until the transaction ends.

        Args:
            timeout_ms(int): milliseconds, at least 1, since both databases read 0 as no limit.
                None puts back the limit given to connect(). On Postgres the end of the transaction
                does that, so None does nothing there: a SET in a transaction an error has aborted
                would fail
        """
        comment = " /* DBProxy.set_call_timeout */"
        if timeout_ms is not None:
            if timeout_ms < 1:
                raise ValueError(f"call timeout must be at least 1 ms, got {timeout_ms}")
            if self.call_timeout_ms is not None:
                timeout_ms = min(timeout_ms, self.call_timeout_ms)
        if self.backend == "oracle":
            # on the driver's connection, for the reason given in connect()
            self.conn.orig_conn.call_timeout = timeout_ms if timeout_ms is not None else (self.call_timeout_ms or 0)
        elif self.backend == "postgres":
            if timeout_ms is not None:
                self.cur.execute(f"SET LOCAL statement_timeout TO {int(timeout_ms)}" + comment)
        else:
            raise NotImplementedError(f"call timeout is not available with {self.backend}")
