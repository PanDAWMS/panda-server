# Description: Unit tests for the call timeout of DBProxy connections and the connection
# options of DBProxyPool and TaskBuffer.init. No database is used: the drivers are replaced
import unittest
from typing import Any, cast
from unittest import mock

from pandaserver.config import panda_config
from pandaserver.srvcore.exceptions import DatabaseConnectionError
from pandaserver.taskbuffer import DBProxyPool as db_proxy_pool_module
from pandaserver.taskbuffer import OraDBProxy, WrappedPostgresConn, wrapped_oracle_conn
from pandaserver.taskbuffer import TaskBuffer as task_buffer_module


class FakeDriverConnection:
    # a plain object, so that an attribute the code under test never set is absent. Declared
    # only, the way the driver's connection has it
    call_timeout: int

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeCursor:
    def __init__(self, conn: Any) -> None:
        self.conn = conn
        self.executed: list[str] = []

    def initialize(self) -> str:
        return "db-host"

    def execute(self, sql: str, var_map: dict[str, Any] | None = None) -> None:
        self.executed.append(sql)


class ProxyTestBase(unittest.TestCase):
    backend = "oracle"

    def setUp(self) -> None:
        self.driver_connections: list[FakeDriverConnection] = []

        def driver_connect(**kwargs: Any) -> FakeDriverConnection:
            conn = FakeDriverConnection(**kwargs)
            self.driver_connections.append(conn)
            return conn

        fake_oracledb = mock.Mock(connect=mock.Mock(side_effect=driver_connect))
        fake_psycopg = mock.Mock(connect=mock.Mock(side_effect=driver_connect))
        # create=True since the module imports only the driver of the configured backend
        patches: list[Any] = [
            mock.patch.object(OraDBProxy, "oracledb", fake_oracledb, create=True),
            mock.patch.object(OraDBProxy, "psycopg", fake_psycopg, create=True),
            mock.patch.object(OraDBProxy, "wrapped_oracle_conn", wrapped_oracle_conn, create=True),
            mock.patch.object(OraDBProxy, "WrappedPostgresConn", WrappedPostgresConn, create=True),
            mock.patch.object(OraDBProxy, "WrappedCursor", FakeCursor),
            mock.patch.object(OraDBProxy.atexit, "register"),
            mock.patch.object(panda_config, "dump_sql", False),
        ]
        for patch in patches:
            patch.start()
            self.addCleanup(patch.stop)
        self.proxy = OraDBProxy.DBProxy()
        self.proxy.backend = self.backend

    def connect(self, call_timeout_ms: int | None) -> bool:
        return self.proxy.connect(dbhost="host", dbpasswd="secret", dbuser="reader", dbname="db", dbtimeout=60, dbport=5432, call_timeout_ms=call_timeout_ms)


class TestOracleCallTimeout(ProxyTestBase):
    backend = "oracle"

    def test_set_on_driver_connection_and_kept_on_reconnect(self) -> None:
        self.assertTrue(self.connect(30000))
        self.assertEqual(self.proxy.conn.orig_conn.call_timeout, 30000)
        self.assertTrue(self.proxy.connect(reconnect=True))
        self.assertEqual(len(self.driver_connections), 2)
        self.assertTrue(self.driver_connections[0].closed)
        self.assertIs(self.proxy.conn.orig_conn, self.driver_connections[1])
        self.assertEqual(self.driver_connections[1].call_timeout, 30000)
        self.assertEqual(self.driver_connections[1].kwargs["user"], "reader")

    def test_not_set_without_limit(self) -> None:
        self.assertTrue(self.connect(None))
        self.assertFalse(hasattr(self.driver_connections[0], "call_timeout"))

    def test_lowered_within_limit(self) -> None:
        self.assertTrue(self.connect(30000))
        raw = self.proxy.conn.orig_conn
        self.proxy.set_call_timeout(5000)
        self.assertEqual(raw.call_timeout, 5000)
        self.proxy.set_call_timeout(60000)
        self.assertEqual(raw.call_timeout, 30000)
        self.proxy.set_call_timeout(None)
        self.assertEqual(raw.call_timeout, 30000)
        # 0 would mean no limit to the driver
        with self.assertRaises(ValueError):
            self.proxy.set_call_timeout(0)
        self.assertEqual(raw.call_timeout, 30000)

    def test_put_back_to_no_limit(self) -> None:
        self.assertTrue(self.connect(None))
        self.proxy.set_call_timeout(5000)
        self.assertEqual(self.proxy.conn.orig_conn.call_timeout, 5000)
        self.proxy.set_call_timeout(None)
        self.assertEqual(self.proxy.conn.orig_conn.call_timeout, 0)


class TestPostgresCallTimeout(ProxyTestBase):
    backend = "postgres"

    def test_in_dsn_and_kept_on_reconnect(self) -> None:
        self.assertTrue(self.connect(30000))
        self.assertTrue(self.proxy.connect(reconnect=True))
        self.assertEqual(len(self.driver_connections), 2)
        for conn in self.driver_connections:
            self.assertEqual(conn.kwargs["options"], "-c statement_timeout=30000")
            self.assertEqual(conn.kwargs["user"], "reader")

    def test_not_in_dsn_without_limit(self) -> None:
        self.assertTrue(self.connect(None))
        self.assertNotIn("options", self.driver_connections[0].kwargs)

    def test_lowered_with_set_local(self) -> None:
        self.assertTrue(self.connect(30000))
        self.proxy.set_call_timeout(5000)
        self.proxy.set_call_timeout(60000)
        self.proxy.set_call_timeout(None)
        executed = [sql.split(" /*")[0] for sql in cast(FakeCursor, self.proxy.cur).executed]
        # the end of the transaction puts the limit back, so None runs nothing
        self.assertEqual(executed, ["SET LOCAL statement_timeout TO 5000", "SET LOCAL statement_timeout TO 30000"])


class TestMySQLCallTimeout(ProxyTestBase):
    backend = "mysql"

    def test_refused(self) -> None:
        self.assertFalse(self.connect(30000))
        self.assertEqual(self.driver_connections, [])


class FakePoolProxy:
    # stands in for DBProxy in DBProxyPool, connecting after the given number of failures
    failures_left = 0
    instances: list["FakePoolProxy"] = []

    def __init__(self) -> None:
        self.connect_calls: list[dict[str, Any]] = []
        self.cleaned_up = False
        FakePoolProxy.instances.append(self)

    def connect(self, dbhost: str, dbpasswd: str, **kwargs: Any) -> bool:
        self.connect_calls.append(kwargs)
        if FakePoolProxy.failures_left > 0:
            FakePoolProxy.failures_left -= 1
            return False
        return True

    def cleanup(self) -> None:
        self.cleaned_up = True


class TestDBProxyPool(unittest.TestCase):
    def setUp(self) -> None:
        FakePoolProxy.failures_left = 0
        FakePoolProxy.instances = []
        proxy_patch = mock.patch.object(db_proxy_pool_module.DBProxy, "DBProxy", FakePoolProxy)
        proxy_patch.start()
        self.addCleanup(proxy_patch.stop)
        sleep_patch = mock.patch.object(db_proxy_pool_module.time, "sleep")
        self.sleep = sleep_patch.start()
        self.addCleanup(sleep_patch.stop)

    def test_options_passed_only_when_given(self) -> None:
        db_proxy_pool_module.DBProxyPool("host", "secret", 1)
        self.assertEqual(FakePoolProxy.instances[0].connect_calls, [{"dbtimeout": 60}])
        db_proxy_pool_module.DBProxyPool("host", "secret", 1, dbuser="reader", call_timeout_ms=30000)
        self.assertEqual(FakePoolProxy.instances[1].connect_calls, [{"dbtimeout": 60, "dbuser": "reader", "call_timeout_ms": 30000}])

    def test_gives_up_after_max_attempts(self) -> None:
        FakePoolProxy.failures_left = 10
        with self.assertRaises(DatabaseConnectionError):
            db_proxy_pool_module.DBProxyPool("host", "secret", 1, max_connect_attempts=3)
        proxy = FakePoolProxy.instances[0]
        self.assertEqual(len(proxy.connect_calls), 3)
        # no wait after the last attempt
        self.assertEqual(self.sleep.call_count, 2)
        self.assertTrue(proxy.cleaned_up)

    def test_retries_without_max_attempts(self) -> None:
        FakePoolProxy.failures_left = 4
        db_proxy_pool_module.DBProxyPool("host", "secret", 1)
        self.assertEqual(len(FakePoolProxy.instances[0].connect_calls), 5)

    def test_con_bridge_refuses_options(self) -> None:
        with mock.patch.object(panda_config, "usedbtimeout", True, create=True):
            with self.assertRaises(ValueError):
                db_proxy_pool_module.DBProxyPool("host", "secret", 1, useTimeout=True, dbuser="reader")


class TestTaskBufferInit(unittest.TestCase):
    def test_options_forwarded_and_lock_released_on_failure(self) -> None:
        pool_class = mock.Mock(side_effect=DatabaseConnectionError("down"))
        with mock.patch.object(task_buffer_module, "DBProxyPool", pool_class):
            task_buffer = task_buffer_module.TaskBuffer()
            with self.assertRaises(DatabaseConnectionError):
                task_buffer.init("host", "secret", 1, dbuser="reader", call_timeout_ms=30000, max_connect_attempts=1)
        pool_class.assert_called_once_with("host", "secret", 1, False, dbuser="reader", call_timeout_ms=30000, max_connect_attempts=1)
        self.assertIsNone(task_buffer.proxyPool)
        self.assertTrue(task_buffer.lock.acquire(blocking=False))
        task_buffer.lock.release()


if __name__ == "__main__":
    unittest.main()
