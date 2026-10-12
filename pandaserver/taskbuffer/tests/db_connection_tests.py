# Description: Unit tests for the call timeout of DBProxy connections. No database is used:
# the drivers are replaced
import unittest
from typing import Any, cast
from unittest import mock

from pandaserver.config import panda_config
from pandaserver.taskbuffer import OraDBProxy, WrappedPostgresConn, wrapped_oracle_conn


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


if __name__ == "__main__":
    unittest.main()
