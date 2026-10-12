# Description: Unit tests for the wrappers of the Oracle and Postgres connections. No database is used
import unittest
from typing import Any

from pandaserver.taskbuffer.wrapped_oracle_conn import WrappedOracleConn
from pandaserver.taskbuffer.WrappedPostgresConn import WrappedPostgresConn


class RawConnection:
    closed = 0

    def commit(self) -> str:
        return "committed"


def counting(wrapper_class: type[Any]) -> tuple[type[Any], list[str]]:
    # a subclass recording each name its __getattribute__ is asked for, which is also what a
    # read of an attribute through self inside the wrapper's own __getattribute__ would call
    names: list[str] = []

    class Counting(wrapper_class):  # type: ignore[misc, valid-type]
        def __getattribute__(self, item: str) -> Any:
            names.append(item)
            return super().__getattribute__(item)

    return Counting, names


class TestWrappedConnections(unittest.TestCase):
    def test_one_lookup_per_read(self) -> None:
        for wrapper_class in (WrappedOracleConn, WrappedPostgresConn):
            counting_class, names = counting(wrapper_class)
            wrapped = counting_class(RawConnection())
            self.assertEqual(wrapped.commit(), "committed")
            self.assertEqual(names, ["commit"], wrapper_class.__name__)

    def test_names_of_the_connection_then_of_the_wrapper(self) -> None:
        raw = RawConnection()
        for wrapper_class in (WrappedOracleConn, WrappedPostgresConn):
            wrapped = wrapper_class(raw)
            self.assertIs(wrapped.orig_conn, raw)
            self.assertEqual(wrapped.commit(), "committed")
            with self.assertRaises(AttributeError):
                wrapped.no_such_attribute
        # the methods the Postgres connection lacks come from the wrapper. Any, since what is read
        # here is what __getattribute__ finds, which the static types do not tell
        postgres: Any = WrappedPostgresConn(raw)
        self.assertIs(postgres.begin.__func__, WrappedPostgresConn.begin)
        self.assertIs(postgres.ping.__func__, WrappedPostgresConn.ping)
        # reads closed of the connection through the wrapper, and passes since it is not closed
        postgres.ping()


if __name__ == "__main__":
    unittest.main()
