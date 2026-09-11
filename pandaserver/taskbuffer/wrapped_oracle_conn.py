from types import TracebackType
from typing import Any


# wrapper for Oracle Connection
class WrappedOracleConn(object):
    # oracledb ships no stubs and CI does not install it, so the connection is Any either way
    def __init__(self, conn: Any) -> None:
        self.orig_conn = conn

    def __getattribute__(self, item: str) -> Any:
        try:
            return object.__getattribute__(self.orig_conn, item)
        except Exception:
            pass
        return object.__getattribute__(self, item)

    # override context manager protocol not to close connection
    def __enter__(self) -> Any:
        return self.orig_conn.__enter__()

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None:
        self.orig_conn.commit()
