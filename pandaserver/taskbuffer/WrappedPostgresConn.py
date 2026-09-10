from types import TracebackType
from typing import Any


# wrapper for Postgres Connection
class WrappedPostgresConn(object):
    # psycopg2 ships no stubs and CI does not install it, so the connection is Any either way
    def __init__(self, conn: Any) -> None:
        self.orig_conn = conn

    def __getattribute__(self, item: str) -> Any:
        try:
            return object.__getattribute__(self.orig_conn, item)
        except Exception:
            pass
        return object.__getattribute__(self, item)

    def begin(self) -> None:
        pass

    def ping(self) -> None:
        if self.orig_conn.closed:
            raise RuntimeError("connection closed")

    def __enter__(self) -> Any:
        return self.orig_conn.__enter__()

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None:
        self.orig_conn.__exit__(exc_type, exc_val, exc_tb)
