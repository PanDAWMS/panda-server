# wrapper for Oracle Connection
class WrappedOracleConn(object):
    def __init__(self, conn):
        self.orig_conn = conn

    def __getattribute__(self, item):
        try:
            return object.__getattribute__(self.orig_conn, item)
        except Exception:
            pass
        return object.__getattribute__(self, item)

    # override context manager protocol not to close connection
    def __enter__(self):
        return self.orig_conn.__enter__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.orig_conn.commit()
