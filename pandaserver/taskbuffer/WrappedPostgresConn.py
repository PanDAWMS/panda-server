# wrapper for Postgres Connection
class WrappedPostgresConn (object):

    def __init__(self, conn):
        self.orig_conn = conn

    def __getattribute__(self, item):
        try:
            return object.__getattribute__(self.orig_conn, item)
        except Exception:
            pass
        return object.__getattribute__(self, item)

    def begin(self):
        pass

    def ping(self):
        if self.orig_conn.closed:
            raise RuntimeError('connection closed')
