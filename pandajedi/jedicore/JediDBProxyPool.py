import taskbuffer.DBProxyPool

from pandaserver import taskbuffer

from . import JediDBProxy

# use customized proxy
taskbuffer.DBProxyPool.DBProxy = JediDBProxy


class DBProxyPool(taskbuffer.DBProxyPool.DBProxyPool):
    # constructor
    def __init__(self, dbhost, dbpasswd, nConnection, useTimeout=False):
        taskbuffer.DBProxyPool.DBProxyPool.__init__(self, dbhost, dbpasswd, nConnection, useTimeout)

    # get a DBProxyObj containing a proxy
    def get(self):
        proxy_obj = DBProxyObj(db_proxy_pool=self)
        return proxy_obj


# object of context manager for db proxy
class DBProxyObj(object):
    # constructor
    def __init__(self, db_proxy_pool):
        self.proxy_pool = db_proxy_pool
        self.proxy = None

    # get proxy
    def __enter__(self):
        self.proxy = self.proxy_pool.getProxy()
        return self.proxy

    # release proxy
    def __exit__(self, type, value, traceback):
        self.proxy_pool.putProxy(self.proxy)
        self.proxy = None
