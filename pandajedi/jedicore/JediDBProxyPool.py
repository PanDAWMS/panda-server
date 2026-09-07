from types import TracebackType
from typing import Any

from pandaserver.taskbuffer import DBProxyPool as panda_db_proxy_pool

from . import JediDBProxy

# use customized proxy. The module attribute is deliberately rebound: the pool below
# looks DBProxy up on its own module at call time, so this is what makes it instantiate
# the JEDI proxy. JediDBProxy is the module, which is what DBProxy is there too
panda_db_proxy_pool.DBProxy = JediDBProxy  # type: ignore[attr-defined]


class DBProxyPool(panda_db_proxy_pool.DBProxyPool):
    # constructor
    def __init__(self, dbhost: str, dbpasswd: str, nConnection: int, useTimeout: bool = False) -> None:
        panda_db_proxy_pool.DBProxyPool.__init__(self, dbhost, dbpasswd, nConnection, useTimeout)

    # get a DBProxyObj containing a proxy. The base class reaches the same end with a
    # @contextmanager generator, so this override and DBProxyObj below duplicate it; the
    # two are only interchangeable because every one of the 406 call sites is a plain
    # `with pool.get() as proxy`, which is what the ignore stands for
    def get(self) -> "DBProxyObj":  # type: ignore[override]
        proxy_obj = DBProxyObj(db_proxy_pool=self)
        return proxy_obj


# object of context manager for db proxy
class DBProxyObj(object):
    # constructor
    def __init__(self, db_proxy_pool: DBProxyPool) -> None:
        self.proxy_pool = db_proxy_pool
        # the JEDI DBProxy the pool handed out, held only between __enter__ and __exit__
        self.proxy: Any = None

    # get proxy
    def __enter__(self) -> Any:
        self.proxy = self.proxy_pool.getProxy()
        return self.proxy

    # release proxy
    def __exit__(self, type: type[BaseException] | None, value: BaseException | None, traceback: TracebackType | None) -> None:
        self.proxy_pool.putProxy(self.proxy)
        self.proxy = None
