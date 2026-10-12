from pandaserver.taskbuffer import DBProxyPool as panda_db_proxy_pool

from . import JediDBProxy

# use customized proxy. The module attribute is deliberately rebound: the pool below
# looks DBProxy up on its own module at call time, so this is what makes it instantiate
# the JEDI proxy. JediDBProxy is the module, which is what DBProxy is there too
panda_db_proxy_pool.DBProxy = JediDBProxy  # type: ignore[attr-defined]


class DBProxyPool(panda_db_proxy_pool.DBProxyPool):
    # constructor. TaskBuffer.init builds the pool with the keyword arguments too, since JediTaskBuffer
    # puts this class where TaskBuffer looks the pool up
    def __init__(
        self,
        dbhost: str,
        dbpasswd: str,
        nConnection: int,
        useTimeout: bool = False,
        dbuser: str | None = None,
        call_timeout_ms: int | None = None,
        max_connect_attempts: int | None = None,
    ) -> None:
        panda_db_proxy_pool.DBProxyPool.__init__(
            self,
            dbhost,
            dbpasswd,
            nConnection,
            useTimeout,
            dbuser=dbuser,
            call_timeout_ms=call_timeout_ms,
            max_connect_attempts=max_connect_attempts,
        )
