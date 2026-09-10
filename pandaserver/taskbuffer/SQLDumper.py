from typing import Any, Iterator

from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger("SQLDumper")


# Every value here comes straight out of the database driver, which ships no stubs and is not
# installed in CI, so the cursor and everything it returns are Any whatever is written down
class SQLDumper(object):
    def __init__(self, cur: Any) -> None:
        self.cursor = cur

    def __iter__(self) -> Iterator[Any]:
        rows: Iterator[Any] = self.cursor.__iter__()
        return rows

    def next(self) -> Any:
        return self.cursor.next()

    def my_execute(self, sql: str, var: dict[str, Any] | None = None) -> Any:
        if var is None:
            var = {}
        _logger.debug(f"SQL={sql} var={str(var)}")
        return self.cursor.execute(sql, var)

    def my_executemany(self, sql: str, vars: list[dict[str, Any]] | None = None) -> Any:
        if vars is None:
            vars = []
        _logger.debug(f"SQL_many={sql} var_many={str(vars)}")
        return self.cursor.executemany(sql, vars)

    def __getattribute__(self, name: str) -> Any:
        if name == "execute":
            return object.__getattribute__(self, "my_execute")
        elif name == "executemany":
            return object.__getattribute__(self, "my_executemany")
        elif name in ["cursor", "__iter__", "next"]:
            return object.__getattribute__(self, name)
        else:
            return getattr(self.cursor, name)
