from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger("SQLDumper")


class SQLDumper(object):
    def __init__(self, cur):
        self.cursor = cur

    def __iter__(self):
        return self.cursor.__iter__()

    def next(self):
        return self.cursor.next()

    def my_execute(self, sql, var=None):
        if var is None:
            var = {}
        _logger.debug(f"SQL={sql} var={str(var)}")
        return self.cursor.execute(sql, var)

    def my_executemany(self, sql, vars=None):
        if vars is None:
            vars = []
        _logger.debug(f"SQL_many={sql} var_many={str(vars)}")
        return self.cursor.executemany(sql, vars)

    def __getattribute__(self, name):
        if name == "execute":
            return object.__getattribute__(self, "my_execute")
        elif name == "executemany":
            return object.__getattribute__(self, "my_executemany")
        elif name in ["cursor", "__iter__", "next"]:
            return object.__getattribute__(self, name)
        else:
            return getattr(self.cursor, name)
