import sys
from types import TupleType

import oracledb

oracledb.init_oracle_client()
from pandaserver.config import panda_config

# Common items
ORAC_CON = None


OracleList = ["oracle"]


#
# Connection initializers
#


def getOracleConnection(db_type):
    global ORAC_CON
    if ORAC_CON is not None:
        return ORAC_CON
    user = panda_config.dbuser
    pssw = panda_config.dbpasswd
    serv = panda_config.dbhost
    poolmin = 1
    poolmax = 3
    poolincr = 1

    print("Initializing Oracle connection")
    try:
        cpool = oracledb.create_pool(user=user, password=pssw, dsn=serv, min=poolmin, max=poolmax, increment=poolincr)
        ORAC_CON = cpool.acquire()
    except Exception:
        import traceback

        traceback.print_stack()
        traceback.print_exc()
        sys.exit(1)
    return ORAC_CON


class DBMSql:
    db_type = ""
    oracle_con = None

    # constructor with database type [sqlite|postgres]
    def __init__(self, _db_type):
        self.db_type = _db_type

        if self.db_type in OracleList:
            self.oracle_con = getOracleConnection(self.db_type)
        else:
            print("Unknown database type:" + self.db_type)
            print("ERROR. Unknown database type", self.db_type)
            sys.exit(1)

        print("Database connection created for " + self.db_type)

    # Method to execute sql query (SELECT)
    def executeQuery(self, sql):
        bindDict = None
        if isinstance(sql, TupleType):
            bindDict = sql[1]
            sql = sql[0]
        if sql.strip()[0:6].lower() != "select":
            print("not a SELECT statement!!")
            return []
        try:
            if self.db_type in OracleList:
                with self.oracle_con.cursor() as cursor:
                    if bindDict is not None:
                        cursor.execute(sql, bindDict)
                    else:
                        cursor.execute(sql)
                    colMap = cursor.description
                    result = cursor.fetchall()
                    ret = []
                    for t in result:
                        i = 0
                        d = {}
                        for e in t:
                            if isinstance(e, oracledb.Timestamp):
                                e = str(e)
                            d[colMap[i][0]] = e
                            i = i + 1
                        ret.append(d)

                return ret

            else:
                raise DBMSqlError(f"not support:{self.db_type}")
        except Exception:
            raise DBMSqlError(f"executeQuery error:{sys.exc_info()[1]} \n {sql}")


class DBMSqlError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


# -----------------------------
# Codes below are for test...
#

if __name__ == "__main__":
    dbm = DBMSql(_db_type="oracle")
    print(dbm.executeQuery("select * from services"))
