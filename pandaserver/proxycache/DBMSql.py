import cx_Oracle

import sys
from types import TupleType

from pandaserver.config import panda_config

# Common items
ORAC_CON = None


OracleList = ['oracle']


#
# Connection initializers
#

def getOracleConnection(db_type) :
    global ORAC_CON
    if ORAC_CON is not None :
        return ORAC_CON
    user = panda_config.dbuser
    pssw = panda_config.dbpasswd
    serv = panda_config.dbhost
    poolmin = 1
    poolmax = 3
    poolincr = 1

    print("Initializing Oracle connection")
    try :
        cpool = cx_Oracle.SessionPool(user, pssw, serv, poolmin, poolmax, poolincr)
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

        if self.db_type in OracleList :
            self.oracle_con = getOracleConnection(self.db_type)
        else:
            print("Unknown database type:" + self.db_type)
            print("ERROR. Unknown database type", self.db_type)
            sys.exit(1)

        print("Database connection created for " + self.db_type)

    # Method to execute sql query (SELECT)
    def executeQuery(self, sql):
        bindDict = None
        if isinstance(sql, TupleType) :
            bindDict = sql[1]
            sql = sql[0]
        if sql.strip()[0:6].lower() != "select":
            print("not a SELECT statement!!")
            return []
        try:
            if self.db_type in OracleList :
                cursor = self.oracle_con.cursor()
                if bindDict is not None :
                    cursor.execute(sql, bindDict)
                else :
                    cursor.execute(sql)
                colMap =  cursor.description
                result = cursor.fetchall()
                ret = []
                for t in result:
                    i = 0
                    d = {}
                    for e in t:
                        if str(type(e)) == "<type 'cx_Oracle.Timestamp'>":
                            e = str(e)
                        d[colMap[i][0]] = e
                        i = i + 1
                    ret.append(d)
                cursor.close()
                return ret

            else:
                raise DBMSqlError("not support:%s" % (self.db_type))
        except Exception:
            raise DBMSqlError("executeQuery error:%s \n %s" % (sys.exc_info()[1], sql))


class DBMSqlError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

# -----------------------------
# Codes below are for test...
#

if __name__ == '__main__':

    dbm = DBMSql(_db_type='oracle')
    print(dbm.executeQuery('select * from services'))
