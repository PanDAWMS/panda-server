"""
WrappedCursor for a generic database connection proxy

"""

import re
import os
import warnings
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config

try:
    long
except NameError:
    long = int

warnings.filterwarnings('ignore')

# logger
_logger = PandaLogger().getLogger('WrappedCursor')


# convert SQL and parameters in_printf format
def convert_query_in_printf_format(sql, var_dict):
    # %
    sql = re.sub(r'%', r'%%', sql)
    # current date except for being used for interval
    if re.search(r'CURRENT_DATE\s*[\+-]', sql, flags=re.IGNORECASE) is None:
        sql = re.sub(r'CURRENT_DATE', r'CURRENT_TIMESTAMP', sql, flags=re.IGNORECASE)
    # sequence
    sql = re.sub(r"""([^ $,()]+).currval""", r"currval('\1')", sql, flags=re.IGNORECASE)
    sql = re.sub(r"""([^ $,()]+).nextval""", r"nextval('\1')", sql, flags=re.IGNORECASE)
    # returning
    sql = re.sub(r"(RETURNING\s+\S+\s+)INTO\s+\S+", r"\1", sql, flags=re.IGNORECASE)
    # sub query + rownum
    sql = re.sub(r"\)\s+WHERE\s+rownum", r") tmp_sub WHERE rownum", sql, flags=re.IGNORECASE)
    # sub query + GROUP BY
    if re.search(r'FROM\s+\(SELECT', sql, flags=re.IGNORECASE):
        sql = re.sub(r"\)\s+GROUP\s+BY", r") tmp_sub GROUP BY", sql, flags=re.IGNORECASE)
    # rownum
    sql = re.sub(r"(WHERE|AND)\s+rownum[^\d:]+(\d+|:[^ \)]+)", r" LIMIT \2", sql, flags=re.IGNORECASE)
    # NVL
    sql = re.sub(r"NVL\(", r"COALESCE(", sql, flags=re.IGNORECASE)
    # random
    sql = re.sub(r"DBMS_RANDOM.value", r"RANDOM()", sql, flags=re.IGNORECASE)
    # GENERATE_SERIES
    sql = re.sub(r'\(SELECT\s+level\s+FROM\s+dual\s+CONNECT\s+BY\s+level\s*<=\s*(:[^ \)]+)\)*',
                 r'GENERATE_SERIES(1,\1)',
                 sql, flags=re.IGNORECASE)
    # extract placeholders
    paramList = []
    items = re.findall(r':[^ $,)\n]+', sql)
    for item in items:
        if item not in var_dict:
            raise KeyError('{0} is missing in SQL parameters'.format(item))
        if item not in paramList:
            paramList.append(var_dict[item])
    # using the printf style syntax
    sql = re.sub(':[^ $,)]+', '%s', sql)
    return sql, paramList


# proxy
class WrappedCursor(object):

    # constructor
    def __init__(self, connection):
        # connection object
        self.conn = connection
        # cursor object
        self.cur = self.conn.cursor()
        # backend
        self.backend = panda_config.backend
        # statement
        self.statement = None
        # dump
        if hasattr(panda_config, 'cursor_dump') and panda_config.cursor_dump:
            self.dump = True
        else:
            self.dump = False


    # __iter__
    def __iter__(self):
        return iter(self.cur)


    # serialize
    def __str__(self):
        return 'WrappedCursor[%(conn)s]' % ({'conn': self.conn})

    # initialize
    def initialize(self):
        hostname = None
        if self.backend == 'oracle':
            # get hostname
            self.execute("SELECT SYS_CONTEXT('USERENV','HOST') FROM dual")
            res = self.fetchone()
            if res is not None:
                hostname = res[0]
            # set TZ
            self.execute("ALTER SESSION SET TIME_ZONE='UTC'")
            # set DATE format
            self.execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY/MM/DD HH24:MI:SS'")
            # set Oracle optimizer version. This is done only temporarily for controlled migration to 19c
            self.execute("ALTER SESSION SET optimizer_features_enable='19.1.0'")

        elif self.backend == 'postgres':
            # disable autocommit
            # make sure that always have commit() since any query execution, including SELECT will start a transaction
            self.conn.set_session(autocommit=False)
            # encoding
            self.conn.set_client_encoding('UTF-8')
            # TZ
            self.execute("SET timezone=0")
        else:
            # get hostname
            self.execute("SELECT SUBSTRING_INDEX(USER(),'@',-1)")
            res = self.fetchone()
            if res is not None:
                hostname = res[0]
            # set TZ
            self.execute("SET @@SESSION.TIME_ZONE = '+00:00'")
            # set DATE format
            #self.execute("SET @@SESSION.DATETIME_FORMAT='%%Y/%%m/%%d %%H:%%i:%%s'")
            # disable autocommit
            self.execute("SET autocommit=0")
        return hostname

    # execute query on cursor
    def execute(self, sql, varDict=None, cur=None  # , returningInto=None
                ):
        if varDict is None:
            varDict = {}
        if cur is None:
            cur = self.cur
        ret = None
        # schema names
        sql = re.sub('ATLAS_PANDA\.', panda_config.schemaPANDA + '.', sql)
        sql = re.sub('ATLAS_PANDAMETA\.', panda_config.schemaMETA + '.', sql)
        sql = re.sub('ATLAS_GRISLI\.', panda_config.schemaGRISLI + '.', sql)
        sql = re.sub('ATLAS_PANDAARCH\.', panda_config.schemaPANDAARCH + '.', sql)
        # remove `
        sql = re.sub('`', '', sql)
        if self.backend == 'oracle':
            ret = cur.execute(sql, varDict)
        elif self.backend == 'postgres':
            if self.dump:
                _logger.debug('OLD: {} {}'.format(sql, str(varDict)))
            sql, varList = convert_query_in_printf_format(sql, varDict)
            if self.dump:
                _logger.debug('NEW: {} {}'.format(sql, str(varList)))
            ret = cur.execute(sql, varList)
        elif self.backend == 'mysql':
            print("DEBUG execute : original SQL     %s " % sql)
            print("DEBUG execute : original varDict %s " % varDict)
            # CURRENT_DATE interval
            sql = re.sub("CURRENT_DATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(CURRENT_TIMESTAMP,INTERVAL \g<1> DAY)", sql)
            #CURRENT_DATE
            sql = re.sub('CURRENT_DATE', 'CURRENT_TIMESTAMP', sql)
            # SYSDATE interval
            sql = re.sub("SYSDATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(SYSDATE,INTERVAL \g<1> DAY)", sql)
            # SYSDATE
            sql = re.sub('SYSDATE', 'SYSDATE()', sql)
            # EMPTY_CLOB()
            sql = re.sub('EMPTY_CLOB\(\)', "''", sql)
            # ROWNUM
            sql = re.sub("(?i)(AND)*\s*ROWNUM.*(\d+)", " LIMIT \g<2>", sql)
            sql = re.sub("(?i)(WHERE)\s*LIMIT\s*(\d+)", " LIMIT \g<2>" , sql)
            # NOWAIT
            sql = re.sub('NOWAIT', "", sql)
            # RETURNING INTO
            returningInto = None
            m = re.search("RETURNING ([^\s]+) INTO ([^\s]+)", sql, re.I)
            if m is not None:
                returningInto = [{'returning': m.group(1), 'into': m.group(2)}]
                self._returningIntoMySQLpre(returningInto, varDict, cur)
                sql = re.sub(m.group(0), '', sql)
            # Addressing sequence
            if "INSERT" in sql:
                sql = re.sub('[a-zA-Z\._]+\.nextval','NULL',sql)
            # schema names
            sql = re.sub('ATLAS_PANDA\.',     panda_config.schemaPANDA + '.',     sql)
            sql = re.sub('ATLAS_PANDAMETA\.', panda_config.schemaMETA + '.',      sql)
            sql = re.sub('ATLAS_GRISLI\.',    panda_config.schemaGRISLI + '.',    sql)
            sql = re.sub('ATLAS_PANDAARCH\.', panda_config.schemaPANDAARCH + '.', sql)
            # bind variables
            newVarDict = {}
            # make sure that :prodDBlockToken will not be replaced by %(prodDBlock)sToken
            keys = sorted(list(varDict), key=lambda s:-len(str(s)))
            for key in keys:
                val = varDict[key]
                if key[0] == ':':
                    newKey = key[1:]
                    sql = sql.replace(key, '%(' + newKey + ')s')
                else:
                    newKey = key
                    sql = sql.replace(':' + key, '%(' + newKey + ')s')
                newVarDict[newKey] = val
            try:
                # from PanDA monitor it is hard to log queries sometimes, so let's debug with hardcoded query dumps
                import time
                if os.path.exists('/data/atlpan/oracle/panda/monitor/logs/write_queries.txt'):
                    f = open('/data/atlpan/oracle/panda/monitor/logs/mysql_queries_WrappedCursor.txt', 'a')
                    f.write('mysql|%s|%s|%s\n' % (str(time.time()), str(sql), str(newVarDict)))
                    f.close()
            except Exception:
                pass
            _logger.debug("execute : SQL     %s " % sql)
            _logger.debug("execute : varDict %s " % newVarDict)
            print("DEBUG execute : SQL     %s " % sql)
            print("DEBUG execute : varDict %s " % newVarDict)
            ret = cur.execute(sql, newVarDict)
            if returningInto is not None:
                ret = self._returningIntoMySQLpost(returningInto, varDict, cur)
        return ret


    def _returningIntoOracle(self, returningInputData, varDict, cur, dryRun=False):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        result = ''
        if returningInputData is not None:
            try:
                valReturning = str(',').join([x['returning'] for x in returningInputData])
                listInto = [x['into'] for x in returningInputData]
                valInto = str(',').join(listInto)
                # assuming that we use RETURNING INTO only for PandaID or row_ID columns
                if not dryRun:
                    for x in listInto:
                        varDict[x] = cur.var(cx_Oracle.NUMBER)
                result = ' RETURNING %(returning)s INTO %(into)s ' % {'returning': valReturning, 'into': valInto}
            except Exception:
                pass
        return result


    def _returningIntoMySQLpre(self, returningInputData, varDict, cur):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        if returningInputData is not None:
            try:
                # get rid of "returning into" items in varDict
                listInto = [x['into'] for x in returningInputData]
                for x in listInto:
                    try:
                        del varDict[x]
                    except KeyError:
                        pass
                if len(returningInputData) == 1:
                    # and set original value in varDict to null, let auto_increment do the work
                    listReturning = [x['returning'] for x in returningInputData]
                    for x in listReturning:
                        varDict[':' + x] = None
            except Exception:
                pass

    def _returningIntoMySQLpost(self, returningInputData, varDict, cur):
        # returningInputData=[{'returning': 'PandaID', 'into': ':newPandaID'}, {'returning': 'row_ID', 'into': ':newRowID'}]
        result = long(0)
        if len(returningInputData) == 1:
            ret = self.cur.execute(""" SELECT LAST_INSERT_ID() """)
            result, = self.cur.fetchone()
            if returningInputData is not None:
                try:
                    # update of "returning into" items in varDict
                    listInto = [x['into'] for x in returningInputData]
                    for x in listInto:
                        try:
                            varDict[x] = long(result)
                        except KeyError:
                            pass
                except Exception:
                    pass
        return result

    # fetchall
    def fetchall(self):
        return self.cur.fetchall()


    # fetchmany
    def fetchmany(self, arraysize=1000):
        self.cur.arraysize = arraysize
        return self.cur.fetchmany()


    # fetchall
    def fetchone(self):
        return self.cur.fetchone()


    # var
    def var(self, dataType, *args, **kwargs):
        if self.backend == 'mysql':
            return apply(dataType,[0])
        elif self.backend == 'postgres':
            return None
        else:
            return self.cur.var(dataType, *args, **kwargs)


    # get value
    def getvalue(self,dataItem):
        if self.backend == 'mysql':
            return dataItem
        elif self.backend == 'postgres':
            return self.cur.fetchone()[0]
        else:
            return dataItem.getvalue()


    # next
    def next(self):
        if self.backend == 'mysql':
            return self.cur.fetchone()
        else:
            return self.cur.next()


    # close
    def close(self):
        return self.cur.close()

    # prepare
    def prepare(self, statement):
        self.statement = statement

    # executemany
    def executemany(self, sql, params):
        if sql is None:
            sql = self.statement
        sql = self.change_schema(sql)
        if self.backend == 'oracle':
            self.cur.executemany(sql,params)
        else: 
            for paramsItem in params:
                self.execute(sql, paramsItem)

    # get_description
    @property
    def description(self):
        return self.cur.description

    # rowcount
    @property
    def rowcount(self):
        return self.cur.rowcount

    # arraysize
    @property
    def arraysize(self):
        return self.cur.arraysize

    @arraysize.setter
    def arraysize(self,val):
        self.cur.arraysize = val

    # change schema
    def change_schema(self, sql):
        if panda_config.schemaPANDA != 'ATLAS_PANDA':
            sql = re.sub('ATLAS_PANDA\.',
                         panda_config.schemaPANDA + '.', sql)
        if panda_config.schemaMETA != 'ATLAS_PANDAMETA':
            sql = re.sub('ATLAS_PANDAMETA\.',
                         panda_config.schemaMETA + '.', sql)
        if panda_config.schemaGRISLI != 'ATLAS_GRISLI':
            sql = re.sub('ATLAS_GRISLI\.',
                         panda_config.schemaGRISLI + '.', sql)
        if panda_config.schemaPANDAARCH != 'ATLAS_PANDAARCH':
            sql = re.sub('ATLAS_PANDAARCH\.',
                         panda_config.schemaPANDAARCH + '.', sql)
        return sql
