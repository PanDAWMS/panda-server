"""
WrappedCursor for a generic database connection proxy

"""

import re
import os
import sys
import warnings
from pandalogger.PandaLogger import PandaLogger
from config import panda_config

warnings.filterwarnings('ignore')

# logger
_logger = PandaLogger().getLogger('WrappedCursor')

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


    # __iter__
    def __iter__(self):
        return iter(self.cur)


    # serialize
    def __str__(self):
        return 'WrappedCursor[%(conn)s]' % ({'conn': self.conn})


    # execute query on cursor
    def execute(self, sql, varDict=None, cur=None  # , returningInto=None
                ):
        if varDict is None:
            varDict = {}
        if cur is None:
            cur = self.cur
        ret = None
        if self.backend == 'oracle':
            # schema names
            sql = re.sub('ATLAS_PANDA\.',     panda_config.schemaPANDA + '.',     sql)
            sql = re.sub('ATLAS_PANDAMETA\.', panda_config.schemaMETA + '.',      sql)
            sql = re.sub('ATLAS_GRISLI\.',    panda_config.schemaGRISLI + '.',    sql)
            sql = re.sub('ATLAS_PANDAARCH\.', panda_config.schemaPANDAARCH + '.', sql)

            # remove `
            sql = re.sub('`','',sql)
            ret = cur.execute(sql, varDict)
        elif self.backend == 'mysql':
            print "DEBUG execute : original SQL     %s " % sql
            print "DEBUG execute : original varDict %s " % varDict
            # CURRENT_DATE interval
            sql = re.sub("CURRENT_DATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(CURRENT_DATE,INTERVAL \g<1> DAY)", sql)
            # SYSDATE interval
            sql = re.sub("SYSDATE\s*-\s*(\d+|:[^\s\)]+)", "DATE_SUB(SYSDATE,INTERVAL \g<1> DAY)", sql)
            # SYSDATE
            sql = re.sub('SYSDATE', 'SYSDATE()', sql)
            # EMPTY_CLOB()
            sql = re.sub('EMPTY_CLOB\(\)', "''", sql)
            # ROWNUM
            sql = re.sub("(?i)(AND)*\s*ROWNUM\s*<=\s*(\d+)", " LIMIT \g<2>", sql)
            sql = re.sub("(?i)(WHERE)\s*LIMIT\s*(\d+)", " LIMIT \g<2>" , sql)
            # RETURNING INTO
            returningInto = None
            m = re.search("RETURNING ([^\s]+) INTO ([^\s]+)", sql, re.I)
            if m is not None:
                returningInto = [{'returning': m.group(1), 'into': m.group(2)}]
                self._returningIntoMySQLpre(returningInto, varDict, cur)
                sql = re.sub(m.group(0), '', sql)
            # schema names
            sql = re.sub('ATLAS_PANDA\.',     panda_config.schemaPANDA + '.',     sql)
            sql = re.sub('ATLAS_PANDAMETA\.', panda_config.schemaMETA + '.',      sql)
            sql = re.sub('ATLAS_GRISLI\.',    panda_config.schemaGRISLI + '.',    sql)
            sql = re.sub('ATLAS_PANDAARCH\.', panda_config.schemaPANDAARCH + '.', sql)
            # bind variables
            newVarDict = {}
            # make sure that :prodDBlockToken will not be replaced by %(prodDBlock)sToken
            keys = sorted(varDict.keys(), key=lambda s:-len(str(s)))
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
            except:
                pass
            _logger.debug("execute : SQL     %s " % sql)
            _logger.debug("execute : varDict %s " % newVarDict)
            print "DEBUG execute : SQL     %s " % sql
            print "DEBUG execute : varDict %s " % newVarDict
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
            except:
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
            except:
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
                except:
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
        else:
            return self.cur.var(dataType, *args, **kwargs)


    # get value
    def getvalue(self,dataItem):
        if self.backend == 'mysql':
            return dataItem
        else:
            return dataItem.getvalue()


    # next
    def next(self):
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




