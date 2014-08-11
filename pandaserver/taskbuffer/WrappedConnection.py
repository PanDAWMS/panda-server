"""
WrappedConnection for a generic database connection proxy

"""

import re
import os
import sys
import time
# import fcntl
# import types
# import random
# import urllib
import socket
# import datetime
# import commands
import traceback
import warnings
try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None
try:
    import MySQLdb
except ImportError:
    MySQLdb = None
# import ErrorCode
# import SiteSpec
# import CloudSpec
# import PrioUtil
# import ProcessGroups
# from JobSpec  import JobSpec
# from FileSpec import FileSpec
# from DatasetSpec import DatasetSpec
# from CloudTaskSpec import CloudTaskSpec
from pandalogger.PandaLogger import PandaLogger
from config import panda_config
# from brokerage.PandaSiteIDs import PandaSiteIDs

warnings.filterwarnings('ignore')

# logger
_logger = PandaLogger().getLogger('DBProxyWrappedConnection')
#_logger = PandaLogger().getLogger('DBProxy')

# proxy
class WrappedConnection(object):
    # connection object
    conn = None
    # cursor
    cur = None
    # backend
    backend = 'oracle'
    # dbhost
    dbhost = 'INTR'
    # dbport
    dbport = 4444
    # dbpasswd
    dbpasswd = 'DONOTFIXME'
    #dbuser
    dbuser = 'ATLAS_PANDA'
    # dbname
    dbname = 'PandaDB'
    # dbtimeout
    dbtimeout = 60
    # threaded
    threaded = False
    # reconnect
    reconnect = False


    # constructor
    def __init__(self, backend='oracle', \
                        dbhost=panda_config.dbhost, dbpasswd=panda_config.dbpasswd,
                        dbuser=panda_config.dbuser, dbname=panda_config.dbname,
#                        dbtimeout=None, reconnect=False,
                        dbtimeout=60, reconnect=False,
                        dbengine=panda_config.dbengine, dbport=panda_config.dbport,
                        threaded=False
                    ):
        # connection object
        self.conn = None
        # cursor object
        self.cur = None
        # backend
        self.backend = backend
        # dbhost
        self.dbhost = dbhost
        # dbport
        self.dbport = dbport
        # dbpasswd
        self.dbpasswd = dbpasswd
        #dbuser
        self.dbuser = dbuser
        # dbname
        self.dbname = dbname
        # dbtimeout
        self.dbtimeout = dbtimeout
        # threaded
        self.threaded = threaded
        # reconnect
        self.reconnect = reconnect

        # imported cx_Oracle, MySQLdb?
        _logger.info('cx_Oracle=%s' % str(cx_Oracle))
        _logger.info('MySQLdb=%s' % str(MySQLdb))
        # connect, update self.conn
        self._connect(dbhost, dbpasswd, dbuser, dbname, dbtimeout, reconnect, dbport, threaded)
        ### initiated WrappedConnection


    # connect to DB
    def _connect(self, dbhost=panda_config.dbhost, dbpasswd=panda_config.dbpasswd,
                dbuser=panda_config.dbuser, dbname=panda_config.dbname,
#                dbtimeout=None, reconnect=False, dbport=panda_config.dbport,
                dbtimeout=60, reconnect=False, dbport=panda_config.dbport,
                threaded=False):
        _logger.debug("connect : re=%s" % reconnect)
        # keep parameters for reconnect
        _logger.debug("connect : backend = %s" % self.backend)
        if not reconnect:
            # configure connection and backend-related properties
            if str(self.backend) == str('mysql'):
                self._connectConfigMySQL(dbhost, dbpasswd, dbuser, dbname, dbtimeout, threaded, dbport)
            else:  # oracle
                self._connectConfigOracle(dbhost, dbpasswd, dbuser, dbname, dbtimeout, threaded)
        # close old connection
        if reconnect:
            _logger.debug("closing old connection")
            try:
#                self.conn.close()
                self.close()
            except:
                _logger.debug("failed to close old connection")
        # connect
        connectBackend = { \
            'oracle': self._connectOracle, \
            'mysql': self._connectMySQL \
        }
        if self.backend in connectBackend.keys():
            return connectBackend[self.backend]()
        else:
            return connectBackend['oracle']()


    def _connectConfigOracle(self, dbhost=panda_config.dbhost, dbpasswd=panda_config.dbpasswd,
                dbuser=panda_config.dbuser, dbname=panda_config.dbname,
#                dbtimeout=None, threaded=False):
                dbtimeout=60, threaded=False):
        self.dbhost = dbhost
        self.dbpasswd = dbpasswd
        self.dbuser = dbuser
        self.dbname = dbname
        self.dbtimeout = dbtimeout
        self.threaded = threaded


    def _connectConfigMySQL(self, dbhost=panda_config.dbhostmysql, dbpasswd=panda_config.dbpasswdmysql,
                dbuser=panda_config.dbusermysql, dbname=panda_config.dbnamemysql,
#                dbtimeout=None, threaded=False, dbport=panda_config.dbportmysql):
                dbtimeout=60, threaded=False, dbport=panda_config.dbportmysql):
        self.dbhost = dbhost
        self.dbpasswd = dbpasswd
        self.dbuser = dbuser
        self.dbname = dbname
        self.dbtimeout = dbtimeout
        self.threaded = threaded
        self.dbport = dbport


    # connect to Oracle dbengine
    def _connectOracle(self):
        _logger.debug("WrappedConnection._connectOracle : re=%s" % self.reconnect)
        # connect
        try:
            self.conn = cx_Oracle.connect(dsn=self.dbhost, user=self.dbuser,
                                          password=self.dbpasswd, threaded=self.threaded)
            self.cur = self.conn.cursor()
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("WrappedConnection._connectOracle : %s %s" % (type, value))
            return False


    # connect to MySQL dbengine
    def _connectMySQL(self):
        _logger.debug("WrappedConnection._connectMySQL : re=%s" % self.reconnect)
        # connect
        try:
            self.conn = MySQLdb.connect(host=self.dbhost, db=self.dbname, \
                                   port=self.dbport, connect_timeout=self.dbtimeout, \
                                   user=self.dbuser, passwd=self.dbpasswd)
            self.cur = self.conn.cursor()
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("WrappedConnection._connectMySQL : %s %s" % (type, value))
            return False


    # __setattr__
    def __setattr__(self, name, value):
        super(WrappedConnection, self).__setattr__(name, value)

    # __getattr__
    def __getattr__(self, name):
        _logger.debug('mark')
        return super(WrappedConnection, self).__getattr__(name)

    # serialize
    def __str__(self):
        return 'WrappedConnection[%(backend)s://%(dbuser)s@%(dbhost)s:%(dbport)s/%(dbname)s;timeout:%(dbtimeout)s,threaded:%(threaded)s]' \
                % ({'backend': self.backend, 'dbuser': self.dbuser, \
                    'dbhost': self.dbhost, 'dbport': self.dbport, \
                    'dbname': self.dbname, 'dbtimeout': self.dbtimeout, \
                    'threaded': self.threaded})

    # begin
    def begin(self):
        return self.conn.begin()

    # close
    def close(self):
        return self.conn.close()

    # disconnect
    def disconnect(self):
        return self.close()

    # ping
    def ping(self):
        return self.conn.ping()

    # commit
    def commit(self):
        return self.conn.commit()

    # rollback
    def rollback(self):
        return self.conn.rollback()

    # cursor
    def cursor(self):
        ret = self.cur
        if self.cur is None:
            if self.conn is not None:
                ret = self.conn.cursor()
        return ret

    # dictcursor
    def dictcursor(self):
        # TODO: implement WrappedDictCursor(WrappedCursor)
        return NotImplemented

    # setCursor
    def setCursor(self, cur):
        self.cur = cur

    # wakeUp
    def wakeUp(self):
        for iTry in range(5):
            try:
                # check if the connection is working
                self.ping()
                return
            except:
                type, value, traceBack = sys.exc_info()
                _logger.debug("wakeUp %d : %s %s" % (iTry, type, value))
                # wait for reconnection
                time.sleep(1)
                self._connect(reconnect=True)

    # connection
    def connection(self):
        return self.conn
