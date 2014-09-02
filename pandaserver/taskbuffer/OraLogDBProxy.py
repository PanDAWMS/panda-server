"""
proxy for log database connection

"""

import re
import sys
import time

# logger
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('LogDBProxy')

try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None
    _logger.error('Cannot import cx_Oracle')
try:
    import MySQLdb
except ImportError:
    MySQLdb = None
    _logger.error('Cannot import MySQLdb')
from WrappedCursor import WrappedCursor
from WrappedConnection import WrappedConnection

from config import panda_config

import SiteSpec
import CloudSpec

from JobSpec  import JobSpec
from FileSpec import FileSpec


# proxy
class LogDBProxy:

    # constructor
    def __init__(self):
        # connection object
        self.conn = None
        # cursor object
        self.cur = None
        # backend
        self.backend = 'oracle'
        # schema name, PANDA
        self.schemaPANDA = 'ATLAS_PANDA'
        # schema name, PANDAMETA
        self.schemaMETA = 'ATLAS_PANDAMETA'
        # schema name, GRISLI
        self.schemaGRISLI = 'ATLAS_GRISLI'
        # schema name, PANDAARCH
        self.schemaPANDAARCH = 'ATLAS_PANDAARCH'
        # schema name, JEDI
        schemaJEDI = 'ATLAS_PANDA'
        # schema name, DEFT
        schemaDEFT = 'ATLAS_DEFT'
        # imported cx_Oracle, MySQLdb?
        _logger.info('cx_Oracle=%s' % str(cx_Oracle))
        _logger.info('MySQLdb=%s' % str(MySQLdb))


    # connect to DB
    def connect(self, dbhost=panda_config.logdbhost, dbpasswd=panda_config.logdbpasswd,
                dbuser=panda_config.logdbuser, dbname=panda_config.logdbname,
                dbtimeout=None, reconnect=False,
                dbengine=panda_config.logdbengine, dbport=panda_config.logdbport):
        _logger.debug("connect : re=%s" % reconnect)
        # keep parameters for reconnect
        # backend
        self.backend = dbengine
        _logger.debug("connect : backend = %s" % self.backend)
        if not reconnect:
            # configure connection and backend-related properties
            if str(self.backend) == str('mysql'):
                self._connectConfigMySQL()
            else:  # oracle
                self._connectConfigOracle(dbhost, dbpasswd, dbuser, dbname, dbtimeout)
        # close old connection
        if reconnect:
            _logger.debug("closing old connection")
            try:
                self.conn.close()
            except:
                _logger.debug("failed to close old connection")
        # connect
        connectBackend = { \
            'oracle': self._connectOracle, \
            'mysql': self._connectMySQL \
        }
        if self.backend in connectBackend.keys():
            return connectBackend[self.backend](self.dbhost, self.dbpasswd, self.dbuser, self.dbname, self.dbtimeout, reconnect, self.backend, self.dbport)
        else:
            return connectBackend['oracle'](self.dbhost, self.dbpasswd, self.dbuser, self.dbname, self.dbtimeout, reconnect, self.backend, self.dbport)


    def _connectConfigOracle(self, dbhost=panda_config.logdbhost, dbpasswd=panda_config.logdbpasswd,
                dbuser=panda_config.logdbuser, dbname=panda_config.logdbname,
                dbtimeout=None, reconnect=False):
        self.dbhost = dbhost
        self.dbpasswd = dbpasswd
        self.dbuser = dbuser
        self.dbname = dbname
        self.dbtimeout = dbtimeout
        self.dbport = None

        if hasattr(panda_config, 'schemaPANDA'):
            self.schemaPANDA = panda_config.schemaPANDA
        if hasattr(panda_config, 'schemaMETA'):
            self.schemaMETA = panda_config.schemaMETA
        if hasattr(panda_config, 'schemaGRISLI'):
            self.schemaGRISLI = panda_config.schemaGRISLI
        if hasattr(panda_config, 'schemaPANDAARCH'):
            self.schemaPANDAARCH = panda_config.schemaPANDAARCH


    def _connectConfigMySQL(self):
        if hasattr(panda_config, 'dbtimeout'):
            self.dbtimeout = panda_config.dbtimeout

        if hasattr(panda_config, 'logdbhost'):
            self.dbhost = panda_config.logdbhost
        if hasattr(panda_config, 'logdbpasswd'):
            self.dbpasswd = panda_config.logdbpasswd
        if hasattr(panda_config, 'logdbuser'):
            self.dbuser = panda_config.logdbuser
        if hasattr(panda_config, 'logdbname'):
            self.dbname = panda_config.logdbname
            self.schemaPANDA = panda_config.logdbname
            self.schemaMETA = panda_config.logdbname
            self.schemaGRISLI = panda_config.logdbname
            self.schemaPANDAARCH = panda_config.logdbname
        if hasattr(panda_config, 'logdbport'):
            self.dbport = panda_config.logdbport


    # connect to Oracle dbengine
    def _connectOracle(self, dbhost=panda_config.dbhost, dbpasswd=panda_config.dbpasswd,
                dbuser=panda_config.dbuser, dbname=panda_config.dbname,
                dbtimeout=None, reconnect=False,
                dbengine=None, dbport=None):
        _logger.debug("_connectOracle : re=%s" % reconnect)
        # connect
        try:
            _logger.debug("mark")
#            self.conn = cx_Oracle.connect(dsn=self.dbhost, user=self.dbuser,
#                                          password=self.dbpasswd, threaded=True)
            self.conn = WrappedConnection(backend=self.backend, reconnect=reconnect,
                                          dbhost=self.dbhost, dbuser=self.dbuser,
                                          dbpasswd=self.dbpasswd, threaded=True)
            _logger.debug("mark")
            _logger.debug("conn=" + str(self.conn))
            _logger.debug("mark")
#            self.cur = self.conn.cursor()
            self.cur = WrappedCursor(self.conn)
            self.conn.setCursor(self.cur)
            _logger.debug("mark")
            _logger.debug("cur=" + str(self.cur))
            _logger.debug("mark")
            # set TZ
            self.cur.execute("ALTER SESSION SET TIME_ZONE='UTC'")
            # set DATE format
            self.cur.execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY/MM/DD HH24:MI:SS'")
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("_connectOracle : %s %s" % (type, value))
            return False


    # connect to MySQL dbengine
    def _connectMySQL(self, dbhost=panda_config.logdbhost, dbpasswd=panda_config.logdbpasswd,
                dbuser=panda_config.logdbuser, dbname=panda_config.logdbname,
                dbtimeout=None, reconnect=False,
                dbengine=panda_config.logdbengine, dbport=panda_config.logdbport):
        """ MySQLmigrateD: connect """
        _logger.debug("_connectMySQL : re=%s" % reconnect)

        # connect
        try:
#            self.conn = MySQLdb.connect(host=self.dbhost, db=self.dbname, \
#                                   port=self.dbport, connect_timeout=self.dbtimeout, \
#                                   user=self.dbuser, passwd=self.dbpasswd
#                                   )
            _logger.debug("mark")
            self.conn = WrappedConnection(backend=self.backend, dbname=self.dbname,
                                          dbhost=self.dbhost, dbuser=self.dbuser,
                                          dbport=self.dbport, dbtimeout=self.dbtimeout,
                                          dbpasswd=self.dbpasswd, reconnect=reconnect)
            _logger.debug("mark")
            _logger.debug("conn=" + str(self.conn))
            _logger.debug("mark")
#            self.cur = self.conn.cursor()
            self.cur = WrappedCursor(self.conn)
            self.conn.setCursor(self.cur)
            _logger.debug("mark")
            _logger.debug("cur=" + str(self.cur))
            _logger.debug("mark")
            # set TZ
            self.cur.execute("SET @@SESSION.TIME_ZONE = '+00:00'")

            # set DATE format
            self.cur.execute("SET @@SESSION.DATETIME_FORMAT='%%Y/%%m/%%d %%H:%%i:%%s'")

            # disable autocommit
            self.cur.execute("SET autocommit=0")

            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("_connectMySQL : %s %s" % (type, value))
            return False


    # query an SQL
    def querySQL(self,sql,arraySize=1000):
        try:
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = arraySize
            self.cur.execute(sql)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return res
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("querySQL : %s %s" % (type,value))
            return None


    # get site data
    def getCurrentSiteData(self):
        _logger.debug("getCurrentSiteData")
        sql = "SELECT SITE,getJob,updateJob FROM SiteData WHERE FLAG='production' and HOURS=3"
        try:
            # set autocommit on
            self.conn.begin()
            # select
            self.cur.arraysize = 10000
            self.cur.execute(sql)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            ret = {}
            for item in res:
                ret[item[0]] = {'getJob':item[1],'updateJob':item[2]}
            _logger.debug(ret)
            return ret
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getCurrentSiteData : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # get list of site
    def getSiteList(self):
        _logger.debug("getSiteList start")
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT siteid,nickname FROM schedconfig WHERE siteid IS NOT NULL"
            self.cur.arraysize = 10000
            self.cur.execute(sql)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retMap = {}
            if res != None and len(res) != 0:
                for siteid,nickname in res:
                    # skip invalid siteid
                    if siteid in [None,'']:
                        continue
                    # append
                    if not retMap.has_key(siteid):
                        retMap[siteid] = []
                    retMap[siteid].append(nickname)
            _logger.debug(retMap)
            _logger.debug("getSiteList done")
            return retMap
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getSiteList : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # get site info
    def getSiteInfo(self):
        _logger.debug("getSiteInfo start")
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT nickname,dq2url,cloud,ddm,lfchost,se,gatekeeper,releases,memory,"
            sql+= "maxtime,status,space,retry,cmtconfig,setokens,seprodpath,glexec,"
            sql += "priorityoffset,allowedgroups,defaulttoken,siteid,queue,localqueue "
            sql+= "FROM schedconfig WHERE siteid IS NOT NULL"
            self.cur.arraysize = 10000
            self.cur.execute(sql)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retList = {}
            if resList != None:
                # loop over all results
                for res in resList:
                    # change None to ''
                    resTmp = []
                    for tmpItem in res:
                        if tmpItem == None:
                            tmpItem = ''
                        resTmp.append(tmpItem)
                    nickname,dq2url,cloud,ddm,lfchost,se,gatekeeper,releases,memory,\
                       maxtime,status,space,retry,cmtconfig,setokens,seprodpath,glexec,\
                       priorityoffset,allowedgroups,defaulttoken,siteid,queue,localqueue \
                       = resTmp
                    # skip invalid siteid
                    if siteid in [None,'']:
                        continue
                    # instantiate SiteSpec
                    ret = SiteSpec.SiteSpec()
                    ret.sitename   = siteid
                    ret.nickname   = nickname
                    ret.dq2url     = dq2url
                    ret.cloud      = cloud
                    ret.ddm        = ddm.split(',')[0]
                    ret.lfchost    = lfchost
                    ret.se         = se
                    ret.gatekeeper = gatekeeper
                    ret.memory     = memory
                    ret.maxtime    = maxtime
                    ret.status     = status
                    ret.space      = space
                    ret.glexec     = glexec
                    ret.queue      = queue
                    ret.localqueue = localqueue
                    # job recoverty
                    ret.retry = True
                    if retry == 'FALSE':
                        ret.retry = False
                    # convert releases to list
                    ret.releases = []
                    for tmpRel in releases.split('|'):
                        # remove white space
                        tmpRel = tmpRel.strip()
                        if tmpRel != '':
                            ret.releases.append(tmpRel)
                    # cmtconfig
                    # add slc3 if the column is empty
                    ret.cmtconfig = ['i686-slc3-gcc323-opt']
                    if cmtconfig != '':
                        ret.cmtconfig.append(cmtconfig)
                    # map between token and DQ2 ID
                    ret.setokens = {}
                    tmpTokens = setokens.split(',')
                    for idxToken,tmpddmID in enumerate(ddm.split(',')):
                        if idxToken < len(tmpTokens):
                            ret.setokens[tmpTokens[idxToken]] = tmpddmID
                    # expand [] in se path
                    match = re.search('([^\[]*)\[([^\]]+)\](.*)',seprodpath)
                    if match != None and len(match.groups()) == 3:
                        seprodpath = ''
                        for tmpBody in match.group(2).split(','):
                            seprodpath += '%s%s%s,' % (match.group(1),tmpBody,match.group(3))
                        seprodpath = seprodpath[:-1]
                    # map between token and se path
                    ret.seprodpath = {}
                    tmpTokens = setokens.split(',')
                    for idxToken,tmpSePath in enumerate(seprodpath.split(',')):
                        if idxToken < len(tmpTokens):
                            ret.seprodpath[tmpTokens[idxToken]] = tmpSePath
                    # VO related params
                    ret.priorityoffset = priorityoffset
                    ret.allowedgroups  = allowedgroups
                    ret.defaulttoken   = defaulttoken
                    # append
                    retList[ret.nickname] = ret
            _logger.debug("getSiteInfo done")
            return retList
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getSiteInfo : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # get cloud list
    def getCloudList(self):
        _logger.debug("getCloudList start")
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql  = "SELECT name,tier1,tier1SE,relocation,weight,server,status,transtimelo,"
            sql += "transtimehi,waittime,validation,mcshare,countries,fasttrack "
            sql+= "FROM cloudconfig"
            self.cur.arraysize = 10000
            self.cur.execute(sql)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            ret = {}
            if resList != None and len(resList) != 0:
                for res in resList:
                    # change None to ''
                    resTmp = []
                    for tmpItem in res:
                        if tmpItem == None:
                            tmpItem = ''
                        resTmp.append(tmpItem)
                    name,tier1,tier1SE,relocation,weight,server,status,transtimelo,transtimehi,\
                        waittime,validation,mcshare,countries,fasttrack = resTmp 
                    # instantiate CloudSpec
                    tmpC = CloudSpec.CloudSpec()
                    tmpC.name = name
                    tmpC.tier1 = tier1
                    tmpC.tier1SE = re.sub(' ','',tier1SE).split(',')
                    tmpC.relocation  = relocation
                    tmpC.weight      = weight
                    tmpC.server      = server
                    tmpC.status      = status
                    tmpC.transtimelo = transtimelo
                    tmpC.transtimehi = transtimehi
                    tmpC.waittime    = waittime
                    tmpC.validation  = validation
                    tmpC.mcshare     = mcshare
                    tmpC.countries   = countries
                    tmpC.fasttrack   = fasttrack
                    # append
                    ret[name] = tmpC
            _logger.debug("getCloudList done")
            return ret
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getCloudList : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # extract name from DN
    def cleanUserID(self, id):
        try:
            up = re.compile('/(DC|O|OU|C|L)=[^\/]+')
            username = up.sub('', id)
            up2 = re.compile('/CN=[0-9]+')
            username = up2.sub('', username)
            up3 = re.compile(' [0-9]+')
            username = up3.sub('', username)
            up4 = re.compile('_[0-9]+')
            username = up4.sub('', username)
            username = username.replace('/CN=proxy','')
            username = username.replace('/CN=limited proxy','')
            username = username.replace('limited proxy','')
            pat = re.compile('.*/CN=([^\/]+)/CN=([^\/]+)')
            mat = pat.match(username)
            if mat:
                username = mat.group(2)
            else:
                username = username.replace('/CN=','')
            if username.lower().find('/email') > 0:
                username = username[:username.lower().find('/email')]
            pat = re.compile('.*(limited.*proxy).*')
            mat = pat.match(username)
            if mat:
                username = mat.group(1)
            username = username.replace('(','')
            username = username.replace(')','')
            return username
        except:
            return id


    # check quota
    def checkQuota(self,dn):
        _logger.debug("checkQuota %s" % dn)
        try:
            # set autocommit on
            self.conn.begin()
            # select
            name = self.cleanUserID(dn)
            sql = "SELECT cpua1,cpua7,cpua30,quotaa1,quotaa7,quotaa30 FROM users WHERE name = :name"
            varMap = {}
            varMap[':name'] = name
            self.cur.arraysize = 10
            self.cur.execute(sql,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            weight = 0.0
            if res != None and len(res) != 0:
                item = res[0]
                # cpu and quota
                cpu1    = item[0]
                cpu7    = item[1]
                cpu30   = item[2]
                quota1  = item[3] * 3600
                quota7  = item[4] * 3600
                quota30 = item[5] * 3600
                # CPU usage
                if cpu1 == None:
                    cpu1 = 0.0
                # weight
                weight = float(cpu1) / float(quota1)
                # not exceeded the limit
                if weight < 1.0:
                    weight = 0.0
                _logger.debug("checkQuota %s Weight:%s Quota:%s CPU:%s" % (dn,weight,quota1,cpu1))
            else:
                _logger.debug("checkQuota cannot found %s" % dn)
            return weight
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("checkQuota : %s %s" % (type,value))
            # roll back
            self._rollback()
            return 0.0


    # get serialize JobID and status
    def getUserParameter(self,dn,jobID):
        _logger.debug("getUserParameter %s %s" % (dn,jobID))
        try:
            # set autocommit on
            self.conn.begin()
            # select
            name = self.cleanUserID(dn)
            sql = "SELECT jobid,status FROM users WHERE name = :name"
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql,varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retJobID  = jobID
            retStatus = True
            if res != None and len(res) != 0:
                item = res[0]
                # JobID in DB
                dbJobID  = item[0]
                # check status
                if item[1] in ['disabled']:
                    retStatus = False
                # use larger JobID
                if dbJobID >= int(retJobID):
                    retJobID = dbJobID+1
                # update DB
                sql = "UPDATE users SET jobid=%d WHERE name = '%s'" % (retJobID,name)
                self.cur.execute(sql)
                _logger.debug("getUserParameter set JobID=%s for %s" % (retJobID,dn))
            return retJobID,retStatus
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getUserParameter : %s %s" % (type,value))
            # roll back
            self._rollback()
            return jobID,True


    # get email address for a user
    def getEmailAddr(self,name):
        _logger.debug("get email for %s" % name) 
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT email FROM users WHERE name=:name"
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql,varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if res != None and len(res) != 0:
                return res[0][0]
            # return empty string
            return ""
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getEmailAddr : %s %s" % (type,value))
            # roll back
            self._rollback()
            return ""


    # register proxy key
    def registerProxyKey(self,params):
        _logger.debug("register ProxyKey %s" % str(params))
        try:
            # set autocommit on
            self.conn.begin()
            # construct SQL
            sql0 = 'INSERT INTO proxykey ('
            sql1 = 'VALUES ('
            vals = {}
            for key,val in params.iteritems():
                sql0 += '%s,'  % key
                sql1 += ':%s,' % key
                vals[':%s' % key] = val
            sql0 = sql0[:-1]
            sql1 = sql1[:-1]
            sql = sql0 + ') ' + sql1 + ') '
            # insert
            self.cur.execute(sql,vals)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return True
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("registerProxyKey : %s %s" % (type,value))
            # roll back
            self._rollback()
            return ""


    # get proxy key
    def getProxyKey(self,dn):
        _logger.debug("get ProxyKey %s" % dn)
        try:
            # set autocommit on
            self.conn.begin()
            # construct SQL
            sql = 'SELECT credname,expires,origin,myproxy FROM proxykey WHERE dn=:dn ORDER BY expires DESC'
            varMap = {}
            varMap[':dn'] = dn
            # select
            self.cur.execute(sql,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            retMap = {}
            if res != None and len(res) != 0:
                credname,expires,origin,myproxy = res[0]
                retMap['credname'] = credname
                retMap['expires']  = expires
                retMap['origin']   = origin
                retMap['myproxy']  = myproxy
            _logger.debug(retMap)
            return retMap
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getProxyKey : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # get list of archived tables
    def getArchiveTables(self):
        tables = []
        cdate = datetime.datetime.utcnow()
        for iCycle in range(2): # 2 = (1 months + 2 just in case)/2
            if cdate.month==1:
                cdate = cdate.replace(year = (cdate.year-1))
                cdate = cdate.replace(month = 12, day = 1)
            else:
                cdate = cdate.replace(month = (cdate.month/2)*2, day = 1)
            tableName = "jobsArchived_%s%s" % (cdate.strftime('%b'),cdate.year)
            if not tableName in tables:
                tables.append(tableName)
            # one older table
            if cdate.month > 2:
                cdate = cdate.replace(month = (cdate.month-2))
            else:
                cdate = cdate.replace(year = (cdate.year-1), month = 12)
        # return
        return tables


    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,dn,timeRange,retJobIDs):
        comment = ' /* LogDBProxy.getJobIDsInTimeRange */'
        _logger.debug("getJobIDsInTimeRange : %s %s" % (dn,timeRange.strftime('%Y-%m-%d %H:%M:%S')))
        try:
            # get list of archived tables
            tables = self.getArchiveTables()
            # select
            for table in tables:
                # make sql
                sql  = "SELECT jobDefinitionID FROM %s " % table
                sql += "WHERE prodUserID=:prodUserID AND modificationTime>:modificationTime "
                sql += "AND prodSourceLabel='user' GROUP BY jobDefinitionID"
                varMap = {}
                varMap[':prodUserID'] = dn
                varMap[':modificationTime'] = timeRange
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                _logger.debug(sql+comment+str(varMap))
                self.cur.execute(sql+comment, varMap)
                resList = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # append
                for tmpID, in resList:
                    if not tmpID in retJobIDs:
                        retJobIDs.append(tmpID)
            _logger.debug("getJobIDsInTimeRange : %s" % str(retJobIDs))
            return retJobIDs
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobIDsInTimeRange : %s %s" % (type,value))
            # return empty list
            return retJobIDs


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,idStatus,nJobs):
        comment = ' /* LogProxy.getPandIDsWithJobID */'
        _logger.debug("getPandIDsWithJobID : %s %s" % (dn,jobID))
        try:
            # get list of archived tables
            tables = self.getArchiveTables()
            # select
            for table in tables:
                # skip if all jobs have already been gotten
                if nJobs > 0 and len(idStatus) >= nJobs:
                    continue
                # make sql
                sql = "SELECT PandaID,jobStatus,commandToPilot FROM %s " % table
                sql += "WHERE prodUserID=:prodUserID AND jobDefinitionID=:jobDefinitionID "
                sql += "AND prodSourceLabel in ('user','panda') "
                varMap = {}
                varMap[':prodUserID'] = dn
                varMap[':jobDefinitionID'] = jobID
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 5000
                # select
                _logger.debug(sql+comment+str(varMap))
                self.cur.execute(sql+comment, varMap)
                resList = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # append
                for tmpID,tmpStatus,tmpCommand in resList:
                    if not idStatus.has_key(tmpID):
                        idStatus[tmpID] = (tmpStatus,tmpCommand)
            _logger.debug("getPandIDsWithJobID : %s" % str(idStatus))
            return idStatus
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getPandIDsWithJobID : %s %s" % (type,value))
            # return empty list
            return {}


    # peek at job 
    def peekJob(self,pandaID):
        comment = ' /* LogDBProxy.peekJob */'
        _logger.debug("peekJob : %s" % pandaID)
        # return None for NULL PandaID
        if pandaID in ['NULL','','None',None]:
            return None
        sql1_0 = "SELECT %s FROM %s "
        sql1_1 = "WHERE PandaID=:PandaID"
        # select
        varMap = {}
        varMap[':PandaID'] = pandaID
        try:
            # get list of archived tables
            tables = self.getArchiveTables()
            # select
            for table in tables:
                # start transaction
                self.conn.begin()
                # select
                sql = sql1_0 % (JobSpec.columnNames(),table) + sql1_1
                self.cur.arraysize = 10
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                if len(res) != 0:
                    # Job
                    job = JobSpec()
                    job.pack(res[0])
                    # Files
                    # start transaction
                    self.conn.begin()
                    # select
                    fileTableName = re.sub('jobsArchived','filesTable',table)
                    sqlFile = "SELECT %s " % FileSpec.columnNames()
                    sqlFile+= "FROM %s " % fileTableName
                    sqlFile+= "WHERE PandaID=:PandaID"
                    self.cur.arraysize = 10000
                    self.cur.execute(sqlFile+comment, varMap)
                    resFs = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # set files
                    for resF in resFs:
                        file = FileSpec()
                        file.pack(resF)
                        job.addFile(file)
                    return job
            _logger.debug("peekJob() : PandaID %s not found" % pandaID)
            return None
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("peekJob : %s %s" % (type,value))
            # return None
            return None


    # wake up connection
    def wakeUp(self):
        for iTry in range(5):
            try:
                # check if the connection is working
                self.cur.execute("select user from dual")
                return
            except:
                type, value, traceBack = sys.exc_info()
                _logger.debug("wakeUp %d : %s %s" % (iTry,type,value))
                # wait for reconnection
                time.sleep(1)
                self.connect(reconnect=True)


    # close 
    def close(self):
        try:
            self.cur.close()
            self.conn.close()
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("close : %s %s" % (type,value))


    # commit
    def _commit(self):
        try:
            self.conn.commit()
            return True
        except:
            _logger.error("commit error")
            return False


    # rollback
    def _rollback(self):
        try:
            self.conn.rollback()
            return True
        except:
            _logger.error("rollback error")
            return False

