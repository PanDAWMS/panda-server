"""
proxy for log database connection

"""

import re
import sys
import time
import datetime

import MySQLdb

from pandalogger.PandaLogger import PandaLogger
from config import panda_config

import SiteSpec
import CloudSpec

from JobSpec  import JobSpec
from FileSpec import FileSpec

# logger
_logger = PandaLogger().getLogger('LogDBProxy')

# proxy
class LogDBProxy:

    # constructor
    def __init__(self):
        # connection object
        self.conn = None
        # cursor object
        self.cur = None
        
    # connect to DB
    def connect(self,dbhost=panda_config.logdbhost,dbpasswd=panda_config.logdbpasswd,
                dbuser=panda_config.logdbuser,dbname=panda_config.logdbname,reconnect=False):
        # keep parameters for reconnect
        if not reconnect:
            self.dbhost    = dbhost
            self.dbpasswd  = dbpasswd
            self.dbuser    = dbuser
            self.dbname    = dbname
        # connect    
        try:
            self.conn = MySQLdb.connect(host=self.dbhost,user=self.dbuser,
                                        passwd=self.dbpasswd,db=self.dbname)
            self.cur=self.conn.cursor()
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("connect : %s %s" % (type,value))
            # roll back
            self._rollback()
            return False


    # query an SQL   
    def querySQL(self,sql):
        try:
            # begin transaction
            self.cur.execute("START TRANSACTION")
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql = "SELECT siteid,nickname FROM schedconfig WHERE siteid<>''"
            self.cur.execute(sql)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retMap = {}
            if res != None and len(res) != 0:
                for siteid,nickname in res:
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql = "SELECT nickname,dq2url,cloud,ddm,lfchost,se,gatekeeper,releases,memory,"
            sql+= "maxtime,status,space,retry,cmtconfig,setokens,seprodpath,glexec,"
            sql+= "priorityoffset,allowedgroups,defaulttoken,siteid,queue,localqueue,"
            sql+= "validatedreleases,accesscontrol "
            sql+= "FROM schedconfig WHERE siteid<>''"
            self.cur.execute(sql)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retList = {}
            if resList != None:
                # loop over all results
                for res in resList:
                    nickname,dq2url,cloud,ddm,lfchost,se,gatekeeper,releases,memory,\
                       maxtime,status,space,retry,cmtconfig,setokens,seprodpath,glexec,\
                       priorityoffset,allowedgroups,defaulttoken,siteid,queue,localqueue,\
                       validatedreleases,accesscontrol \
                       = res
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
                    ret.accesscontrol = accesscontrol
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
                    # convert validatedreleases to list
                    ret.validatedreleases = []
                    for tmpRel in validatedreleases.split('|'):
                        # remove white space
                        tmpRel = tmpRel.strip()
                        if tmpRel != '':
                            ret.validatedreleases.append(tmpRel)
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql  = "SELECT name,tier1,tier1SE,relocation,weight,server,status,transtimelo,"
            sql += "transtimehi,waittime,validation,mcshare,countries,fasttrack,nprestage,"
            sql += "pilotowners "
            sql+= "FROM cloudconfig"
            self.cur.execute(sql)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            ret = {}
            if res != None and len(res) != 0:
                for name,tier1,tier1SE,relocation,weight,server,status,transtimelo,transtimehi,\
                        waittime,validation,mcshare,countries,fasttrack,nprestage,pilotowners in res:
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
                    tmpC.nprestage   = nprestage
                    tmpC.pilotowners = pilotowners
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            name = self.cleanUserID(dn)
            sql = "SELECT cpua1,cpua7,cpua30,quotaa1,quotaa7,quotaa30 FROM users WHERE name = '%s'" % name
            self.cur.execute(sql)
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            name = self.cleanUserID(dn)
            sql = "SELECT jobid,status FROM users WHERE name = '%s'" % name
            self.cur.execute(sql)
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql = "SELECT email FROM users WHERE name='%s'" % name
            self.cur.execute(sql)
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # construct SQL
            sql0 = 'INSERT INTO proxykey ('
            sql1 = 'VALUES ('
            vals = []
            for key,val in params.iteritems():
                sql0 += '%s,' % key
                sql1 += '%s,'
                vals.append(val)
            sql0 = sql0[:-1]
            sql1 = sql1[:-1]            
            sql = sql0 + ') ' + sql1 + ') '
            # insert
            self.cur.execute(sql,tuple(vals))
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
            self.cur.execute("SET AUTOCOMMIT=1")
            # construct SQL
            sql = 'SELECT credname,expires,origin,myproxy FROM proxykey WHERE dn=%s ORDER BY expires DESC'
            # select
            self.cur.execute(sql,(dn,))
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


    # check site access
    def checkSiteAccess(self,siteid,dn):
        comment = ' /* LogDBProxy.checkSiteAccess */'
        _logger.debug("checkSiteAccess %s:%s" % (siteid,dn))
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # construct SQL
            sql = 'SELECT poffset,rights,status FROM siteaccess WHERE dn=%s AND pandasite=%s'
            # select
            self.cur.execute(sql+comment,(dn,siteid))
            res = self.cur.fetchall()            
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            retMap = {}
            if res != None and len(res) != 0:
                poffset,rights,status = res[0]
                retMap['poffset'] = poffset
                retMap['rights']  = rights
                retMap['status']  = status
            _logger.debug(retMap)
            return retMap
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("checkSiteAccess : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # add account to siteaccess
    def addSiteAccess(self,siteID,dn):
        comment = ' /* LogDBProxy.addSiteAccess */'                        
        _logger.debug("addSiteAccess : %s %s" % (siteID,dn))
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql = 'SELECT status FROM siteaccess WHERE dn=%s AND pandasite=%s'
            self.cur.execute(sql+comment, (dn,siteID))
            res = self.cur.fetchone()
            if res != None:
                _logger.debug("account already exists with status=%s" % res[0])
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return res[0]
            # add
            sql = 'INSERT INTO siteaccess (dn,pandasite,status) VALUES (%s,%s,%s)'
            self.cur.execute(sql+comment, (dn,siteID,'requested'))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("account was added")
            return 0
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("addSiteAccess( : %s %s" % (type,value))
            # return None
            return -1


    # list site access
    def listSiteAccess(self,siteid=None,dn=None):
        comment = ' /* LogDBProxy.listSiteAccess */'
        _logger.debug("listSiteAccess %s:%s" % (siteid,dn))
        try:
            if siteid==None and dn==None:
                return []
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # construct SQL
            if siteid != None:
                varMap = (siteid,)
                sql = 'SELECT dn,status FROM siteaccess WHERE pandasite=%s ORDER BY dn'
            else:
                varMap = (dn,)
                sql = 'SELECT pandasite,status FROM siteaccess WHERE dn=%s ORDER BY pandasite'
            # select
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchall()            
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            ret = []
            if res != None and len(res) != 0:
                for tmpRes in res:
                    ret.append(tmpRes)
            _logger.debug(ret)
            return ret
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("listSiteAccess : %s %s" % (type,value))
            # roll back
            self._rollback()
            return []

        
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
                sql += "WHERE prodUserID=%s AND modificationTime>%s AND prodSourceLabel='user'"
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                _logger.debug(sql+comment+str((dn,timeRange.strftime('%Y-%m-%d %H:%M:%S'))))
                self.cur.execute(sql+comment, (dn,timeRange.strftime('%Y-%m-%d %H:%M:%S')))
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
                sql  = "SELECT PandaID,jobStatus,commandToPilot FROM %s " % table                
                sql += "WHERE prodUserID=%s AND jobDefinitionID=%s "
                sql += "AND prodSourceLabel in ('user','panda') "
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                _logger.debug(sql+comment+str((dn,jobID)))
                self.cur.execute(sql+comment, (dn,jobID))
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
        sql1_1 = "WHERE PandaID=%s"
        try:
            # get list of archived tables
            tables = self.getArchiveTables()
            # select
            for table in tables:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                sql = sql1_0 % (JobSpec.columnNames(),table) + sql1_1
                self.cur.execute(sql+comment, (pandaID,))
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                if len(res) != 0:
                    # Job
                    job = JobSpec()
                    job.pack(res[0])
                    # Files
                    # set autocommit on
                    self.cur.execute("SET AUTOCOMMIT=1")
                    # select
                    fileTableName = re.sub('jobsArchived','filesTable',table)
                    sqlFile = "SELECT %s " % FileSpec.columnNames()
                    sqlFile+= "FROM %s " % fileTableName
                    sqlFile+= "WHERE PandaID=%s"
                    self.cur.execute(sqlFile+comment, (job.PandaID,))
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
                self.conn.ping()
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
                
