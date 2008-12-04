"""
proxy for log database connection

"""

import re
import sys
import time

import MySQLdb

from pandalogger.PandaLogger import PandaLogger
from config import panda_config

import SiteSpec
import CloudSpec

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
                dbuser=panda_config.logdbuser,dbname=panda_config.logdbname):
        try:
            self.conn = MySQLdb.connect(host=dbhost,user=dbuser,
                                        passwd=dbpasswd,db=dbname)
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
        _logger.debug("getSiteList")
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
            return retMap
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getSiteList : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # get site info
    def getSiteInfo(self,site):
        #_logger.debug("getSiteInfo")
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql = "SELECT nickname,dq2url,cloud,ddm,lfchost,se,gatekeeper,releases,memory,"
            sql+= "maxtime,status,space,retry,cmtconfig,setokens,seprodpath,glexec "
            sql+= "FROM schedconfig WHERE nickname='%s'" % site
            self.cur.execute(sql)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            ret = None
            if res != None and len(res) != 0:
                nickname,dq2url,cloud,ddm,lfchost,se,gatekeeper,releases,memory,\
                   maxtime,status,space,retry,cmtconfig,setokens,seprodpath,glexec = res[0]
                # instantiate SiteSpec
                ret = SiteSpec.SiteSpec()
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
            return ret
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getSiteInfo : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # get cloud list
    def getCloudList(self):
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql = "SELECT name,tier1,tier1SE,relocation,weight,server,status,transtimelo,transtimehi,waittime,validation "
            sql+= "FROM cloudconfig"
            self.cur.execute(sql)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            ret = {}
            if res != None and len(res) != 0:
                for name,tier1,tier1SE,relocation,weight,server,status,transtimelo,transtimehi,waittime,validation \
                    in res:
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
                    # append
                    ret[name] = tmpC
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
                
