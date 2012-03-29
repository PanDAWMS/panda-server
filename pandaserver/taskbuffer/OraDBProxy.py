"""
proxy for database connection

"""

import re
import os
import sys
import time
import fcntl
import types
import random
import urllib
import datetime
import commands
import traceback
import warnings
import cx_Oracle
import ErrorCode
import SiteSpec
import CloudSpec
import PrioUtil
import ProcessGroups
from JobSpec  import JobSpec
from FileSpec import FileSpec
from DatasetSpec import DatasetSpec
from CloudTaskSpec import CloudTaskSpec
from pandalogger.PandaLogger import PandaLogger
from config import panda_config
from brokerage.PandaSiteIDs import PandaSiteIDs

warnings.filterwarnings('ignore')

# logger
_logger = PandaLogger().getLogger('DBProxy')

# lock file
_lockGetSN   = open(panda_config.lockfile_getSN, 'w')
_lockSetDS   = open(panda_config.lockfile_setDS, 'w')
_lockGetCT   = open(panda_config.lockfile_getCT, 'w')


# proxy
class DBProxy:

    # constructor
    def __init__(self,useOtherError=False):
        # connection object
        self.conn = None
        # cursor object
        self.cur = None
        # host name
        self.hostname = None
        # retry count
        self.nTry = 5
        # use special error codes for reconnection in querySQL
        self.useOtherError = useOtherError
        # memcached client
        self.memcache = None
        # pledge resource ratio
        self.beyondPledgeRatio = {}
        # update time for pledge resource ratio
        self.updateTimeForPledgeRatio = None
        # fareshare policy
        self.faresharePolicy = {}
        # update time for fareshare policy
        self.updateTimeForFaresharePolicy = None
        
        
    # connect to DB
    def connect(self,dbhost=panda_config.dbhost,dbpasswd=panda_config.dbpasswd,
                dbuser=panda_config.dbuser,dbname=panda_config.dbname,
                dbtimeout=None,reconnect=False):
        _logger.debug("connect : re=%s" % reconnect)
        # keep parameters for reconnect
        if not reconnect:
            self.dbhost    = dbhost
            self.dbpasswd  = dbpasswd
            self.dbuser    = dbuser
            self.dbname    = dbname
            self.dbtimeout = dbtimeout
        # close old connection
        if reconnect:
            _logger.debug("closing old connection")                            
            try:
                self.conn.close()
            except:
                _logger.debug("failed to close old connection")                                                
        # connect    
        try:
            self.conn = cx_Oracle.connect(dsn=self.dbhost,user=self.dbuser,
                                          password=self.dbpasswd,threaded=True)
            self.cur=self.conn.cursor()
            try:
                # use SQL dumper
                if panda_config.dump_sql:
                    import SQLDumper
                    self.cur = SQLDumper.SQLDumper(self.cur)
            except:
                pass
            # get hostname
            self.cur.execute("SELECT SYS_CONTEXT('USERENV','HOST') FROM dual")
            res = self.cur.fetchone()
            if res != None:
                self.hostname = res[0]
            # set TZ
            self.cur.execute("ALTER SESSION SET TIME_ZONE='UTC'")
            # set DATE format
            self.cur.execute("ALTER SESSION SET NLS_DATE_FORMAT='YYYY/MM/DD HH24:MI:SS'")
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("connect : %s %s" % (type,value))
            return False


    # query an SQL   
    def querySQL(self,sql,arraySize=1000):
        comment = ' /* DBProxy.querySQL */'
        try:
            _logger.debug("querySQL : %s " % sql)            
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = arraySize
            self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return res
        except:
            # roll back
            self._rollback(self.useOtherError)
            type, value, traceBack = sys.exc_info()
            _logger.error("querySQL : %s " % sql)
            _logger.error("querySQL : %s %s" % (type,value))
            return None


    # query an SQL return Status  
    def querySQLS(self,sql,varMap,arraySize=1000):
        comment = ' /* DBProxy.querySQLS */'            
        try:
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = arraySize
            ret = self.cur.execute(sql+comment,varMap)
            if sql.startswith('INSERT') or sql.startswith('UPDATE') or \
                   sql.startswith('DELETE'):
                res = self.cur.rowcount
            else:
                res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return ret,res
        except:
            # roll back
            self._rollback(self.useOtherError)
            type, value, traceBack = sys.exc_info()
            _logger.error("querySQLS : %s %s" % (sql,str(varMap)))
            _logger.error("querySQLS : %s %s" % (type,value))
            return -1,None


    # get CLOB
    def getClobObj(self,sql,varMap,arraySize=10000):
        comment = ' /* DBProxy.getClobObj */'            
        try:
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = arraySize
            ret = self.cur.execute(sql+comment,varMap)
            res = []
            for items in self.cur:
                resItem = []
                for item in items:
                    # read CLOB
                    resItem.append(item.read())
                # append    
                res.append(resItem)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return ret,res
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getClobObj : %s %s" % (sql,str(varMap)))
            _logger.error("getClobObj : %s %s" % (type,value))
            return -1,None


    # insert job to jobsDefined
    def insertNewJob(self,job,user,serNum,weight=0.0,priorityOffset=0,userVO=None,groupJobSN=0,toPending=False):
        comment = ' /* DBProxy.insertNewJob */'
        if not toPending:
            sql1 = "INSERT INTO ATLAS_PANDA.jobsDefined4 (%s) " % JobSpec.columnNames()
        else:
            sql1 = "INSERT INTO ATLAS_PANDA.jobsWaiting4 (%s) " % JobSpec.columnNames()            
        sql1+= JobSpec.bindValuesExpression(useSeq=True)
        sql1+= " RETURNING PandaID INTO :newPandaID"
        # make sure PandaID is NULL
        job.PandaID = None
        # job status
        if not toPending:
            job.jobStatus='defined'
        else:
            job.jobStatus='pending'            
        # host and time information
        job.modificationHost = self.hostname
        job.creationTime     = datetime.datetime.utcnow()
        job.modificationTime = job.creationTime
        job.stateChangeTime  = job.creationTime
        job.prodDBUpdateTime = datetime.datetime(1,1,1)
        # DN
        if job.prodUserID == "NULL" or job.prodSourceLabel in ['user','panda']:
            job.prodUserID = user
        # compact user name
        job.prodUserName = self.cleanUserID(job.prodUserID)
        if job.prodUserName in ['','NULL']:
            # use prodUserID as compact user name
            job.prodUserName = job.prodUserID
        # VO
        job.VO = userVO
        # priority
        if job.assignedPriority != 'NULL':
            job.currentPriority = job.assignedPriority
        if job.prodSourceLabel == 'user':
            if job.processingType in ['usermerge'] and not job.currentPriority in ['NULL',None]:
                # avoid prio reduction for merge jobs 
                pass
            else:
                job.currentPriority = PrioUtil.calculatePriority(priorityOffset,serNum,weight)
                if 'express' in job.specialHandling:
                    job.currentPriority = 6000
        elif job.prodSourceLabel == 'panda':
            job.currentPriority = 2000 + priorityOffset
            if 'express' in job.specialHandling:
                job.currentPriority = 6500
        # usergroup
        if job.prodSourceLabel == 'regional':
            job.computingSite= "BNLPROD"
        # group job SN
        groupJobSN = "%05d" % groupJobSN
        # set attempt numbers
        if job.prodSourceLabel in ['user','panda','ptest','rc_test']:
            if job.attemptNr in [None,'NULL','']:
                job.attemptNr = 0
            if job.maxAttempt in [None,'NULL','']:
                job.maxAttempt = 0
            # set maxAttempt to attemptNr to disable server/pilot retry
            if job.maxAttempt == -1:
                job.maxAttempt = job.attemptNr
            else:
                # set maxAttempt to have server/pilot retries for retried jobs
                if job.maxAttempt <= job.attemptNr:    
                    job.maxAttempt = job.attemptNr + 2    
        try:
            # begin transaction
            self.conn.begin()
            # insert
            varMap = job.valuesMap(useSeq=True)
            varMap[':newPandaID'] = self.cur.var(cx_Oracle.NUMBER)
            retI = self.cur.execute(sql1+comment, varMap)
            # set PandaID
            job.PandaID = long(varMap[':newPandaID'].getvalue())
            # get jobsetID
            if job.jobsetID in [None,'NULL',-1]:
                jobsetID = 0
            else:
                jobsetID = job.jobsetID
            jobsetID = '%06d' % jobsetID    
            # insert files
            _logger.debug("insertNewJob : %s Label:%s prio:%s" % (job.PandaID,job.prodSourceLabel,
                                                                      job.currentPriority))
            sqlFile = "INSERT INTO ATLAS_PANDA.filesTable4 (%s) " % FileSpec.columnNames()
            sqlFile+= FileSpec.bindValuesExpression(useSeq=True)
            sqlFile+= " RETURNING row_ID INTO :newRowID"
            for file in job.Files:
                file.row_ID = None
                if file.status != 'ready':
                    file.status='unknown'
                # replace $PANDAID with real PandaID
                file.lfn = re.sub('\$PANDAID', '%05d' % job.PandaID, file.lfn)
                # replace $JOBSETID with real jobsetID
                if not job.prodSourceLabel in ['managed']:
                    file.lfn = re.sub('\$JOBSETID', jobsetID, file.lfn)
                    file.lfn = re.sub('\$GROUPJOBSN', groupJobSN, file.lfn)
                # insert
                varMap = file.valuesMap(useSeq=True)
                varMap[':newRowID'] = self.cur.var(cx_Oracle.NUMBER)
                self.cur.execute(sqlFile+comment, varMap)
                # get rowID
                file.row_ID = long(varMap[':newRowID'].getvalue())
                # reset changed attribute list
                file.resetChangedList()
            # metadata
            if job.prodSourceLabel in ['user','panda']:
                sqlMeta = "INSERT INTO ATLAS_PANDA.metaTable (PandaID,metaData) VALUES (:PandaID,:metaData)"
                varMap = {}
                varMap[':PandaID']  = job.PandaID
                varMap[':metaData'] = job.metadata
                self.cur.execute(sqlMeta+comment, varMap)
            # job parameters
            if not job.prodSourceLabel in ['managed']:
                job.jobParameters = re.sub('\$JOBSETID', jobsetID, job.jobParameters)
                job.jobParameters = re.sub('\$GROUPJOBSN', groupJobSN, job.jobParameters)                
            sqlJob = "INSERT INTO ATLAS_PANDA.jobParamsTable (PandaID,jobParameters) VALUES (:PandaID,:param)"
            varMap = {}
            varMap[':PandaID'] = job.PandaID
            varMap[':param']   = job.jobParameters
            self.cur.execute(sqlJob+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("insertNewJob : %s File OK" % job.PandaID)
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("insertNewJob : %s %s" % (type,value))
            # roll back
            self._rollback()
            return False


    # simply insert job to a table
    def insertJobSimple(self,job,table,fileTable,jobParamsTable,metaTable):
        comment = ' /* DBProxy.insertJobSimple */'                            
        _logger.debug("insertJobSimple : %s" % job.PandaID)
        sql1 = "INSERT INTO %s (%s) " % (table,JobSpec.columnNames())
        sql1+= JobSpec.bindValuesExpression()
        try:
            # begin transaction
            self.conn.begin()
            # insert
            self.cur.execute(sql1+comment, job.valuesMap())
            # files
            sqlFile = "INSERT INTO %s " % fileTable
            sqlFile+= "(%s) " % FileSpec.columnNames(withMod=True)
            sqlFile+= FileSpec.bindValuesExpression(withMod=True)
            for file in job.Files:
                varMap = file.valuesMap()
                varMap[':modificationTime'] = job.modificationTime
                self.cur.execute(sqlFile+comment, varMap)
            # job parameters
            sqlJob = "INSERT INTO %s (PandaID,jobParameters,modificationTime) VALUES (:PandaID,:param,:modificationTime)" \
                     % jobParamsTable
            varMap = {}
            varMap[':PandaID'] = job.PandaID
            varMap[':param']   = job.jobParameters
            varMap[':modificationTime'] = job.modificationTime
            self.cur.execute(sqlJob+comment, varMap)
            # metadata
            if not job.metadata in [None,'NULL','']:
                sqlMeta = "INSERT INTO %s (PandaID,metaData,modificationTime) VALUES(:PandaID,:metaData,:modificationTime)" \
                          % metaTable
                varMap = {}
                varMap[':PandaID']  = job.PandaID
                varMap[':metaData'] = job.metadata
                varMap[':modificationTime'] = job.modificationTime
                self.cur.execute(sqlMeta+comment,varMap)
            # set flag to avoid duplicated insertion attempts
            varMap = {}
            varMap[':PandaID']      = job.PandaID
            varMap[':archivedFlag'] = 1
            sqlArch = "UPDATE ATLAS_PANDA.jobsArchived4 SET archivedFlag=:archivedFlag WHERE PandaID=:PandaID"
            self.cur.execute(sqlArch+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("insertJobSimple : %s %s" % (type,value))
            # roll back
            self._rollback()
            return False


    # simply insert job to a table without reading
    def insertJobSimpleUnread(self,pandaID,modTime):
        comment = ' /* DBProxy.insertJobSimpleUnread */'                            
        _logger.debug("insertJobSimpleUnread : %s" % pandaID)
        # check
        sqlC = "SELECT archivedFlag FROM ATLAS_PANDA.jobsArchived4 "
        sqlC += "WHERE PandaID=:pandaID "
        # job
        sqlJ  = "INSERT INTO ATLAS_PANDAARCH.jobsArchived (%s) " % JobSpec.columnNames()
        sqlJ += "SELECT %s FROM ATLAS_PANDA.jobsArchived4 " % JobSpec.columnNames()
        sqlJ += "WHERE PandaID=:pandaID "
        # file
        sqlF  = "INSERT INTO ATLAS_PANDAARCH.filesTable_ARCH (%s) " % FileSpec.columnNames(withMod=True)
        sqlF += "SELECT %s,:modTime FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames(withMod=False)
        sqlF += "WHERE PandaID=:pandaID "
        # parameters
        sqlP  = "INSERT INTO ATLAS_PANDAARCH.jobParamsTable_ARCH (PandaID,jobParameters,modificationTime) "
        sqlP += "SELECT PandaID,jobParameters,:modTime FROM ATLAS_PANDA.jobParamsTable "
        sqlP += "WHERE PandaID=:pandaID "
        # metadata
        sqlM1  = "SELECT PandaID FROM ATLAS_PANDA.metaTable "
        sqlM1 += "WHERE PandaID=:pandaID AND rownum<=1 "
        sqlM2  = "INSERT INTO ATLAS_PANDAARCH.metaTable_ARCH (PandaID,metaData,modificationTime) "
        sqlM2 += "SELECT PandaID,metaData,:modTime FROM ATLAS_PANDA.metaTable "
        sqlM2 += "WHERE PandaID=:pandaID "
        try:
            # begin transaction
            self.conn.begin()
            # check
            varMap = {}
            varMap[':pandaID'] = pandaID
            self.cur.execute(sqlC+comment,varMap)
            res = self.cur.fetchone()
            if res == None or res[0] == 1:
                if res == None:
                    _logger.error("insertJobSimpleUnread : %s cannot get archivedFlag" % pandaID)
                else:
                    _logger.debug("insertJobSimpleUnread : %s skip" % pandaID)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True
            # insert
            varMap = {}
            varMap[':pandaID'] = pandaID
            self.cur.execute(sqlJ+comment,varMap)
            varMap = {}
            varMap[':pandaID'] = pandaID
            varMap[':modTime'] = modTime
            self.cur.execute(sqlF+comment,varMap)
            varMap = {}
            varMap[':pandaID'] = pandaID
            varMap[':modTime'] = modTime
            self.cur.execute(sqlP+comment,varMap)
            varMap = {}
            varMap[':pandaID'] = pandaID
            self.cur.execute(sqlM1+comment,varMap)
            res = self.cur.fetchone()
            if res != None:
                varMap = {}
                varMap[':pandaID'] = pandaID
                varMap[':modTime'] = modTime
                self.cur.execute(sqlM2+comment,varMap)                                    
            # set flag to avoid duplicated insertion attempts
            varMap = {}
            varMap[':PandaID']      = pandaID
            varMap[':archivedFlag'] = 1
            sqlArch = "UPDATE ATLAS_PANDA.jobsArchived4 SET archivedFlag=:archivedFlag WHERE PandaID=:PandaID"
            self.cur.execute(sqlArch+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("insertJobSimpleUnread %s : %s %s" % (pandaID,type,value))
            # roll back
            self._rollback()
            return False


    # delete job
    def deleteJobSimple(self,pandaID):
        comment = ' /* DBProxy.deleteJobSimple */'                            
        _logger.debug("deleteJobSimple : %s" % pandaID)
        try:
            # begin transaction
            self.conn.begin()
            # delete
            varMap = {}
            varMap[':PandaID']  = pandaID
            sql = 'DELETE from ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID'
            self.cur.execute(sql+comment, varMap)
            sql = "DELETE from ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID"
            self.cur.execute(sql+comment, varMap)            
            sql = "DELETE from ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"
            self.cur.execute(sql+comment, varMap)            
            sql = "DELETE from ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
            self.cur.execute(sql+comment, varMap)            
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            type, value = sys.exc_info()[:2]
            _logger.error("deleteJobSimple %s : %s %s" % (pandaID,type,value))
            # roll back
            self._rollback()
            return False


    # activate job. move job from jobsDefined to jobsActive 
    def activateJob(self,job):
        comment = ' /* DBProxy.activateJob */'        
        if job==None:
            _logger.debug("activateJob : None")
            return True
        _logger.debug("activateJob : %s" % job.PandaID)                        
        sql0 = "SELECT row_ID FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type AND status!=:status "
        sql1 = "DELETE FROM ATLAS_PANDA.jobsDefined4 "
        sql1+= "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) AND commandToPilot IS NULL"
        sql2 = "INSERT INTO ATLAS_PANDA.jobsActive4 (%s) " % JobSpec.columnNames()
        sql2+= JobSpec.bindValuesExpression()
        # host and time information
        job.modificationTime = datetime.datetime.utcnow()
        # set stateChangeTime for defined->activated but not for assigned->activated
        if job.jobStatus in ['defined']:
            job.stateChangeTime = job.modificationTime
        nTry=3
        for iTry in range(nTry):
            try:
                # check if all files are ready
                allOK = True
                for file in job.Files:
                    if file.type == 'input' and file.status != 'ready':
                        allOK = False
                        break
                # begin transaction
                self.conn.begin()
                # check all inputs are ready
                varMap = {}
                varMap[':type']    = 'input'
                varMap[':status']  = 'ready'
                varMap[':PandaID'] = job.PandaID
                self.cur.arraysize = 100
                self.cur.execute(sql0+comment, varMap)
                res = self.cur.fetchall()
                if len(res) == 0 or allOK:
                    # change status
                    job.jobStatus = "activated"
                    # delete
                    varMap = {}
                    varMap[':PandaID']       = job.PandaID
                    varMap[':oldJobStatus1'] = 'assigned'
                    varMap[':oldJobStatus2'] = 'defined'
                    self.cur.execute(sql1+comment, varMap)
                    n = self.cur.rowcount
                    if n==0:
                        # already killed or activated
                        _logger.debug("activateJob : Not found %s" % job.PandaID)
                    else:
                        # insert
                        self.cur.execute(sql2+comment, job.valuesMap())
                        # update files
                        for file in job.Files:
                            sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                            varMap = file.valuesMap(onlyChanged=True)
                            if varMap != {}:
                                varMap[':row_ID'] = file.row_ID
                                _logger.debug(sqlF+comment+str(varMap))
                                self.cur.execute(sqlF+comment, varMap)
                        # job parameters
                        sqlJob = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[':PandaID'] = job.PandaID
                        varMap[':param']   = job.jobParameters
                        self.cur.execute(sqlJob+comment, varMap)
                else:
                    # update job
                    sqlJ = ("UPDATE ATLAS_PANDA.jobsDefined4 SET %s " % JobSpec.bindUpdateExpression()) + \
                           "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
                    varMap = job.valuesMap()
                    varMap[':PandaID']       = job.PandaID
                    varMap[':oldJobStatus1'] = 'assigned'
                    varMap[':oldJobStatus2'] = 'defined'
                    self.cur.execute(sqlJ+comment, varMap)
                    n = self.cur.rowcount
                    if n==0:
                        # already killed or activated
                        _logger.debug("activateJob : Not found %s" % job.PandaID)
                    else:
                        # update files
                        for file in job.Files:
                            sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                            varMap = file.valuesMap(onlyChanged=True)
                            if varMap != {}:
                                varMap[':row_ID'] = file.row_ID
                                _logger.debug(sqlF+comment+str(varMap))                                
                                self.cur.execute(sqlF+comment, varMap)
                        # job parameters
                        sqlJob = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[':PandaID'] = job.PandaID
                        varMap[':param']   = job.jobParameters
                        self.cur.execute(sqlJob+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("activateJob : %s retry : %s" % (job.PandaID,iTry))  
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("activateJob : %s %s" % (type,value))
                return False


    # send job to jobsWaiting
    def keepJob(self,job):
        comment = ' /* DBProxy.keepJob */'        
        _logger.debug("keepJob : %s" % job.PandaID)                        
        sql1 = "DELETE FROM ATLAS_PANDA.jobsDefined4 "
        sql1+= "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) AND commandToPilot IS NULL"
        sql2 = "INSERT INTO ATLAS_PANDA.jobsWaiting4 (%s) " % JobSpec.columnNames()
        sql2+= JobSpec.bindValuesExpression()
        # time information
        job.modificationTime = datetime.datetime.utcnow()
        job.stateChangeTime  = job.modificationTime        
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.conn.begin()
                # delete
                varMap = {}
                varMap[':PandaID']       = job.PandaID
                varMap[':oldJobStatus1'] = 'assigned'
                varMap[':oldJobStatus2'] = 'defined'
                self.cur.execute(sql1+comment, varMap)
                n = self.cur.rowcount                
                if n==0:
                    # already killed
                    _logger.debug("keepJob : Not found %s" % job.PandaID)
                else:
                    # set status
                    job.jobStatus = 'waiting'
                    # insert
                    self.cur.execute(sql2+comment, job.valuesMap())
                    # update files
                    for file in job.Files:
                        sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                        varMap = file.valuesMap(onlyChanged=True)
                        if varMap != {}:
                            varMap[':row_ID'] = file.row_ID
                            _logger.debug(sqlF+comment+str(varMap))                            
                            self.cur.execute(sqlF+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("keepJob : %s retry : %s" % (job.PandaID,iTry))  
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("keepJob : %s %s" % (type,value))
                return False


    # archive job to jobArchived and remove the job from jobsActive or jobsDefined
    def archiveJob(self,job,fromJobsDefined):
        comment = ' /* DBProxy.archiveJob */'                
        _logger.debug("archiveJob : %s" % job.PandaID)                
        if fromJobsDefined:
            sql1 = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
        else:
            sql1 = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"            
        sql2 = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
        sql2+= JobSpec.bindValuesExpression()
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.conn.begin()
                # delete
                varMap = {}
                varMap[':PandaID'] = job.PandaID
                if fromJobsDefined:
                    varMap[':oldJobStatus1'] = 'assigned'
                    varMap[':oldJobStatus2'] = 'defined'
                self.cur.execute(sql1+comment, varMap)
                n = self.cur.rowcount                
                if n==0:
                    # already killed
                    _logger.debug("archiveJob : Not found %s" % job.PandaID)
                else:
                    # insert
                    job.modificationTime = datetime.datetime.utcnow()
                    job.stateChangeTime  = job.modificationTime                    
                    if job.endTime == 'NULL':
                        job.endTime = job.modificationTime
                    self.cur.execute(sql2+comment, job.valuesMap())
                    # update files
                    for file in job.Files:
                        sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                        varMap = file.valuesMap(onlyChanged=True)
                        if varMap != {}:
                            varMap[':row_ID'] = file.row_ID
                            _logger.debug(sqlF+comment+str(varMap))
                            self.cur.execute(sqlF+comment, varMap)
                    # update metadata and parameters
                    sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"                    
                    sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[':PandaID'] = job.PandaID
                    varMap[':modificationTime'] = job.modificationTime
                    self.cur.execute(sqlFMod+comment,varMap)
                    self.cur.execute(sqlMMod+comment,varMap)
                    self.cur.execute(sqlPMod+comment,varMap)
                    # increment the number of failed jobs in _dis
                    myDisList = []
                    if job.jobStatus == 'failed' and job.prodSourceLabel in ['managed','test']:
                        for tmpFile in job.Files:
                            if tmpFile.type == 'input' and not tmpFile.dispatchDBlock in ['','NULL',None] \
                                   and not tmpFile.dispatchDBlock in myDisList:
                                varMap = {}
                                varMap[':name'] = tmpFile.dispatchDBlock
                                # check currentfiles
                                sqlGetCurFiles  = """SELECT /*+ BEGIN_OUTLINE_DATA """
                                sqlGetCurFiles += """INDEX_RS_ASC(@"SEL$1" "TAB"@"SEL$1" ("DATASETS"."NAME")) """
                                sqlGetCurFiles += """OUTLINE_LEAF(@"SEL$1") ALL_ROWS """
                                sqlGetCurFiles += """OPTIMIZER_FEATURES_ENABLE('10.2.0.4') """
                                sqlGetCurFiles += """IGNORE_OPTIM_EMBEDDED_HINTS """
                                sqlGetCurFiles += """END_OUTLINE_DATA */ """
                                sqlGetCurFiles += "currentfiles,vuid FROM ATLAS_PANDA.Datasets tab WHERE name=:name"
                                self.cur.execute(sqlGetCurFiles+comment,varMap)
                                resCurFiles = self.cur.fetchone()
                                _logger.debug("archiveJob : %s %s" % (job.PandaID,str(resCurFiles)))
                                if resCurFiles != None:
                                    # increment currentfiles only for the first failed job since that is enough
                                    tmpCurrentFiles,tmpVUID = resCurFiles
                                    _logger.debug("archiveJob : %s %s currentfiles=%s" % (job.PandaID,tmpFile.dispatchDBlock,tmpCurrentFiles))
                                    if tmpCurrentFiles == 0:
                                        _logger.debug("archiveJob : %s %s update currentfiles" % (job.PandaID,tmpFile.dispatchDBlock))
                                        varMap = {}
                                        varMap[':vuid'] = tmpVUID
                                        sqlFailedInDis  = 'UPDATE ATLAS_PANDA.Datasets '
                                        sqlFailedInDis += 'SET currentfiles=currentfiles+1 WHERE vuid=:vuid'
                                        self.cur.execute(sqlFailedInDis+comment,varMap)
                                myDisList.append(tmpFile.dispatchDBlock) 
                # delete downstream jobs
                ddmIDs     = []
                newJob     = None
                ddmAttempt = 0
                if job.prodSourceLabel == 'panda' and job.jobStatus == 'failed':
                    # look for outputs
                    upOutputs = []
                    for file in job.Files:
                        if file.type == 'output':
                            upOutputs.append(file.lfn)
                    toBeClosedSubList = {}
                    topUserDsList = []
                    # look for downstream jobs
                    sqlD   = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 WHERE type=:type AND lfn=:lfn GROUP BY PandaID"
                    sqlDJS = "SELECT %s " % JobSpec.columnNames()
                    sqlDJS+= "FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                    sqlDJD = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                    sqlDJI = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
                    sqlDJI+= JobSpec.bindValuesExpression()
                    sqlDFup = "UPDATE ATLAS_PANDA.filesTable4 SET status=:status WHERE PandaID=:PandaID AND type IN (:type1,:type2)"
                    sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                    sqlGetSub = "SELECT DISTINCT destinationDBlock FROM ATLAS_PANDA.filesTable4 WHERE type=:type AND PandaID=:PandaID"
                    sqlCloseSub  = 'UPDATE /*+ INDEX_RS_ASC(TAB("DATASETS"."NAME")) */ ATLAS_PANDA.Datasets tab '
                    sqlCloseSub += 'SET status=:status,modificationDate=CURRENT_DATE WHERE name=:name'
                    for upFile in upOutputs:
                        _logger.debug("look for downstream jobs for %s" % upFile)
                        # select PandaID
                        varMap = {}
                        varMap[':lfn']  = upFile
                        varMap[':type'] = 'input'
                        self.cur.arraysize = 100000
                        self.cur.execute(sqlD+comment, varMap)
                        res = self.cur.fetchall()
                        for downID, in res:
                            _logger.debug("delete : %s" % downID)        
                            # select jobs
                            varMap = {}
                            varMap[':PandaID'] = downID
                            self.cur.arraysize = 10
                            self.cur.execute(sqlDJS+comment, varMap)
                            resJob = self.cur.fetchall()
                            if len(resJob) == 0:
                                continue
                            # instantiate JobSpec
                            dJob = JobSpec()
                            dJob.pack(resJob[0])
                            # delete
                            varMap = {}
                            varMap[':PandaID'] = downID
                            self.cur.execute(sqlDJD+comment, varMap)
                            retD = self.cur.rowcount
                            if retD == 0:
                                continue
                            # error code
                            dJob.jobStatus = 'cancelled'
                            dJob.endTime   = datetime.datetime.utcnow()
                            dJob.taskBufferErrorCode = ErrorCode.EC_Kill
                            dJob.taskBufferErrorDiag = 'killed by Panda server : upstream job failed'
                            dJob.modificationTime = dJob.endTime
                            dJob.stateChangeTime  = dJob.endTime
                            # insert
                            self.cur.execute(sqlDJI+comment, dJob.valuesMap())
                            # update file status
                            varMap = {}
                            varMap[':PandaID'] = downID
                            varMap[':status']  = 'failed'
                            varMap[':type1']   = 'output'
                            varMap[':type2']   = 'log'
                            self.cur.execute(sqlDFup+comment, varMap)
                            # update files,metadata,parametes
                            varMap = {}
                            varMap[':PandaID'] = downID
                            varMap[':modificationTime'] = dJob.modificationTime
                            self.cur.execute(sqlFMod+comment,varMap)
                            self.cur.execute(sqlMMod+comment,varMap)
                            self.cur.execute(sqlPMod+comment,varMap)
                            # set tobeclosed to sub datasets
                            if not toBeClosedSubList.has_key(dJob.jobDefinitionID):
                                # init
                                toBeClosedSubList[dJob.jobDefinitionID] = []
                                # get sub datasets
                                varMap = {}
                                varMap[':type'] = 'output'
                                varMap[':PandaID'] = downID
                                self.cur.arraysize = 1000
                                self.cur.execute(sqlGetSub+comment, varMap)
                                resGetSub = self.cur.fetchall()
                                if len(resGetSub) == 0:
                                    continue
                                # loop over all sub datasets
                                for tmpDestinationDBlock, in resGetSub:
                                    if re.search('_sub\d+$',tmpDestinationDBlock) == None:
                                        continue
                                    if not tmpDestinationDBlock in toBeClosedSubList[dJob.jobDefinitionID]:
                                        # set tobeclosed
                                        varMap = {}
                                        varMap[':status'] = 'tobeclosed'
                                        varMap[':name'] = tmpDestinationDBlock
                                        self.cur.execute(sqlCloseSub+comment, varMap)
                                        _logger.debug("set tobeclosed for %s" % tmpDestinationDBlock)
                                        # append
                                        toBeClosedSubList[dJob.jobDefinitionID].append(tmpDestinationDBlock)
                                        # close top-level user dataset
                                        topUserDsName = re.sub('_sub\d+$','',tmpDestinationDBlock)
                                        if topUserDsName != tmpDestinationDBlock and not topUserDsName in topUserDsList:
                                            # set tobeclosed
                                            varMap = {}
                                            varMap[':status'] = 'tobeclosed'
                                            varMap[':name'] = topUserDsName
                                            self.cur.execute(sqlCloseSub+comment, varMap)
                                            _logger.debug("set tobeclosed for %s" % topUserDsName)
                                            # append
                                            topUserDsList.append(topUserDsName)
                elif job.prodSourceLabel == 'ddm' and job.jobStatus == 'failed' and job.transferType=='dis':
                    # get corresponding jobs for production movers
                    vuid = ''
                    # extract vuid
                    match = re.search('--callBack (\S+)',job.jobParameters)
                    if match != None:
                        try:
                            callbackUrl = urllib.unquote(match.group(1))
                            callbackUrl = re.sub('[&\?]',' ', callbackUrl)
                            # look for vuid=
                            for item in callbackUrl.split():
                                if item.startswith('vuid='):
                                    vuid = item.split('=')[-1]
                                    break
                        except:
                            pass
                        if vuid == '':
                            _logger.error("cannot extract vuid from %s" % job.jobParameters)
                        else:
                            # get name
                            varMap = {}
                            varMap[':vuid'] = vuid
                            varMap[':type'] = 'dispatch'
                            self.cur.arraysize = 10
                            self.cur.execute("SELECT name FROM ATLAS_PANDA.Datasets WHERE vuid=:vuid AND type=:type "+comment, varMap)
                            res = self.cur.fetchall()
                            if len(res) != 0:
                                disName = res[0][0]
                                # check lost files
                                varMap = {}
                                varMap[':status'] = 'lost'
                                varMap[':dispatchDBlock'] = disName
                                sqlLost = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ distinct PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE status=:status AND dispatchDBlock=:dispatchDBlock"
                                self.cur.execute(sqlLost+comment,varMap)
                                resLost = self.cur.fetchall()
                                # fail jobs with lost files
                                sqlDJS = "SELECT %s " % JobSpec.columnNames()
                                sqlDJS+= "FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                                sqlDJD = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                                sqlDJI = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
                                sqlDJI+= JobSpec.bindValuesExpression()
                                sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                                sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                                sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                                lostJobIDs = []
                                for tmpID, in resLost:
                                    _logger.debug("fail due to lost files : %s" % tmpID)
                                    varMap = {}
                                    varMap[':PandaID'] = tmpID
                                    self.cur.arraysize = 10
                                    self.cur.execute(sqlDJS+comment, varMap)
                                    resJob = self.cur.fetchall()
                                    if len(resJob) == 0:
                                        continue
                                    # instantiate JobSpec
                                    dJob = JobSpec()
                                    dJob.pack(resJob[0])
                                    # delete
                                    varMap = {}
                                    varMap[':PandaID'] = tmpID
                                    self.cur.execute(sqlDJD+comment, varMap)
                                    retD = self.cur.rowcount
                                    if retD == 0:
                                        continue
                                    # error code
                                    dJob.jobStatus = 'failed'
                                    dJob.endTime   = datetime.datetime.utcnow()
                                    dJob.ddmErrorCode = 101 #ErrorCode.EC_LostFile
                                    dJob.ddmErrorDiag = 'lost file in SE'
                                    dJob.modificationTime = dJob.endTime
                                    dJob.stateChangeTime  = dJob.endTime
                                    # insert
                                    self.cur.execute(sqlDJI+comment, dJob.valuesMap())
                                    # update files,metadata,parametes
                                    varMap = {}
                                    varMap[':PandaID'] = tmpID
                                    varMap[':modificationTime'] = dJob.modificationTime
                                    self.cur.execute(sqlFMod+comment,varMap)
                                    self.cur.execute(sqlMMod+comment,varMap)
                                    self.cur.execute(sqlPMod+comment,varMap)
                                    # append
                                    lostJobIDs.append(tmpID)
                                # get PandaIDs
                                varMap = {}
                                varMap[':jobStatus'] = 'assigned'
                                varMap[':dispatchDBlock'] = disName
                                self.cur.execute("SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE dispatchDBlock=:dispatchDBlock AND jobStatus=:jobStatus "+comment,
                                                 varMap)
                                resDDM = self.cur.fetchall()
                                for tmpID, in resDDM:
                                    if not tmpID in lostJobIDs:
                                        ddmIDs.append(tmpID)
                                # get offset
                                ddmAttempt = job.attemptNr
                                _logger.debug("get PandaID for reassign : %s ddmAttempt=%s" % (str(ddmIDs),ddmAttempt))
                elif job.prodSourceLabel == 'ddm' and job.jobStatus == 'failed' and job.transferType=='ddm' and job.attemptNr<2 \
                         and job.commandToPilot != 'tobekilled':
                    # instantiate new mover to retry subscription
                    newJob = JobSpec()
                    newJob.jobDefinitionID   = job.jobDefinitionID
                    newJob.jobName           = job.jobName
                    newJob.attemptNr         = job.attemptNr + 1           
                    newJob.transformation    = job.transformation
                    newJob.destinationDBlock = job.destinationDBlock
                    newJob.destinationSE     = job.destinationSE
                    newJob.currentPriority   = job.currentPriority
                    newJob.prodSourceLabel   = job.prodSourceLabel
                    newJob.prodUserID        = job.prodUserID                    
                    newJob.computingSite     = job.computingSite
                    newJob.transferType      = job.transferType
                    newJob.sourceSite        = job.sourceSite
                    newJob.destinationSite   = job.destinationSite
                    newJob.jobParameters     = job.jobParameters
                    if job.Files != []:
                        file = job.Files[0]
                        fileOL = FileSpec()
                        # add attempt nr
                        fileOL.lfn = re.sub("\.\d+$","",file.lfn)
                        fileOL.lfn = "%s.%d" % (fileOL.lfn,job.attemptNr)
                        fileOL.destinationDBlock = file.destinationDBlock
                        fileOL.destinationSE     = file.destinationSE
                        fileOL.dataset           = file.dataset
                        fileOL.type              = file.type
                        newJob.addFile(fileOL)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True,ddmIDs,ddmAttempt,newJob
            except:
                # roll back
                self._rollback(True)
                if iTry+1 < nTry:
                    _logger.debug("archiveJob : %s retry : %s" % (job.PandaID,iTry))                
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("archiveJob : %s" % job.PandaID)
                _logger.error("archiveJob : %s %s" % (type,value))
                return False,[],0,None


    # overload of archiveJob
    def archiveJobLite(self,pandaID,jobStatus,param):
        comment = ' /* DBProxy.archiveJobLite */'                        
        _logger.debug("archiveJobLite : %s" % pandaID)        
        sql1 = "SELECT %s FROM ATLAS_PANDA.jobsActive4 " % JobSpec.columnNames()
        sql1+= "WHERE PandaID=:PandaID"
        sql2 = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
        sql3 = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
        sql3+= JobSpec.bindValuesExpression()
        sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.conn.begin()
                # select
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.arraysize = 10
                self.cur.execute(sql1+comment, varMap)
                res = self.cur.fetchall()
                if len(res) == 0:
                    _logger.error("archiveJobLite() : PandaID %d not found" % pandaID)
                    self._rollback()
                    return False
                job = JobSpec()
                job.pack(res[0])
                job.jobStatus = jobStatus
                for key in param.keys():
                    if param[key] != None:
                        setattr(job,key,param[key])
                job.modificationTime = datetime.datetime.utcnow()
                job.endTime          = job.modificationTime
                job.stateChangeTime  = job.modificationTime
                # delete
                self.cur.execute(sql2+comment, varMap)
                n = self.cur.rowcount
                if n==0:
                    # already killed
                    _logger.debug("archiveJobLite : Not found %s" % pandaID)        
                else:        
                    # insert
                    self.cur.execute(sql3+comment, job.valuesMap())
                    # update files
                    for file in job.Files:
                        sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                        varMap = file.valuesMap(onlyChanged=True)
                        if varMap != {}:
                            varMap[':row_ID'] = file.row_ID
                            _logger.debug(sqlF+comment+str(varMap))                            
                            self.cur.execute(sqlF+comment, varMap)
                    # update files,metadata,parametes
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    varMap[':modificationTime'] = job.modificationTime
                    self.cur.execute(sqlFMod+comment,varMap)
                    self.cur.execute(sqlMMod+comment,varMap)
                    self.cur.execute(sqlPMod+comment,varMap)
                # delete downstream jobs
                if job.prodSourceLabel == 'panda' and job.jobStatus == 'failed':
                    # file select
                    sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                    sqlFile+= "WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    self.cur.arraysize = 100000
                    self.cur.execute(sqlFile+comment, varMap)
                    resFs = self.cur.fetchall()
                    for resF in resFs:
                        file = FileSpec()
                        file.pack(resF)
                        job.addFile(file)
                    # look for outputs
                    upOutputs = []
                    for file in job.Files:
                        if file.type == 'output':
                            upOutputs.append(file.lfn)
                    # look for downstream jobs
                    sqlD   = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 WHERE type=:type AND lfn=:lfn GROUP BY PandaID"
                    sqlDJS = "SELECT %s " % JobSpec.columnNames()
                    sqlDJS+= "FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                    sqlDJD = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                    sqlDJI = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
                    sqlDJI+= JobSpec.bindValuesExpression()
                    for upFile in upOutputs:
                        _logger.debug("look for downstream jobs for %s" % upFile)
                        # select PandaID
                        varMap = {}
                        varMap[':lfn'] = upFile
                        varMap[':type'] = 'input'
                        self.cur.arraysize = 100000
                        self.cur.execute(sqlD+comment, varMap)
                        res = self.cur.fetchall()
                        for downID, in res:
                            _logger.debug("delete : %s" % downID)        
                            # select jobs
                            varMap = {}
                            varMap[':PandaID'] = downID
                            self.cur.arraysize = 10
                            self.cur.execute(sqlDJS+comment, varMap)
                            resJob = self.cur.fetchall()
                            if len(resJob) == 0:
                                continue
                            # instantiate JobSpec
                            dJob = JobSpec()
                            dJob.pack(resJob[0])
                            # delete
                            varMap = {}
                            varMap[':PandaID'] = downID
                            self.cur.execute(sqlDJD+comment, varMap)                            
                            retD = self.cur.rowcount
                            if retD == 0:
                                continue
                            # error code
                            dJob.jobStatus = 'failed'
                            dJob.endTime   = datetime.datetime.utcnow()
                            dJob.taskBufferErrorCode = ErrorCode.EC_Kill
                            dJob.taskBufferErrorDiag = 'killed by Panda server : upstream job failed'
                            dJob.modificationTime = dJob.endTime
                            dJob.stateChangeTime  = dJob.endTime
                            # insert
                            self.cur.execute(sqlDJI+comment, dJob.valuesMap())
                            # update files,metadata,parametes
                            varMap = {}
                            varMap[':PandaID'] = downID
                            varMap[':modificationTime'] = dJob.modificationTime
                            self.cur.execute(sqlFMod+comment,varMap)
                            self.cur.execute(sqlMMod+comment,varMap)
                            self.cur.execute(sqlPMod+comment,varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("archiveJobLite : %s retry : %s" % (pandaID,iTry))        
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("archiveJobLite : %s %s" % (type,value))
                return False


    # finalize pending jobs
    def finalizePendingJobs(self,prodUserName,jobDefinitionID):
        comment = ' /* DBProxy.finalizePendingJobs */'                        
        _logger.debug("finalizePendingJobs : %s %s" % (prodUserName,jobDefinitionID))
        sql0 = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
        sql0+= "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
        sql0+= "AND prodSourceLabel=:prodSourceLabel AND jobStatus=:jobStatus "
        sqlU = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:newJobStatus "
        sqlU+= "WHERE PandaID=:PandaID AND jobStatus=:jobStatus "
        sql1 = "SELECT %s FROM ATLAS_PANDA.jobsActive4 " % JobSpec.columnNames()
        sql1+= "WHERE PandaID=:PandaID AND jobStatus=:jobStatus "
        sql2 = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID AND jobStatus=:jobStatus "
        sql3 = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
        sql3+= JobSpec.bindValuesExpression()
        sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        try:
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # select
            varMap = {}
            varMap[':jobStatus']       = 'failed'
            varMap[':prodUserName']    = prodUserName
            varMap[':jobDefinitionID'] = jobDefinitionID
            varMap[':prodSourceLabel'] = 'user'
            self.cur.execute(sql0+comment,varMap)
            resPending = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # lock
            pPandaIDs = []
            for pandaID, in resPending:
                # begin transaction                
                self.conn.begin()
                # update
                varMap = {}
                varMap[':jobStatus']    = 'failed'
                varMap[':newJobStatus'] = 'holding'
                varMap[':PandaID']      = pandaID
                self.cur.execute(sqlU+comment,varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                retU = self.cur.rowcount
                if retU != 0:
                    pPandaIDs.append(pandaID)
            # loop over all PandaIDs
            for pandaID in pPandaIDs:
                # begin transaction
                self.conn.begin()
                # get job
                varMap = {}
                varMap[':PandaID']   = pandaID
                varMap[':jobStatus'] = 'holding'                
                self.cur.arraysize = 10
                self.cur.execute(sql1+comment,varMap)
                res = self.cur.fetchall()
                if len(res) == 0:
                    _logger.debug("finalizePendingJobs : PandaID %d not found" % pandaID)
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    continue
                job = JobSpec()
                job.pack(res[0])
                job.jobStatus = 'failed'
                job.modificationTime = datetime.datetime.utcnow()
                # delete
                self.cur.execute(sql2+comment,varMap)
                n = self.cur.rowcount
                if n==0:
                    # already killed
                    _logger.debug("finalizePendingJobs : Not found %s" % pandaID)        
                else:        
                    # insert
                    self.cur.execute(sql3+comment,job.valuesMap())
                    # update files,metadata,parametes
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    varMap[':modificationTime'] = job.modificationTime
                    self.cur.execute(sqlFMod+comment,varMap)
                    self.cur.execute(sqlMMod+comment,varMap)
                    self.cur.execute(sqlPMod+comment,varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            _logger.debug("finalizePendingJobs : %s %s done for %s" % (prodUserName,jobDefinitionID,len(pPandaIDs)))
            return True
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("finalizePendingJobs : %s %s" % (errType,errValue))
            return False


    # delete stalled jobs
    def deleteStalledJobs(self,libFileName):
        comment = ' /* DBProxy.deleteStalledJobs */'                
        _logger.debug("deleteStalledJobs : %s" % libFileName)
        sql2 = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
        sql2+= JobSpec.bindValuesExpression()
        nTry=3
        try:
            # begin transaction
            self.conn.begin()
            # look for downstream jobs
            sqlD   = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 WHERE type=:type AND lfn=:lfn GROUP BY PandaID"
            sqlDJS = "SELECT %s " % JobSpec.columnNames()
            sqlDJS+= "FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
            sqlDJD = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
            sqlDJI = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
            sqlDJI+= JobSpec.bindValuesExpression()
            sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            _logger.debug("deleteStalledJobs : look for downstream jobs for %s" % libFileName)
            # select PandaID
            varMap = {}
            varMap[':lfn']  = libFileName
            varMap[':type'] = 'input'
            self.cur.arraysize = 100000
            self.cur.execute(sqlD+comment, varMap)
            res = self.cur.fetchall()
            for downID, in res:
                _logger.debug("deleteStalledJobs : delete %s" % downID)        
                # select jobs
                varMap = {}
                varMap[':PandaID'] = downID
                self.cur.arraysize = 10
                self.cur.execute(sqlDJS+comment, varMap)
                resJob = self.cur.fetchall()
                if len(resJob) == 0:
                    continue
                # instantiate JobSpec
                dJob = JobSpec()
                dJob.pack(resJob[0])
                # delete
                varMap = {}
                varMap[':PandaID'] = downID
                self.cur.execute(sqlDJD+comment, varMap)
                retD = self.cur.rowcount
                if retD == 0:
                    continue
                # error code
                dJob.jobStatus = 'cancelled'
                dJob.endTime   = datetime.datetime.utcnow()
                dJob.taskBufferErrorCode = ErrorCode.EC_Kill
                dJob.taskBufferErrorDiag = 'killed by Panda server : upstream job failed'
                dJob.modificationTime = dJob.endTime
                dJob.stateChangeTime  = dJob.endTime
                # insert
                self.cur.execute(sqlDJI+comment, dJob.valuesMap())
                # update files,metadata,parametes
                varMap = {}
                varMap[':PandaID'] = downID
                varMap[':modificationTime'] = dJob.modificationTime
                self.cur.execute(sqlFMod+comment,varMap)
                self.cur.execute(sqlMMod+comment,varMap)
                self.cur.execute(sqlPMod+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback(True)
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("deleteStalledJobs : %s %s" % (errtype,errvalue))
            return False


    # update Job status in jobsActive
    def updateJobStatus(self,pandaID,jobStatus,param,updateStateChange=False,attemptNr=None):
        comment = ' /* DBProxy.updateJobStatus */'        
        _logger.debug("updateJobStatus : PandaID=%s attemptNr=%s" % (pandaID,attemptNr))
        sql1 = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:jobStatus,modificationTime=CURRENT_DATE"
        if updateStateChange or jobStatus in ['starting']:
            sql1 += ",stateChangeTime=CURRENT_DATE"
        varMap = {}
        varMap[':jobStatus'] = jobStatus
        for key in param.keys():
            if param[key] != None:
                sql1 += ',%s=:%s' % (key,key)
                varMap[':%s' % key] = param[key]
                try:
                    # store positive error code even for pilot retry
                    if key == 'pilotErrorCode' and param[key].startswith('-'):
                        varMap[':%s' % key] = param[key][1:]
                except:
                    pass
        sql1 += " WHERE PandaID=:PandaID "
        varMap[':PandaID'] = pandaID
        if attemptNr != None:
            sql1 += "AND attemptNr=:attemptNr "
            varMap[':attemptNr'] = attemptNr
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.conn.begin()
                # update
                self.cur.execute (sql1+comment,varMap)
                nUp = self.cur.rowcount
                _logger.debug("updateJobStatus : PandaID=%s attemptNr=%s nUp=%s" % (pandaID,attemptNr,nUp))
                if nUp != 0:
                    # get command
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    self.cur.arraysize = 10
                    self.cur.execute ('SELECT commandToPilot,endTime FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID'+comment,varMap)
                    res = self.cur.fetchone()
                    if res != None:
                        ret     = res[0]
                        # convert no-command to 'NULL'
                        if ret == None:
                            ret = 'NULL'
                        # update endTime
                        endTime = res[1]
                        if jobStatus == 'holding' and endTime==None:
                            self.cur.execute ("UPDATE ATLAS_PANDA.jobsActive4 SET endTime=CURRENT_DATE WHERE PandaID=:PandaID"+comment,
                                              varMap)
                    else:
                        # already deleted
                        ret = 'tobekilled'
                else:
                    # attempt number inconsistent
                    ret = "badattemptnr"
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return ret
            except:
                # roll back
                self._rollback(True)
                if iTry+1 < nTry:
                    _logger.debug("updateJobStatus : %s retry : %s" % (pandaID,iTry))            
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("updateJobStatus : %s %s" % (type,value))
                _logger.error("updateJobStatus : %s" % pandaID)            
                return False


    # update job information in jobsActive or jobsDefined
    def updateJob(self,job,inJobsDefined):
        comment = ' /* DBProxy.updateJob */'        
        _logger.debug("updateJob : %s" % job.PandaID)
        if inJobsDefined:
            sql1 = "UPDATE ATLAS_PANDA.jobsDefined4 SET %s " % JobSpec.bindUpdateExpression()
        else:
            sql1 = "UPDATE ATLAS_PANDA.jobsActive4 SET %s " % JobSpec.bindUpdateExpression()            
        sql1+= "WHERE PandaID=:PandaID "
        if inJobsDefined:        
            sql1+= " AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) "
        nTry=3
        for iTry in range(nTry):
            try:
                job.modificationTime = datetime.datetime.utcnow()
                # set stateChangeTime for defined->assigned
                if inJobsDefined:
                    job.stateChangeTime = job.modificationTime
                # begin transaction
                self.conn.begin()
                # update
                varMap = job.valuesMap()
                varMap[':PandaID'] = job.PandaID
                if inJobsDefined:
                    varMap[':oldJobStatus1'] = 'assigned'
                    varMap[':oldJobStatus2'] = 'defined'
                self.cur.execute(sql1+comment, varMap)
                n = self.cur.rowcount                
                if n==0:
                    # already killed or activated
                    _logger.debug("updateJob : Not found %s" % job.PandaID)
                else:
                    for file in job.Files:
                        sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                        varMap = file.valuesMap(onlyChanged=True)
                        if varMap != {}:
                            varMap[':row_ID'] = file.row_ID
                            _logger.debug(sqlF+comment+str(varMap))
                            self.cur.execute(sqlF+comment, varMap)
                    # update job parameters
                    sqlJobP = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[':PandaID'] = job.PandaID
                    varMap[':param']   = job.jobParameters
                    self.cur.execute(sqlJobP+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True
            except:
                # roll back
                self._rollback(True)
                if iTry+1 < nTry:
                    _logger.debug("updateJob : %s retry : %s" % (job.PandaID,iTry))
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("updateJob : %s %s" % (type,value))
                return False


    # retry analysis job
    def retryJob(self,pandaID,param,failedInActive=False,changeJobInMem=False,inMemJob=None,
                 getNewPandaID=False):
        comment = ' /* DBProxy.retryJob */'                
        _logger.debug("retryJob : %s inActive=%s" % (pandaID,failedInActive))
        sql1 = "SELECT %s FROM ATLAS_PANDA.jobsActive4 " % JobSpec.columnNames()
        sql1+= "WHERE PandaID=:PandaID "
        if failedInActive:
            sql1+= "AND jobStatus=:jobStatus "
        sql2 = "UPDATE ATLAS_PANDA.jobsActive4 SET %s " % JobSpec.bindUpdateExpression()            
        sql2+= "WHERE PandaID=:PandaID "
        nTry=3
        for iTry in range(nTry):
            try:
                retValue = False
                if not changeJobInMem:
                    # begin transaction
                    self.conn.begin()
                    # select
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    if failedInActive:
                        varMap[':jobStatus'] = 'failed'
                    self.cur.arraysize = 10                
                    self.cur.execute(sql1+comment, varMap)
                    res = self.cur.fetchall()
                    if len(res) == 0:
                        _logger.debug("retryJob() : PandaID %d not found" % pandaID)
                        self._rollback()
                        return retValue
                    job = JobSpec()
                    job.pack(res[0])
                else:
                    job = inMemJob
                # don't use getNewPandaID for buildJob since the order of PandaIDs is broken
                if getNewPandaID and job.prodSourceLabel in ['panda']:
                    if not changeJobInMem:    
                        # commit
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                    # return
                    return retValue
                # check pilot retry
                usePilotRetry = False
                if job.prodSourceLabel in ['user','panda','ptest','rc_test'] and \
                   param.has_key('pilotErrorCode') and param['pilotErrorCode'].startswith('-') and \
                   job.maxAttempt > job.attemptNr and \
                   (not job.processingType.startswith('gangarobot') or job.processingType=='gangarobot-rctest') and \
                   not job.processingType.startswith('hammercloud'):
                    usePilotRetry = True
                # check if it's analysis job # FIXME once pilot retry works correctly the conditions below will be cleaned up
                if (((job.prodSourceLabel == 'user' or job.prodSourceLabel == 'panda') \
                     and not job.processingType.startswith('gangarobot') \
                     and not job.processingType.startswith('hammercloud') \
                     and job.computingSite.startswith('ANALY_') and param.has_key('pilotErrorCode') \
                     and param['pilotErrorCode'] in ['1200','1201'] and (not job.computingSite.startswith('ANALY_LONG_')) \
                     and job.attemptNr < 2) or (job.prodSourceLabel == 'ddm' and job.cloud == 'CA' and job.attemptNr <= 10) \
                     or failedInActive or usePilotRetry) \
                     and job.commandToPilot != 'tobekilled':
                    _logger.debug('reset PandaID:%s #%s' % (job.PandaID,job.attemptNr))
                    if not changeJobInMem:
                        # job parameters
                        sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[':PandaID'] = job.PandaID
                        self.cur.execute(sqlJobP+comment, varMap)
                        for clobJobP, in self.cur:
                            job.jobParameters = clobJobP.read()
                            break
                    # reset job
                    job.jobStatus = 'activated'
                    job.startTime = None
                    job.modificationTime = datetime.datetime.utcnow()
                    job.attemptNr = job.attemptNr + 1
                    if usePilotRetry:
                        job.currentPriority -= 10
                    if failedInActive:
                        job.endTime             = None
                        job.transExitCode       = None
                        for attr in job._attributes:
                            if attr.endswith('ErrorCode') or attr.endswith('ErrorDiag'):
                                setattr(job,attr,None)
                    # remove flag regarding to pledge-resource handling
                    if not job.specialHandling in [None,'NULL','']:
                        newSpecialHandling = re.sub(',*localpool','',job.specialHandling)
                        if newSpecialHandling == '':
                            job.specialHandling = None
                        else:
                            job.specialHandling = newSpecialHandling
                    # send it to long queue for analysis jobs
                    oldComputingSite = job.computingSite
                    if not changeJobInMem:
                        if job.computingSite.startswith('ANALY'):
                            longSite = None
                            tmpLongSiteList = []
                            tmpLongSite = re.sub('^ANALY_','ANALY_LONG_',job.computingSite)
                            tmpLongSite = re.sub('_\d+$','',tmpLongSite)
                            tmpLongSiteList.append(tmpLongSite)
                            tmpLongSite = job.computingSite + '_LONG'
                            tmpLongSiteList.append(tmpLongSite)
                            # loop over all possible long sitenames
                            for tmpLongSite in tmpLongSiteList:
                                varMap = {}
                                varMap[':siteID'] = tmpLongSite
                                varMap[':status'] = 'online'
                                sqlSite = "SELECT COUNT(*) FROM ATLAS_PANDAMETA.schedconfig WHERE siteID=:siteID AND status=:status"
                                self.cur.execute(sqlSite+comment, varMap)
                                resSite = self.cur.fetchone()
                                if resSite != None and resSite[0] > 0:
                                    longSite = tmpLongSite
                                    break
                            # use long site if exists
                            if longSite != None:
                                job.computingSite = longSite
                                # set destinationSE if queue is changed
                                if oldComputingSite == job.destinationSE:
                                    job.destinationSE = job.computingSite
                    if not changeJobInMem:                                
                        # select files
                        varMap = {}
                        varMap[':PandaID'] = job.PandaID
                        if not getNewPandaID:
                            varMap[':type1'] = 'log'
                            varMap[':type2'] = 'output'
                        sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                        if not getNewPandaID:
                            sqlFile+= "WHERE PandaID=:PandaID AND (type=:type1 OR type=:type2)"
                        else:
                            sqlFile+= "WHERE PandaID=:PandaID"
                        self.cur.arraysize = 100
                        self.cur.execute(sqlFile+comment, varMap)
                        resFs = self.cur.fetchall()
                    else:
                        # get log or output files only
                        resFs = []
                        for tmpFile in job.Files:
                            if tmpFile.type in ['log','output']:
                                resFs.append(tmpFile)
                    # loop over all files            
                    for resF in resFs:
                        if not changeJobInMem:
                            # set PandaID
                            file = FileSpec()
                            file.pack(resF)
                            job.addFile(file)
                        else:
                            file = resF
                        # set new GUID
                        if file.type == 'log':
                            file.GUID = commands.getoutput('uuidgen')
                        # don't change input and lib.tgz    
                        if file.type == 'input' or (file.type == 'output' and job.prodSourceLabel == 'panda') or \
                               (file.type == 'output' and file.lfn.endswith('.lib.tgz') and job.prodSourceLabel in ['rc_test','ptest']):
                            continue
                        # append attemptNr to LFN
                        oldName = file.lfn
                        file.lfn = re.sub('\.\d+$','',file.lfn)
                        file.lfn = '%s.%s' % (file.lfn,job.attemptNr)
                        newName = file.lfn
                        # set destinationSE
                        if oldComputingSite == file.destinationSE:
                            file.destinationSE = job.computingSite
                        # modify jobParameters
                        sepPatt = "(\'|\"|%20)" + oldName + "(\'|\"|%20)"
                        matches = re.findall(sepPatt,job.jobParameters)
                        for match in matches:
                            oldPatt = match[0]+oldName+match[-1]
                            newPatt = match[0]+newName+match[-1]
                            job.jobParameters = re.sub(oldPatt,newPatt,job.jobParameters)
                        if not changeJobInMem and not getNewPandaID:    
                            # update files
                            sqlFup = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                            varMap = file.valuesMap(onlyChanged=True)
                            if varMap != {}:
                                varMap[':row_ID'] = file.row_ID
                                self.cur.execute(sqlFup+comment, varMap)
                    if not changeJobInMem:
                        # reuse original PandaID
                        if not getNewPandaID:
                            # update job
                            varMap = job.valuesMap()
                            varMap[':PandaID'] = job.PandaID
                            self.cur.execute(sql2+comment, varMap)
                            # update job parameters
                            sqlJobP = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                            varMap = {}
                            varMap[':PandaID'] = job.PandaID
                            varMap[':param']   = job.jobParameters
                            self.cur.execute(sqlJobP+comment, varMap)
                        else:
                            # read metadata
                            sqlMeta = "SELECT metaData FROM ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"
                            varMap = {}
                            varMap[':PandaID'] = job.PandaID
                            self.cur.execute(sqlMeta+comment, varMap)
                            for clobJobP, in self.cur:
                                job.metadata = clobJobP.read()
                                break
                            # insert job with new PandaID
                            sql1 = "INSERT INTO ATLAS_PANDA.jobsActive4 (%s) " % JobSpec.columnNames()
                            sql1+= JobSpec.bindValuesExpression(useSeq=True)
                            sql1+= " RETURNING PandaID INTO :newPandaID"
                            # set parentID
                            job.parentID = job.PandaID
                            varMap = job.valuesMap(useSeq=True)
                            varMap[':newPandaID'] = self.cur.var(cx_Oracle.NUMBER)
                            # insert
                            retI = self.cur.execute(sql1+comment, varMap)
                            # set PandaID
                            job.PandaID = long(varMap[':newPandaID'].getvalue())
                            _logger.debug('Generate new PandaID %s -> %s #%s' % (job.parentID,job.PandaID,job.attemptNr))
                            # insert files
                            sqlFile = "INSERT INTO ATLAS_PANDA.filesTable4 (%s) " % FileSpec.columnNames()
                            sqlFile+= FileSpec.bindValuesExpression(useSeq=True)
                            sqlFile+= " RETURNING row_ID INTO :newRowID"
                            for file in job.Files:
                                # reset rowID
                                file.row_ID = None
                                # insert
                                varMap = file.valuesMap(useSeq=True)
                                varMap[':newRowID'] = self.cur.var(cx_Oracle.NUMBER)
                                self.cur.execute(sqlFile+comment, varMap)
                            # update mod time for files
                            varMap = {}
                            varMap[':PandaID'] = job.PandaID
                            varMap[':modificationTime'] = job.modificationTime
                            sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
                            self.cur.execute(sqlFMod+comment,varMap)
                            # metadata
                            sqlMeta = "INSERT INTO ATLAS_PANDA.metaTable (PandaID,metaData,modificationTime) VALUES (:PandaID,:metaData,:modTime)"
                            varMap = {}
                            varMap[':PandaID']  = job.PandaID
                            varMap[':metaData'] = job.metadata
                            varMap[':modTime']  = job.modificationTime                            
                            self.cur.execute(sqlMeta+comment, varMap)
                            # job parameters
                            sqlJob = "INSERT INTO ATLAS_PANDA.jobParamsTable (PandaID,jobParameters,modificationTime) VALUES (:PandaID,:param,:modTime)"
                            varMap = {}
                            varMap[':PandaID'] = job.PandaID
                            varMap[':param']   = job.jobParameters
                            varMap[':modTime'] = job.modificationTime
                            self.cur.execute(sqlJob+comment, varMap)
                            # set error code to original job to avoid being retried by another process
                            sqlE = "UPDATE ATLAS_PANDA.jobsActive4 SET taskBufferErrorCode=:errCode,taskBufferErrorDiag=:errDiag WHERE PandaID=:PandaID"
                            varMap = {}
                            varMap[':PandaID'] = job.parentID
                            varMap[':errCode'] = ErrorCode.EC_PilotRetried
                            varMap[':errDiag'] = 'retrying at the same site. new PandaID=%s' % job.PandaID
                            self.cur.execute(sqlE+comment, varMap)
                    # set return
                    if not getNewPandaID:
                        retValue = True
                if not changeJobInMem:    
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                return retValue
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("retryJob : %s retry : %s" % (pandaID,iTry))
                    time.sleep(random.randint(10,20))
                    continue
                # error report
                type, value, traceBack = sys.exc_info()
                _logger.error("retryJob : %s %s" % (type,value))
                return False


    # retry failed analysis jobs in Active4
    def retryJobsInActive(self,prodUserName,jobDefinitionID):
        comment = ' /* DBProxy.retryJobsInActive */'                
        _logger.debug("retryJobsInActive : start - %s %s" % (prodUserName,jobDefinitionID))
        try:
            # begin transaction
            self.conn.begin()
            # count the number of jobs in Defined
            sqlC  = "SELECT COUNT(*) FROM ATLAS_PANDA.jobsDefined4 "
            sqlC += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
            sqlC += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
            varMap = {}
            varMap[':prodUserName']    = prodUserName
            varMap[':jobDefinitionID'] = jobDefinitionID
            varMap[':prodSourceLabel1'] = 'user'
            varMap[':prodSourceLabel2'] = 'panda'            
            self.cur.arraysize = 10
            self.cur.execute(sqlC+comment,varMap)
            res = self.cur.fetchone()
            # failed to get the number of jobs in Defined
            if res == None:
                _logger.error("retryJobsInActive : %s %s - failed to get num of jobs in Def" % (prodUserName,jobDefinitionID))                
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return None for DB error 
                return None
            nJobsInDef = res[0]
            # get failed PandaIDs in Active
            sql0 = "SELECT PandaID,jobStatus,taskBufferErrorCode FROM ATLAS_PANDA.jobsActive4 "
            sql0+= "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
            sql0+= "AND prodSourceLabel=:prodSourceLabel "
            varMap = {}
            varMap[':prodUserName']    = prodUserName
            varMap[':jobDefinitionID'] = jobDefinitionID
            varMap[':prodSourceLabel'] = 'user'
            self.cur.execute(sql0+comment,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # the number of jobs in Active
            nJobsInAct = len(res)
            # loop over all PandaID
            failedPandaIDs = []
            for pandaID,tmpJobStatus,tmpTaskBufferErrorCode in res:
                if tmpJobStatus == 'failed' and not tmpTaskBufferErrorCode in \
                       [ErrorCode.EC_Reassigned,ErrorCode.EC_Retried,ErrorCode.EC_PilotRetried]:
                    failedPandaIDs.append(pandaID)
            _logger.debug("retryJobsInActive : %s %s - %s failed jobs" % (prodUserName,jobDefinitionID,len(failedPandaIDs)))
            # there are some failed jobs in Active
            if failedPandaIDs != []:
                # get list of sub datasets to lock Closer
                sqlF  = "SELECT DISTINCT destinationDBlock FROM ATLAS_PANDA.filesTable4 "
                sqlF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
                varMap = {}
                varMap[':PandaID'] = failedPandaIDs[0]
                varMap[':type1']   = 'log'
                varMap[':type2']   = 'output'                
                # begin transaction
                self.conn.begin()
                self.cur.arraysize = 100000
                self.cur.execute(sqlF+comment,varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                subDsList = []
                for tmpDSname, in res:
                    tmpDS = self.queryDatasetWithMap({'name':tmpDSname})
                    if tmpDS == None:
                        _logger.error("retryJobsInActive : %s %s - failed to get DS=%s" % (prodUserName,jobDefinitionID,tmpDSname))
                        # return None for DB error 
                        return None
                    # append    
                    subDsList.append(tmpDS)
                # lock datasets
                lockedDS = True
                ngStatus = ['closed','tobeclosed','completed','tobemerged','merging','cleanup']
                sqlD = "UPDATE ATLAS_PANDA.Datasets SET status=:status,modificationdate=CURRENT_DATE "
                sqlD+= "WHERE vuid=:vuid AND NOT status IN ("
                for tmpIdx,tmpNgStat in enumerate(ngStatus):
                    sqlD += ':ngSt%s,' % tmpIdx
                sqlD = sqlD[:-1]
                sqlD += ") "
                self.conn.begin()
                self.cur.arraysize = 10
                for tmpDS in subDsList:
                    varMap = {}
                    varMap[':status'] = 'locked'
                    varMap[':vuid'] = tmpDS.vuid
                    for tmpIdx,tmpNgStat in enumerate(ngStatus):
                        tmpKey = ':ngSt%s' % tmpIdx
                        varMap[tmpKey] = tmpNgStat
                    # update
                    self.cur.execute(sqlD+comment,varMap)
                    retD = self.cur.rowcount
                    # datasets already closed  
                    if retD == 0:
                        # roll back
                        self._rollback()
                        # failed to lock datasets
                        _logger.debug("retryJobsInActive : %s %s - %s is closed" % (prodUserName,jobDefinitionID,tmpDS.name))
                        lockedDS = False
                        break
                # retry jobs                    
                if lockedDS:
                    # commit for dataset lock
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # loop over all PandaIDs
                    for pandaID in failedPandaIDs:
                        self.retryJob(pandaID,{},failedInActive=True)
                    # unlock datasets
                    sqlDU = "UPDATE ATLAS_PANDA.Datasets SET status=:nStatus,modificationdate=CURRENT_DATE "
                    sqlDU+= "WHERE vuid=:vuid AND status=:oStatus"
                    self.conn.begin()
                    self.cur.arraysize = 10
                    for tmpDS in subDsList:
                        varMap = {}
                        varMap[':oStatus'] = 'locked'
                        varMap[':nStatus'] = tmpDS.status
                        varMap[':vuid'] = tmpDS.vuid
                        # update
                        self.cur.execute(sqlDU+comment,varMap)
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
            # return True when job is active
            retVal = False
            if nJobsInAct > 0 or nJobsInDef > 0:
                retVal = True
            _logger.debug("retryJobsInActive : end %s - %s %s" % (retVal,prodUserName,jobDefinitionID))                
            return retVal
        except:
            # roll back
            self._rollback()
            # error report
            errType,errValue = sys.exc_info()[:2]
            _logger.error("retryJobsInActive : %s %s" % (errType,errValue))
            return None

        
    # get jobs
    def getJobs(self,nJobs,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                atlasRelease,prodUserID,countryGroup,workingGroup,allowOtherCountry):
        comment = ' /* DBProxy.getJobs */'
        # use memcache
        useMemcache = False
        try:
            if panda_config.memcached_enable and siteName in ['MWT2_UC','ANALY_MWT2','BNL_ATLAS_test','ANALY_BNL_test',
                                                              'ANALY_GLASGOW']: # FIXME
                # initialize memcache
                if self.memcache == None:
                    from MemProxy import MemProxy
                    self.memcache = MemProxy()
                if not self.memcache in [None,False]:
                    useMemcache = True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("failed to initialize memcached with %s %s" % (errType,errValue))
        # aggregated sites which use different appdirs
        aggSiteMap = {'CERN-PROD':{'CERN-RELEASE':'release',
                                   'CERN-UNVALID':'unvalid',
                                   'CERN-BUILDS' :'builds',
                                   },
                      }
        # construct where clause   
        dynamicBrokering = False
        getValMap = {}
        getValMap[':oldJobStatus'] = 'activated'
        getValMap[':computingSite'] = siteName
        if not aggSiteMap.has_key(siteName):
            sql1 = "WHERE jobStatus=:oldJobStatus AND computingSite=:computingSite AND commandToPilot IS NULL "
        else:
            # aggregated sites 
            sql1 = "WHERE jobStatus=:oldJobStatus AND computingSite IN (:computingSite,"
            for tmpAggIdx,tmpAggSite in enumerate(aggSiteMap[siteName].keys()):
                tmpKeyName = ':computingSite%s' % tmpAggIdx
                sql1 += '%s,' % tmpKeyName
                getValMap[tmpKeyName] = tmpAggSite
            sql1 = sql1[:-1]
            sql1 += ") AND commandToPilot IS NULL "
        if not mem in [0,'0']:
            sql1+= "AND (minRamCount<=:minRamCount OR minRamCount=0) "
            getValMap[':minRamCount'] = mem
        if not diskSpace in [0,'0']:
            sql1+= "AND (maxDiskCount<=:maxDiskCount OR maxDiskCount=0) "
            getValMap[':maxDiskCount'] = diskSpace
        if prodSourceLabel == 'user':
            sql1+= "AND (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2) "
            getValMap[':prodSourceLabel1'] = 'user'
            getValMap[':prodSourceLabel2'] = 'panda'
        elif prodSourceLabel == 'ddm':
            dynamicBrokering = True
            sql1+= "AND prodSourceLabel=:prodSourceLabel "
            getValMap[':prodSourceLabel'] = 'ddm'
        elif prodSourceLabel in [None,'managed']:
            sql1+= "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3) "
            getValMap[':prodSourceLabel1'] = 'managed'
            getValMap[':prodSourceLabel2'] = 'test'
            getValMap[':prodSourceLabel3'] = 'prod_test'            
        elif prodSourceLabel == 'software':
            sql1+= "AND prodSourceLabel=:prodSourceLabel "
            getValMap[':prodSourceLabel'] = 'software'
        elif prodSourceLabel == 'test' and computingElement != None:
            dynamicBrokering = True
            sql1+= "AND (processingType IN (:processingType1,:processingType2,:processingType3) OR prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2)) "
            getValMap[':processingType1']   = 'gangarobot'
            getValMap[':processingType2']   = 'analy_test'            
            getValMap[':processingType3']   = 'prod_test'            
            getValMap[':prodSourceLabel1']  = 'test'
            getValMap[':prodSourceLabel2']  = 'prod_test'            
        else:
            sql1+= "AND prodSourceLabel=:prodSourceLabel "
            getValMap[':prodSourceLabel'] = prodSourceLabel
        # user ID
        if prodUserID != None:
            # get compact DN
            compactDN = self.cleanUserID(prodUserID)
            if compactDN in ['','NULL',None]:
                compactDN = prodUserID
            sql1+= "AND prodUserName=:prodUserName " 
            getValMap[':prodUserName'] = compactDN
        # country group
        specialHandled = False
        if prodSourceLabel == 'user':
            # update pledge resource ratio
            self.getPledgeResourceRatio()
            # other country is allowed to use the pilot
            if allowOtherCountry=='True' and self.beyondPledgeRatio.has_key(siteName) and self.beyondPledgeRatio[siteName] > 0:
                # check if countryGroup needs to be used for beyond-pledge
                if self.checkCountryGroupForBeyondPledge(siteName):
                    countryGroup = self.beyondPledgeRatio[siteName]['countryGroup']
                    specialHandled = True
                else:
                    countryGroup = ''
            # countryGroup
            if not countryGroup in ['',None]:
                sql1+= "AND countryGroup IN ("
                idxCountry = 1
                for tmpCountry in countryGroup.split(','):
                    tmpKey = ":countryGroup%s" % idxCountry
                    sql1+= "%s," % tmpKey
                    getValMap[tmpKey] = tmpCountry
                    idxCountry += 1
                sql1 = sql1[:-1]
                sql1+= ") "
            # workingGroup    
            if not workingGroup in ['',None]:
                sql1+= "AND workingGroup IN ("
                idxWorking = 1
                for tmpWorking in workingGroup.split(','):
                    tmpKey = ":workingGroup%s" % idxWorking
                    sql1+= "%s," % tmpKey
                    getValMap[tmpKey] = tmpWorking
                    idxWorking += 1
                sql1 = sql1[:-1]
                sql1+= ") "
        # production share
        if prodSourceLabel in ['managed',None,'sharetest']:
            aggSitesForFairshare = []
            if aggSiteMap.has_key(siteName):
                aggSitesForFairshare = aggSiteMap[siteName].keys()
            shareSQL,shareVarMap = self.getCriteriaForProdShare(siteName,aggSitesForFairshare)
            if shareVarMap != {}:
                sql1 += shareSQL
                for tmpShareKey in shareVarMap.keys():
                    getValMap[tmpShareKey] = shareVarMap[tmpShareKey] 
        sql2 = "SELECT %s FROM ATLAS_PANDA.jobsActive4 " % JobSpec.columnNames()
        sql2+= "WHERE PandaID=:PandaID"
        retJobs = []
        nSent = 0           
        try:
            timeLimit = datetime.timedelta(seconds=timeout-10)
            timeStart = datetime.datetime.utcnow()
            strName   = datetime.datetime.isoformat(timeStart)
            attLimit  = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
            attSQL    = "AND ((creationTime<:creationTime AND attemptNr>1) OR attemptNr<=1) "
            # get nJobs
            for iJob in range(nJobs):
                pandaID = 0
                fileMapForMem = {}
                # select channel for ddm jobs
                if prodSourceLabel == 'ddm':
                    sqlDDM = "SELECT count(*),jobStatus,sourceSite,destinationSite,transferType FROM ATLAS_PANDA.jobsActive4 WHERE computingSite=:computingSite AND prodSourceLabel=:prodSourceLabel " \
                             + attSQL + "GROUP BY jobStatus,sourceSite,destinationSite,transferType"
                    ddmValMap = {}
                    ddmValMap[':computingSite']   = siteName
                    ddmValMap[':creationTime']    = attLimit
                    ddmValMap[':prodSourceLabel'] = 'ddm'
                    _logger.debug(sqlDDM+comment+str(ddmValMap))
                    # start transaction
                    self.conn.begin()
                    # select
                    self.cur.arraysize = 100                    
                    self.cur.execute(sqlDDM+comment, ddmValMap)
                    resDDM = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # make a channel map
                    channelMap = {}
                    for tmp_count,tmp_jobStatus,tmp_sourceSite,tmp_destinationSite,tmp_transferType in resDDM:
                        # use source,dest,type as the key
                        channel = (tmp_sourceSite,tmp_destinationSite,tmp_transferType)
                        if not channelMap.has_key(channel):
                            channelMap[channel] = {}
                        # ignore holding
                        if tmp_jobStatus == 'holding':
                            continue
                        # distinguish activate from other stats
                        if tmp_jobStatus != 'activated':
                            tmp_jobStatus = 'others'
                        # append
                        if not channelMap[channel].has_key(tmp_jobStatus):
                            channelMap[channel][tmp_jobStatus] = int(tmp_count)
                        else:
                            channelMap[channel][tmp_jobStatus] += int(tmp_count)
                    _logger.debug(channelMap)
                    # choose channel
                    channels = channelMap.keys()
                    random.shuffle(channels)
                    foundChannel = False
                    for channel in channels:
                        # no activated jobs
                        if (not channelMap[channel].has_key('activated')) or channelMap[channel]['activated'] == 0:
                            continue
                        maxRunning = 10
                        # prestaging job
                        if channel[0] == channel[1] and channel[2] == 'dis':
                            maxRunning = 50
                        if (not channelMap[channel].has_key('others')) or channelMap[channel]['others'] < maxRunning:
                            # set SQL
                            sql1+= "AND sourceSite=:sourceSite AND destinationSite=:destinationSite AND transferType=:transferType "
                            getValMap[':sourceSite']      = channel[0]
                            getValMap[':destinationSite'] = channel[1]
                            getValMap[':transferType']    = channel[2]
                            foundChannel = True
                            break
                    # no proper channel
                    if not foundChannel:
                        _logger.debug("getJobs : no DDM jobs for Site %s" % siteName)
                        break
                # get job
                if prodSourceLabel in ['ddm']:
                    # to add some delay for attempts
                    sql1 += attSQL
                    getValMap[':creationTime'] = attLimit                    
                nTry=1
                for iTry in range(nTry):
                    # set siteID
                    tmpSiteID = siteName
                    if siteName.startswith('ANALY_BNL_ATLAS'):
                        tmpSiteID = 'ANALY_BNL_ATLAS_1'
                    # get file lock
                    _logger.debug("getJobs : %s -> lock" % strName)
                    if (datetime.datetime.utcnow() - timeStart) < timeLimit:
                        toGetPandaIDs = True
                        pandaIDs = []
                        specialHandlingMap = {}
                        # get max priority for analysis jobs
                        if prodSourceLabel in ['panda','user']:
                            sqlMX = "SELECT /*+ INDEX_RS_ASC(tab (PRODSOURCELABEL COMPUTINGSITE JOBSTATUS) ) */ MAX(currentPriority) FROM ATLAS_PANDA.jobsActive4 tab "
                            sqlMX+= sql1
                            _logger.debug(sqlMX+comment+str(getValMap))
                            # start transaction
                            self.conn.begin()
                            # select
                            self.cur.arraysize = 10                            
                            self.cur.execute(sqlMX+comment, getValMap)
                            tmpPriority, = self.cur.fetchone()
                            # commit
                            if not self._commit():
                                raise RuntimeError, 'Commit error'
                            # no jobs
                            if tmpPriority == None:
                                toGetPandaIDs = False
                            else:
                                # set priority
                                sql1 += "AND currentPriority=:currentPriority"
                                getValMap[':currentPriority'] = tmpPriority
                        maxAttemptIDx = 10
                        if toGetPandaIDs:
                            # get PandaIDs
                            sqlP = "SELECT /*+ INDEX_RS_ASC(tab (PRODSOURCELABEL COMPUTINGSITE JOBSTATUS) ) */ PandaID,currentPriority,specialHandling FROM ATLAS_PANDA.jobsActive4 tab "
                            sqlP+= sql1
                            _logger.debug(sqlP+comment+str(getValMap))
                            # start transaction
                            self.conn.begin()
                            # select
                            self.cur.arraysize = 100000
                            self.cur.execute(sqlP+comment, getValMap)
                            resIDs = self.cur.fetchall()
                            # commit
                            if not self._commit():
                                raise RuntimeError, 'Commit error'
                            maxCurrentPriority = None
                            # get max priority and min PandaID
                            for tmpPandaID,tmpCurrentPriority,tmpSpecialHandling in resIDs:
                                if maxCurrentPriority==None or maxCurrentPriority < tmpCurrentPriority:
                                    maxCurrentPriority = tmpCurrentPriority
                                    pandaIDs = [tmpPandaID]
                                elif maxCurrentPriority == tmpCurrentPriority:
                                    pandaIDs.append(tmpPandaID)
                                specialHandlingMap[tmpPandaID] = tmpSpecialHandling    
                            # sort
                            pandaIDs.sort()
                        if pandaIDs == []:
                            _logger.debug("getJobs : %s -> no PandaIDs" % strName)
                            retU = 0
                        else:
                            # check the number of available files
                            if useMemcache:
                                _logger.debug("getJobs : %s -> memcache check start" % strName)                                
                                # truncate
                                pandaIDs = pandaIDs[:maxAttemptIDx]
                                # get input files
                                availableFileMap = {}
                                self.cur.arraysize = 100000
                                sqlMemFile = "SELECT lfn FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type"
                                for tmpPandaID in pandaIDs:
                                    varMap = {}
                                    varMap[':type'] = 'input'
                                    varMap[':PandaID'] = tmpPandaID
                                    # start transaction
                                    self.conn.begin()
                                    # select
                                    self.cur.execute(sqlMemFile+comment,varMap)
                                    resFiles = self.cur.fetchall()
                                    # commit
                                    if not self._commit():
                                        raise RuntimeError, 'Commit error'
                                    # get list
                                    fileMapForMem[tmpPandaID] = []
                                    for tmpItem, in resFiles:
                                        fileMapForMem[tmpPandaID].append(tmpItem)
                                    # get number of available files
                                    nAvailable = self.memcache.checkFiles(tmpPandaID,fileMapForMem[tmpPandaID],
                                                                          siteName,node)
                                    # append
                                    if not nAvailable in availableFileMap:
                                        availableFileMap[nAvailable] = []
                                    availableFileMap[nAvailable].append(tmpPandaID)
                                # sort by the number of available files
                                tmpAvaKeys = availableFileMap.keys()
                                tmpAvaKeys.sort()
                                tmpAvaKeys.reverse()
                                pandaIDs = []
                                for tmpAvaKey in tmpAvaKeys:
                                    pandaIDs += availableFileMap[tmpAvaKey]
                                _logger.debug("getJobs : %s -> memcache check done" % strName)
                            # update
                            for indexID,tmpPandaID in enumerate(pandaIDs):
                                # max attempts
                                if indexID > maxAttemptIDx:
                                    break
                                # update
                                sqlJ = "UPDATE ATLAS_PANDA.jobsActive4 "
                                sqlJ+= "SET jobStatus=:newJobStatus,modificationTime=CURRENT_DATE,modificationHost=:modificationHost,startTime=CURRENT_DATE"
                                varMap = {}
                                varMap[':PandaID']          = tmpPandaID
                                varMap[':newJobStatus']     = 'sent'
                                varMap[':oldJobStatus']     = 'activated'
                                varMap[':modificationHost'] = node
                                # set CE
                                if computingElement != None:
                                    sqlJ+= ",computingElement=:computingElement"
                                    varMap[':computingElement'] =  computingElement
                                # set special handlng
                                if specialHandled:
                                    sqlJ+= ",specialHandling=:specialHandling"
                                    spString = 'localpool'
                                    if specialHandlingMap.has_key(tmpPandaID) and isinstance(specialHandlingMap[tmpPandaID],types.StringType):
                                        if not spString in specialHandlingMap[tmpPandaID]:
                                            varMap[':specialHandling'] = specialHandlingMap[tmpPandaID]+','+spString
                                        else:
                                            varMap[':specialHandling'] = specialHandlingMap[tmpPandaID]
                                    else:
                                        varMap[':specialHandling'] = spString
                                sqlJ+= " WHERE PandaID=:PandaID AND jobStatus=:oldJobStatus"
                                # SQL to get nSent
                                sentLimit = timeStart - datetime.timedelta(seconds=60)
                                sqlSent  = "SELECT /*+ INDEX_COMBINE(tab JOBSACTIVE4_JOBSTATUS_IDX JOBSACTIVE4_COMPSITE_IDX) */ count(*) FROM ATLAS_PANDA.jobsActive4 tab WHERE jobStatus=:jobStatus "
                                sqlSent += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
                                sqlSent += "AND computingSite=:computingSite "
                                sqlSent += "AND modificationTime>:modificationTime "
                                varMapSent = {}
                                varMapSent[':jobStatus'] = 'sent'
                                varMapSent[':computingSite'] = tmpSiteID
                                varMapSent[':modificationTime'] = sentLimit
                                varMapSent[':prodSourceLabel1'] = 'managed'
                                varMapSent[':prodSourceLabel2'] = 'test'
                                # start
                                _logger.debug(sqlJ+comment+str(varMap))
                                # start transaction
                                self.conn.begin()
                                # update
                                self.cur.execute(sqlJ+comment, varMap)
                                retU = self.cur.rowcount
                                if retU != 0:
                                    # get nSent for production jobs
                                    if prodSourceLabel in [None,'managed']:
                                        _logger.debug(sqlSent+comment+str(varMapSent))                                        
                                        self.cur.execute(sqlSent+comment, varMapSent)
                                        resSent = self.cur.fetchone()
                                        if resSent != None:
                                            nSent, = resSent
                                # commit
                                if not self._commit():
                                    raise RuntimeError, 'Commit error'
                                # succeeded
                                if retU != 0:
                                    pandaID = tmpPandaID
                                    break
                    else:
                        _logger.debug("getJobs : %s -> do nothing" % strName)
                        retU = 0
                    # release file lock
                    _logger.debug("getJobs : %s -> unlock" % strName)
                    # succeeded
                    if retU != 0:
                        break
                    if iTry+1 < nTry:
                        #time.sleep(0.5)
                        pass
                # failed to UPDATE
                if retU == 0:
                    # reset pandaID
                    pandaID = 0
                _logger.debug("getJobs : Site %s : retU %s : PandaID %s - %s"
                              % (siteName,retU,pandaID,prodSourceLabel))
                if pandaID == 0:
                    break
                # start transaction
                self.conn.begin()
                # select
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.arraysize = 10                
                self.cur.execute(sql2+comment, varMap)
                res = self.cur.fetchone()
                if len(res) == 0:
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    break
                # instantiate Job
                job = JobSpec()
                job.pack(res)
                # Files
                sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                sqlFile+= "WHERE PandaID=:PandaID"
                self.cur.arraysize = 10000                                
                self.cur.execute(sqlFile+comment, varMap)
                resFs = self.cur.fetchall()
                # job parameters
                sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                varMap = {}
                varMap[':PandaID'] = job.PandaID
                self.cur.execute(sqlJobP+comment, varMap)
                for clobJobP, in self.cur:
                    job.jobParameters = clobJobP.read()
                    break
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                for resF in resFs:
                    file = FileSpec()
                    file.pack(resF)
                    job.addFile(file)
                # overwrite processingType for appdir at aggrigates sites
                if aggSiteMap.has_key(siteName):
                    if aggSiteMap[siteName].has_key(job.computingSite):
                        job.processingType = aggSiteMap[siteName][job.computingSite]
                        job.computingSite  = job.computingSite
                # append
                retJobs.append(job)
            return retJobs,nSent
        except:
            # roll back
            self._rollback()
            # error report
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobs : %s %s" % (type,value))
            return [],0
        

    # reset job in jobsActive or jobsWaiting
    def resetJob(self,pandaID,activeTable=True,keepSite=False,getOldSubs=False,forPending=True):
        comment = ' /* DBProxy.resetJob */'        
        _logger.debug("resetJobs : %s" % pandaID)
        # select table
        table = 'ATLAS_PANDA.jobsWaiting4'        
        if activeTable:
            table = 'ATLAS_PANDA.jobsActive4'
        sql1 = "SELECT %s FROM %s " % (JobSpec.columnNames(),table)
        sql1+= "WHERE PandaID=:PandaID"
        sql2 = "DELETE FROM %s " % table
        sql2+= "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
        sql3 = "INSERT INTO ATLAS_PANDA.jobsDefined4 (%s) " % JobSpec.columnNames()
        sql3+= JobSpec.bindValuesExpression()
        try:
            # transaction causes Request ndbd time-out in ATLAS_PANDA.jobsActive4
            self.conn.begin()
            # select
            varMap = {}
            varMap[':PandaID'] = pandaID
            self.cur.arraysize = 10                
            self.cur.execute(sql1+comment,varMap)
            res = self.cur.fetchone()
            # not found
            if res == None:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                return None
            # instantiate Job
            job = JobSpec()
            job.pack(res)
            # if already running
            if job.jobStatus != 'waiting' and job.jobStatus != 'activated' \
                   and (forPending and job.jobStatus != 'pending'):
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                return None
            # do nothing for analysis jobs
            if job.prodSourceLabel in ['user','panda']:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                return None
            # delete
            varMap = {}
            varMap[':PandaID'] = pandaID
            if not forPending:
                varMap[':oldJobStatus1'] = 'waiting'
            else:
                varMap[':oldJobStatus1'] = 'pending'
            varMap[':oldJobStatus2'] = 'activated'
            self.cur.execute(sql2+comment,varMap)
            retD = self.cur.rowcount            
            # delete failed
            _logger.debug("resetJobs : retD = %s" % retD)
            if retD != 1:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return None
            # delete from jobsDefined4 just in case
            varMap = {}
            varMap[':PandaID'] = pandaID
            sqlD = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
            self.cur.execute(sqlD+comment,varMap)
            # increase priority
            if job.jobStatus == 'activated' and job.currentPriority < 100:
                job.currentPriority = 100
            # reset computing site and dispatchDBlocks
            job.jobStatus = 'defined'
            job.dispatchDBlock   = None
            # erase old assignment
            if (not keepSite) and job.relocationFlag != 1:
                job.computingSite = None
            job.computingElement = None
            # host and time information
            job.modificationHost = self.hostname
            job.modificationTime = datetime.datetime.utcnow()
            job.stateChangeTime  = job.modificationTime
            # reset
            job.brokerageErrorDiag = None
            job.brokerageErrorCode = None
            # insert
            self.cur.execute(sql3+comment, job.valuesMap())
            # job parameters
            sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
            self.cur.execute(sqlJobP+comment, varMap)
            for clobJobP, in self.cur:
                job.jobParameters = clobJobP.read()
                break
            # Files
            oldSubList = []
            sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
            sqlFile+= "WHERE PandaID=:PandaID"
            self.cur.arraysize = 10000
            self.cur.execute(sqlFile+comment, varMap)
            resFs = self.cur.fetchall()
            for resF in resFs:
                file = FileSpec()
                file.pack(resF)
                # reset GUID to trigger LRC/LFC scanning
                if file.status == 'missing':
                    file.GUID = None
                # collect old subs
                if job.prodSourceLabel in ['managed','test'] and file.type in ['output','log'] \
                       and re.search('_sub\d+$',file.destinationDBlock) != None:
                    if not file.destinationDBlock in oldSubList:
                        oldSubList.append(file.destinationDBlock)
                # reset status, destinationDBlock and dispatchDBlock
                file.status         ='unknown'
                file.dispatchDBlock = None
                file.destinationDBlock = re.sub('_sub\d+$','',file.destinationDBlock)
                # add file
                job.addFile(file)                
                # update files
                sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                varMap = file.valuesMap(onlyChanged=True)
                if varMap != {}:
                    varMap[':row_ID'] = file.row_ID
                    _logger.debug(sqlF+comment+str(varMap))                    
                    self.cur.execute(sqlF+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if getOldSubs:
                return job,oldSubList
            return job
        except:
            # roll back
            self._rollback()
            # error report
            type, value, traceBack = sys.exc_info()
            _logger.error("resetJobs : %s %s" % (type,value))
            _logger.error("resetJobs : %s" % pandaID)
            return None


    # reset jobs in jobsDefined
    def resetDefinedJob(self,pandaID,keepSite=False,getOldSubs=False):
        comment = ' /* DBProxy.resetDefinedJob */'                
        _logger.debug("resetDefinedJob : %s" % pandaID)
        sql1  = "UPDATE ATLAS_PANDA.jobsDefined4 SET "
        sql1 += "jobStatus=:newJobStatus,"
        sql1 += "modificationTime=CURRENT_DATE,"
        sql1 += "dispatchDBlock=NULL,"
        sql1 += "computingElement=NULL"         
        sql1 += " WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
        sql2 = "SELECT %s FROM ATLAS_PANDA.jobsDefined4 " % JobSpec.columnNames()
        sql2+= "WHERE PandaID=:PandaID"
        try:
            oldSubList = []
            # begin transaction
            self.conn.begin()
            # update
            varMap = {}
            varMap[':PandaID'] = pandaID
            varMap[':newJobStatus']  = 'defined'
            varMap[':oldJobStatus1'] = 'assigned'
            varMap[':oldJobStatus2'] = 'defined'
            self.cur.execute(sql1+comment,varMap)
            retU = self.cur.rowcount            
            # not found
            job = None
            if retU == 0:
                _logger.debug("resetDefinedJob : Not found %s" % pandaID)
            else:
                # select
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.arraysize = 10
                self.cur.execute(sql2+comment,varMap)
                res = self.cur.fetchone()
                # not found
                if res == None:
                    raise RuntimeError, 'Could not SELECT : PandaID=%s' % pandaID
                # instantiate Job
                job = JobSpec()
                job.pack(res)
                # do nothing for analysis jobs
                if job.prodSourceLabel in ['user','panda']:
                    _logger.debug('resetDefinedJob : rollback since PandaID=%s is analysis job' % pandaID)
                    # roll back
                    self._rollback()
                    return None
                job.dispatchDBlock = None
                if (not keepSite) and job.relocationFlag != 1:
                    # erase old assignment
                    job.computingSite = None
                job.computingElement = None
                # job parameters
                sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                self.cur.execute(sqlJobP+comment, varMap)
                for clobJobP, in self.cur:
                    job.jobParameters = clobJobP.read()
                    break
                # Files
                sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                sqlFile+= "WHERE PandaID=:PandaID"
                self.cur.arraysize = 10000
                self.cur.execute(sqlFile+comment, varMap)
                resFs = self.cur.fetchall()
                for resF in resFs:
                    file = FileSpec()
                    file.pack(resF)
                    # collect old subs
                    if job.prodSourceLabel in ['managed','test'] and file.type in ['output','log'] \
                           and re.search('_sub\d+$',file.destinationDBlock) != None:
                        if not file.destinationDBlock in oldSubList:
                            oldSubList.append(file.destinationDBlock)
                    # reset status, destinationDBlock and dispatchDBlock
                    file.status         ='unknown'
                    file.dispatchDBlock = None
                    file.destinationDBlock = re.sub('_sub\d+$','',file.destinationDBlock)
                    # add file
                    job.addFile(file)
                    # update files
                    sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                    varMap = file.valuesMap()
                    if varMap != {}:
                        varMap[':row_ID'] = file.row_ID
                        _logger.debug(sqlF+comment+str(varMap))                        
                        self.cur.execute(sqlF+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if getOldSubs:
                return job,oldSubList
            return job
        except:
            # error report
            type, value, traceBack = sys.exc_info()
            _logger.error("resetDefinedJobs : %s %s" % (type,value))
            # roll back
            self._rollback()
            return None


    # kill job
    def killJob(self,pandaID,user,code,prodManager,getUserInfo=False,wgProdRole=[]):
        # code
        # 2 : expire
        # 3 : aborted
        # 4 : expire in waiting
        # 7 : retry by server
        # 8 : rebrokerage
        # 9 : force kill
        comment = ' /* DBProxy.killJob */'        
        _logger.debug("killJob : code=%s PandaID=%s role=%s user=%s wg=%s" % (code,pandaID,prodManager,user,wgProdRole))
        # check PandaID
        try:
            long(pandaID)
        except:
            _logger.error("not an integer : %s" % pandaID)
            if getUserInfo:
                return False,{}                
            return False
        sql0  = "SELECT prodUserID,prodSourceLabel,jobDefinitionID,jobsetID,workingGroup FROM %s WHERE PandaID=:PandaID"        
        sql1  = "UPDATE %s SET commandToPilot=:commandToPilot,taskBufferErrorDiag=:taskBufferErrorDiag WHERE PandaID=:PandaID AND commandToPilot IS NULL"
        sql1F = "UPDATE %s SET commandToPilot=:commandToPilot,taskBufferErrorDiag=:taskBufferErrorDiag WHERE PandaID=:PandaID"
        sql2  = "SELECT %s " % JobSpec.columnNames()
        sql2 += "FROM %s WHERE PandaID=:PandaID AND jobStatus<>:jobStatus"
        sql3  = "DELETE FROM %s WHERE PandaID=:PandaID"
        sqlU  = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
        sql4  = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
        sql4 += JobSpec.bindValuesExpression()
        sqlF  = "UPDATE ATLAS_PANDA.filesTable4 SET status=:status WHERE PandaID=:PandaID AND type IN (:type1,:type2)"
        sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
        try:
            flagCommand = False
            flagKilled  = False
            userProdUserID      = ''
            userProdSourceLabel = ''
            userJobDefinitionID = ''
            userJobsetID        = ''
            # begin transaction
            self.conn.begin()
            for table in ('ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4'):
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # begin transaction
                self.conn.begin()
                # get DN if user is not production DN
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.arraysize = 10                    
                self.cur.execute((sql0+comment) % table, varMap)
                res = self.cur.fetchone()
                # not found
                if res == None:
                    continue
                # owner?
                def getCN(dn):
                    distinguishedName = ''
                    for line in dn.split('/'):
                        if line.startswith('CN='):
                            distinguishedName = re.sub('^CN=','',line)
                            distinguishedName = re.sub('\d+$','',distinguishedName)
                            distinguishedName = distinguishedName.strip()
                            break
                    if distinguishedName == '':
                        distinguishedName = dn
                    return distinguishedName
                # prevent prod proxy from killing analysis jobs
                userProdUserID,userProdSourceLabel,userJobDefinitionID,userJobsetID,workingGroup = res
                # check group prod role
                validGroupProdRole = False
                if res[1] in ['managed','test'] and workingGroup != '':
                    for tmpGroupProdRole in wgProdRole:
                        if tmpGroupProdRole == '':
                            continue
                        if re.search('(^|_)'+tmpGroupProdRole+'$',workingGroup,re.I) != None:
                            validGroupProdRole = True
                            break
                if prodManager:
                    if res[1] in ['user','panda'] and (not code in ['2','4','7','8','9']):
                        _logger.debug("ignore killJob -> prod proxy tried to kill analysis job type=%s" % res[1])
                        break
                    _logger.debug("killJob : %s using prod role" % pandaID)
                elif validGroupProdRole:
                    # WGs with prod role
                    _logger.debug("killJob : %s using group prod role for workingGroup=%s" % (pandaID,workingGroup))
                    pass
                else:   
                    cn1 = getCN(res[0])
                    cn2 = getCN(user)
                    _logger.debug("Owner:%s  - Requester:%s " % (cn1,cn2))
                    if cn1 != cn2:
                        _logger.debug("ignore killJob -> Owner != Requester")
                        break
                # update
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':commandToPilot'] = 'tobekilled'
                varMap[':taskBufferErrorDiag'] = 'killed by %s' % user
                if userProdSourceLabel in ['managed','test'] and code in ['9',]:
                    # ignore commandToPilot for force kill
                    self.cur.execute((sql1F+comment) % table, varMap)
                else:
                    self.cur.execute((sql1+comment) % table, varMap)
                retU = self.cur.rowcount
                if retU == 0:
                    continue
                # set flag
                flagCommand = True
                # select
                varMap = {}
                varMap[':PandaID'] = pandaID
                if userProdSourceLabel in ['managed','test'] and code in ['9',]:
                    # use dummy for force kill
                    varMap[':jobStatus'] = 'dummy'
                else:
                    varMap[':jobStatus'] = 'running'
                self.cur.arraysize = 10
                self.cur.execute((sql2+comment) % table, varMap)
                res = self.cur.fetchall()
                if len(res) == 0:
                    continue
                # instantiate JobSpec
                job = JobSpec()
                job.pack(res[0])
                # delete
                if table=='ATLAS_PANDA.jobsDefined4':
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    varMap[':oldJobStatus1'] = 'assigned'
                    varMap[':oldJobStatus2'] = 'defined'
                    self.cur.execute(sqlU+comment, varMap)
                else:
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    self.cur.execute((sql3+comment) % table, varMap)
                retD = self.cur.rowcount                                        
                if retD == 0:
                    continue
                # error code
                if job.jobStatus != 'failed':
                    # set status etc for non-failed jobs                    
                    job.endTime   = datetime.datetime.utcnow()
                    job.modificationTime = job.endTime
                    if code in ['2','4']:
                        # expire
                        if code == '2':
                            job.taskBufferErrorCode = ErrorCode.EC_Expire
                            job.taskBufferErrorDiag = 'expired after 7 days since submission'
                        else:
                            # waiting timeout 
                            job.taskBufferErrorCode = ErrorCode.EC_Expire
                            #job.taskBufferErrorCode = ErrorCode.EC_WaitTimeout
                            job.taskBufferErrorDiag = 'expired after waiting for input data for 2 days'
                    elif code=='3':
                        # aborted
                        job.taskBufferErrorCode = ErrorCode.EC_Aborted
                        job.taskBufferErrorDiag = 'aborted by ExtIF'
                    elif code=='8':
                        # reassigned by rebrokeage
                        job.taskBufferErrorCode = ErrorCode.EC_Reassigned
                        job.taskBufferErrorDiag = 'reassigned to another site by rebrokerage. new %s' % user
                        job.commandToPilot      = None
                    else:
                        # killed
                        job.taskBufferErrorCode = ErrorCode.EC_Kill
                        job.taskBufferErrorDiag = 'killed by %s' % user
                    # set job status    
                    job.jobStatus = 'cancelled'
                else:
                    # keep status for failed jobs
                    job.modificationTime = datetime.datetime.utcnow()
                    if code=='7':
                        # retried by server
                        job.taskBufferErrorCode = ErrorCode.EC_Retried
                        job.taskBufferErrorDiag = 'retrying at another site. new %s' % user                            
                        job.commandToPilot      = None
                job.stateChangeTime = job.modificationTime
                # insert
                self.cur.execute(sql4+comment, job.valuesMap())
                # update file
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':status'] = 'failed'
                varMap[':type1'] = 'output'
                varMap[':type2'] = 'log'
                self.cur.execute(sqlF+comment,varMap)
                # update files,metadata,parametes
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':modificationTime'] = job.modificationTime
                self.cur.execute(sqlFMod+comment,varMap)
                self.cur.execute(sqlMMod+comment,varMap)
                self.cur.execute(sqlPMod+comment,varMap)
                flagKilled = True
                break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("killJob : com=%s kill=%s " % (flagCommand,flagKilled))
            if getUserInfo:
                return (flagCommand or flagKilled),{'prodUserID':userProdUserID,
                                                    'prodSourceLabel':userProdSourceLabel,
                                                    'jobDefinitionID':userJobDefinitionID,
                                                    'jobsetID':userJobsetID}
            return (flagCommand or flagKilled)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("killJob : %s %s" % (type,value))
            # roll back
            self._rollback()
            if getUserInfo:
                return False,{}                
            return False
        

    # peek at job 
    def peekJob(self,pandaID,fromDefined,fromActive,fromArchived,fromWaiting,forAnal=False):
        comment = ' /* DBProxy.peekJob */'                        
        _logger.debug("peekJob : %s" % pandaID)
        # return None for NULL PandaID
        if pandaID in ['NULL','','None',None]:
            return None
        sql1_0 = "SELECT %s FROM %s "
        sql1_1 = "WHERE PandaID=:PandaID"
        nTry=3        
        for iTry in range(nTry):
            try:
                tables=[]
                if fromDefined:
                    tables.append('ATLAS_PANDA.jobsDefined4')
                if fromActive:
                    tables.append('ATLAS_PANDA.jobsActive4')
                if fromArchived:
                    tables.append('ATLAS_PANDA.jobsArchived4')
                if fromWaiting:
                    tables.append('ATLAS_PANDA.jobsWaiting4')
                if fromDefined:
                    # for jobs which are just reset
                    tables.append('ATLAS_PANDA.jobsDefined4')
                # select
                varMap = {}
                varMap[':PandaID'] = pandaID
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
                        sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                        sqlFile+= "WHERE PandaID=:PandaID"
                        self.cur.arraysize = 10000
                        self.cur.execute(sqlFile+comment, varMap)
                        resFs = self.cur.fetchall()
                        # metadata
                        resMeta = None
                        if table == 'ATLAS_PANDA.jobsArchived4' or forAnal:
                            # read metadata only for finished/failed production jobs
                            sqlMeta = "SELECT metaData FROM ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"
                            self.cur.execute(sqlMeta+comment, varMap)
                            for clobMeta, in self.cur:
                                if clobMeta != None:
                                    resMeta = clobMeta.read()
                                break
                        # job parameters
                        job.jobParameters = None
                        sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[':PandaID'] = job.PandaID
                        self.cur.execute(sqlJobP+comment, varMap)
                        for clobJobP, in self.cur:
                            if clobJobP != None:
                                job.jobParameters = clobJobP.read()
                            break
                        # commit
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        # set files
                        for resF in resFs:
                            file = FileSpec()
                            file.pack(resF)
                            job.addFile(file)
                        # set metadata
                        job.metadata = resMeta
                        return job
                _logger.debug("peekJob() : PandaID %s not found" % pandaID)
                return None
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("peekJob : %s retry : %s" % (pandaID,iTry))
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("peekJob : %s %s %s" % (pandaID,type,value))
                # return None for analysis
                if forAnal:
                    return None
                # return 'unknown'
                job = JobSpec()
                job.PandaID = pandaID
                job.jobStatus = 'unknown'
                return job


    # get PandaID with jobexeID
    def getPandaIDwithJobExeID(self,jobexeID):
        comment = ' /* DBProxy.getPandaIDwithJobExeID */'                        
        _logger.debug("getPandaIDwithJobExeID : %s" % jobexeID)
        failedRetVal = (None,None,'')
        # return for wrong jobexeID
        if jobexeID in ['NULL','','None',None]:
            return failedRetVal
        # SQL
        sql  = "SELECT PandaID,jobDefinitionID,jobName FROM ATLAS_PANDA.jobsWaiting4 "
        sql += "WHERE jobExecutionID=:jobexeID AND prodSourceLabel=:prodSourceLabel "
        sql += "AND jobStatus=:jobStatus "        
        varMap = {}
        varMap[':jobexeID'] = jobexeID
        varMap[':jobStatus'] = 'pending'
        varMap[':prodSourceLabel'] = 'managed'
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10                                        
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # not found
            if res == None:
                _logger.debug("getPandaIDwithJobExeID : jobexeID %s not found" % jobexeID)
                return failedRetVal
            _logger.debug("getPandaIDwithJobExeID : %s -> %s" % (jobexeID,str(res)))
            return res
        except:
            # roll back
            self._rollback()
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getPandaIDwithJobExeID : %s %s %s" % (jobexeID,errtype,errvalue))
            return failedRetVal


    # get express jobs
    def getExpressJobs(self,dn):
        comment = ' /* DBProxy.getExpressJobs */'                        
        _logger.debug("getExpressJobs : %s" % dn)
        sqlX  = "SELECT specialHandling,COUNT(*) FROM %s "
        sqlX += "WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLabel1 "
        sqlX += "AND specialHandling IS NOT NULL "
        sqlXJob  = "SELECT PandaID,jobStatus,prodSourceLabel,modificationTime,jobDefinitionID,jobsetID,startTime,endTime FROM %s "
        sqlXJob += "WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLabel1 "
        sqlXJob += "AND specialHandling IS NOT NULL AND specialHandling=:specialHandling "
        sqlQ  = sqlX
        sqlQ += "GROUP BY specialHandling "
        sqlQJob  = sqlXJob
        sqlA  = sqlX
        sqlA += "AND modificationTime>:modificationTime GROUP BY specialHandling "
        sqlAJob  = sqlXJob
        sqlAJob += "AND modificationTime>:modificationTime "
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            expressStr = 'express'
            activeExpressU = []            
            timeUsageU  = datetime.timedelta(0)
            executionTimeU = datetime.timedelta(hours=1)
            jobCreditU  = 3            
            timeCreditU = executionTimeU * jobCreditU
            timeNow   = datetime.datetime.utcnow()
            timeLimit = timeNow - datetime.timedelta(hours=6)
            # loop over tables
            for table in ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsArchived4']:
                varMap = {}
                varMap[':prodUserName'] = compactDN
                varMap[':prodSourceLabel1'] = 'user'
                if table == 'ATLAS_PANDA.jobsArchived4':                
                    varMap[':modificationTime'] = timeLimit
                    sql = sqlA % table
                    sqlJob = sqlAJob % table
                else:
                    sql = sqlQ % table
                    sqlJob = sqlQJob % table                    
                # start transaction
                self.conn.begin()
                # get the number of jobs for each specialHandling
                self.cur.arraysize = 10
                _logger.debug(sql+comment+str(varMap))
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchall()
                _logger.debug("getExpressJobs %s" % str(res))
                for specialHandling,countJobs in res:
                    if specialHandling == None:
                        continue
                    # look for express jobs
                    if expressStr in specialHandling:
                        varMap[':specialHandling'] = specialHandling
                        self.cur.arraysize = 1000
                        self.cur.execute(sqlJob+comment, varMap)
                        resJobs = self.cur.fetchall()
                        _logger.debug("getExpressJobs %s" % str(resJobs))
                        for tmp_PandaID,tmp_jobStatus,tmp_prodSourceLabel,tmp_modificationTime,\
                                tmp_jobDefinitionID,tmp_jobsetID,tmp_startTime,tmp_endTime \
                                in resJobs:
                            # collect active jobs
                            if not tmp_jobStatus in ['finished','failed','cancelled']:
                                activeExpressU.append((tmp_PandaID,tmp_jobsetID,tmp_jobDefinitionID))
                            # get time usage
                            if not tmp_jobStatus in ['defined','activated']:
                                # check only jobs which actually use or used CPU on WN
                                if tmp_startTime != None:
                                    # running or not
                                    if tmp_endTime == None:
                                        # job got started before/after the time limit
                                        if timeLimit > tmp_startTime:
                                            timeDelta = timeNow - timeLimit
                                        else:
                                            timeDelta = timeNow - tmp_startTime
                                    else:
                                        # job got started before/after the time limit
                                        if timeLimit > tmp_startTime:
                                            timeDelta = tmp_endTime - timeLimit
                                        else:
                                            timeDelta = tmp_endTime - tmp_startTime
                                    # add
                                    if timeDelta > datetime.timedelta(0):
                                        timeUsageU += timeDelta                                            
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # check quota
            rRet = True
            rRetStr = ''
            rQuota = 0            
            if len(activeExpressU) >= jobCreditU:
                rRetStr += "The number of queued runXYZ exceeds the limit = %s. " % jobCreditU
                rRet = False
            if timeUsageU >= timeCreditU:
                rRetStr += "The total execution time for runXYZ exceeds the limit = %s min. " % (timeCreditU.seconds / 60)
                rRet = False
            # calculate available quota
            if rRet:
                tmpQuota = jobCreditU - len(activeExpressU) - timeUsageU.seconds/executionTimeU.seconds
                if tmpQuota < 0:
                    rRetStr += "Quota for runXYZ exceeds. "
                    rRet = False
                else:
                    rQuota = tmpQuota
            # return        
            retVal = {'status':rRet,'quota':rQuota,'output':rRetStr,'usage':timeUsageU,'jobs':activeExpressU}
            _logger.debug("getExpressJobs : %s" % str(retVal))
            return retVal
        except:
            # roll back
            self._rollback()
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getExpressJobs : %s %s" % (errtype,errvalue))
            return None


    # get PandaID with destinationDBlock
    def getPandaIDwithDestDBlock(self,destinationDBlock):
        comment = ' /* DBProxy.getPandaIDwithDestDBlock */'                        
        _logger.debug("getPandaIDwithDestDBlock : %s" % destinationDBlock)
        try:
            sqlP  = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID FROM ATLAS_PANDA.filesTable4 tab "
            sqlP += "WHERE type IN (:type1,:type2) AND destinationDBlock=:destinationDBlock AND rownum<=1"
            # start transaction
            self.conn.begin()
            pandaID = None
            varMap = {}
            varMap[':type1'] = 'log'
            varMap[':type2'] = 'output'
            varMap[':destinationDBlock'] = destinationDBlock
            # select
            self.cur.arraysize = 10
            self.cur.execute(sqlP+comment, varMap)
            res = self.cur.fetchone()
            # append
            if res != None:
                pandaID, = res
            # commit to release tables
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            return pandaID
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getPandaIDwithDestDBlock : %s %s" % (errType,errValue))
            # return empty list
            return None
            

    # get number of activated/defined jobs with output datasets
    def getNumWaitingJobsWithOutDS(self,outputDSs):
        comment = ' /* DBProxy.getNumWaitingJobsWithOutDS */'                        
        _logger.debug("getNumWaitingJobsWithOutDS : %s" % str(outputDSs))
        try:
            sqlD  = "SELECT /*+ index(tab FILESTABLE4_DATASET_IDX) */ distinct destinationDBlock FROM ATLAS_PANDA.filesTable4 tab "
            sqlD += "WHERE type IN (:type1,:type2) AND dataset=:dataset AND status IN (:status1,:status2)"
            sqlP  = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID FROM ATLAS_PANDA.filesTable4 tab "
            sqlP += "WHERE type IN (:type1,:type2) AND destinationDBlock=:destinationDBlock AND status IN (:status1,:status2) AND rownum<=1"
            sqlJ  = "SELECT jobDefinitionID,taskID,prodUserName,jobStatus,prodSourceLabel FROM %s "
            sqlJ += "WHERE PandaID=:PandaID"
            sqlC  = "SELECT count(*) FROM ATLAS_PANDA.jobsActive4 "
            sqlC += "WHERE jobDefinitionID=:jobDefinitionID AND prodUserName=:prodUserName AND jobStatus IN (:jobStatus1)"
            # start transaction
            self.conn.begin()
            # get sub datasets
            subDSList = []
            for outputDS in outputDSs:
                varMap = {}
                varMap[':type1'] = 'log'
                varMap[':type2'] = 'output'                
                varMap[':status1'] = 'unknown'
                varMap[':status2'] = 'pending'
                varMap[':dataset'] = outputDS
                # select
                self.cur.arraysize = 1000
                self.cur.execute(sqlD+comment, varMap)
                resList = self.cur.fetchall()
                # append
                for destinationDBlock, in resList:
                    subDSList.append(destinationDBlock)
            # get PandaIDs
            pandaIDs = []
            for subDS in subDSList:
                varMap = {}
                varMap[':type1'] = 'log'
                varMap[':type2'] = 'output'
                varMap[':status1'] = 'unknown'
                varMap[':status2'] = 'pending'
                varMap[':destinationDBlock'] = subDS
                # select
                self.cur.arraysize = 10
                self.cur.execute(sqlP+comment, varMap)
                res = self.cur.fetchone()
                # append
                if res != None:
                    pandaID, = res
                    pandaIDs.append(pandaID)
            # commit to release tables
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all PandaIDs
            jobInfos = []
            for pandaID in pandaIDs:
                varMap = {}
                varMap[':PandaID'] = pandaID
                # start transaction        
                self.conn.begin()
                # get jobID,nJobs,jobStatus,userName
                res = None
                for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']:
                    # select
                    self.cur.arraysize = 10
                    self.cur.execute((sqlJ % table)+comment,varMap)
                    res = self.cur.fetchone()
                    if res != None:
                        break
                # commit to release tables
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # not found
                if res == None:
                    continue
                # append
                jobInfos.append(res)
            # no jobs
            if jobInfos == []:
                _logger.error("getNumWaitingJobsWithOutDS : no jobs found")
                return False,{}
            # loop over all jobIDs
            retMap = {}
            for jobID,taskID,prodUserName,jobStatus,prodSourceLabel in jobInfos:
                if retMap.has_key(jobID):
                    continue
                retMap[jobID] = {}
                retMap[jobID]['nJobs'] = taskID
                retMap[jobID]['sourceLabel'] = prodSourceLabel
                # don't check # of activated
                if jobStatus in ['defined']:
                    retMap[jobID]['activated'] = False
                    retMap[jobID]['nActs'] = 0
                    continue
                retMap[jobID]['activated'] = True
                # get # of activated jobs
                varMap = {}
                varMap[':prodUserName'] = prodUserName
                varMap[':jobDefinitionID'] = jobID
                varMap[':jobStatus1'] = 'activated'
                # start transaction        
                self.conn.begin()
                # select
                self.cur.arraysize = 10
                self.cur.execute(sqlC+comment, varMap)
                res = self.cur.fetchone()
                # commit to release tables
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                if res == None:
                    _logger.error("getNumWaitingJobsWithOutDS : cannot get # of activated for %s:%s" % \
                                  (jobID,prodUserName))
                    return False,{}
                # set # of activated
                nActs, = res
                retMap[jobID]['nActs'] = nActs
            # return    
            _logger.debug("getNumWaitingJobsWithOutDS -> %s" % str(retMap))
            return True,retMap
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getNumWaitingJobsWithOutDS : %s %s" % (errType,errValue))
            # return empty list
            return False,{}


    # get slimmed file info with PandaIDs
    def getSlimmedFileInfoPandaIDs(self,pandaIDs):
        comment = ' /* DBProxy.getSlimmedFileInfoPandaIDs */'                        
        _logger.debug("getSlimmedFileInfoPandaIDs : %s len=%s" % (pandaIDs[0],len(pandaIDs)))
        try:
            sqlL  = "SELECT lfn,type,dataset FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID"
            sqlA  = "SELECT /*+ INDEX(tab FILES_ARCH_PANDAID_IDX)*/ lfn,type,dataset FROM ATLAS_PANDAARCH.filesTable_ARCH tab "
            sqlA += "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-60)"
            retMap = {'inDS':[],'outDS':[]}
            # start transaction
            self.conn.begin()
            # select
            for pandaID in pandaIDs:
                # make sql
                varMap = {}
                varMap[':PandaID'] = pandaID
                # select
                self.cur.arraysize = 10000                
                self.cur.execute(sqlL+comment, varMap)
                resList = self.cur.fetchall()
                # try archived if not found in filesTable4
                if len(resList) == 0:
                    self.cur.execute(sqlA+comment, varMap)
                    resList = self.cur.fetchall()
                # append
                for tmp_lfn,tmp_type,tmp_dataset in resList:
                    # skip lib.tgz
                    if tmp_lfn.endswith('.lib.tgz'):
                        continue
                    if tmp_type == 'input':
                        if not tmp_dataset in retMap['inDS']:
                            retMap['inDS'].append(tmp_dataset)
                    elif tmp_type == 'output':
                        if not tmp_dataset in retMap['outDS']:
                            retMap['outDS'].append(tmp_dataset)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("getSlimmedFileInfoPandaIDs : %s" % str(retMap))
            return retMap
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getSlimmedFileInfoPandaIDs : %s %s" % (type,value))
            # return empty list
            return {}
            
        
    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,dn,timeRange,retJobIDs):
        comment = ' /* DBProxy.getJobIDsInTimeRange */'                        
        _logger.debug("getJobIDsInTimeRange : %s %s" % (dn,timeRange.strftime('%Y-%m-%d %H:%M:%S')))
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            tables = ['ATLAS_PANDA.jobsArchived4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']
            # select
            for table in tables:
                # make sql
                if table == 'ATLAS_PANDA.jobsArchived4':
                    sql  = 'SELECT /*+ INDEX_RS_ASC(TAB("JOBSARCHIVED4"."PRODUSERNAME")) NO_INDEX(TAB("JOBSARCHIVED4"."MODIFICATIONTIME")) */ jobDefinitionID FROM %s tab ' % table
                elif table == 'ATLAS_PANDA.jobsActive4':
                    sql  = 'SELECT /*+ INDEX_RS_ASC(TAB("JOBSACTIVE4"."PRODUSERNAME")) NO_INDEX(TAB("JOBSACTIVE4"."MODIFICATIONTIME")) */ jobDefinitionID FROM %s tab ' % table
                else:
                    sql  = "SELECT jobDefinitionID FROM %s " % table                    
                sql += "WHERE prodUserName=:prodUserName AND modificationTime>:modificationTime "
                sql += "AND prodSourceLabel=:prodSourceLabel GROUP BY jobDefinitionID"
                varMap = {}
                varMap[':prodUserName'] = compactDN
                varMap[':prodSourceLabel']  = 'user'
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
            return []


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,idStatus,nJobs):
        comment = ' /* DBProxy.getPandIDsWithJobID */'                        
        _logger.debug("getPandIDsWithJobID : %s %s" % (dn,jobID))
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            tables = ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsArchived4']
            buildJobID = None
            # select
            for table in tables:
                # skip if all jobs have already been gotten
                if nJobs > 0 and len(idStatus) >= nJobs:
                    continue
                # make sql
                sql  = "SELECT PandaID,jobStatus,commandToPilot,prodSourceLabel,taskBufferErrorCode FROM %s " % table                
                sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
                sql += "AND prodSourceLabel in (:prodSourceLabel1,:prodSourceLabel2)"
                varMap = {}
                varMap[':prodUserName'] = compactDN
                varMap[':jobDefinitionID']  = jobID
                varMap[':prodSourceLabel1'] = 'user'
                varMap[':prodSourceLabel2'] = 'panda'
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                # select
                _logger.debug(sql+comment+str(varMap))
                self.cur.execute(sql+comment, varMap)
                resList = self.cur.fetchall()
                # append
                for tmpID,tmpStatus,tmpCommand,tmpProdSourceLabel,tmpTaskBufferErrorCode in resList:
                    # ignore jobs retried by pilot since they have new PandaIDs with the same jobsetID/jobdefID
                    if tmpTaskBufferErrorCode in [ErrorCode.EC_PilotRetried]:
                        continue
                    # ignore old buildJob which was replaced by rebrokerage 
                    if tmpProdSourceLabel == 'panda':
                        if buildJobID == None:
                            # first buildJob
                            buildJobID = tmpID
                        elif buildJobID >= tmpID:
                            # don't append old one
                            continue
                        else:
                            # delete old one
                            del idStatus[buildJobID]
                            buildJobID = tmpID
                    # append        
                    idStatus[tmpID] = (tmpStatus,tmpCommand)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            _logger.debug("getPandIDsWithJobID : %s" % str(idStatus))
            return idStatus,buildJobID
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getPandIDsWithJobID : %s %s" % (type,value))
            # return empty list
            return {},None


    # lock jobs for reassign
    def lockJobsForReassign(self,tableName,timeLimit,statList,labels,processTypes,sites,clouds):
        comment = ' /* DBProxy.lockJobsForReassign */'                        
        _logger.debug("lockJobsForReassign : %s %s %s %s %s %s %s" % \
                      (tableName,timeLimit,statList,labels,processTypes,sites,clouds))
        try:
            # make sql
            sql  = "SELECT PandaID FROM %s " % tableName
            sql += "WHERE modificationTime<:modificationTime "
            varMap = {}
            varMap[':modificationTime'] = timeLimit
            if statList != []:
                sql += 'AND jobStatus IN ('
                tmpIdx = 0
                for tmpStat in statList:
                    tmpKey = ':stat%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                sql = sql[:-1]
                sql += ') '
            if labels != []:
                sql += 'AND prodSourceLabel IN ('
                tmpIdx = 0
                for tmpStat in labels:
                    tmpKey = ':label%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                sql = sql[:-1]
                sql += ') '
            if processTypes != []:
                sql += 'AND processingType IN ('
                tmpIdx = 0
                for tmpStat in processTypes:
                    tmpKey = ':processType%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                sql = sql[:-1]
                sql += ') '
            if sites != []:
                sql += 'AND computingSite IN ('
                tmpIdx = 0
                for tmpStat in sites:
                    tmpKey = ':site%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                sql = sql[:-1]
                sql += ') '
            if clouds != []:
                sql += 'AND cloud IN ('
                tmpIdx = 0
                for tmpStat in clouds:
                    tmpKey = ':cloud%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                sql = sql[:-1]
                sql += ') '
            # sql for lock
            sqlLock = 'UPDATE %s SET modificationTime=CURRENT_DATE WHERE PandaID=:PandaID' % tableName
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000000
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            retList = []
            # lock
            for tmpID, in resList:
                varLock = {':PandaID':tmpID}
                self.cur.execute(sqlLock+comment,varLock)
                retList.append((tmpID,))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # sort
            retList.sort()
            _logger.debug("lockJobsForReassign : %s" % (len(retList)))
            return True,retList
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("lockJobsForReassign : %s %s" % (errType,errValue))
            # return empty
            return False,[]


    # get the number of waiting jobs with a dataset
    def getNumWaitingJobsForPD2P(self,datasetName):
        comment = ' /* DBProxy.getNumWaitingJobsForPD2P */'                        
        _logger.debug("getNumWaitingJobsForPD2P : %s" % datasetName)
        try:
            tables = ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4']
            nJobs = 0
            # select
            for table in tables:
                # make sql
                sql  = "SELECT COUNT(*) FROM %s " % table                
                sql += "WHERE prodDBlock=:prodDBlock AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
                sql += "AND jobStatus IN (:jobStatus1,:jobStatus2) "
                varMap = {}
                varMap[':prodDBlock'] = datasetName
                varMap[':jobStatus1'] = 'defined'
                varMap[':jobStatus2'] = 'activated'                
                varMap[':prodSourceLabel1'] = 'user'
                varMap[':prodSourceLabel2'] = 'panda'
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchone()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                if res != None: 
                    tmpN, = res
                    nJobs += tmpN
            _logger.debug("getNumWaitingJobsForPD2P : %s -> %s" % (datasetName,nJobs))
            return nJobs
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getNumWaitingJobsForPD2P : %s %s" % (errType,errValue))
            # return 0
            return 0


    # get the number of waiting jobsets with a dataset
    def getNumWaitingJobsetsForPD2P(self,datasetName):
        comment = ' /* DBProxy.getNumWaitingJobsetsForPD2P */'                        
        _logger.debug("getNumWaitingJobsetsForPD2P : %s" % datasetName)
        try:
            tables = ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4']
            jobsetIDuserList = []
            # select
            for table in tables:
                # make sql
                sql  = "SELECT jobsetID,prodUserName FROM %s " % table                
                sql += "WHERE prodDBlock=:prodDBlock AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
                sql += "AND jobStatus IN (:jobStatus1,:jobStatus2) GROUP BY jobsetID,prodUserName"
                varMap = {}
                varMap[':prodDBlock'] = datasetName
                varMap[':jobStatus1'] = 'defined'
                varMap[':jobStatus2'] = 'activated'                
                varMap[':prodSourceLabel1'] = 'user'
                varMap[':prodSourceLabel2'] = 'panda'
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                self.cur.execute(sql+comment, varMap)
                resList = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                for jobsetID,prodUserName in resList:
                    tmpKey = (jobsetID,prodUserName)
                    if not tmpKey in jobsetIDuserList:
                        jobsetIDuserList.append(tmpKey)
            _logger.debug("getNumWaitingJobsetsForPD2P : %s -> %s" % (datasetName,len(jobsetIDuserList)))
            return len(jobsetIDuserList)
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getNumWaitingJobsetsForPD2P : %s %s" % (errType,errValue))
            # return 0
            return 0


    # lock job for re-brokerage
    def lockJobForReBrokerage(self,dn,jobID,simulation,forceOpt,forFailed=False):
        comment = ' /* lockJobForReBrokerage */'                        
        _logger.debug("lockJobForReBrokerage : %s %s %s %s %s" % (dn,jobID,simulation,forceOpt,forFailed))
        try:
            errMsg = ''
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            # start transaction
            self.conn.begin()
            buildJobPandaID   = None
            buildJobStatus    = None
            buildJobDefID     = None
            buildCreationTime = None
            runPandaID        = None
            minPandaIDlibDS   = None
            maxPandaIDlibDS   = None
            # get one runXYZ job
            if errMsg == '':
                for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']:
                    sql  = "SELECT PandaID FROM %s " % table
                    sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
                    sql += "AND prodSourceLabel=:prodSourceLabel1 AND jobStatus IN (:jobStatus1,:jobStatus2) "
                    sql += "AND rownum <= 1"
                    varMap = {}
                    varMap[':prodUserName'] = compactDN
                    varMap[':jobDefinitionID']  = jobID
                    varMap[':prodSourceLabel1'] = 'user'
                    if not forFailed:
                        # lock active jobs for normal rebrokerage
                        varMap[':jobStatus1'] = 'defined'
                        varMap[':jobStatus2'] = 'activated'
                    else:
                        # lock failed jobs for retry
                        varMap[':jobStatus1'] = 'failed'
                        varMap[':jobStatus2'] = 'dummy'
                    # select
                    self.cur.execute(sql+comment, varMap)
                    res = self.cur.fetchone()
                    # not found
                    if res != None:
                        runPandaID, = res
                        break
                if runPandaID == None:
                    if not forFailed:
                        errMsg = "could not get runXYZ jobs in defined/activated state"
                    else:
                        errMsg = "could not get failed runXYZ jobs"                        
            # get libDS
            libDS = ''
            if errMsg == '':
                sql = "SELECT lfn,dataset FROM ATLAS_PANDA.filesTable4 WHERE type=:type AND PandaID=:PandaID"
                varMap = {}                
                varMap[':type']    = 'input'
                varMap[':PandaID'] = runPandaID
                # select
                self.cur.arraysize = 10000
                self.cur.execute(sql+comment, varMap)
                resList = self.cur.fetchall()
                for tmpLFN,tmpDS in resList:
                    if tmpLFN.endswith('.lib.tgz'):
                        libDS = tmpDS
                        break
            # check status of corresponding buildJob
            if libDS != '':
                sql  = "SELECT /*+ index(tab FILESTABLE4_DATASET_IDX) */ PandaID FROM ATLAS_PANDA.filesTable4 tab "
                sql += "WHERE type=:type AND dataset=:dataset"
                varMap = {}                
                varMap[':type']    = 'output'
                varMap[':dataset'] = libDS
                # select
                self.cur.arraysize = 10
                # select
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchone()
                # not found in active table
                if res == None:
                    # look for buildJob in archived table
                    sql  = "SELECT /*+ NO_INDEX(tab JOBS_MODTIME_IDX) INDEX_COMBINE(tab JOBS_PRODSOURCELABEL_IDX JOBS_PRODUSERNAME_IDX) */ "
                    sql += "PandaID,jobStatus,jobDefinitionID,creationTime "
                    sql += "FROM ATLAS_PANDAARCH.jobsArchived tab "
                    sql += "WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLable1 "
                    sql += "AND modificationTime>(CURRENT_DATE-10) ORDER BY PandaID DESC"
                    varMap = {}
                    varMap[':prodUserName'] = compactDN
                    varMap[':prodSourceLable1'] = 'panda'
                    # select
                    self.cur.arraysize = 10000
                    self.cur.execute(sql+comment, varMap)
                    resList = self.cur.fetchall()
                    # loop over PandaIDs to find corresponding libDS
                    sql  = "SELECT /*+ INDEX(tab FILES_ARCH_PANDAID_IDX)*/ PandaID FROM ATLAS_PANDAARCH.filesTable_ARCH tab "
                    sql += "WHERE PandaID=:PandaID AND type=:type AND dataset=:dataset AND status=:status "
                    sql += "AND modificationTime>(CURRENT_DATE-10)"
                    self.cur.arraysize = 10
                    for tmpID,tmpJobStatus,tmpJobDefID,tmpCreationTime in resList:
                        varMap = {}
                        varMap[':PandaID'] = tmpID
                        varMap[':type']    = 'output'
                        varMap[':status']  = 'ready'                        
                        varMap[':dataset'] = libDS
                        # select
                        self.cur.execute(sql+comment, varMap)
                        res = self.cur.fetchone()
                        if res != None:
                            # get PandaID of buildJob
                            buildJobPandaID, = res
                            buildJobStatus = tmpJobStatus
                            buildJobDefID  = tmpJobDefID
                            buildCreationTime = tmpCreationTime
                            break
                    # not found
                    if buildJobPandaID == None:
                        errMsg = "could not find successful buildJob for %s" % libDS
                else:
                    # get PandaID of buildJob
                    buildJobPandaID, = res
                # found buildJob
                if errMsg == '':
                    # get current buildJob status
                    if buildJobStatus == None:
                        for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsArchived4','ATLAS_PANDA.jobsDefined4']:
                            # make sql
                            sql  = "SELECT jobStatus,jobDefinitionID,creationTime FROM %s " % table
                            sql += "WHERE PandaID=:PandaID "
                            varMap = {}
                            varMap[':PandaID'] = buildJobPandaID
                            # select
                            self.cur.execute(sql+comment, varMap)
                            res = self.cur.fetchone()
                            # found
                            if res != None:
                                buildJobStatus,buildJobDefID,buildCreationTime = res
                                break
                        # not found
                        if buildJobStatus == None:
                            errMsg = "could not find buildJob=%s in database" % buildJobPandaID
                    # check status
                    if errMsg != '':
                        if not buildJobStatus in ['defined','activated','finished','cancelled']:
                            errMsg = "status of buildJob is '%s' != defined/activated/finished/cancelled which cannot be reassigned" \
                                     % buildJobStatus
            # get max/min PandaIDs using the libDS
            if errMsg == '':
                sql = "SELECT /*+ index(tab FILESTABLE4_DATASET_IDX) */ MAX(PandaID),MIN(PandaID) FROM ATLAS_PANDA.filesTable4 tab "
                sql += "WHERE type=:type AND dataset=:dataset"
                varMap = {}
                varMap[':type']    = 'input'
                varMap[':dataset'] = libDS
                self.cur.arraysize = 10
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchone()
                if res == None:
                    errMsg = "cannot get MAX/MIN PandaID for multiple usage for %s" % libDS
                else:
                    maxPandaIDlibDS,minPandaIDlibDS = res
            # check creationDate of buildJob
            if errMsg == '':
                # buildJob has already finished
                timeLimit = datetime.datetime.utcnow()-datetime.timedelta(days=6)
                if buildJobStatus in ['finished','cancelled'] and buildCreationTime < timeLimit:
                    errMsg = "corresponding buildJob %s is too old %s" % (buildJobPandaID,buildCreationTime.strftime('%Y-%m-%d %H:%M:%S'))
            # check modificationTime
            if errMsg == '':
                # make sql
                tables = ['ATLAS_PANDA.jobsDefined4']
                if not buildJobStatus in ['defined']:
                    tables.append('ATLAS_PANDA.jobsActive4')
                sql  = "SELECT modificationTime FROM %s "
                sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
                sql += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND jobStatus IN (:jobStatus1,:jobStatus2) "
                sql += "AND rownum <=1"
                varMap = {}
                varMap[':prodUserName'] = compactDN
                varMap[':jobDefinitionID']  = jobID
                varMap[':prodSourceLabel1'] = 'user'
                varMap[':prodSourceLabel2'] = 'panda'
                if not forFailed:
                    # normal rebrokerage
                    varMap[':jobStatus1'] = 'defined'
                    varMap[':jobStatus2'] = 'activated'
                else:
                    # retry
                    varMap[':jobStatus1'] = 'failed'
                    varMap[':jobStatus2'] = 'dummy'
                for tableName in tables:
                    # select
                    self.cur.execute((sql % tableName)+comment, varMap)
                    res = self.cur.fetchone()
                    if res != None:
                        break
                if res == None:
                    if not forFailed:
                        errMsg = "no defined/activated jobs to be reassigned"
                    else:
                        errMsg = "no failed jobs to be retried"                        
                else:
                    tmpModificationTime, = res
                    # prevent users from rebrokering more than once in one hour
                    timeLimit = datetime.datetime.utcnow()-datetime.timedelta(hours=1)
                    if simulation:
                        pass
                    elif timeLimit < tmpModificationTime and not forceOpt:
                        errMsg = "last mod time is %s > current-1hour. Cannot run (re)brokerage more than once in one hour" \
                                 % tmpModificationTime.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        # update modificationTime for locking
                        if buildJobStatus in ['defined']:
                            sql = 'UPDATE ATLAS_PANDA.jobsActive4 '
                        else:
                            sql = 'UPDATE ATLAS_PANDA.jobsActive4 '
                        sql += 'SET modificationTime=CURRENT_DATE '
                        sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
                        sql += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND jobStatus IN (:jobStatus1,:jobStatus2) "
                        self.cur.execute(sql+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return failure
            if errMsg != '':
                _logger.debug('lockJobForReBrokerage : '+errMsg)
                return False,{'err':errMsg}
            # return
            retMap = {'bPandaID':buildJobPandaID,'bStatus':buildJobStatus,'userName':compactDN,
                      'bJobID':buildJobDefID,'rPandaID':runPandaID,
                      'maxPandaIDlibDS':maxPandaIDlibDS,'minPandaIDlibDS':minPandaIDlibDS}
            _logger.debug("lockJobForReBrokerage %s" % str(retMap))
            return True,retMap
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("lockJobForReBrokerage : %s %s" % (type,value))
            # return empty list
            return False,{'err':'database error'}


    # get input datasets for rebrokerage
    def getInDatasetsForReBrokerage(self,jobID,userName):
        comment = ' /* DBProxy.getInDatasetsForReBrokerage */'        
        failedRet = False,{},None
        try:
            _logger.debug("getInDatasetsForReBrokerage(%s,%s)" % (jobID,userName))
            # start transaction
            self.conn.begin()
            # get pandaID
            pandaIDs = []
            maxTotalFileSize = None
            for table in ['jobsActive4','jobsDefined4']:
                sql  = "SELECT PandaID FROM ATLAS_PANDA.%s WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID " % table
                sql += "AND prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2)"
                varMap = {}
                varMap[':prodUserName'] = userName
                varMap[':jobDefinitionID']  = jobID
                varMap[':prodSourceLabel'] = 'user'
                varMap[':jobStatus1'] = 'defined'
                varMap[':jobStatus2'] = 'activated'
                self.cur.arraysize = 10000
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchall()
                if res != []:
                    for tmpItem in res:
                        pandaIDs.append(tmpItem[0])
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # not found    
            if pandaIDs == []:
                _logger.debug("getInDatasetsForReBrokerage : PandaIDs not found")
                return failedRet
            # get dataset and lfn
            retMapLFN = {}
            sql  = "SELECT dataset,lfn,fsize FROM ATLAS_PANDA.filesTable4 "
            sql += "WHERE PandaID=:PandaID AND type=:type"
            for pandaID in pandaIDs:
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':type'] = 'input'                        
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 10000
                self.cur.execute(sql+comment, varMap)
                resL = self.cur.fetchall()
                # append
                tmpTotalFileSize = 0
                for tmpDataset,tmpLFN,tmpFileSize in resL:
                    # ignore lib.tgz
                    if tmpLFN.endswith('.lib.tgz'):
                        continue
                    if not retMapLFN.has_key(tmpDataset):
                        retMapLFN[tmpDataset] = []
                    if not tmpLFN in retMapLFN[tmpDataset]:
                        retMapLFN[tmpDataset].append(tmpLFN)
                    try:
                        tmpTotalFileSize += long(tmpFileSize)
                    except:
                        pass
                if maxTotalFileSize == None or maxTotalFileSize < tmpTotalFileSize:
                    maxTotalFileSize = tmpTotalFileSize
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            _logger.debug("getInDatasetsForReBrokerage : done")
            # max size in MB
            maxTotalFileSize /= (1024*1024)
            # return
            return True,retMapLFN,maxTotalFileSize
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getInDatasetsForReBrokerage(%s,%s) : %s %s" % (jobID,userName,errType,errValue))
            return failedRet


    # move jobs to jobsDefine4 for re-brokerage
    def resetBuildJobForReBrokerage(self,pandaID):
        comment = ' /* resetBuildJobForReBrokerage */'                        
        _logger.debug("resetBuildJobForReBrokerage : start %s" % pandaID)
        try:
            # make sql to move jobs
            sql1 = "SELECT %s FROM ATLAS_PANDA.jobsActive4 " % JobSpec.columnNames()
            sql1+= "WHERE PandaID=:PandaID AND jobStatus=:jobStatus1"
            sql3 = "INSERT INTO ATLAS_PANDA.jobsDefined4 (%s) " % JobSpec.columnNames()
            sql3+= JobSpec.bindValuesExpression()
            # start transaction
            self.conn.begin()
            # select
            varMap = {}
            varMap[':PandaID'] = pandaID
            varMap[':jobStatus1'] = 'activated'
            self.cur.arraysize = 10
            self.cur.execute(sql1+comment,varMap)
            res = self.cur.fetchone()
            # not found
            if res == None:
                _logger.error("resetBuildJobForReBrokerage : PandaID=%s not found" % pandaID)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                return False
            # instantiate Job
            job = JobSpec()
            job.pack(res)
            # delete from jobsDefined4 just in case
            varMap = {}
            varMap[':PandaID'] = pandaID
            sqlD = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
            self.cur.execute(sqlD+comment,varMap)
            # reset job status
            job.jobStatus = 'defined'
            # host and time information
            job.modificationHost = self.hostname
            job.modificationTime = datetime.datetime.utcnow()
            # insert to Defined
            self.cur.execute(sql3+comment, job.valuesMap())
            # delete from Active
            varMap = {}
            varMap[':PandaID'] = pandaID
            varMap[':jobStatus1'] = 'activated'
            sql2 = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID AND jobStatus=:jobStatus1"
            self.cur.execute(sql2+comment,varMap)
            retD = self.cur.rowcount
            # delete failed
            if retD != 1:
                _logger.error("resetBuildJobForReBrokerage : failed to delete PandaID=%s" % pandaID)                    
                # rollback
                self._rollback()
                return False
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            _logger.debug("resetBuildJobForReBrokerage : end %s" % pandaID)
            return True
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("resetBuildJobForReBrokerage : %s %s" % (type,value))
            # return empty list
            return False

        
    # get PandaIDs using userName/jobID for re-brokerage or retry
    def getPandaIDsForReBrokerage(self,userName,jobID,fromActive,forFailed=False):
        comment = ' /* DBProxy.getPandaIDsForReBrokerage */'                        
        _logger.debug("getPandaIDsForReBrokerage : %s %s %s %s" % (userName,jobID,fromActive,forFailed))
        try:
            returnList = []
            varMap = {}
            varMap[':prodUserName'] = userName
            varMap[':jobDefinitionID'] = jobID
            if not forFailed:
                varMap[':jobStatus1'] = 'activated'
            else:
                varMap[':jobStatus1'] = 'failed'
            sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
            sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
            sql += "AND jobStatus=:jobStatus1"
            # get IDs from Active table
            if fromActive:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 20000
                self.cur.execute(sql+comment,varMap)
                resList = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # append
                for tmpID, in resList:
                    if not tmpID in returnList:
                        returnList.append(tmpID)
                # set holding to prevent activated jobs from being picked up
                if not forFailed:
                    sql  = 'UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:newStatus '
                    sql += 'WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID '
                    sql += "AND jobStatus=:jobStatus1"
                    varMap[':newStatus'] = 'holding'
                    # start transaction
                    self.conn.begin()
                    # update
                    self.cur.execute(sql+comment,varMap)                
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
            # get IDs from Defined table just in case
            varMap = {}
            varMap[':prodUserName'] = userName
            varMap[':jobDefinitionID'] = jobID
            varMap[':jobStatus1'] = 'defined'
            varMap[':jobStatus2'] = 'assgined'
            sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
            sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
            sql += "AND jobStatus IN (:jobStatus1,:jobStatus2)"
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 20000
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # append
            for tmpID, in resList:
                if not tmpID in returnList:
                    returnList.append(tmpID)
            # sort
            returnList.sort()
            # return
            return returnList
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getPandaIDsForReBrokerage : %s %s" % (type,value))
            # return empty list
            return []


    # get outDSs with userName/jobID
    def getOutDSsForReBrokerage(self,userName,jobID):
        comment = ' /* DBProxy.getOutDSsForReBrokerage */'                        
        _logger.debug("getOutDSsForReBrokerage : %s %s" % (userName,jobID))
        falseRet = (False,[],None,None)
        try:
            # get one PandaID
            sql  = "SELECT PandaID,computingSite,destinationSE FROM ATLAS_PANDA.jobsActive4 "
            sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
            sql += "AND prodSourceLabel=:prodSourceLabel AND rownum<=1"
            varMap = {}
            varMap[':prodUserName'] = userName
            varMap[':jobDefinitionID'] = jobID
            varMap[':prodSourceLabel'] = 'user'
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchone()
            # not found
            if res == None:
                _logger.debug("getOutDSsForReBrokerage : failed to get PandaID")
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return falseRet
            pandaID,computingSite,destinationSE = res
            # get outDSs
            sql = "SELECT dataset FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type IN (:type1,:type2)"
            varMap = {}
            varMap[':type1'] = 'output'
            varMap[':type2'] = 'log'            
            varMap[':PandaID'] = pandaID
            self.cur.arraysize = 1000
            self.cur.execute(sql+comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # append
            returnList = []
            for tmpOutDS, in resList:
                if not tmpOutDS in returnList:
                    returnList.append(tmpOutDS)
            # return
            return True,returnList,computingSite,destinationSE
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getOutDSsForReBrokerage : %s %s" % (type,value))
            # return empty list
            return falseRet

        
    # query PandaID
    def queryPandaID(self,jobDefID):
        comment = ' /* DBProxy.queryPandaID */'                
        _logger.debug("queryPandaID : %s" % jobDefID)
        sql0 = "SELECT PandaID,attemptNr FROM %s WHERE attemptNr=("
        sql0+= "SELECT MAX(attemptNr) FROM %s"
        sql1= " WHERE prodSourceLabel=:prodSourceLabel AND jobDefinitionID=:jobDefinitionID)"
        sql1+=" AND prodSourceLabel=:prodSourceLabel AND jobDefinitionID=:jobDefinitionID"
        try:
            ids = []
            # select
            varMap = {}
            varMap[':jobDefinitionID'] = jobDefID
            varMap[':prodSourceLabel'] = 'managed'
            for table in ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsArchived4','ATLAS_PANDA.jobsWaiting4']:
                # start transaction
                self.conn.begin()
                # select
                sql = sql0 % (table,table) + sql1
                self.cur.arraysize = 10
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchall()
                ids += list(res)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # look for the latest attempt
            preAtt =-1
            pandaID=None
            for pID,att in ids:
                if att > preAtt:
                    pandaID = pID
                    preAtt = att
                if att == preAtt:
                    if pandaID < pID:
                        pandaID = pID
            return pandaID
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("queryPandaID : %s %s" % (type,value))
            # roll back
            self._rollback()
            return None


    # query job info per cloud
    def queryJobInfoPerCloud(self,cloud,schedulerID=None):
        comment = ' /* DBProxy.queryJobInfoPerCloud */'                
        _logger.debug("queryJobInfoPerCloud : %s %s" % (cloud,schedulerID))
        attrs = ['PandaID','jobStatus','jobName']
        sql0 = "SELECT "
        for attr in attrs:
            sql0 += "%s," % attr
        sql0 = "%s " % sql0[:-1]
        sql0+= "FROM %s "
        sql0+= "WHERE cloud=:cloud "
        varMap = {}
        varMap[':cloud'] = cloud
        if schedulerID != None:
            sql0+= "AND schedulerID=:schedulerID "
            varMap[':schedulerID'] = schedulerID            
        try:
            ids = []
            returnList = []
            # select
            for table in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']:
                # start transaction
                self.conn.begin()
                # select
                sql = sql0 % table
                self.cur.arraysize = 10000
                self.cur.execute(sql+comment,varMap)
                resList = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # loop over all
                for res in resList:
                    valMap = {}
                    # skip if already in the list
                    PandaID = res[0]
                    if PandaID in ids:
                        continue
                    # convert to map
                    for idx,attr in enumerate(attrs):
                        valMap[attr] = res[idx]
                    # append to list
                    ids.append(PandaID)
                    returnList.append(valMap)
            # return
            return returnList
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("queryJobInfoPerCloud : %s %s" % (type,value))
            # roll back
            self._rollback()
            return None

        
    # get PandaIDs at Site
    def getPandaIDsSite(self,site,status,limit):
        comment = ' /* DBProxy.getPandaIDsSite */'                
        _logger.debug("getPandaIDsSite : %s %s %s" % (site,status,limit))
        try:
            ids = []
            # find table
            if status in ['defined','assigned']:
                table = 'ATLAS_PANDA.jobsDefined4'
            elif status in ['activated','running','holding','trasnferring']:
                table = 'ATLAS_PANDA.jobsActive4'
            elif status in ['waiting']:
                table = 'ATLAS_PANDA.jobsWaiting4'
            elif status in ['finished','failed']:
                table = 'ATLAS_PANDA.jobsArchived4'
            else:
                _logger.error("unknown status:%s" % status)
                return ids
            # limit
            limit = int(limit)
            # SQL
            sql  = "SELECT PandaID FROM %s " % table
            sql += "WHERE computingSite=:computingSite AND jobStatus=:jobStatus AND prodSourceLabel=:prodSourceLabel "
            sql += "AND rownum<=:limit"    
            # start transaction
            self.conn.begin()
            # select
            varMap = {}
            varMap[':computingSite'] = site
            varMap[':jobStatus'] = status
            varMap[':limit'] = limit
            varMap[':prodSourceLabel'] = 'managed'
            self.cur.arraysize = limit
            self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # convert to list
            for id, in res:
                ids.append(id)
            return ids
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getPandaIDsSite : %s %s" % (type,value))
            # roll back
            self._rollback()
            return []


    # get PandaIDs to be updated in prodDB
    def getPandaIDsForProdDB(self,limit,lockedby):
        comment = ' /* DBProxy.getPandaIDsForProdDB */'                
        _logger.debug("getPandaIDsForProdDB %s" % limit)
        sql0 = "PandaID,jobStatus,stateChangeTime,attemptNr,jobDefinitionID,jobExecutionID FROM %s "
        sqlW = "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND lockedby=:lockedby "
        sqlX = "AND stateChangeTime>prodDBUpdateTime "
        sqlA = "AND (CASE WHEN stateChangeTime>prodDBUpdateTime THEN 1 ELSE null END) = 1 "
        sql1 = "AND rownum<=:limit "
        varMap = {}
        varMap[':lockedby'] = lockedby
        varMap[':limit'] = limit
        varMap[':prodSourceLabel1'] = 'managed'
        varMap[':prodSourceLabel2'] = 'rc_test'                
        try:
            retMap   = {}
            totalIDs = 0
            # select
            for table in ['ATLAS_PANDA.jobsArchived4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']:
                # start transaction
                self.conn.begin()
                # select
                sql = sql0 % table
                if table in ['ATLAS_PANDA.jobsArchived4']:
                    sql = "SELECT /*+ INDEX_RS_ASC(tab JOBSARCHIVED4_CHANGETIME) NO_INDEX(tab(PRODSOURCELABEL))*/ " + sql + " tab " + sqlW + sqlA
                else:
                    sql = "SELECT " + sql + sqlW + sqlX
                sql += sql1    
                self.cur.arraysize = limit
                _logger.debug("getPandaIDsForProdDB %s %s" % (sql+comment,str(varMap)))
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchall()
                _logger.debug("getPandaIDsForProdDB got %s" % len(res))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                for PandaID,jobStatus,stateChangeTime,attemptNr,jobDefinitionID,jobExecutionID in res:
                    # ignore dummy jobs in jobsDefined4
                    if table == 'ATLAS_PANDA.jobsDefined4' and (not jobStatus in ['defined','assigned']):
                        continue
                    # add status
                    if not retMap.has_key(jobStatus):
                        retMap[jobStatus] = []
                    # append    
                    retMap[jobStatus].append({'PandaID':PandaID,'attemptNr':attemptNr,
                                              'stateChangeTime':stateChangeTime.strftime('%Y-%m-%d %H:%M:%S'),
                                              'jobDefinitionID':jobDefinitionID,
                                              'jobExecutionID':jobExecutionID})
                    totalIDs += 1
                    # limit
                    if totalIDs > limit:
                        break
            _logger.debug("getPandaIDsForProdDB %s ret->%s" % (limit,totalIDs))
            return retMap
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getPandaIDsForProdDB : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # update prodDBUpdateTime 
    def updateProdDBUpdateTime(self,param):
        comment = ' /* DBProxy.updateProdDBUpdateTime */'                
        _logger.debug("updateProdDBUpdateTime %s" % str(param))
        sql0 = "UPDATE %s "
        sql0+= "SET prodDBUpdateTime=TO_TIMESTAMP(:prodDBUpdateTime,'YYYY-MM-DD HH24:MI:SS') "
        sql0+= "WHERE PandaID=:PandaID AND jobStatus=:jobStatus AND stateChangeTime=TO_TIMESTAMP(:stateChangeTime,'YYYY-MM-DD HH24:MI:SS') "
        varMap = {}
        varMap[':prodDBUpdateTime'] = param['stateChangeTime']
        varMap[':PandaID']          = param['PandaID']
        varMap[':jobStatus']        = param['jobStatus']
        varMap[':stateChangeTime']  = param['stateChangeTime']
        try:
            # convert to string
            if isinstance(varMap[':prodDBUpdateTime'],datetime.datetime):
                varMap[':prodDBUpdateTime'] = varMap[':prodDBUpdateTime'].strftime('%Y-%m-%d %H:%M:%S')
            if isinstance(varMap[':stateChangeTime'],datetime.datetime):
                varMap[':stateChangeTime'] = varMap[':stateChangeTime'].strftime('%Y-%m-%d %H:%M:%S')
            # set table
            if param['jobStatus'] in ['defined','assigned']:
                table = 'ATLAS_PANDA.jobsDefined4'
            elif param['jobStatus'] in ['waiting','pending']:
                table = 'ATLAS_PANDA.jobsWaiting4'
            elif param['jobStatus'] in ['activated','sent','starting','running','holding','transferring']:
                table = 'ATLAS_PANDA.jobsActive4'
            elif param['jobStatus'] in ['finished','failed','cancelled']:
                table = 'ATLAS_PANDA.jobsArchived4'
            else:
                _logger.error("invalid status %s" % param['jobStatus'])
                return False
            # set transaction
            self.conn.begin()
            # update
            sql = sql0 % table
            _logger.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment, varMap)
            retU = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("updateProdDBUpdateTime %s ret=%s" % (param['PandaID'],retU))
            if retU == 1:
                return True
            return False
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("updateProdDBUpdateTime : %s %s" % (type,value))
            # roll back
            self._rollback()
            return False


    # add metadata
    def addMetadata(self,pandaID,metadata):
        comment = ' /* DBProxy.addMetaData */'        
        _logger.debug("addMetaData : %s" % pandaID)
        sql0 = "SELECT PandaID FROM ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"        
        sql1 = "INSERT INTO ATLAS_PANDA.metaTable (PandaID,metaData) VALUES (:PandaID,:metaData)"
        nTry=3        
        for iTry in range(nTry):
            try:
                # autocommit on
                self.conn.begin()
                # select
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.arraysize = 10
                self.cur.execute(sql0+comment, varMap)
                res = self.cur.fetchone()
                # already exist
                if res != None:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    return True
                # insert
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':metaData'] = metadata
                self.cur.execute(sql1+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("addMetaData : %s retry : %s" % (pandaID,iTry))
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("addMetaData : %s %s" % (type,value))
                return False


    # insert dataset
    def insertDataset(self,dataset,tablename="ATLAS_PANDA.Datasets"):
        comment = ' /* DBProxy.insertDataset */'        
        _logger.debug("insertDataset(%s)" % dataset.name)
        sql0 = "SELECT COUNT(*) FROM %s WHERE vuid=:vuid" % tablename
        sql1 = "INSERT INTO %s " % tablename
        sql1+= "(%s) " % DatasetSpec.columnNames()
        sql1+= DatasetSpec.bindValuesExpression()
        # time information
        dataset.creationdate = datetime.datetime.utcnow()
        dataset.modificationdate = dataset.creationdate
        try:
            # subtype
            if dataset.subType in ['','NULL',None]:
                # define using name
                if re.search('_dis\d+$',dataset.name) != None:
                    dataset.subType = 'dis'
                elif re.search('_sub\d+$',dataset.name) != None:
                    dataset.subType= 'sub'
                else:
                    dataset.subType= 'top'
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[':vuid'] = dataset.vuid
            self.cur.execute(sql0+comment, varMap)
            nDS, = self.cur.fetchone()
            _logger.debug("insertDataset nDS=%s with %s" % (nDS,dataset.vuid))
            if nDS == 0:
                # insert
                _logger.debug("insertDataset insert %s" % dataset.name)
                self.cur.execute(sql1+comment, dataset.valuesMap())
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("insertDataset() : %s %s" % (type,value))
            return False


    # get and lock dataset with a query
    def getLockDatasets(self,sqlQuery,varMapGet,modTimeOffset='',getVersion=False):
        comment = ' /* DBProxy.getLockDatasets */'        
        _logger.debug("getLockDatasets(%s,%s,%s)" % (sqlQuery,str(varMapGet),modTimeOffset))
        sqlGet  = "SELECT /*+ INDEX_RS_ASC(tab(STATUS,TYPE,MODIFICATIONDATE)) */ vuid,name,modificationdate,version,transferStatus FROM ATLAS_PANDA.Datasets tab WHERE " + sqlQuery
        sqlLock = "UPDATE ATLAS_PANDA.Datasets SET modificationdate=CURRENT_DATE"
        if modTimeOffset != '':
            sqlLock += "+%s" % modTimeOffset
        sqlLock += ",transferStatus=MOD(transferStatus+1,10)"
        if getVersion:
            sqlLock += ",version=:version"
        sqlLock += " WHERE vuid=:vuid AND transferStatus=:transferStatus"    
        retList = []
        try:
            # begin transaction
            self.conn.begin()
            # get datasets
            self.cur.arraysize = 1000000
            self.cur.execute(sqlGet+comment,varMapGet)            
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all datasets
            if res != None and len(res) != 0:
                for vuid,name,modificationdate,version,transferStatus in res:
                    # lock
                    varMapLock = {}
                    varMapLock[':vuid'] = vuid
                    varMapLock[':transferStatus'] = transferStatus
                    if getVersion:
                        try:
                            varMapLock[':version'] = str(int(version) + 1)
                        except:
                            varMapLock[':version'] = str(1)
                    # begin transaction
                    self.conn.begin()
                    # update for lock
                    self.cur.execute(sqlLock+comment,varMapLock)
                    retU = self.cur.rowcount
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    if retU > 0:
                        # append
                        if not getVersion:
                            retList.append((vuid,name,modificationdate))
                        else:
                            retList.append((vuid,name,modificationdate,version))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # retrun 
            return retList
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getLockDatasets : %s %s" % (type,value))
            return []


    # query dataset with map
    def queryDatasetWithMap(self,map):
        comment = ' /* DBProxy.queryDatasetWithMap */'               
        _logger.debug("queryDatasetWithMap(%s)" % map)
        if map.has_key('name'):
            sql1  = """SELECT /*+ BEGIN_OUTLINE_DATA """
            sql1 += """INDEX_RS_ASC(@"SEL$1" "TAB"@"SEL$1" ("DATASETS"."NAME")) """
            sql1 += """OUTLINE_LEAF(@"SEL$1") ALL_ROWS """
            sql1 += """OPTIMIZER_FEATURES_ENABLE('10.2.0.4') """
            sql1 += """IGNORE_OPTIM_EMBEDDED_HINTS """
            sql1 += """END_OUTLINE_DATA */ """
            sql1 += "%s FROM ATLAS_PANDA.Datasets tab" % DatasetSpec.columnNames()
        else:
            sql1 = "SELECT %s FROM ATLAS_PANDA.Datasets" % DatasetSpec.columnNames()            
        varMap = {}
        for key in map.keys():
            if len(varMap)==0:
                sql1+= " WHERE %s=:%s" % (key,key)
            else:
                sql1+= " AND %s=:%s" % (key,key)
            varMap[':%s' % key] = map[key]
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 100
            _logger.debug(sql1+comment+str(varMap))            
            self.cur.execute(sql1+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # instantiate Dataset
            if res != None and len(res) != 0:
                dataset = DatasetSpec()
                dataset.pack(res[0])
                return dataset
            _logger.error("queryDatasetWithMap(%s) : dataset not found" % map)
            return None
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("queryDatasetWithMap(%s) : %s %s" % (map,type,value))
            return None


    # update dataset
    def updateDataset(self,datasets,withLock,withCriteria,criteriaMap):
        comment = ' /* DBProxy.updateDataset */'               
        _logger.debug("updateDataset()")        
        sql1 = "UPDATE ATLAS_PANDA.Datasets SET %s " % DatasetSpec.bindUpdateExpression()
        sql1+= "WHERE vuid=:vuid"
        if withCriteria != "":
            sql1+= " AND %s" % withCriteria
        retList = []
        try:
            # start transaction
            self.conn.begin()
            for dataset in datasets:
                _logger.debug("updateDataset(%s,%s)" % (dataset.name,dataset.status))
                # time information
                dataset.modificationdate = datetime.datetime.utcnow()
                # update
                varMap = dataset.valuesMap()
                varMap[':vuid'] = dataset.vuid
                for cKey in criteriaMap.keys():
                    varMap[cKey] = criteriaMap[cKey]
                self.cur.execute(sql1+comment, varMap)                
                retU = self.cur.rowcount            
                if retU != 0 and retU != 1:
                    raise RuntimeError, 'Invalid retrun %s' % retU
                retList.append(retU)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("updateDataset() ret:%s" % retList)                    
            return retList
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("updateDataset() : %s %s" % (type,value))
            return []

            
    # delete dataset
    def deleteDataset(self,name):
        comment = ' /* DBProxy.deleteDataset */'        
        sql1 = "DELETE /*+ INDEX(tab DATASETS_NAME_IDX)*/ FROM ATLAS_PANDA.Datasets tab WHERE name=:name"
        try:
            # start transaction
            self.conn.begin()
            # delete
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql1+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback()
            # error 
            type, value, traceBack = sys.exc_info()
            _logger.error("deleteDataset() : %s %s" % (type,value))
            return False


    # get serial number for dataset, insert dummy datasets to increment SN
    def getSerialNumber(self,datasetname,definedFreshFlag=None):
        comment = ' /* DBProxy.getSerialNumber */'        
        try:
            _logger.debug("getSerialNumber(%s,%s)" % (datasetname,definedFreshFlag))
            # start transaction
            self.conn.begin()
            # check freashness
            if definedFreshFlag == None:
                # select
                varMap = {}
                varMap[':name'] = datasetname
                varMap[':type'] = 'output'
                sql = "SELECT /*+ INDEX_RS_ASC(TAB (DATASETS.NAME)) */ COUNT(*) FROM ATLAS_PANDA.Datasets tab WHERE type=:type AND name=:name"
                self.cur.arraysize = 100            
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchone()
                # fresh dataset or not
                if res != None and len(res) != 0 and res[0] > 0:
                    freshFlag = False
                else:
                    freshFlag = True
            else:
                # use predefined flag
                freshFlag = definedFreshFlag
            # get serial number
            sql = "SELECT ATLAS_PANDA.SUBCOUNTER_SUBID_SEQ.nextval FROM dual";
            self.cur.arraysize = 100
            self.cur.execute(sql+comment, {})
            sn, = self.cur.fetchone()            
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # release file lock
            _logger.debug("getSerialNumber : %s %s" % (sn,freshFlag))
            return (sn,freshFlag)
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getSerialNumber() : %s %s" % (type,value))
            return (-1,False)


    # get serial number for group job
    def getSerialNumberForGroupJob(self,name):
        comment = ' /* DBProxy.getSerialNumberForGroupJob */'
        retVal = {'sn':'','status':False}
        try:
            _logger.debug("getSerialNumberForGroupJob(%s)" % name)
            # start transaction
            self.conn.begin()
            # get serial number
            sql = "SELECT ATLAS_PANDA.GROUP_JOBID_SEQ.nextval FROM dual";
            self.cur.execute(sql+comment, {})
            sn, = self.cur.fetchone()            
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            retVal['sn'] = sn
            retVal['status'] = True
            _logger.debug("getSerialNumberForGroupJob : %s %s" % (name,str(retVal)))
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getSerialNumberForGroupJob : %s %s" % (errtype,errvalue))
            retVal['status'] = False
            return retVal


    # update transfer status for a dataset
    def updateTransferStatus(self,datasetname,bitMap):
        comment = ' /* DBProxy.updateTransferStatus */'        
        try:
            _logger.debug("updateTransferStatus(%s,%s)" % (datasetname,hex(bitMap)))
            # start transaction
            self.conn.begin()
            retTransSt = 0
            # update bitmap
            sqlU = 'UPDATE /*+ INDEX_RS_ASC(TAB("DATASETS"."NAME")) */ ATLAS_PANDA.Datasets tab SET transferStatus=ATLAS_PANDA.BITOR(transferStatus,:bitMap) WHERE name=:name'
            varMap = {}
            varMap[':bitMap'] = bitMap
            varMap[':name'] = datasetname
            retU = self.cur.execute(sqlU+comment, varMap)
            # get transferStatus
            sqlS = 'SELECT /*+ INDEX_RS_ASC(TAB("DATASETS"."NAME")) */ transferStatus FROM ATLAS_PANDA.Datasets tab WHERE name=:name'
            varMap = {}
            varMap[':name'] = datasetname
            self.cur.arraysize = 10                        
            retS = self.cur.execute(sqlS+comment, varMap)
            resS = self.cur.fetchall()
            if resS != None and len(resS) != 0:
                retTransSt = resS[0][0]
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("updateTransferStatus : %s" % hex(retTransSt))
            return retTransSt
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("updateTransferStatus : %s %s" % (type,value))
            return 0

        
    # get CloudTask. If not exist, create it
    def getCloudTask(self,tid):
        comment = ' /* getCloudTask */'        
        try:
            _logger.debug("getCloudTask(%s)" % tid)
            # check tid
            if tid in [None,'NULL']:
                _logger.error("invalid TID : %s" % tid)
                return None
            # start transaction
            self.conn.begin()
            # get CloudTask
            sql  = "SELECT %s FROM ATLAS_PANDA.cloudtasks " % CloudTaskSpec.columnNames()
            sql += "WHERE taskid=:taskid"
            varMap = {}
            varMap[':taskid'] = tid 
            # select
            self.cur.arraysize = 10            
            self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchall()
            # already exist
            if res != None and len(res) != 0:
                # instantiate CloudTask
                cloudTask = CloudTaskSpec()
                cloudTask.pack(res[0])
                # update tmod if status is defined
                if cloudTask.status == 'defined':
                    sql = "UPDATE ATLAS_PANDA.cloudtasks SET tmod=CURRENT_DATE WHERE taskid=:taskid"
                    varMap = {}
                    varMap[':taskid'] = cloudTask.taskid
                    self.cur.execute(sql+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return cloudTask
            # insert new CloudTask
            _logger.debug("insert new CloudTask")
            cloudTask = CloudTaskSpec()
            cloudTask.taskid = tid
            cloudTask.status = 'defined' 
            sql = "INSERT INTO ATLAS_PANDA.cloudtasks (id,taskid,status,tmod,tenter) VALUES(ATLAS_PANDA.CLOUDTASKS_ID_SEQ.nextval,:taskid,:status,CURRENT_DATE,CURRENT_DATE)"
            sql+= " RETURNING id INTO :newID"          
            varMap = {}
            varMap[':taskid'] = cloudTask.taskid
            varMap[':status'] = cloudTask.status
            varMap[':newID']  = self.cur.var(cx_Oracle.NUMBER)                       
            self.cur.execute(sql+comment, varMap)
            # get id
            cloudTask.id = long(varMap[':newID'].getvalue())
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("return new CloudTask")                            
            return cloudTask
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getCloudTask() : %s %s" % (type,value))
            return None

        
    # set cloud to CloudTask
    def setCloudTask(self,cloudTask):
        comment = ' /* setCloudTask */'        
        try:
            _logger.debug("setCloudTask(id=%s,taskid=%s)" % (cloudTask.id,cloudTask.taskid))
            sql  = "UPDATE ATLAS_PANDA.cloudtasks SET cloud=:cloud,status=:newStatus,tmod=CURRENT_DATE WHERE id=:id AND status=:oldStatus" 
            # start transaction
            self.conn.begin()
            # update
            varMap = {}
            varMap[':cloud'] = cloudTask.cloud
            varMap[':id'] = cloudTask.id
            varMap[':newStatus'] = 'assigned'
            varMap[':oldStatus'] = 'defined'
            self.cur.execute(sql+comment, varMap)
            retU = self.cur.rowcount            
            # succeeded
            if retU == 1:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return cloudTask
            # read if it is already set by another thread
            sql  = "SELECT %s FROM ATLAS_PANDA.cloudtasks " % CloudTaskSpec.columnNames()
            sql += "WHERE id=:id"
            varMap = {}
            varMap[':id'] = cloudTask.id
            # select
            self.cur.arraysize = 10            
            retS = self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # retrun CloudTask
            if res != None and len(res) != 0:
                # instantiate CloudTask
                cloudTask = CloudTaskSpec()
                cloudTask.pack(res[0])
                return cloudTask
            _logger.error("setCloudTask() : cannot find CloudTask for %s" % cloudTask.id)
            return None
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("setCloudTask() : %s %s" % (type,value))
            return None


    # see CloudTask
    def seeCloudTask(self,tid):
        comment = ' /* seeCloudTask */'        
        try:
            _logger.debug("seeCloudTask(%s)" % tid)
            # check tid
            if tid in [None,'NULL']:
                _logger.error("invalid TID : %s" % tid)
                return None
            # start transaction
            self.conn.begin()
            # select
            sql  = "SELECT cloud FROM ATLAS_PANDA.cloudtasks WHERE taskid=:taskid"
            varMap = {}
            varMap[':taskid'] = tid
            self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # existing task
            if res != None and len(res) != 0:
                # return cloud
                return res[0][0]
            else:
                return None
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("seeCloudTask() : %s %s" % (type,value))
            return None

        
    # get assigning task
    def getAssigningTask(self):
        comment = ' /* getAssigningTask */'        
        try:
            _logger.debug("getAssigningTask")
            timeLimit  = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
            # start transaction
            self.conn.begin()
            # select
            sql = "SELECT taskid FROM ATLAS_PANDA.cloudtasks WHERE status=:status AND tmod>:tmod"
            varMap = {}
            varMap[':tmod']   = timeLimit
            varMap[':status'] = 'defined'
            self.cur.arraysize = 100                        
            self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all taskid
            retList = []
            if res != None:
                for tid, in res:
                    retList.append(tid)
            # return        
            _logger.debug("getAssigningTask ret:%s" % retList)                            
            return retList
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getAssigningTask : %s %s" % (type,value))
            return []


    # set CloudTask by user
    def setCloudTaskByUser(self,user,tid,cloud,status):
        comment = ' /* setCloudTaskByUser */'        
        try:
            _logger.debug("setCloudTaskByUser(tid=%s,cloud=%s,status=%s) by %s" % (tid,cloud,status,user))
            # check tid
            if tid in [None,'NULL']:
                tmpMsg = "invalid TID : %s" % tid
                _logger.error(tmpMsg)
                return "ERROR: " + tmpMsg
            # check status
            statusList = ['tobeaborted']
            if not status in statusList:
                tmpMsg = "invalid status=%s. Must be one of %s" (status,str(statusList))
                _logger.error(tmpMsg)
                return "ERROR: " + tmpMsg
            # start transaction
            self.conn.begin()
            # get CloudTask
            sql  = "SELECT %s FROM ATLAS_PANDA.cloudtasks " % CloudTaskSpec.columnNames()
            sql += "WHERE taskid=:taskid"
            varMap = {}
            varMap[':taskid'] = tid 
            # select
            self.cur.arraysize = 10            
            self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchall()
            # already exist
            if res != None and len(res) != 0:
                # set status
                sql = "UPDATE ATLAS_PANDA.cloudtasks SET status=:status,tmod=CURRENT_DATE WHERE taskid=:taskid"
                varMap = {}
                varMap[':taskid'] = tid
                varMap[':status'] = status
                self.cur.execute(sql+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return "SUCCEEDED"
            # insert new CloudTask
            sql = "INSERT INTO ATLAS_PANDA.cloudtasks (id,taskid,status,tmod,tenter) VALUES(ATLAS_PANDA.CLOUDTASKS_ID_SEQ.nextval,:taskid,:status,CURRENT_DATE,CURRENT_DATE)"
            varMap = {}
            varMap[':taskid'] = tid
            varMap[':status'] = status
            self.cur.execute(sql+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return "SUCCEEDED"
        except:
            # roll back
            self._rollback()
            # error
            errType,errValue = sys.exc_info()[:2]
            _logger.error("setCloudTaskByUser() : %s %s" % (errType,errValue))
            return "ERROR: database error"

        
    # query files with map
    def queryFilesWithMap(self,map):
        comment = ' /* DBProxy.queryFilesWithMap */'        
        _logger.debug("queryFilesWithMap()")
        sql1 = "SELECT PandaID,%s FROM ATLAS_PANDA.filesTable4" % FileSpec.columnNames()
        varMap = {}
        for key in map.keys():
            if len(varMap)==0:
                sql1+= " WHERE %s=:%s" % (key,key)
            else:
                sql1+= " AND %s=:%s" % (key,key)
            varMap[':%s' % key] = map[key]
        nTry=3
        for iTry in range(nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000                
                self.cur.execute(sql1+comment, varMap)
                res = self.cur.fetchall()
                _logger.debug("queryFilesWithMap() : %s" % str(res))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # instantiate files
                retList = []
                for item in res:
                    # instantiate dummy JobSpec obj for PandaID
                    job = JobSpec()
                    job.PandaID = item[0]
                    # instantiate file
                    file = FileSpec()
                    file.pack(item[1:])
                    # set owner
                    file.setOwner(job)
                    # append
                    retList.append(file)
                return retList
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("queryFilesWithMap retry : %s" % iTry)
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("queryFilesWithMap : %s %s" % (type,value))
                return []


    # count the number of files with map
    def countFilesWithMap(self,map):
        comment = ' /* DBProxy.countFilesWithMap */'        
        sql1 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ COUNT(*) FROM ATLAS_PANDA.filesTable4 tab"
        varMap = {}
        for key in map.keys():
            if len(varMap)==0:
                sql1+= " WHERE %s=:%s" % (key,key)
            else:
                sql1+= " AND %s=:%s" % (key,key)                
            varMap[':%s' % key] = map[key]
        nTry=3
        for iTry in range(nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                _logger.debug("countFilesWithMap() : %s %s" % (sql1,str(map)))
                self.cur.arraysize = 10                
                retS = self.cur.execute(sql1+comment, varMap)
                res = self.cur.fetchone()
                _logger.debug("countFilesWithMap() : %s %s" % (retS,str(res)))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                nFiles=0
                if res != None:
                    nFiles=res[0]
                return nFiles
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("countFilesWithMap() retry : %s" % iTry)
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("countFilesWithMap(%s) : %s %s" % (map,type,value))
                return -1


    # count the number of pending files
    def countPendingFiles(self,pandaID):
        comment = ' /* DBProxy.countPendingFiles */'        
        sql1 = "SELECT COUNT(*) FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:pandaID AND type=:type AND status<>:status "
        varMap = {}
        varMap[':pandaID'] = pandaID
        varMap[':type'] = 'input'
        varMap[':status'] = 'ready'
        try:
            # start transaction
            self.conn.begin()
            # select
            _logger.debug("countPendingFiles : %s start" % pandaID)
            self.cur.arraysize = 10                
            retS = self.cur.execute(sql1+comment, varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            nFiles = -1
            if res != None:
                nFiles=res[0]
            return nFiles
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("countPendingFiles : %s : %s %s" % (pandaID,errType,errValue))
            return -1


    # get input files currently in use for analysis
    def getFilesInUseForAnal(self,outDataset):
        comment = ' /* DBProxy.getFilesInUseForAnal */'        
        sqlSub  = "SELECT /*+ index(tab FILESTABLE4_DATASET_IDX) */ destinationDBlock,PandaID FROM ATLAS_PANDA.filesTable4 tab "
        sqlSub += "WHERE dataset=:dataset AND type IN (:type1,:type2) AND modificationTime<=CURRENT_DATE GROUP BY destinationDBlock,PandaID"
        sqlPan  = "SELECT jobStatus FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID AND modificationTime<=CURRENT_DATE "
        sqlPan += "UNION "
        sqlPan += "SELECT jobStatus FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30)"        
        sqlDis  = "SELECT distinct dispatchDBlock FROM ATLAS_PANDA.filesTable4 "
        sqlDis += "WHERE PandaID=:PandaID AND type=:type AND dispatchDBlock IS NOT NULL AND modificationTime <= CURRENT_DATE"
        sqlLfn  = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ lfn,PandaID FROM ATLAS_PANDA.filesTable4 tab "
        sqlLfn += "WHERE dispatchDBlock=:dispatchDBlock AND type=:type "
        sqlLfn += "AND (destinationDBlockToken IS NULL OR destinationDBlockToken<>:noshadow) AND modificationTime<=CURRENT_DATE"
        nTry=3
        for iTry in range(nTry):
            inputFilesList = []
            try:
                # start transaction
                self.conn.begin()
                # get sub datasets
                varMap = {}
                varMap[':dataset'] = outDataset
                varMap[':type1'] = 'output'
                varMap[':type2'] = 'log'                
                _logger.debug("getFilesInUseForAnal : %s %s" % (sqlSub,str(varMap)))
                self.cur.arraysize = 100000
                retS = self.cur.execute(sqlSub+comment, varMap)
                res = self.cur.fetchall()
                subDSpandaIDmap = {}
                checkedPandaIDs = {}
                for subDataset,pandaID in res:
                    if not checkedPandaIDs.has_key(pandaID):
                        # check status
                        varMap = {}
                        varMap[':PandaID'] = pandaID
                        retP = self.cur.execute(sqlPan+comment, varMap)
                        resP = self.cur.fetchall()
                        # append
                        if len(resP) != 0:
                            checkedPandaIDs[pandaID] = resP[0][0]
                        else:
                            checkedPandaIDs[pandaID] = 'running'
                    # reuse failed files if jobs are in Archived since they cannot change back to active
                    if checkedPandaIDs[pandaID] in ['failed','cancelled']:
                        continue
                    # collect PandaIDs
                    if not subDSpandaIDmap.has_key(subDataset):
                        subDSpandaIDmap[subDataset] = []
                    subDSpandaIDmap[subDataset].append(pandaID)
                # loop over all sub datasets
                for subDataset,activePandaIDs in subDSpandaIDmap.iteritems():
                    # get dispatchDBlocks
                    pandaID = activePandaIDs[0]
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    varMap[':type'] = 'input'                        
                    _logger.debug("getFilesInUseForAnal : %s %s" % (sqlDis,str(varMap)))
                    self.cur.arraysize = 10000
                    retD = self.cur.execute(sqlDis+comment, varMap)
                    resD = self.cur.fetchall()
                    # get LFNs
                    for disDataset, in resD:
                        # use new style only
                        if not disDataset.startswith('user_disp.'):
                            continue
                        varMap = {}
                        varMap[':dispatchDBlock'] = disDataset
                        varMap[':type'] = 'input'
                        varMap[':noshadow'] = 'noshadow'
                        _logger.debug("getFilesInUseForAnal : %s %s" % (sqlLfn,str(varMap)))
                        self.cur.arraysize = 100000
                        retL = self.cur.execute(sqlLfn+comment, varMap)
                        resL = self.cur.fetchall()
                        # append
                        for lfn,filePandaID in resL:
                            # skip files used by archived failed or cancelled jobs
                            if filePandaID in activePandaIDs and not lfn in inputFilesList:
                                inputFilesList.append(lfn)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                _logger.debug("getFilesInUseForAnal : %s" % len(inputFilesList))
                return inputFilesList
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("inputFilesList retry : %s" % iTry)
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("inputFilesList(%s) : %s %s" % (outDataset,type,value))
                return []


    # get list of dis dataset to get input files in shadow
    def getDisInUseForAnal(self,outDataset):
        comment = ' /* DBProxy.getDisInUseForAnal */'        
        sqlSub  = "SELECT /*+ index(tab FILESTABLE4_DATASET_IDX) */ destinationDBlock,PandaID FROM ATLAS_PANDA.filesTable4 tab "
        sqlSub += "WHERE dataset=:dataset AND type IN (:type1,:type2) AND modificationTime<=CURRENT_DATE GROUP BY destinationDBlock,PandaID"
        sqlPan  = "SELECT jobStatus FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID AND modificationTime<=CURRENT_DATE "
        sqlPan += "UNION "
        sqlPan += "SELECT jobStatus FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30)"        
        sqlDis  = "SELECT distinct dispatchDBlock FROM ATLAS_PANDA.filesTable4 "
        sqlDis += "WHERE PandaID=:PandaID AND type=:type AND dispatchDBlock IS NOT NULL AND modificationTime <= CURRENT_DATE"
        inputDisList = []
        try:
            # start transaction
            self.conn.begin()
            # get sub datasets
            varMap = {}
            varMap[':dataset'] = outDataset
            varMap[':type1'] = 'output'
            varMap[':type2'] = 'log'                
            _logger.debug("getFilesInUseForAnal : %s %s" % (sqlSub,str(varMap)))
            self.cur.arraysize = 100000
            retS = self.cur.execute(sqlSub+comment, varMap)
            res = self.cur.fetchall()
            subDSpandaIDmap = {}
            checkedPandaIDs = {}
            for subDataset,pandaID in res:
                if not checkedPandaIDs.has_key(pandaID):
                    # check status
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    retP = self.cur.execute(sqlPan+comment, varMap)
                    resP = self.cur.fetchall()
                    # append
                    if len(resP) != 0:
                        checkedPandaIDs[pandaID] = resP[0][0]
                    else:
                        checkedPandaIDs[pandaID] = 'running'
                # reuse failed files if jobs are in Archived since they cannot change back to active
                if checkedPandaIDs[pandaID] in ['failed','cancelled']:
                    continue
                # collect PandaIDs
                if not subDSpandaIDmap.has_key(subDataset):
                    subDSpandaIDmap[subDataset] = []
                subDSpandaIDmap[subDataset].append(pandaID)
            # loop over all sub datasets
            for subDataset,activePandaIDs in subDSpandaIDmap.iteritems():
                resDisList = []                    
                # get dispatchDBlocks
                pandaID = activePandaIDs[0]
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':type'] = 'input'                        
                _logger.debug("getFilesInUseForAnal : %s %s" % (sqlDis,str(varMap)))
                self.cur.arraysize = 10000
                retD = self.cur.execute(sqlDis+comment, varMap)
                resD = self.cur.fetchall()
                # get shadow dis
                for disDataset, in resD:
                    # use new style only
                    if not disDataset.startswith('user_disp.'):
                        continue
                    if not disDataset in resDisList:
                        resDisList.append(disDataset)
                # append
                inputDisList.append((resDisList,activePandaIDs))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("getDisInUseForAnal : %s" % len(inputDisList))
            return inputDisList
        except:
            # roll back
            self._rollback()
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getDisInUseForAnal(%s) : %s %s" % (outDataset,errtype,errvalue))
            return None


    # get input LFNs currently in use for analysis with shadow dis
    def getLFNsInUseForAnal(self,inputDisList):
        comment = ' /* DBProxy.getLFNsInUseForAnal */'        
        sqlLfn  = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ lfn,PandaID FROM ATLAS_PANDA.filesTable4 tab "
        sqlLfn += "WHERE dispatchDBlock=:dispatchDBlock AND type=:type "
        sqlLfn += "AND (destinationDBlockToken IS NULL OR destinationDBlockToken<>:noshadow) AND modificationTime<=CURRENT_DATE"
        inputFilesList = []
        try:
            # loop over all shadow dis datasets
            for disDatasetList,activePandaIDs in inputDisList:
                for disDataset in disDatasetList:
                    # use new style only
                    if not disDataset.startswith('user_disp.'):
                        continue
                    # start transaction
                    self.conn.begin()
                    varMap = {}
                    varMap[':dispatchDBlock'] = disDataset
                    varMap[':type'] = 'input'
                    varMap[':noshadow'] = 'noshadow'
                    _logger.debug("getLFNsInUseForAnal : %s %s" % (sqlLfn,str(varMap)))
                    self.cur.arraysize = 100000
                    retL = self.cur.execute(sqlLfn+comment, varMap)
                    resL = self.cur.fetchall()
                    # append
                    for lfn,filePandaID in resL:
                        # skip files used by archived failed or cancelled jobs
                        if filePandaID in activePandaIDs and not lfn in inputFilesList:
                            inputFilesList.append(lfn)
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
            _logger.debug("getLFNsInUseForAnal : %s" % len(inputFilesList))
            return inputFilesList
        except:
            # roll back
            self._rollback()
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getLFNsInUseForAnal(%s) : %s %s" % (str(inputDisList),errtype,errvalue))
            return None


    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self,dataset,status,fileGUID=''):
        comment = ' /* DBProxy.updateInFilesReturnPandaIDs */'                                
        _logger.debug("updateInFilesReturnPandaIDs(%s)" % dataset)
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status=:status WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        varMap = {}
        varMap[':status'] = status
        varMap[':dispatchDBlock'] = dataset
        if fileGUID != '':
            sql0 += " GUID=:guid"
            sql1 += " GUID=:guid"
            varMap[':guid'] = fileGUID
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000                                
                retS = self.cur.execute(sql0+comment, varMap)
                resS = self.cur.fetchall()
                # update
                retU = self.cur.execute(sql1+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # collect PandaIDs
                retList = []
                for tmpRowID,tmpPandaID in resS:
                    # append
                    if not tmpPandaID in retList:
                        retList.append(tmpPandaID)
                # return
                _logger.debug("updateInFilesReturnPandaIDs : %s" % str(retList))
                return retList
            except:
                # roll back
                self._rollback()
                # error report
                if iTry+1 < self.nTry:
                    _logger.debug("updateInFilesReturnPandaIDs retry : %s" % iTry)                    
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("updateInFilesReturnPandaIDs : %s %s" % (type, value))
        return []


    # update file status in dispatch dataset
    def updateFileStatusInDisp(self,dataset,fileStatusMap):
        comment = ' /* DBProxy.updateFileStatusInDisp */'                                
        _logger.debug("updateFileStatusInDisp(%s,%s)" % (dataset,fileStatusMap))
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status=:status WHERE dispatchDBlock=:dispatchDBlock AND lfn=:lfn"
        nTry = 1
        for iTry in range(nTry):
            try:
                # start transaction
                self.conn.begin()
                # update
                for status,lfns in fileStatusMap.iteritems():
                    varMap = {}
                    varMap[':status'] = status
                    varMap[':dispatchDBlock'] = dataset
                    # loop over all files
                    for lfn in lfns:
                        varMap['lfn'] = lfn
                        # update
                        retU = self.cur.execute(sql1+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                _logger.debug("updateFileStatusInDisp : done")
                return True
            except:
                # roll back
                self._rollback()
                # error report
                if iTry+1 < nTry:
                    _logger.debug("updateFileStatusInDisp retry : %s" % iTry)                    
                    time.sleep(random.randint(5,10))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("updateFileStatusInDisp : %s %s" % (type, value))
        return False


    # update output files and return corresponding PandaIDs
    def updateOutFilesReturnPandaIDs(self,dataset):
        comment = ' /* DBProxy.updateOutFilesReturnPandaIDs */'                        
        _logger.debug("updateOutFilesReturnPandaIDs(%s)" % dataset)
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND status=:status"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status='ready' WHERE destinationDBlock=:destinationDBlock AND status=:status"
        varMap = {}
        varMap[':status'] = 'transferring'
        varMap[':destinationDBlock'] = dataset
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000                
                retS = self.cur.execute(sql0+comment, varMap)
                resS = self.cur.fetchall()
                # update
                retList = []
                retU = self.cur.execute(sql1+comment, varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # collect PandaIDs
                retList = []
                for tmpRowID,tmpPandaID in resS:
                    # append
                    if not tmpPandaID in retList:
                        retList.append(tmpPandaID)
                # return
                _logger.debug("updateOutFilesReturnPandaIDs : %s" % str(retList))
                return retList
            except:
                # roll back
                self._rollback()
                # error report
                if iTry+1 < self.nTry:
                    _logger.debug("updateOutFilesReturnPandaIDs retry : %s" % iTry)                    
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("updateOutFilesReturnPandaIDs : %s %s" % (type, value))
        return []


    # get _dis datasets associated to _sub
    def getAssociatedDisDatasets(self,subDsName):
        comment = ' /* DBProxy.getAssociatedDisDatasets */'                        
        _logger.debug("getAssociatedDisDatasets(%s)" % subDsName)
        sqlF = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ distinct PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock"
        sqlJ = "SELECT distinct dispatchDBlock FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type"
        try:
            # start transaction
            self.conn.begin()
            # get PandaIDs
            varMap = {}
            varMap[':destinationDBlock'] = subDsName
            self.cur.arraysize = 10000                
            self.cur.execute(sqlF+comment,varMap)
            resS = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all PandaIDs
            retList = []
            for pandaID, in resS:
                # start transaction
                self.conn.begin()
                # get _dis name
                varMap = {}
                varMap[':type'] = 'input'
                varMap[':PandaID'] = pandaID
                self.cur.arraysize = 1000                                
                self.cur.execute(sqlJ+comment,varMap)
                resD = self.cur.fetchall()                
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # append
                for disName, in resD:
                    if disName != None and not disName in retList:
                        retList.append(disName)
            # return
            _logger.debug("getAssociatedDisDatasets : %s" % str(retList))
            return retList
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getAssociatedDisDatasets : %s : %s %s" % (subDsName,errType,errValue))
            return []


    # set GUIDs
    def setGUIDs(self,files):
        comment = ' /* DBProxy.setGUIDs */'                        
        _logger.debug("setGUIDs(%s)" % files)
        sql0 = "UPDATE ATLAS_PANDA.filesTable4 SET GUID=:GUID,fsize=:fsize,checksum=:checksum WHERE lfn=:lfn"
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                # update
                for file in files:
                    varMap = {}
                    varMap[':GUID']     = file['guid']
                    varMap[':lfn']      = file['lfn']
                    if file['checksum'] in ['','NULL']:
                        varMap[':checksum'] = None
                    else:
                        varMap[':checksum'] = file['checksum']
                    varMap[':fsize']    = file['fsize']                    
                    self.cur.execute(sql0+comment, varMap)
                    retU = self.cur.rowcount
                    _logger.debug("setGUIDs : retU %s" % retU)
                    if retU<0:
                        raise RuntimeError, 'SQL error'
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True 
            except:
                # roll back
                self._rollback()
                    # error report
                if iTry+1 < self.nTry:
                    _logger.debug("setGUIDs retry : %s" % iTry)                    
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("setGUIDs : %s %s" % (type, value))
        return False

    
    # query PandaID with Datasets
    def queryPandaIDwithDataset(self,datasets):
        comment = ' /* DBProxy.queryPandaIDwithDataset */'                
        _logger.debug("queryPandaIDwithDataset(%s)" % datasets)
        if len(datasets) == 0:
            return []
        # make SQL query
        sql1 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock GROUP BY PandaID"
        # execute
        try:
            retList = []
            for dataset in datasets:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                varMap = {}
                varMap[':destinationDBlock'] = dataset
                self.cur.execute(sql1+comment,varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # get IDs
                for r in res:
                    retList.append(r[0])
            # return
            _logger.debug("queryPandaIDwithDataset : %s" % str(retList))
            return retList
        except:
            # roll back
            self._rollback()
            # error report
            type, value, traceBack = sys.exc_info()
            _logger.error("queryPandaIDwithDataset : %s %s" % (type, value))
            return []
            

    # query last files in datasets
    def queryLastFilesInDataset(self,datasets):
        comment = ' /* DBProxy.queryLastFilesInDataset */'                
        _logger.debug("queryLastFilesInDataset(%s)" % datasets)
        if len(datasets) == 0:
            return []
        # make SQL query
        sql1 = "SELECT /*+ index(tab FILESTABLE4_DATASET_IDX) */ lfn,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE dataset=:dataset AND type=:type ORDER BY lfn DESC"
        sqlL = "SELECT processingType FROM %s WHERE PandaID=:PandaID "
        sqlA = "UNION SELECT processingType FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30)"
        sql2 = "SELECT lfn FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type"
        # execute
        try:
            retMap = {}
            for dataset in datasets:
                # start transaction
                self.conn.begin()
                # select max LFN
                varMap = {}
                varMap[':type'] = 'output'
                varMap[':dataset'] = dataset
                self.cur.arraysize = 100000                
                self.cur.execute(sql1+comment, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # found
                retList = []
                for tmpLFN,pandaID in res:
                    # skip log.tgz
                    if re.search('\.log\.tgz(\.\d+)*$',tmpLFN) != None:
                        continue
                    # start transaction
                    self.conn.begin()
                    self.cur.arraysize = 10
                    # check processingType
                    processingType = None
                    for tmpTable in ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsArchived4']:
                        varMap = {}
                        varMap[':PandaID'] = pandaID
                        if tmpTable == 'ATLAS_PANDA.jobsArchived4':
                            self.cur.execute((sqlL % tmpTable)+sqlA+comment, varMap)
                        else:
                            self.cur.execute((sqlL % tmpTable)+comment, varMap)                            
                        resP = self.cur.fetchone()
                        if resP != None:
                            processingType = resP[0]
                            break
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # job not found
                    if processingType == None:
                        raise RuntimeError, 'job not found'
                    # ignore merge jobs
                    if processingType in ['usermerge']:
                        continue
                    # start transaction
                    self.conn.begin()
                    # select LFNs
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    varMap[':type'] = 'output'
                    self.cur.arraysize = 1000
                    self.cur.execute(sql2+comment, varMap)
                    res = self.cur.fetchall()
                    for r in res:
                        retList.append(r[0])
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # get only the largest one
                    break
                # append
                retMap[dataset] = retList
            # return
            _logger.debug("queryLastFilesInDataset : %s" % str(retMap))
            return retMap
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("queryLastFilesInDataset : %s %s" % (type, value))
            return {}


    # query PandaID with filenames
    def queryPandaIDwithLFN(self,vlfns):
        comment = ' /* DBProxy.queryPandaIDwithLFN */'        
        _logger.debug("queryPandaIDwithLFN(%s)" % vlfns)
        if len(vlfns) == 0:
            return []
        # make SQL query
        sql1 = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 WHERE lfn=:lfn GROUP BY PandaID"
        # execute
        retList = []
        for lfn in vlfns:
            # get generic LFNs
            gLFN = re.sub('\.\d+$','',lfn)
            # try
            try:
                # start transaction
                self.conn.begin()
                # select
                varMap = {}
                varMap[':lfn'] = gLFN
                self.cur.arraysize = 10000
                self.cur.execute(sql1+comment, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # append IDs
                for tmpID, in res:
                    if not tmpID in retList:
                        retList.append(tmpID)
            except:
                # roll back
                self._rollback()
                # error report
                type, value, traceBack = sys.exc_info()
                _logger.error("queryPandaIDwithLFN : %s %s" % (type, value))
                return []
        # return
        _logger.debug("queryPandaIDwithLFN : %s" % str(retList))
        return retList


    # get job statistics
    def getJobStatistics(self,archived=False,predefined=False,workingGroup='',countryGroup='',jobType='',forAnal=None,minPriority=None):
        comment = ' /* DBProxy.getJobStatistics */'        
        _logger.debug("getJobStatistics(%s,%s,'%s','%s','%s',%s,%s)" % (archived,predefined,workingGroup,countryGroup,jobType,forAnal,minPriority))
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        sql0  = "SELECT computingSite,jobStatus,COUNT(*) FROM %s WHERE prodSourceLabel IN ("
        # processingType
        tmpJobTypeMap = {}
        sqlJobType = ''
        if jobType == "":
            if forAnal == None:
                tmpJobTypeMap[':prodSourceLabel1'] = 'managed'
                tmpJobTypeMap[':prodSourceLabel2'] = 'user'
                tmpJobTypeMap[':prodSourceLabel3'] = 'panda'
                tmpJobTypeMap[':prodSourceLabel4'] = 'ddm'
                tmpJobTypeMap[':prodSourceLabel5'] = 'rc_test'
                sqlJobType = ":prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3,:prodSourceLabel4,:prodSourceLabel5) "
            elif forAnal == True:
                tmpJobTypeMap[':prodSourceLabel1'] = 'user'
                tmpJobTypeMap[':prodSourceLabel2'] = 'panda'
                sqlJobType = ":prodSourceLabel1,:prodSourceLabel2) "
            else:
                tmpJobTypeMap[':prodSourceLabel1'] = 'managed'
                sqlJobType = ":prodSourceLabel1) "
        else:
            # loop over all types
            idxJT = 1
            for tmpJT in jobType.split(','):
                tmpJTkey = ':prodSourceLabel%s' % idxJT
                sqlJobType += "%s," % tmpJTkey
                tmpJobTypeMap[tmpJTkey] = tmpJT
                idxJT += 1                
            sqlJobType = sqlJobType[:-1] + ") "
        sql0 += sqlJobType
        # predefined    
        if predefined:
            sql0 += "AND relocationFlag=1 "
        # working group
        tmpGroupMap = {}
        sqlGroups = ''
        if workingGroup != '':
            sqlGroups += "AND workingGroup IN ("
            # loop over all groups
            idxWG = 1
            for tmpWG in workingGroup.split(','):
                tmpWGkey = ':workingGroup%s' % idxWG
                sqlGroups += "%s," % tmpWGkey
                tmpGroupMap[tmpWGkey] = tmpWG
                idxWG += 1                
            sqlGroups = sqlGroups[:-1] + ") "
        # country group    
        if countryGroup != '':
            sqlGroups += "AND countryGroup IN ("
            # loop over all groups
            idxCG = 1
            for tmpCG in countryGroup.split(','):
                tmpCGkey = ':countryGroup%s' % idxCG
                sqlGroups += "%s," % tmpCGkey
                tmpGroupMap[tmpCGkey] = tmpCG
                idxCG += 1
            sqlGroups = sqlGroups[:-1] + ") "
        sql0 += sqlGroups
        # minimum priority
        sqlPrio = ''
        tmpPrioMap = {}
        if minPriority != None:
            sqlPrio = "AND currentPriority>=:minPriority "
            tmpPrioMap[':minPriority'] = minPriority
        sql0 += sqlPrio    
        sql0 += "GROUP BY computingSite,jobStatus"
        sqlA =  "SELECT /*+ index(tab JOBSARCHIVED4_MODTIME_IDX) */ computingSite,jobStatus,COUNT(*) FROM ATLAS_PANDA.jobsArchived4 tab WHERE modificationTime>:modificationTime "
        sqlA += "AND prodSourceLabel IN ("
        sqlA += sqlJobType            
        if predefined:
            sqlA += "AND relocationFlag=1 "
        sqlA += sqlGroups
        sqlA += sqlPrio
        sqlA += "GROUP BY computingSite,jobStatus"
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
        if archived:
            tables.append('ATLAS_PANDA.jobsArchived4')
        # sql for materialized view
        sqlMV = re.sub('COUNT\(\*\)','SUM(num_of_jobs)',sql0)
        sqlMV = re.sub(':minPriority','TRUNC(:minPriority,-1)',sqlMV)
        sqlMV = re.sub('SELECT ','SELECT /*+ RESULT_CACHE */ ',sqlMV)
        ret = {}
        nTry=3
        for iTry in range(nTry):
            try:
                for table in tables:
                    # start transaction
                    self.conn.begin()
                    # select
                    varMap = {}
                    for tmpJobType in tmpJobTypeMap.keys():
                        varMap[tmpJobType] = tmpJobTypeMap[tmpJobType]
                    for tmpGroup in tmpGroupMap.keys():
                        varMap[tmpGroup] = tmpGroupMap[tmpGroup]
                    for tmpPrio in tmpPrioMap.keys():
                        varMap[tmpPrio] = tmpPrioMap[tmpPrio]
                    if table != 'ATLAS_PANDA.jobsArchived4':
                        self.cur.arraysize = 10000
                        if table == 'ATLAS_PANDA.jobsActive4':
                            sqlExeTmp = (sqlMV+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS'
                        else:
                            sqlExeTmp = (sql0+comment) % table
                        _logger.debug("getJobStatistics : %s %s" % (sqlExeTmp,str(varMap)))
                        self.cur.execute(sqlExeTmp, varMap)
                    else:
                        varMap[':modificationTime'] = timeLimit
                        self.cur.arraysize = 10000                        
                        self.cur.execute(sqlA+comment, varMap)
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # create map
                    for item in res:
                        if not ret.has_key(item[0]):
                            ret[item[0]] = {}
                        if not ret[item[0]].has_key(item[1]):
                            ret[item[0]][item[1]] = 0
                        ret[item[0]][item[1]] += item[2]
                # for zero
                stateList = ['assigned','activated','running']
                if archived:
                    stateList += ['finished','failed']
                for site in ret.keys():
                    for state in stateList:
                        if not ret[site].has_key(state):
                            ret[site][state] = 0
                # return
                _logger.debug("getJobStatistics -> %s" % str(ret))
                return ret
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("getJobStatistics() retry : %s" % iTry)
                    time.sleep(2)
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("getJobStatistics : %s %s" % (type, value))
                return {}


    # get job statistics with label
    def getJobStatisticsWithLabel(self,siteStr=''):
        comment = ' /* DBProxy.getJobStatisticsWithLabel */'        
        _logger.debug("getJobStatisticsWithLabel(%s)" % siteStr)
        sql0 = "SELECT computingSite,prodSourceLabel,jobStatus,COUNT(*) FROM %s "
        # site
        tmpSiteMap = {}
        if siteStr != '':
            sql0 += "WHERE computingSite IN ("            
            # loop over all sites
            idxSite = 1
            for tmpSite in siteStr.split(','):
                tmpSiteKey = ':site%s' % idxSite
                sql0 += "%s," % tmpSiteKey
                tmpSiteMap[tmpSiteKey] = tmpSite
                idxSite += 1                
            sql0 = sql0[:-1] + ") "
        sql0 += "GROUP BY computingSite,prodSourceLabel,jobStatus "
        sqlMV = re.sub('COUNT\(\*\)','SUM(num_of_jobs)',sql0)
        sqlMV = re.sub('SELECT ','SELECT /*+ RESULT_CACHE */ ',sqlMV)        
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
        returnMap = {}
        try:
            for table in tables:
                # start transaction
                self.conn.begin()
                # select
                varMap = {}
                self.cur.arraysize = 10000
                if table == 'ATLAS_PANDA.jobsActive4':
                    sqlExeTmp = (sqlMV+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS'
                else:
                    sqlExeTmp = (sql0+comment) % table
                self.cur.execute(sqlExeTmp,tmpSiteMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # create map
                for computingSite,prodSourceLabel,jobStatus,nCount in res:
                    # add site
                    if not returnMap.has_key(computingSite):
                        returnMap[computingSite] = {}
                    # add SourceLabel
                    if not returnMap[computingSite].has_key(prodSourceLabel):
                        returnMap[computingSite][prodSourceLabel] = {}
                    # add jobstatus
                    if not returnMap[computingSite][prodSourceLabel].has_key(jobStatus):
                        returnMap[computingSite][prodSourceLabel][jobStatus] = 0
                    # add    
                    returnMap[computingSite][prodSourceLabel][jobStatus] += nCount
            # return
            _logger.debug("getJobStatisticsWithLabel() : %s" % str(returnMap))
            return returnMap
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getJobStatisticsWithLabel : %s %s" % (errType,errValue))
            return {}


    # get job statistics for brokerage
    def getJobStatisticsBrokerage(self,minPriority=None):
        comment = ' /* DBProxy.getJobStatisticsBrokerage */'        
        _logger.debug("getJobStatisticsBrokerage(%s)" % minPriority)
        sql0 = "SELECT cloud,computingSite,jobStatus,processingType,COUNT(*) FROM %s WHERE "
        sql0 += "prodSourceLabel IN (:prodSourceLabel1) "
        tmpPrioMap = {}
        if minPriority != None:
            sql0 += "AND currentPriority>=:minPriority "
            tmpPrioMap[':minPriority'] = minPriority
        sql0 += "GROUP BY cloud,computingSite,jobStatus,processingType"
        # sql for materialized view
        sqlMV = re.sub('COUNT\(\*\)','SUM(num_of_jobs)',sql0)
        sqlMV = re.sub(':minPriority','TRUNC(:minPriority,-1)',sqlMV)
        sqlMV = re.sub('SELECT ','SELECT /*+ RESULT_CACHE */ ',sqlMV)
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
        ret = {}
        nTry=3
        for iTry in range(nTry):
            try:
                for table in tables:
                    # start transaction
                    self.conn.begin()
                    # select
                    varMap = {}
                    varMap[':prodSourceLabel1'] = 'managed'
                    for tmpPrio in tmpPrioMap.keys():
                        varMap[tmpPrio] = tmpPrioMap[tmpPrio]
                    self.cur.arraysize = 10000
                    if table == 'ATLAS_PANDA.jobsActive4':
                        self.cur.execute((sqlMV+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS', varMap)
                    else:
                        self.cur.execute((sql0+comment) % table, varMap)
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # create map
                    for cloud,computingSite,jobStatus,processingType,count in res:
                        # add cloud
                        if not ret.has_key(cloud):
                            ret[cloud] = {}                            
                        # add site
                        if not ret[cloud].has_key(computingSite):
                            ret[cloud][computingSite] = {}
                        # add processingType
                        if not ret[cloud][computingSite].has_key(processingType):
                            ret[cloud][computingSite][processingType] = {}
                        # add jobStatus
                        if not ret[cloud][computingSite][processingType].has_key(jobStatus):
                            ret[cloud][computingSite][processingType][jobStatus] = count
                # for zero
                for cloud,cloudVal in ret.iteritems():
                    for site,siteVal in cloudVal.iteritems():
                        for pType,typeVal in siteVal.iteritems():
                            for stateItem in ['assigned','activated','running','transferring']:
                                if not typeVal.has_key(stateItem):
                                    typeVal[stateItem] = 0
                # return
                _logger.debug("getJobStatisticsBrokerage -> %s" % str(ret))
                return ret
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("getJobStatisticsBrokerage retry : %s" % iTry)
                    time.sleep(2)
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("getJobStatisticsBrokerage : %s %s" % (type, value))
                return {}


    # get job statistics for analysis brokerage
    def getJobStatisticsAnalBrokerage(self,minPriority=None):
        comment = ' /* DBProxy.getJobStatisticsAnalBrokerage */'        
        _logger.debug("getJobStatisticsAnalBrokerage(%s)" % minPriority)
        sql0 = "SELECT computingSite,jobStatus,processingType,COUNT(*) FROM %s WHERE "
        sql0 += "prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
        if minPriority != None:
            sql0 += "AND currentPriority>=:minPriority "
        sql0 += "GROUP BY cloud,computingSite,jobStatus,processingType"
        # sql for materialized view
        sqlMV = re.sub('COUNT\(\*\)','SUM(num_of_jobs)',sql0)
        sqlMV = re.sub(':minPriority','TRUNC(:minPriority,-1)',sqlMV)
        sqlMV = re.sub('SELECT ','SELECT /*+ RESULT_CACHE */ ',sqlMV)
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
        ret = {}
        nTry=3
        for iTry in range(nTry):
            try:
                for table in tables:
                    # start transaction
                    self.conn.begin()
                    # select
                    varMap = {}
                    varMap[':prodSourceLabel1'] = 'user'
                    varMap[':prodSourceLabel2'] = 'panda'
                    if minPriority != None:
                        varMap[':minPriority'] = minPriority
                    self.cur.arraysize = 10000
                    if table == 'ATLAS_PANDA.jobsActive4':
                        self.cur.execute((sqlMV+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS', varMap)
                    else:
                        self.cur.execute((sql0+comment) % table, varMap)
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # create map
                    for computingSite,jobStatus,processingType,count in res:
                        # add site
                        if not ret.has_key(computingSite):
                            ret[computingSite] = {}
                        # add processingType
                        if not ret[computingSite].has_key(processingType):
                            ret[computingSite][processingType] = {}
                        # add jobStatus
                        if not ret[computingSite][processingType].has_key(jobStatus):
                            ret[computingSite][processingType][jobStatus] = count
                # for zero
                for site,siteVal in ret.iteritems():
                    for pType,typeVal in siteVal.iteritems():
                        for stateItem in ['assigned','activated','running']:
                            if not typeVal.has_key(stateItem):
                                typeVal[stateItem] = 0
                # return
                _logger.debug("getJobStatisticsAnalBrokerage -> %s" % str(ret))                
                return ret
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.debug("getJobStatisticsAnalBrokerage retry : %s" % iTry)
                    time.sleep(2)
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("getJobStatisticsAnalBrokerage : %s %s" % (type, value))
                return {}


    # get highest prio jobs
    def getHighestPrioJobStat(self):
        comment = ' /* DBProxy.getHighestPrioJobStat */'        
        _logger.debug("getHighestPrioJobStat()")
        sql0  = "SELECT cloud,max(currentPriority) FROM %s WHERE "
        sql0 += "prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) GROUP BY cloud"
        sqlC  = "SELECT COUNT(*) FROM %s WHERE "
        sqlC += "prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) AND "
        sqlC += "cloud=:cloud AND currentPriority=:currentPriority"
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
        ret = {}
        try:
            for table in tables:
                # start transaction
                self.conn.begin()
                # select
                varMap = {}
                varMap[':prodSourceLabel'] = 'managed'
                if table == 'ATLAS_PANDA.jobsActive4':
                    varMap[':jobStatus1'] = 'activated'
                    varMap[':jobStatus2'] = 'dummy'
                else:
                    varMap[':jobStatus1'] = 'defined'
                    varMap[':jobStatus2'] = 'assigned'
                self.cur.arraysize = 100
                _logger.debug((sql0+comment) % table)
                self.cur.execute((sql0+comment) % table, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # create map
                for cloud,maxPriority in res:
                    # add cloud
                    if not ret.has_key(cloud):
                        ret[cloud] = {}
                    # add max priority
                    prioKey = 'highestPrio'
                    nNotRunKey = 'nNotRun'
                    getNumber = False
                    if not ret[cloud].has_key(prioKey):
                        ret[cloud][prioKey] = maxPriority
                        ret[cloud][nNotRunKey] = 0
                        getNumber = True
                    else:
                        # use highest one
                        if ret[cloud][prioKey] < maxPriority:
                            ret[cloud][prioKey] = maxPriority
                            # reset
                            ret[cloud][nNotRunKey] = 0
                            getNumber = True
                        elif ret[cloud][prioKey] == maxPriority:
                            getNumber = True
                    # get number of jobs with highest prio
                    if getNumber:
                        varMap[':cloud'] = cloud
                        varMap[':currentPriority'] = maxPriority
                        self.cur.arraysize = 10
                        _logger.debug((sqlC+comment) % table)                        
                        self.cur.execute((sqlC+comment) % table, varMap)
                        resC = self.cur.fetchone()
                        # commit
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        ret[cloud][nNotRunKey] += resC[0]
            # return
            return ret
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getHighestPrioJobStat : %s %s" % (type, value))
            return {}


    # get queued analysis jobs at a site
    def getQueuedAnalJobs(self,site,dn):
        comment = ' /* DBProxy.getQueuedAnalJobs */'        
        _logger.debug("getQueuedAnalJobs(%s,%s)" % (site,dn))
        sql0 = "SELECT COUNT(*),jobStatus FROM %s WHERE "
        sql0 += "prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) "
        sql0 += "AND computingSite=:computingSite AND prodUserName != :prodUserName "
        sql0 += "GROUP BY jobStatus "
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            nQueued  = 0
            nRunning = 0
            # loop over all tables    
            for table in tables:
                # start transaction
                self.conn.begin()
                # select
                varMap = {}
                varMap[':prodSourceLabel'] = 'user'
                varMap[':computingSite']   = site
                varMap[':prodUserName']    = compactDN
                if table == 'ATLAS_PANDA.jobsActive4':
                    varMap[':jobStatus1'] = 'activated'
                    varMap[':jobStatus2'] = 'running'
                else:
                    varMap[':jobStatus1'] = 'defined'
                    varMap[':jobStatus2'] = 'assigned'
                self.cur.arraysize = 10
                self.cur.execute((sql0+comment) % table, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # sum
                for cnt,jobStatus in res:
                    if jobStatus == 'running':
                        nRunning += cnt
                    else:
                        nQueued += cnt
            # return
            return {'queued':nQueued, 'running':nRunning} 
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getQueuedAnalJobs : %s %s" % (errType,errValue))
            return {}

            
    # get computingSite and destinationSE for a dataset
    def getDestSE(self,dsname):
        comment = ' /* DBProxy.getDestSE */'        
        _logger.debug("getDestSE(%s)" % dsname)
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND status=:status AND rownum=1"
        sql1 = "SELECT computingSite,destinationSE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
        try:
            # start transaction
            self.conn.begin()
            # select
            varMap = {}
            varMap[':status'] = 'transferring'
            varMap[':destinationDBlock'] = dsname            
            self.cur.arraysize = 10
            self.cur.execute(sql0+comment, varMap)
            res = self.cur.fetchall()
            # get PandaID
            pandaID = None
            if len(res) != 0:
                pandaID = res[0][0]
            # get computingSite and destinationSE
            destSE = None,None
            if pandaID != None:
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.execute(sql1+comment, varMap)
                res = self.cur.fetchall()
                if len(res) != 0:
                    destSE = res[0]
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            _logger.debug("getDestSE(%s) : %s" % (dsname,str(destSE)))
            return destSE
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getDestSE : %s %s" % (type, value))
            return None,None

        
    # get destinationDBlockToken for a dataset
    def getDestTokens(self,dsname):
        comment = ' /* DBProxy.getDestTokens */'        
        _logger.debug("getDestTokens(%s)" % dsname)
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ destinationDBlockToken FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND rownum=1"
        try:
            # start transaction
            self.conn.begin()
            # select
            varMap = {}
            varMap[':destinationDBlock'] = dsname
            self.cur.arraysize = 10            
            self.cur.execute(sql0+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # create map
            retToken = None
            if len(res) != 0:
                retToken = res[0][0]
                # convert None to NULL
                if retToken == None:
                    retToken = 'NULL'
            # return
            _logger.debug("getDestTokens(%s) : %s" % (dsname,retToken))
            return retToken
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getDestTokens : %s %s" % (type, value))
            return None


    # get the number of job for a user
    def getNumberJobsUser(self,dn,workingGroup=None):
        comment = ' /* DBProxy.getNumberJobsUser */'        
        _logger.debug("getNumberJobsUsers(%s,%s)" % (dn,workingGroup))
        # get compact DN
        compactDN = self.cleanUserID(dn)
        if compactDN in ['','NULL',None]:
            compactDN = dn
        if workingGroup != None:    
            sql0 = "SELECT COUNT(*) FROM %s WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLabel AND workingGroup=:workingGroup"
        else:
            sql0 = "SELECT COUNT(*) FROM %s WHERE prodUserName=:prodUserName AND prodSourceLabel=:prodSourceLabel AND workingGroup IS NULL"
        nTry = 1
        nJob = 0
        for iTry in range(nTry):
            try:
                for table in ('ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4'):
                    # start transaction
                    self.conn.begin()
                    # select
                    varMap = {}
                    varMap[':prodUserName'] = compactDN
                    varMap[':prodSourceLabel'] = 'user'
                    if workingGroup != None:
                        varMap[':workingGroup'] = workingGroup
                    self.cur.arraysize = 10
                    self.cur.execute((sql0+comment) % table, varMap)
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # create map
                    if len(res) != 0:
                        nJob += res[0][0]
                # return
                _logger.debug("getNumberJobsUsers(%s) : %s" % (dn,nJob))
                return nJob
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    time.sleep(2)
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("getNumberJobsUsers : %s %s" % (type, value))
                return 0


    # get job statistics for ExtIF
    def getJobStatisticsForExtIF(self,sourcetype=None):
        comment = ' /* DBProxy.getJobStatisticsForExtIF */'                
        _logger.debug("getJobStatisticsForExtIF()")
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        if sourcetype == 'analysis':
            sql0 = "SELECT jobStatus,COUNT(*),cloud FROM %s WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) GROUP BY jobStatus,cloud"
            sqlA = "SELECT /*+ index(tab JOBSARCHIVED4_MODTIME_IDX) */ jobStatus,COUNT(*),cloud FROM %s tab WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
        else:
            sql0 = "SELECT jobStatus,COUNT(*),cloud FROM %s WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) GROUP BY jobStatus,cloud"
            sqlA = "SELECT /*+ index(tab JOBSARCHIVED4_MODTIME_IDX) */ jobStatus,COUNT(*),cloud FROM %s tab WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
        sqlA+= "AND modificationTime>:modificationTime GROUP BY jobStatus,cloud"
        # sql for materialized view
        sqlMV = re.sub('COUNT\(\*\)','SUM(num_of_jobs)',sql0)
        sqlMV = re.sub('SELECT ','SELECT /*+ RESULT_CACHE */ ',sqlMV)
        ret = {}
        try:
            for table in ('ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsArchived4','ATLAS_PANDA.jobsDefined4'):
                # start transaction
                self.conn.begin()
                # select
                varMap = {}
                if sourcetype == 'analysis':
                    varMap[':prodSourceLabel1'] = 'user'
                    varMap[':prodSourceLabel2'] = 'panda'
                else:
                    varMap[':prodSourceLabel1'] = 'managed'
                    varMap[':prodSourceLabel2'] = 'rc_test'                    
                if table != 'ATLAS_PANDA.jobsArchived4':
                    self.cur.arraysize = 10000
                    if table == 'ATLAS_PANDA.jobsActive4':
                        self.cur.execute((sqlMV+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS', varMap)
                    else:
                        self.cur.execute((sql0+comment) % table, varMap)                        
                else:
                    varMap[':modificationTime'] = timeLimit
                    self.cur.arraysize = 10000                    
                    self.cur.execute((sqlA+comment) % table, varMap)                    
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # change NULL to US for old jobs
                newRes = []
                usMap = {}
                for jobStatus,count,cloud in res:
                    if not cloud in ['US','NULL']:
                        # append since no conversion is required
                        newRes.append((jobStatus,count,cloud))
                    else:
                        # sum
                        if not usMap.has_key(jobStatus):
                            usMap[jobStatus] = 0
                        usMap[jobStatus] += count
                # append US counts
                for jobStatus,count in usMap.iteritems():
                    newRes.append((jobStatus,count,'US'))
                # create map
                for item in newRes:
                    # add cloud
                    if not ret.has_key(item[2]):
                        ret[item[2]] = {}
                    # this is needed for auto_increment of InnoDB
                    if not ret[item[2]].has_key(item[0]):
                        ret[item[2]][item[0]] = item[1]
            # return
            _logger.debug("getJobStatisticsForExtIF -> %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobStatisticsForExtIF : %s %s" % (type, value))
            return {}


    # get job statistics per processingType
    def getJobStatisticsPerProcessingType(self):
        comment = ' /* DBProxy.getJobStatisticsPerProcessingType */'                
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        _logger.debug("getJobStatisticsPerProcessingType()")
        sqlN  = "SELECT jobStatus,COUNT(*),cloud,processingType FROM %s "
        sqlN += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) GROUP BY jobStatus,cloud,processingType"
        sqlA  = "SELECT /*+ index(tab JOBSARCHIVED4_MODTIME_IDX) */ jobStatus,COUNT(*),cloud,processingType FROM %s tab "
        sqlA += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND modificationTime>:modificationTime GROUP BY jobStatus,cloud,processingType"
        # sql for materialized view
        sqlMV = re.sub('COUNT\(\*\)','SUM(num_of_jobs)',sqlN)
        sqlMV = re.sub('SELECT ','SELECT /*+ RESULT_CACHE */ ',sqlMV)
        ret = {}
        try:
            for table in ('ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsArchived4','ATLAS_PANDA.jobsDefined4'):
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                # select
                varMap = {}
                varMap[':prodSourceLabel1'] = 'managed'
                varMap[':prodSourceLabel2'] = 'rc_test'                
                if table == 'ATLAS_PANDA.jobsArchived4':
                    varMap[':modificationTime'] = timeLimit
                    self.cur.execute((sqlA+comment) % table, varMap)
                else:
                    if table == 'ATLAS_PANDA.jobsActive4': 
                        self.cur.execute((sqlMV+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS', varMap)
                    else:
                        self.cur.execute((sqlN+comment) % table, varMap)                        
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # create map
                for jobStatus,count,cloud,processingType in res:
                    # add cloud
                    if not ret.has_key(cloud):
                        ret[cloud] = {}
                    # add processingType
                    if not ret[cloud].has_key(processingType):
                        ret[cloud][processingType] = {}
                    # this is needed for auto_increment of InnoDB
                    if not ret[cloud][processingType].has_key(jobStatus):
                        ret[cloud][processingType][jobStatus] = count
            # return
            _logger.debug("getJobStatisticsPerProcessingType -> %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobStatisticsPerProcessingType : %s %s" % (type, value))
            return {}


    # get the number of waiting jobs per site and user
    def getJobStatisticsPerUserSite(self):
        comment = ' /* DBProxy.getJobStatisticsPerUserSite */'                
        _logger.debug("getJobStatisticsPerUserSite()")
        sqlN  = "SELECT COUNT(*),prodUserID,computingSite FROM %s "
        sqlN += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND jobStatus=:jobStatus GROUP BY prodUserID,computingSite"
        ret = {}
        try:
            for table in ('ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4'):
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 100000
                # select
                if table == 'ATLAS_PANDA.jobsActive4':
                    jobStatus = 'activated'
                else:
                    jobStatus = 'assigned'
                varMap = {}
                varMap[':prodSourceLabel1'] = 'user'
                varMap[':prodSourceLabel2'] = 'panda'
                varMap[':jobStatus'] = jobStatus
                self.cur.execute((sqlN+comment) % table, varMap)                        
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # create map
                for cnt,prodUserName,computingSite in res:
                    # add site
                    if not ret.has_key(computingSite):
                        ret[computingSite] = {}
                    # add user
                    if not ret[computingSite].has_key(prodUserName):
                        ret[computingSite][prodUserName] = {'assigned':0,'activated':0}
                    # add info
                    ret[computingSite][prodUserName][jobStatus] = cnt
            # return
            _logger.debug("getJobStatisticsPerUserSite -> %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getJobStatisticsPerUserSite : %s %s" % (errtype,errvalue))
            return {}

        
    # get number of analysis jobs per user
    def getNUserJobs(self,siteName,nJobs):
        comment = ' /* DBProxy.getNUserJobs */'        
        _logger.debug("getNUserJobs(%s)" % siteName)
        sql0  = "SELECT * FROM (SELECT prodUserID FROM ATLAS_PANDA.jobsActive4 "
        sql0 += "WHERE jobStatus=:jobStatus AND prodSourceLabel in (:prodSourceLabel1,:prodSourceLabel2) "
        sql0 += "AND computingSite=:computingSite ORDER BY currentPriority DESC) WHERE rownum<=:nJobs"
        varMap = {}
        varMap[':computingSite'] = siteName
        varMap[':nJobs'] = nJobs
        varMap[':jobStatus'] = 'activated'
        varMap[':prodSourceLabel1'] = 'user'
        varMap[':prodSourceLabel2'] = 'panda'
        ret = {}
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10000            
            self.cur.execute(sql0+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # create map
            for prodUserID, in res:
                if not ret.has_key(prodUserID):
                    ret[prodUserID] = 0
                ret[prodUserID] += 1
            # return
            _logger.debug("getNUserJobs() : %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getNUserJobs : %s %s" % (type, value))
            return {}

        
    # get number of activated analysis jobs
    def getNAnalysisJobs(self,nProcesses):
        comment = ' /* DBProxy.getNAnalysisJobs */'        
        _logger.debug("getNAnalysisJobs(%s)" % nProcesses)
        sql0 =  "SELECT computingSite,COUNT(*) FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus "
        sql0 += "AND (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2) GROUP BY computingSite"
        varMap = {}
        varMap[':jobStatus'] = 'activated' 
        varMap[':prodSourceLabel1'] = 'user'
        varMap[':prodSourceLabel2'] = 'panda'
        ret = {}
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10000            
            self.cur.execute(sql0+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # create map
            for item in res:
                ret[item[0]] = float(item[1])/nProcesses
            # return
            _logger.debug("getNAnalysisJobs() : %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getNAnalysisJobs : %s %s" % (type, value))
            return {}


    # generate pilot token
    def genPilotToken(self,schedulerhost,scheduleruser,schedulerid):
        comment = ' /* DBProxy.genPilotToken */'                    
        try:
            _logger.debug("genPilotToken(%s,%s,%s)" % (schedulerhost,scheduleruser,schedulerid))
            token = commands.getoutput('uuidgen')
            timeNow = datetime.datetime.utcnow()
            timeExp = timeNow + datetime.timedelta(days=4)
            sql  = "INSERT INTO ATLAS_PANDA.pilottoken (token,schedulerhost,scheduleruser,schedulerid,created,expires) "
            sql += "VALUES (:token,:schedulerhost,:scheduleruser,:schedulerid,:created,:expires)"
            # start transaction
            self.conn.begin()
            # execute
            varMap = {':token':token,':schedulerhost':schedulerhost,':scheduleruser':scheduleruser,
                      ':schedulerid':schedulerid,':created':timeNow,':expires':timeExp}
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            retVal = "token=%s,created=%s,expires=%s" % (token,timeNow.strftime('%Y-%m-%d %H:%M:%S'),
                                                         timeExp.strftime('%Y-%m-%d %H:%M:%S'))
            _logger.debug("genPilotToken -> %s" % retVal)
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("genPilotToken : %s %s" % (type, value))
            return None

        
    # get list of scheduler users
    def getListSchedUsers(self):
        comment = ' /* DBProxy.getListSchedUsers */'                    
        try:
            _logger.debug("getListSchedUsers")
            sql  = "SELECT token,scheduleruser FROM ATLAS_PANDA.pilottoken WHERE expires>CURRENT_DATE"
            # start transaction
            self.conn.begin()
            # execute
            self.cur.arraysize = 100
            self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            retVal = {}
            for token,scheduleruser in res:
                retVal[token] = scheduleruser
            _logger.debug("getListSchedUsers->%s" % str(retVal))
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getListSchedUsers : %s %s" % (type, value))
            return {}


    ###########################################################################
    #        
    # LogDBProxy stuff   

    # update site data
    def updateSiteData(self,hostID,pilotRequests):
        comment = ' /* DBProxy.updateSiteData */'                            
        _logger.debug("updateSiteData start")
        sqlDel =  "DELETE FROM ATLAS_PANDAMETA.SiteData WHERE HOURS=:HOURS AND LASTMOD<:LASTMOD"
        sqlCh  =  "SELECT count(*) FROM ATLAS_PANDAMETA.SiteData WHERE FLAG=:FLAG AND HOURS=:HOURS AND SITE=:SITE"
        sqlIn  =  "INSERT INTO ATLAS_PANDAMETA.SiteData (SITE,FLAG,HOURS,GETJOB,UPDATEJOB,LASTMOD,"
        sqlIn  += "NSTART,FINISHED,FAILED,DEFINED,ASSIGNED,WAITING,ACTIVATED,HOLDING,RUNNING,TRANSFERRING) "
        sqlIn  += "VALUES (:SITE,:FLAG,:HOURS,:GETJOB,:UPDATEJOB,CURRENT_DATE,"
        sqlIn  += "0,0,0,0,0,0,0,0,0,0)"
        sqlUp  =  "UPDATE ATLAS_PANDAMETA.SiteData SET GETJOB=:GETJOB,UPDATEJOB=:UPDATEJOB,LASTMOD=CURRENT_DATE "
        sqlUp  += "WHERE FLAG=:FLAG AND HOURS=:HOURS AND SITE=:SITE"
        sqlAll  = "SELECT getJob,updateJob,FLAG FROM ATLAS_PANDAMETA.SiteData WHERE HOURS=:HOURS AND SITE=:SITE"
        try:
            # delete old records
            varMap = {}
            varMap[':HOURS'] = 3
            varMap[':LASTMOD'] = datetime.datetime.utcnow()-datetime.timedelta(hours=varMap[':HOURS'])
            self.conn.begin()
            self.cur.execute(sqlDel+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # shuffle to avoid concatenation
            tmpSiteList = pilotRequests.keys()
            random.shuffle(tmpSiteList)
            # loop over all sites
            for tmpSite in tmpSiteList:
                tmpVal = pilotRequests[tmpSite]
                # start transaction
                self.conn.begin()
                # check individual host info first
                varMap = {}
                varMap[':FLAG']  = hostID
                varMap[':SITE']  = tmpSite
                varMap[':HOURS'] = 3                
                self.cur.arraysize = 10
                self.cur.execute(sqlCh+comment,varMap)
                res = self.cur.fetchone()
                # row exists or not
                if res[0] == 0:
                    sql = sqlIn
                else:
                    sql = sqlUp
                if tmpVal.has_key('getJob'):    
                    varMap[':GETJOB'] = len(tmpVal['getJob'])
                else:
                    varMap[':GETJOB'] = 0
                if tmpVal.has_key('updateJob'):    
                    varMap[':UPDATEJOB'] = len(tmpVal['updateJob'])
                else:
                    varMap[':UPDATEJOB'] = 0
                # update    
                self.cur.execute(sql+comment,varMap)
                # get all info
                sumExist = False
                varMap = {}
                varMap[':SITE']  = tmpSite
                varMap[':HOURS'] = 3
                self.cur.arraysize = 100
                self.cur.execute(sqlAll+comment,varMap)
                res = self.cur.fetchall()
                # get total getJob/updateJob
                varMap[':GETJOB'] = 0
                varMap[':UPDATEJOB'] = 0
                nCol = 0
                for tmpGetJob,tmpUpdateJob,tmpFlag in res:
                    # don't use summed info 
                    if tmpFlag == 'production':
                        sumExist = True
                        continue
                    if tmpFlag == 'analysis':
                        if tmpSite.startswith('ANALY_'):
                            sumExist = True
                        continue
                    if tmpFlag in ['test']:
                        continue
                    # sum
                    varMap[':GETJOB'] += tmpGetJob
                    varMap[':UPDATEJOB'] += tmpUpdateJob
                    nCol += 1
                # get average
                if nCol != 0:
                    if varMap[':GETJOB'] >= nCol:
                        varMap[':GETJOB'] /= nCol
                    if varMap[':UPDATEJOB'] >= nCol:    
                        varMap[':UPDATEJOB'] /= nCol
                if tmpSite.startswith('ANALY_'):    
                    varMap[':FLAG']  = 'analysis'
                else:
                    varMap[':FLAG']  = 'production'
                # row exists or not
                if sumExist:
                    sql = sqlUp
                else:
                    sql = sqlIn
                # update
                self.cur.execute(sql+comment,varMap)
                _logger.debug('updateSiteData : %s getJob=%s updateJob=%s' % \
                              (tmpSite,varMap[':GETJOB'],varMap[':UPDATEJOB']))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            _logger.debug("updateSiteData done")
            return True
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("updateSiteData : %s %s" % (type,value))
            return False
        
        
    # get site data
    def getCurrentSiteData(self):
        comment = ' /* DBProxy.getCurrentSiteData */'
        _logger.debug("getCurrentSiteData")
        sql = "SELECT SITE,getJob,updateJob,FLAG FROM ATLAS_PANDAMETA.SiteData WHERE FLAG IN (:FLAG1,:FLAG2) and HOURS=3"
        varMap = {}
        varMap[':FLAG1'] = 'production'
        varMap[':FLAG2'] = 'analysis'        
        try:
            # set autocommit on
            self.conn.begin()
            # select
            self.cur.arraysize = 10000
            self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            ret = {}
            for site,getJob,updateJob,flag in res:
                if site.startswith('ANALY_'):
                    if flag != 'analysis':
                        continue
                else:
                    if flag != 'production':
                        continue
                ret[site] = {'getJob':getJob,'updateJob':updateJob}
            return ret
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getCurrentSiteData : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}


    # insert nRunning in site data
    def insertnRunningInSiteData(self):
        comment = ' /* DBProxy.insertnRunningInSiteData */'                            
        _logger.debug("insertnRunningInSiteData start")
        sqlDel =  "DELETE FROM ATLAS_PANDAMETA.SiteData WHERE FLAG IN (:FLAG1,:FLAG2) AND LASTMOD<CURRENT_DATE-1"
        sqlRun =  "SELECT COUNT(*),computingSite FROM ATLAS_PANDA.jobsActive4 "
        sqlRun += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
        sqlRun += "AND jobStatus=:jobStatus GROUP BY computingSite"
        sqlCh  =  "SELECT COUNT(*) FROM ATLAS_PANDAMETA.SiteData WHERE FLAG=:FLAG AND HOURS=:HOURS AND SITE=:SITE"
        sqlIn  =  "INSERT INTO ATLAS_PANDAMETA.SiteData (SITE,FLAG,HOURS,GETJOB,UPDATEJOB,LASTMOD,"
        sqlIn  += "NSTART,FINISHED,FAILED,DEFINED,ASSIGNED,WAITING,ACTIVATED,HOLDING,RUNNING,TRANSFERRING) "
        sqlIn  += "VALUES (:SITE,:FLAG,:HOURS,0,0,CURRENT_DATE,"
        sqlIn  += "0,0,0,0,0,0,0,0,:RUNNING,0)"
        sqlUp  =  "UPDATE ATLAS_PANDAMETA.SiteData SET RUNNING=:RUNNING,LASTMOD=CURRENT_DATE "
        sqlUp  += "WHERE FLAG=:FLAG AND HOURS=:HOURS AND SITE=:SITE"
        sqlMax =  "SELECT SITE,MAX(RUNNING) FROM ATLAS_PANDAMETA.SiteData "
        sqlMax += "WHERE FLAG=:FLAG GROUP BY SITE"
        try:
            # use offset(1000)+minites for :HOURS
            timeNow = datetime.datetime.utcnow()
            nHours = 1000 + timeNow.hour*60 + timeNow.minute
            # delete old records
            varMap = {}
            varMap[':FLAG1'] = 'max'
            varMap[':FLAG2'] = 'snapshot'            
            self.conn.begin()
            self.cur.execute(sqlDel+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # get nRunning
            varMap = {}
            varMap[':jobStatus'] = 'running' 
            varMap[':prodSourceLabel1'] = 'user'
            varMap[':prodSourceLabel2'] = 'panda'
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlRun+comment,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all sites 
            for nRunning,computingSite in res:
                # only ANALY_ sites
                if not computingSite.startswith('ANALY_'):
                    continue
                # check if the row is already there
                varMap = {}
                varMap[':FLAG']  = 'snapshot'
                varMap[':SITE']  = computingSite
                varMap[':HOURS'] = nHours
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 10
                self.cur.execute(sqlCh+comment,varMap)
                res = self.cur.fetchone()
                # row exists or not
                if res[0] == 0:
                    sql = sqlIn
                else:
                    sql = sqlUp
                # set current nRunning
                varMap = {}
                varMap[':FLAG']    = 'snapshot'
                varMap[':SITE']    = computingSite
                varMap[':HOURS']   = nHours
                varMap[':RUNNING'] = nRunning
                # insert or update
                self.cur.execute(sql+comment,varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # get max nRunning
            varMap = {}
            varMap[':FLAG']  = 'snapshot'
            self.conn.begin()
            self.cur.arraysize = 10000
            self.cur.execute(sqlMax+comment,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all sites 
            for computingSite,maxnRunning in res:
                # start transaction
                self.conn.begin()
                # check if the row is already there
                varMap = {}
                varMap[':FLAG']  = 'max'
                varMap[':SITE']  = computingSite
                varMap[':HOURS'] = 0
                self.cur.arraysize = 10
                self.cur.execute(sqlCh+comment,varMap)
                res = self.cur.fetchone()
                # row exists or not
                if res[0] == 0:
                    sql = sqlIn
                else:
                    sql = sqlUp
                # set max nRunning
                varMap = {}
                varMap[':FLAG']  = 'max'
                varMap[':SITE']  = computingSite
                varMap[':HOURS'] = 0                
                varMap[':RUNNING'] = maxnRunning
                self.cur.execute(sql+comment,varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            _logger.debug("insertnRunningInSiteData done")
            return True
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("insertnRunningInSiteData : %s %s" % (type,value))
            return False


    # get nRunning in site data
    def getnRunningInSiteData(self):
        comment = ' /* DBProxy.getnRunningInSiteData */'                            
        _logger.debug("getnRunningInSiteData start")
        sqlMax =  "SELECT SITE,RUNNING FROM ATLAS_PANDAMETA.SiteData WHERE HOURS=:HOURS AND FLAG=:FLAG"
        try:
            # get nRunning
            varMap = {}
            varMap[':FLAG']  = 'max'
            varMap[':HOURS'] = 0                
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # get
            self.cur.execute(sqlMax+comment,varMap)
            self.cur.execute(sqlMax+comment,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all sites
            retMap = {}
            for computingSite,maxnRunning in res:
                retMap[computingSite] = maxnRunning
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("getnRunningInSiteData done")
            return retMap
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getnRunningInSiteData : %s %s" % (type,value))
            return {}


    # get list of site
    def getSiteList(self):
        _logger.debug("getSiteList start")
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT siteid,nickname FROM ATLAS_PANDAMETA.schedconfig WHERE siteid IS NOT NULL"
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
        comment = ' /* DBProxy.getSiteInfo */'
        _logger.debug("getSiteInfo start")
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT nickname,dq2url,cloud,ddm,lfchost,se,gatekeeper,releases,memory,"
            sql+= "maxtime,status,space,retry,cmtconfig,setokens,seprodpath,glexec,"
            sql+= "priorityoffset,allowedgroups,defaulttoken,siteid,queue,localqueue,"
            sql+= "validatedreleases,accesscontrol,copysetup,maxinputsize,cachedse,"
            sql+= "allowdirectaccess,comment_,lastmod,multicloud,lfcregister,"
            sql+= "countryGroup,availableCPU,pledgedCPU "
            sql+= "FROM ATLAS_PANDAMETA.schedconfig WHERE siteid IS NOT NULL"
            self.cur.arraysize = 10000            
            self.cur.execute(sql+comment)
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
                       priorityoffset,allowedgroups,defaulttoken,siteid,queue,localqueue,\
                       validatedreleases,accesscontrol,copysetup,maxinputsize,cachedse,\
                       allowdirectaccess,comment,lastmod,multicloud,lfcregister, \
                       countryGroup,availableCPU,pledgedCPU \
                       = resTmp
                    # skip invalid siteid
                    if siteid in [None,'']:
                        continue
                    # instantiate SiteSpec
                    ret = SiteSpec.SiteSpec()
                    ret.sitename   = siteid
                    ret.nickname   = nickname
                    ret.dq2url     = dq2url
                    ret.cloud      = cloud.split(',')[0]
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
                    ret.cachedse   = cachedse
                    ret.accesscontrol = accesscontrol
                    ret.copysetup     = copysetup
                    ret.maxinputsize  = maxinputsize
                    ret.comment       = comment
                    ret.statusmodtime = lastmod
                    ret.lfcregister   = lfcregister
                    # contry groups
                    if not countryGroup in ['',None]:
                        ret.countryGroup = countryGroup.split(',')
                    else:
                        ret.countryGroup = []
                    # available CPUs
                    ret.availableCPU = 0
                    if not availableCPU in ['',None]:
                        try:
                            ret.availableCPU = int(availableCPU)
                        except:
                            pass
                    # pledged CPUs
                    ret.pledgedCPU = 0
                    if not pledgedCPU in ['',None]:
                        try:
                            ret.pledgedCPU = int(pledgedCPU)
                        except:
                            pass
                    # cloud list
                    if cloud != '':
                        ret.cloudlist = [cloud.split(',')[0]]
                        if not multicloud in ['',None,'None']:
                            ret.cloudlist += multicloud.split(',')
                    else:
                        ret.cloudlist = []
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
                    if cmtconfig in ['x86_64-slc5-gcc43']:
                        # set empty for slc5-gcc43 validation
                        ret.cmtconfig = [] # FIXME
                    elif cmtconfig in ['i686-slc5-gcc43-opt']:
                        # set slc4 for slc5 to get slc4 jobs too
                        ret.cmtconfig = ['i686-slc4-gcc34-opt']
                    else:
                        # set slc3 if the column is empty
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
                    # direct access
                    if allowdirectaccess == 'True':
                        ret.allowdirectaccess = True
                    else:
                        ret.allowdirectaccess = False
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


    # check if countryGroup is used for beyond-pledge
    def checkCountryGroupForBeyondPledge(self,siteName):
        comment = ' /* DBProxy.checkCountryGroupForBeyondPledge */'        
        _logger.debug("checkCountryGroupForBeyondPledge %s" % siteName)
        try:
            # counties for beyond-pledge
            countryForBeyondPledge = self.beyondPledgeRatio[siteName]['countryGroup'].split(',')
            # set autocommit on
            self.conn.begin()
            # check if countryGroup has activated jobs
            varMap = {}
            varMap[':jobStatus'] = 'activated'
            varMap[':computingSite'] = siteName
            varMap[':prodSourceLabel1'] = 'user'
            varMap[':prodSourceLabel2'] = 'panda'
            sql  = "SELECT /*+ INDEX_COMBINE(tab JOBSACTIVE4_JOBSTATUS_IDX JOBSACTIVE4_COMPSITE_IDX) */ PandaID "
            sql += "FROM ATLAS_PANDA.jobsActive4 tab WHERE jobStatus=:jobStatus "
            sql += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
            sql += "AND computingSite=:computingSite "
            sql += "AND countryGroup IN ("
            for idxCountry,tmptmpCountry in enumerate(countryForBeyondPledge):
                tmpKey = ":countryGroup%s" % idxCountry
                sql += "%s," % tmpKey
                varMap[tmpKey] = tmptmpCountry
            sql = sql[:-1]    
            sql += ") AND rownum <= 1" 
            self.cur.arraysize = 10                        
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchall()
            # no activated jobs
            if res == None or len(res) == 0:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                _logger.debug("checkCountryGroupForBeyondPledge : ret=False - no activated")
                return False
            # get ratio
            varMap = {}
            varMap[':jobStatus1'] = 'running'
            varMap[':jobStatus2'] = 'sent'            
            varMap[':computingSite'] = siteName
            varMap[':prodSourceLabel1'] = 'user'
            varMap[':prodSourceLabel2'] = 'panda'
            sql  = "SELECT /*+ INDEX_COMBINE(tab JOBSACTIVE4_JOBSTATUS_IDX JOBSACTIVE4_COMPSITE_IDX) */ COUNT(*),countryGroup "            
            sql += "FROM ATLAS_PANDA.jobsActive4 tab WHERE jobStatus IN (:jobStatus1,:jobStatus2) "
            sql += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
            sql += "AND computingSite=:computingSite "
            sql += "GROUP BY countryGroup "
            self.cur.arraysize = 1000
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            beyondCount = 0
            totalCount  = 0
            if res != None and len(res) != 0:
                for cnt,countryGroup in res:
                    totalCount += cnt
                    # counties for beyond pledge 
                    if countryGroup in countryForBeyondPledge:
                        beyondCount += cnt
            # no running
            if totalCount == 0:
                _logger.debug("checkCountryGroupForBeyondPledge : ret=False - no running")
                return False
            # check ratio
            ratioVal = float(beyondCount)/float(totalCount)
            if ratioVal < self.beyondPledgeRatio[siteName]['ratio']:
                retVal = True
            else:
                retVal = False
            _logger.debug("checkCountryGroupForBeyondPledge : ret=%s - %s/total=%s/%s=%s thr=%s" % \
                          (retVal,self.beyondPledgeRatio[siteName]['countryGroup'],beyondCount,totalCount,
                           ratioVal,self.beyondPledgeRatio[siteName]['ratio'])) 
            return retVal
        except:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("checkCountryGroupForBeyondPledge : %s %s" % (errtype,errvalue))
            # roll back
            self._rollback()
            return


    # get selection criteria for share of production activities
    def getCriteriaForProdShare(self,siteName,aggSites=[]):
        comment = ' /* DBProxy.getCriteriaForProdShare */'        
        # return for no criteria
        retForNone = '',{}
        # update fareshare policy
        self.getFaresharePolicy()
        # not defined
        if not self.faresharePolicy.has_key(siteName):
            return retForNone
        _logger.debug("getCriteriaForProdShare %s %s" % (siteName,str(aggSites)))
        try:
            # definition for fareshare
            usingGroup = self.faresharePolicy[siteName]['usingGroup'] 
            usingType  = self.faresharePolicy[siteName]['usingType'] 
            usingPrio  = self.faresharePolicy[siteName]['usingPrio'] 
            shareDefList   = []
            for tmpDefItem in self.faresharePolicy[siteName]['policyList']:
                shareDefList.append({'policy':tmpDefItem,'count':{}})
            # set autocommit on
            self.conn.begin()
            # check if countryGroup has activated jobs
            varMap = {}

            varMap[':prodSourceLabel'] = 'managed'
            sql  = "SELECT /*+ RESULT_CACHE */ jobStatus,"
            if usingGroup:
                sql += 'workingGroup,'
            if usingType:
                sql += 'processingType,'
            if usingPrio:
                sql += 'currentPriority,'
            sql += "SUM(num_of_jobs) FROM ATLAS_PANDA.MV_JOBSACTIVE4_STATS "
            sql += "WHERE computingSite IN ("
            tmpIdx = 0
            for tmpSiteName in [siteName]+aggSites:
                tmpKey = ':computingSite%s' % tmpIdx
                varMap[tmpKey] = tmpSiteName
                sql += '%s,' % tmpKey
                tmpIdx += 1
            sql = sql[:-1]
            sql += ") AND prodSourceLabel=:prodSourceLabel "
            sql += "GROUP BY jobStatus"
            if usingGroup:
                sql += ',workingGroup'
            if usingType:
                sql += ',processingType'
            if usingPrio:
                sql += ',currentPriority'
            self.cur.arraysize = 100000
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # no info about the site
            if res == None or len(res) == 0:
                _logger.debug("getCriteriaForProdShare %s : ret=None - no jobs" % siteName)
                return retForNone
            # loop over all rows
            workingGroupInQueueMap = {}
            processGroupInQueueMap = {} 
            for tmpItem in res:
                tmpIdx = 0
                jobStatus = tmpItem[tmpIdx]
                tmpIdx += 1
                if usingGroup:
                    workingGroup = tmpItem[tmpIdx]
                    tmpIdx += 1
                else:
                    workingGroup = None
                if usingType:
                    processingType = tmpItem[tmpIdx]
                    tmpIdx += 1
                    # get process group
                    processGroup = ProcessGroups.getProcessGroup(processingType)
                else:
                    processingType = None
                    processGroup = None
                if usingPrio:
                    currentPriority = tmpItem[tmpIdx]
                    tmpIdx += 1
                else:
                    currentPriority = None
                cnt = tmpItem[tmpIdx]
                # append processingType list    
                if not processGroupInQueueMap.has_key(processGroup):
                    processGroupInQueueMap[processGroup] = []
                if not processingType in processGroupInQueueMap[processGroup]:
                    processGroupInQueueMap[processGroup].append(processingType)
                # count the number of jobs for each policy
                for tmpShareDef in shareDefList:
                    policyName = tmpShareDef['policy']['name']                    
                    # use different list based on usage of priority
                    if tmpShareDef['policy']['priority'] == None:
                        groupInDefList = self.faresharePolicy[siteName]['groupList']
                        typeInDefList  = self.faresharePolicy[siteName]['typeList'][tmpShareDef['policy']['group']] 
                    else:
                        groupInDefList = self.faresharePolicy[siteName]['groupListWithPrio']
                        typeInDefList  = self.faresharePolicy[siteName]['typeListWithPrio'][tmpShareDef['policy']['group']]
                    # check working group
                    if usingGroup:
                        if tmpShareDef['policy']['group'] == None:
                            # catchall doesn't contain WGs used by other policies
                            if workingGroup != None and workingGroup in groupInDefList:
                                continue
                            # check for wildcard
                            toBeSkippedFlag = False
                            for tmpPattern in groupInDefList:
                                if '*' in tmpPattern:
                                    tmpPattern = '^' + tmpPattern.replace('*','.*') + '$'
                                    # don't use WG if it is included in other policies
                                    if re.search(tmpPattern,workingGroup) != None:
                                        toBeSkippedFlag = True
                                        break
                            if toBeSkippedFlag:
                                continue
                        else:
                            # needs to be matched if it is specified in the policy
                            if '*' in tmpShareDef['policy']['group']:
                                # using wild card
                                tmpPattern = '^' + tmpShareDef['policy']['group'].replace('*','.*') + '$'
                                if re.search(tmpPattern,workingGroup) == None:
                                    continue
                            else:
                                if tmpShareDef['policy']['group'] != workingGroup:
                                    continue
                            # collect real WGs per defined WG mainly for wildcard
                            if not workingGroupInQueueMap.has_key(tmpShareDef['policy']['group']):
                                workingGroupInQueueMap[tmpShareDef['policy']['group']] = []
                            if not workingGroup in workingGroupInQueueMap[tmpShareDef['policy']['group']]:
                                workingGroupInQueueMap[tmpShareDef['policy']['group']].append(workingGroup)
                    # check processingType
                    if usingType:
                        if tmpShareDef['policy']['type'] == None:
                            # catchall doesn't contain processGroups used by other policies
                            if processGroup != None and processGroup in typeInDefList:
                                continue
                        else:
                            # needs to be matched if it is specified in the policy
                            if tmpShareDef['policy']['type'] != processGroup:
                                continue
                    # check priority    
                    if usingPrio:
                        if currentPriority != None and tmpShareDef['policy']['priority'] != None:
                            if tmpShareDef['policy']['prioCondition'] == '>':
                                if currentPriority <= tmpShareDef['policy']['priority']:
                                    continue
                            elif tmpShareDef['policy']['prioCondition'] == '>=':
                                if currentPriority < tmpShareDef['policy']['priority']:
                                    continue
                            elif tmpShareDef['policy']['prioCondition'] == '<=':
                                if currentPriority > tmpShareDef['policy']['priority']:
                                    continue
                            elif tmpShareDef['policy']['prioCondition'] == '<':    
                                if currentPriority >= tmpShareDef['policy']['priority']:
                                    continue
                    # map some job status to running
                    if jobStatus in ['sent','starting']:
                        jobStatus = 'running'
                    # append job status
                    if not tmpShareDef['count'].has_key(jobStatus):
                        tmpShareDef['count'][jobStatus] = 0
                    # sum
                    tmpShareDef['count'][jobStatus] += cnt
            # loop over all policies to calcurate total number of running jobs and total share
            totalShare   = 0
            totalRunning = 0
            shareMap     = {}
            msgShare     = 'share->'
            for tmpShareDef in shareDefList:
                tmpNumMap = tmpShareDef['count']
                policyName = tmpShareDef['policy']['name']
                # policies with priorities are used only to limit the numer of jobs
                if tmpShareDef['policy']['priority'] != None:
                    continue
                # the number of activated jobs
                if not tmpNumMap.has_key('activated') or tmpNumMap['activated'] == 0:
                    tmpNumActivated = 0
                else:
                    tmpNumActivated = tmpNumMap['activated']
                # get share, removing % 
                tmpShareValue = tmpShareDef['policy']['share'][:-1]
                tmpShareValue = int(tmpShareValue)
                # get the number of runnig                
                if not tmpNumMap.has_key('running'):
                    tmpNumRunning = 0
                else:
                    tmpNumRunning = tmpNumMap['running']
                # debug message for share
                msgShare += '%s:activated=%s:running=%s,' % (policyName,tmpNumActivated,tmpNumRunning)
                # not use the policy if no activated jobs
                if tmpNumActivated == 0:
                    continue
                # append
                shareMap[policyName] = {
                    'share':tmpShareValue,
                    'running':tmpNumRunning,
                    'policy':tmpShareDef['policy'],
                    }
                # sum
                totalShare += tmpShareValue
                totalRunning += tmpNumRunning
            msgShare = msgShare[:-1]
            # loop over all policies to check if the priority constraint should be activated
            prioToBeImposed = []
            msgPrio = ''
            if usingPrio:            
                msgPrio += 'prio->'
                for tmpShareDef in shareDefList:
                    tmpNumMap = tmpShareDef['count']
                    policyName = tmpShareDef['policy']['name']
                    # only policies with priorities are used to limit the numer of jobs
                    if tmpShareDef['policy']['priority'] == None:
                        continue
                    # get the number of runnig                
                    if not tmpNumMap.has_key('running'):
                        tmpNumRunning = 0
                    else:
                        tmpNumRunning = tmpNumMap['running']
                    # the number of activated jobs
                    if not tmpNumMap.has_key('activated') or tmpNumMap['activated'] == 0:
                        tmpNumActivated = 0
                    else:
                        tmpNumActivated = tmpNumMap['activated']
                    # get limit
                    tmpLimitValue = tmpShareDef['policy']['share']
                    # check if more jobs are running than the limit
                    toBeImposed = False
                    if tmpLimitValue.endswith('%'):
                        # percentage based
                        tmpLimitValue = tmpLimitValue[:-1]
                        if float(tmpNumRunning) > float(totalRunning) * float(tmpLimitValue) / 100.0:
                            toBeImposed = True
                        # debug message for prio
                        msgPrio += '%s:total=%s:running=%s:impose=%s,' % (policyName,totalRunning,tmpNumRunning,toBeImposed)
                    else:
                        # number based
                        if tmpNumRunning > int(tmpLimitValue):
                            toBeImposed = True
                        # debug message for prio
                        msgPrio += '%s:running=%s:impose=%s,' % (policyName,tmpNumRunning,toBeImposed)
                    # append
                    if toBeImposed:
                        prioToBeImposed.append(tmpShareDef['policy'])
                msgPrio = msgPrio[:-1]
            # no activated
            if shareMap == {}:
                _logger.debug("getCriteriaForProdShare %s : ret=None - no activated" % siteName)
                return retForNone
            # no running
            if totalRunning == 0:
                _logger.debug("getCriteriaForProdShare %s : ret=None - no running" % siteName)
                return retForNone
            # zero share
            if totalShare == 0:
                _logger.debug("getCriteriaForProdShare %s : ret=None - zero share" % siteName)
                return retForNone
            # select the group where share most diverges from the definition
            lowestShareRatio  = None
            lowestSharePolicy = None
            for policyName,tmpVarMap in shareMap.iteritems():
                # ignore zero share
                if tmpVarMap['share'] == 0:
                    continue
                tmpShareDef = float(tmpVarMap['share']) / float(totalShare)
                tmpShareNow = float(tmpVarMap['running']) / float(totalRunning) 
                tmpShareRatio = tmpShareNow / tmpShareDef
                if lowestShareRatio == None or lowestShareRatio > tmpShareRatio:
                    lowestShareRatio  = tmpShareRatio
                    lowestSharePolicy = policyName
            # make criteria
            retVarMap = {}
            retStr = ''
            if lowestSharePolicy != None:
                tmpShareDef = shareMap[lowestSharePolicy]['policy']
                # working group
                if tmpShareDef['group'] == None:
                    groupInDefList = self.faresharePolicy[siteName]['groupList']
                    # catch all except WGs used by other policies
                    if groupInDefList != []:
                        groupUsedInClause = []
                        tmpIdx = 0
                        # use real name of workingGroup
                        for tmpGroupIdx in groupInDefList:
                            if not workingGroupInQueueMap.has_key(tmpGroupIdx):
                                continue
                            for tmpGroup in workingGroupInQueueMap[tmpGroupIdx]:
                                if tmpGroup in groupUsedInClause:
                                    continue
                                # add AND at the first WG
                                if groupUsedInClause == []:
                                    retStr += 'AND workingGroup NOT IN ('
                                # add WG
                                tmpKey = ':shareWG%s' % tmpIdx
                                retVarMap[tmpKey] = tmpGroup
                                retStr += '%s,' % tmpKey
                                tmpIdx += 1
                                # append
                                groupUsedInClause.append(tmpGroup)
                        if groupUsedInClause != []:        
                            retStr = retStr[:-1]
                            retStr += ') '
                else:
                    # match with one WG
                    if workingGroupInQueueMap.has_key(tmpShareDef['group']):
                        groupUsedInClause = []
                        tmpIdx = 0
                        # use real name of workingGroup
                        for tmpGroup in workingGroupInQueueMap[tmpShareDef['group']]:
                            if tmpGroup in groupUsedInClause:
                                continue
                            # add AND at the first WG
                            if groupUsedInClause == []:
                                retStr += 'AND workingGroup IN ('
                            # add WG
                            tmpKey = ':shareWG%s' % tmpIdx
                            retVarMap[tmpKey] = tmpGroup
                            retStr += '%s,' % tmpKey
                            tmpIdx += 1
                            # append
                            groupUsedInClause.append(tmpGroup)
                        if groupUsedInClause != []:        
                            retStr = retStr[:-1]
                            retStr += ') '
                # processing type
                if tmpShareDef['type'] == None:
                    typeInDefList  = self.faresharePolicy[siteName]['typeList'][tmpShareDef['group']]
                    # catch all except WGs used by other policies
                    if typeInDefList != []:
                        # get the list of processingTypes from the list of processGroups
                        retVarMapP = {}
                        retStrP = 'AND processingType NOT IN ('
                        tmpIdx  = 0
                        for tmpTypeGroup in typeInDefList:
                            if processGroupInQueueMap.has_key(tmpTypeGroup):
                                for tmpType in processGroupInQueueMap[tmpTypeGroup]:
                                    tmpKey = ':sharePT%s' % tmpIdx
                                    retVarMapP[tmpKey] = tmpType
                                    retStrP += '%s,' % tmpKey
                                    tmpIdx += 1
                        retStrP = retStrP[:-1]
                        retStrP += ') '
                        # copy
                        if retVarMapP != {}:
                            retStr += retStrP
                            for tmpKey,tmpType in retVarMapP.iteritems():
                                retVarMap[tmpKey] = tmpType
                else:
                    # match with one processingGroup
                    if processGroupInQueueMap.has_key(tmpShareDef['type']) and processGroupInQueueMap[tmpShareDef['type']] != []:
                        retStr += 'AND processingType IN ('
                        tmpIdx = 0
                        for tmpType in processGroupInQueueMap[tmpShareDef['type']]:
                            tmpKey = ':sharePT%s' % tmpIdx
                            retVarMap[tmpKey] = tmpType
                            retStr += '%s,' % tmpKey
                            tmpIdx += 1
                        retStr = retStr[:-1]
                        retStr += ') '
            # priority
            tmpIdx = 0
            for tmpDefItem in prioToBeImposed:
                if tmpDefItem['group'] in [None,tmpShareDef['group']] and \
                   tmpDefItem['type'] in [None,tmpShareDef['type']]:
                    if tmpDefItem['prioCondition'] == '>':
                        retStrP = '<='
                    elif tmpDefItem['prioCondition'] == '>=':
                        retStrP = '<'
                    elif tmpDefItem['prioCondition'] == '<=':
                        retStrP = '>'                        
                    elif tmpDefItem['prioCondition'] == '<':
                        retStrP = '>='
                    else:
                        continue
                    tmpKey = ':sharePrio%s' % tmpIdx
                    retVarMap[tmpKey] = tmpDefItem['priority']
                    retStr += ('AND currentPriority%s%s' % (retStrP,tmpKey)) 
                    tmpIdx += 1
            _logger.debug("getCriteriaForProdShare %s : sql='%s' var=%s %s %s" % \
                          (siteName,retStr,str(retVarMap),msgShare,msgPrio))
            # append criteria for test jobs
            if retStr != '':
                retVarMap[':shareLabel1'] = 'managed'
                retVarMap[':shareLabel2'] = 'test'
                retVarMap[':shareLabel3'] = 'prod_test'
                retStr = 'AND (prodSourceLabel IN (:shareLabel2,:shareLabel3) OR (prodSourceLabel=:shareLabel1 ' + retStr + '))'
            return retStr,retVarMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getCriteriaForProdShare %s : %s %s" % (siteName,errtype,errvalue))
            # roll back
            self._rollback()
            return retForNone


    # get beyond pledge resource ratio
    def getPledgeResourceRatio(self):
        comment = ' /* DBProxy.getPledgeResourceRatio */'
        # check utime
        if self.updateTimeForPledgeRatio != None and (datetime.datetime.utcnow()-self.updateTimeForPledgeRatio) < datetime.timedelta(hours=3):
            return
        # update utime
        self.updateTimeForPledgeRatio = datetime.datetime.utcnow()
        _logger.debug("getPledgeResourceRatio")
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql  = "SELECT siteid,countryGroup,availableCPU,availableStorage,pledgedCPU,pledgedStorage "
            sql += "FROM ATLAS_PANDAMETA.schedconfig WHERE countryGroup IS NOT NULL AND siteid LIKE 'ANALY_%' "
            self.cur.arraysize = 100000            
            self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # update ratio
            self.beyondPledgeRatio = {}
            if res != None and len(res) != 0:
                for siteid,countryGroup,tmp_availableCPU,tmp_availableStorage,tmp_pledgedCPU,tmp_pledgedStorage in res:
                    # ignore when countryGroup is undefined
                    if countryGroup in ['',None]:
                        continue
                    # append
                    self.beyondPledgeRatio[siteid] = {} 
                    self.beyondPledgeRatio[siteid]['countryGroup'] = countryGroup
                    # convert to float
                    try:
                        availableCPU = float(tmp_availableCPU)
                    except:
                        availableCPU = 0
                    try:
                        pledgedCPU = float(tmp_pledgedCPU)
                    except:
                        pledgedCPU = 0
                    # calculate ratio
                    if availableCPU == 0 or pledgedCPU == 0:
                        # set 0% when CPU ratio is undefined
                        self.beyondPledgeRatio[siteid]['ratio'] = 0
                    else:
                        # ratio = (availableCPU-pledgedCPU)/availableCPU*(1-storageTerm)
                        self.beyondPledgeRatio[siteid]['ratio'] = (availableCPU-pledgedCPU)/availableCPU
            _logger.debug("getPledgeResourceRatio -> %s" % str(self.beyondPledgeRatio))
            return
        except:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getPledgeResourceRatio : %s %s" % (errtype,errvalue))
            # roll back
            self._rollback()
            return


    # get fareshare policy
    def getFaresharePolicy(self,getNewMap=False):
        comment = ' /* DBProxy.getFaresharePolicy */'
        # check utime
        if not getNewMap and self.updateTimeForFaresharePolicy != None and \
               (datetime.datetime.utcnow()-self.updateTimeForFaresharePolicy) < datetime.timedelta(hours=3):
            return
        if not getNewMap:
            # update utime
            self.updateTimeForFaresharePolicy = datetime.datetime.utcnow()
        _logger.debug("getFaresharePolicy")
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql  = "SELECT siteid,fairsharePolicy "
            sql += "FROM ATLAS_PANDAMETA.schedconfig WHERE fairsharePolicy IS NOT NULL AND NOT siteid LIKE 'ANALY_%' GROUP BY siteid,fairsharePolicy"
            self.cur.arraysize = 100000            
            self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # update policy
            faresharePolicy = {}
            for siteid,faresharePolicyStr in res:
                try:
                    # decompose
                    hasNonPrioPolicy = False
                    for tmpItem in faresharePolicyStr.split(','):
                        # skip empty
                        tmpItem = tmpItem.strip()
                        if tmpItem == '':
                            continue
                        # keep name
                        tmpPolicy = {'name':tmpItem}
                        # group
                        tmpPolicy['group'] = None
                        tmpMatch = re.search('group=([^:]+)',tmpItem)
                        if tmpMatch != None:
                            if tmpMatch.group(1) in ['','central','*','any']:
                                # use None for catchall
                                pass
                            else:
                                tmpPolicy['group'] = tmpMatch.group(1)
                        # type
                        tmpPolicy['type'] = None
                        tmpMatch = re.search('type=([^:]+)',tmpItem)
                        if tmpMatch != None:
                            if tmpMatch.group(1) in ['*','any']:
                                # use None for catchall
                                pass
                            else:
                                tmpPolicy['type'] = tmpMatch.group(1)
                        # priority
                        tmpPolicy['priority'] = None
                        tmpPolicy['prioCondition'] = None                        
                        tmpMatch = re.search('priority([=<>]+)(\d+)',tmpItem)
                        if tmpMatch != None:
                            tmpPolicy['priority'] = int(tmpMatch.group(2))
                            tmpPolicy['prioCondition'] = tmpMatch.group(1)
                        else:
                            hasNonPrioPolicy = True
                        # share
                        tmpPolicy['share'] = tmpItem.split(':')[-1]
                        # append
                        if not faresharePolicy.has_key(siteid):
                            faresharePolicy[siteid] = {'policyList':[]}
                        faresharePolicy[siteid]['policyList'].append(tmpPolicy)
                    # add any:any if only priority policies
                    if not hasNonPrioPolicy:
                        tmpPolicy = {'name'          : 'type=any',
                                     'group'         : None,
                                     'type'          : None,
                                     'priority'      : None,
                                     'prioCondition' : None,
                                     'share'         : '100%'}
                        # FIXME once pilotfactory is ready to use fairshare
                        #faresharePolicy[siteid]['policyList'].append(tmpPolicy)
                    # some translation
                    faresharePolicy[siteid]['usingGroup'] = False
                    faresharePolicy[siteid]['usingType']  = False
                    faresharePolicy[siteid]['usingPrio']  = False
                    faresharePolicy[siteid]['groupList']  = []
                    faresharePolicy[siteid]['typeList']   = {}
                    faresharePolicy[siteid]['groupListWithPrio']  = []
                    faresharePolicy[siteid]['typeListWithPrio']   = {}
                    for tmpDefItem in faresharePolicy[siteid]['policyList']:
                        # using WG
                        if tmpDefItem['group'] != None:
                            faresharePolicy[siteid]['usingGroup'] = True
                        # using PG    
                        if tmpDefItem['type'] != None:
                            faresharePolicy[siteid]['usingType'] = True
                        # using prio    
                        if tmpDefItem['priority'] != None:
                            faresharePolicy[siteid]['usingPrio'] = True
                        # get list of WG and PG with/without priority     
                        if tmpDefItem['priority'] == None:    
                            # get list of woringGroups
                            if tmpDefItem['group'] != None and not tmpDefItem['group'] in faresharePolicy[siteid]['groupList']:
                                faresharePolicy[siteid]['groupList'].append(tmpDefItem['group'])
                            # get list of processingGroups 
                            if not faresharePolicy[siteid]['typeList'].has_key(tmpDefItem['group']):
                                faresharePolicy[siteid]['typeList'][tmpDefItem['group']] = []
                            if tmpDefItem['type'] != None and not tmpDefItem['type'] in faresharePolicy[siteid]['typeList'][tmpDefItem['group']]:
                                faresharePolicy[siteid]['typeList'][tmpDefItem['group']].append(tmpDefItem['type'])
                        else:
                            # get list of woringGroups
                            if tmpDefItem['group'] != None and not tmpDefItem['group'] in faresharePolicy[siteid]['groupListWithPrio']:
                                faresharePolicy[siteid]['groupListWithPrio'].append(tmpDefItem['group'])
                            # get list of processingGroups 
                            if not faresharePolicy[siteid]['typeListWithPrio'].has_key(tmpDefItem['group']):
                                faresharePolicy[siteid]['typeListWithPrio'][tmpDefItem['group']] = []
                            if tmpDefItem['type'] != None and not tmpDefItem['type'] in faresharePolicy[siteid]['typeListWithPrio'][tmpDefItem['group']]:
                                faresharePolicy[siteid]['typeListWithPrio'][tmpDefItem['group']].append(tmpDefItem['type'])
                except:
                    errtype,errvalue = sys.exc_info()[:2]                    
                    _logger.warning("getFaresharePolicy : wrond definition '%s' for %s : %s %s" % (faresharePolicy,siteid,errtype,errvalue))                    
            _logger.debug("getFaresharePolicy -> %s" % str(faresharePolicy))
            if not getNewMap:
                self.faresharePolicy = faresharePolicy
                return
            else:
                return faresharePolicy
        except:
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getFaresharePolicy : %s %s" % (errtype,errvalue))
            # roll back
            self._rollback()
            if not getNewMap:
                return
            else:
                return {}


    # get cloud list
    def getCloudList(self):
        comment = ' /* DBProxy.getCloudList */'        
        _logger.debug("getCloudList start")        
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql  = "SELECT name,tier1,tier1SE,relocation,weight,server,status,transtimelo,"
            sql += "transtimehi,waittime,validation,mcshare,countries,fasttrack,nprestage,"
            sql += "pilotowners "
            sql+= "FROM ATLAS_PANDAMETA.cloudconfig"
            self.cur.arraysize = 10000            
            self.cur.execute(sql+comment)
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
                        waittime,validation,mcshare,countries,fasttrack,nprestage,pilotowners = resTmp 
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


    # check sites with release/cache
    def checkSitesWithRelease(self,sites,releases,caches,cmtConfig=None):
        comment = ' /* DBProxy.checkSitesWithRelease */'
        try:
            relStr = releases
            if releases != None:
                relStr = releases.replace('\n',' ')
            caStr = caches
            if caches != None:
                caStr = caches.replace('\n',' ')
            _logger.debug("checkSitesWithRelease(%s,%s,%s,%s)" % (sites,relStr,caStr,cmtConfig))
            # select
            sql  = "SELECT distinct siteid FROM ATLAS_PANDAMETA.InstalledSW WHERE "
            if not caches in ['','NULL',None]:
                loopKey = ':cache'
                loopValues = caches.split('\n')
                sql += "cache=:cache "
            elif not releases in ['','NULL',None]:
                loopKey = ':release'
                loopValues = releases.split('\n')
                sql += "release=:release AND cache='None' "
            else:
                # don't check
                return sites
            checkCMT = False
            if not cmtConfig in ['','NULL',None]:
                sql += "AND cmtConfig=:cmtConfig "
                checkCMT = True
            sql += "AND siteid IN ("
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            # loop over all releases/caches
            for loopVal in loopValues:
                # remove Atlas-
                loopVal = re.sub('^Atlas-','',loopVal)
                sqlSite = sql
                varMap = {}
                varMap[loopKey] = loopVal
                if checkCMT:
                    varMap[':cmtConfig'] = cmtConfig
                tmpRetSites = []
                # loop over sites
                nSites = 10
                iSite = 0
                for siteIndex,site in enumerate(sites):
                    iSite += 1
                    tmpSiteKey = ':siteid%s' % iSite
                    varMap[tmpSiteKey] = site
                    sqlSite += '%s,' % tmpSiteKey
                    if iSite == nSites or (siteIndex+1) == len(sites):
                        iSite = 0
                        # close bracket in SQL
                        sqlSite = sqlSite[:-1]
                        sqlSite += ')'
                        # execute
                        _logger.debug(sqlSite+comment+str(varMap))
                        self.cur.execute(sqlSite+comment, varMap)
                        resList = self.cur.fetchall()
                        # collect candidates
                        if len(resList) > 0:
                            for tmpSite, in resList:
                                # append
                                tmpRetSites.append(tmpSite)
                        # reset
                        sqlSite = sql
                        varMap = {}
                        varMap[loopKey] = loopVal
                        if checkCMT:
                            varMap[':cmtConfig'] = cmtConfig
                # set
                sites = tmpRetSites
                # escape
                if sites == []:
                    break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("checkSitesWithRelease -> %s" % sites)
            return sites
        except:
            # roll back
            self._rollback()
            type,value,traceBack = sys.exc_info()
            _logger.error("checkSitesWithRelease : %s %s" % (type,value))
            return []


    # get sites with release/cache in cloud 
    def getSitesWithReleaseInCloud(self,cloud,releases,caches,validation):
        comment = ' /* DBProxy.getSitesWithReleaseInCloud */'        
        try:
            relStr = releases
            if releases != None:
                relStr = releases.replace('\n',' ')
            caStr = caches
            if caches != None:
                caStr = caches.replace('\n',' ')
            _logger.debug("getSitesWithReleaseInCloud(%s,%s,%s,%s)" % (cloud,relStr,caStr,validation))
            # select
            sql  = "SELECT distinct siteid FROM ATLAS_PANDAMETA.InstalledSW WHERE cloud=:cloud AND "
            varMap = {}
            varMap[':cloud'] = cloud
            if not caches in ['','NULL',None]:
                loopKey = ':cache'
                loopValues = caches.split('\n')
                sql += "cache=:cache "
            else:
                loopKey = ':release'
                loopValues = releases.split('\n')
                sql += "release=:release AND cache='None' "
            # validation
            if validation:
                sql += "validation=:validation "
                varMap[':validation'] = 'validated'
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100
            # loop over all releases/caches
            retSites = None
            for loopVal in loopValues:
                # remove Atlas-
                loopVal = re.sub('^Atlas-','',loopVal)
                varMap[loopKey] = loopVal
                # execute
                _logger.debug(sql+comment+str(varMap))
                self.cur.execute(sql+comment, varMap)
                resList = self.cur.fetchall()
                # append
                tmpRetSites = []
                for tmpItem, in resList:
                    if retSites == None or (tmpItem in retSites):
                        tmpRetSites.append(tmpItem)
                # set
                retSites = tmpRetSites
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # append
            retSites = []
            for tmpItem, in resList:
                retSites.append(tmpItem)
            _logger.debug("getSitesWithReleaseInCloud -> %s" % retSites)
            return retSites
        except:
            # roll back
            self._rollback()
            type,value,traceBack = sys.exc_info()
            _logger.error("getSitesWithReleaseInCloud : %s %s" % (type,value))
            return []


    # get list of cache prefix
    def getCachePrefixes(self):
        comment = ' /* DBProxy.getCachePrefixes */'        
        try:
            _logger.debug("getCachePrefixes")
            # select
            sql  = "SELECT distinct cache FROM ATLAS_PANDAMETA.installedSW WHERE cache IS NOT NULL"
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10000
            # execute
            self.cur.execute(sql+comment, {})
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # append
            tmpList = []
            for tmpItem, in resList:
                match = re.search('^([^-]+)-',tmpItem)
                if match != None:
                    tmpPrefix = match.group(1)
                    if not tmpPrefix in tmpList:
                        tmpList.append(tmpPrefix)
            _logger.debug("getCachePrefixes -> %s" % tmpList)
            return tmpList
        except:
            # roll back
            self._rollback()
            type,value,traceBack = sys.exc_info()
            _logger.error("getCachePrefixes : %s %s" % (type,value))
            return []

        
    # get pilot owners
    def getPilotOwners(self):
        comment = ' /* DBProxy.getPilotOwners */'        
        _logger.debug("getPilotOwners")        
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql  = "SELECT pilotowners FROM ATLAS_PANDAMETA.cloudconfig"
            self.cur.arraysize = 100
            self.cur.execute(sql+comment)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            ret = []
            for tmpItem, in resList:
                if tmpItem != None:
                    for tmpOwner in tmpItem.split('|'):
                        if tmpOwner != '':
                            ret.append(tmpOwner)
            _logger.debug("getPilotOwners -> %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            type,value,traceBack = sys.exc_info()
            _logger.error("getPilotOwners : %s %s" % (type,value))
            return []


    # get allowed nodes
    def getAllowedNodes(self):
        comment = ' /* DBProxy.getAllowedNodes */'        
        _logger.debug("getAllowedNodes")        
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql  = "SELECT siteid,allowedNode FROM ATLAS_PANDAMETA.schedconfig "
            sql += "WHERE siteid IS NOT NULL AND allowedNode IS NOT NULL"
            self.cur.arraysize = 1000
            self.cur.execute(sql+comment)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            ret = {}
            for tmpSiteID,tmpAllowedNode in resList:
                if not ret.has_key(tmpSiteID):
                    ret[tmpSiteID] = tmpAllowedNode.split(',')
            _logger.debug("getAllowedNodes -> %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            tmpType,tmpValue = sys.exc_info()[:2]
            _logger.error("getAllowedNodes : %s %s" % (tmpType,tmpValue))
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
            username = re.sub('/CN=Robot:[^/]+','',username)
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
            username = username.replace("'",'')
            return username
        except:
            return id

    
    # check quota
    def checkQuota(self,dn):
        comment = ' /* DBProxy.checkQuota */'
        _logger.debug("checkQuota %s" % dn)
        try:
            # set autocommit on
            self.conn.begin()
            # select
            name = self.cleanUserID(dn)
            sql = "SELECT cpua1,cpua7,cpua30,quotaa1,quotaa7,quotaa30 FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[':name'] = name
            self.cur.arraysize = 10            
            self.cur.execute(sql+comment,varMap)
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
                if item[3] in [0,None]:
                    quota1 = 0
                else:
                    quota1 = item[3] * 3600
                if item[4] in [0,None]:
                    quota7 = 0
                else:
                    quota7 = item[4] * 3600
                if item[5] in [0,None]:    
                    quota30 = 0
                else:
                    quota30 = item[5] * 3600                    
                # CPU usage
                if cpu1 == None:
                    cpu1 = 0.0
                # weight
                if quota1 > 0:
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
    def getUserParameter(self,dn,jobID,jobsetID):
        comment = ' /* DBProxy.getUserParameter */'                            
        _logger.debug("getUserParameter %s JobID=%s JobsetID=%s" % (dn,jobID,jobsetID))
        try:
            # set initial values
            retStatus = True
            if jobsetID == -1:
                # generate new jobsetID
                retJobsetID = jobID
                # new jobID = 1 + new jobsetID
                retJobID = retJobsetID + 1
            elif jobsetID in ['NULL',None,0]:
                # no jobsetID
                retJobsetID = None
                retJobID = jobID
            else:
                # user specified jobsetID
                retJobsetID = jobsetID
                retJobID = jobID
            # set autocommit on
            self.conn.begin()
            # select
            name = self.cleanUserID(dn)
            sql = "SELECT jobid,status FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10            
            res = self.cur.fetchall()
            if res != None and len(res) != 0:
                item = res[0]
                # JobID in DB
                dbJobID  = item[0]
                # check status
                if item[1] in ['disabled']:
                    retStatus = False
                # use larger JobID
                if dbJobID >= int(retJobID) or (jobsetID == -1 and dbJobID >= int(retJobsetID)):
                    if jobsetID == -1:
                        # generate new jobsetID = 1 + exsiting jobID
                        retJobsetID = dbJobID+1
                        # new jobID = 1 + new jobsetID
                        retJobID = retJobsetID + 1
                    else:
                        # new jobID = 1 + exsiting jobID
                        retJobID = dbJobID+1
                # update DB
                varMap = {}
                varMap[':name'] = name
                varMap[':jobid'] = retJobID
                sql = "UPDATE ATLAS_PANDAMETA.users SET jobid=:jobid WHERE name=:name"
                self.cur.execute(sql+comment,varMap)
                _logger.debug("getUserParameter set JobID=%s for %s" % (retJobID,dn))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("getUserParameter %s return JobID=%s JobsetID=%s Status=%s" % (dn,retJobID,retJobsetID,retStatus))
            return retJobID,retJobsetID,retStatus
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getUserParameter : %s %s" % (type,value))
            # roll back
            self._rollback()
            return retJobID,retJobsetID,retStatus


    # get JobID for user
    def getJobIdUser(self,dn):
        comment = ' /* DBProxy.getJobIdUser */'
        _logger.debug("getJobIdUser %s" % dn)
        jobID = 0
        try:
            # set autocommit on
            self.conn.begin()
            # select
            name = self.cleanUserID(dn)
            sql = "SELECT jobid FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[':name'] = name
            self.cur.arraysize = 10            
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchone()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if res != None:
                jobID, = res
            _logger.debug("getJobIdUser %s -> %s" % (name,jobID))
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getJobIdUser : %s %s" % (errType,errValue))
            # roll back
            self._rollback()
        return jobID

        
    # check ban user
    def checkBanUser(self,dn,sourceLabel):
        comment = ' /* DBProxy.checkBanUser */'                            
        _logger.debug("checkBanUser %s %s" % (dn,sourceLabel))
        try:
            # set initial values
            retStatus = True
            # set autocommit on
            self.conn.begin()
            # select
            name = self.cleanUserID(dn)
            sql = "SELECT status FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchone()            
            if res != None:
                # check status
                tmpStatus, = res
                if tmpStatus in ['disabled']:
                    retStatus = False
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("checkBanUser %s %s Status=%s" % (dn,sourceLabel,retStatus))
            return retStatus
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("checkBanUser %s %s : %s %s" % (dn,sourceLabel,errType,errValue))
            # roll back
            self._rollback()
            return retStatus

        
    # get email address for a user
    def getEmailAddr(self,name):
        comment = ' /* DBProxy.getEmailAddr */'
        _logger.debug("get email for %s" % name) 
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT email FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql+comment,varMap)
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


    # get client version
    def getPandaClientVer(self):
        comment = ' /* DBProxy.getPandaClientVer */'
        _logger.debug("getPandaClientVer") 
        try:
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT pathena FROM ATLAS_PANDAMETA.pandaconfig WHERE name=:name"
            varMap = {}
            varMap[':name'] = 'current'
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10            
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retStr = ''
            if res != None and len(res) != 0:
                retStr = res[0][0]
            _logger.debug("getPandaClientVer -> %s" % retStr)                 
            return retStr
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getPandaClientVer : %s %s" % (type,value))
            return ""

        
    # add files to memcached
    def addFilesToMemcached(self,site,node,files):
        _logger.debug("addFilesToMemcached start %s %s" % (site,node)) 
        # memcached is unused
        if not panda_config.memcached_enable:
            _logger.debug("addFilesToMemcached skip %s %s" % (site,node))
            return True
        try:
            # initialize memcache if needed
            if self.memcache == None:
                from MemProxy import MemProxy
                self.memcache = MemProxy()
            # convert string to list    
            fileList = files.split(',')
            # remove ''
            try:
                fileList.remove('')
            except:
                pass
            # empty list
            if len(fileList) == 0:
                _logger.debug("addFilesToMemcached skipped for empty list")
                return True
            # list of siteIDs
            siteIDs = site.split(',')
            # loop over all siteIDs
            for tmpSite in siteIDs:
                # add
                iFiles = 0
                nFiles = 100
                retS = True
                while iFiles < len(fileList):            
                    tmpRetS = self.memcache.setFiles(None,tmpSite,node,fileList[iFiles:iFiles+nFiles])
                    if not tmpRetS:
                        retS = False
                    iFiles += nFiles                    
            _logger.debug("addFilesToMemcached done %s %s with %s" % (site,node,retS))
            return retS
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("addFilesToMemcached : %s %s" % (errType,errValue))
            return False


    # delete files from memcached
    def deleteFilesFromMemcached(self,site,node,files):
        _logger.debug("deleteFilesFromMemcached start %s %s" % (site,node)) 
        # memcached is unused
        if not panda_config.memcached_enable:
            _logger.debug("deleteFilesFromMemcached skip %s %s" % (site,node))             
            return True
        try:
            # initialize memcache if needed
            if self.memcache == None:
                from MemProxy import MemProxy
                self.memcache = MemProxy()
            # list of siteIDs
            siteIDs = site.split(',')
            # loop over all siteIDs
            for tmpSite in siteIDs:
                # delete    
                self.memcache.deleteFiles(tmpSite,node,files)
            _logger.debug("deleteFilesFromMemcached done %s %s" % (site,node))             
            return True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("deleteFilesFromMemcached : %s %s" % (errType,errValue))
            return False


    # flush memcached
    def flushMemcached(self,site,node):
        _logger.debug("flushMemcached start %s %s" % (site,node)) 
        # memcached is unused
        if not panda_config.memcached_enable:
            _logger.debug("flushMemcached skip %s %s" % (site,node))             
            return True
        try:
            # initialize memcache if needed
            if self.memcache == None:
                from MemProxy import MemProxy
                self.memcache = MemProxy()
            # list of siteIDs
            siteIDs = site.split(',')
            # loop over all siteIDs
            for tmpSite in siteIDs:
                # flush    
                self.memcache.flushFiles(tmpSite,node)
            _logger.debug("flushMemcached done %s %s" % (site,node))             
            return True
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("flushMemcached : %s %s" % (errType,errValue))
            return False


    # check files with memcached
    def checkFilesWithMemcached(self,site,node,files):
        _logger.debug("checkFilesWithMemcached start %s %s" % (site,node))
        # convert string to list    
        fileList = files.split(',')
        # remove ''
        try:
            fileList.remove('')
        except:
            pass
        # memcached is unused
        if not panda_config.memcached_enable:
            _logger.debug("checkFilesWithMemcached skip %s %s" % (site,node))
            # return 0
            retStr = ''
            for tmpF in fileList:
                retStr += '0,'
            retStr = retStr[:-1]    
            return retStr
        try:
            # initialize memcache if needed
            if self.memcache == None:
                from MemProxy import MemProxy
                self.memcache = MemProxy()
            # empty list
            if len(fileList) == 0:
                _logger.debug("checkFilesWithMemcached skipped for empty list")
                return ''
            # check
            iFiles = 0
            nFiles = 100
            retS = ''
            while iFiles < len(fileList):
                retS += self.memcache.checkFiles(None,fileList[iFiles:iFiles+nFiles],site,node,getDetail=True)
                retS += ','
                iFiles += nFiles
            retS = retS[:-1]    
            _logger.debug("checkFilesWithMemcached done %s %s with %s" % (site,node,retS))
            return retS
        except:
            errType,errValue = sys.exc_info()[:2]
            _logger.error("checkFilesWithMemcached : %s %s" % (errType,errValue))
            return False

        
    # register proxy key
    def registerProxyKey(self,params):
        comment = ' /* DBProxy.registerProxyKey */'
        _logger.debug("register ProxyKey %s" % str(params))
        try:
            # set autocommit on
            self.conn.begin()
            # construct SQL
            sql0 = 'INSERT INTO ATLAS_PANDAMETA.proxykey (id,'
            sql1 = 'VALUES (ATLAS_PANDAMETA.PROXYKEY_ID_SEQ.nextval,'
            vals = {}
            for key,val in params.iteritems():
                sql0 += '%s,'  % key
                sql1 += ':%s,' % key
                vals[':%s' % key] = val
            sql0 = sql0[:-1]
            sql1 = sql1[:-1]            
            sql = sql0 + ') ' + sql1 + ') '
            # insert
            self.cur.execute(sql+comment,vals)
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
        comment = ' /* DBProxy.getProxyKey */'        
        _logger.debug("get ProxyKey %s" % dn)
        try:
            # set autocommit on
            self.conn.begin()
            # construct SQL
            sql = 'SELECT credname,expires,origin,myproxy FROM ATLAS_PANDAMETA.proxykey WHERE dn=:dn ORDER BY expires DESC'
            varMap = {}
            varMap[':dn'] = dn
            # select
            self.cur.execute(sql+comment,varMap)
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
    def checkSiteAccess(self,siteid,longDN):
        comment = ' /* DBProxy.checkSiteAccess */'
        _logger.debug("checkSiteAccess %s:%s" % (siteid,longDN))
        try:
            # use compact DN
            dn = self.cleanUserID(longDN)
            # construct SQL
            sql = 'SELECT poffset,rights,status,workingGroups FROM ATLAS_PANDAMETA.siteaccess WHERE dn=:dn AND pandasite=:pandasite'
            varMap = {}
            varMap[':dn'] = dn
            varMap[':pandasite'] = siteid
            # set autocommit on
            self.conn.begin()
            # select
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10            
            res = self.cur.fetchall()            
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            retMap = {}
            if res != None and len(res) != 0:
                poffset,rights,status,workingGroups = res[0]
                retMap['poffset'] = poffset
                retMap['rights']  = rights
                retMap['status']  = status
                if workingGroups in ['',None]:
                    workingGroups = []
                else:
                    workingGroups = workingGroups.split(',')                    
                retMap['workingGroups'] = workingGroups
            _logger.debug(retMap)
            return retMap
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("checkSiteAccess : %s %s" % (type,value))
            # roll back
            self._rollback()
            return {}

        
    # add account to siteaccess
    def addSiteAccess(self,siteID,longDN):
        comment = ' /* DBProxy.addSiteAccess */'                        
        _logger.debug("addSiteAccess : %s %s" % (siteID,longDN))
        try:
            # use compact DN
            dn = self.cleanUserID(longDN)
            # set autocommit on
            self.conn.begin()
            # select
            sql = 'SELECT status FROM ATLAS_PANDAMETA.siteaccess WHERE dn=:dn AND pandasite=:pandasite'
            varMap = {}
            varMap[':dn'] = dn
            varMap[':pandasite'] = siteID
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10                        
            res = self.cur.fetchone()
            if res != None:
                _logger.debug("account already exists with status=%s" % res[0])
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return res[0]
            # add
            sql = 'INSERT INTO ATLAS_PANDAMETA.siteaccess (id,dn,pandasite,status,created) VALUES (ATLAS_PANDAMETA.SITEACCESS_ID_SEQ.nextval,:dn,:pandasite,:status,CURRENT_DATE)'
            varMap = {}
            varMap[':dn'] = dn
            varMap[':pandasite'] = siteID
            varMap[':status'] = 'requested'            
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("account was added")
            return 0
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("addSiteAccess : %s %s" % (type,value))
            # return None
            return -1


    # list site access
    def listSiteAccess(self,siteid=None,dn=None,longFormat=False):
        comment = ' /* DBProxy.listSiteAccess */'
        _logger.debug("listSiteAccess %s:%s" % (siteid,dn))
        try:
            if siteid==None and dn==None:
                return []
            longAttributes = 'status,poffset,rights,workingGroups,created' 
            # set autocommit on
            self.conn.begin()
            # construct SQL
            if siteid != None:
                varMap = {':pandasite':siteid}
                if not longFormat:
                    sql = 'SELECT dn,status FROM ATLAS_PANDAMETA.siteaccess WHERE pandasite=:pandasite ORDER BY dn'
                else:
                    sql  = 'SELECT dn,%s FROM ATLAS_PANDAMETA.siteaccess ' % longAttributes
                    sql += 'WHERE pandasite=:pandasite ORDER BY dn'
            else:
                shortDN = self.cleanUserID(dn)
                varMap = {':dn':shortDN}
                if not longFormat:
                    sql = 'SELECT pandasite,status FROM ATLAS_PANDAMETA.siteaccess WHERE dn=:dn ORDER BY pandasite'
                else:
                    sql  = 'SELECT pandasite,%s FROM ATLAS_PANDAMETA.siteaccess ' % longAttributes
                    sql += 'WHERE dn=:dn ORDER BY pandasite'                    
            # select
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 1000
            res = self.cur.fetchall()            
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            ret = []
            if res != None and len(res) != 0:
                for tmpRes in res:
                    if not longFormat:
                        ret.append(tmpRes)
                    else:
                        # create map for long format
                        tmpRetMap = {}
                        # use first value as a primary key
                        tmpRetMap['primKey'] = tmpRes[0]
                        idxVal = 1
                        for tmpKey in longAttributes.split(','):
                            tmpRetMap[tmpKey] = tmpRes[idxVal]
                            idxVal += 1
                        ret.append(tmpRetMap)    
            _logger.debug(ret)
            return ret
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("listSiteAccess : %s %s" % (type,value))
            return []


    # update site access
    def updateSiteAccess(self,method,siteid,requesterDN,userName,attrValue):
        comment = ' /* DBProxy.updateSiteAccess */'
        _logger.debug("updateSiteAccess %s:%s:%s:%s:%s" % (method,siteid,requesterDN,userName,attrValue))
        try:
            # set autocommit on
            self.conn.begin()
            # check existence
            varMap = {}
            varMap[':pandasite'] = siteid
            varMap[':dn'] = userName
            sql = 'SELECT count(*) FROM ATLAS_PANDAMETA.siteaccess WHERE pandasite=:pandasite AND dn=:dn'
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchall()            
            if res == None or res[0][0] == 0:
                _logger.error("updateSiteAccess : No request for %s" % varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                return 'No request for %s:%s' % (siteid,userName)
            # get cloud
            varMap = {':pandasite':siteid}
            sql = 'SELECT cloud,dn FROM ATLAS_PANDAMETA.schedconfig WHERE siteid=:pandasite AND rownum<=1'
            self.cur.execute(sql+comment,varMap)            
            res = self.cur.fetchall()
            if res == None or len(res) == 0:
                _logger.error("updateSiteAccess : No cloud in schedconfig for %s" % siteid)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                return "No cloud in schedconfig for %s" % siteid
            cloud = res[0][0]
            siteContact = res[0][1]
            # get cloud responsible
            varMap = {':cloud':cloud}
            sql = 'SELECT dn FROM ATLAS_PANDAMETA.cloudconfig WHERE name=:cloud'
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchall()
            if res == None or len(res) == 0:
                _logger.error("updateSiteAccess : No contact in cloudconfig for %s" % cloud)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                return "No contact in cloudconfig for %s" % cloud
            contactNames = res[0][0]
            if contactNames in [None,'']:
                contactNames = []
            else:
                contactNames = contactNames.split(',')
            # get site responsible
            if not siteContact in [None,'']:
                contactNames += siteContact.split(',')
            # check privilege
            if not self.cleanUserID(requesterDN) in contactNames:
                _logger.error("updateSiteAccess : %s is not one of contacts %s" % (requesterDN,str(contactNames)))
                # return
                return "Insufficient privilege"
            # update
            varMap = {}
            varMap[':pandasite'] = siteid
            varMap[':dn'] = userName
            if method in ['approve','reject']:
                # update status
                sql = 'UPDATE ATLAS_PANDAMETA.siteaccess SET status=:newStatus WHERE pandasite=:pandasite AND dn=:dn'
                if method == 'approve':
                    varMap[':newStatus'] = 'tobeapproved'
                else:
                    varMap[':newStatus'] = 'toberejected'                    
            elif method == 'delete':
                # delete
                sql = 'DELETE FROM ATLAS_PANDAMETA.siteaccess WHERE pandasite=:pandasite AND dn=:dn'
            elif method == 'set':
                # check value
                if re.search('^[a-z,A-Z]+:[a-z,A-Z,0-9,\,_\-]+$',attrValue) == None:
                    errStr = "Invalid argument for set : %s. Must be key:value" % attrValue
                    _logger.error("updateSiteAccess : %s" % errStr)
                    # retrun
                    return errStr
                # decompose to key and value
                tmpKey = attrValue.split(':')[0].lower()
                tmpVal = attrValue.split(':')[-1]
                # check key
                changeableKeys = ['poffset','workinggroups','rights']
                if not tmpKey in changeableKeys:
                    errStr = "%s cannot be set. Only %s are allowed" % (tmpKey,str(changeableKeys))
                    _logger.error("updateSiteAccess : %s" % errStr)
                    # retrun
                    return errStr
                # set value map
                varMap[':%s' % tmpKey] = tmpVal
                sql = 'UPDATE ATLAS_PANDAMETA.siteaccess SET %s=:%s WHERE pandasite=:pandasite AND dn=:dn' % (tmpKey,tmpKey)
            else:
                _logger.error("updateSiteAccess : Unknown method '%s'" % method)
                # return
                return "Unknown method '%s'" % method
            # execute
            _logger.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("updateSiteAccess : completed")
            return True
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("updateSiteAccess : %s %s" % (type,value))
            return 'DB error %s %s' % (type,value)

        
    # get list of archived tables
    def getArchiveTables(self):
        # return
        return ['ATLAS_PANDAARCH.jobsArchived']
    

    # get JobIDs in a time range
    def getJobIDsInTimeRangeLog(self,dn,timeRange,retJobIDs):
        comment = ' /* DBProxy.getJobIDsInTimeRangeLog */'                        
        _logger.debug("getJobIDsInTimeRangeLog : %s %s" % (dn,timeRange.strftime('%Y-%m-%d %H:%M:%S')))
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            # get list of archived tables
            tables = self.getArchiveTables()
            # select
            for table in tables:
                # make sql
                sql  = "SELECT /*+ NO_INDEX(tab JOBS_MODTIME_IDX) INDEX_COMBINE(tab JOBS_PRODSOURCELABEL_IDX JOBS_PRODUSERNAME_IDX) */ "
                sql += "jobDefinitionID FROM %s tab " % table
                sql += "WHERE prodUserName=:prodUserName AND modificationTime>:modificationTime "
                sql += "AND prodSourceLabel=:prodSourceLabel GROUP BY jobDefinitionID"
                varMap = {}
                varMap[':prodUserName'] = compactDN
                varMap[':prodSourceLabel'] = 'user'
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
            _logger.debug("getJobIDsInTimeRangeLog : %s" % str(retJobIDs))
            return retJobIDs
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobIDsInTimeRangeLog : %s %s" % (type,value))
            # return empty list
            return retJobIDs

        
    # get PandaIDs for a JobID
    def getPandIDsWithJobIDLog(self,dn,jobID,idStatus,nJobs,buildJobID=None):
        comment = ' /* Proxy.getPandIDsWithJobIDLog */'                        
        _logger.debug("getPandIDsWithJobIDLog : %s %s" % (dn,jobID))
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            # get list of archived tables
            tables = self.getArchiveTables()
            # select
            for table in tables:
                # skip if all jobs have already been gotten
                if nJobs > 0 and len(idStatus) >= nJobs:
                    continue
                # make sql
                sql  = "SELECT /*+ NO_INDEX(tab JOBS_MODTIME_IDX) INDEX_COMBINE(tab JOBS_PRODUSERNAME_IDX JOBS_JOBDEFID_IDX) */ "
                sql += "PandaID,jobStatus,commandToPilot,prodSourceLabel,taskBufferErrorCode FROM %s tab " % table                
                sql += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
                sql += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND modificationTime>(CURRENT_DATE-30) "
                varMap = {}
                varMap[':prodUserName'] = compactDN
                varMap[':jobDefinitionID'] = jobID
                varMap[':prodSourceLabel1'] = 'user'
                varMap[':prodSourceLabel2'] = 'panda'
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10000
                # select
                _logger.debug(sql+comment+str(varMap))
                self.cur.execute(sql+comment, varMap)
                resList = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # append
                for tmpID,tmpStatus,tmpCommand,tmpProdSourceLabel,tmpTaskBufferErrorCode in resList:
                    # ignore jobs retried by pilot since they have new PandaIDs with the same jobsetID/jobdefID
                    if tmpTaskBufferErrorCode in [ErrorCode.EC_PilotRetried]:
                        continue
                    # ignore old buildJob which was replaced by rebrokerage 
                    if tmpProdSourceLabel == 'panda':
                        if buildJobID == None:
                            # first buildJob
                            buildJobID = tmpID
                        elif buildJobID >= tmpID:
                            # don't append old one
                            continue
                        else:
                            # delete old one
                            del idStatus[buildJobID]
                            buildJobID = tmpID
                    # append        
                    if not idStatus.has_key(tmpID):
                        idStatus[tmpID] = (tmpStatus,tmpCommand)
            _logger.debug("getPandIDsWithJobIDLog : %s" % str(idStatus))
            return idStatus
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getPandIDsWithJobIDLog : %s %s" % (type,value))
            # return empty list
            return {}


    # get PandaIDs for a JobsetID or JobdefID in jobsArchived
    def getPandIDsWithIdInArch(self,prodUserName,id,isJobset):
        comment = ' /* Proxy.getPandIDsWithIdInArch */'                        
        _logger.debug("getPandIDsWithIdInArch : %s %s %s" % (prodUserName,id,isJobset))
        try:
            # make sql
            if isJobset:
                sql  = "SELECT /*+ NO_INDEX(tab JOBS_MODTIME_IDX) INDEX_COMBINE(tab JOBS_PRODUSERNAME_IDX JOBS_JOBSETID_IDX) */ "
            else:
                sql  = "SELECT /*+ NO_INDEX(tab JOBS_MODTIME_IDX) INDEX_COMBINE(tab JOBS_PRODUSERNAME_IDX JOBS_JOBDEFID_IDX) */ "
            sql += "PandaID FROM ATLAS_PANDAARCH.jobsArchived tab "
            sql += "WHERE prodUserName=:prodUserName "
            sql += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND modificationTime>(CURRENT_DATE-30) "
            if isJobset:
                sql += "AND jobsetID=:jobID "
            else:
                sql += "AND jobDefinitionID=:jobID "                
            varMap = {}
            varMap[':prodUserName'] = prodUserName
            varMap[':jobID'] = id
            varMap[':prodSourceLabel1'] = 'user'
            varMap[':prodSourceLabel2'] = 'panda'
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000000
            # select
            _logger.debug(sql+comment+str(varMap))            
            self.cur.execute(sql+comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # append
            pandaIDs = []
            for tmpID, in resList:
                pandaIDs.append(tmpID)
            _logger.debug("getPandIDsWithIdInArch : %s %s -> %s" % (prodUserName,id,str(pandaIDs)))
            return pandaIDs
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]            
            _logger.error("getPandIDsWithIdInArch : %s %s" % (errType,errValue))
            # return empty list
            return []

        
    # peek at job 
    def peekJobLog(self,pandaID):
        comment = ' /* DBProxy.peekJobLog */'                        
        _logger.debug("peekJobLog : %s" % pandaID)
        # return None for NULL PandaID
        if pandaID in ['NULL','','None',None]:
            return None
        sql1_0 = "SELECT %s FROM %s "
        sql1_1 = "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30) "
        # select
        varMap = {}
        varMap[':PandaID'] = pandaID
        nTry=3        
        for iTry in range(nTry):
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
                        fileTableName = re.sub('jobsArchived','filesTable_ARCH',table)
                        sqlFile = "SELECT /*+ INDEX(tab FILES_ARCH_PANDAID_IDX)*/ %s " % FileSpec.columnNames()
                        sqlFile+= "FROM %s tab " % fileTableName
                        # put constraint on modificationTime to avoid full table scan
                        sqlFile+= "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-60)"
                        self.cur.arraysize = 10000
                        self.cur.execute(sqlFile+comment, varMap)
                        resFs = self.cur.fetchall()
                        # metadata
                        job.metadata = None
                        metaTableName = re.sub('jobsArchived','metaTable_ARCH',table)
                        sqlMeta = "SELECT metaData FROM %s WHERE PandaID=:PandaID" % metaTableName
                        self.cur.execute(sqlMeta+comment, varMap)
                        for clobMeta, in self.cur:
                            if clobMeta != None:
                                job.metadata = clobMeta.read()
                            break
                        # job parameters
                        job.jobParameters = None
                        jobParamTableName = re.sub('jobsArchived','jobParamsTable_ARCH',table)
                        sqlJobP = "SELECT jobParameters FROM %s WHERE PandaID=:PandaID" % jobParamTableName
                        varMap = {}
                        varMap[':PandaID'] = job.PandaID
                        self.cur.execute(sqlJobP+comment, varMap)
                        for clobJobP, in self.cur:
                            if clobJobP != None:
                                job.jobParameters = clobJobP.read()
                            break
                        # commit
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        # set files
                        for resF in resFs:
                            file = FileSpec()
                            file.pack(resF)
                            # remove redundant white spaces
                            try:
                                file.md5sum = file.md5sum.strip()
                            except:
                                pass
                            try:
                                file.checksum = file.checksum.strip()
                            except:
                                pass
                            job.addFile(file)
                        return job
                _logger.debug("peekJobLog() : PandaID %s not found" % pandaID)
                return None
            except:
                # roll back
                self._rollback()
                if iTry+1 < nTry:
                    _logger.error("peekJobLog : %s" % pandaID)
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("peekJobLog : %s %s" % (type,value))
                # return None
                return None

        
    # get user subscriptions
    def getUserSubscriptions(self,datasetName,timeRange):
        comment = ' /* DBProxy.getUserSubscriptions */'                        
        _logger.debug("getUserSubscriptions(%s,%s)" % (datasetName,timeRange))
        sql0  = "SELECT site FROM ATLAS_PANDAMETA.UserSubs "
        sql0 += "WHERE datasetName=:datasetName and modificationDate>CURRENT_DATE-:timeRange"
        varMap = {}
        varMap[':datasetName'] = datasetName
        varMap[':timeRange']   = timeRange
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.execute(sql0+comment, varMap)
            resSs = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retList = []
            for tmpSite, in resSs:
                retList.append(tmpSite)
            return retList
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getUserSubscriptions : %s %s" % (errType,errValue))
            return []


    # get the number of user subscriptions
    def getNumUserSubscriptions(self):
        comment = ' /* DBProxy.getNumUserSubscriptions */'                        
        _logger.debug("getNumUserSubscriptions")
        sql0  = "SELECT site,COUNT(*) FROM ATLAS_PANDAMETA.UserSubs "
        sql0 += "WHERE creationDate>CURRENT_DATE-2 GROUP BY site"
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.execute(sql0+comment,{})
            resSs = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retList = {}
            for tmpSite,countNum in resSs:
                retList[tmpSite] = countNum
            return retList
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getNumUserSubscriptions : %s %s" % (errType,errValue))
            return []


    # add user subscriptions
    def addUserSubscription(self,datasetName,dq2IDs):
        comment = ' /* DBProxy.addUserSubscription */'                        
        _logger.debug("addUserSubscription(%s,%s)" % (datasetName,dq2IDs))
        sql0  = "INSERT INTO ATLAS_PANDAMETA.UserSubs "
        sql0 += "(datasetName,site,creationDate,modificationDate,nUsed) "
        sql0 += "VALUES (:datasetName,:site,CURRENT_DATE,CURRENT_DATE,:nUsed)"
        try:
            # start transaction
            self.conn.begin()
            for site in dq2IDs:
                varMap = {}
                varMap[':datasetName'] = datasetName
                varMap[':site']        = site
                varMap[':nUsed']       = 0
                # insert
                self.cur.execute(sql0+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("addUserSubscription : %s %s" % (errType,errValue))
            return False

    
    # increment counter for subscription
    def incrementUsedCounterSubscription(self,datasetName):
        comment = ' /* DBProxy.incrementUsedCounterSubscription */'                        
        _logger.debug("incrementUsedCounterSubscription(%s)" % datasetName)
        sql0  = "UPDATE ATLAS_PANDAMETA.UserSubs SET nUsed=nUsed+1 "
        sql0 += "WHERE datasetName=:datasetName AND nUsed IS NOT NULL"
        sqlU  = "SELECT MAX(nUsed) FROM ATLAS_PANDAMETA.UserSubs "
        sqlU += "WHERE datasetName=:datasetName"
        try:
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[':datasetName'] = datasetName
            # update
            self.cur.execute(sql0+comment,varMap)
            # get nUsed
            nUsed = 0
            retU = self.cur.rowcount
            if retU > 0:
                # get nUsed
                self.cur.execute(sqlU+comment,varMap)
                self.cur.arraysize = 10
                res = self.cur.fetchone()
                if res != None:
                    nUsed = res[0]
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return nUsed
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("incrementUsedCounterSubscription : %s %s" % (errType,errValue))
            return -1

        
    # get active datasets
    def getActiveDatasets(self,computingSite,prodSourceLabel):
        comment = ' /* DBProxy.getActiveDatasets */'                        
        _logger.debug("getActiveDatasets(%s,%s)" % (computingSite,prodSourceLabel))
        varMap = {}
        varMap[':computingSite']   = computingSite
        varMap[':jobStatus1']      = 'assigned'
        varMap[':jobStatus2']      = 'activated'
        varMap[':jobStatus3']      = 'waiting'
        varMap[':prodSourceLabel'] = prodSourceLabel
        try:
            retList = []
            for table in ['jobsActive4','jobsDefined4','jobsWaiting4']:
                if table == 'jobsActive4':
                    sql0  = "SELECT /*+ INDEX_COMBINE(tab JOBSACTIVE4_JOBSTATUS_IDX JOBSACTIVE4_COMPSITE_IDX) */ distinct prodDBlock FROM ATLAS_PANDA.%s tab " % table
                else:
                    sql0  = "SELECT distinct prodDBlock FROM ATLAS_PANDA.%s " % table
                sql0 += "WHERE computingSite=:computingSite AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3) "
                sql0 += "AND prodSourceLabel=:prodSourceLabel"
                # start transaction
                self.conn.begin()
                # select
                self.cur.execute(sql0+comment, varMap)
                resSs = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # append
                for prodDBlock, in resSs:
                    if not prodDBlock in retList:
                        retList.append(prodDBlock)
            # make string
            retStr = ''
            for tmpItem in retList:
                retStr += '%s,' % tmpItem
            retStr = retStr[:-1]    
            return retStr
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getActiveDatasets : %s %s" % (errType,errValue))
            return ""


    # check status of all sub datasets to trigger Notifier
    def checkDatasetStatusForNotifier(self,jobsetID,jobDefinitionID,prodUserName):
        comment = ' /* DBProxy.checkDatasetStatusForNotifier */'                        
        _logger.debug("checkDatasetStatusForNotifier(%s,%s,%s)" % (jobsetID,jobDefinitionID,prodUserName))
        try:
            # get PandaIDs to get all associated destinationDBlocks
            varMap = {}
            varMap[':jobsetID']     = jobsetID
            varMap[':prodUserName'] = prodUserName
            sql = "SELECT MAX(PandaID),jobDefinitionID FROM %s WHERE prodUserName=:prodUserName AND jobsetID=:jobsetID GROUP BY jobDefinitionID"
            pandaIDs = {}
            for table in ['ATLAS_PANDA.jobsArchived4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsWaiting4']:
                # start transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 1000                
                self.cur.execute((sql % table)+comment, varMap)
                resSs = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # get PandaIDs
                for tmpPandaID,tmpJobDefID in resSs:
                    if (not pandaIDs.has_key(tmpJobDefID)) or tmpPandaID > pandaIDs[tmpJobDefID]:
                        pandaIDs[tmpJobDefID] = tmpPandaID
            # get all destinationDBlocks
            varMap = {}
            varMap[':type1'] = 'log'
            varMap[':type2'] = 'output'            
            sql = 'SELECT DISTINCT destinationDBlock FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type IN (:type1,:type2)'
            datasetMap = {}
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000                
            for tmpJobDefID,tmpPandaID in pandaIDs.iteritems():
                varMap[':PandaID'] = tmpPandaID
                # select
                self.cur.execute(sql+comment, varMap)
                resSs = self.cur.fetchall()
                # get destinationDBlock
                for tmpDestDBlock, in resSs:
                    if not datasetMap.has_key(tmpJobDefID):
                        datasetMap[tmpJobDefID] = []
                    if not tmpDestDBlock in datasetMap[tmpJobDefID]:
                        datasetMap[tmpJobDefID].append(tmpDestDBlock)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # check status
            allClosed = True
            retInfo = {}
            latestUpdate   = None
            latestJobDefID = None
            varMap = {}
            varMap[':type1'] = 'log'
            varMap[':type2'] = 'output'            
            sql = 'SELECT status,modificationDate FROM ATLAS_PANDA.Datasets WHERE name=:name AND type IN (:type1,:type2)'
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000                
            for tmpJobDefID,tmpDatasets in datasetMap.iteritems():
                retInfo[tmpJobDefID] = []
                for tmpDataset in tmpDatasets:
                    if not tmpDataset in retInfo[tmpJobDefID]:
                        retInfo[tmpJobDefID].append(tmpDataset)
                    varMap[':name'] = tmpDataset
                    # select
                    self.cur.execute(sql+comment, varMap)
                    resSs = self.cur.fetchall()
                    # check status and mod time
                    for tmpStatus,tmpModificationDate in resSs:
                        if not tmpStatus in ['closed','tobeclosed','completed']:
                            # some datasets are still active 
                            allClosed = False
                            _logger.debug("checkDatasetStatusForNotifier(%s,%s) wait due to %s %s %s" % \
                                          (jobsetID,jobDefinitionID,tmpJobDefID,tmpDataset,tmpStatus))
                            break
                        elif tmpStatus == 'tobeclosed':
                            if latestUpdate == None or latestUpdate < tmpModificationDate:
                                # use the latest updated jobDefID
                                latestUpdate   = tmpModificationDate
                                latestJobDefID = tmpJobDefID
                            elif latestUpdate == tmpModificationDate and latestJobDefID < tmpJobDefID:
                                # use larger jobDefID when datasets are closed at the same time
                                latestJobDefID = tmpJobDefID
                    # escape
                    if not allClosed:
                        break
                # escape
                if not allClosed:
                    break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("checkDatasetStatusForNotifier(%s,%s) -> %s %s" % \
                          (jobsetID,jobDefinitionID,allClosed,latestJobDefID))
            # return
            if not allClosed or jobDefinitionID != latestJobDefID:
                return False,{}
            return True,retInfo
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("checkDatasetStatusForNotifier : %s %s" % (errType,errValue))
            return False,{}

        
    # get MoU share for T2 PD2P
    def getMouShareForT2PD2P(self):
        comment = ' /* DBProxy.getMouShareForT2PD2P */'                        
        _logger.debug("getMouShareForT2PD2P start")
        sqlG  = "SELECT gid,ntup_share FROM ATLAS_GRISLI.t_tier2_groups "
        sqlT  = "SELECT tier2,t2group,status FROM ATLAS_GRISLI.t_m4regions_replication"
        try:
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # get weight for each group
            self.cur.execute(sqlG+comment)
            resG = self.cur.fetchall()
            gidShareMap = {}
            for gid,ntup_share in resG:
                gidShareMap[gid] = {'ntup_share':ntup_share,'nSites':0}
            # get group for each site
            self.cur.execute(sqlT+comment)
            resT = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            siteGroupMap = {}
            # loop over all sites
            for tier2,t2group,t2status in resT:
                # unknown group
                if not gidShareMap.has_key(t2group):
                    _logger.error("getMouShareForT2PD2P unknown group %s for %s" % (t2group,tier2))
                    continue
                # use only DATADISK
                if not tier2.endswith('_DATADISK'):
                    continue
                # count the number of ready sites per group
                if t2status in ['ready']:
                    gidShareMap[t2group]['nSites'] += 1
                # append
                siteGroupMap[tier2] = {'group':t2group,'status':t2status}
            # normalize
            _logger.debug("getMouShareForT2PD2P normalize factor = %s" % str(gidShareMap))
            weightsMap = {}
            for tier2,t2Val in siteGroupMap.iteritems():
                t2group  = t2Val['group']
                t2status = t2Val['status']
                if gidShareMap[t2group]['ntup_share'] == 0:
                    # set 0 to be skipped in the brokerage 
                  tmpWeight = 0
                elif gidShareMap[t2group]['nSites'] > 0:
                    # normalize
                    tmpWeight = float(gidShareMap[t2group]['ntup_share']) / float(gidShareMap[t2group]['nSites'])
                else:
                    # no site is ready in this group 
                    tmpWeight = 0
                weightsMap[tier2] = {'weight':tmpWeight,'status':t2status}
            _logger.debug("getMouShareForT2PD2P -> %s" % str(weightsMap))                
            return weightsMap
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getMouShareForT2PD2P : %s %s" % (errType,errValue))
            return {}

        
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
                
    
    # commit
    def _commit(self):
        try:
            self.conn.commit()
            return True
        except:
            _logger.error("commit error")
            return False


    # rollback
    def _rollback(self,useOtherError=False):
        retVal = True
        # rollback
        _logger.debug("rollback")            
        try:
            self.conn.rollback()
        except:
            _logger.error("rollback error")
            retVal = False
        # reconnect if needed
        try:
            # get ORA ErrorCode
            errType,errValue = sys.exc_info()[:2]
            oraErrCode = str(errValue).split()[0]
            oraErrCode = oraErrCode[:-1]
            _logger.debug("rollback EC:%s %s" % (oraErrCode,errValue))
            # error codes for connection error
            error_Codes  = ['ORA-01012','ORA-01033','ORA-01034','ORA-01089',
                            'ORA-03113','ORA-03114','ORA-12203','ORA-12500',
                            'ORA-12571','ORA-03135','ORA-25402']
            # other errors are apperantly given when connection lost contact
            if useOtherError:
                error_Codes += ['ORA-01861','ORA-01008']
            if oraErrCode in error_Codes:
                # reconnect
                retFlag = self.connect(reconnect=True)
                _logger.debug("rollback reconnected %s" % retFlag)
        except:
            pass
        # return
        return retVal
