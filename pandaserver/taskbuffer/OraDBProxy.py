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
            _logger.debug("close old connection")                            
            try:
                self.conn.close()
            except:
                _logger.error("failed to close old connection")                                                
        # connect    
        try:
            self.conn = cx_Oracle.connect(dsn=self.dbhost,user=self.dbuser,
                                          password=self.dbpasswd,threaded=True)
            self.cur=self.conn.cursor()
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
    def insertNewJob(self,job,user,serNum,weight=0.0,priorityOffset=0,userVO=None):
        comment = ' /* DBProxy.insertNewJob */'                    
        sql1 = "INSERT INTO ATLAS_PANDA.jobsDefined4 (%s) " % JobSpec.columnNames()
        sql1+= JobSpec.bindValuesExpression(useSeq=True)
        sql1+= " RETURNING PandaID INTO :newPandaID"
        # make sure PandaID is NULL
        job.PandaID = None
        # job status
        job.jobStatus='defined'
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
            job.currentPriority = 1000 + priorityOffset - (serNum / 5) - int(100 * weight)
        elif job.prodSourceLabel == 'panda':
            job.currentPriority = 2000 + priorityOffset
        # usergroup
        if job.prodSourceLabel == 'regional':
            job.computingSite= "BNLPROD"
        try:
            # begin transaction
            self.conn.begin()
            # insert
            varMap = job.valuesMap(useSeq=True)
            varMap[':newPandaID'] = self.cur.var(cx_Oracle.NUMBER)
            retI = self.cur.execute(sql1+comment, varMap)
            # set PandaID
            job.PandaID = long(varMap[':newPandaID'].getvalue())
            # insert files
            _logger.debug("insertNewJob : %s Label : %s ret : %s" % (job.PandaID,job.prodSourceLabel,retI))
            sqlFile = "INSERT INTO ATLAS_PANDA.filesTable4 (%s) " % FileSpec.columnNames()
            sqlFile+= FileSpec.bindValuesExpression(useSeq=True)
            sqlFile+= " RETURNING row_ID INTO :newRowID"
            for file in job.Files:
                file.row_ID = None
                if file.status != 'ready':
                    file.status='unknown'
                # replace $PANDAID with real PandaID
                file.lfn = re.sub('\$PANDAID', '%05d' % job.PandaID, file.lfn)
                varMap = file.valuesMap(useSeq=True)
                varMap[':newRowID'] = self.cur.var(cx_Oracle.NUMBER)
                self.cur.execute(sqlFile+comment, varMap)
                # get rowID
                file.row_ID = long(varMap[':newRowID'].getvalue())
            # metadata
            if job.prodSourceLabel in ['user','panda']:
                sqlMeta = "INSERT INTO ATLAS_PANDA.metaTable (PandaID,metaData) VALUES (:PandaID,:metaData)"
                varMap = {}
                varMap[':PandaID']  = job.PandaID
                varMap[':metaData'] = job.metadata
                self.cur.execute(sqlMeta+comment, varMap)
            # job parameters
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
        sql1 = "UPDATE ATLAS_PANDA.jobsDefined4 SET jobStatus=:newJobStatus "
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
                    # update. Not delete for InnoDB
                    varMap = {}
                    varMap[':PandaID']       = job.PandaID
                    varMap[':newJobStatus']  = 'activated'
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
                        sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                        for file in job.Files:
                            varMap = file.valuesMap()
                            varMap[':row_ID'] = file.row_ID
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
                        sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                        for file in job.Files:
                            varMap = file.valuesMap()
                            varMap[':row_ID'] = file.row_ID
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
        sql1 = "UPDATE ATLAS_PANDA.jobsDefined4 SET jobStatus=:newJobStatus "
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
                varMap[':newJobStatus']  = 'waiting'
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
                    sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                    for file in job.Files:
                        varMap = file.valuesMap()
                        varMap[':row_ID'] = file.row_ID
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
            sql1 = "UPDATE ATLAS_PANDA.jobsDefined4 SET jobStatus=:newJobStatus WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
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
                    varMap[':newJobStatus']  = 'failed'
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
                    sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                    for file in job.Files:
                        varMap = file.valuesMap()
                        varMap[':row_ID'] = file.row_ID
                        self.cur.execute(sqlF+comment, varMap)
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
                    # look for downstream jobs
                    sqlD   = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 WHERE type=:type AND lfn=:lfn GROUP BY PandaID"
                    sqlDJS = "SELECT %s " % JobSpec.columnNames()
                    sqlDJS+= "FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID"
                    sqlDJD = "UPDATE ATLAS_PANDA.jobsDefined4 SET jobStatus=:jobStatus WHERE PandaID=:PandaID"
                    sqlDJI = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
                    sqlDJI+= JobSpec.bindValuesExpression()
                    for upFile in upOutputs:
                        _logger.debug("look for downstream jobs for %s" % upFile)
                        # select PandaID
                        varMap = {}
                        varMap[':lfn']  = upFile
                        varMap[':type'] = 'input'
                        self.cur.arraysize = 3000                        
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
                            varMap[':jobStatus'] = 'failed'
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
                                # get PandaIDs
                                varMap = {}
                                varMap[':jobStatus'] = 'assigned'
                                varMap[':dispatchDBlock'] = disName
                                self.cur.execute("SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 WHERE dispatchDBlock=:dispatchDBlock AND jobStatus=:jobStatus "+comment,
                                                 varMap)
                                resDDM = self.cur.fetchall()
                                for tmpID, in resDDM:
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
                    sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                    for file in job.Files:
                        varMap = file.valuesMap()
                        varMap[':row_ID'] = file.row_ID
                        self.cur.execute(sqlF+comment, varMap)
                # delete downstream jobs
                if job.prodSourceLabel == 'panda' and job.jobStatus == 'failed':
                    # file select
                    sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                    sqlFile+= "WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    self.cur.arraysize = 1000
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
                    sqlDJD = "UPDATE ATLAS_PANDA.jobsDefined4 SET jobStatus=:jobStatus WHERE PandaID=:PandaID"
                    sqlDJI = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
                    sqlDJI+= JobSpec.bindValuesExpression()
                    for upFile in upOutputs:
                        _logger.debug("look for downstream jobs for %s" % upFile)
                        # select PandaID
                        varMap = {}
                        varMap[':lfn'] = upFile
                        varMap[':type'] = 'input'
                        self.cur.arraysize = 3000
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
                            varMap[':jobStatus'] = 'failed'
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


    # update Job status in jobsActive
    def updateJobStatus(self,pandaID,jobStatus,param):
        comment = ' /* DBProxy.updateJobStatus */'        
        _logger.debug("updateJobStatus : %s" % pandaID)
        sql1 = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:jobStatus,modificationTime=CURRENT_DATE"
        if jobStatus in ['starting']:
            sql1 += ",stateChangeTime=CURRENT_DATE"
        varMap = {}
        varMap[':jobStatus'] = jobStatus
        for key in param.keys():
            if param[key] != None:
                sql1 += ',%s=:%s' % (key,key)
                varMap[':%s' % key] = param[key]
        sql1 += " WHERE PandaID=:PandaID"
        varMap[':PandaID'] = pandaID
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.conn.begin()
                # update
                self.cur.execute (sql1+comment,varMap)
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
                    sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                    for file in job.Files:
                        varMap = file.valuesMap()
                        varMap[':row_ID'] = file.row_ID
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
    def retryJob(self,pandaID,param):
        comment = ' /* DBProxy.retryJob */'                
        _logger.debug("retryJob : %s" % pandaID)        
        sql1 = "SELECT %s FROM ATLAS_PANDA.jobsActive4 " % JobSpec.columnNames()
        sql1+= "WHERE PandaID=:PandaID"
        sql2 = "UPDATE ATLAS_PANDA.jobsActive4 SET %s " % JobSpec.bindUpdateExpression()            
        sql2+= "WHERE PandaID=:PandaID"
        nTry=3
        for iTry in range(nTry):
            try:
                retValue = False
                # begin transaction
                self.conn.begin()
                # select
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.arraysize = 10                
                self.cur.execute(sql1+comment, varMap)
                res = self.cur.fetchall()
                if len(res) == 0:
                    _logger.debug("retryJob() : PandaID %d not found" % pandaID)
                    self._rollback()
                    return retValue
                job = JobSpec()
                job.pack(res[0])
                # check if it's analysis job
                if (((job.prodSourceLabel == 'user' or job.prodSourceLabel == 'panda') \
                     and job.computingSite.startswith('ANALY_') and param.has_key('pilotErrorCode') \
                     and param['pilotErrorCode'] in ['1200','1201'] and (not job.computingSite.startswith('ANALY_LONG_')) \
                     and job.attemptNr < 2) or (job.prodSourceLabel == 'ddm' and job.cloud == 'CA' and job.attemptNr <= 10)) \
                     and job.commandToPilot != 'tobekilled':
                    _logger.debug(' -> reset PandaID:%s #%s' % (job.PandaID,job.attemptNr))
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
                    # send it to long queue for analysis jobs
                    oldComputingSite = job.computingSite
                    if job.computingSite.startswith('ANALY') and (not job.computingSite.startswith('ANALY_LONG_')):
                        longSite = re.sub('^ANALY_','ANALY_LONG_',job.computingSite)
                        longSite = re.sub('_\d+$','',longSite)
                        if longSite in PandaSiteIDs.keys():
                            job.computingSite = longSite
                            # set destinationSE if queue is changed
                            if oldComputingSite == job.destinationSE:
                                job.destinationSE = job.computingSite
                    # select files
                    varMap = {}
                    varMap[':PandaID'] = job.PandaID
                    varMap[':type1'] = 'log'
                    varMap[':type2'] = 'output'
                    sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                    sqlFile+= "WHERE PandaID=:PandaID AND (type=:type1 OR type=:type2)"
                    self.cur.arraysize = 100
                    self.cur.execute(sqlFile+comment, varMap)
                    resFs = self.cur.fetchall()
                    for resF in resFs:
                        # set PandaID
                        file = FileSpec()
                        file.pack(resF)
                        job.addFile(file)
                        # set new GUID
                        if file.type == 'log':
                            file.GUID = commands.getoutput('uuidgen')
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
                        # update
                        varMap = file.valuesMap()
                        varMap[':row_ID'] = file.row_ID
                        sqlFup = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                        self.cur.execute(sqlFup+comment, varMap)
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
                    # set return
                    retValue = True
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

        
    # get jobs
    def getJobs(self,nJobs,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                atlasRelease,prodUserID,countryGroup,workingGroup):
        comment = ' /* DBProxy.getJobs */'
        dynamicBrokering = False
        getValMap = {}
        sql1 = "WHERE jobStatus=:oldJobStatus AND computingSite=:computingSite AND commandToPilot IS NULL "
        getValMap[':oldJobStatus'] = 'activated'
        getValMap[':computingSite'] = siteName
        if not mem in [0,'0']:
            sql1+= "AND (minRamCount<=:minRamCount OR minRamCount=0) "
            getValMap[':minRamCount'] = mem
        if not diskSpace in [0,'0']:
            sql1+= "AND (maxDiskCount<:maxDiskCount OR maxDiskCount=0) "
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
            sql1+= "AND (prodSourceLabel=:prodSourceLabel1 OR prodSourceLabel=:prodSourceLabel2) "
            getValMap[':prodSourceLabel1'] = 'managed'
            getValMap[':prodSourceLabel2'] = 'test'
        elif prodSourceLabel == 'software':
            sql1+= "AND prodSourceLabel=:prodSourceLabel "
            getValMap[':prodSourceLabel'] = 'software'
        elif prodSourceLabel == 'test' and computingElement != None:
            dynamicBrokering = True
            sql1+= "AND (computingElement=:computingElement1 OR computingElement=:computingElement2 OR processingType=:processingType OR prodSourceLabel=:prodSourceLabel) "
            getValMap[':computingElement1'] = computingElement
            getValMap[':computingElement2'] = 'to.be.set'
            getValMap[':processingType']    = 'prod_test'
            getValMap[':prodSourceLabel']   = 'test'
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
        if prodSourceLabel == 'user' or (isinstance(prodSourceLabel,types.StringType) and re.search('test',prodSourceLabel) != None): 
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
                        # get max priority for analysis jobs
                        if prodSourceLabel in ['panda','user']:
                            sqlMX = "SELECT /*+ INDEX_COMBINE(tab JOBSACTIVE4_JOBSTATUS_IDX JOBSACTIVE4_COMPSITE_IDX) */ MAX(currentPriority) FROM ATLAS_PANDA.jobsActive4 tab "
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
                        if toGetPandaIDs:
                            # get PandaIDs
                            sqlP = "SELECT /*+ INDEX_COMBINE(tab JOBSACTIVE4_JOBSTATUS_IDX JOBSACTIVE4_COMPSITE_IDX) */ PandaID,currentPriority FROM ATLAS_PANDA.jobsActive4 tab "
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
                            for tmpPandaID,tmpCurrentPriority in resIDs:
                                if maxCurrentPriority==None or maxCurrentPriority < tmpCurrentPriority:
                                    maxCurrentPriority = tmpCurrentPriority
                                    pandaIDs = [tmpPandaID]
                                elif maxCurrentPriority == tmpCurrentPriority:
                                    pandaIDs.append(tmpPandaID)
                            # sort
                            pandaIDs.sort()
                        if pandaIDs == []:
                            _logger.debug("getJobs : %s -> no PandaIDs" % strName)
                            retU = 0
                        else:
                            # update
                            for indexID,tmpPandaID in enumerate(pandaIDs):
                                # max attempts
                                if indexID > 10:
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
    def resetJob(self,pandaID,activeTable=True,keepSite=False):
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
            if job.jobStatus != 'waiting' and job.jobStatus != 'activated':
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # return
                return None
            # delete
            varMap = {}
            varMap[':PandaID'] = pandaID
            varMap[':oldJobStatus1'] = 'waiting'
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
            # insert
            self.cur.execute(sql3+comment, job.valuesMap())
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
                # reset GUID to trigger LRC/LFC scanning
                if file.status == 'missing':
                    file.GUID = None
                # reset status, destinationDBlock and dispatchDBlock
                file.status         ='unknown'
                file.dispatchDBlock = None
                file.destinationDBlock = re.sub('_sub\d+$','',file.destinationDBlock)
                # add file
                job.addFile(file)                
                # update files
                sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                varMap = file.valuesMap()
                varMap[':row_ID'] = file.row_ID
                self.cur.execute(sqlF+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
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
    def resetDefinedJob(self,pandaID,keepSite=False):
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
                    # reset status, destinationDBlock and dispatchDBlock
                    file.status         ='unknown'
                    file.dispatchDBlock = None
                    file.destinationDBlock = re.sub('_sub\d+$','',file.destinationDBlock)
                    # add file
                    job.addFile(file)
                    # update files
                    sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % FileSpec.bindUpdateExpression()) + "WHERE row_ID=:row_ID"
                    varMap = file.valuesMap()
                    varMap[':row_ID'] = file.row_ID
                    self.cur.execute(sqlF+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return job
        except:
            # error report
            type, value, traceBack = sys.exc_info()
            _logger.error("resetDefinedJobs : %s %s" % (type,value))
            # roll back
            self._rollback()
            return None


    # kill job
    def killJob(self,pandaID,user,code,prodManager):
        comment = ' /* DBProxy.killJob */'        
        _logger.debug("killJob : %s %s %s %s" % (code,pandaID,prodManager,user))
        # check PandaID
        try:
            long(pandaID)
        except:
            _logger.error("not an integer : %s" % pandaID)
            return False
        sql0 = "SELECT prodUserID FROM %s WHERE PandaID=:PandaID"        
        sql1 = "UPDATE %s SET commandToPilot=:commandToPilot WHERE PandaID=:PandaID AND commandToPilot IS NULL"
        sql2 = "SELECT %s " % JobSpec.columnNames()
        sql2+= "FROM %s WHERE PandaID=:PandaID AND jobStatus<>:jobStatus"
        sql3 = "DELETE FROM %s WHERE PandaID=:PandaID"
        sqlU = "UPDATE ATLAS_PANDA.jobsDefined4 SET jobStatus=:newJobStatus WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
        sql4 = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
        sql4+= JobSpec.bindValuesExpression()
        try:
            flagCommand = False
            flagKilled  = False
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
                if (not prodManager) and (not user.startswith('/DC=org/DC=doegrids/OU=People/CN=Nurcan Ozturk')) \
                       and (not user.startswith('/DC=org/DC=doegrids/OU=People/CN=Torre Wenaus')):
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
                        return distinguishedName
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
                self.cur.execute((sql1+comment) % table, varMap)
                retU = self.cur.rowcount
                if retU == 0:
                    continue
                # set flag
                flagCommand = True
                # select
                varMap = {}
                varMap[':PandaID'] = pandaID
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
                    varMap[':newJobStatus'] = 'failed'
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
                job.jobStatus = 'failed'
                job.endTime   = datetime.datetime.utcnow()
                job.modificationTime = job.endTime
                job.stateChangeTime  = job.modificationTime
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
                else:
                    # killed
                    job.taskBufferErrorCode = ErrorCode.EC_Kill
                    job.taskBufferErrorDiag = 'killed by %s' % user
                # insert
                self.cur.execute(sql4+comment, job.valuesMap())
                flagKilled = True
                break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("killJob : com=%s kill=%s " % (flagCommand,flagKilled))
            return (flagCommand or flagKilled)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("killJob : %s %s" % (type,value))
            # roll back
            self._rollback()
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
                if fromActive:
                    tables.append('ATLAS_PANDA.jobsActive4')
                if fromArchived:
                    tables.append('ATLAS_PANDA.jobsArchived4')
                if fromWaiting:
                    tables.append('ATLAS_PANDA.jobsWaiting4')
                if fromDefined:
                    # defined needs to be the last one due to InnoDB's auto_increment
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
                                resMeta = clobMeta.read()
                                break
                        # job parameters
                        job.jobParameters = None
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
            tables = ['ATLAS_PANDA.jobsArchived4','ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsWaiting4','ATLAS_PANDA.jobsDefined4']
            # select
            for table in tables:
                # skip if all jobs have already been gotten
                if nJobs > 0 and len(idStatus) >= nJobs:
                    continue
                # make sql
                sql  = "SELECT PandaID,jobStatus,commandToPilot FROM %s " % table                
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
        sql0 = "SELECT PandaID,jobStatus,stateChangeTime,attemptNr,jobDefinitionID,jobExecutionID FROM %s "
        sql0+= "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND lockedby=:lockedby "
        sql0+= "AND stateChangeTime>prodDBUpdateTime "
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
                sql += sql1    
                self.cur.arraysize = limit                
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchall()
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
            elif param['jobStatus'] in ['waiting']:
                table = 'ATLAS_PANDA.jobsWaiting4'
            elif param['jobStatus'] in ['activated','sent','starting','running','holding','transferring']:
                table = 'ATLAS_PANDA.jobsActive4'
            elif param['jobStatus'] in ['finished','failed']:
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


    # query dataset with map
    def queryDatasetWithMap(self,map):
        comment = ' /* DBProxy.queryDatasetWithMap */'               
        _logger.debug("queryDatasetWithMap(%s)" % map)
        if map.has_key('name'):
            sql1 = "SELECT /*+ index(tab DATASETS_NAME_IDX) */ %s FROM ATLAS_PANDA.Datasets tab" % DatasetSpec.columnNames()
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
        sql1 = "DELETE FROM ATLAS_PANDA.Datasets WHERE name=:name"
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
    def getSerialNumber(self,datasetname):
        comment = ' /* DBProxy.getSerialNumber */'        
        try:
            _logger.debug("getSerialNumber(%s)" % datasetname)
            # start transaction
            self.conn.begin()
            # select
            varMap = {}
            varMap[':name'] = datasetname
            varMap[':type'] = 'output'
            sql = "SELECT /*+ INDEX(tab DATASETS_NAME_IDX)*/ COUNT(*) FROM ATLAS_PANDA.Datasets tab WHERE type=:type AND name=:name"
            self.cur.arraysize = 100            
            self.cur.execute(sql+comment, varMap)
            res = self.cur.fetchone()
            # fresh dataset or not
            if res != None and len(res) != 0 and res[0] > 0:
                freshFlag = False
            else:
                freshFlag = True
            # get serial number
            sql = "SELECT ATLAS_PANDA.SUBCOUNTER_SUBID_SEQ.nextval FROM dual";
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


    # update transfer status for a dataset
    def updateTransferStatus(self,datasetname,bitMap):
        comment = ' /* DBProxy.updateTransferStatus */'        
        try:
            _logger.debug("updateTransferStatus(%s,%s)" % (datasetname,hex(bitMap)))
            # start transaction
            self.conn.begin()
            retTransSt = 0
            # update bitmap
            sqlU = "UPDATE /*+ INDEX(tab DATASETS_NAME_IDX)*/ ATLAS_PANDA.Datasets tab SET transferStatus=ATLAS_PANDA.BITOR(transferStatus,:bitMap) WHERE name=:name"
            varMap = {}
            varMap[':bitMap'] = bitMap
            varMap[':name'] = datasetname
            retU = self.cur.execute(sqlU+comment, varMap)
            # get transferStatus
            sqlS = "SELECT /*+ INDEX(tab DATASETS_NAME_IDX)*/ transferStatus FROM ATLAS_PANDA.Datasets tab WHERE name=:name"
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
                # update tmod if status <> 'assigned'
                if cloudTask.status <> 'assigned':
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
            sql = "SELECT taskid FROM ATLAS_PANDA.cloudtasks WHERE status<>:status AND tmod>:tmod"
            varMap = {}
            varMap[':tmod']   = timeLimit
            varMap[':status'] = 'assigned'
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


    # get input files currently in use for analysis
    def getFilesInUseForAnal(self,outDataset):
        comment = ' /* DBProxy.getFilesInUseForAnal */'        
        sqlSub  = "SELECT /*+ index(tab FILESTABLE4_DATASET_IDX) */ destinationDBlock,PandaID FROM ATLAS_PANDA.filesTable4 tab "
        sqlSub += "WHERE dataset=:dataset AND type=:type AND status=:fileStatus GROUP BY destinationDBlock,PandaID"
        sqlPan  = "SELECT jobStatus FROM ATLAS_PANDA.jobsArchived4 WHERE PandaID=:PandaID"
        sqlDis  = "SELECT distinct dispatchDBlock FROM ATLAS_PANDA.filesTable4 "
        sqlDis += "WHERE PandaID=:PandaID AND type=:type"
        sqlLfn  = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ lfn,PandaID FROM ATLAS_PANDA.filesTable4 tab "
        sqlLfn += "WHERE dispatchDBlock=:dispatchDBlock AND type=:type"
        nTry=3
        for iTry in range(nTry):
            inputFilesList = []
            try:
                # start transaction
                self.conn.begin()
                # get sub datasets
                varMap = {}
                varMap[':dataset'] = outDataset
                varMap[':type'] = 'output'
                varMap[':fileStatus'] = 'unknown'                
                _logger.debug("getFilesInUseForAnal : %s %s" % (sqlSub,str(varMap)))
                self.cur.arraysize = 10000
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
                    # reuse failed files
                    if checkedPandaIDs[pandaID] == 'failed':
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
                        _logger.debug("getFilesInUseForAnal : %s %s" % (sqlLfn,str(varMap)))
                        self.cur.arraysize = 100000
                        retL = self.cur.execute(sqlLfn+comment, varMap)
                        resL = self.cur.fetchall()
                        # append
                        for lfn,filePandaID in resL:
                            # skip files used by finished/failed jobs
                            if filePandaID in activePandaIDs:
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


    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self,dataset,status):
        comment = ' /* DBProxy.updateInFilesReturnPandaIDs */'                                
        _logger.debug("updateInFilesReturnPandaIDs(%s)" % dataset)
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status=:status WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        varMap = {}
        varMap[':status'] = status
        varMap[':dispatchDBlock'] = dataset
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


    # set GUIDs
    def setGUIDs(self,files):
        comment = ' /* DBProxy.setGUIDs */'                        
        _logger.debug("setGUIDs(%s)" % files)
        sql0 = "UPDATE ATLAS_PANDA.filesTable4 SET GUID=:GUID WHERE lfn=:lfn"
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                # update
                for file in files:
                    varMap = {}
                    varMap[':GUID'] = file['guid']
                    varMap[':lfn']  = file['lfn']
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
        sql1 = "SELECT /*+ index(tab FILESTABLE4_DATASET_IDX) */ MAX(PandaID) FROM ATLAS_PANDA.filesTable4 tab WHERE dataset=:dataset AND type=:type"
        sql2 = "SELECT lfn FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type"
        # execute
        try:
            retMap = {}
            for dataset in datasets:
                # start transaction
                self.conn.begin()
                # select PandaID
                varMap = {}
                varMap[':type'] = 'output'
                varMap[':dataset'] = dataset
                self.cur.arraysize = 10                
                self.cur.execute(sql1+comment, varMap)
                res = self.cur.fetchone()
                # found
                retList = []
                if res != None:
                    pandaID = res[0]
                    # select LFNs
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    varMap[':type'] = 'output'
                    self.cur.arraysize = 100
                    self.cur.execute(sql2+comment, varMap)
                    res = self.cur.fetchall()
                    for r in res:
                        retList.append(r[0])
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
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
    def getJobStatistics(self,archived=False,predefined=False,workingGroup='',countryGroup=''):
        comment = ' /* DBProxy.getJobStatistics */'        
        _logger.debug("getJobStatistics(%s,%s,'%s','%s')" % (archived,predefined,workingGroup,countryGroup))
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        sql0 = "SELECT computingSite,jobStatus,COUNT(*) FROM %s WHERE prodSourceLabel in (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3,:prodSourceLabel4,:prodSourceLabel5) "
        if predefined:
            sql0 += "AND relocationFlag=1 "
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
        sql0 += "GROUP BY computingSite,jobStatus"
        sqlA =  "SELECT /*+ index(tab JOBSARCHIVED4_MODTIME_IDX) */ computingSite,jobStatus,COUNT(*) FROM ATLAS_PANDA.jobsArchived4 tab WHERE modificationTime>:modificationTime "
        sqlA += "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3,:prodSourceLabel4,:prodSourceLabel5) " 
        if predefined:
            sqlA += "AND relocationFlag=1 "
        sqlA += sqlGroups                
        sqlA += "GROUP BY computingSite,jobStatus"
        tables = ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4']
        if archived:
            tables.append('ATLAS_PANDA.jobsArchived4')
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
                    varMap[':prodSourceLabel2'] = 'user'
                    varMap[':prodSourceLabel3'] = 'panda'
                    varMap[':prodSourceLabel4'] = 'ddm'
                    varMap[':prodSourceLabel5'] = 'rc_test'
                    for tmpGroup in tmpGroupMap.keys():
                        varMap[tmpGroup] = tmpGroupMap[tmpGroup]
                    if table != 'ATLAS_PANDA.jobsArchived4':
                        self.cur.arraysize = 10000                        
                        self.cur.execute((sql0+comment) % table, varMap)
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
                _logger.debug("getJobStatistics() : %s" % str(ret))
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


    # get job statistics for brokerage
    def getJobStatisticsBrokerage(self):
        comment = ' /* DBProxy.getJobStatisticsBrokerage */'        
        _logger.debug("getJobStatisticsBrokerage()")
        sql0 = "SELECT computingSite,jobStatus,processingType,COUNT(*) FROM %s WHERE "
        sql0 += "prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3,:prodSourceLabel4,:prodSourceLabel5) "
        sql0 += "GROUP BY computingSite,jobStatus,processingType"
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
                    varMap[':prodSourceLabel2'] = 'user'
                    varMap[':prodSourceLabel3'] = 'panda'
                    varMap[':prodSourceLabel4'] = 'ddm'
                    varMap[':prodSourceLabel5'] = 'rc_test'                    
                    self.cur.arraysize = 10000                        
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
            _logger.debug("getJobStatisticsForExtIF() : %s" % str(ret))
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
            _logger.debug("getJobStatisticsPerProcessingType() : %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobStatisticsPerProcessingType : %s %s" % (type, value))
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
        
    # get site data
    def getCurrentSiteData(self):
        comment = ' /* DBProxy.getCurrentSiteData */'                            
        _logger.debug("getCurrentSiteData")
        sql = "SELECT SITE,getJob,updateJob FROM ATLAS_PANDAMETA.SiteData WHERE FLAG=:FLAG and HOURS=3"
        varMap = {}
        varMap[':FLAG'] = 'production'
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
            for item in res:
                ret[item[0]] = {'getJob':item[1],'updateJob':item[2]}
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
            sql+= "validatedreleases,accesscontrol "
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
                       validatedreleases,accesscontrol \
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
                    ret += tmpItem.split(',')
            _logger.debug("getPilotOwners -> %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            type,value,traceBack = sys.exc_info()
            _logger.error("getPilotOwners : %s %s" % (type,value))
            return []


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
        comment = ' /* DBProxy.getUserParameter */'                            
        _logger.debug("getUserParameter %s %s" % (dn,jobID))
        try:
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
                varMap = {}
                varMap[':name'] = name
                varMap[':jobid'] = retJobID
                sql = "UPDATE ATLAS_PANDAMETA.users SET jobid=:jobid WHERE name=:name"
                self.cur.execute(sql+comment,varMap)
                _logger.debug("getUserParameter set JobID=%s for %s" % (retJobID,dn))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return retJobID,retStatus
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getUserParameter : %s %s" % (type,value))
            # roll back
            self._rollback()
            return jobID,True

        
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
            sql = 'SELECT cloud FROM ATLAS_PANDAMETA.schedconfig WHERE siteid=:pandasite AND rownum<=1'
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
            # check privilege
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
                sql  = "SELECT /*+ INDEX_COMBINE(tab JOBS_MODTIME_IDX JOBS_PRODUSERNAME_IDX) */ jobDefinitionID FROM %s tab " % table
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
    def getPandIDsWithJobIDLog(self,dn,jobID,idStatus,nJobs):
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
                sql  = "SELECT PandaID,jobStatus,commandToPilot FROM %s " % table                
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
            _logger.debug("getPandIDsWithJobIDLog : %s" % str(idStatus))
            return idStatus
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getPandIDsWithJobIDLog : %s %s" % (type,value))
            # return empty list
            return {}

        
    # peek at job 
    def peekJobLog(self,pandaID):
        comment = ' /* DBProxy.peekJobLog */'                        
        _logger.debug("peekJob : %s" % pandaID)
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
                        sqlFile = "SELECT %s " % FileSpec.columnNames()
                        sqlFile+= "FROM %s " % fileTableName
                        sqlFile+= "WHERE PandaID=:PandaID"
                        self.cur.arraysize = 10000
                        self.cur.execute(sqlFile+comment, varMap)
                        resFs = self.cur.fetchall()
                        # metadata
                        job.metadata = None
                        metaTableName = re.sub('jobsArchived','metaTable_ARCH',table)
                        sqlMeta = "SELECT metaData FROM %s WHERE PandaID=:PandaID" % metaTableName
                        self.cur.execute(sqlMeta+comment, varMap)
                        for clobMeta, in self.cur:
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
                
