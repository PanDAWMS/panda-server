"""
proxy for database connection


obsolete 2013-08-14
"""

import re
import os
import sys
import time
import fcntl
import random
import urllib
import MySQLdb
import datetime
import commands
import traceback
import warnings
import ErrorCode
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
    def __init__(self):
        # connection object
        self.conn = None
        # cursor object
        self.cur = None
        # host name
        self.hostname = None
        # retry count
        self.nTry = 5
        
    # connect to DB
    def connect(self,dbhost=panda_config.dbhost,dbpasswd=panda_config.dbpasswd,
                dbuser=panda_config.dbuser,dbname=panda_config.dbname,
                dbtimeout=None,reconnect=False):
        # keep parameters for reconnect
        if not reconnect:
            self.dbhost    = dbhost
            self.dbpasswd  = dbpasswd
            self.dbuser    = dbuser
            self.dbname    = dbname
            self.dbtimeout = dbtimeout
        # connect    
        try:
            if self.dbtimeout == None:
                self.conn = MySQLdb.connect(host=self.dbhost,user=self.dbuser,
                                            passwd=self.dbpasswd,db=self.dbname)
            else:
                self.conn = MySQLdb.connect(host=self.dbhost,user=self.dbuser,
                                            passwd=self.dbpasswd,db=self.dbname,
                                            connect_timeout=self.dbtimeout)
            self.cur=self.conn.cursor()
            # get hostname
            self.cur.execute('SELECT USER()')
            res = self.cur.fetchone()
            match = re.search('^([^@]+)@([^@]+)$',res[0])
            if match != None:
                self.hostname = match.group(2)
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("connect : %s %s" % (type,value))
            # roll back
            self._rollback()
            return False


    # query an SQL   
    def querySQL(self,sql):
        comment = ' /* DBProxy.querySQL */'
        try:
            _logger.debug("querySQL : %s " % sql)            
            # begin transaction
            self.cur.execute("START TRANSACTION")
            self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return res
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("querySQL : %s " % sql)
            _logger.error("querySQL : %s %s" % (type,value))
            return None


    # query an SQL return Status  
    def querySQLS(self,sql):
        comment = ' /* DBProxy.querySQLS */'            
        try:
            # begin transaction
            self.cur.execute("SET AUTOCOMMIT=1")
            ret = self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return ret,res
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("querySQLS : %s " % sql)
            _logger.error("querySQLS : %s %s" % (type,value))
            return -1,None


    # query an SQL with list return Status  
    def querySQLwList(self,sql,valList):
        comment = ' /* DBProxy.querySQLwList */'            
        try:
            # begin transaction
            self.cur.execute("SET AUTOCOMMIT=1")
            ret = self.cur.execute(sql+comment,valList)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return ret,res
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("querySQLwList : %s %s" % (sql,str(valList)))
            _logger.error("querySQLwList : %s %s" % (type,value))
            return -1,None


    # insert job to jobsDefined
    def insertNewJob(self,job,user,serNum,weight=0.0,priorityOffset=0,userVO=None):
        comment = ' /* DBProxy.insertNewJob */'                    
        sql1 = "INSERT INTO jobsDefined4 (%s) " % JobSpec.columnNames()
        sql1+= JobSpec.valuesExpression()
        # make sure PandaID is NULL
        job.PandaID = None
        # job status
        job.jobStatus='defined'
        # host and time information
        job.modificationHost = self.hostname
        job.creationTime     = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
        job.modificationTime = job.creationTime
        job.stateChangeTime  = job.creationTime
        # DN
        if job.prodUserID == "NULL" or job.prodSourceLabel in ['user','panda']:
            job.prodUserID = user
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
            self.cur.execute("START TRANSACTION")
            # insert
            retI = self.cur.execute(sql1+comment, job.values())
            # set PandaID
            job.PandaID = self.conn.insert_id()
            # insert files
            _logger.debug("insertNewJob : %s Label : %s ret : %s" % (job.PandaID,job.prodSourceLabel,retI))
            sqlFile = "INSERT INTO filesTable4 (%s) " % FileSpec.columnNames()
            sqlFile+= FileSpec.valuesExpression()
            for file in job.Files:
                file.rowID = None
                if file.status != 'ready':
                    file.status='unknown'
                # replace $PANDAID with real PandaID
                file.lfn = re.sub('\$PANDAID', '%05d' % job.PandaID, file.lfn)
                self.cur.execute(sqlFile+comment, file.values())
                # get rowID
                file.rowID = self.conn.insert_id()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("insertNewJob : %s File OK" % job.PandaID)
            # update job info in MonALISA - Job Defined.
            #aThr = apmonInterface(job)
            #aThr.start()
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("insertNewJob : %s %s" % (type,value))
            # roll back
            self._rollback()
            return False


    # simply insert job to a table
    def insertJobSimple(self,job,table,fileTable):
        comment = ' /* DBProxy.insertJobSimple */'                            
        _logger.debug("insertJobSimple : %s" % job.PandaID)
        sql1 = "INSERT INTO %s (%s) " % (table,JobSpec.columnNames())
        sql1+= JobSpec.valuesExpression()
        try:
            # begin transaction
            self.cur.execute("START TRANSACTION")
            # insert
            self.cur.execute(sql1+comment, job.values())
            # files
            sqlFile = "INSERT INTO %s " % fileTable
            sqlFile+= "(%s) " % FileSpec.columnNames()
            sqlFile+= FileSpec.valuesExpression()
            for file in job.Files:
                self.cur.execute(sqlFile+comment, file.values())
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


    # activate job. move job from jobsDefined to jobsActive 
    def activateJob(self,job):
        comment = ' /* DBProxy.activateJob */'        
        if job==None:
            _logger.debug("activateJob : None")
            return True
        _logger.debug("activateJob : %s" % job.PandaID)                        
        sql0 = "SELECT rowID FROM filesTable4 WHERE PandaID=%s AND type=%s AND status!=%s"
        sql1 = "UPDATE jobsDefined4 SET jobStatus='activated' "
        sql1+= "WHERE PandaID=%s AND (jobStatus='assigned' OR jobStatus='defined') AND commandToPilot<>'tobekilled'"
        sql2 = "INSERT INTO jobsActive4 (%s) " % JobSpec.columnNames()
        sql2+= JobSpec.valuesExpression()
        # host and time information
        job.modificationTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
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
                self.cur.execute("START TRANSACTION")
                # check all inputs are ready
                self.cur.execute(sql0+comment, (job.PandaID,"input","ready"))
                res = self.cur.fetchall()
                if len(res) == 0 or allOK:
                    # change status
                    job.jobStatus = "activated"
                    # update. Not delete for InnoDB
                    n = self.cur.execute(sql1+comment, (job.PandaID,))
                    if n==0:
                        # already killed or activated
                        _logger.debug("activateJob : Not found %s" % job.PandaID)
                    else:
                        # insert
                        self.cur.execute(sql2+comment, job.values())
                        # update files
                        sqlF = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                        for file in job.Files:
                            self.cur.execute(sqlF+comment, file.values()+(file.rowID,))
                else:
                    # update job
                    sqlJ = ("UPDATE jobsDefined4 SET %s " % JobSpec.updateExpression()) + \
                           "WHERE PandaID=%s AND (jobStatus='assigned' OR jobStatus='defined')"
                    n = self.cur.execute(sqlJ+comment, job.values()+(job.PandaID,))
                    if n==0:
                        # already killed or activated
                        _logger.debug("activateJob : Not found %s" % job.PandaID)
                    else:
                        # update files
                        sqlF = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                        for file in job.Files:
                            self.cur.execute(sqlF+comment, file.values()+(file.rowID,))
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
        sql1 = "UPDATE jobsDefined4 SET jobStatus='waiting' "
        sql1+= "WHERE PandaID=%s AND (jobStatus='assigned' OR jobStatus='defined') AND commandToPilot<>'tobekilled'"
        sql2 = "INSERT INTO jobsWaiting4 (%s) " % JobSpec.columnNames()
        sql2+= JobSpec.valuesExpression()
        # time information
        job.modificationTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
        job.stateChangeTime  = job.modificationTime        
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.cur.execute("START TRANSACTION")
                # delete 
                n = self.cur.execute(sql1+comment, (job.PandaID,))
                if n==0:
                    # already killed
                    _logger.debug("keepJob : Not found %s" % job.PandaID)
                else:
                    # set status
                    job.jobStatus = 'waiting'
                    # insert
                    self.cur.execute(sql2+comment, job.values())
                    # update files
                    sqlF = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                    for file in job.Files:
                        self.cur.execute(sqlF+comment, file.values()+(file.rowID,))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # update job info in MonALISA - Job sent to waiting state
                #aThr = apmonInterface(job)
                #aThr.start()
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
            sql1 = "UPDATE jobsDefined4 SET jobStatus='failed' WHERE PandaID=%s AND (jobStatus='assigned' OR jobStatus='defined')"
        else:
            sql1 = "DELETE FROM jobsActive4 WHERE PandaID=%s"            
        sql2 = "INSERT INTO jobsArchived4 (%s) " % JobSpec.columnNames()
        sql2+= JobSpec.valuesExpression()
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.cur.execute("START TRANSACTION")
                # delete 
                n = self.cur.execute(sql1+comment, (job.PandaID,))
                if n==0:
                    # already killed
                    _logger.debug("archiveJob : Not found %s" % job.PandaID)
                else:
                    # insert
                    job.modificationTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                    job.stateChangeTime  = job.modificationTime                    
                    if job.endTime == 'NULL':
                        job.endTime = job.modificationTime
                    self.cur.execute(sql2+comment, job.values())
                    # update files
                    sqlF = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                    for file in job.Files:
                        self.cur.execute(sqlF+comment, file.values()+(file.rowID,))
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
                    sqlD   = "SELECT PandaID FROM filesTable4 WHERE type='input' AND lfn='%s' GROUP BY PandaID"
                    sqlDJS = "SELECT %s " % JobSpec.columnNames()
                    sqlDJS+= "FROM jobsDefined4 WHERE PandaID=%s"
                    sqlDJD = "UPDATE jobsDefined4 SET jobStatus='failed' WHERE PandaID=%s"
                    sqlDJI = "INSERT INTO jobsArchived4 (%s) " % JobSpec.columnNames()
                    sqlDJI+= JobSpec.valuesExpression()
                    for upFile in upOutputs:
                        _logger.debug("look for downstream jobs for %s" % upFile)
                        # select PandaID
                        self.cur.execute((sqlD+comment) % upFile)
                        res = self.cur.fetchall()
                        for downID in res:
                            _logger.debug("delete : %s" % downID)        
                            # select jobs
                            self.cur.execute((sqlDJS+comment) % downID)
                            resJob = self.cur.fetchall()
                            if len(resJob) == 0:
                                continue
                            # instantiate JobSpec
                            dJob = JobSpec()
                            dJob.pack(resJob[0])
                            # delete
                            retD = self.cur.execute((sqlDJD+comment) % downID)
                            if retD == 0:
                                continue
                            # error code
                            dJob.jobStatus = 'failed'
                            dJob.endTime   = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                            dJob.taskBufferErrorCode = ErrorCode.EC_Kill
                            dJob.taskBufferErrorDiag = 'killed by Panda server : upstream job failed'
                            dJob.modificationTime = dJob.endTime
                            dJob.stateChangeTime  = dJob.endTime
                            # insert
                            self.cur.execute(sqlDJI+comment, dJob.values())
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
                            self.cur.execute(("SELECT name FROM Datasets WHERE vuid='%s' AND type='dispatch'" % vuid)+comment)
                            res = self.cur.fetchall()
                            if len(res) != 0:
                                disName = res[0]
                                # get PandaIDs
                                self.cur.execute(("SELECT PandaID FROM jobsDefined4 WHERE dispatchDBlock='%s' AND jobStatus='assigned'" % disName)+comment)
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
                self._rollback()
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
        sql1 = "SELECT %s FROM jobsActive4 " % JobSpec.columnNames()
        sql1+= "WHERE PandaID=%s"
        sql2 = "DELETE FROM jobsActive4 WHERE PandaID=%s"
        sql3 = "INSERT INTO jobsArchived4 (%s) " % JobSpec.columnNames()
        sql3+= JobSpec.valuesExpression()
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.cur.execute("START TRANSACTION")
                # select
                self.cur.execute(sql1+comment, (pandaID,))
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
                job.modificationTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                job.endTime          = job.modificationTime
                job.stateChangeTime  = job.modificationTime
                # delete
                n = self.cur.execute(sql2+comment, (job.PandaID,))
                if n==0:
                    # already killed
                    _logger.debug("archiveJobLite : Not found %s" % pandaID)        
                else:        
                    # insert
                    self.cur.execute(sql3+comment, job.values())
                    # update files
                    sqlF = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                    for file in job.Files:
                        self.cur.execute(sqlF+comment, file.values()+(file.rowID,))
                # delete downstream jobs
                if job.prodSourceLabel == 'panda' and job.jobStatus == 'failed':
                    # file select
                    sqlFile = "SELECT %s FROM filesTable4 " % FileSpec.columnNames()
                    sqlFile+= "WHERE PandaID=%s"
                    self.cur.execute(sqlFile+comment, (job.PandaID,))
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
                    sqlD   = "SELECT PandaID FROM filesTable4 WHERE type='input' AND lfn='%s' GROUP BY PandaID"
                    sqlDJS = "SELECT %s " % JobSpec.columnNames()
                    sqlDJS+= "FROM jobsDefined4 WHERE PandaID=%s"
                    sqlDJD = "UPDATE jobsDefined4 SET jobStatus='failed' WHERE PandaID=%s"
                    sqlDJI = "INSERT INTO jobsArchived4 (%s) " % JobSpec.columnNames()
                    sqlDJI+= JobSpec.valuesExpression()
                    for upFile in upOutputs:
                        _logger.debug("look for downstream jobs for %s" % upFile)
                        # select PandaID
                        self.cur.execute((sqlD+comment) % upFile)
                        res = self.cur.fetchall()
                        for downID in res:
                            _logger.debug("delete : %s" % downID)        
                            # select jobs
                            self.cur.execute((sqlDJS+comment) % downID)
                            resJob = self.cur.fetchall()
                            if len(resJob) == 0:
                                continue
                            # instantiate JobSpec
                            dJob = JobSpec()
                            dJob.pack(resJob[0])
                            # delete
                            retD = self.cur.execute((sqlDJD+comment) % downID)
                            if retD == 0:
                                continue
                            # error code
                            dJob.jobStatus = 'failed'
                            dJob.endTime   = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                            dJob.taskBufferErrorCode = ErrorCode.EC_Kill
                            dJob.taskBufferErrorDiag = 'killed by Panda server : upstream job failed'
                            dJob.modificationTime = dJob.endTime
                            dJob.stateChangeTime  = dJob.endTime
                            # insert
                            self.cur.execute((sqlDJI+comment), dJob.values())
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
        sql1 = "UPDATE jobsActive4 SET jobStatus=%s,modificationTime=UTC_TIMESTAMP()"
        if jobStatus in ['starting']:
            sql1 += ",stateChangeTime=UTC_TIMESTAMP()"
        values = [jobStatus]
        for key in param.keys():
            if param[key] != None:
                sql1 = sql1 + (',%s=' % key) + '%s'
                values.append(param[key])
        sql1 += " WHERE PandaID=%s"
        values.append(pandaID)
        nTry=3
        for iTry in range(nTry):
            try:
                # begin transaction
                self.cur.execute("START TRANSACTION")
                # update
                self.cur.execute (sql1+comment,tuple(values))
                # get command
                self.cur.execute ('SELECT commandToPilot,endTime FROM jobsActive4 WHERE PandaID=%s'+comment,(pandaID,))
                res = self.cur.fetchone()
                if res != None:
                    ret     = res[0]
                    # update endTime
                    endTime = res[1]
                    if jobStatus == 'holding' and endTime==None:
                        self.cur.execute ("UPDATE jobsActive4 SET endTime=UTC_TIMESTAMP() WHERE PandaID=%s"+comment,(pandaID,))
                else:
                    # already deleted
                    ret = 'tobekilled'
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return ret
            except:
                # roll back
                self._rollback()
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
            sql1 = "UPDATE jobsDefined4 SET %s " % JobSpec.updateExpression()
        else:
            sql1 = "UPDATE jobsActive4 SET %s " % JobSpec.updateExpression()            
        sql1+= "WHERE PandaID=%s"
        if inJobsDefined:        
            sql1+= " AND (jobStatus='assigned' OR jobStatus='defined')"
        nTry=3
        for iTry in range(nTry):
            try:
                job.modificationTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
                # set stateChangeTime for defined->assigned
                if inJobsDefined:
                    job.stateChangeTime = job.modificationTime
                # begin transaction
                self.cur.execute("START TRANSACTION")
                # update
                n = self.cur.execute(sql1+comment, job.values()+(job.PandaID,))
                if n==0:
                    # already killed or activated
                    _logger.debug("updateJob : Not found %s" % job.PandaID)
                else:
                    sqlF = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                    for file in job.Files:
                        self.cur.execute(sqlF+comment, file.values()+(file.rowID,))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return True
            except:
                # roll back
                self._rollback()
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
        sql1 = "SELECT %s FROM jobsActive4 " % JobSpec.columnNames()
        sql1+= "WHERE PandaID=%s"
        sql2 = "UPDATE jobsActive4 SET %s " % JobSpec.updateExpression()            
        sql2+= "WHERE PandaID=%s"
        nTry=3
        for iTry in range(nTry):
            try:
                retValue = False
                # begin transaction
                self.cur.execute("START TRANSACTION")
                # select
                self.cur.execute(sql1+comment, (pandaID,))
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
                    # reset job
                    job.jobStatus = 'activated'
                    job.startTime = None
                    job.modificationTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
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
                    sqlFile = "SELECT %s FROM filesTable4 " % FileSpec.columnNames()
                    sqlFile+= "WHERE PandaID=%s AND (type='log' OR type='output')" 
                    self.cur.execute(sqlFile+comment, (job.PandaID,))
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
                        sqlFup = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                        self.cur.execute(sqlFup+comment, file.values()+(file.rowID,))
                    # update job
                    self.cur.execute(sql2+comment, job.values()+(job.PandaID,))
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
                atlasRelease,prodUserID):
        comment = ' /* DBProxy.getJobs */'
        dynamicBrokering = False
        sql1 = "WHERE jobStatus=%s AND computingSite=%s AND commandToPilot<>'tobekilled' "
        if not mem in [0,'0']:
            sql1+= "AND (minRamCount<=%s OR minRamCount=0) " % mem
        if not diskSpace in [0,'0']:
            sql1+= "AND (maxDiskCount<%s OR maxDiskCount=0) " % diskSpace
        if prodSourceLabel == 'user':
            sql1+= "AND (prodSourceLabel='user' OR prodSourceLabel='panda') "
        elif prodSourceLabel == 'ddm':
            dynamicBrokering = True
            sql1+= "AND prodSourceLabel='ddm' "
        elif prodSourceLabel in [None,'managed']:
            sql1+= "AND (prodSourceLabel='managed' OR prodSourceLabel='test') "
        elif prodSourceLabel == 'software':
            sql1+= "AND prodSourceLabel='software' "
        elif prodSourceLabel == 'test' and computingElement != None:
            dynamicBrokering = True
            sql1+= "AND (computingElement='%s' OR computingElement='to.be.set' OR processingType='prod_test' OR prodSourceLabel='test') " % computingElement
        else:
            sql1+= "AND prodSourceLabel='%s' " % prodSourceLabel
        # user ID
        if prodUserID != None:
            sql1+= "AND prodUserID='%s' " % prodUserID
        sql2 = "SELECT %s FROM jobsActive4 " % JobSpec.columnNames()
        sql2+= "WHERE PandaID=%s"
        retJobs = []
        nSent = 0        
        try:
            timeLimit = datetime.timedelta(seconds=timeout-10)
            timeStart = datetime.datetime.utcnow()
            strName   = datetime.datetime.isoformat(timeStart)
            attLimit  = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
            attSQL    = "AND ((creationTime<'%s' AND attemptNr>1) OR attemptNr<=1) " % attLimit.strftime('%Y-%m-%d %H:%M:%S')
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # get nJobs
            for iJob in range(nJobs):
                pandaID = 0
                # select channel for ddm jobs
                if prodSourceLabel == 'ddm':
                    sqlDDM = "SELECT count(*),jobStatus,sourceSite,destinationSite,transferType FROM jobsActive4 WHERE computingSite=%s AND prodSourceLabel='ddm' " + attSQL + "GROUP BY jobStatus,sourceSite,destinationSite,transferType"
                    _logger.debug((sqlDDM+comment) % siteName)
                    self.cur.execute(sqlDDM+comment,(siteName,))
                    resDDM = self.cur.fetchall()
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
                            sql1+= "AND sourceSite='%s' AND destinationSite='%s' AND transferType='%s' " \
                                   % channel
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
                            sqlMX = "SELECT MAX(currentPriority) FROM jobsActive4 "
                            sqlMX+= sql1
                            _logger.debug((sqlMX+comment) % ("activated",tmpSiteID))
                            self.cur.execute(sqlMX+comment, ("activated",tmpSiteID))
                            tmpPriority, = self.cur.fetchone()
                            # no jobs
                            if tmpPriority == None:
                                toGetPandaIDs = False
                            else:
                                # set priority
                                sql1 += "AND currentPriority=%s" % tmpPriority
                        if toGetPandaIDs:
                            # get PandaIDs
                            sqlP = "SELECT PandaID,currentPriority FROM jobsActive4 "
                            sqlP+= sql1
                            _logger.debug((sqlP+comment) % ("activated",tmpSiteID))
                            self.cur.execute(sqlP+comment, ("activated",tmpSiteID))
                            resIDs = self.cur.fetchall()
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
                            # get nSent for production jobs
                            if prodSourceLabel in [None,'managed']:
                                sentLimit = timeStart - datetime.timedelta(seconds=60)
                                sqlSent  = "SELECT count(*) FROM jobsActive4 WHERE jobStatus='sent' "
                                sqlSent += "AND prodSourceLabel IN ('managed','test') "
                                sqlSent += "AND computingSite='%s' " % tmpSiteID
                                sqlSent += "AND modificationTime>'%s' " % sentLimit.strftime('%Y-%m-%d %H:%M:%S')
                                self.cur.execute(sqlSent+comment)
                                resSent = self.cur.fetchone()
                                if resSent != None:
                                    nSent, = resSent
                            # update
                            for indexID,tmpPandaID in enumerate(pandaIDs):
                                # max attempts
                                if indexID > 10:
                                    break
                                # update
                                sqlJ = "UPDATE jobsActive4 "
                                sqlJ+= "SET jobStatus=%s,modificationTime=UTC_TIMESTAMP(),modificationHost=%s,startTime=UTC_TIMESTAMP()"
                                # set CE
                                if computingElement != None:
                                    sqlJ+= ",computingElement='%s'" % computingElement
                                sqlJ+= " WHERE PandaID=%s AND jobStatus=%s" 
                                _logger.debug((sqlJ+comment) % ("sent",node,tmpPandaID,"activated"))
                                retU = self.cur.execute(sqlJ+comment,("sent",node,tmpPandaID,"activated"))
                                # succeeded
                                if retU != 0:
                                    pandaID = tmpPandaID
                                    # increment nSent
                                    if prodSourceLabel in [None,'managed']:
                                        nSent += (indexID+1)
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
                # select
                self.cur.execute(sql2+comment, (pandaID,))
                res = self.cur.fetchone()
                if len(res) == 0:
                    break
                # instantiate Job
                job = JobSpec()
                job.pack(res)
                # Files
                sqlFile = "SELECT %s FROM filesTable4 " % FileSpec.columnNames()
                sqlFile+= "WHERE PandaID=%s"
                self.cur.execute(sqlFile+comment, (job.PandaID,))
                resFs = self.cur.fetchall()
                for resF in resFs:
                    file = FileSpec()
                    file.pack(resF)
                    job.addFile(file)
                # append
                retJobs.append(job)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
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
        table = 'jobsWaiting4'        
        if activeTable:
            table = 'jobsActive4'
        sql1 = "SELECT %s FROM %s " % (JobSpec.columnNames(),table)
        sql1+= "WHERE PandaID=%s"
        sql2 = "DELETE FROM %s " % table
        sql2+= "WHERE PandaID=%s AND (jobStatus='waiting' OR jobStatus='activated')"
        sql3 = "INSERT INTO jobsDefined4 (%s) " % JobSpec.columnNames()
        sql3+= JobSpec.valuesExpression()
        try:
            # transaction causes Request ndbd time-out in jobsActive4
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            self.cur.execute(sql1+comment,(pandaID,))
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
            retD = self.cur.execute(sql2+comment,(pandaID,))
            # delete failed
            _logger.debug("resetJobs : retD = %s" % retD)
            if retD != 1:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return None
            # delete from jobsDefined4 just in case
            sqlD = "DELETE FROM jobsDefined4 WHERE PandaID=%s"
            self.cur.execute(sqlD+comment,(pandaID,))
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
            job.modificationTime = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
            job.stateChangeTime  = job.modificationTime           
            # insert
            self.cur.execute(sql3+comment, job.values())
            # Files
            sqlFile = "SELECT %s FROM filesTable4 " % FileSpec.columnNames()
            sqlFile+= "WHERE PandaID=%s"
            self.cur.execute(sqlFile+comment, (job.PandaID,))
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
                sqlF = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                self.cur.execute(sqlF+comment, file.values()+(file.rowID,))
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
        sql1  = "UPDATE jobsDefined4 SET "
        sql1 += "jobStatus='defined',"
        sql1 += "modificationTime=UTC_TIMESTAMP(),"
        sql1 += "dispatchDBlock=NULL,"
        sql1 += "computingElement=NULL"         
        sql1 += " WHERE PandaID=%s AND (jobStatus='assigned' OR jobStatus='defined')"
        sql2 = "SELECT %s FROM jobsDefined4 " % JobSpec.columnNames()
        sql2+= "WHERE PandaID=%s"
        try:
            # begin transaction
            self.cur.execute("START TRANSACTION")
            # update
            retU = self.cur.execute(sql1+comment,(pandaID,))
            # not found
            job = None
            if retU == 0:
                _logger.debug("resetDefinedJob : Not found %s" % pandaID)
            else:
                # select
                self.cur.execute(sql2+comment,(pandaID,))
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
                # Files
                sqlFile = "SELECT %s FROM filesTable4 " % FileSpec.columnNames()
                sqlFile+= "WHERE PandaID=%s"
                self.cur.execute(sqlFile+comment, (job.PandaID,))
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
                    sqlF = ("UPDATE filesTable4 SET %s" % FileSpec.updateExpression()) + "WHERE rowID=%s"
                    self.cur.execute(sqlF+comment, file.values()+(file.rowID,))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return job
        except:
            # error report
            type, value, traceBack = sys.exc_info()
            _logger.error("resetDefinedJobs : %s %s" % (type,value))
            #_logger.error(traceback.format_exc())
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
        sql0 = "SELECT prodUserID FROM %s WHERE PandaID=%s"        
        sql1 = "UPDATE %s SET commandToPilot='tobekilled' WHERE PandaID=%s AND commandToPilot<>'tobekilled'"
        sql2 = "SELECT %s " % JobSpec.columnNames()
        sql2+= "FROM %s WHERE PandaID=%s AND jobStatus<>'running'"
        sql3 = "DELETE FROM %s WHERE PandaID=%s"
        sqlU = "UPDATE jobsDefined4 SET jobStatus='failed' WHERE PandaID=%s AND (jobStatus='assigned' OR jobStatus='defined')"
        sql4 = "INSERT INTO jobsArchived4 (%s) " % JobSpec.columnNames()
        sql4+= JobSpec.valuesExpression()
        try:
            flagCommand = False
            flagKilled  = False
            # begin transaction
            self.cur.execute("START TRANSACTION")
            for table in ('jobsDefined4','jobsActive4','jobsWaiting4'):
                # get DN if user is not production DN
                if (not prodManager) and (not user.startswith('/DC=org/DC=doegrids/OU=People/CN=Nurcan Ozturk')) \
                       and (not user.startswith('/DC=org/DC=doegrids/OU=People/CN=Torre Wenaus')): 
                    self.cur.execute((sql0+comment) % (table,pandaID))
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
                retU = self.cur.execute((sql1+comment) % (table,pandaID))
                if retU == 0:
                    continue
                # set flag
                flagCommand = True
                # select
                self.cur.execute((sql2+comment) % (table,pandaID))
                res = self.cur.fetchall()
                if len(res) == 0:
                    continue
                # instantiate JobSpec
                job = JobSpec()
                job.pack(res[0])
                # delete
                if table=='jobsDefined4':
                    retD = self.cur.execute((sqlU+comment) % (pandaID,))
                else:
                    retD = self.cur.execute((sql3+comment) % (table,pandaID))
                if retD == 0:
                    continue
                # error code
                job.jobStatus = 'failed'
                job.endTime   = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
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
                self.cur.execute(sql4+comment, job.values())
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
        sql1_1 = "WHERE PandaID=%s"
        try:
            tables=[]
            if fromActive:
                tables.append('jobsActive4')
            if fromArchived:
                tables.append('jobsArchived4')
            if fromWaiting:
                tables.append('jobsWaiting4')
            if fromDefined:
                # defined needs to be the last one due to InnoDB's auto_increment
                tables.append('jobsDefined4')
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
                    sqlFile = "SELECT %s FROM filesTable4 " % FileSpec.columnNames()
                    sqlFile+= "WHERE PandaID=%s"
                    self.cur.execute(sqlFile+comment, (job.PandaID,))
                    resFs = self.cur.fetchall()
                    # metadata
                    if table == 'jobsArchived4' and (not forAnal):
                        # read metadata only for finished/failed jobs
                        sqlMeta = "SELECT metaData FROM metaTable WHERE PandaID=%s"
                        self.cur.execute(sqlMeta+comment, (job.PandaID,))
                        resMeta = self.cur.fetchone()
                    else:
                        resMeta = None
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # set files
                    for resF in resFs:
                        file = FileSpec()
                        file.pack(resF)
                        job.addFile(file)
                    # set metadata
                    if resMeta != None:
                        job.metadata = resMeta[0]
                    return job
            _logger.debug("peekJob() : PandaID %s not found" % pandaID)
            return None
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("peekJob : %s %s" % (type,value))
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
            tables = ['jobsArchived4','jobsActive4','jobsWaiting4','jobsDefined4']
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
            return []


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,idStatus,nJobs):
        comment = ' /* DBProxy.getPandIDsWithJobID */'                        
        _logger.debug("getPandIDsWithJobID : %s %s" % (dn,jobID))
        try:
            tables = ['jobsArchived4','jobsActive4','jobsWaiting4','jobsDefined4']
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


    # query PandaID
    def queryPandaID(self,jobDefID):
        comment = ' /* DBProxy.queryPandaID */'                
        _logger.debug("queryPandaID : %s" % jobDefID)
        sql0 = "SELECT PandaID,attemptNr FROM %s WHERE attemptNr=("
        sql0+= "SELECT MAX(attemptNr) FROM %s"
        sql1= " WHERE prodSourceLabel=%s AND jobDefinitionID=%s) AND prodSourceLabel=%s AND jobDefinitionID=%s"
        try:
            ids = []
            # select
            for table in ['jobsDefined4','jobsActive4','jobsArchived4','jobsWaiting4']:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                sql = sql0 % (table,table) + sql1
                self.cur.execute(sql+comment, ('managed',jobDefID,'managed',jobDefID))
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
        sql0+= "WHERE cloud='%s' " % cloud
        if schedulerID != None:
            sql0+= "AND schedulerID='%s' " % schedulerID
        try:
            ids = []
            returnList = []
            # select
            for table in ['jobsActive4','jobsWaiting4','jobsDefined4']:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                sql = sql0 % table
                self.cur.execute(sql+comment)
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
                table = 'jobsDefined4'
            elif status in ['activated','running','holding','trasnferring']:
                table = 'jobsActive4'
            elif status in ['waiting']:
                table = 'jobsWaiting4'
            elif status in ['finished','failed']:
                table = 'jobsArchived4'
            else:
                _logger.error("unknown status:%s" % status)
                return ids
            # limit
            limit = int(limit)
            # SQL
            sql  = "SELECT PandaID FROM %s " % table
            sql += "WHERE computingSite=%s AND jobStatus=%s AND prodSourceLabel=%s "
            sql += "LIMIT %d" % limit
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            self.cur.execute(sql+comment, (site,status,'managed'))
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
        sql0+= "WHERE prodSourceLabel IN ('managed','rc_test') AND lockedby='%s' " % lockedby
        sql0+= "AND stateChangeTime>prodDBUpdateTime AND stateChangeTime<>'0000-00-00 00:00:00'"
        try:
            retMap   = {}
            totalIDs = 0
            # select
            for table in ['jobsArchived4','jobsActive4','jobsWaiting4','jobsDefined4']:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                sql = sql0 % table
                self.cur.execute(sql+comment)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                for PandaID,jobStatus,stateChangeTime,attemptNr,jobDefinitionID,jobExecutionID in res:
                    # ignore dummy jobs in jobsDefined4
                    if table == 'jobsDefined4' and (not jobStatus in ['defined','assigned']):
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
        sql0+= "SET prodDBUpdateTime='%s' " % param['stateChangeTime']
        sql0+= "WHERE PandaID=%s AND jobStatus='%s' AND stateChangeTime='%s'" % (param['PandaID'],
                                                                                 param['jobStatus'],
                                                                                 param['stateChangeTime'])
        try:
            if param['jobStatus'] in ['defined','assigned']:
                table = 'jobsDefined4'
            elif param['jobStatus'] in ['waiting']:
                table = 'jobsWaiting4'
            elif param['jobStatus'] in ['activated','sent','starting','running','holding','transferring']:
                table = 'jobsActive4'
            elif param['jobStatus'] in ['finished','failed']:
                table = 'jobsArchived4'
            else:
                _logger.error("invalid status %s" % param['jobStatus'])
                return False
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # update
            sql = sql0 % table
            _logger.debug(sql)
            retU = self.cur.execute(sql+comment)
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
        sql0 = "SELECT PandaID FROM metaTable WHERE PandaID=%s"        
        sql1 = "INSERT INTO metaTable (PandaID,metaData) VALUE (%s,%s)"
        nTry=3        
        for iTry in range(nTry):
            try:
                # autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                self.cur.execute(sql0+comment, (pandaID,))
                res = self.cur.fetchone()
                # already exist
                if res != None:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    return True
                # insert
                self.cur.execute(sql1+comment, (pandaID,metadata))
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
    def insertDataset(self,dataset,tablename="Datasets"):
        comment = ' /* DBProxy.insertDataset */'        
        _logger.debug("insertDataset(%s)" % dataset.name)
        sql1 = "INSERT INTO %s " % tablename
        sql1+= "(%s) " % DatasetSpec.columnNames()
        sql1+= DatasetSpec.valuesExpression()
        # time information
        dataset.creationdate = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())
        dataset.modificationdate = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())        
        try:
            # get file lock
            #fcntl.flock(_lockGetSN.fileno(), fcntl.LOCK_EX)
            # begin transaction
            self.cur.execute("START TRANSACTION")
            # avoid duplication
            self.cur.execute("SELECT vuid FROM "+tablename+" WHERE vuid=%s"+comment, (dataset.vuid,))
            res = self.cur.fetchall()
            if len(res) == 0:
                # insert
                self.cur.execute(sql1+comment, dataset.values())
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # release file lock
            #fcntl.flock(_lockGetSN.fileno(), fcntl.LOCK_UN)
            return True
        except:
            # roll back
            self._rollback()
            # release file lock
            #fcntl.flock(_lockGetSN.fileno(), fcntl.LOCK_UN)
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("insertDataset() : %s %s" % (type,value))
            return False


    # query dataset with map
    def queryDatasetWithMap(self,map):
        comment = ' /* DBProxy.queryDatasetWithMap */'               
        _logger.debug("queryDatasetWithMap(%s)" % map)
        sql1 = "SELECT %s FROM Datasets" % DatasetSpec.columnNames()
        valueL = []
        for key in map.keys():
            if len(valueL)==0:
                sql1+= " WHERE %s=" % key
            else:
                sql1+= " AND %s=" % key                
            sql1+= "%s"
            valueL.append(map[key])
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            nTry=5
            for iTry in range(nTry):
                retS = self.cur.execute(sql1+comment, tuple(valueL))
                res = self.cur.fetchall()
                if retS>=0 and res != None and retS==len(res):
                    break
                if iTry+1 < nTry:
                    _logger.debug("queryDatasetWithMap : retS %s retry : %s" % (retS,iTry))
                    time.sleep(random.randint(10,20))
            _logger.debug("queryDatasetWithMap(%s) : retS %s ret %s" % (str(map),retS,str(res)))
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
    def updateDataset(self,datasets,withLock,withCriteria):
        comment = ' /* DBProxy.updateDataset */'               
        _logger.debug("updateDataset()")        
        sql1 = "UPDATE Datasets SET %s " % DatasetSpec.updateExpression()
        sql1+= "WHERE vuid=%s"
        if withCriteria != "":
            sql1+= " AND %s" % withCriteria
        nTry=3
        for iTry in range(nTry):
            try:
                # get file lock
                if withLock:                
                    fcntl.flock(_lockSetDS.fileno(), fcntl.LOCK_EX)
                retList = []
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                for dataset in datasets:
                    _logger.debug("updateDataset(%s,%s)" % (dataset.name,dataset.status))
                    # time information
                    dataset.modificationdate = time.strftime('%Y-%m-%d %H:%M:%S',time.gmtime())                    
                    # update
                    retU = self.cur.execute(sql1+comment, dataset.values()+(dataset.vuid,))
                    if retU != 0 and retU != 1:
                        raise RuntimeError, 'Invalid retrun %s' % retU
                    retList.append(retU)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # release file lock
                if withLock:
                    fcntl.flock(_lockSetDS.fileno(), fcntl.LOCK_UN)
                _logger.debug("updateDataset() ret:%s" % retList)                    
                return retList
            except:
                # roll back
                self._rollback()
                # release file lock
                if withLock:
                    fcntl.flock(_lockSetDS.fileno(), fcntl.LOCK_UN)
                if iTry+1 < nTry:
                    _logger.debug("updateDataset : retry : %s" % iTry)
                    time.sleep(random.randint(10,20))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("updateDataset() : %s %s" % (type,value))
                return []

            
    # delete dataset
    def deleteDataset(self,name):
        comment = ' /* DBProxy.deleteDataset */'        
        sql1 = "DELETE FROM Datasets WHERE name=%s"
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # delete
            self.cur.execute(sql1+comment,(name,))
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
            # get file lock
            #fcntl.flock(_lockGetSN.fileno(), fcntl.LOCK_EX)
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql = "SELECT COUNT(*) FROM Datasets WHERE type='output' AND name='%s'" % datasetname
            nTry=3
            for iTry in range(nTry):
                retS = self.cur.execute(sql+comment)
                res = self.cur.fetchone()
                _logger.debug("getSerialNumber : retS %s, res %s" % (retS,res))
                if retS>=0 and res != None:
                    break
                if iTry+1 < nTry:
                    time.sleep(random.randint(10,20))
            # fresh dataset or not
            if res != None and len(res) != 0 and res[0] > 0:
                freshFlag = False
            else:
                freshFlag = True
            # get serial number
            sql = "INSERT INTO subCounter (subID) VALUES ('NULL')"
            self.cur.execute(sql+comment)
            sn = self.conn.insert_id()
            # delete. '<' is needed for auto_incr of InnoDB
            sql = "DELETE FROM subCounter where subID<%s" % sn
            self.cur.execute(sql+comment)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # release file lock
            #fcntl.flock(_lockGetSN.fileno(), fcntl.LOCK_UN)
            _logger.debug("getSerialNumber : %s %s" % (sn,freshFlag))
            return (sn,freshFlag)
        except:
            # roll back
            self._rollback()
            # release file lock
            #fcntl.flock(_lockGetSN.fileno(), fcntl.LOCK_UN)
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getSerialNumber() : %s %s" % (type,value))
            return (-1,False)


    # update transfer status for a dataset
    def updateTransferStatus(self,datasetname,bitMap):
        comment = ' /* DBProxy.updateTransferStatus */'        
        try:
            _logger.debug("updateTransferStatus(%s,%s)" % (datasetname,hex(bitMap)))
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            retTransSt = 0
            # update bitmap
            sqlU = "UPDATE Datasets SET transferStatus=transferStatus|%s WHERE name='%s'" % (bitMap,datasetname)
            retU = self.cur.execute(sqlU+comment)
            # get transferStatus
            sqlS = "SELECT transferStatus from Datasets WHERE name='%s'" % datasetname
            retS = self.cur.execute(sqlS+comment)
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
            # get file lock
            fcntl.flock(_lockGetCT.fileno(), fcntl.LOCK_EX)
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql  = "SELECT %s FROM cloudtasks " % CloudTaskSpec.columnNames()
            sql += "WHERE taskid=%s" % tid
            nTry=5
            for iTry in range(nTry):
                retS = self.cur.execute(sql+comment)
                res = self.cur.fetchall()
                _logger.debug("getCloudTask : retS %s" % retS)
                if retS>=0 and res != None and retS==len(res):
                    break
                if iTry+1 < nTry:
                    time.sleep(random.randint(10,20))
            # already exist
            if res != None and len(res) != 0:
                # instantiate CloudTask
                cloudTask = CloudTaskSpec()
                cloudTask.pack(res[0])
                # update tmod if status <> 'assigned'
                if cloudTask.status <> 'assigned':
                    sql = "UPDATE cloudtasks SET tmod=UTC_TIMESTAMP() WHERE taskid=%s" % cloudTask.taskid
                    self.cur.execute(sql+comment)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # release file lock
                fcntl.flock(_lockGetCT.fileno(), fcntl.LOCK_UN)
                _logger.debug("return existing CloudTask")                
                return cloudTask
            # insert new CloudTask
            _logger.debug("insert new CloudTask")
            cloudTask = CloudTaskSpec()
            cloudTask.taskid = tid
            cloudTask.status = 'defined' 
            sql = "INSERT INTO cloudtasks (taskid,status,tmod,tenter) VALUES(%s,%s,UTC_TIMESTAMP(),UTC_TIMESTAMP())"
            self.cur.execute(sql+comment,(cloudTask.taskid,cloudTask.status))
            # get id
            cloudTask.id = self.conn.insert_id()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # release file lock
            fcntl.flock(_lockGetCT.fileno(), fcntl.LOCK_UN)
            _logger.debug("return new CloudTask")                            
            return cloudTask
        except:
            # roll back
            self._rollback()
            # release file lock
            fcntl.flock(_lockGetCT.fileno(), fcntl.LOCK_UN)
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("getCloudTask() : %s %s" % (type,value))
            return None

        
    # set cloud to CloudTask
    def setCloudTask(self,cloudTask):
        comment = ' /* setCloudTask */'        
        try:
            _logger.debug("setCloudTask(id=%s,taskid=%s)" % (cloudTask.id,cloudTask.taskid))
            sql  = "UPDATE cloudtasks SET cloud=%s,status=%s,tmod=UTC_TIMESTAMP() WHERE id=%s AND status='defined'" 
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # update
            retU = self.cur.execute(sql+comment,(cloudTask.cloud,'assigned',cloudTask.id))
            # succeeded
            if retU != 0:
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return cloudTask
            # read if it is already set by another thread
            sql  = "SELECT %s FROM cloudtasks " % CloudTaskSpec.columnNames()
            sql += "WHERE id=%s" % cloudTask.id
            # select
            retS = self.cur.execute(sql+comment)
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
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql  = "SELECT cloud FROM cloudtasks WHERE taskid=%s" % tid
            nTry=5
            for iTry in range(nTry):
                retS = self.cur.execute(sql+comment)
                res = self.cur.fetchall()
                _logger.debug("seeCloudTask : retS %s" % retS)
                if retS>=0 and res != None and retS==len(res):
                    break
                if iTry+1 < nTry:
                    time.sleep(random.randint(10,20))
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
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            sql = "SELECT taskid FROM cloudtasks WHERE status<>'assigned' AND tmod>'%s'" % timeLimit.strftime('%Y-%m-%d %H:%M:%S')
            self.cur.execute(sql+comment)
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
        sql1 = "SELECT PandaID,%s FROM filesTable4" % FileSpec.columnNames()
        valueL = []
        for key in map.keys():
            if len(valueL)==0:
                sql1+= " WHERE %s=" % key
            else:
                sql1+= " AND %s=" % key                
            sql1+= "%s"
            valueL.append(map[key])
        nTry=3
        for iTry in range(nTry):
            try:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                self.cur.execute(sql1+comment, tuple(valueL))
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
        sql1 = "SELECT COUNT(*) FROM filesTable4"
        valueL = []
        for key in map.keys():
            if len(valueL)==0:
                sql1+= " WHERE %s=" % key
            else:
                sql1+= " AND %s=" % key                
            sql1+= "%s"
            valueL.append(map[key])
        nTry=3
        for iTry in range(nTry):
            try:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                _logger.debug("countFilesWithMap() : %s" % str(map))            
                retS = self.cur.execute(sql1+comment, tuple(valueL))
                res = self.cur.fetchone()
                _logger.debug("countFilesWithMap() : %s %s" % (retS,str(res)))
                # check return
                if retS != 1:
                    raise RuntimeError, 'Invalid return'                    
                nFiles=0
                if res != None:
                    nFiles=res[0]
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
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


    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self,dataset,status):
        comment = ' /* DBProxy.updateInFilesReturnPandaIDs */'                                
        _logger.debug("updateInFilesReturnPandaIDs(%s)" % dataset)
        sql0 = "SELECT rowID,PandaID FROM filesTable4 WHERE status<>%s AND dispatchDBlock=%s"        
        for iTry in range(self.nTry):
            try:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                retS = self.cur.execute(sql0+comment, (status,dataset))
                resS = self.cur.fetchall()
                _logger.debug("updateInFilesReturnPandaIDs : retS %s" % retS)                
                if retS<0 or resS==None or retS!=len(resS):
                    raise RuntimeError, 'SQL error'
                # avoid too long expression
                nDiv = 10
                nRow,tmpMod = divmod(len(resS),nDiv)
                if tmpMod != 0:
                    nRow += 1
                # update
                retList = []
                for iRow in range(nRow):
                    rows = []
                    pandaIDs = []
                    for tmpRowID,tmpPandaID in resS[iRow*nDiv:(iRow+1)*nDiv]:
                        rows.append(tmpRowID)
                        if not tmpPandaID in pandaIDs:
                            pandaIDs.append(tmpPandaID)
                    # make SQL query
                    sql1 = "UPDATE filesTable4 SET status=%s WHERE "
                    for row in rows:
                        if row != rows[0]:
                            sql1+= "OR "                
                        sql1+= "rowID=%s "
                    # update
                    retU = self.cur.execute(sql1+comment, tuple([status]+rows))
                    _logger.debug("updateInFilesReturnPandaIDs : retU %s" % retU)
                    # append
                    for tmpPandaID in pandaIDs:
                        if not tmpPandaID in retList:
                            retList.append(tmpPandaID)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
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
        sql0 = "SELECT rowID,PandaID FROM filesTable4 WHERE destinationDBlock=%s AND status='transferring'"
        for iTry in range(self.nTry):
            try:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                retS = self.cur.execute(sql0+comment, (dataset,))            
                resS = self.cur.fetchall()
                _logger.debug("updateOutFilesReturnPandaIDs : retS %s" % retS)
                if retS<0 or resS==None or retS!=len(resS):
                    raise RuntimeError, 'SQL error'
                # avoid too long expression
                nDiv = 10
                nRow,tmpMod = divmod(len(resS),nDiv)
                if tmpMod != 0:
                    nRow += 1
                # update
                retList = []
                for iRow in range(nRow):
                    rows = []
                    pandaIDs = []
                    for tmpRowID,tmpPandaID in resS[iRow*nDiv:(iRow+1)*nDiv]:
                        rows.append(tmpRowID)
                        if not tmpPandaID in pandaIDs:
                            pandaIDs.append(tmpPandaID)
                    # make SQL query
                    sql1 = "UPDATE filesTable4 SET status=%s WHERE "
                    for row in rows:
                        if row != rows[0]:
                            sql1+= "OR "                
                        sql1+= "rowID=%s "
                    # update
                    retU = self.cur.execute(sql1+comment, tuple(['ready']+rows))
                    _logger.debug("updateOutFilesReturnPandaIDs : retU %s" % retU)
                    # append
                    for tmpPandaID in pandaIDs:
                        if not tmpPandaID in retList:
                            retList.append(tmpPandaID)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
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
        sql0 = "UPDATE filesTable4 SET GUID=%s WHERE lfn=%s"
        for iTry in range(self.nTry):
            try:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # update
                for file in files:
                    retU = self.cur.execute(sql0+comment, (file['guid'],file['lfn']))
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
        sql1 = "SELECT PandaID FROM filesTable4 WHERE "
        for dataset in datasets:
            if dataset != datasets[0]:
                sql1+= "OR "                
            sql1+= "destinationDBlock='%s' " % dataset
        sql1+= "GROUP BY PandaID"
        # execute
        for iTry in range(self.nTry):
            try:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                self.cur.execute(sql1+comment)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                retList = []
                for r in res:
                    retList.append(r[0])
                # return
                _logger.debug("queryPandaIDwithDataset : %s" % str(retList))
                return retList
            except:
                # roll back
                self._rollback()
                # error report
                if iTry+1 < self.nTry:
                    _logger.debug("queryPandaIDwithDataset retry : %s" % iTry)                    
                    time.sleep(random.randint(10,20))
                    continue
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
        sql1 = "SELECT MAX(PandaID) FROM filesTable4 WHERE dataset=%s AND type='output'"
        sql2 = "SELECT lfn FROM filesTable4 WHERE PandaID=%s AND type='output'"
        # execute
        try:
            retMap = {}
            for dataset in datasets:
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select PandaID
                self.cur.execute(sql1+comment,(dataset,))
                res = self.cur.fetchone()
                # found
                retList = []
                if res != None:
                    pandaID = res[0]
                    # select LFNs
                    self.cur.execute(sql2+comment,(pandaID,))
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
        # avoid too long expression
        nDiv = 15
        nLFN,tmpMod = divmod(len(vlfns),nDiv)
        if tmpMod != 0:
            nLFN += 1
        # execute
        retList = []
        for iLFN in range(nLFN):
            lfns = vlfns[iLFN*nDiv:(iLFN+1)*nDiv]
            # make SQL query
            sql1 = "SELECT PandaID FROM filesTable4 WHERE "
            for lfn in lfns:
                if lfn != lfns[0]:
                    sql1+= "OR "                
                sql1+= "lfn=%s "
            sql1+= "GROUP BY PandaID"
            # get generic LFNs
            gLFNs = []
            for lfn in lfns:
                gLFNs.append(re.sub('\.\d+$','',lfn))
            # try
            for iTry in range(self.nTry):
                try:
                    # set autocommit on
                    self.cur.execute("SET AUTOCOMMIT=1")
                    # select
                    self.cur.execute(sql1+comment, tuple(gLFNs))
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # append IDs
                    for r in res:
                        if not r[0] in retList:
                            retList.append(r[0])
                    break
                except:
                    # roll back
                    self._rollback()
                    # error report
                    if iTry+1 < self.nTry:
                        _logger.debug("queryPandaIDwithLFN retry : %s" % iTry)                    
                        time.sleep(random.randint(10,20))
                        continue
                    type, value, traceBack = sys.exc_info()
                    _logger.error("queryPandaIDwithLFN : %s %s" % (type, value))
                    return []
        # return
        _logger.debug("queryPandaIDwithLFN : %s" % str(retList))
        return retList


    # get job statistics
    def getJobStatistics(self,archived=False,predefined=False):
        comment = ' /* DBProxy.getJobStatistics */'        
        _logger.debug("getJobStatistics()")
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        sql0 = "SELECT computingSite,jobStatus,COUNT(*) FROM %s WHERE prodSourceLabel in ('managed','rc_test','user','panda','ddm') "
        if predefined:
            sql0 += "AND relocationFlag=1 "
        sql0 += "GROUP BY computingSite,jobStatus"
        sqlA = "SELECT computingSite,jobStatus,COUNT(*) FROM jobsArchived4 WHERE modificationTime>'%s' AND prodSourceLabel in ('managed','rc_test','user','panda','ddm') " \
               % (timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
        if predefined:
            sqlA += "AND relocationFlag=1 "
        sqlA += "GROUP BY computingSite,jobStatus"
        tables = ['jobsActive4','jobsDefined4']
        if archived:
            tables.append('jobsArchived4')
        ret = {}
        nTry=3
        for iTry in range(nTry):
            try:
                for table in tables:
                    # set autocommit on
                    self.cur.execute("SET AUTOCOMMIT=1")
                    # select
                    if table != 'jobsArchived4':
                        self.cur.execute((sql0+comment) % table)
                    else:
                        self.cur.execute(sqlA+comment)
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
        sql0 = "SELECT computingSite,jobStatus,processingType,COUNT(*) FROM %s WHERE prodSourceLabel IN ('managed','rc_test','user','panda','ddm') "
        sql0 += "GROUP BY computingSite,jobStatus,processingType"
        tables = ['jobsActive4','jobsDefined4']
        ret = {}
        nTry=3
        for iTry in range(nTry):
            try:
                for table in tables:
                    # set autocommit on
                    self.cur.execute("SET AUTOCOMMIT=1")
                    # select
                    self.cur.execute((sql0+comment) % table)
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
        sql0 = "SELECT PandaID FROM filesTable4 WHERE destinationDBlock='%s' AND status='transferring' LIMIT 1" % dsname
        sql1 = "SELECT computingSite,destinationSE FROM jobsActive4 WHERE PandaID=%s"
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            self.cur.execute(sql0+comment)
            res = self.cur.fetchall()
            # get PandaID
            pandaID = None
            if len(res) != 0:
                pandaID = res[0][0]
            # get computingSite and destinationSE
            destSE = None,None
            if pandaID != None:
                self.cur.execute((sql1+comment) % pandaID)
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
        sql0 = "SELECT destinationDBlockToken FROM filesTable4 WHERE destinationDBlock='%s' LIMIT 1" % dsname
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            self.cur.execute(sql0+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # create map
            retToken = None
            if len(res) != 0:
                retToken = res[0][0]
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
    def getNumberJobsUser(self,dn):
        comment = ' /* DBProxy.getNumberJobsUser */'        
        _logger.debug("getNumberJobsUsers(%s)" % dn)
        sql0 = "SELECT COUNT(*) FROM %s WHERE prodUserID='%s' AND prodSourceLabel='user'"
        nTry = 1
        nJob = 0
        for iTry in range(nTry):
            try:
                for table in ('jobsActive4','jobsDefined4'):
                    # set autocommit on
                    self.cur.execute("SET AUTOCOMMIT=1")
                    # select
                    self.cur.execute((sql0+comment) % (table,dn))
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
            sql0 = "SELECT jobStatus,COUNT(*),cloud FROM %s WHERE prodSourceLabel in ('user','panda') GROUP BY jobStatus,cloud"
            sqlA = "SELECT jobStatus,COUNT(*),cloud FROM %s WHERE prodSourceLabel in ('user','panda') "
        else:
            sql0 = "SELECT jobStatus,COUNT(*),cloud FROM %s WHERE prodSourceLabel IN ('managed','rc_test') GROUP BY jobStatus,cloud"
            sqlA = "SELECT jobStatus,COUNT(*),cloud FROM %s WHERE prodSourceLabel IN ('managed','rc_test') "
        sqlA+= "AND modificationTime>'%s' GROUP BY jobStatus,cloud" % (timeLimit.strftime('%Y-%m-%d %H:%M:%S'))
        ret = {}
        try:
            for table in ('jobsActive4','jobsWaiting4','jobsArchived4','jobsDefined4'):
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                if table != 'jobsArchived4':
                    self.cur.execute((sql0+comment) % table)
                else:
                    self.cur.execute((sqlA+comment) % table)                    
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
        _logger.debug("getJobStatisticsPerProcessingType()")
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        sql0  = "SELECT jobStatus,COUNT(*),cloud,processingType FROM %s "
        sql0 += "WHERE prodSourceLabel IN ('managed','rc_test') "
        sqlT  = "AND modificationTime>'%s' " % timeLimit.strftime('%Y-%m-%d %H:%M:%S')
        sql1  = "GROUP BY jobStatus,cloud,processingType"
        sqlN  = sql0 + sql1
        sqlA  = sql0 + sqlT + sql1
        ret = {}
        try:
            for table in ('jobsActive4','jobsWaiting4','jobsArchived4','jobsDefined4'):
                # set autocommit on
                self.cur.execute("SET AUTOCOMMIT=1")
                # select
                if table == 'jobsArchived4':
                    self.cur.execute((sqlA+comment) % table)
                else:
                    self.cur.execute((sqlN+comment) % table)                    
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
        sql0 = "SELECT prodUserID FROM jobsActive4 WHERE jobStatus='activated' AND prodSourceLabel in ('user','panda') AND computingSite='%s' ORDER BY currentPriority DESC LIMIT %s" % (siteName,nJobs)
        ret = {}
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
            self.cur.execute(sql0+comment)
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
        sql0 = "SELECT computingSite,COUNT(*) FROM jobsActive4 WHERE jobStatus='activated' AND (prodSourceLabel='user' OR prodSourceLabel='panda') GROUP BY computingSite"
        ret = {}
        try:
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # select
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


    # count pilot requests
    def countPilotRequests(self,ids,prodSourceLabel='None'):
        comment = ' /* DBProxy.countPilotRequests */'                    
        # prodSourceLabel
        if prodSourceLabel=='user':
            criteria = " AND MESSAGE REGEXP 'user$'"
        else:
            criteria = " AND MESSAGE REGEXP 'None$'"            
        # time limit
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=3)
        ret = {}
        try:
            for siteID in ids:
                # begin transaction
                self.cur.execute("START TRANSACTION")
                # select
                sql0 = "SELECT COUNT(*) FROM PANDALOG WHERE Type='getJob' AND BINTIME>'%s'" % \
                       timeLimit.strftime('%Y-%m-%d %H:%M:%S')
                sql0+= " AND MESSAGE REGEXP '%s'" % siteID
                sql0+= criteria
                self.cur.execute(sql0+comment)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # create map
                ret[siteID] = res[0][0]
            # return
            _logger.debug("countPilotRequests() : %s" % str(ret))
            return ret
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("countPilotRequests : %s %s" % (type, value))
            # for zero
            for siteID in ids:
                if not ret.has_key(siteID):
                    ret[siteID]=0
            return ret


    # generate pilot token
    def genPilotToken(self,schedulerhost,scheduleruser,schedulerid):
        comment = ' /* DBProxy.genPilotToken */'                    
        try:
            _logger.debug("genPilotToken(%s,%s,%s)" % (schedulerhost,scheduleruser,schedulerid))
            token = commands.getoutput('uuidgen')
            timeNow = datetime.datetime.utcnow()
            timeExp = timeNow + datetime.timedelta(days=4)
            sql  = "INSERT INTO pilottoken (token,schedulerhost,scheduleruser,schedulerid,created,expires) "
            sql += "VALUES (%s,%s,%s,%s,%s,%s)"
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # execute
            self.cur.execute(sql+comment,(token,schedulerhost,scheduleruser,schedulerid,
                                          timeNow.strftime('%Y-%m-%d %H:%M:%S'),
                                          timeExp.strftime('%Y-%m-%d %H:%M:%S')))
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
            sql  = "SELECT token,scheduleruser FROM pilottoken WHERE expires>UTC_TIMESTAMP()"
            # set autocommit on
            self.cur.execute("SET AUTOCOMMIT=1")
            # execute
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
    def _rollback(self):
        try:
            self.conn.rollback()
            return True
        except:
            _logger.error("rollback error")
            return False
                
