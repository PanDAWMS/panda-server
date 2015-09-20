"""
proxy for database connection

"""
import re
import os
import sys
import json
import time
import copy
import glob
import fcntl
import types
import random
import urllib
import socket
import inspect
import datetime
import commands
import traceback
import warnings
import ErrorCode
import SiteSpec
import CloudSpec
import PrioUtil
import ProcessGroups
import EventServiceUtils
from JobSpec  import JobSpec
from FileSpec import FileSpec
from DatasetSpec import DatasetSpec
from CloudTaskSpec import CloudTaskSpec
from WrappedCursor import WrappedCursor
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper
from config import panda_config
from brokerage.PandaSiteIDs import PandaSiteIDs
from __builtin__ import True

if panda_config.backend == 'oracle':
    import cx_Oracle
    varNUMBER = cx_Oracle.NUMBER
else:
    import MySQLdb
    varNUMBER = long

warnings.filterwarnings('ignore')

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
        # hostname
        self.myHostName = socket.getfqdn()
        self.backend = panda_config.backend
        
        # logger
        global _logger
        _logger = PandaLogger().getLogger('DBProxy')

        
        
    # connect to DB
    def connect(self,dbhost=panda_config.dbhost,dbpasswd=panda_config.dbpasswd,
                dbuser=panda_config.dbuser,dbname=panda_config.dbname,
                dbtimeout=panda_config.dbtimeout,reconnect=False,dbport=panda_config.dbport):
        _logger.debug("connect : re=%s" % reconnect)
        # keep parameters for reconnect
        if not reconnect:
            self.dbhost    = dbhost
            self.dbpasswd  = dbpasswd
            self.dbuser    = dbuser
            self.dbname    = dbname
            self.dbtimeout = dbtimeout
            self.dbport    = dbport
        # close old connection
        if reconnect:
            _logger.debug("closing old connection")
            try:
                self.conn.close()
            except:
                _logger.debug("failed to close old connection")
        # connect    
        try:

            if self.backend == 'oracle':
                self.conn = cx_Oracle.connect(dsn=self.dbhost,user=self.dbuser,
                                              password=self.dbpasswd,threaded=True)
            else:
                self.conn = MySQLdb.connect(host=self.dbhost, db=self.dbname,
                                            port=self.dbport, connect_timeout=self.dbtimeout,
                                            user=self.dbuser, passwd=self.dbpasswd)
            self.cur = WrappedCursor(self.conn)
            try:
                # use SQL dumper
                if panda_config.dump_sql:
                    import SQLDumper
                    self.cur = SQLDumper.SQLDumper(self.cur)
            except:
                pass
            self.hostname = self.cur.initialize()
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("connect : %s %s" % (type,value))
            return False
    
    #Internal caching of a result. Use only for information 
    #with low update frequency and low memory footprint
    def memoize(f):
        memo = {}
        kwd_mark = object()
        def helper(self, *args, **kwargs):
            now = datetime.datetime.now()
            key = args + (kwd_mark,) + tuple(sorted(kwargs.items()))
            if key not in memo or memo[key]['timestamp'] < now - datetime.timedelta(hours=1):
                memo[key] = {}
                memo[key]['value'] = f(self, *args, **kwargs)
                memo[key]['timestamp'] = now
            return memo[key]['value']
        return helper

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
                    try:
                        itemRead = item.read()
                    except AttributeError:
                        itemRead = item
                    resItem.append(itemRead)
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
    def insertNewJob(self,job,user,serNum,weight=0.0,priorityOffset=0,userVO=None,groupJobSN=0,toPending=False,
                     origEsJob=False,eventServiceInfo=None,oldPandaIDs=None,relationType=None):
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
        job.prodDBUpdateTime = datetime.datetime(1970,1,1)
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
        if job.prodSourceLabel == 'install':
            job.currentPriority = 4100
        elif job.prodSourceLabel == 'user':
            if job.processingType in ['usermerge','pmerge'] and not job.currentPriority in ['NULL',None]:
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
            # get jobsetID for event service
            if origEsJob:
                if self.backend == 'oracle':
                    sqlESS = "SELECT ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ.nextval FROM dual ";
                    self.cur.arraysize = 10
                    self.cur.execute(sqlESS+comment, {})
                    job.jobsetID, = self.cur.fetchone()
                else:
                    #panda_config.backend == 'mysql':
                    ### fake sequence
                    sql = " INSERT INTO ATLAS_PANDA.JOBSDEFINED4_PANDAID_SEQ (col) VALUES (NULL) "
                    self.cur.arraysize = 10
                    self.cur.execute(sql + comment, {})
                    sql2 = """ SELECT LAST_INSERT_ID() """
                    self.cur.execute(sql2 + comment, {})
                    job.jobsetID, = self.cur.fetchone()
            # insert job
            varMap = job.valuesMap(useSeq=True)
            varMap[':newPandaID'] = self.cur.var(varNUMBER)
            retI = self.cur.execute(sql1+comment, varMap)
            # set PandaID
            job.PandaID = long(self.cur.getvalue(varMap[':newPandaID']))
            # get jobsetID
            if job.jobsetID in [None,'NULL',-1]:
                jobsetID = 0
            else:
                jobsetID = job.jobsetID
            jobsetID = '%06d' % jobsetID
            try:
                strJediTaskID = str(job.jediTaskID)
            except:
                strJediTaskID = ''
            # get originPandaID
            originPandaID = None
            if oldPandaIDs != None and len(oldPandaIDs) > 0:
                varMap = {}
                varMap[':jediTaskID'] = job.jediTaskID
                varMap[':pandaID'] = oldPandaIDs[0]
                sqlOrigin  = "SELECT originPandaID FROM {0}.JEDI_Job_Retry_History ".format(panda_config.schemaJEDI)
                sqlOrigin += "WHERE jediTaskID=:jediTaskID AND newPandaID=:pandaID "
                self.cur.execute(sqlOrigin+comment,varMap)
                resOrigin = self.cur.fetchone() 
                if resOrigin != None:
                    originPandaID, = resOrigin
                else:
                    originPandaID = oldPandaIDs[0]
            if originPandaID == None:
                originPandaID = job.PandaID
            newJobName = re.sub('\$ORIGINPANDAID',str(originPandaID),job.jobName)
            # update jobName
            if newJobName != job.jobName:
                job.jobName = newJobName
                if not toPending:
                    sqlJobName  = "UPDATE ATLAS_PANDA.jobsDefined4 "
                else:
                    sqlJobName  = "UPDATE ATLAS_PANDA.jobsWaiting4 "
                sqlJobName += "SET jobName=:jobName WHERE PandaID=:pandaID "
                varMap ={}
                varMap[':jobName'] = job.jobName
                varMap[':pandaID'] = job.PandaID
                self.cur.execute(sqlJobName+comment,varMap)
            # reset changed attribute list
            job.resetChangedList()
            # insert files
            _logger.debug("insertNewJob : inserted %s label:%s prio:%s jediTaskID:%s" % (job.PandaID,job.prodSourceLabel,
                                                                                         job.currentPriority,
                                                                                         job.jediTaskID))
            sqlFile = "INSERT INTO ATLAS_PANDA.filesTable4 (%s) " % FileSpec.columnNames()
            sqlFile+= FileSpec.bindValuesExpression(useSeq=True)
            sqlFile+= " RETURNING row_ID INTO :newRowID"
            useJEDI = False
            if hasattr(panda_config,'useJEDI') and panda_config.useJEDI == True and \
                    job.lockedby == 'jedi' and self.checkTaskStatusJEDI(job.jediTaskID,self.cur):
                useJEDI = True
            for file in job.Files:
                file.row_ID = None
                if not file.status in ['ready','cached']:
                    file.status='unknown'
                # replace $PANDAID with real PandaID
                file.lfn = re.sub('\$PANDAID', '%05d' % job.PandaID, file.lfn)
                # replace $JOBSETID with real jobsetID
                if not job.prodSourceLabel in ['managed']:
                    file.lfn = re.sub('\$JOBSETID', jobsetID, file.lfn)
                    file.lfn = re.sub('\$GROUPJOBSN', groupJobSN, file.lfn)
                    try:
                        file.lfn = re.sub('\$JEDITASKID', strJediTaskID, file.lfn)
                    except:
                        pass
                # set scope
                if file.type in ['output','log'] and job.VO in ['atlas']:
                    file.scope = self.extractScope(file.dataset)
                # insert
                varMap = file.valuesMap(useSeq=True)
                varMap[':newRowID'] = self.cur.var(varNUMBER)
                self.cur.execute(sqlFile+comment, varMap)
                # get rowID
                file.row_ID = long(self.cur.getvalue(varMap[':newRowID']))
                # reset changed attribute list
                file.resetChangedList()
                # update JEDI table
                if useJEDI:
                    # skip if no JEDI
                    if file.fileID == 'NULL':
                        continue
                    # update Dataset_Contents table
                    varMap = {}
                    varMap[':fileID'] = file.fileID
                    varMap[':status'] = 'running'
                    varMap[':oldStatusI'] = 'picked'
                    varMap[':oldStatusO'] = 'defined'
                    varMap[':attemptNr']  = file.attemptNr
                    varMap[':datasetID']  = file.datasetID
                    varMap[':keepTrack']  = 1
                    varMap[':jediTaskID'] = file.jediTaskID
                    varMap[':PandaID'] = file.PandaID
                    sqlJediFile  = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET status=:status,PandaID=:PandaID"
                    if file.type in ['output','log']:
                        sqlJediFile += ",outPandaID=:PandaID"
                    sqlJediFile += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    sqlJediFile += "AND attemptNr=:attemptNr AND status IN (:oldStatusI,:oldStatusO) AND keepTrack=:keepTrack "
                    self.cur.execute(sqlJediFile+comment, varMap)
                    # insert event tables
                    if origEsJob and eventServiceInfo != None and file.lfn in eventServiceInfo:
                        # discard old successful event ranges
                        sqlJediOdEvt  = "UPDATE {0}.JEDI_Events ".format(panda_config.schemaJEDI)
                        sqlJediOdEvt += "SET status=:esDiscarded "
                        sqlJediOdEvt += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                        sqlJediOdEvt += "AND status=:esDone "
                        varMap = {}
                        varMap[':jediTaskID']  = file.jediTaskID
                        varMap[':datasetID']   = file.datasetID
                        varMap[':fileID']      = file.fileID
                        varMap[':esDone']      = EventServiceUtils.ST_done
                        varMap[':esDiscarded'] = EventServiceUtils.ST_discarded
                        self.cur.execute(sqlJediOdEvt+comment, varMap)
                        # insert new ranges
                        sqlJediEvent  = "INSERT INTO {0}.JEDI_Events ".format(panda_config.schemaJEDI)
                        sqlJediEvent += "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
                        sqlJediEvent += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID) "
                        sqlJediEvent += "VALUES(:jediTaskID,:datasetID,:pandaID,:fileID,:attemptNr,:eventStatus,"
                        sqlJediEvent += ":startEvent,:startEvent,:lastEvent,:processedEvent) "
                        iEvent = 1
                        varMaps = []
                        while iEvent <= eventServiceInfo[file.lfn]['nEvents']:
                            varMap = {}
                            varMap[':jediTaskID'] = file.jediTaskID
                            varMap[':datasetID'] = file.datasetID
                            varMap[':pandaID'] = job.jobsetID
                            varMap[':fileID'] = file.fileID
                            varMap[':attemptNr'] = eventServiceInfo[file.lfn]['maxAttempt']
                            varMap[':eventStatus'] = EventServiceUtils.ST_ready
                            varMap[':processedEvent'] = 0
                            varMap[':startEvent'] = eventServiceInfo[file.lfn]['startEvent'] + iEvent
                            iEvent += eventServiceInfo[file.lfn]['nEventsPerRange']
                            if iEvent > eventServiceInfo[file.lfn]['nEvents']:
                                iEvent = eventServiceInfo[file.lfn]['nEvents'] + 1
                            lastEvent = eventServiceInfo[file.lfn]['startEvent'] + iEvent -1
                            varMap[':lastEvent'] = lastEvent
                            varMaps.append(varMap)
                        self.cur.executemany(sqlJediEvent+comment, varMaps)
                        _logger.debug("insertNewJob : %s inserted %s event ranges jediTaskID:%s" % (job.PandaID,len(varMaps),
                                                                                                        job.jediTaskID))
            # update t_task
            if useJEDI and not job.prodSourceLabel in ['panda'] and job.processingType != 'pmerge':
                varMap = {}
                varMap[':jediTaskID'] = job.jediTaskID
                schemaDEFT = self.getSchemaDEFT()
                sqlTtask  = "UPDATE {0}.T_TASK ".format(schemaDEFT)
                sqlTtask += "SET total_req_jobs=total_req_jobs+1,timestamp=CURRENT_DATE "
                sqlTtask += "WHERE taskid=:jediTaskID "
                _logger.debug(sqlTtask+comment+str(varMap))
                self.cur.execute(sqlTtask+comment,varMap)
                _logger.debug("insertNewJob : %s updated T_TASK jediTaskID:%s" % (job.PandaID,job.jediTaskID))
            # metadata
            if job.prodSourceLabel in ['user','panda'] and job.metadata != '':
                sqlMeta = "INSERT INTO ATLAS_PANDA.metaTable (PandaID,metaData) VALUES (:PandaID,:metaData)"
                varMap = {}
                varMap[':PandaID']  = job.PandaID
                varMap[':metaData'] = job.metadata
                _logger.debug("insertNewJob : %s inserting meta jediTaskID:%s" % (job.PandaID,job.jediTaskID))
                self.cur.execute(sqlMeta+comment, varMap)
                _logger.debug("insertNewJob : %s inserted meta jediTaskID:%s" % (job.PandaID,job.jediTaskID))
            # job parameters
            if not job.prodSourceLabel in ['managed']:
                job.jobParameters = re.sub('\$JOBSETID', jobsetID, job.jobParameters)
                job.jobParameters = re.sub('\$GROUPJOBSN', groupJobSN, job.jobParameters)
                try:
                    job.jobParameters = re.sub('\$JEDITASKID', strJediTaskID, job.jobParameters)
                except:
                    pass
            sqlJob = "INSERT INTO ATLAS_PANDA.jobParamsTable (PandaID,jobParameters) VALUES (:PandaID,:param)"
            varMap = {}
            varMap[':PandaID'] = job.PandaID
            varMap[':param']   = job.jobParameters
            _logger.debug("insertNewJob : %s inserting jobParam jediTaskID:%s" % (job.PandaID,job.jediTaskID))
            self.cur.execute(sqlJob+comment, varMap)
            _logger.debug("insertNewJob : %s inserted jobParam jediTaskID:%s" % (job.PandaID,job.jediTaskID))
            # record retry history
            if oldPandaIDs != None and len(oldPandaIDs) > 0:
                _logger.debug("insertNewJob : %s recording history nOld=%s jediTaskID:%s" % (job.PandaID,len(oldPandaIDs),job.jediTaskID))
                self.recordRetryHistoryJEDI(job.jediTaskID,job.PandaID,oldPandaIDs,relationType)
                _logger.debug("insertNewJob : %s recorded history jediTaskID:%s" % (job.PandaID,job.jediTaskID))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("insertNewJob : %s all OK jediTaskID:%s" % (job.PandaID,job.jediTaskID))
            # record status change
            try:
                self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
            except:
                _logger.error('recordStatusChange in insertNewJob')
            return True
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("insertNewJob : jediTaskID:%s %s %s" % (job.jediTaskID,type,value))
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
        updatedFlag = False
        if job==None:
            _logger.debug("activateJob : None")
            return True
        _logger.debug("activateJob : %s" % job.PandaID)                        
        sql0 = "SELECT row_ID FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:PandaID AND type=:type AND NOT status IN (:status1,:status2) "
        sql1 = "DELETE FROM ATLAS_PANDA.jobsDefined4 "
        sql1+= "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) AND commandToPilot IS NULL"
        sqlS = "SELECT sitershare,cloudrshare FROM ATLAS_PANDAMETA.schedconfig WHERE siteID=:siteID "
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
                    if file.type == 'input' and not file.status in ['ready','cached']:
                        allOK = False
                        break
                # begin transaction
                self.conn.begin()
                # check all inputs are ready
                varMap = {}
                varMap[':type']    = 'input'
                varMap[':status1'] = 'ready'
                varMap[':status2'] = 'cached'                
                varMap[':PandaID'] = job.PandaID
                self.cur.arraysize = 100
                self.cur.execute(sql0+comment, varMap)
                res = self.cur.fetchall()
                if len(res) == 0 or allOK:
                    # check resource share
                    job.jobStatus = "activated"
                    if job.lockedby == 'jedi':
                        varMap = {}
                        varMap[':siteID'] = job.computingSite
                        self.cur.execute(sqlS+comment, varMap)
                        resSite = self.cur.fetchone()
                        # change status
                        """
                        if resSite != None and (not resSite[0] in [None,''] or not resSite[1] in [None,'']):
                            job.jobStatus = "throttled"
                            _logger.debug("activateJob : {0} to {1}".format(job.PandaID,job.jobStatus))
                        """    
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
                        datasetContentsStat = {}
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
                        updatedFlag = True                        
                else:
                    # update job
                    sqlJ = ("UPDATE ATLAS_PANDA.jobsDefined4 SET %s " % job.bindUpdateChangesExpression()) + \
                           "WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
                    varMap = job.valuesMap(onlyChanged=True)
                    varMap[':PandaID']       = job.PandaID
                    varMap[':oldJobStatus1'] = 'assigned'
                    varMap[':oldJobStatus2'] = 'defined'
                    _logger.debug(sqlJ+comment+str(varMap))
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
                        updatedFlag = True
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # record status change
                try:
                    if updatedFlag:
                        self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
                except:
                    _logger.error('recordStatusChange in activateJob')
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
        updatedFlag = False
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
                    # update parameters
                    sqlJob = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[':PandaID'] = job.PandaID
                    varMap[':param']   = job.jobParameters
                    self.cur.execute(sqlJob+comment, varMap)
                    updatedFlag = True        
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # record status change
                try:
                    if updatedFlag:
                        self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
                except:
                    _logger.error('recordStatusChange in keepJob')
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
    def archiveJob(self,job,fromJobsDefined,useCommit=True,extraInfo=None):
        comment = ' /* DBProxy.archiveJob */'                
        _logger.debug("archiveJob : %s" % job.PandaID)                
        if fromJobsDefined:
            sql1 = "DELETE FROM ATLAS_PANDA.jobsDefined4 WHERE PandaID=:PandaID AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2)"
        else:
            sql1 = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"            
        sql2 = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
        sql2+= JobSpec.bindValuesExpression()
        updatedJobList = []
        nTry=1
        for iTry in range(nTry):
            try:
                # begin transaction
                if useCommit:
                    self.conn.begin()
                # check if JEDI is used
                useJEDI = False
                if hasattr(panda_config,'useJEDI') and panda_config.useJEDI == True and \
                        job.lockedby == 'jedi' and self.checkTaskStatusJEDI(job.jediTaskID,self.cur):
                    useJEDI = True
                if useCommit:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                # delete downstream jobs first
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
                    sqlDFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                    sqlDFile+= "WHERE PandaID=:PandaID"
                    for upFile in upOutputs:
                        _logger.debug("look for downstream jobs for %s" % upFile)
                        if useCommit:
                            self.conn.begin()
                        # select PandaID
                        varMap = {}
                        varMap[':lfn']  = upFile
                        varMap[':type'] = 'input'
                        self.cur.arraysize = 100000
                        self.cur.execute(sqlD+comment, varMap)
                        res = self.cur.fetchall()
                        if useCommit:
                            if not self._commit():
                                raise RuntimeError, 'Commit error'
                        iDownJobs = 0
                        nDownJobs = len(res)
                        nDownChunk = 20
                        inTransaction = False 
                        _logger.debug("archiveJob : {0} found {1} downstream jobs for {2}".format(job.PandaID,nDownJobs,upFile))
                        # loop over all downstream IDs
                        for downID, in res:
                            if useCommit:
                                if not inTransaction:
                                    self.conn.begin()
                                    inTransaction = True
                            _logger.debug("archiveJob : {0} delete : {1} ({2}/{3})".format(job.PandaID,downID,iDownJobs,nDownJobs))
                            iDownJobs += 1
                            # select jobs
                            varMap = {}
                            varMap[':PandaID'] = downID
                            self.cur.arraysize = 10
                            self.cur.execute(sqlDJS+comment, varMap)
                            resJob = self.cur.fetchall()
                            if len(resJob) == 0:
                                if useCommit and (iDownJobs % nDownChunk) == 0:
                                    if not self._commit():
                                        raise RuntimeError, 'Commit error'
                                    inTransaction = False
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
                                if useCommit and (iDownJobs % nDownChunk) == 0:
                                    if not self._commit():
                                        raise RuntimeError, 'Commit error'
                                    inTransaction = False
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
                            # collect to record state change
                            updatedJobList.append(dJob)
                            # update JEDI tables
                            if useJEDI:
                                # read files
                                varMap = {}
                                varMap[':PandaID'] = downID
                                self.cur.arraysize = 100000
                                self.cur.execute(sqlDFile+comment,varMap)
                                resDFiles = self.cur.fetchall()
                                for resDFile in resDFiles:
                                    tmpDFile = FileSpec()
                                    tmpDFile.pack(resDFile)
                                    dJob.addFile(tmpDFile)
                                self.propagateResultToJEDI(dJob,self.cur)
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
                                    if useCommit and (iDownJobs % nDownChunk) == 0:
                                        if not self._commit():
                                            raise RuntimeError, 'Commit error'
                                        inTransaction = False
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
                                        if not useJEDI and topUserDsName != tmpDestinationDBlock and not topUserDsName in topUserDsList:
                                            # set tobeclosed
                                            varMap = {}
                                            if dJob.processingType.startswith('gangarobot') or \
                                                   dJob.processingType.startswith('hammercloud'):
                                                varMap[':status'] = 'completed'
                                            else:
                                                varMap[':status'] = 'tobeclosed'
                                            varMap[':name'] = topUserDsName
                                            self.cur.execute(sqlCloseSub+comment, varMap)
                                            _logger.debug("set %s for %s" % (varMap[':status'],topUserDsName))
                                            # append
                                            topUserDsList.append(topUserDsName)
                            if useCommit and (iDownJobs % nDownChunk) == 0:
                                if not self._commit():
                                    raise RuntimeError, 'Commit error'
                                inTransaction = False
                        if useCommit and inTransaction:
                            if not self._commit():
                                raise RuntimeError, 'Commit error'
                elif job.prodSourceLabel == 'ddm' and job.jobStatus == 'failed' and job.transferType=='dis':
                    if useCommit:
                        self.conn.begin()
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
                                    # collect to record state change
                                    updatedJobList.append(dJob)
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
                    if useCommit:
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
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
                # main job
                if useCommit:
                    self.conn.begin()
                # actions for successful normal ES jobs
                if useJEDI and EventServiceUtils.isEventServiceJob(job) \
                        and not EventServiceUtils.isJobCloningJob(job):
                    retEvS,retNewPandaID = self.ppEventServiceJob(job,False)
                    # DB error
                    if retEvS == None:
                        raise RuntimeError, 'Faied to retry for Event Service'
                    elif retEvS == 0:
                        # retry event ranges
                        job.jobStatus = 'cancelled'
                        job.jobSubStatus = 'finished'
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceRetried
                        job.taskBufferErrorDiag = 'cancelled to retry unprocessed even ranges in PandaID={0}'.format(retNewPandaID)
                    elif retEvS == 2:
                        # goes to merging
                        job.jobStatus = 'cancelled'
                        job.jobSubStatus = 'finished'
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceMerge
                        job.taskBufferErrorDiag = 'cancelled to merge pre-merged files in PandaID={0}'.format(retNewPandaID)
                        # kill unused event service consumers
                        self.killUnusedEventServiceConsumers(job,False,killAll=True)
                    elif retEvS == 3:
                        # maximum attempts reached
                        job.jobStatus = 'failed'
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceMaxAttempt
                        job.taskBufferErrorDiag = 'maximum attempts reached for Event Service'
                        # kill other consumers
                        self.killEventServiceConsumers(job,False,False)
                        self.killUnusedEventServiceConsumers(job,False)
                    elif retEvS == 4:
                        # other consumers are running
                        job.jobStatus = 'cancelled'
                        job.jobSubStatus = 'finished'
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceWaitOthers
                        job.taskBufferErrorDiag = 'no further action since other Event ServiceEvent Service consumers were still running'
                        # kill unused
                        self.killUnusedEventServiceConsumers(job,False)
                    elif retEvS == 5:
                        # didn't process any event ranges
                        job.jobStatus = 'cancelled'
                        job.jobSubStatus = 'finished'
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceUnprocessed
                        job.taskBufferErrorDiag = "didn't process any events on WN for Event Service"
                    elif retEvS == 6:
                        # didn't process any event ranges and last consumer
                        job.jobStatus = 'failed'
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceLastUnprocessed
                        job.taskBufferErrorDiag = "didn't process any events on WN for Event Service"
                    elif retEvS == 7:
                        # all event ranges failed
                        job.jobStatus = 'failed'
                        job.taskBufferErrorCode = ErrorCode.EC_EventServiceAllFailed
                        job.taskBufferErrorDiag = "all event ranges failed"
                    # kill unused event ranges
                    if job.jobStatus == 'failed':
                        self.killUnusedEventRanges(job.jediTaskID,job.jobsetID)
                # delete from jobsDefined/Active
                varMap = {}
                varMap[':PandaID'] = job.PandaID
                if fromJobsDefined:
                    varMap[':oldJobStatus1'] = 'assigned'
                    varMap[':oldJobStatus2'] = 'defined'
                self.cur.execute(sql1+comment, varMap)
                n = self.cur.rowcount                
                if n==0:
                    # already deleted
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
                    # collect to record state change
                    updatedJobList.append(job)
                    # update JEDI tables unless it is an ES consumer job which was successful but waits for merging or other running consumers
                    if useJEDI and not (EventServiceUtils.isEventServiceJob(job) and job.jobStatus == 'cancelled'):
                        self.propagateResultToJEDI(job,self.cur,extraInfo=extraInfo)
                # propagate successful result to unmerge job
                if useJEDI and job.processingType == 'pmerge' and job.jobStatus == 'finished':
                    self.updateUnmergedJobs(job)
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                # record status change
                try:
                    for tmpJob in updatedJobList:
                        self.recordStatusChange(tmpJob.PandaID,tmpJob.jobStatus,jobInfo=tmpJob,useCommit=useCommit)
                except:
                    _logger.error('recordStatusChange in archiveJob')
                _logger.debug("archiveJob : %s done" % job.PandaID)                
                return True,ddmIDs,ddmAttempt,newJob
            except:
                # roll back
                if useCommit:
                    self._rollback(True)
                    if iTry+1 < nTry:
                        _logger.debug("archiveJob : %s retry : %s" % (job.PandaID,iTry))                
                        time.sleep(random.randint(10,20))
                        continue
                errtype,errvalue = sys.exc_info()[:2]
                errStr = "archiveJob %s : %s %s" % (job.PandaID,errtype,errvalue) 
                errStr.strip()
                errStr += traceback.format_exc()
                _logger.error(errStr)
                if not useCommit:
                    raise RuntimeError, 'archiveJob failed'
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
    def finalizePendingJobs(self,prodUserName,jobDefinitionID,waitLock=False):
        comment = ' /* DBProxy.finalizePendingJobs */'                        
        _logger.debug("finalizePendingJobs : %s %s" % (prodUserName,jobDefinitionID))
        sql0 = "SELECT PandaID,lockedBy,jediTaskID FROM ATLAS_PANDA.jobsActive4 "
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
            lockedBy = None
            jediTaskID = None
            for pandaID,tmpLockedBy,tmpJediTaskID in resPending:
                if lockedBy == None:
                    lockedBy = tmpLockedBy
                if jediTaskID == None:
                    jediTaskID = tmpJediTaskID
                pPandaIDs.append(pandaID)
            # check if JEDI is used
            useJEDI = False
            if hasattr(panda_config,'useJEDI') and panda_config.useJEDI == True and \
                    lockedBy == 'jedi' and self.checkTaskStatusJEDI(jediTaskID,self.cur):
                useJEDI = True
            # loop over all PandaIDs
            for pandaID in pPandaIDs:
                # begin transaction
                self.conn.begin()
                # lock
                varMap = {}
                varMap[':jobStatus']    = 'failed'
                varMap[':newJobStatus'] = 'holding'
                varMap[':PandaID']      = pandaID
                self.cur.execute(sqlU+comment,varMap)
                retU = self.cur.rowcount
                if retU == 0:
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
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
                    _logger.debug("finalizePendingJobs : finalizing %s" % pandaID)        
                    # insert
                    self.cur.execute(sql3+comment,job.valuesMap())
                    # update files,metadata,parametes
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    varMap[':modificationTime'] = job.modificationTime
                    self.cur.execute(sqlFMod+comment,varMap)
                    self.cur.execute(sqlMMod+comment,varMap)
                    self.cur.execute(sqlPMod+comment,varMap)
                    # update JEDI tables
                    if useJEDI:
                        # read files
                        sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                        sqlFile+= "WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[':PandaID'] = pandaID
                        self.cur.arraysize = 100000
                        self.cur.execute(sqlFile+comment, varMap)
                        resFs = self.cur.fetchall()
                        for resF in resFs:
                            tmpFile = FileSpec()
                            tmpFile.pack(resF)
                            job.addFile(tmpFile)
                        self.propagateResultToJEDI(job,self.cur,finishPending=True,waitLock=waitLock)
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
        _logger.debug("updateJobStatus : PandaID=%s attemptNr=%s status=%s" % (pandaID,attemptNr,jobStatus))
        sql0  = "SELECT commandToPilot,endTime,specialHandling,jobStatus,computingSite,cloud,prodSourceLabel,lockedby,jediTaskID,jobsetID "
        sql0 += "FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
        varMap0 = {}
        varMap0[':PandaID'] = pandaID
        sql1 = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:jobStatus,modificationTime=CURRENT_DATE"
        varMap = {}
        varMap[':jobStatus'] = jobStatus
        presetEndTime = False
        for key in param.keys():
            if param[key] != None:
                param[key] = JobSpec.truncateStringAttr(key,param[key])
                sql1 += ',%s=:%s' % (key,key)
                varMap[':%s' % key] = param[key]
                if key == 'endTime':
                    presetEndTime = True
                try:
                    # store positive error code even for pilot retry
                    if key == 'pilotErrorCode' and param[key].startswith('-'):
                        varMap[':%s' % key] = param[key][1:]
                except:
                    pass
        sql1W = " WHERE PandaID=:PandaID "
        varMap[':PandaID'] = pandaID
        if attemptNr != None:
            sql0  += "AND attemptNr=:attemptNr "
            sql1W += "AND attemptNr=:attemptNr "
            varMap[':attemptNr'] = attemptNr
            varMap0[':attemptNr'] = attemptNr
        # prevent change from holding to transferring which doesn't register files to sub/tid
        if jobStatus == 'transferring':
            sql1W += "AND NOT jobStatus=:ngStatus "
            varMap[':ngStatus'] = 'holding'
        updatedFlag = False
        nTry=1
        for iTry in range(nTry):
            try:
                # begin transaction
                self.conn.begin()
                # select
                self.cur.arraysize = 10
                self.cur.execute (sql0+comment,varMap0)
                res = self.cur.fetchone()
                if res != None:
                    ret = ''
                    commandToPilot,endTime,specialHandling,oldJobStatus,computingSite,cloud,prodSourceLabel,lockedby,jediTaskID,jobsetID = res
                    # debug mode
                    if not specialHandling in [None,''] and 'debug' in specialHandling:
                        ret += 'debug,'
                    # FIXME
                    #else:
                    #    ret += 'debugoff,'
                    # kill command    
                    if not commandToPilot in [None,'']:
                        ret += '%s,' % commandToPilot
                    ret = ret[:-1]
                    # convert empty to NULL
                    if ret == '':
                        ret = 'NULL'
                    # don't update holding or merging
                    if oldJobStatus == 'holding' and jobStatus == 'holding':
                        _logger.debug("updateJobStatus : PandaID=%s skip to reset holding" % pandaID)
                    elif oldJobStatus == 'merging':
                        _logger.debug("updateJobStatus : PandaID=%s skip to change from merging" % pandaID)
                    else:
                        # update stateChangeTime
                        if updateStateChange or (jobStatus=='starting' and oldJobStatus != 'starting'):
                            sql1 += ",stateChangeTime=CURRENT_DATE"
                        # set endTime if undefined for holding
                        if jobStatus == 'holding' and endTime==None and not presetEndTime:
                            sql1 += ',endTime=CURRENT_DATE '
                        # update
                        self.cur.execute (sql1+sql1W+comment,varMap)
                        nUp = self.cur.rowcount
                        _logger.debug("updateJobStatus : PandaID=%s attemptNr=%s nUp=%s" % (pandaID,attemptNr,nUp))
                        if nUp == 1:
                            updatedFlag = True
                        if nUp == 0 and jobStatus == 'transferring':
                            _logger.debug("updateJobStatus : PandaID=%s ignore to update for transferring" % pandaID)
                        # update waiting ES jobs not to get reassigned
                        if updatedFlag and EventServiceUtils.isEventServiceSH(specialHandling):
                            # sql to update ES jobs
                            sqlUE  = "UPDATE ATLAS_PANDA.jobsActive4 SET modificationTime=CURRENT_DATE "
                            sqlUE += "WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID AND jobStatus=:jobStatus "
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':jobsetID']   = jobsetID
                            varMap[':jobStatus']  = 'activated'
                            self.cur.execute (sqlUE+comment,varMap)
                            nUE = self.cur.rowcount
                            _logger.debug("updateJobStatus : PandaID=%s updated %s ES jobs" % (pandaID,nUE))
                        # update nFilesOnHold for JEDI RW calculation
                        if updatedFlag and oldJobStatus != jobStatus and (jobStatus == 'transferring' or oldJobStatus == 'transferring') and \
                                hasattr(panda_config,'useJEDI') and panda_config.useJEDI == True and \
                                lockedby == 'jedi' and self.checkTaskStatusJEDI(jediTaskID,self.cur):
                            # SQL to get file list from Panda
                            sqlJediFP  = "SELECT datasetID,fileID,attemptNr FROM ATLAS_PANDA.filesTable4 "
                            sqlJediFP += "WHERE PandaID=:pandaID AND type IN (:type1,:type2) ORDER BY datasetID,fileID "
                            # SQL to check JEDI files
                            sqlJediFJ  = "SELECT 1 FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                            sqlJediFJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                            sqlJediFJ += "AND attemptNr=:attemptNr AND status=:status AND keepTrack=:keepTrack "
                            # get file list 
                            varMap = {}
                            varMap[':pandaID'] = pandaID
                            varMap[':type1'] = 'input'
                            varMap[':type2'] = 'pseudo_input'
                            self.cur.arraysize = 100000
                            self.cur.execute(sqlJediFP+comment, varMap)
                            resJediFile = self.cur.fetchall()
                            datasetContentsStat = {}
                            # loop over all files
                            for tmpDatasetID,tmpFileID,tmpAttemptNr in resJediFile:
                                # check file in JEDI
                                varMap = {}
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':datasetID']  = tmpDatasetID
                                varMap[':fileID']     = tmpFileID
                                varMap[':attemptNr']  = tmpAttemptNr
                                varMap[':status']     = 'running'
                                varMap[':keepTrack']  = 1
                                self.cur.execute(sqlJediFJ+comment, varMap)
                                res = self.cur.fetchone()
                                if res != None:
                                    if not datasetContentsStat.has_key(tmpDatasetID):
                                        datasetContentsStat[tmpDatasetID] = 0
                                    if jobStatus == 'transferring':
                                        # increment nOnHold
                                        datasetContentsStat[tmpDatasetID] += 1
                                    else:
                                        # decrement nOnHold
                                        datasetContentsStat[tmpDatasetID] -= 1
                            # loop over all datasets
                            tmpDatasetIDs = datasetContentsStat.keys()
                            tmpDatasetIDs.sort()
                            for tmpDatasetID in tmpDatasetIDs:
                                diffNum = datasetContentsStat[tmpDatasetID]
                                # no difference
                                if diffNum == 0:
                                    continue
                                # SQL to lock
                                sqlJediDL  = "SELECT nFilesOnHold FROM ATLAS_PANDA.JEDI_Datasets "
                                sqlJediDL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                                sqlJediDL += "FOR UPDATE NOWAIT "
                                varMap = {}
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':datasetID']  = tmpDatasetID
                                _logger.debug(sqlJediDL+comment+str(varMap))
                                self.cur.execute(sqlJediDL+comment, varMap)
                                # SQL to update 
                                sqlJediDU  = "UPDATE ATLAS_PANDA.JEDI_Datasets SET "
                                if diffNum > 0:
                                    sqlJediDU += "nFilesOnHold=nFilesOnHold+:diffNum "
                                else:
                                    sqlJediDU += "nFilesOnHold=nFilesOnHold-:diffNum "
                                sqlJediDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                                sqlJediDU += "AND NOT type IN (:ngType1,:ngType2) "
                                varMap = {}
                                varMap[':jediTaskID'] = jediTaskID
                                varMap[':datasetID']  = tmpDatasetID
                                varMap[':diffNum']    = abs(diffNum)
                                varMap[':ngType1']    = 'trn_log'
                                varMap[':ngType2']    = 'trn_output'
                                _logger.debug(sqlJediDU+comment+str(varMap))
                                self.cur.execute(sqlJediDU+comment, varMap)
                        # update lastStart
                        if oldJobStatus in ('starting','sent') and jobStatus=='running' and \
                                prodSourceLabel in ('managed','user','panda'):
                            sqlLS  = "UPDATE ATLAS_PANDAMETA.siteData SET lastStart=CURRENT_DATE "
                            sqlLS += "WHERE site=:site AND hours=:hours AND flag IN (:flag1,:flag2) "
                            varMap = {}
                            varMap[':site'] = computingSite
                            varMap[':hours'] = 3
                            varMap[':flag1'] = 'production'
                            varMap[':flag2'] = 'analysis'
                            self.cur.execute(sqlLS+comment, varMap)
                            _logger.debug("updateJobStatus : PandaID=%s attemptNr=%s updated lastStart" % (pandaID,attemptNr))
                else:
                    _logger.debug("updateJobStatus : PandaID=%s attemptNr=%s notFound" % (pandaID,attemptNr))
                    # already deleted or bad attempt number
                    ret = 'tobekilled'
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # record status change
                try:
                    if updatedFlag and oldJobStatus != None and oldJobStatus != jobStatus:
                        self.recordStatusChange(pandaID,jobStatus,
                                                infoMap={'computingSite':computingSite,
                                                         'cloud':cloud,
                                                         'prodSourceLabel':prodSourceLabel})
                except:
                    _logger.error('recordStatusChange in updateJobStatus')
                _logger.debug("updateJobStatus : PandaID=%s done" % pandaID)
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
    def updateJob(self,job,inJobsDefined,oldJobStatus=None,extraInfo=None):
        comment = ' /* DBProxy.updateJob */'        
        _logger.debug("updateJob : %s" % job.PandaID)
        updatedFlag = False
        nTry=3
        for iTry in range(nTry):
            try:
                job.modificationTime = datetime.datetime.utcnow()
                # set stateChangeTime for defined->assigned
                if inJobsDefined:
                    job.stateChangeTime = job.modificationTime
                # make SQL    
                if inJobsDefined:
                    sql1 = "UPDATE ATLAS_PANDA.jobsDefined4 SET %s " % job.bindUpdateChangesExpression()
                else:
                    sql1 = "UPDATE ATLAS_PANDA.jobsActive4 SET %s " % job.bindUpdateChangesExpression()            
                sql1+= "WHERE PandaID=:PandaID "
                if inJobsDefined:        
                    sql1+= " AND (jobStatus=:oldJobStatus1 OR jobStatus=:oldJobStatus2) "
                # begin transaction
                self.conn.begin()
                # update
                varMap = job.valuesMap(onlyChanged=True)
                varMap[':PandaID'] = job.PandaID
                if inJobsDefined:
                    varMap[':oldJobStatus1'] = 'assigned'
                    varMap[':oldJobStatus2'] = 'defined'
                _logger.debug(sql1+comment+str(varMap))                    
                self.cur.execute(sql1+comment, varMap)
                n = self.cur.rowcount                
                if n==0:
                    # already killed or activated
                    _logger.debug("updateJob : Not found %s" % job.PandaID)
                else:
                    # check if JEDI is used
                    useJEDI = False
                    if oldJobStatus != job.jobStatus and (job.jobStatus in ['transferring','merging'] or \
                                                              oldJobStatus in ['transferring','merging']) and \
                            hasattr(panda_config,'useJEDI') and panda_config.useJEDI == True and \
                            job.lockedby == 'jedi' and self.checkTaskStatusJEDI(job.jediTaskID,self.cur):
                        useJEDI = True
                    # SQL to check JEDI files
                    sqlJediFJ  = "SELECT 1 FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                    sqlJediFJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    sqlJediFJ += "AND attemptNr=:attemptNr AND status=:status AND keepTrack=:keepTrack "
                    datasetContentsStat = {}
                    # loop over all files    
                    for file in job.Files:
                        sqlF = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                        varMap = file.valuesMap(onlyChanged=True)
                        if varMap != {}:
                            varMap[':row_ID'] = file.row_ID
                            _logger.debug(sqlF+comment+str(varMap))
                            self.cur.execute(sqlF+comment, varMap)
                        # actions for JEDI
                        if useJEDI and (job.jobStatus == 'transferring' or oldJobStatus == 'transferring') and \
                                file.type in ['input','pseudo_input'] and job.processingType != 'pmerge':
                            # check file in JEDI
                            varMap = {}
                            varMap[':jediTaskID'] = file.jediTaskID
                            varMap[':datasetID']  = file.datasetID
                            varMap[':fileID']     = file.fileID
                            varMap[':attemptNr']  = file.attemptNr
                            varMap[':status']     = 'running'
                            varMap[':keepTrack']  = 1
                            self.cur.execute(sqlJediFJ+comment, varMap)
                            res = self.cur.fetchone()
                            if res != None:
                                if not datasetContentsStat.has_key(file.datasetID):
                                    datasetContentsStat[file.datasetID] = {'diff':0,'cType':'hold'}
                                if job.jobStatus == 'transferring':
                                    # increment nOnHold
                                    datasetContentsStat[file.datasetID]['diff'] += 1
                                else:
                                    # decrement nOnHold
                                    datasetContentsStat[file.datasetID]['diff'] -= 1
                        elif useJEDI and job.jobStatus == 'merging' and file.type in ['log','output']:
                            # SQL to update JEDI files
                            varMap = {}
                            varMap[':fileID']     = file.fileID
                            varMap[':attemptNr']  = file.attemptNr
                            varMap[':datasetID']  = file.datasetID
                            varMap[':keepTrack']  = 1
                            varMap[':jediTaskID'] = file.jediTaskID
                            varMap[':status']     = 'ready'
                            varMap[':boundaryID'] = job.PandaID
                            varMap[':maxAttempt'] = file.attemptNr + 3
                            sqlJFile =  "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents "
                            sqlJFile += "SET status=:status,boundaryID=:boundaryID,maxAttempt=:maxAttempt"
                            for tmpKey in ['lfn','GUID','fsize','checksum']:
                                tmpVal = getattr(file,tmpKey)
                                if tmpVal == 'NULL':
                                    if tmpKey in file._zeroAttrs:
                                        tmpVal = 0
                                    else:
                                        tmpVal = None
                                tmpMapKey = ':%s' % tmpKey        
                                sqlJFile += ",%s=%s" % (tmpKey,tmpMapKey)
                                varMap[tmpMapKey] = tmpVal
                            sqlJFile += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                            sqlJFile += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
                            # update JEDI file
                            _logger.debug(sqlJFile+comment+str(varMap))
                            self.cur.execute(sqlJFile+comment,varMap)
                            nRow = self.cur.rowcount
                            if nRow == 1:
                                if not datasetContentsStat.has_key(file.datasetID):
                                    datasetContentsStat[file.datasetID] = {'diff':0,'cType':'hold'}
                                datasetContentsStat[file.datasetID]['diff'] += 1
                        # update metadata in JEDI
                        if useJEDI and file.type in ['output','log'] and extraInfo != None:
                            varMap = {}
                            sqlFileMeta = ''
                            if extraInfo.has_key('nevents') and file.lfn in extraInfo['nevents']:
                                tmpKey = 'nEvents'
                                tmpMapKey = ':%s' % tmpKey
                                sqlFileMeta += "%s=%s," % (tmpKey,tmpMapKey)
                                varMap[tmpMapKey] = extraInfo['nevents'][file.lfn]
                            if extraInfo.has_key('lbnr') and file.lfn in extraInfo['lbnr']:
                                tmpKey = 'lumiBlockNr'
                                tmpMapKey = ':%s' % tmpKey
                                sqlFileMeta += "%s=%s," % (tmpKey,tmpMapKey)
                                varMap[tmpMapKey] = extraInfo['lbnr'][file.lfn]
                            if varMap != {}:
                                # update
                                varMap[':fileID']     = file.fileID
                                varMap[':attemptNr']  = file.attemptNr
                                varMap[':datasetID']  = file.datasetID
                                varMap[':jediTaskID'] = file.jediTaskID
                                varMap[':keepTrack']  = 1
                                sqlFileMeta = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET " + sqlFileMeta
                                sqlFileMeta = sqlFileMeta[:-1]
                                sqlFileMeta += " "
                                sqlFileMeta += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                                sqlFileMeta += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
                                _logger.debug(sqlFileMeta+comment+str(varMap))
                                self.cur.execute(sqlFileMeta+comment,varMap)
                    # loop over all JEDI datasets
                    tmpDatasetIDs = datasetContentsStat.keys()
                    tmpDatasetIDs.sort()
                    for tmpDatasetID in tmpDatasetIDs:
                        valMap = datasetContentsStat[tmpDatasetID]
                        diffNum = valMap['diff']
                        cType = valMap['cType']
                        # no difference
                        if diffNum == 0:
                            continue
                        # SQL to check lock
                        varMap = {}
                        varMap[':jediTaskID'] = job.jediTaskID
                        varMap[':datasetID']  = tmpDatasetID
                        sqlJediCL  = "SELECT nFilesTobeUsed,nFilesOnHold,status FROM ATLAS_PANDA.JEDI_Datasets "
                        sqlJediCL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                        sqlJediCL += "FOR UPDATE NOWAIT "
                        _logger.debug(sqlJediCL+comment+str(varMap))
                        self.cur.execute(sqlJediCL+comment, varMap)
                        # SQL to update dataset 
                        varMap = {}
                        varMap[':jediTaskID'] = job.jediTaskID
                        varMap[':datasetID']  = tmpDatasetID
                        varMap[':diffNum']    = abs(diffNum)
                        sqlJediDU  = "UPDATE ATLAS_PANDA.JEDI_Datasets SET "
                        if cType == 'hold':
                            if diffNum > 0:
                                sqlJediDU += "nFilesOnHold=nFilesOnHold+:diffNum "
                            else:
                                sqlJediDU += "nFilesOnHold=nFilesOnHold-:diffNum "
                        elif cType == 'touse':
                            varMap[':status'] = 'ready'
                            sqlJediDU += "nFilesTobeUsed=nFilesTobeUsed+:diffNum,status=:status "
                        sqlJediDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                        _logger.debug(sqlJediDU+comment+str(varMap))
                        self.cur.execute(sqlJediDU+comment, varMap)
                    # update job parameters
                    sqlJobP = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                    varMap = {}
                    varMap[':PandaID'] = job.PandaID
                    varMap[':param']   = job.jobParameters
                    self.cur.execute(sqlJobP+comment, varMap)
                    updatedFlag = True
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # record status change
                try:
                    if updatedFlag:
                        self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
                except:
                    _logger.error('recordStatusChange in updateJob')
                return True
            except:
                # roll back
                self._rollback(True)
                if iTry+1 < nTry:
                    _logger.debug("updateJob : %s retry : %s" % (job.PandaID,iTry))
                    time.sleep(random.randint(3,10))
                    continue
                type, value, traceBack = sys.exc_info()
                _logger.error("updateJob : %s %s" % (type,value))
                return False


    # retry analysis job
    def retryJob(self,pandaID,param,failedInActive=False,changeJobInMem=False,inMemJob=None,
                 getNewPandaID=False,attemptNr=None,recoverableEsMerge=False):
        comment = ' /* DBProxy.retryJob */'                
        _logger.debug("retryJob : %s inActive=%s" % (pandaID,failedInActive))
        sql1 = "SELECT %s FROM ATLAS_PANDA.jobsActive4 " % JobSpec.columnNames()
        sql1+= "WHERE PandaID=:PandaID "
        if failedInActive:
            sql1+= "AND jobStatus=:jobStatus "
        updatedFlag = False    
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
                # convert attemptNr to int
                try:
                    attemptNr = int(attemptNr)
                except:
                    _logger.debug("retryJob : %s attemptNr=%s non-integer" % (pandaID,attemptNr))
                    attemptNr = -999
                # check attemptNr
                if attemptNr != None:
                    if job.attemptNr != attemptNr:
                        _logger.debug("retryJob : %s bad attemptNr job.%s != pilot.%s" % (pandaID,job.attemptNr,attemptNr))
                        if not changeJobInMem:
                            # commit
                            if not self._commit():
                                raise RuntimeError, 'Commit error'
                        # return
                        return retValue
                # check if already retried
                if job.taskBufferErrorCode in [ErrorCode.EC_Reassigned,ErrorCode.EC_Retried,ErrorCode.EC_PilotRetried]:
                    _logger.debug("retryJob : %s already retried %s" % (pandaID,job.taskBufferErrorCode))
                    if not changeJobInMem:
                        # commit
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                    # return
                    return retValue
                # use JEDI
                useJEDI = False
                if hasattr(panda_config,'useJEDI') and panda_config.useJEDI == True and \
                        job.lockedby == 'jedi' and self.checkTaskStatusJEDI(job.jediTaskID,self.cur):
                    useJEDI = True
                # check pilot retry
                usePilotRetry = False
                if job.prodSourceLabel in ['user','panda','ptest','rc_test'] and \
                   param.has_key('pilotErrorCode') and \
                   param['pilotErrorCode'].startswith('-') and \
                   job.maxAttempt > job.attemptNr and \
                   (not job.processingType.startswith('gangarobot') or job.processingType=='gangarobot-rctest') and \
                   not job.processingType.startswith('hammercloud'):
                    usePilotRetry = True
                # retry for ES merge
                if recoverableEsMerge and EventServiceUtils.isEventServiceMerge(job):
                    usePilotRetry = True
                # check if it's analysis job # FIXME once pilot retry works correctly the conditions below will be cleaned up
                if (((job.prodSourceLabel == 'user' or job.prodSourceLabel == 'panda') \
                     and not job.processingType.startswith('gangarobot') \
                     and not job.processingType.startswith('hammercloud') \
                     and job.computingSite.startswith('ANALY_') and param.has_key('pilotErrorCode') \
                     and param['pilotErrorCode'] in ['1200','1201','1213'] and (not job.computingSite.startswith('ANALY_LONG_')) \
                     and job.attemptNr < 2) or (job.prodSourceLabel == 'ddm' and job.cloud == 'CA' and job.attemptNr <= 10) \
                     or failedInActive or usePilotRetry) \
                     and job.commandToPilot != 'tobekilled':
                    # check attemptNr for JEDI
                    moreRetryForJEDI = True
                    if useJEDI:
                        moreRetryForJEDI = self.checkMoreRetryJEDI(job)
                    # OK in JEDI
                    if moreRetryForJEDI:
                        _logger.debug('reset PandaID:%s #%s' % (job.PandaID,job.attemptNr))
                        if not changeJobInMem:
                            # job parameters
                            sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                            varMap = {}
                            varMap[':PandaID'] = job.PandaID
                            self.cur.execute(sqlJobP+comment, varMap)
                            for clobJobP, in self.cur:
                                try:
                                    job.jobParameters = clobJobP.read()
                                except AttributeError:
                                    job.jobParameters = str(clobJobP)
                                break
                        # reset job
                        job.jobStatus = 'activated'
                        job.startTime = None
                        job.modificationTime = datetime.datetime.utcnow()
                        job.attemptNr = job.attemptNr + 1
                        if usePilotRetry:
                            job.currentPriority -= 10
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
                                tmpLongSite = re.sub('SHORT','LONG',job.computingSite)
                                if tmpLongSite != job.computingSite:
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
                                    _logger.debug('sending PandaID:%s to %s' % (job.PandaID,longSite))
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
                            # don't change input or lib.tgz, or ES merge output/log since it causes a problem with input name construction
                            if file.type in ['input','pseudo_input'] or (file.type == 'output' and job.prodSourceLabel == 'panda') or \
                                   (file.type == 'output' and file.lfn.endswith('.lib.tgz') and job.prodSourceLabel in ['rc_test','ptest']) or \
                                   recoverableEsMerge:
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
                            if not recoverableEsMerge:
                                sepPatt = "(\'|\"|%20|:)" + oldName + "(\'|\"|%20| )"
                            else:
                                sepPatt = "(\'|\"| |:|=)" + oldName + "(\'|\"| |<|$)"
                            matches = re.findall(sepPatt,job.jobParameters)
                            for match in matches:
                                oldPatt = match[0]+oldName+match[-1]
                                newPatt = match[0]+newName+match[-1]
                                job.jobParameters = re.sub(oldPatt,newPatt,job.jobParameters)
                            if not changeJobInMem and not getNewPandaID:
                                # reset file status
                                if file.type in ['output','log']:
                                    file.status = 'unknown'
                                # update files
                                sqlFup = ("UPDATE ATLAS_PANDA.filesTable4 SET %s" % file.bindUpdateChangesExpression()) + "WHERE row_ID=:row_ID"
                                varMap = file.valuesMap(onlyChanged=True)
                                if varMap != {}:
                                    varMap[':row_ID'] = file.row_ID
                                    self.cur.execute(sqlFup+comment, varMap)
                        if not changeJobInMem:
                            # reuse original PandaID
                            if not getNewPandaID:
                                # update job
                                sql2 = "UPDATE ATLAS_PANDA.jobsActive4 SET %s " % job.bindUpdateChangesExpression()            
                                sql2+= "WHERE PandaID=:PandaID "
                                varMap = job.valuesMap(onlyChanged=True)
                                varMap[':PandaID'] = job.PandaID
                                self.cur.execute(sql2+comment, varMap)
                                # update job parameters
                                sqlJobP = "UPDATE ATLAS_PANDA.jobParamsTable SET jobParameters=:param WHERE PandaID=:PandaID"
                                varMap = {}
                                varMap[':PandaID'] = job.PandaID
                                varMap[':param']   = job.jobParameters
                                self.cur.execute(sqlJobP+comment, varMap)
                                updatedFlag = True
                            else:
                                # read metadata
                                sqlMeta = "SELECT metaData FROM ATLAS_PANDA.metaTable WHERE PandaID=:PandaID"
                                varMap = {}
                                varMap[':PandaID'] = job.PandaID
                                self.cur.execute(sqlMeta+comment, varMap)
                                for clobJobP, in self.cur:
                                    try:
                                        job.metadata = clobJobP.read()
                                    except AttributeError:
                                        job.metadata = str(clobJobP)
                                    break
                                # insert job with new PandaID
                                sql1 = "INSERT INTO ATLAS_PANDA.jobsActive4 (%s) " % JobSpec.columnNames()
                                sql1+= JobSpec.bindValuesExpression(useSeq=True)
                                sql1+= " RETURNING PandaID INTO :newPandaID"
                                # set parentID
                                job.parentID = job.PandaID
                                job.creationTime = datetime.datetime.utcnow()
                                job.modificationTime = job.creationTime
                                varMap = job.valuesMap(useSeq=True)
                                varMap[':newPandaID'] = self.cur.var(varNUMBER)
                                # insert
                                retI = self.cur.execute(sql1+comment, varMap)
                                # set PandaID
                                job.PandaID = long(self.cur.getvalue(varMap[':newPandaID']))
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
                                    varMap[':newRowID'] = self.cur.var(varNUMBER)
                                    self.cur.execute(sqlFile+comment, varMap)
                                    file.row_ID = long(self.cur.getvalue(varMap[':newRowID']))
                                # metadata
                                if job.VO != 'cms':
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
                                # set error code to original job to avoid being retried by another process
                                sqlE = "UPDATE ATLAS_PANDA.jobsActive4 SET taskBufferErrorCode=:errCode,taskBufferErrorDiag=:errDiag WHERE PandaID=:PandaID"
                                varMap = {}
                                varMap[':PandaID'] = job.parentID
                                varMap[':errCode'] = ErrorCode.EC_PilotRetried
                                varMap[':errDiag'] = 'retrying at the same site. new PandaID=%s' % job.PandaID
                                self.cur.execute(sqlE+comment, varMap)
                                # propagate change to JEDI
                                if useJEDI:
                                    self.updateForPilotRetryJEDI(job,self.cur)
                        # set return
                        if not getNewPandaID:
                            retValue = True
                if not changeJobInMem:    
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # record status change
                    try:
                        if updatedFlag:
                            self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
                    except:
                        _logger.error('recordStatusChange in retryJob')
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
    def retryJobsInActive(self,prodUserName,jobDefinitionID,isJEDI=False):
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
            sql0 = "SELECT PandaID,jobStatus,taskBufferErrorCode,attemptNr FROM ATLAS_PANDA.jobsActive4 "
            sql0+= "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
            sql0+= "AND prodSourceLabel=:prodSourceLabel "
            if isJEDI:
                sql0+= "AND attemptNr<maxAttempt " 
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
            for pandaID,tmpJobStatus,tmpTaskBufferErrorCode,tmpAttemptNr in res:
                if tmpJobStatus == 'failed' and not tmpTaskBufferErrorCode in \
                       [ErrorCode.EC_Reassigned,ErrorCode.EC_Retried,ErrorCode.EC_PilotRetried]:
                    failedPandaIDs.append((pandaID,tmpAttemptNr))
            _logger.debug("retryJobsInActive : %s %s - %s failed jobs" % (prodUserName,jobDefinitionID,len(failedPandaIDs)))
            # there are some failed jobs in Active
            if failedPandaIDs != []:
                # get list of sub datasets to lock Closer
                sqlF  = "SELECT DISTINCT destinationDBlock FROM ATLAS_PANDA.filesTable4 "
                sqlF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
                varMap = {}
                varMap[':PandaID'] = failedPandaIDs[0][0]
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
                    for pandaID,tmpAttemptNr in failedPandaIDs:
                        retryRet = self.retryJob(pandaID,{},failedInActive=True,attemptNr=tmpAttemptNr)
                        _logger.debug("retryJobsInActive : %s %s - PandaID=%s %s" % (prodUserName,jobDefinitionID,pandaID,retryRet))
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
                atlasRelease,prodUserID,countryGroup,workingGroup,allowOtherCountry,taskID):
        comment = ' /* DBProxy.getJobs */'
        # use memcache
        useMemcache = False
        try:
            if panda_config.memcached_enable and siteName in panda_config.memcached_sites: # FIXME
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
            sql1+= "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3) "
            getValMap[':prodSourceLabel1'] = 'user'
            getValMap[':prodSourceLabel2'] = 'panda'
            getValMap[':prodSourceLabel3'] = 'install'
        elif prodSourceLabel == 'ddm':
            dynamicBrokering = True
            sql1+= "AND prodSourceLabel=:prodSourceLabel "
            getValMap[':prodSourceLabel'] = 'ddm'
        elif prodSourceLabel in [None,'managed']:
            sql1+= "AND prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3,:prodSourceLabel4) "
            getValMap[':prodSourceLabel1'] = 'managed'
            getValMap[':prodSourceLabel2'] = 'test'
            getValMap[':prodSourceLabel3'] = 'prod_test'
            getValMap[':prodSourceLabel4'] = 'install'
        elif prodSourceLabel == 'software':
            sql1+= "AND prodSourceLabel=:prodSourceLabel "
            getValMap[':prodSourceLabel'] = 'software'
        elif prodSourceLabel == 'test' and computingElement != None:
            dynamicBrokering = True
            sql1+= "AND (processingType IN (:processingType1,:processingType2,:processingType3) "
            sql1+= "OR prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2,:prodSourceLabel3)) "
            getValMap[':processingType1']   = 'gangarobot'
            getValMap[':processingType2']   = 'analy_test'            
            getValMap[':processingType3']   = 'prod_test'            
            getValMap[':prodSourceLabel1']  = 'test'
            getValMap[':prodSourceLabel2']  = 'prod_test'
            getValMap[':prodSourceLabel3'] = 'install'
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
        # taskID
        if taskID != None:
            sql1+= "AND jediTaskID=:taskID "
            getValMap[':taskID'] = taskID
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
        getValMapOrig = copy.copy(getValMap)
        try:
            timeLimit = datetime.timedelta(seconds=timeout-10)
            timeStart = datetime.datetime.utcnow()
            strName   = datetime.datetime.isoformat(timeStart)
            attLimit  = datetime.datetime.utcnow() - datetime.timedelta(minutes=15)
            attSQL    = "AND ((creationTime<:creationTime AND attemptNr>1) OR attemptNr<=1) "
            # get nJobs
            for iJob in range(nJobs):
                getValMap = copy.copy(getValMapOrig)
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
                        maxRunning = 15
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
                                getValMap[':currentPriority'] = tmpPriority
                        maxAttemptIDx = 10
                        if toGetPandaIDs:
                            # get PandaIDs
                            sqlP = "SELECT /*+ INDEX_RS_ASC(tab (PRODSOURCELABEL COMPUTINGSITE JOBSTATUS) ) */ PandaID,currentPriority,specialHandling FROM ATLAS_PANDA.jobsActive4 tab "
                            sqlP+= sql1
                            if ':currentPriority' in getValMap:
                                sqlP += "AND currentPriority=:currentPriority "
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
                                sqlSent  = "SELECT count(*) FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus "
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
                # sql to read range
                sqlRR  = "SELECT PandaID,job_processID,attemptNr "
                sqlRR += "FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
                sqlRR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:eventStatus "
                # read parent PandaID
                sqlPP  = "SELECT DISTINCT oldPandaID FROM {0}.JEDI_Job_Retry_History ".format(panda_config.schemaJEDI)
                sqlPP += "WHERE jediTaskID=:jediTaskID AND newPandaID=:pandaID " # FIXME 'to ignore normal retry chain by JEDI' AND type=:esRetry "
                # read files
                sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
                sqlFile+= "WHERE PandaID=:PandaID"
                self.cur.arraysize = 10000                                
                self.cur.execute(sqlFile+comment, varMap)
                resFs = self.cur.fetchall()
                eventRangeIDs = {}
                esDonePandaIDs = []
                for resF in resFs:
                    file = FileSpec()
                    file.pack(resF)
                    # add files
                    if not EventServiceUtils.isEventServiceMerge(job) or file.type in ['output','log']: 
                        job.addFile(file)
                    # get event ragnes for event service
                    if EventServiceUtils.isEventServiceMerge(job):
                        # only for input
                        if not file.type in ['output','log']:
                            # get ranges
                            varMap = {}
                            varMap[':jediTaskID'] = file.jediTaskID
                            varMap[':datasetID']  = file.datasetID
                            varMap[':fileID']     = file.fileID
                            varMap[':eventStatus'] = EventServiceUtils.ST_done
                            self.cur.execute(sqlRR+comment, varMap)
                            resRR = self.cur.fetchall()
                            for esPandaID,job_processID,attemptNr in resRR:
                                tmpEventRangeID = self.makeEventRangeID(file.jediTaskID,esPandaID,file.fileID,job_processID,attemptNr)
                                if not eventRangeIDs.has_key(file.fileID):
                                    eventRangeIDs[file.fileID] = {}
                                addFlag = False
                                if not job_processID in eventRangeIDs[file.fileID]:
                                    addFlag= True
                                else:
                                    oldEsPandaID = eventRangeIDs[file.fileID][job_processID]['pandaID']
                                    if esPandaID > oldEsPandaID:
                                        addFlag= True
                                        if oldEsPandaID in esDonePandaIDs:
                                            esDonePandaIDs.remove(oldEsPandaID)
                                if addFlag:
                                    eventRangeIDs[file.fileID][job_processID] = {'pandaID':esPandaID,
                                                                                 'eventRangeID':tmpEventRangeID}
                                    if not esPandaID in esDonePandaIDs:
                                        esDonePandaIDs.append(esPandaID)
                # make input for event service output merging
                mergeInputOutputMap = {}
                mergeInputFiles = []
                for tmpFileID,tmpMapEventRangeID in eventRangeIDs.iteritems():
                    jobProcessIDs = tmpMapEventRangeID.keys()
                    jobProcessIDs.sort()
                    # make input
                    for jobProcessID in jobProcessIDs:
                        for tmpFileSpec in job.Files:
                            if not tmpFileSpec.type in ['output']:
                                continue
                            tmpInputFileSpec = copy.copy(tmpFileSpec)
                            tmpInputFileSpec.type = 'input'
                            # append eventRangeID as suffix
                            tmpInputFileSpec.lfn  = tmpInputFileSpec.lfn + \
                                '.' + tmpMapEventRangeID[jobProcessID]['eventRangeID']
                            # add file
                            mergeInputFiles.append(tmpInputFileSpec)
                            # make input/output map
                            if not mergeInputOutputMap.has_key(tmpFileSpec.lfn):
                                mergeInputOutputMap[tmpFileSpec.lfn] = []
                            mergeInputOutputMap[tmpFileSpec.lfn].append(tmpInputFileSpec.lfn)
                for tmpInputFileSpec in mergeInputFiles:
                    job.addFile(tmpInputFileSpec)
                # make input for event service log merging
                mergeLogFiles = []
                for tmpFileSpec in job.Files:
                    if not tmpFileSpec.type in ['log']:
                        continue
                    # make files
                    for esPandaID in esDonePandaIDs:
                        tmpInputFileSpec = copy.copy(tmpFileSpec)
                        tmpInputFileSpec.type = 'input'
                        tmpInputFileSpec.GUID = None
                        # append PandaID as suffix
                        tmpInputFileSpec.lfn  = tmpInputFileSpec.lfn + '.{0}'.format(esPandaID)
                        # add file
                        mergeLogFiles.append(tmpInputFileSpec)
                        # make input/output map
                        if not mergeInputOutputMap.has_key(tmpFileSpec.lfn):
                            mergeInputOutputMap[tmpFileSpec.lfn] = []
                        mergeInputOutputMap[tmpFileSpec.lfn].append(tmpInputFileSpec.lfn)
                for tmpInputFileSpec in mergeLogFiles:
                    job.addFile(tmpInputFileSpec)
                # job parameters
                sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                varMap = {}
                varMap[':PandaID'] = job.PandaID
                self.cur.execute(sqlJobP+comment, varMap)
                for clobJobP, in self.cur:
                    try:
                        job.jobParameters = clobJobP.read()
                    except AttributeError:
                        job.jobParameters = str(clobJobP)
                    break
                # remove or extract parameters for merge
                if EventServiceUtils.isEventServiceJob(job):
                    try:
                        job.jobParameters = re.sub('<PANDA_ESMERGE_.+>.*</PANDA_ESMERGE_.+>','',job.jobParameters)
                    except:
                        pass
                elif EventServiceUtils.isEventServiceMerge(job):
                    try:
                        origJobParameters = job.jobParameters
                        tmpMatch = re.search('<PANDA_ESMERGE_JOBP>(.*)</PANDA_ESMERGE_JOBP>',origJobParameters)
                        job.jobParameters = tmpMatch.group(1)
                        tmpMatch = re.search('<PANDA_ESMERGE_TRF>(.*)</PANDA_ESMERGE_TRF>',origJobParameters)
                        job.transformation = tmpMatch.group(1)
                    except:
                        pass
                    # pass in/out map for merging via metadata
                    job.metadata = mergeInputOutputMap
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # overwrite processingType for appdir at aggrigates sites
                if aggSiteMap.has_key(siteName):
                    if aggSiteMap[siteName].has_key(job.computingSite):
                        job.processingType = aggSiteMap[siteName][job.computingSite]
                        job.computingSite  = job.computingSite
                # append
                retJobs.append(job)
                # record status change
                try:
                    self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
                except:
                    _logger.error('recordStatusChange in getJobs')
            return retJobs,nSent
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = "getJobs : %s %s" % (errtype,errvalue)
            errStr.strip()
            errStr += traceback.format_exc()
            _logger.error(errStr)
            # roll back
            self._rollback()
            return [],0
        

    # reset job in jobsActive or jobsWaiting
    def resetJob(self,pandaID,activeTable=True,keepSite=False,getOldSubs=False,forPending=True):
        comment = ' /* DBProxy.resetJob */'        
        _logger.debug("resetJob : %s activeTable=%s" % (pandaID,activeTable))
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
            if job.prodSourceLabel in ['user','panda'] and not forPending \
                    and job.jobStatus != 'pending':
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
            if not job.prodSourceLabel in ['user','panda']:
                job.dispatchDBlock   = None
                # erase old assignment
                if (not keepSite) and not job.relocationFlag in [1,2]:
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
                try:
                    job.jobParameters = clobJobP.read()
                except AttributeError:
                    job.jobParameters = str(clobJobP)
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
                if job.lockedby != 'jedi':
                    file.status         ='unknown'
                if not job.prodSourceLabel in ['user','panda']:    
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
            # record status change
            try:
                self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
            except:
                _logger.error('recordStatusChange in resetJobs')
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
            updatedFlag = False
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
                if (not keepSite) and not job.relocationFlag in [1,2]:
                    # erase old assignment
                    job.computingSite = None
                job.computingElement = None
                # job parameters
                sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                self.cur.execute(sqlJobP+comment, varMap)
                for clobJobP, in self.cur:
                    try:
                        job.jobParameters = clobJobP.read()
                    except AttributeError:
                        job.jobParameters = str(clobJobP)
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
                    if job.lockedby != 'jedi':
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
                updatedFlag = True        
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # record status change
            try:
                if updatedFlag:
                    self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
            except:
                _logger.error('recordStatusChange in resetDefinedJobs')
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
        # 2  : expire
        # 3  : aborted
        # 4  : expire in waiting
        # 7  : retry by server
        # 8  : rebrokerage
        # 9  : force kill
        # 50 : kill by JEDI
        # 51 : reassigned by JEDI
        # 52 : force kill by JEDI
        # 91 : kill user jobs with prod role
        comment = ' /* DBProxy.killJob */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += ' <PandaID={0}>'.format(pandaID)
        _logger.debug("%s : code=%s role=%s user=%s wg=%s" % (methodName,code,prodManager,user,wgProdRole))
        # check PandaID
        try:
            long(pandaID)
        except:
            _logger.error("not an integer : %s" % pandaID)
            if getUserInfo:
                return False,{}                
            return False
        sql0  = "SELECT prodUserID,prodSourceLabel,jobDefinitionID,jobsetID,workingGroup,specialHandling,jobStatus FROM %s WHERE PandaID=:PandaID"        
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
        sqlFile = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
        sqlFile+= "WHERE PandaID=:PandaID"
        try:
            flagCommand = False
            flagKilled  = False
            userProdUserID      = ''
            userProdSourceLabel = ''
            userJobDefinitionID = ''
            userJobsetID        = ''
            updatedFlag = False
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
                userProdUserID,userProdSourceLabel,userJobDefinitionID,userJobsetID,workingGroup,specialHandling,jobStatusInDB = res
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
                    if res[1] in ['user','panda'] and (not code in ['2','4','7','8','9','50','51','52','91']):
                        _logger.debug("%s ignored -> prod proxy tried to kill analysis job type=%s" % (methodName,res[1]))
                        break
                    _logger.debug("%s using prod role" % methodName)
                elif validGroupProdRole:
                    # WGs with prod role
                    _logger.debug("%s using group prod role for workingGroup=%s" % (methodName,workingGroup))
                    pass
                else:   
                    cn1 = getCN(res[0])
                    cn2 = getCN(user)
                    _logger.debug("%s Owner:%s  - Requester:%s " % (methodName,cn1,cn2))
                    if cn1 != cn2:
                        _logger.debug("%s ignored  -> Owner != Requester" % methodName)
                        break
                # event service
                useEventService =  EventServiceUtils.isEventServiceSH(specialHandling)
                # update
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':commandToPilot'] = 'tobekilled'
                varMap[':taskBufferErrorDiag'] = 'killed by %s' % user
                if code in ['9','52','2']:
                    # ignore commandToPilot for force kill
                    self.cur.execute((sql1F+comment) % table, varMap)
                elif useEventService or jobStatusInDB in ['merging']:
                    # use force kill for event service or merging
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
                if (userProdSourceLabel in ['managed','test',None] or 'test' in userProdSourceLabel) and code in ['9','52']:
                    # use dummy for force kill
                    varMap[':jobStatus'] = 'dummy'
                elif useEventService or jobStatusInDB in ['merging']:
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
                oldJobStatus = job.jobStatus
                # error code
                if job.jobStatus != 'failed':
                    currentTime = datetime.datetime.utcnow()
                    # set status etc for non-failed jobs
                    if job.endTime in [None,'NULL']:
                        job.endTime = currentTime
                    # reset startTime for aCT where starting jobs don't acutally get started 
                    if job.jobStatus == 'starting':
                        job.startTime = job.endTime
                    job.modificationTime = currentTime
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
                    elif code in ['50','52']:
                        # killed by JEDI
                        job.taskBufferErrorCode = ErrorCode.EC_Kill
                        job.taskBufferErrorDiag = user
                    elif code=='51':
                        # reassigned by JEDI
                        job.taskBufferErrorCode = ErrorCode.EC_Kill
                        job.taskBufferErrorDiag = 'reassigned by JEDI'
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
                updatedFlag = True
                # update JEDI tables
                if hasattr(panda_config,'useJEDI') and panda_config.useJEDI == True and \
                        job.lockedby == 'jedi' and self.checkTaskStatusJEDI(job.jediTaskID,self.cur):
                    # read files
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    self.cur.arraysize = 10000
                    self.cur.execute(sqlFile+comment, varMap)
                    resFs = self.cur.fetchall()
                    for resF in resFs:
                        fileSpec = FileSpec()
                        fileSpec.pack(resF)
                        job.addFile(fileSpec)
                    # kill associated consumers for event service
                    if useEventService:
                        self.killEventServiceConsumers(job,True,False)
                        self.killUnusedEventServiceConsumers(job,False)
                    # update JEDI
                    self.propagateResultToJEDI(job,self.cur,oldJobStatus)
                break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("%s com=%s kill=%s " % (methodName,flagCommand,flagKilled))
            # record status change
            try:
                if updatedFlag:
                    self.recordStatusChange(job.PandaID,job.jobStatus,jobInfo=job)
            except:
                _logger.error('recordStatusChange in killJob')
            if getUserInfo:
                return (flagCommand or flagKilled),{'prodUserID':userProdUserID,
                                                    'prodSourceLabel':userProdSourceLabel,
                                                    'jobDefinitionID':userJobDefinitionID,
                                                    'jobsetID':userJobsetID}
            return (flagCommand or flagKilled)
        except:
            self.dumpErrorMessage(_logger,methodName)
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
        # only int
        try:
            tmpID = int(pandaID)
        except:
            _logger.debug("peekJob : return None for %s:non-integer" % pandaID)
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
                                    try:
                                        resMeta = clobMeta.read()
                                    except AttributeError:
                                        resMeta = str(clobMeta)
                                break
                        # job parameters
                        job.jobParameters = None
                        sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
                        varMap = {}
                        varMap[':PandaID'] = job.PandaID
                        self.cur.execute(sqlJobP+comment, varMap)
                        for clobJobP, in self.cur:
                            if clobJobP != None:
                                try:
                                    job.jobParameters = clobJobP.read()
                                except AttributeError:
                                    job.jobParameters = str(clobJobP)
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


    # get PandaIDs with TaskID
    def getPandaIDsWithTaskID(self,jediTaskID):
        comment = ' /* DBProxy.getPandaIDsWithTaskID */'                        
        methodName = comment.split(' ')[-2].split('.')[-1]
        tmpLog = LogWrapper(_logger,methodName+" <jediTaskID={0}>".format(jediTaskID))
        tmpLog.debug("start")
        # SQL
        sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsWaiting4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        sql += "UNION "
        sql += "SELECT PandaID FROM ATLAS_PANDA.jobsDefined4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        sql += "UNION "
        sql += "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
        sql += "WHERE jediTaskID=:jediTaskID "
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000000
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            retList = []
            for pandaID, in res:
                retList.append(pandaID)
                
            tmpLog.debug("found {0} IDs".format(len(retList)))
            return retList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return []


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


    # get active debug jobs
    def getActiveDebugJobs(self,dn=None,workingGroup=None,prodRole=False):
        comment = ' /* DBProxy.getActiveDebugJobs */'                        
        _logger.debug("getActiveDebugJobs : DN={0} wg={1} prodRole={2}".format(dn,workingGroup,prodRole))
        varMap = {}
        sqlX  = "SELECT PandaID,jobStatus,specialHandling FROM %s "
        sqlX += "WHERE "
        if prodRole:
            pass
        elif workingGroup != None:
            sqlX += "UPPER(workingGroup) IN (:wg1,:wg2) AND "
            varMap[':wg1'] = 'AP_{0}'.format(workingGroup.upper())
            varMap[':wg2'] = 'GP_{0}'.format(workingGroup.upper())
        else:
            sqlX += "prodUserName=:prodUserName AND "
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            varMap[':prodUserName'] = compactDN
        sqlX += "specialHandling IS NOT NULL "
        try:
            debugStr = 'debug'
            activeDebugJobs = []
            # loop over tables
            for table in ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4']:
                sql = sqlX % table
                # start transaction
                self.conn.begin()
                # get jobs with specialHandling
                self.cur.arraysize = 100000
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # loop over all PandaIDs
                for pandaID,jobStatus,specialHandling in res:
                    if specialHandling == None:
                        continue
                    # only active jobs
                    if not jobStatus in ['defined','activated','running','sent','starting']:
                        continue
                    # look for debug jobs
                    if debugStr in specialHandling and not pandaID in activeDebugJobs:
                        activeDebugJobs.append(pandaID)
            # return        
            activeDebugJobs.sort()
            _logger.debug("getActiveDebugJobs : DN=%s -> %s" % (dn,str(activeDebugJobs)))
            return activeDebugJobs
        except:
            # roll back
            self._rollback()
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getActiveDebugJobs : %s %s" % (errtype,errvalue))
            return None


    # set debug mode
    def setDebugMode(self,dn,pandaID,prodManager,modeOn,workingGroup):
        comment = ' /* DBProxy.setDebugMode */'                        
        _logger.debug("turnDebugModeOn : dn=%s id=%s prod=%s wg=%s mode=%s" % (dn,pandaID,prodManager,workingGroup,modeOn))
        sqlX  = "SELECT prodUserName,jobStatus,specialHandling,workingGroup FROM %s "
        sqlX += "WHERE PandaID=:PandaID "
        sqlU  = "UPDATE %s SET specialHandling=:specialHandling "
        sqlU += "WHERE PandaID=:PandaID "        
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            debugStr = 'debug'
            retStr = ''
            retCode = False
            # loop over tables
            for table in ['ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsActive4']:
                varMap = {}
                varMap[':PandaID'] = pandaID
                sql = sqlX % table
                # start transaction
                self.conn.begin()
                # get jobs with specialHandling
                self.cur.arraysize = 10
                self.cur.execute(sql+comment, varMap)
                res = self.cur.fetchone()
                # not found
                if res == None:
                    retStr = 'PandaID={0} not found in active DB'.format(pandaID)
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    continue
                prodUserName,jobStatus,specialHandling,wGroup = res
                # not active
                changeableState = ['defined','activated','running','sent','starting','assigned']
                if not jobStatus in changeableState:
                    retStr = 'Cannot set debugMode since the job status is {0} which is not in one of {1}'.format(jobStatus,
                                                                                                                  str(changeableState))
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    break
                # extract workingGroup
                try:
                    wGroup = wGroup.split('_')[-1]
                    wGroup = wGroup.lower()
                except:
                    pass
                # not owner
                notOwner = False
                if not prodManager:
                    if workingGroup != None:
                        if workingGroup.lower() != wGroup:
                            retStr = 'Permission denied. Not the production manager for workingGroup={0}'.format(wGroup)
                            notOwner = True
                    else:
                        if prodUserName != compactDN:
                            retStr = 'Permission denied. Not the owner or production manager'
                            notOwner = True
                    if notOwner:
                        # commit
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        break
                # set specialHandling
                updateSH = True
                if specialHandling in [None,'']:
                    if modeOn:
                        # set debug mode
                        specialHandling = debugStr
                    else:
                        # already disabled debug mode
                        updateSH = False
                elif debugStr in specialHandling:
                    if modeOn:
                        # already in debug mode
                        updateSH = False
                    else:
                        # disable debug mode
                        specialHandling = re.sub(debugStr,'',specialHandling)
                        specialHandling = re.sub(',,',',',specialHandling)
                        specialHandling = re.sub('^,','',specialHandling)
                        specialHandling = re.sub(',$','',specialHandling)
                else:
                    if modeOn:
                        # set debug mode
                        specialHandling = '%s,%s' % (debugStr,specialHandling)
                    else:
                        # already disabled debug mode
                        updateSH = False
                                                
                # no update
                if not updateSH:
                    retStr = 'Already set accordingly'
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    break
                # update
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':specialHandling'] = specialHandling
                self.cur.execute((sqlU+comment) % table, varMap)
                retD = self.cur.rowcount
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                if retD == 0:
                    retStr = 'Failed to update DB'
                else:
                    retStr = 'Succeeded'
                    break
            # return        
            _logger.debug("setDebugMode : %s %s -> %s" % (dn,pandaID,retStr))
            return retStr
        except:
            # roll back
            self._rollback()
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("setDebugMode : %s %s" % (errtype,errvalue))
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


    # get destSE with destinationDBlock
    def getDestSEwithDestDBlock(self,destinationDBlock):
        comment = ' /* DBProxy.getDestSEwithDestDBlock */'                        
        _logger.debug("getDestSEwithDestDBlock : %s" % destinationDBlock)
        try:
            sqlP  = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ destinationSE,destinationDBlockToken FROM ATLAS_PANDA.filesTable4 tab "
            sqlP += "WHERE type IN (:type1,:type2) AND destinationDBlock=:destinationDBlock AND rownum<=1"
            # start transaction
            self.conn.begin()
            varMap = {}
            varMap[':type1'] = 'log'
            varMap[':type2'] = 'output'
            varMap[':destinationDBlock'] = destinationDBlock
            # select
            self.cur.arraysize = 10
            self.cur.execute(sqlP+comment, varMap)
            res = self.cur.fetchone()
            # append
            destinationSE = None
            destinationDBlockToken = None
            if res != None:
                destinationSE,destinationDBlockToken = res
            # commit to release tables
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # return
            return destinationSE,destinationDBlockToken
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getDestSEwithDestDBlock : %s %s" % (errType,errValue))
            # return empty list
            return None,None
            

    # get number of activated/defined jobs with output datasets
    def getNumWaitingJobsWithOutDS(self,outputDSs):
        comment = ' /* DBProxy.getNumWaitingJobsWithOutDS */'                        
        _logger.debug("getNumWaitingJobsWithOutDS : %s" % str(outputDSs))
        try:
            sqlD  = "SELECT distinct destinationDBlock FROM ATLAS_PANDA.filesTable4 "
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
                sql += "AND prodSourceLabel=:prodSourceLabel AND lockedBy<>:ngLock GROUP BY jobDefinitionID"
                varMap = {}
                varMap[':prodUserName'] = compactDN
                varMap[':prodSourceLabel']  = 'user'
                varMap[':ngLock'] = 'jedi'
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
    def lockJobsForReassign(self,tableName,timeLimit,statList,labels,processTypes,sites,clouds,
                            useJEDI=False,onlyReassignable=False,useStateChangeTime=False):
        comment = ' /* DBProxy.lockJobsForReassign */'                        
        _logger.debug("lockJobsForReassign : %s %s %s %s %s %s %s %s" % \
                      (tableName,timeLimit,statList,labels,processTypes,sites,clouds,useJEDI))
        try:
            # make sql
            if not useJEDI:
                sql  = "SELECT PandaID FROM %s " % tableName
            else:
                sql  = "SELECT PandaID,lockedby FROM %s " % tableName
            if not useStateChangeTime:
                sql += "WHERE modificationTime<:modificationTime "
            else:
                sql += "WHERE stateChangeTime<:modificationTime "
            varMap = {}
            varMap[':modificationTime'] = timeLimit
            if statList != []:
                sql += 'AND jobStatus IN ('
                tmpIdx = 0
                for tmpStat in statList:
                    tmpKey = ':stat%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ') '
            if labels != []:
                sql += 'AND prodSourceLabel IN ('
                tmpIdx = 0
                for tmpStat in labels:
                    tmpKey = ':label%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ') '
            if processTypes != []:
                sql += 'AND processingType IN ('
                tmpIdx = 0
                for tmpStat in processTypes:
                    tmpKey = ':processType%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ') '
            if sites != []:
                sql += 'AND computingSite IN ('
                tmpIdx = 0
                for tmpStat in sites:
                    tmpKey = ':site%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ') '
            if clouds != []:
                sql += 'AND cloud IN ('
                tmpIdx = 0
                for tmpStat in clouds:
                    tmpKey = ':cloud%s' % tmpIdx
                    varMap[tmpKey] = tmpStat
                    sql += '%s,' % tmpKey
                    tmpIdx += 1
                sql = sql[:-1]
                sql += ') '
            if onlyReassignable:
                sql += "AND (relocationFlag IS NULL OR relocationFlag<>:relocationFlag) "
                varMap[':relocationFlag'] = 2
            # sql for lock
            if not useStateChangeTime:
                sqlLock = 'UPDATE %s SET modificationTime=CURRENT_DATE WHERE PandaID=:PandaID' % tableName
            else:
                sqlLock = 'UPDATE %s SET stateChangeTime=CURRENT_DATE WHERE PandaID=:PandaID' % tableName
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000000
            _logger.debug(sql+comment+str(varMap))
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            _logger.debug("lockJobsForReassign : found %s" % (len(resList)))
            retList = []
            # lock
            for tmpItem in resList:
                tmpID = tmpItem[0]
                varLock = {':PandaID':tmpID}
                self.cur.execute(sqlLock+comment,varLock)
                retList.append(tmpItem)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # sort
            retList.sort()
            _logger.debug("lockJobsForReassign : return %s" % (len(retList)))
            return True,retList
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("lockJobsForReassign : %s %s" % (errType,errValue))
            # return empty
            return False,[]


    # lock jobs for finisher
    def lockJobsForFinisher(self,timeNow,rownum,highPrio):
        comment = ' /* DBProxy.lockJobsForFinisher */'                        
        _logger.debug("lockJobsForFinisher : %s %s %s" % (timeNow,rownum,highPrio))
        try:
            varMap = {}
            varMap[':jobStatus'] = 'transferring'
            varMap[':currentPriority'] = 800
            varMap[':pLabel1'] = 'managed'
            varMap[':pLabel2'] = 'test'
            # make sql
            sql  = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 "
            sql += "WHERE jobStatus=:jobStatus AND modificationTime<:modificationTime AND prodSourceLabel IN (:pLabel1,:pLabel2) "
            if highPrio:
                varMap[':modificationTime'] = timeNow - datetime.timedelta(hours=1)
                sql += "AND currentPriority>=:currentPriority AND rownum<=%s " % rownum
            else:
                sql += "AND currentPriority<:currentPriority AND rownum<=%s " % rownum
                varMap[':modificationTime'] = timeNow - datetime.timedelta(hours=2)
            sql += "FOR UPDATE "
            # sql for lock
            sqlLock = 'UPDATE ATLAS_PANDA.jobsActive4 SET modificationTime=CURRENT_DATE WHERE PandaID=:PandaID'
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 1000
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            retList = []
            # lock
            for tmpID, in resList:
                varLock = {':PandaID':tmpID}
                self.cur.execute(sqlLock+comment,varLock)
                retList.append(tmpID)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # sort
            retList.sort()
            _logger.debug("lockJobsForFinisher : %s" % (len(retList)))
            return True,retList
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("lockJobsForFinisher : %s %s" % (errType,errValue))
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
                        errMsg = "no defined/activated jobs to reassign. running/finished/failed jobs are not reassigned by rebrokerage "
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
                sql  = "SELECT PandaID FROM ATLAS_PANDA.filesTable4 "
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
                            errMsg = "status of buildJob is '%s' != defined/activated/finished/cancelled so that jobs cannot be reassigned" \
                                     % buildJobStatus
            # get max/min PandaIDs using the libDS
            if errMsg == '':
                sql = "SELECT MAX(PandaID),MIN(PandaID) FROM ATLAS_PANDA.filesTable4 "
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
                sql += "FOR UPDATE "                
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
                    if timeLimit < tmpModificationTime and not forceOpt:
                        errMsg = "last mod time is %s > current-1hour. Cannot run (re)brokerage more than once in one hour" \
                                 % tmpModificationTime.strftime('%Y-%m-%d %H:%M:%S')
                    elif simulation:
                        pass
                    else:
                        # update modificationTime for locking
                        for tableName in tables:
                            sql = 'UPDATE %s ' % tableName
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


    # add stdout
    def addStdOut(self,pandaID,stdOut):
        comment = ' /* DBProxy.addStdOut */'        
        _logger.debug("addStdOut : %s start" % pandaID)
        sqlJ = "SELECT PandaID FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID FOR UPDATE "                
        sqlC = "SELECT PandaID FROM ATLAS_PANDA.jobsDebug WHERE PandaID=:PandaID "        
        sqlI = "INSERT INTO ATLAS_PANDA.jobsDebug (PandaID,stdOut) VALUES (:PandaID,:stdOut) "
        sqlU = "UPDATE ATLAS_PANDA.jobsDebug SET stdOut=:stdOut WHERE PandaID=:PandaID "
        try:
            # autocommit on
            self.conn.begin()
            # select
            varMap = {}
            varMap[':PandaID'] = pandaID
            self.cur.arraysize = 10
            # check job table
            self.cur.execute(sqlJ+comment, varMap)
            res = self.cur.fetchone()
            if res == None:
                _logger.debug("addStdOut : %s non active" % pandaID)
            else:
                # check debug table
                self.cur.execute(sqlC+comment, varMap)
                res = self.cur.fetchone()
                # already exist
                if res != None:
                    # update
                    sql = sqlU
                else:
                    # insert
                    sql = sqlI
                # write stdout    
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':stdOut']  = stdOut
                self.cur.execute(sql+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback()
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("addStdOut : %s %s" % (errtype,errvalue))
            return False


    # insert sandbox file info
    def insertSandboxFileInfo(self,userName,hostName,fileName,fileSize,checkSum):
        comment = ' /* DBProxy.insertSandboxFileInfo */'        
        _logger.debug("insertSandboxFileInfo : %s %s %s %s %s" % (userName,hostName,fileName,fileSize,checkSum))
        sqlC  = "SELECT userName,fileSize,checkSum FROM ATLAS_PANDAMETA.userCacheUsage "
        sqlC += "WHERE hostName=:hostName AND fileName=:fileName FOR UPDATE"
        sql  = "INSERT INTO ATLAS_PANDAMETA.userCacheUsage "
        sql += "(userName,hostName,fileName,fileSize,checkSum,creationTime,modificationTime) "
        sql += "VALUES (:userName,:hostName,:fileName,:fileSize,:checkSum,CURRENT_DATE,CURRENT_DATE) "
        try:
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[':hostName'] = hostName
            varMap[':fileName'] = fileName
            self.cur.arraysize = 10
            self.cur.execute(sqlC+comment, varMap)
            res = self.cur.fetchall()
            if len(res) != 0:
                _logger.debug("insertSandboxFileInfo : skip %s %s since already exists" % (hostName,fileName))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                return "WARNING: file exist"
            # insert
            varMap = {}
            varMap[':userName'] = userName
            varMap[':hostName'] = hostName
            varMap[':fileName'] = fileName
            varMap[':fileSize'] = fileSize
            varMap[':checkSum'] = checkSum
            self.cur.execute(sql+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return "OK"
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("insertSandboxFileInfo : %s %s" % (type,value))
            return "ERROR: DB failure"


    # check duplicated sandbox file
    def checkSandboxFile(self,dn,fileSize,checkSum):
        comment = ' /* DBProxy.checkSandboxFile */'        
        _logger.debug("checkSandboxFile : %s %s %s" % (dn,fileSize,checkSum))
        sqlC  = "SELECT hostName,fileName FROM ATLAS_PANDAMETA.userCacheUsage "
        sqlC += "WHERE userName=:userName AND fileSize=:fileSize AND checkSum=:checkSum "
        sqlC += "AND hostName<>:ngHostName AND creationTime>CURRENT_DATE-3 "
        sqlC += "AND creationTime>CURRENT_DATE-3 "
        try:
            retStr = 'NOTFOUND'
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            # begin transaction
            self.conn.begin()
            # check if it already exists
            varMap = {}
            varMap[':userName'] = compactDN
            varMap[':fileSize'] = fileSize
            varMap[':checkSum'] = checkSum
            varMap[':ngHostName'] = 'localhost.localdomain'
            self.cur.arraysize = 10
            self.cur.execute(sqlC+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if len(res) != 0:
                hostName,fileName = res[0]
                retStr = "FOUND:%s:%s" % (hostName,fileName)
            _logger.debug("checkSandboxFile -> %s" % retStr)
            return retStr
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("checkSandboxFile : %s %s" % (type,value))
            return "ERROR: DB failure"


    # insert dataset
    def insertDataset(self,dataset,tablename="ATLAS_PANDA.Datasets"):
        comment = ' /* DBProxy.insertDataset */'        
        methodName = comment.split(' ')[-2].split('.')[-1]
        tmpLog = LogWrapper(_logger,methodName+" <dataset={0}>".format(dataset.name))
        tmpLog.debug("start")
        sql0 = "SELECT COUNT(*) FROM %s WHERE vuid=:vuid " % tablename
        sql1 = "INSERT INTO %s " % tablename
        sql1+= "(%s) " % DatasetSpec.columnNames()
        sql1+= DatasetSpec.bindValuesExpression()
        sql2 = "SELECT name FROM %s WHERE vuid=:vuid " % tablename
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
            tmpLog.debug("nDS=%s with %s" % (nDS,dataset.vuid))
            if nDS == 0:
                # insert
                tmpLog.debug(sql1+comment+str(dataset.valuesMap()))
                self.cur.execute(sql1+comment, dataset.valuesMap())
                # check name in DB
                varMap = {}
                varMap[':vuid'] = dataset.vuid
                self.cur.execute(sql2+comment, varMap)
                nameInDB, = self.cur.fetchone()
                tmpLog.debug("inDB -> %s %s" % (nameInDB,dataset.name==nameInDB))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
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
                _logger.debug(sql1+comment+str(varMap))
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
            if self.backend == 'oracle':
                sql = "SELECT ATLAS_PANDA.SUBCOUNTER_SUBID_SEQ.nextval FROM dual";
                self.cur.arraysize = 100
                self.cur.execute(sql+comment, {})
                sn, = self.cur.fetchone()
            else:
                # panda_config.backend == 'mysql'
                ### fake sequence
                sql = " INSERT INTO ATLAS_PANDA.SUBCOUNTER_SUBID_SEQ (col) VALUES (NULL) "
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                sql2 = """ SELECT LAST_INSERT_ID() """
                self.cur.execute(sql2 + comment, {})
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
            if self.backend == 'oracle':
                sql = "SELECT ATLAS_PANDA.GROUP_JOBID_SEQ.nextval FROM dual";
                self.cur.execute(sql+comment, {})
                sn, = self.cur.fetchone()
            else:
                # panda_config.backend == 'mysql'
                ### fake sequence
                sql = " INSERT INTO ATLAS_PANDA.GROUP_JOBID_SEQ (col) VALUES (NULL) "
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                sql2 = """ SELECT LAST_INSERT_ID() """
                self.cur.execute(sql2 + comment, {})
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


    # change job priorities
    def changeJobPriorities(self,newPrioMap):
        comment = ' /* DBProxy.changeJobPriorities */'
        try:
            _logger.debug("changeJobPriorities start")
            sql  = "UPDATE %s SET currentPriority=:currentPriority,assignedPriority=:assignedPriority "
            sql += "WHERE PandaID=:PandaID"
            # loop over all PandaIDs
            for pandaID,newPrio in newPrioMap.iteritems():
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':currentPriority']  = newPrio
                varMap[':assignedPriority'] = newPrio
                _logger.debug("changeJobPriorities PandaID=%s -> prio=%s" % (pandaID,newPrio))
                # start transaction
                self.conn.begin()
                # try active tables
                retU = None
                for tableName in ['ATLAS_PANDA.jobsActive4','ATLAS_PANDA.jobsDefined4','ATLAS_PANDA.jobsWaiting4']:
                    # execute
                    self.cur.execute((sql % tableName)+comment,varMap)
                    retU = self.cur.rowcount
                    if retU > 0:
                        break
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                _logger.debug("changeJobPriorities PandaID=%s retU=%s" % (pandaID,retU))
            # return
            _logger.debug("changeJobPriorities done")
            return True,''
        except:
            # roll back
            self._rollback()
            # error
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("changeJobPriorities : %s %s" % (errtype,errvalue))
            return False,'database error'


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
            varMap[':newID']  = self.cur.var(varNUMBER)                       
            self.cur.execute(sql+comment, varMap)
            # get id
            cloudTask.id = long(self.cur.getvalue(varMap[':newID']))
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


    # reset modification time of a task to shorten retry interval
    def resetTmodCloudTask(self,tid):
        comment = ' /* resetTmodCloudTask */'        
        try:
            _logger.debug("resetTmodCloudTask %s" % tid)
            # check tid
            if tid in [None,'NULL']:
                _logger.error("invalid TID : %s" % tid)
                return None
            # start transaction
            self.conn.begin()
            # update
            sql  = "UPDATE ATLAS_PANDA.cloudtasks SET tmod=:tmod WHERE taskid=:taskid"
            varMap = {}
            varMap[':taskid'] = tid
            varMap[':tmod'] = datetime.datetime.utcnow() - datetime.timedelta(minutes=165)
            self.cur.execute(sql+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback()
            # error
            type, value, traceBack = sys.exc_info()
            _logger.error("resetTmodCloudTask : %s %s" % (type,value))
            return False

        
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
    def setCloudTaskByUser(self,user,tid,cloud,status,forceUpdate=False):
        comment = ' /* setCloudTaskByUser */'        
        try:
            _logger.debug("setCloudTaskByUser(tid=%s,cloud=%s,status=%s) by %s" % (tid,cloud,status,user))
            # check tid
            if tid in [None,'NULL']:
                tmpMsg = "invalid TID : %s" % tid
                _logger.error(tmpMsg)
                return "ERROR: " + tmpMsg
            # check status
            statusList = ['tobeaborted','assigned']
            if not status in statusList:
                tmpMsg = "invalid status=%s. Must be one of %s" % (status,str(statusList))
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
                sql  = "UPDATE ATLAS_PANDA.cloudtasks SET status=:status,tmod=CURRENT_DATE"
                if forceUpdate:
                    sql += ",cloud=:cloud"
                sql += " WHERE taskid=:taskid"
                varMap = {}
                varMap[':taskid'] = tid
                varMap[':status'] = status
                if forceUpdate:
                    varMap[':cloud'] = cloud
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
    def countPendingFiles(self,pandaID,forInput=True):
        comment = ' /* DBProxy.countPendingFiles */'        
        varMap = {}
        varMap[':pandaID'] = pandaID
        varMap[':status'] = 'ready'
        if forInput:
            sql1 = "SELECT COUNT(*) FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:pandaID AND type=:type AND status<>:status "
            varMap[':type'] = 'input'
        else:
            sql1 = "SELECT COUNT(*) FROM ATLAS_PANDA.filesTable4 WHERE PandaID=:pandaID AND type IN (:type1,:type2) AND status<>:status "
            varMap[':type1'] = 'output'
            varMap[':type2'] = 'log'            
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
            _logger.debug("countPendingFiles : %s -> %s" % (pandaID,nFiles))
            return nFiles
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("countPendingFiles : %s : %s %s" % (pandaID,errType,errValue))
            return -1


    # get datasets associated with file
    def getDatasetWithFile(self,lfn,jobPrioity=0):
        comment = ' /* DBProxy.getDatasetWithFile */'        
        varMap = {}
        varMap[':lfn'] = lfn
        varMap[':status1'] = 'pending'
        varMap[':status2'] = 'transferring'        
        sql1  = "SELECT PandaID,status,destinationDBlock,destinationDBlockToken,dispatchDBlock FROM ATLAS_PANDA.filesTable4 "
        sql1 += "WHERE lfn=:lfn AND status IN (:status1,:status2) AND modificationTime<CURRENT_DATE-60 "
        try:
            # start transaction
            self.conn.begin()
            retMap = {}
            # select
            _logger.debug("getDatasetWithFile : %s start" % lfn)
            self.cur.arraysize = 1000                
            retS = self.cur.execute(sql1+comment, varMap)
            res = self.cur.fetchall()
            if res != None and len(res) != 0:
                for pandaID,status,destinationDBlock,destinationDBlockToken,dispatchDBlock in res:
                    varMap = {}
                    varMap[':PandaID'] = pandaID 
                    if status == 'pending':
                        # input
                        sqlP = 'SELECT computingSite,prodSourceLabel FROM ATLAS_PANDA.jobsDefined4 ' 
                        varMap[':jobStatus'] = 'assigned'
                        dsName  = dispatchDBlock
                        dsToken = ''
                    else:
                        # output
                        sqlP = 'SELECT destinationSE,prodSourceLabel FROM ATLAS_PANDA.jobsActive4 '
                        varMap[':jobStatus'] = 'transferring'
                        dsName  = destinationDBlock
                        dsToken = destinationDBlockToken
                    # check duplication
                    if retMap.has_key(dsName):
                        continue
                    # get site info    
                    sqlP += 'WHERE PandaID=:PandaID AND jobStatus=:jobStatus AND currentPriority>=:currentPriority '
                    varMap[':currentPriority'] = jobPrioity
                    self.cur.execute(sqlP+comment, varMap)
                    resP = self.cur.fetchone()
                    # append
                    if resP != None and resP[1] in ['managed','test']:
                        retMap[dsName] = (resP[0],dsToken)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("getDatasetWithFile : %s -> %s" % (lfn,str(retMap)))
            return retMap
        except:
            # roll back
            self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("getDatasetWithFile : %s : %s %s" % (lfn,errType,errValue))
            return {}


    # get input files currently in use for analysis
    def getFilesInUseForAnal(self,outDataset):
        comment = ' /* DBProxy.getFilesInUseForAnal */'        
        sqlSub  = "SELECT destinationDBlock,PandaID FROM ATLAS_PANDA.filesTable4 "
        sqlSub += "WHERE dataset=:dataset AND type IN (:type1,:type2) GROUP BY destinationDBlock,PandaID"
        sqlPaA  = "SELECT jobDefinitionID,prodUserName FROM ATLAS_PANDA.jobsDefined4 "
        sqlPaA += "WHERE PandaID=:PandaID "
        sqlPaA += "UNION "
        sqlPaA += "SELECT jobDefinitionID,prodUserName FROM ATLAS_PANDA.jobsActive4 "
        sqlPaA += "WHERE PandaID=:PandaID "
        sqlPan  = "SELECT jobDefinitionID,prodUserName FROM ATLAS_PANDA.jobsArchived4 "
        sqlPan += "WHERE PandaID=:PandaID AND modificationTime<=CURRENT_DATE "
        sqlPan += "UNION "
        sqlPan += "SELECT jobDefinitionID,prodUserName FROM ATLAS_PANDAARCH.jobsArchived "
        sqlPan += "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30)"
        sqlIdA  = "SELECT PandaID,jobStatus FROM ATLAS_PANDA.jobsArchived4 "
        sqlIdA += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
        sqlIdA += "AND prodSourceLabel=:prodSourceLabel1 "
        sqlIdL  = "SELECT /*+ NO_INDEX(tab JOBS_MODTIME_IDX) INDEX_COMBINE(tab JOBS_PRODUSERNAME_IDX JOBS_JOBDEFID_IDX) */ "
        sqlIdL += "PandaID,jobStatus FROM ATLAS_PANDAARCH.jobsArchived tab "
        sqlIdL += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID "
        sqlIdL += "AND prodSourceLabel=:prodSourceLabel1 AND modificationTime>(CURRENT_DATE-30) "
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
                    # avoid redundunt lookup
                    if checkedPandaIDs.has_key(pandaID):
                        continue
                    if subDSpandaIDmap.has_key(subDataset):
                        # append jobs as running since they are not in archived tables
                        if not pandaID in subDSpandaIDmap[subDataset]:
                            checkedPandaIDs[pandaID] = 'running'
                            subDSpandaIDmap[subDataset].append(pandaID)
                        continue
                    # look for jobdefID and userName
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    _logger.debug("getFilesInUseForAnal : %s %s" % (sqlPaA,str(varMap)))                    
                    retP = self.cur.execute(sqlPaA+comment, varMap)
                    resP = self.cur.fetchall()
                    if len(resP) != 0:
                        jobDefinitionID,prodUserName = resP[0]
                    else:
                        _logger.debug("getFilesInUseForAnal : %s %s" % (sqlPan,str(varMap)))
                        retP = self.cur.execute(sqlPan+comment, varMap)
                        resP = self.cur.fetchall()
                        if len(resP) != 0:
                            jobDefinitionID,prodUserName = resP[0]
                        else:
                            continue
                    # get PandaIDs with obdefID and userName
                    tmpPandaIDs = [] 
                    varMap = {}
                    varMap[':prodUserName']     = prodUserName
                    varMap[':jobDefinitionID']  = jobDefinitionID
                    varMap[':prodSourceLabel1'] = 'user'
                    _logger.debug("getFilesInUseForAnal : %s %s" % (sqlIdA,str(varMap)))                    
                    retID = self.cur.execute(sqlIdA+comment, varMap)
                    resID = self.cur.fetchall()
                    for tmpPandaID,tmpJobStatus in resID:
                        checkedPandaIDs[tmpPandaID] = tmpJobStatus
                        tmpPandaIDs.append(tmpPandaID)
                    _logger.debug("getFilesInUseForAnal : %s %s" % (sqlIdL,str(varMap)))
                    retID = self.cur.execute(sqlIdL+comment, varMap)
                    resID = self.cur.fetchall()
                    for tmpPandaID,tmpJobStatus in resID:
                        if not tmpPandaID in tmpPandaIDs:
                            checkedPandaIDs[tmpPandaID] = tmpJobStatus
                            tmpPandaIDs.append(tmpPandaID)
                    # append
                    if not subDSpandaIDmap.has_key(subDataset):
                        subDSpandaIDmap[subDataset] = []
                    for tmpPandaID in tmpPandaIDs:
                        # reuse failed files if jobs are in Archived since they cannot change back to active
                        if checkedPandaIDs[tmpPandaID] in ['failed','cancelled']:
                            continue
                        # collect PandaIDs
                        subDSpandaIDmap[subDataset].append(tmpPandaID)
                # loop over all sub datasets
                for subDataset,activePandaIDs in subDSpandaIDmap.iteritems():
                    # skip empty
                    if activePandaIDs == []:
                        continue
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
        sqlSub  = "SELECT destinationDBlock,PandaID,status FROM ATLAS_PANDA.filesTable4 "
        sqlSub += "WHERE dataset=:dataset AND type=:type1 GROUP BY destinationDBlock,PandaID,status"
        sqlPaA  = "SELECT jobStatus FROM ATLAS_PANDA.jobsDefined4 "
        sqlPaA += "WHERE PandaID=:PandaID "
        sqlPaA += "UNION "
        sqlPaA += "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 "
        sqlPaA += "WHERE PandaID=:PandaID "
        sqlPan  = "SELECT jobStatus FROM ATLAS_PANDA.jobsArchived4 "
        sqlPan += "WHERE PandaID=:PandaID AND modificationTime<=CURRENT_DATE "
        sqlPan += "UNION "
        sqlPan += "SELECT jobStatus FROM ATLAS_PANDAARCH.jobsArchived "
        sqlPan += "WHERE PandaID=:PandaID AND modificationTime>(CURRENT_DATE-30)"
        sqlDis  = "SELECT distinct dispatchDBlock FROM ATLAS_PANDA.filesTable4 "
        sqlDis += "WHERE PandaID=:PandaID AND type=:type AND dispatchDBlock IS NOT NULL AND modificationTime <= CURRENT_DATE"
        inputDisList = []
        try:
            timeStart = datetime.datetime.utcnow()
            _logger.debug("getDisInUseForAnal start for %s" % outDataset)
            # start transaction
            self.conn.begin()
            # get sub datasets
            varMap = {}
            varMap[':dataset'] = outDataset
            varMap[':type1'] = 'log'
            _logger.debug("getDisInUseForAnal : %s %s" % (sqlSub,str(varMap)))
            self.cur.arraysize = 100000
            retS = self.cur.execute(sqlSub+comment, varMap)
            res = self.cur.fetchall()
            subDSpandaIDmap = {}
            checkedPandaIDs = {}
            for subDataset,pandaID,fileStatus in res:
                # add map
                if not subDSpandaIDmap.has_key(subDataset):
                    subDSpandaIDmap[subDataset] = []
                # check job status
                if fileStatus != 'ready':
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    _logger.debug("getDisInUseForAnal : %s %s" % (sqlPaA,str(varMap)))                    
                    retP = self.cur.execute(sqlPaA+comment, varMap)
                    resP = self.cur.fetchall()
                    if len(resP) != 0:
                        # append jobs as running since they are not in archived tables yet
                        checkedPandaIDs[pandaID] = 'running'
                        subDSpandaIDmap[subDataset].append(pandaID)
                    else:
                        _logger.debug("getDisInUseForAnal : %s %s" % (sqlPan,str(varMap)))
                        retP = self.cur.execute(sqlPan+comment, varMap)
                        resP = self.cur.fetchall()
                        if len(resP) != 0:
                            checkedPandaIDs[pandaID], = resP[0]
                            # reuse failed files if jobs are in Archived since they cannot change back to active
                            if checkedPandaIDs[pandaID] in ['failed','cancelled']:
                                continue
                            # collect PandaIDs
                            subDSpandaIDmap[subDataset].append(pandaID)
                        else:
                            # not found
                            continue
                else:
                    # no job lookup since file was sucessfully finished
                    checkedPandaIDs[pandaID] = 'finished'
                    # collect PandaIDs
                    subDSpandaIDmap[subDataset].append(pandaID)
            # loop over all sub datasets
            for subDataset,activePandaIDs in subDSpandaIDmap.iteritems():
                # skip empty
                if activePandaIDs == []:
                    continue
                resDisList = []                    
                # get dispatchDBlocks
                pandaID = activePandaIDs[0]
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':type'] = 'input'                        
                _logger.debug("getDisInUseForAnal : %s %s" % (sqlDis,str(varMap)))
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
                if resDisList != []:
                    inputDisList.append((resDisList,activePandaIDs))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            timeDelta = datetime.datetime.utcnow()-timeStart
            _logger.debug("getDisInUseForAnal end for %s len=%s time=%ssec" % (outDataset,len(inputDisList),timeDelta.seconds))
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
            token = datetime.datetime.utcnow().isoformat('/')
            # loop over all shadow dis datasets
            pandaIdLfnMap = {}
            for disDatasetList,activePandaIDs in inputDisList:
                for disDataset in disDatasetList:
                    # use new style only
                    if not disDataset.startswith('user_disp.'):
                        continue
                    # read LFNs and PandaIDs
                    if not pandaIdLfnMap.has_key(disDataset):
                        # start transaction
                        self.conn.begin()
                        varMap = {}
                        varMap[':dispatchDBlock'] = disDataset
                        varMap[':type'] = 'input'
                        varMap[':noshadow'] = 'noshadow'
                        _logger.debug("getLFNsInUseForAnal : <%s> %s %s" % (token,sqlLfn,str(varMap)))
                        timeStart = datetime.datetime.utcnow()
                        self.cur.arraysize = 100000
                        retL = self.cur.execute(sqlLfn+comment, varMap)
                        resL = self.cur.fetchall()
                        # commit
                        timeDelta = datetime.datetime.utcnow()-timeStart
                        _logger.debug("getLFNsInUseForAnal : <%s> %s time=%ssec commit" % (token,disDataset,timeDelta.seconds))
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        # make map
                        pandaIdLfnMap[disDataset] = {}
                        for lfn,filePandaID in resL:
                            if not pandaIdLfnMap[disDataset].has_key(filePandaID):
                                pandaIdLfnMap[disDataset][filePandaID] = []
                            pandaIdLfnMap[disDataset][filePandaID].append(lfn)
                        _logger.debug("getLFNsInUseForAnal : <%s> %s map made with len=%s" % \
                                          (token,disDataset,len(resL)))
                # append
                for disDataset in disDatasetList:    
                    _logger.debug("getLFNsInUseForAnal : <%s> %s list making pandaIDs=%s fileLen=%s" % \
                                      (token,disDataset,len(activePandaIDs),len(inputFilesList)))
                    for activePandaID in activePandaIDs:
                        # skip files used by archived failed or cancelled jobs
                        if pandaIdLfnMap[disDataset].has_key(activePandaID):
                            inputFilesList += pandaIdLfnMap[disDataset][activePandaID]
                    _logger.debug("getLFNsInUseForAnal : <%s> %s done" % (token,disDataset))
            _logger.debug("getLFNsInUseForAnal : <%s> %s" % (token,len(inputFilesList)))
            return inputFilesList
        except:
            # roll back
            self._rollback()
            errtype,errvalue = sys.exc_info()[:2]
            _logger.error("getLFNsInUseForAnal(%s) : %s %s" % (str(inputDisList),errtype,errvalue))
            return None


    # update input files and return corresponding PandaIDs
    def updateInFilesReturnPandaIDs(self,dataset,status,fileLFN=''):
        comment = ' /* DBProxy.updateInFilesReturnPandaIDs */'                                
        _logger.debug("updateInFilesReturnPandaIDs(%s,%s)" % (dataset,fileLFN))
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DISPDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status=:status WHERE status<>:status AND dispatchDBlock=:dispatchDBlock"
        varMap = {}
        varMap[':status'] = status
        varMap[':dispatchDBlock'] = dataset
        if fileLFN != '':
            sql0 += " AND lfn=:lfn"
            sql1 += " AND lfn=:lfn"
            varMap[':lfn'] = fileLFN
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
    def updateOutFilesReturnPandaIDs(self,dataset,fileLFN=''):
        comment = ' /* DBProxy.updateOutFilesReturnPandaIDs */'                        
        _logger.debug("updateOutFilesReturnPandaIDs(%s,%s)" % (dataset,fileLFN))
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ row_ID,PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock AND status=:status"
        sql1 = "UPDATE /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ ATLAS_PANDA.filesTable4 tab SET status='ready' WHERE destinationDBlock=:destinationDBlock AND status=:status"
        varMap = {}
        varMap[':status'] = 'transferring'
        varMap[':destinationDBlock'] = dataset
        if fileLFN != '':
            sql0 += " AND lfn=:lfn"
            sql1 += " AND lfn=:lfn"
            varMap[':lfn'] = fileLFN
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
        sql0 = "UPDATE ATLAS_PANDA.filesTable4 SET GUID=:GUID,fsize=:fsize,checksum=:checksum,scope=:scope WHERE lfn=:lfn"
        for iTry in range(self.nTry):
            try:
                # start transaction
                self.conn.begin()
                self.cur.arraysize = 1000000
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
                    if not file.has_key('scope') or file['scope'] in ['','NULL']:
                        varMap[':scope'] = None
                    else:
                        varMap[':scope'] = file['scope']
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
        sql1 = "SELECT lfn,PandaID FROM ATLAS_PANDA.filesTable4 WHERE dataset=:dataset AND type=:type ORDER BY lfn DESC"
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
                        continue
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
        sql0  = "SELECT computingSite,jobStatus,COUNT(*) FROM %s "
        # processingType
        tmpJobTypeMap = {}
        sqlJobType = ''
        useWhereInSQL = True
        if forAnal == None or jobType != "":
            useWhereInSQL = False
        elif forAnal == True:
            tmpJobTypeMap[':prodSourceLabel1'] = 'user'
            tmpJobTypeMap[':prodSourceLabel2'] = 'panda'
            sql0 += "WHERE prodSourceLabel IN ("
            sqlJobType = ":prodSourceLabel1,:prodSourceLabel2) "
        else:
            tmpJobTypeMap[':prodSourceLabel1'] = 'managed'
            sql0 += "WHERE prodSourceLabel IN ("            
            sqlJobType = ":prodSourceLabel1) "
        sql0 += sqlJobType
        # predefined    
        if predefined:
            if useWhereInSQL:
                sql0 += "AND relocationFlag=1 "
            else:
                sql0 += "WHERE relocationFlag=1 "
                useWhereInSQL = True
        # working group
        tmpGroupMap = {}
        sqlGroups = ''
        if workingGroup != '':
            if useWhereInSQL:
                sqlGroups += "AND workingGroup IN ("
            else:
                sqlGroups += "WHERE workingGroup IN ("
                useWhereInSQL = True                
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
            if useWhereInSQL:
                sqlGroups += "AND countryGroup IN ("
            else:
                sqlGroups += "WHERE countryGroup IN ("
                useWhereInSQL = True
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
            if useWhereInSQL:
                sqlPrio = "AND currentPriority>=:minPriority "
            else:
                sqlPrio = "WHERE currentPriority>=:minPriority "
                useWhereInSQL = True
            tmpPrioMap[':minPriority'] = minPriority
        sql0 += sqlPrio    
        sql0 += "GROUP BY computingSite,jobStatus"
        sqlA =  "SELECT /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ computingSite,jobStatus,COUNT(*) FROM ATLAS_PANDA.jobsArchived4 tab WHERE modificationTime>:modificationTime "
        if sqlJobType != "":
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
                    for computingSite,jobStatus,nJobs in res:
                        # FIXME
                        # ignore some job status since they break APF
                        if jobStatus in ['merging']:
                            continue
                        if not ret.has_key(computingSite):
                            ret[computingSite] = {}
                        if not ret[computingSite].has_key(jobStatus):
                            ret[computingSite][jobStatus] = 0
                        ret[computingSite][jobStatus] += nJobs
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
                    # FIXME
                    # ignore some job status since they break APF
                    if jobStatus in ['merging']:
                        continue
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
    def getJobStatisticsBrokerage(self,minPriority=None,maxPriority=None):
        comment = ' /* DBProxy.getJobStatisticsBrokerage */'        
        _logger.debug("getJobStatisticsBrokerage(min=%s max=%s)" % (minPriority,maxPriority))
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
        if minPriority != None or maxPriority != None:
            # read the number of running jobs with prio<=MIN
            tables.append('ATLAS_PANDA.jobsActive4')
            sqlMVforRun = re.sub('currentPriority>=','currentPriority<=',sqlMV)
        ret = {}
        nTry=3
        iActive = 0
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
                    useRunning = None
                    if table == 'ATLAS_PANDA.jobsActive4':
                        # first count non-running and then running if minPriority is specified
                        if minPriority != None:
                            if iActive == 0:
                                useRunning = False
                            else:
                                useRunning = True
                            iActive += 1
                        if useRunning in [None,False]:
                            self.cur.execute((sqlMV+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS', varMap)
                        else:
                            # use maxPriority to avoid underestimation of running jobs
                            if minPriority != None and maxPriority != None:
                                varMap[':minPriority'] = maxPriority
                            self.cur.execute((sqlMVforRun+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS', varMap)
                    else:
                        self.cur.execute((sql0+comment) % table, varMap)
                    res = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    # create map
                    for cloud,computingSite,jobStatus,processingType,count in res:
                        # check jobstatus if minPriority isspecified
                        if minPriority != None:
                            # count the number of non-running with prio>=MIN
                            if useRunning == True and jobStatus != 'running':
                                continue
                            # count the number of running with prio<=MIN
                            if  useRunning == False and jobStatus == 'running':
                                continue
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
                        for stateItem in ['defined','assigned','activated','running']:
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


    # get highest prio jobs per process group
    def getHighestPrioJobStatPerPG(self,useMorePG=False):
        comment = ' /* DBProxy.getHighestPrioJobStatPerPG */'        
        _logger.debug("getHighestPrioJobStatPerPG()")
        if useMorePG == False:
            sql0  = "SELECT cloud,max(currentPriority),processingType FROM %s WHERE "
            sql0 += "prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) GROUP BY cloud,processingType"
            sqlC  = "SELECT COUNT(*) FROM %s WHERE "
            sqlC += "prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) AND "
            sqlC += "cloud=:cloud AND currentPriority=:currentPriority AND processingType=:processingType"
        else:
            sql0  = "SELECT cloud,max(currentPriority),processingType,coreCount,workingGroup FROM %s WHERE "
            sql0 += "prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) "
            sql0 += "GROUP BY cloud,processingType,coreCount,workingGroup"
            sqlC  = "SELECT COUNT(*) FROM %s WHERE "
            sqlC += "prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) AND "
            sqlC += "cloud=:cloud AND currentPriority=:currentPriority AND processingType=:processingType AND "
            sqlC += "coreCount=:coreCount AND workingGroup=:workingGroup"
            sqlCN  = "SELECT COUNT(*) FROM %s WHERE "
            sqlCN += "prodSourceLabel=:prodSourceLabel AND jobStatus IN (:jobStatus1,:jobStatus2) AND "
            sqlCN += "cloud=:cloud AND currentPriority=:currentPriority AND processingType=:processingType AND "
            sqlCN += "coreCount IS NULL AND workingGroup=:workingGroup"
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
                _logger.debug((sql0+comment) % table+str(varMap))
                self.cur.execute((sql0+comment) % table, varMap)
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # create map
                for tmpItem in res:
                    if useMorePG == False:
                        cloud,maxPriority,processingType = tmpItem
                        origCloud = cloud
                        origProcessingType = processingType
                    else:
                        origCloud,maxPriority,origProcessingType,coreCount,workingGroup = tmpItem
                        # convert cloud and processingType for extended process group
                        if useMorePG == ProcessGroups.extensionLevel_1:
                            # extension level 1
                            cloud,processingType = ProcessGroups.converCPTforEPG(origCloud,origProcessingType,
                                                                                 coreCount)
                        else:
                            # extension level 2
                            cloud,processingType = ProcessGroups.converCPTforEPG(origCloud,origProcessingType,
                                                                                 coreCount,workingGroup)
                    # add cloud
                    if not ret.has_key(cloud):
                        ret[cloud] = {}
                    # get process group
                    processGroup = ProcessGroups.getProcessGroup(processingType)
                    # add process group
                    if not ret[cloud].has_key(processGroup):
                        ret[cloud][processGroup] = {}
                    # add max priority
                    prioKey = 'highestPrio'
                    nNotRunKey = 'nNotRun'
                    getNumber = False
                    if not ret[cloud][processGroup].has_key(prioKey):
                        ret[cloud][processGroup][prioKey] = maxPriority
                        ret[cloud][processGroup][nNotRunKey] = 0
                        getNumber = True
                    else:
                        # use highest one
                        if ret[cloud][processGroup][prioKey] < maxPriority:
                            ret[cloud][processGroup][prioKey] = maxPriority
                            # reset
                            ret[cloud][processGroup][nNotRunKey] = 0
                            getNumber = True
                        elif ret[cloud][processGroup][prioKey] == maxPriority:
                            getNumber = True
                    # get number of jobs with highest prio
                    if getNumber:
                        varMap[':cloud'] = origCloud
                        varMap[':currentPriority'] = maxPriority
                        varMap[':processingType'] = origProcessingType
                        if useMorePG != False:
                            varMap[':workingGroup'] = workingGroup
                            if coreCount != None:
                                varMap[':coreCount'] = coreCount
                        self.cur.arraysize = 10
                        _logger.debug((sqlC+comment) % table+str(varMap))
                        self.cur.execute((sqlC+comment) % table, varMap)
                        resC = self.cur.fetchone()
                        # commit
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                        ret[cloud][processGroup][nNotRunKey] += resC[0]
            # return
            _logger.debug("getHighestPrioJobStatPerPG -> %s" % ret)
            return ret
        except:
            # roll back
            self._rollback()
            type, value, traceBack = sys.exc_info()
            _logger.error("getHighestPrioJobStatPerPG : %s %s" % (type, value))
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
    def getDestSE(self,dsname,fromArch=False):
        comment = ' /* DBProxy.getDestSE */'        
        _logger.debug("getDestSE(%s,%s)" % (dsname,fromArch))
        sql0 = "SELECT /*+ index(tab FILESTABLE4_DESTDBLOCK_IDX) */ PandaID FROM ATLAS_PANDA.filesTable4 tab WHERE destinationDBlock=:destinationDBlock "
        if not fromArch:
            sql0 += "AND status=:status "
        sql0 += "AND rownum=1"
        sql1 = "SELECT computingSite,destinationSE FROM %s WHERE PandaID=:PandaID"
        actTableList = ['ATLAS_PANDA.jobsActive4']
        if fromArch:
            actTableList.append("ATLAS_PANDA.jobsArchived4")
        try:
            # start transaction
            self.conn.begin()
            # select
            varMap = {}
            if not fromArch:
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
                # loop over all active tables
                foundInActive = False
                for actTable in actTableList:
                    self.cur.execute((sql1 % actTable)+comment, varMap)
                    res = self.cur.fetchall()
                    if len(res) != 0:
                        destSE = res[0]
                        foundInActive = True
                        break
                # look into ARCH table
                if not foundInActive:
                    if fromArch:
                        sqlA  = "SELECT computingSite,destinationSE FROM ATLAS_PANDAARCH.jobsArchived WHERE PandaID=:PandaID "
                        sqlA += "AND modificationTime>(CURRENT_DATE-30) "
                        self.cur.execute(sqlA+comment, varMap)
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
            sqlA = "SELECT /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ jobStatus,COUNT(*),cloud FROM %s tab WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
        else:
            sql0 = "SELECT jobStatus,COUNT(*),cloud FROM %s WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) GROUP BY jobStatus,cloud"
            sqlA = "SELECT /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ jobStatus,COUNT(*),cloud FROM %s tab WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
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
    def getJobStatisticsPerProcessingType(self,useMorePG=False):
        comment = ' /* DBProxy.getJobStatisticsPerProcessingType */'                
        timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=12)
        _logger.debug("getJobStatisticsPerProcessingType()")
        if useMorePG == False:
            sqlN  = "SELECT jobStatus,COUNT(*),cloud,processingType FROM %s "
            sqlN += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) GROUP BY jobStatus,cloud,processingType"
            sqlA  = "SELECT /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ jobStatus,COUNT(*),cloud,processingType FROM %s tab "
            sqlA += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND modificationTime>:modificationTime GROUP BY jobStatus,cloud,processingType"
        else:
            sqlN  = "SELECT jobStatus,COUNT(*),cloud,processingType,coreCount,workingGroup FROM %s "
            sqlN += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) "
            sqlN += "GROUP BY jobStatus,cloud,processingType,coreCount,workingGroup"
            sqlA  = "SELECT /*+ INDEX_RS_ASC(tab (MODIFICATIONTIME PRODSOURCELABEL)) */ "
            sqlA += "jobStatus,COUNT(*),cloud,processingType,coreCount,workingGroup FROM %s tab "
            sqlA += "WHERE prodSourceLabel IN (:prodSourceLabel1,:prodSourceLabel2) AND modificationTime>:modificationTime "
            sqlA += "GROUP BY jobStatus,cloud,processingType,coreCount,workingGroup"
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
                    if table == 'ATLAS_PANDA.jobsActive4' and useMorePG == False:
                        self.cur.execute((sqlMV+comment) % 'ATLAS_PANDA.MV_JOBSACTIVE4_STATS', varMap)
                    else:
                        # use real table since coreCount is unavailable in MatView
                        self.cur.execute((sqlN+comment) % table, varMap)                        
                res = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # create map
                for tmpItem in res:
                    if useMorePG == False:
                        jobStatus,count,cloud,processingType = tmpItem
                    else:
                        jobStatus,count,cloud,processingType,coreCount,workingGroup = tmpItem
                        # convert cloud and processingType for extended process group
                        if useMorePG == ProcessGroups.extensionLevel_1:
                            # extension level 1
                            cloud,processingType = ProcessGroups.converCPTforEPG(cloud,processingType,
                                                                                 coreCount)
                        else:
                            # extension level 2
                            cloud,processingType = ProcessGroups.converCPTforEPG(cloud,processingType,
                                                                                 coreCount,workingGroup)
                            
                    # add cloud
                    if not ret.has_key(cloud):
                        ret[cloud] = {}
                    # add processingType
                    if not ret[cloud].has_key(processingType):
                        ret[cloud][processingType] = {}
                    # add status
                    if not ret[cloud][processingType].has_key(jobStatus):
                        ret[cloud][processingType][jobStatus] = 0
                    ret[cloud][processingType][jobStatus] += count
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
    def getNUserJobs(self,siteName):
        comment = ' /* DBProxy.getNUserJobs */'        
        _logger.debug("getNUserJobs(%s)" % siteName)
        sql0  = "SELECT prodUserID,count(*) FROM ATLAS_PANDA.jobsActive4 "
        sql0 += "WHERE jobStatus=:jobStatus AND prodSourceLabel in (:prodSourceLabel1,:prodSourceLabel2) "
        sql0 += "AND computingSite=:computingSite GROUP BY prodUserID "
        varMap = {}
        varMap[':computingSite'] = siteName
        varMap[':jobStatus'] = 'activated'
        varMap[':prodSourceLabel1'] = 'user'
        varMap[':prodSourceLabel2'] = 'panda'
        ret = {}
        try:
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10000            
            _logger.debug(1)
            self.cur.execute(sql0+comment, varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # create map
            for prodUserID,nJobs in res:
                ret[prodUserID] = nJobs
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
        sqlRst =  "UPDATE ATLAS_PANDAMETA.SiteData SET GETJOB=:GETJOB,UPDATEJOB=:UPDATEJOB WHERE HOURS=:HOURS AND LASTMOD<:LASTMOD"
        sqlCh  =  "SELECT count(*) FROM ATLAS_PANDAMETA.SiteData WHERE FLAG=:FLAG AND HOURS=:HOURS AND SITE=:SITE"
        sqlIn  =  "INSERT INTO ATLAS_PANDAMETA.SiteData (SITE,FLAG,HOURS,GETJOB,UPDATEJOB,LASTMOD,"
        sqlIn  += "NSTART,FINISHED,FAILED,DEFINED,ASSIGNED,WAITING,ACTIVATED,HOLDING,RUNNING,TRANSFERRING) "
        sqlIn  += "VALUES (:SITE,:FLAG,:HOURS,:GETJOB,:UPDATEJOB,CURRENT_DATE,"
        sqlIn  += "0,0,0,0,0,0,0,0,0,0)"
        sqlUp  =  "UPDATE ATLAS_PANDAMETA.SiteData SET GETJOB=:GETJOB,UPDATEJOB=:UPDATEJOB,LASTMOD=CURRENT_DATE "
        sqlUp  += "WHERE FLAG=:FLAG AND HOURS=:HOURS AND SITE=:SITE"
        sqlAll  = "SELECT getJob,updateJob,FLAG FROM ATLAS_PANDAMETA.SiteData WHERE HOURS=:HOURS AND SITE=:SITE"
        try:
            self.conn.begin()
            # delete old records
            varMap = {}
            varMap[':HOURS'] = 48
            varMap[':LASTMOD'] = datetime.datetime.utcnow()-datetime.timedelta(hours=varMap[':HOURS'])
            self.cur.execute(sqlDel+comment,varMap)
            # set 0 to old records
            varMap = {}
            varMap[':HOURS'] = 3
            varMap[':GETJOB'] = 0
            varMap[':UPDATEJOB'] = 0
            varMap[':LASTMOD'] = datetime.datetime.utcnow()-datetime.timedelta(hours=varMap[':HOURS'])
            self.cur.execute(sqlRst+comment,varMap)
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
            # get reliability info
            reliabilityMap = {}
            try:
                sqlRel = "SELECT tier2,t2group FROM ATLAS_GRISLI.t_m4regions_replication"
                self.cur.arraysize = 10000
                self.cur.execute(sqlRel+comment)
                tmpList = self.cur.fetchall()
                for tier2,t2group in tmpList:
                    # get prefix
                    tmpPrefix = re.sub('_DATADISK','',tier2)
                    reliabilityMap[tmpPrefix] = t2group
            except:
                errType,errValue = sys.exc_info()[:2]
                _logger.error("getSiteInfo %s:%s" % (errType.__class__.__name__,errValue))
            # get CVMFS availability
            sqlCVMFS  = "SELECT distinct siteid FROM ATLAS_PANDAMETA.installedSW WHERE `release`=:release"
            self.cur.execute(sqlCVMFS,{':release':'CVMFS'})
            tmpList = self.cur.fetchall()
            cvmfsSites = []
            for tmpItem, in tmpList:
                if not tmpItem in cvmfsSites:
                    cvmfsSites.append(tmpItem)
            # select
            sql = "SELECT nickname,dq2url,cloud,ddm,lfchost,se,gatekeeper,releases,memory,"
            sql+= "maxtime,status,space,retry,cmtconfig,setokens,seprodpath,glexec,"
            sql+= "priorityoffset,allowedgroups,defaulttoken,siteid,queue,localqueue,"
            sql+= "validatedreleases,accesscontrol,copysetup,maxinputsize,cachedse,"
            sql+= "allowdirectaccess,comment_,lastmod,multicloud,lfcregister,"
            sql+= "countryGroup,availableCPU,pledgedCPU,coreCount,transferringlimit,"
            sql+= "maxwdir,fairsharePolicy,minmemory,maxmemory,mintime,"
            sql+= "catchall,allowfax,wansourcelimit,wansinklimit,site,"
            sql+= "sitershare,cloudrshare,corepower,wnconnectivity,catchall "
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
                       countryGroup,availableCPU,pledgedCPU,coreCount,transferringlimit, \
                       maxwdir,fairsharePolicy,minmemory,maxmemory,mintime, \
                       catchall,allowfax,wansourcelimit,wansinklimit,pandasite, \
                       sitershare,cloudrshare,corepower,wnconnectivity,catchall \
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
                    ret.pandasite     = pandasite
                    ret.corepower     = corepower
                    ret.catchall      = catchall
                    ret.wnconnectivity = wnconnectivity
                    if ret.wnconnectivity == '':
                        ret.wnconnectivity = None
                    ret.fairsharePolicy = fairsharePolicy
                    # resource shares
                    ret.sitershare = None
                    """
                    try:
                        if not sitershare in [None,'']:
                            ret.sitershare = int(sitershare)
                    except:
                        pass
                    """    
                    ret.cloudrshare = None
                    """
                    try:
                        if not cloudrshare in [None,'']:
                            ret.cloudrshare = int(cloudrshare)
                    except:
                        pass
                    """    
                    # maxwdir
                    try:
                        if maxwdir == None:
                            ret.maxwdir = 0
                        else:
                            ret.maxwdir = int(maxwdir)
                    except:
                        if ret.maxinputsize in [0,None]:
                            ret.maxwdir = 0
                        else:
                            try:
                                ret.maxwdir = ret.maxinputsize + 2000
                            except:
                                ret.maxwdir = 16336
                    # memory
                    if minmemory != None:     
                        ret.minmemory = minmemory
                    else:
                        ret.minmemory = 0
                    if maxmemory != None:     
                        ret.maxmemory = maxmemory
                    else:
                        ret.maxmemory = 0
                    # mintime
                    if mintime != None:
                        ret.mintime = mintime
                    else:
                        ret.mintime = 0
                    # reliability
                    tmpPrefix = re.sub('_[^_]+DISK$','',ret.ddm)
                    if reliabilityMap.has_key(tmpPrefix):
                        ret.reliabilityLevel = reliabilityMap[tmpPrefix]
                    else:
                        ret.reliabilityLevel = None
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
                    # core count
                    ret.coreCount = 0
                    if not coreCount in ['',None]:
                        try:
                            ret.coreCount = int(coreCount)
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
                            tmpTokenNameBase = tmpTokens[idxToken]
                            if not ret.setokens.has_key(tmpTokenNameBase):
                                tmpTokenName = tmpTokenNameBase
                            else:
                                for tmpTokenNameIndex in range(10000):
                                    tmpTokenName = '%s%s' % (tmpTokenNameBase,tmpTokenNameIndex)
                                    if not ret.setokens.has_key(tmpTokenName):
                                        break
                            ret.setokens[tmpTokenName] = tmpddmID
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
                            tmpTokenNameBase = tmpTokens[idxToken]
                            if not ret.seprodpath.has_key(tmpTokenNameBase):
                                tmpTokenName = tmpTokenNameBase
                            else:
                                for tmpTokenNameIndex in range(10000):
                                    tmpTokenName = '%s%s' % (tmpTokenNameBase,tmpTokenNameIndex)
                                    if not ret.seprodpath.has_key(tmpTokenName):
                                        break
                            ret.seprodpath[tmpTokenName] = tmpSePath
                    # VO related params
                    ret.priorityoffset = priorityoffset
                    ret.allowedgroups  = allowedgroups
                    ret.defaulttoken   = defaulttoken
                    # direct access
                    if allowdirectaccess == 'True':
                        ret.allowdirectaccess = True
                    else:
                        ret.allowdirectaccess = False
                    # CVMFS
                    if siteid in cvmfsSites:
                        ret.iscvmfs = True
                    else:
                        ret.iscvmfs = False
                    # limit of the number of transferring jobs
                    ret.transferringlimit = 0
                    if not transferringlimit in ['',None]:
                        try:
                            ret.transferringlimit = int(transferringlimit)
                        except:
                            pass
                    # FAX
                    ret.allowfax = False
                    try:
                        if catchall != None and 'allowfax' in catchall:
                            ret.allowfax = True
                        if allowfax == 'True':
                            ret.allowfax = True
                    except:
                        pass
                    ret.wansourcelimit = 0
                    if not wansourcelimit in [None,'']:
                        ret.wansourcelimit = wansourcelimit
                    ret.wansinklimit = 0
                    if not wansinklimit in [None,'']:
                        ret.wansinklimit = wansinklimit
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
            sql  = "SELECT PandaID "
            sql += "FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus=:jobStatus "
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
            sql  = "SELECT COUNT(*),countryGroup "            
            sql += "FROM ATLAS_PANDA.jobsActive4 WHERE jobStatus IN (:jobStatus1,:jobStatus2) "
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
            usingID    = self.faresharePolicy[siteName]['usingID']
            usingPrio  = self.faresharePolicy[siteName]['usingPrio']
            usingCloud = self.faresharePolicy[siteName]['usingCloud']
            shareDefList   = []
            for tmpDefItem in self.faresharePolicy[siteName]['policyList']:
                shareDefList.append({'policy':tmpDefItem,'count':{},'maxprio':{}})
            # check if countryGroup has activated jobs
            varMapAll = {}
            varMapAll[':prodSourceLabel'] = 'managed'
            sqlH  = "SELECT /*+ RESULT_CACHE */ jobStatus,"
            if usingGroup:
                sqlH += 'workingGroup,'
            if usingType:
                sqlH += 'processingType,'
            if usingID:
                sqlH += 'workqueue_id,'
            if usingPrio:
                sqlH += 'currentPriority,'
            sqlH += "SUM(num_of_jobs),MAX(currentPriority) FROM ATLAS_PANDA.MV_JOBSACTIVE4_STATS "
            varMapSite = {}
            sqlSite = "WHERE computingSite IN ("
            tmpIdx = 0
            for tmpSiteName in [siteName]+aggSites:
                tmpKey = ':computingSite%s' % tmpIdx
                varMapSite[tmpKey] = tmpSiteName
                sqlSite += '%s,' % tmpKey
                tmpIdx += 1
            sqlSite = sqlSite[:-1]
            sqlSite += ") "
            if usingCloud != '':
                varMapCloud = {}
                sqlCloud = "WHERE cloud=:cloud "
                varMapCloud[':cloud'] = usingCloud
            sqlT = "AND prodSourceLabel=:prodSourceLabel "
            sqlT += "GROUP BY jobStatus"
            if usingGroup:
                sqlT += ',workingGroup'
            if usingType:
                sqlT += ',processingType'
            if usingID:
                sqlT += ',workqueue_id'
            if usingPrio:
                sqlT += ',currentPriority'
            # set autocommit on
            self.conn.begin()
            self.cur.arraysize = 100000
            # get site info
            varMap = {}
            for tmpKey,tmpVal in varMapAll.iteritems():
                varMap[tmpKey] = tmpVal
            for tmpKey,tmpVal in varMapSite.iteritems():
                varMap[tmpKey] = tmpVal
            sql = sqlH + sqlSite + sqlT
            self.cur.execute(sql+comment,varMap)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # no info about the site
            if res == None or len(res) == 0:
                _logger.debug("getCriteriaForProdShare %s : ret=None - no jobs" % siteName)
                return retForNone
            nSiteRow = len(res)
            # get cloud info
            if usingCloud != '':
                # set autocommit on
                self.conn.begin()
                self.cur.arraysize = 100000
                # get site info
                varMap = {}
                for tmpKey,tmpVal in varMapAll.iteritems():
                    varMap[tmpKey] = tmpVal
                for tmpKey,tmpVal in varMapCloud.iteritems():
                    varMap[tmpKey] = tmpVal
                sql = sqlH + sqlCloud + sqlT
                self.cur.execute(sql+comment,varMap)
                resC = self.cur.fetchall()
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                if resC != None:
                    res += resC
            # loop over all rows
            workingGroupInQueueMap = {}
            processGroupInQueueMap = {} 
            for indexRow,tmpItem in enumerate(res):
                tmpIdx = 0
                # map some job status to running
                jobStatus = tmpItem[tmpIdx]
                if jobStatus in ['sent','starting']:
                    jobStatus = 'running'
                # use cloud info for running jobs if cloud share is used
                if usingCloud != '':
                    if jobStatus == 'running' and indexRow < nSiteRow:
                        continue
                    if jobStatus != 'running' and indexRow >= nSiteRow:
                        continue
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
                if usingID:
                    workqueue_id = tmpItem[tmpIdx]
                    tmpIdx += 1
                if usingPrio:
                    currentPriority = tmpItem[tmpIdx]
                    tmpIdx += 1
                else:
                    currentPriority = None
                cnt = tmpItem[tmpIdx]
                tmpIdx += 1
                maxPriority = tmpItem[tmpIdx]
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
                        idInDefList    = self.faresharePolicy[siteName]['idList']
                    else:
                        groupInDefList = self.faresharePolicy[siteName]['groupListWithPrio']
                        typeInDefList  = self.faresharePolicy[siteName]['typeListWithPrio'][tmpShareDef['policy']['group']]
                        idInDefList    = self.faresharePolicy[siteName]['idListWithPrio']
                    # check working group
                    if usingGroup and not usingID:
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
                                    if workingGroup != None and re.search(tmpPattern,workingGroup) != None:
                                        toBeSkippedFlag = True
                                        break
                            if toBeSkippedFlag:
                                continue
                        else:
                            # needs to be matched if it is specified in the policy
                            if '*' in tmpShareDef['policy']['group']:
                                # using wild card
                                tmpPattern = '^' + tmpShareDef['policy']['group'].replace('*','.*') + '$'
                                if workingGroup == None or re.search(tmpPattern,workingGroup) == None:
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
                    if usingType and not usingID:
                        if tmpShareDef['policy']['type'] == None:
                            # catchall doesn't contain processGroups used by other policies
                            if processGroup != None and processGroup in typeInDefList:
                                continue
                        else:
                            # needs to be matched if it is specified in the policy
                            if tmpShareDef['policy']['type'] != processGroup:
                                continue
                    # check workqueue_id
                    if usingID:
                        if tmpShareDef['policy']['id'] == None:
                            if workqueue_id != None and workqueue_id in idInDefList:
                                continue
                        else:
                            if tmpShareDef['policy']['id'] != workqueue_id:
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
                    # append job status
                    if not tmpShareDef['count'].has_key(jobStatus):
                        tmpShareDef['count'][jobStatus] = 0
                    # sum
                    tmpShareDef['count'][jobStatus] += cnt
                    # max priority
                    if not tmpShareDef['maxprio'].has_key(jobStatus):
                        tmpShareDef['maxprio'][jobStatus] = maxPriority
                    elif tmpShareDef['maxprio'][jobStatus] < maxPriority:
                        tmpShareDef['maxprio'][jobStatus] = maxPriority
            # loop over all policies to calcurate total number of running jobs and total share
            totalRunning = 0
            shareMap     = {}
            msgShare     = 'share->'
            msgShareMap  = {}
            totalShareNonGP       = 0
            totalRunningNonGP     = 0
            totalActiveShareNonGP = 0
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
                msgShareMap[policyName] = '%s:activated=%s:running=%s' % (policyName,tmpNumActivated,tmpNumRunning)
                # get total share and total number of running jobs for non-GP
                if tmpShareDef['policy']['group'] == None:
                    totalShareNonGP += tmpShareValue
                    totalRunningNonGP += tmpNumRunning
                    # get total share for active non-GP
                    if tmpNumActivated != 0:
                        totalActiveShareNonGP += tmpShareValue
                # sum
                totalRunning += tmpNumRunning
                # not use the policy if no activated jobs
                if tmpNumActivated == 0:
                    continue
                # max priority
                maxPriority = 0
                if tmpShareDef['maxprio'].has_key('activated'):
                    maxPriority = tmpShareDef['maxprio']['activated']
                # append
                shareMap[policyName] = {
                    'share':tmpShareValue,
                    'running':tmpNumRunning,
                    'policy':tmpShareDef['policy'],
                    'maxprio':maxPriority,
                    }
            # re-normalize when some non-GP policies are inactive
            if totalShareNonGP != totalActiveShareNonGP and totalActiveShareNonGP != 0:
                for policyName,tmpVarMap in shareMap.iteritems():
                    # essentially non-GP share is multiplied by totalShareNonGP/totalActiveShareNonGP
                    if tmpVarMap['policy']['group'] == None:
                        tmpVarMap['share'] *= totalShareNonGP
                    else:
                        tmpVarMap['share'] *= totalActiveShareNonGP
            # make message with share info    
            for policyName in msgShareMap.keys():
                if shareMap.has_key(policyName):
                    msgShare += '%s:share=%s,' % (msgShareMap[policyName],shareMap[policyName]['share'])
                else:
                    msgShare += '%s:share=0,' % msgShareMap[policyName]
            # get total share
            totalShare = 0
            for policyName,tmpVarMap in shareMap.iteritems():
                totalShare += tmpVarMap['share']
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
                # take max priority into account for cloud share
                if usingCloud != '':
                    # skip over share
                    if tmpShareNow > tmpShareDef:
                        continue
                    tmpShareRatio /= float(1000 + tmpVarMap['maxprio'])
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
                                if tmpGroup == None:
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
                # workqueue_id
                if tmpShareDef['id'] == None:
                    idInDefList  = self.faresharePolicy[siteName]['idList']
                    # catch all except IDs used by other policies
                    if idInDefList != []:
                        # get the list of processingTypes from the list of processGroups
                        retVarMapP = {}
                        retStrP = 'AND workqueue_id NOT IN ('
                        tmpIdx  = 0
                        for tmpID in idInDefList:
                            tmpKey = ':shareID%s' % tmpIdx
                            retVarMapP[tmpKey] = tmpID
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
                    # match with one ID
                    retStr += 'AND workqueue_id IN ('
                    tmpIdx = 0
                    tmpKey = ':shareID%s' % tmpIdx
                    retVarMap[tmpKey] = tmpShareDef['id']
                    retStr += '%s,' % tmpKey
                    tmpIdx += 1
                    retStr = retStr[:-1]
                    retStr += ') '
            # priority
            tmpIdx = 0
            for tmpDefItem in prioToBeImposed:
                if tmpDefItem['group'] in [None,tmpShareDef['group']] and \
                   tmpDefItem['type'] in [None,tmpShareDef['type']] and \
                   tmpDefItem['id'] in [None,tmpShareDef['id']]:
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
                    retStr += ('AND currentPriority%s%s ' % (retStrP,tmpKey)) 
                    tmpIdx += 1
            _logger.debug("getCriteriaForProdShare %s : sql='%s' var=%s cloud=%s %s %s" % \
                          (siteName,retStr,str(retVarMap),usingCloud,msgShare,msgPrio))
            # append criteria for test jobs
            if retStr != '':
                retVarMap[':shareLabel1'] = 'managed'
                retVarMap[':shareLabel2'] = 'test'
                retVarMap[':shareLabel3'] = 'prod_test'
                retVarMap[':shareLabel4'] = 'install'
                retVarMap[':shareLabel5'] = 'pmerge'
                retVarMap[':shareLabel6'] = 'urgent'
                newRetStr  = 'AND (prodSourceLabel IN (:shareLabel2,:shareLabel3,:shareLabel4) '
                newRetStr += 'OR (processingType IN (:shareLabel5,:shareLabel6)) '
                newRetStr += 'OR (prodSourceLabel=:shareLabel1 ' + retStr + '))'
                retStr = newRetStr
            return retStr,retVarMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = "getCriteriaForProdShare %s : %s %s" % (siteName,errtype,errvalue)
            errStr.strip()
            errStr += traceback.format_exc()
            _logger.error(errStr)
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
            if self.backend == 'oracle':
                sql += "FROM ATLAS_PANDAMETA.schedconfig WHERE countryGroup IS NOT NULL AND siteid LIKE 'ANALY_%' "
            else:
                sql += "FROM ATLAS_PANDAMETA.schedconfig WHERE countryGroup IS NOT NULL AND siteid LIKE 'ANALY_%%' "
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
               (datetime.datetime.utcnow()-self.updateTimeForFaresharePolicy) < datetime.timedelta(minutes=15):
            return
        if not getNewMap:
            # update utime
            self.updateTimeForFaresharePolicy = datetime.datetime.utcnow()
        _logger.debug("getFaresharePolicy")
        try:
            # set autocommit on
            self.conn.begin()
            # get default share
            cloudShareMap = {}
            cloudTier1Map = {}
            sqlD = "SELECT name,fairshare,tier1 FROM ATLAS_PANDAMETA.cloudconfig"
            self.cur.arraysize = 100000            
            self.cur.execute(sqlD+comment)
            res = self.cur.fetchall()
            for cloudName,cloudShare,cloudTier1 in res:
                try:
                    cloudTier1Map[cloudName] = cloudTier1.split(',')
                except:
                    pass
                if not cloudShare in ['',None]:
                    cloudShareMap[cloudName] = cloudShare
            # get share per site
            sql  = "SELECT siteid,fairsharePolicy,cloud "
            sql += "FROM ATLAS_PANDAMETA.schedconfig WHERE NOT siteid LIKE 'ANALY_%' GROUP BY siteid,fairsharePolicy,cloud"
            self.cur.execute(sql+comment)
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # update policy
            faresharePolicy = {}
            for siteid,faresharePolicyStr,cloudName in res:
                try:
                    # share is undefined
                    usingCloudShare = ''
                    if faresharePolicyStr in ['',None,' None']:
                        # skip if share is not defined at site or cloud 
                        if not cloudShareMap.has_key(cloudName):
                            continue
                        # skip if T1 doesn't define share
                        if cloudTier1Map.has_key(cloudName) and siteid in cloudTier1Map[cloudName]:
                            continue
                        # use cloud share
                        faresharePolicyStr = cloudShareMap[cloudName]
                        usingCloudShare = cloudName
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
                        # workqueue_ID
                        tmpPolicy['id'] = None
                        tmpMatch = re.search('id=([^:]+)',tmpItem)
                        if tmpMatch != None:
                            if tmpMatch.group(1) in ['*','any']:
                                # use None for catchall
                                pass
                            else:
                                try:
                                    tmpPolicy['id'] = int(tmpMatch.group(1))
                                except:
                                    pass
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
                                     'id'            : None,
                                     'priority'      : None,
                                     'prioCondition' : None,
                                     'share'         : '100%'}
                        faresharePolicy[siteid]['policyList'].append(tmpPolicy)
                    # some translation
                    faresharePolicy[siteid]['usingGroup'] = False
                    faresharePolicy[siteid]['usingType']  = False
                    faresharePolicy[siteid]['usingID']    = False
                    faresharePolicy[siteid]['usingPrio']  = False
                    faresharePolicy[siteid]['usingCloud'] = usingCloudShare
                    faresharePolicy[siteid]['groupList']  = []
                    faresharePolicy[siteid]['typeList']   = {}
                    faresharePolicy[siteid]['idList']     = []
                    faresharePolicy[siteid]['groupListWithPrio']  = []
                    faresharePolicy[siteid]['typeListWithPrio']   = {}
                    faresharePolicy[siteid]['idListWithPrio']     = []
                    for tmpDefItem in faresharePolicy[siteid]['policyList']:
                        # using WG
                        if tmpDefItem['group'] != None:
                            faresharePolicy[siteid]['usingGroup'] = True
                        # using PG    
                        if tmpDefItem['type'] != None:
                            faresharePolicy[siteid]['usingType'] = True
                        # using workqueue_ID
                        if tmpDefItem['id'] != None:
                            faresharePolicy[siteid]['usingID'] = True
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
                            # get list of workqueue_ids
                            if tmpDefItem['id'] != None and not tmpDefItem['id'] in faresharePolicy[siteid]['idList']:
                                faresharePolicy[siteid]['idList'].append(tmpDefItem['id'])
                        else:
                            # get list of woringGroups
                            if tmpDefItem['group'] != None and not tmpDefItem['group'] in faresharePolicy[siteid]['groupListWithPrio']:
                                faresharePolicy[siteid]['groupListWithPrio'].append(tmpDefItem['group'])
                            # get list of processingGroups 
                            if not faresharePolicy[siteid]['typeListWithPrio'].has_key(tmpDefItem['group']):
                                faresharePolicy[siteid]['typeListWithPrio'][tmpDefItem['group']] = []
                            if tmpDefItem['type'] != None and not tmpDefItem['type'] in faresharePolicy[siteid]['typeListWithPrio'][tmpDefItem['group']]:
                                faresharePolicy[siteid]['typeListWithPrio'][tmpDefItem['group']].append(tmpDefItem['type'])
                            # get list of workqueue_ids
                            if tmpDefItem['id'] != None and not tmpDefItem['id'] in faresharePolicy[siteid]['idListWithPrio']:
                                faresharePolicy[siteid]['idListWithPrio'].append(tmpDefItem['id'])
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
    def checkSitesWithRelease(self,sites,releases,caches,cmtConfig=None,onlyCmtConfig=False,
                              cmtConfigPattern=False):
        comment = ' /* DBProxy.checkSitesWithRelease */'
        try:
            relStr = releases
            if releases != None:
                relStr = releases.replace('\n',' ')
            caStr = caches
            if caches != None:
                caStr = caches.replace('\n',' ')
            _logger.debug("checkSitesWithRelease(%s,%s,%s,%s,%s)" % (sites,relStr,caStr,cmtConfig,
                                                                     cmtConfigPattern))
            # select
            sql  = "SELECT distinct siteid FROM ATLAS_PANDAMETA.InstalledSW WHERE "
            loopKey2 = None
            loopValues2 = []
            if not caches in ['','NULL',None]:
                loopKey = ':cache'
                loopValues = caches.split('\n')
                sql += "cache=:cache "                
                if not releases in ['','NULL',None]:
                    loopKey2 = ':release'
                    loopValues2 = releases.split('\n')
                    sql += "AND `release`=:release "
            elif not releases in ['','NULL',None]:
                loopKey = ':release'
                loopValues = releases.split('\n')
                sql += "`release`=:release AND cache='None' "
            elif onlyCmtConfig:
                loopKey = None
                loopValues = [None]
            else:
                # don't check
                return sites
            checkCMT = False
            if not cmtConfig in ['','NULL',None]:
                if onlyCmtConfig:
                    if not cmtConfigPattern:
                        sql += "cmtConfig=:cmtConfig "
                    else:
                        sql += "cmtConfig LIKE :cmtConfig "
                else:
                    sql += "AND cmtConfig=:cmtConfig "
                checkCMT = True
            sql += "AND siteid IN ("
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 1000
            # loop over all releases/caches
            for loopIdx,loopVal in enumerate(loopValues):
                sqlSite = sql
                varMap = {}
                if loopKey != None:
                    # remove Atlas-
                    loopVal = re.sub('^Atlas-','',loopVal)
                    varMap[loopKey] = loopVal
                    if loopKey2 != None:
                        loopVal2 = loopValues2[loopIdx]
                        loopVal2 = re.sub('^Atlas-','',loopVal2)
                        varMap[loopKey2] = loopVal2
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
                        if loopKey != None:
                            varMap[loopKey] = loopVal
                            if loopKey2 != None:
                                varMap[loopKey2] = loopVal2
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
                sql += "`release`=:release AND cache='None' "
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


    # get list of cmtConfig
    def getCmtConfigList(self,relaseVer):
        comment = ' /* DBProxy.getCmtConfigList */'        
        try:
            methodName = "getCmtConfigList"
            _logger.debug("{0} for {1}".format(methodName,relaseVer))
            # select
            sql  = "SELECT distinct cmtConfig FROM ATLAS_PANDAMETA.installedSW WHERE release=:release"
            # start transaction
            self.conn.begin()
            self.cur.arraysize = 10
            # execute
            varMap = {}
            varMap[':release'] = relaseVer
            self.cur.execute(sql+comment, varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # append
            tmpList = []
            for tmpItem, in resList:
                tmpList.append(tmpItem)
            _logger.debug("{0} -> {1}".format(methodName,str(tmpList)))
            return tmpList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
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


    # get special dipatcher parameters
    def getSpecialDispatchParams(self):
        comment = ' /* DBProxy.getSpecialDispatchParams */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        _logger.debug("{0} start".format(methodName))
        try:
            retMap = {}
            # set autocommit on
            self.conn.begin()
            # select for glexec
            sql  = "SELECT DISTINCT siteID,glexec FROM ATLAS_PANDAMETA.schedconfig WHERE glexec IN (:stat1,:stat2) "
            varMap = {}
            varMap[':stat1'] = 'True'
            varMap[':stat2'] = 'test'
            self.cur.arraysize = 10000
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpRet = {}
            for siteID,glexec in resList:
                tmpRet[siteID] = glexec
            retMap['glexecSites'] = tmpRet
            _logger.debug("{0} got {1} glexec sites".format(methodName,len(retMap['glexecSites'])))
            # set autocommit on
            self.conn.begin()
            # select for proxy cache and panda proxy
            sql = "SELECT DISTINCT siteID,catchAll FROM ATLAS_PANDAMETA.schedconfig WHERE catchAll IS NOT NULL "
            varMap = {}
            self.cur.arraysize = 10000
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            tmpRet = {}
            tmpPandaProxy = set()
            # FIXME
            tmpPandaProxy.add('AGLT2_SL6')
            sql = "SELECT dn FROM ATLAS_PANDAMETA.users WHERE name=:name "
            for siteID,catchAll in resList:
                try:
                    if 'PandaProxy' in catchAll.split(','):
                        tmpPandaProxy.add(siteID)
                except:
                    pass
                # extract username
                tmpMatch = re.search('proxyCache=([^,]+)',catchAll)
                if tmpMatch != None:
                    userName,role = tmpMatch.group(1).split(':')
                    # get DN
                    varMap = {}
                    varMap[':name'] = userName
                    self.cur.execute(sql+comment,varMap)
                    res = self.cur.fetchone()
                    if res != None:
                        userDN, = res
                        userDN = re.sub('/CN=limited proxy','',userDN)
                        userDN = re.sub('(/CN=proxy)+','',userDN)
                        tmpRet[siteID] = {'dn':userDN,'role':role}
            retMap['proxyCacheSites'] = tmpRet
            _logger.debug("{0} got {1} proxyCache sites".format(methodName,len(retMap['proxyCacheSites'])))
            retMap['pandaProxySites'] = tmpPandaProxy
            _logger.debug("{0} got {1} pandaProxy sites".format(methodName,len(retMap['pandaProxySites'])))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # set autocommit on
            self.conn.begin()
            # select to get the list of authorized users
            allowKey = []
            allowProxy = []
            sql  = "SELECT DISTINCT name,gridpref FROM ATLAS_PANDAMETA.users WHERE (status IS NULL OR status<>:ngStatus) AND gridpref IS NOT NULL "
            varMap = {}
            varMap[':ngStatus'] = 'disabled'
            self.cur.arraysize = 100000
            self.cur.execute(sql+comment,varMap)
            resList = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            for compactDN,gridpref in resList:
                # users authorized for proxy retrieval
                if 'p' in gridpref:
                    if not compactDN in allowProxy:
                        allowProxy.append(compactDN)
                # users authorized for key-pair retrieval 
                if 'k' in gridpref:
                    if not compactDN in allowKey:
                        allowKey.append(compactDN)
            retMap['allowKey'] = allowKey
            retMap['allowProxy'] = allowProxy
            _logger.debug("{0} got {1} users for key {2} users for proxy".format(methodName,
                                                                                 len(retMap['allowKey']),
                                                                                 len(retMap['allowProxy'])))
            # read key pairs
            keyPair = {}
            try:
                keyFileNames = glob.glob(panda_config.keyDir+'/*')
                for keyName in keyFileNames:
                    tmpF = open(keyName)
                    keyPair[os.path.basename(keyName)] = tmpF.read()
                    tmpF.close()
            except:
                self.dumpErrorMessage(_logger,methodName)
            retMap['keyPair'] = keyPair
            _logger.debug("{0} got {1} key files".format(methodName,len(retMap['keyPair'])))
            _logger.debug("{0} done".format(methodName))
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return {}


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

    
    # extract scope from dataset name
    def extractScope(self,name):
        try:
            if name.lower().startswith('user') or \
                   name.lower().startswith('group'):
                # return None if there are not enough fields
                if len(name.split('.')) < 2:
                    return None
                return name.lower().split('.')[0] + '.' + name.lower().split('.')[1]
            return name.split('.')[0]
        except:
            return None
        
    
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
            sql  = "SELECT jobid,status FROM ATLAS_PANDAMETA.users WHERE name=:name "
            sql += "FOR UPDATE "
            sqlAdd  = "INSERT INTO ATLAS_PANDAMETA.users "
            sqlAdd += "(ID,NAME,LASTMOD,FIRSTJOB,LATESTJOB,CACHETIME,NCURRENT,JOBID) "
            sqlAdd += "VALUES(ATLAS_PANDAMETA.USERS_ID_SEQ.nextval,:name,"
            sqlAdd += "CURRENT_DATE,CURRENT_DATE,CURRENT_DATE,CURRENT_DATE,0,1) "
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10            
            res = self.cur.fetchall()
            # insert if no record
            if res == None or len(res) == 0:
                try:
                    self.cur.execute(sqlAdd+comment,varMap)
                    retI = self.cur.rowcount
                    _logger.debug("getUserParameter %s inserted new row with %s" % (dn,retI))
                    # emulate DB response
                    res = [[1,'']]
                except:
                    errType,errValue = sys.exc_info()[:2]
                    _logger.error("getUserParameter %s failed to insert new row with %s:%s" % (dn,errType,errValue))
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
    def checkBanUser(self,dn,sourceLabel,jediCheck=False):
        comment = ' /* DBProxy.checkBanUser */'                            
        try:
            methodName = "checkBanUser"
            # set initial values
            retStatus = True
            name = self.cleanUserID(dn)
            methodName += ' {0}'.format(name)
            _logger.debug("{0} start dn={1} label={2} jediCheck={3}".format(methodName,dn,sourceLabel,jediCheck))
            # set autocommit on
            self.conn.begin()
            # select
            sql = "SELECT status,dn FROM ATLAS_PANDAMETA.users WHERE name=:name"
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10
            res = self.cur.fetchone()            
            if res != None:
                # check status
                tmpStatus,dnInDB = res
                if tmpStatus in ['disabled']:
                    retStatus = False
                elif jediCheck and (dnInDB in ['',None] or dnInDB != dn): 
                    # add DN
                    sqlUp  = "UPDATE ATLAS_PANDAMETA.users SET dn=:dn WHERE name=:name "
                    varMap = {}
                    varMap[':name'] = name
                    varMap[':dn']   = dn
                    self.cur.execute(sqlUp+comment,varMap)
                    retI = self.cur.rowcount
                    _logger.debug("{0} update DN with Status={1}".format(methodName,retI))
                    if retI != 1:
                        retStatus = 1
            else:
                # new user
                if jediCheck:
                    name = self.cleanUserID(dn)
                    sqlAdd  = "INSERT INTO ATLAS_PANDAMETA.users "
                    sqlAdd += "(ID,NAME,DN,LASTMOD,FIRSTJOB,LATESTJOB,CACHETIME,NCURRENT,JOBID) "
                    sqlAdd += "VALUES(ATLAS_PANDAMETA.USERS_ID_SEQ.nextval,:name,:dn,"
                    sqlAdd += "CURRENT_DATE,CURRENT_DATE,CURRENT_DATE,CURRENT_DATE,0,1) "
                    varMap = {}
                    varMap[':name'] = name
                    varMap[':dn']   = dn
                    self.cur.execute(sqlAdd+comment,varMap)
                    retI = self.cur.rowcount
                    _logger.debug("{0} inserted new row with Status={1}".format(methodName,retI))
                    if retI != 1:
                        retStatus = 2
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} done with Status={1}".format(methodName,retStatus))
            return retStatus
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return retStatus

        
    # get email address for a user
    def getEmailAddr(self,name,withDN=False,withUpTime=False):
        comment = ' /* DBProxy.getEmailAddr */'
        _logger.debug("get email for %s" % name) 
        # sql
        if withDN:
            failedRet = "","",None
            sql = "SELECT email,dn,location FROM ATLAS_PANDAMETA.users WHERE name=:name"
        elif withUpTime:
            failedRet = "",None
            sql = "SELECT email,location FROM ATLAS_PANDAMETA.users WHERE name=:name"
        else:
            failedRet = ""
            sql = "SELECT email FROM ATLAS_PANDAMETA.users WHERE name=:name"
        try:
            # set autocommit on
            self.conn.begin()
            # select
            varMap = {}
            varMap[':name'] = name
            self.cur.execute(sql+comment,varMap)
            self.cur.arraysize = 10            
            res = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            if res != None and len(res) != 0:
                if withDN or withUpTime:
                    if withDN:
                        email,dn,upTime = res[0]
                    else:
                        email,upTime = res[0]
                    # convert time
                    try:
                        upTime = datetime.datetime.strptime(upTime,'%Y-%m-%d %H:%M:%S')
                    except:
                        upTime = None
                    if withDN:
                        return email,dn,upTime
                    else:
                        return email,upTime
                else:
                    return res[0][0]
            # return empty string
            return failedRet
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getEmailAddr : %s %s" % (type,value))
            # roll back
            self._rollback()
            return failedRet


    # set email address for a user
    def setEmailAddr(self,userName,emailAddr):
        comment = ' /* DBProxy.setEmailAddr */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        _logger.debug("{0} {1} to {2}".format(methodName,userName,emailAddr))
        # sql
        sql = "UPDATE ATLAS_PANDAMETA.users SET email=:email,location=:uptime WHERE name=:name "
        try:
            # set autocommit on
            self.conn.begin()
            # set
            varMap = {}
            varMap[':name'] = userName
            varMap[':email'] = emailAddr
            varMap[':uptime'] = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            self.cur.execute(sql+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False


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
            vals = {}
            sql0 = 'INSERT INTO ATLAS_PANDAMETA.proxykey (id,'
            sql1 = 'VALUES (ATLAS_PANDAMETA.PROXYKEY_ID_SEQ.nextval,'

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
                sql += "AND prodSourceLabel=:prodSourceLabel AND lockedBy<>:ngLock GROUP BY jobDefinitionID"
                varMap = {}
                varMap[':prodUserName'] = compactDN
                varMap[':prodSourceLabel'] = 'user'
                varMap[':ngLock'] = 'jedi'
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
                                try:
                                    job.metadata = clobMeta.read()
                                except AttributeError:
                                    job.metadata = str(clobMeta)
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
                                try:
                                    job.jobParameters = clobJobP.read()
                                except AttributeError:
                                    job.jobParameters = str(clobJobP)
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
                    sql0  = "SELECT distinct prodDBlock FROM ATLAS_PANDA.%s " % table
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
            # check dataset status
            allClosed = True
            retInfo = {}
            latestUpdate   = None
            latestJobDefID = None
            varMap = {}
            varMap[':type1'] = 'log'
            varMap[':type2'] = 'output'            
            sql = 'SELECT status,modificationDate FROM ATLAS_PANDA.Datasets WHERE name=:name AND type IN (:type1,:type2)'
            sqlJ =  "SELECT MAX(modificationTime) FROM ATLAS_PANDA.jobsArchived4 "
            sqlJ += "WHERE prodUserName=:prodUserName AND jobDefinitionID=:jobDefinitionID" 
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
                        _logger.debug("checkDatasetStatusForNotifier(%s,%s) %s has %s with %s at %s" % \
                                      (jobsetID,jobDefinitionID,tmpJobDefID,tmpDataset,tmpStatus,tmpModificationDate))
                        if not tmpStatus in ['closed','tobeclosed','completed']:
                            # some datasets are still active 
                            allClosed = False
                            _logger.debug("checkDatasetStatusForNotifier(%s,%s) wait due to %s %s %s" % \
                                          (jobsetID,jobDefinitionID,tmpJobDefID,tmpDataset,tmpStatus))
                            break
                        elif tmpStatus == 'tobeclosed':
                            # select latest modificationTime in job table
                            varMapJ = {}
                            varMapJ[':prodUserName'] = prodUserName
                            varMapJ[':jobDefinitionID'] = tmpJobDefID
                            self.cur.execute(sqlJ+comment, varMapJ)
                            resJ = self.cur.fetchone()
                            if resJ == None:
                                # error
                                allClosed = False
                                _logger.error("checkDatasetStatusForNotifier(%s,%s) %s cannot find job" % \
                                              (jobsetID,jobDefinitionID,tmpJobDefID))
                                break
                            tmpModificationTime, = resJ
                            _logger.debug("checkDatasetStatusForNotifier(%s,%s) %s modtime:%s" % \
                                          (jobsetID,jobDefinitionID,tmpJobDefID,tmpModificationTime))
                            if latestUpdate == None or latestUpdate < tmpModificationTime:
                                # use the latest updated jobDefID
                                latestUpdate   = tmpModificationTime
                                latestJobDefID = tmpJobDefID
                            elif latestUpdate == tmpModificationTime and latestJobDefID < tmpJobDefID:
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
            _logger.debug("checkDatasetStatusForNotifier(%s,%s) -> all:%s %s latest:%s" % \
                          (jobsetID,jobDefinitionID,allClosed,latestJobDefID,
                           jobDefinitionID == latestJobDefID))
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


    # record status change
    def recordStatusChange(self,pandaID,jobStatus,jobInfo=None,infoMap={},useCommit=True):
        comment = ' /* DBProxy.recordStatusChange */'
        # check config
        if not hasattr(panda_config,'record_statuschange') or panda_config.record_statuschange != True:
            return
        # get job info
        varMap = {}
        varMap[':PandaID']          = pandaID
        varMap[':jobStatus']        = jobStatus
        varMap[':modificationHost'] = self.myHostName
        if jobInfo != None:
            varMap[':computingSite']   = jobInfo.computingSite
            varMap[':cloud']           = jobInfo.cloud
            varMap[':prodSourceLabel'] = jobInfo.prodSourceLabel
        elif infoMap != None:
            varMap[':computingSite']   = infoMap['computingSite']
            varMap[':cloud']           = infoMap['cloud']
            varMap[':prodSourceLabel'] = infoMap['prodSourceLabel']
        else:
            # no info
            return
        # convert NULL to None
        for tmpKey in varMap.keys():
            if varMap[tmpKey] == 'NULL':
                varMap[tmpKey] = None
        # insert
        sql  = "INSERT INTO ATLAS_PANDA.jobs_StatusLog "
        sql += "(PandaID,modificationTime,jobStatus,prodSourceLabel,cloud,computingSite,modificationHost) "
        sql += "VALUES (:PandaID,CURRENT_DATE,:jobStatus,:prodSourceLabel,:cloud,:computingSite,:modificationHost) "
        try:
            # start transaction
            if useCommit:
                self.conn.begin()
            self.cur.execute(sql+comment,varMap)
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError, 'Commit error'
        except:
            # roll back
            if useCommit:
                self._rollback()
            errType,errValue = sys.exc_info()[:2]
            _logger.error("recordStatusChange %s %s: %s %s" % (pandaID,jobStatus,errType,errValue))
            if not useCommit:
                raise RuntimeError, 'recordStatusChange failed'
        return 


    # propagate result to JEDI
    def propagateResultToJEDI(self,jobSpec,cur,oldJobStatus=None,extraInfo=None,finishPending=False,waitLock=False):
        comment = ' /* DBProxy.propagateResultToJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(jobSpec.PandaID)
        datasetContentsStat = {}
        # loop over all files
        finishUnmerge = False
        hasInput = False
        _logger.debug(methodName+' waitLock={0}'.format(waitLock))
        for fileSpec in jobSpec.Files:
            # skip if no JEDI
            if fileSpec.fileID == 'NULL':
                continue
            # do nothing for unmerged output/log files when merged job successfully finishes,
            # since they were already updated by merged job
            if jobSpec.jobStatus == 'finished' and fileSpec.isUnMergedOutput():
                continue
            # check file status
            varMap = {}
            varMap[':fileID']     = fileSpec.fileID
            varMap[':datasetID']  = fileSpec.datasetID
            varMap[':jediTaskID'] = jobSpec.jediTaskID
            varMap[':attemptNr']  = fileSpec.attemptNr
            sqlFileStat  = "SELECT status FROM ATLAS_PANDA.JEDI_Dataset_Contents "
            sqlFileStat += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND attemptNr=:attemptNr "
            sqlFileStat += "FOR UPDATE "
            if not waitLock:
                sqlFileStat += "NOWAIT "
            _logger.debug(methodName+' '+sqlFileStat+comment+str(varMap))
            cur.execute(sqlFileStat+comment,varMap)
            resFileStat = self.cur.fetchone()
            if resFileStat != None:
                oldFileStatus, = resFileStat
            else:
                oldFileStatus = None
            # skip if already cancelled
            if oldFileStatus in ['cancelled']:
                continue
            # update Dataset Contents table
            updateMetadata  = False
            updateAttemptNr = False
            updateNumEvents = False
            updateFailedAttempt = False
            varMap = {}
            varMap[':fileID']     = fileSpec.fileID
            varMap[':datasetID']  = fileSpec.datasetID
            varMap[':keepTrack']  = 1
            varMap[':jediTaskID'] = jobSpec.jediTaskID
            varMap[':attemptNr']  = fileSpec.attemptNr
            # set file status
            if fileSpec.type in ['input','pseudo_input']:
                hasInput = True
                if jobSpec.jobStatus == 'finished':
                    varMap[':status'] = 'finished'
                else:
                    """ WILL MOVE TO RETRY MODULE
                    if fileSpec.status == 'missing' and jobSpec.processingType != 'pmerge':
                        # lost file
                        varMap[':status'] = 'lost'
                    else:
                    """
                    # set ready for next attempt
                    varMap[':status'] = 'ready'
                    updateAttemptNr = True
                    if jobSpec.jobStatus == 'failed':
                        updateFailedAttempt = True
            else:
                varMap[':status'] = jobSpec.jobStatus
                if jobSpec.jobStatus == 'finished':
                    varMap[':status'] = 'finished'
                    # update metadata
                    updateMetadata = True
                    # update nEvents
                    updateNumEvents = True
                elif fileSpec.status == 'merging':
                    # set ready to merge files for failed jobs
                    varMap[':status'] = 'ready'
                     # update metadata
                    updateMetadata = True
            sqlFile = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET status=:status"
            # attempt number
            if updateAttemptNr == True:
                # increment attemptNr for next attempt
                sqlFile += ",attemptNr=attemptNr+1"
            # failed attempts
            if updateFailedAttempt == True:
                sqlFile += ",failedAttempt=failedAttempt+1"
            # metadata
            if updateMetadata:
                # set file metadata
                for tmpKey in ['lfn','GUID','fsize','checksum']:
                    tmpVal = getattr(fileSpec,tmpKey)
                    if tmpVal == 'NULL':
                        if tmpKey in fileSpec._zeroAttrs:
                            tmpVal = 0
                        else:
                            tmpVal = None
                    tmpMapKey = ':%s' % tmpKey        
                    sqlFile += ",%s=%s" % (tmpKey,tmpMapKey)
                    varMap[tmpMapKey] = tmpVal
                # extra metadata
                if extraInfo != None:
                    # nevents
                    if extraInfo.has_key('nevents') and extraInfo['nevents'].has_key(fileSpec.lfn):
                        tmpKey = 'nEvents'
                        tmpMapKey = ':%s' % tmpKey
                        sqlFile += ",%s=%s" % (tmpKey,tmpMapKey)
                        varMap[tmpMapKey] = extraInfo['nevents'][fileSpec.lfn]
                    # LB number
                    if extraInfo.has_key('lbnr') and extraInfo['lbnr'].has_key(fileSpec.lfn):
                        tmpKey = 'lumiBlockNr'
                        tmpMapKey = ':%s' % tmpKey
                        sqlFile += ",%s=%s" % (tmpKey,tmpMapKey)
                        varMap[tmpMapKey] = extraInfo['lbnr'][fileSpec.lfn]
                # reset keepTrack unless merging
                if fileSpec.status != 'merging':
                    sqlFile += ",keepTrack=NULL"
                else:
                    # set boundaryID for merging
                    sqlFile += ",boundaryID=:boundaryID"
                    varMap[':boundaryID'] = jobSpec.PandaID
                    # set max attempt
                    sqlFile += ",maxAttempt=attemptNr+3"
            sqlFile += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlFile += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
            _logger.debug(methodName+' '+sqlFile+comment+str(varMap))
            cur.execute(sqlFile+comment,varMap)
            nRow = cur.rowcount
            if nRow == 1:
                datasetID = fileSpec.datasetID
                fileStatus = varMap[':status']
                if not datasetContentsStat.has_key(datasetID):
                    datasetContentsStat[datasetID] = {'nFilesUsed':0,'nFilesFinished':0,
                                                      'nFilesFailed':0,'nFilesOnHold':0,
                                                      'nFilesTobeUsed':0,'nEvents':0}
                # read nEvents
                if updateNumEvents:
                    sqlEVT = "SELECT nEvents FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                    sqlEVT += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    if not waitLock:
                        sqlEVT += "FOR UPDATE NOWAIT "
                    varMap = {}
                    varMap[':fileID']     = fileSpec.fileID
                    varMap[':datasetID']  = fileSpec.datasetID
                    varMap[':jediTaskID'] = jobSpec.jediTaskID
                    _logger.debug(methodName+' '+sqlEVT+comment+str(varMap))
                    cur.execute(sqlEVT+comment,varMap)
                    resEVT = self.cur.fetchone()
                    if resEVT != None:
                        tmpNumEvents, = resEVT
                        if tmpNumEvents != None:
                            try:
                                datasetContentsStat[datasetID]['nEvents'] += tmpNumEvents 
                            except:
                                pass
                # update file counts
                if fileSpec.status == 'merging' and \
                        (finishPending or not jobSpec.prodSourceLabel in ['user','panda']):
                    # files to be merged for pending failed jobs
                    datasetContentsStat[datasetID]['nFilesOnHold'] += 1
                elif fileStatus == 'ready':
                    # check attemptNr and maxAttempt when the file failed (ready = input failed)
                    # skip secondary datasets which have maxAttempt=None 
                    sqlAttNr  = "SELECT attemptNr,maxAttempt,failedAttempt,maxFailure FROM ATLAS_PANDA.JEDI_Dataset_Contents "
                    sqlAttNr += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    varMap = {}
                    varMap[':fileID']     = fileSpec.fileID
                    varMap[':datasetID']  = fileSpec.datasetID
                    varMap[':jediTaskID'] = jobSpec.jediTaskID
                    _logger.debug(methodName+' '+sqlAttNr+comment+str(varMap))
                    cur.execute(sqlAttNr+comment,varMap)
                    resAttNr = self.cur.fetchone()
                    if resAttNr != None:
                        newAttemptNr,maxAttempt,failedAttempt,maxFailure = resAttNr
                        if maxAttempt != None:
                            if maxAttempt > newAttemptNr and (maxFailure == None or maxFailure > failedAttempt):
                                if fileSpec.status != 'merging': 
                                    # decrement nUsed to trigger reattempt
                                    datasetContentsStat[datasetID]['nFilesUsed'] -= 1
                                else:
                                    # increment nTobeUsed to trigger merging
                                    datasetContentsStat[datasetID]['nFilesTobeUsed'] += 1
                            else:
                                # no more reattempt
                                datasetContentsStat[datasetID]['nFilesFailed'] += 1
                                # merge job failed
                                if jobSpec.processingType == 'pmerge':
                                    # update unmerged file
                                    sqlUmFile  = "UPDATE ATLAS_PANDA.JEDI_Dataset_Contents SET status=:status,keepTrack=NULL "
                                    sqlUmFile += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                                    varMap = {}
                                    varMap[':fileID']     = fileSpec.fileID
                                    varMap[':datasetID']  = fileSpec.datasetID
                                    varMap[':jediTaskID'] = jobSpec.jediTaskID
                                    varMap[':status']     = 'notmerged'
                                    _logger.debug(methodName+' '+sqlUmFile+comment+str(varMap))
                                    cur.execute(sqlUmFile+comment,varMap)
                                    # set flag to update unmerged jobs
                                    finishUnmerge = True
                elif fileStatus in ['finished','lost']:
                    # successfully used or produced, or lost
                    datasetContentsStat[datasetID]['nFilesFinished'] += 1
                else:
                    # failed to produce the file
                    datasetContentsStat[datasetID]['nFilesFailed'] += 1
                # changed from transferring
                if fileSpec.type in ['input','pseudo_input']:
                    if oldJobStatus == 'transferring':
                        datasetContentsStat[datasetID]['nFilesOnHold'] -= 1
                # killed dring merging
                if jobSpec.jobStatus == 'cancelled' and oldJobStatus == 'merging' and fileSpec.isUnMergedOutput():
                    # get corresponding sub
                    varMap = {}
                    varMap[':pandaID']    = jobSpec.PandaID
                    varMap[':fileID']     = fileSpec.fileID
                    varMap[':datasetID']  = fileSpec.datasetID
                    varMap[':jediTaskID'] = jobSpec.jediTaskID
                    sqlGetDest  = "SELECT destinationDBlock FROM ATLAS_PANDA.filesTable4 "
                    sqlGetDest += "WHERE pandaID=:pandaID AND jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                    _logger.debug(methodName+' '+sqlGetDest+comment+str(varMap))
                    cur.execute(sqlGetDest+comment,varMap)
                    preMergedDest, = self.cur.fetchone()
                    # check if corresponding sub is closed
                    varMap = {}
                    varMap[':name']    = preMergedDest
                    varMap[':subtype'] = 'sub'
                    sqlCheckDest  = "SELECT status FROM ATLAS_PANDA.Datasets "
                    sqlCheckDest += "WHERE name=:name AND subtype=:subtype "
                    _logger.debug(methodName+' '+sqlCheckDest+comment+str(varMap))
                    cur.execute(sqlCheckDest+comment,varMap)
                    tmpResDestStat = self.cur.fetchone()
                    if tmpResDestStat != None:
                        preMergedDestStat, = tmpResDestStat
                    else:
                        preMergedDestStat = 'notfound'
                        _logger.debug(methodName+' {0} not found for datasetID={1}'.format(preMergedDest,datasetID))
                    if not preMergedDestStat in ['tobeclosed','completed']:
                        datasetContentsStat[datasetID]['nFilesOnHold'] -= 1
                    else:
                        _logger.debug(methodName+' '+\
                                          'not change nFilesOnHold for datasetID={0} since sub is in {1}'.format(datasetID,
                                                                                                                 preMergedDestStat))
                        # increment nUsed when mergeing is killed before merge job is generated
                        if oldFileStatus == 'ready':
                            datasetContentsStat[datasetID]['nFilesUsed'] += 1
        # update JEDI_Datasets table
        nOutEvents = 0
        if datasetContentsStat != {}:
            tmpDatasetIDs = datasetContentsStat.keys()
            tmpDatasetIDs.sort()
            for tmpDatasetID in tmpDatasetIDs:
                tmpContentsStat = datasetContentsStat[tmpDatasetID]
                sqlJediDL = "SELECT nFilesUsed,nFilesFailed,nFilesTobeUsed,nFilesFinished,nFilesOnHold FROM ATLAS_PANDA.JEDI_Datasets "
                sqlJediDL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID FOR UPDATE "
                if not waitLock:
                    sqlJediDL += "NOWAIT "
                varMap = {}
                varMap[':jediTaskID'] = jobSpec.jediTaskID
                varMap[':datasetID']  = tmpDatasetID
                _logger.debug(methodName+' '+sqlJediDL+comment+str(varMap))
                cur.execute(sqlJediDL+comment,varMap)
                # sql to update nFiles info
                toUpdateFlag = False
                eventsToRead = False
                sqlJediDS = "UPDATE ATLAS_PANDA.JEDI_Datasets SET "
                for tmpStatKey,tmpStatVal in tmpContentsStat.iteritems():
                    if tmpStatVal == 0:
                        continue
                    if tmpStatVal > 0:
                        sqlJediDS += '%s=%s+%s,' % (tmpStatKey,tmpStatKey,tmpStatVal)
                    else:
                        sqlJediDS += '%s=%s-%s,' % (tmpStatKey,tmpStatKey,abs(tmpStatVal))
                    toUpdateFlag = True
                    if tmpStatKey == 'nEvents' and tmpStatVal > nOutEvents:
                        nOutEvents = tmpStatVal
                sqlJediDS  = sqlJediDS[:-1]
                sqlJediDS += " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                varMap = {}
                varMap[':jediTaskID'] = jobSpec.jediTaskID
                varMap[':datasetID']  = tmpDatasetID
                # update
                if toUpdateFlag:
                    _logger.debug(methodName+' '+sqlJediDS+comment+str(varMap))                            
                    cur.execute(sqlJediDS+comment,varMap)
        # update t_task
        if jobSpec.jobStatus == 'finished' and not jobSpec.prodSourceLabel in ['panda']:
            varMap = {}
            varMap[':jediTaskID'] = jobSpec.jediTaskID
            varMap[':status1']    = 'running'
            varMap[':status2']    = 'submitting'
            varMap['noutevents']  = nOutEvents
            schemaDEFT = self.getSchemaDEFT()
            sqlTtask  = "UPDATE {0}.T_TASK ".format(schemaDEFT)
            if jobSpec.processingType != 'pmerge':
                sqlTtask += "SET total_done_jobs=total_done_jobs+1,timestamp=CURRENT_DATE,total_events=total_events+:noutevents "
            else:
                sqlTtask += "SET timestamp=CURRENT_DATE,total_events=total_events+:noutevents "
            sqlTtask += "WHERE taskid=:jediTaskID AND status IN (:status1,:status2) "
            _logger.debug(methodName+' '+sqlTtask+comment+str(varMap))
            cur.execute(sqlTtask+comment,varMap)
        # propagate failed result to unmerge job
        if finishUnmerge:
            self.updateUnmergedJobs(jobSpec)
        # return
        return True



    # check if task is active
    def checkTaskStatusJEDI(self,jediTaskID,cur):
        comment = ' /* DBProxy.checkTaskStatusJEDI */'
        retVal = False
        if not jediTaskID in ['NULL',None]:
            sql = "SELECT status FROM ATLAS_PANDA.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            cur.execute(sql+comment,varMap)
            res = cur.fetchone()
            if res != None:
                if res[0] in ['ready','running','scouting','pending',
                              'topreprocess','preprocessing','aborting',
                              'finishing','scouted','toreassign','paused',
                              'throttled']:
                    retVal = True
        _logger.debug('checkTaskStatusJEDI jediTaskID=%s with %s' % (jediTaskID,retVal))            
        return retVal



    # update JEDI for pilot retry
    def updateForPilotRetryJEDI(self,job,cur,onlyHistory=False):
        comment = ' /* DBProxy.updateForPilotRetryJEDI */'
        # sql to update file
        sqlFJ  = "UPDATE {0}.JEDI_Dataset_Contents ".format(panda_config.schemaJEDI)
        sqlFJ += "SET attemptNr=attemptNr+1,failedAttempt=failedAttempt+1,PandaID=:PandaID "
        sqlFJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlFJ += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack "
        sqlFP  = "UPDATE ATLAS_PANDA.filesTable4 SET attemptNr=attemptNr+1 "
        sqlFP += "WHERE row_ID=:row_ID "
        if not onlyHistory:
            for tmpFile in job.Files:
                # skip if no JEDI
                if tmpFile.fileID == 'NULL':
                    continue
                # update JEDI contents
                varMap = {}
                varMap[':jediTaskID'] = tmpFile.jediTaskID
                varMap[':datasetID']  = tmpFile.datasetID
                varMap[':fileID']     = tmpFile.fileID
                varMap[':attemptNr']  = tmpFile.attemptNr
                varMap[':PandaID']    = tmpFile.PandaID
                varMap[':keepTrack']  = 1
                _logger.debug(sqlFJ+comment+str(varMap))
                cur.execute(sqlFJ+comment,varMap)
                nRow = cur.rowcount
                if nRow == 1:
                    # update fileTable if JEDI contents was updated
                    varMap = {}
                    varMap[':row_ID'] = tmpFile.row_ID
                    _logger.debug(sqlFP+comment+str(varMap))
                    cur.execute(sqlFP+comment,varMap)
        # get origin
        originIDs = self.getOriginPandaIDsJEDI(job.parentID,job.jediTaskID,cur)
        # sql to record retry history
        sqlRH  = "INSERT INTO {0}.JEDI_Job_Retry_History ".format(panda_config.schemaJEDI)
        sqlRH += "(jediTaskID,oldPandaID,newPandaID,originPandaID,relationType) "
        sqlRH += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID,:relationType) "
        # record retry history
        for originID in originIDs:
            varMap = {}
            varMap[':jediTaskID'] = job.jediTaskID
            varMap[':oldPandaID'] = job.parentID
            varMap[':newPandaID'] = job.PandaID
            varMap[':originPandaID'] = originID
            varMap[':relationType'] = 'retry'
            cur.execute(sqlRH+comment,varMap)
        return



    # get origin PandaIDs
    def getOriginPandaIDsJEDI(self,pandaID,jediTaskID,cur):
        comment = ' /* DBProxy.getOriginPandaIDsJEDI */'
        # sql to get parent IDs
        sqlFJ  = "SELECT MIN(originPandaID) FROM {0}.JEDI_Job_Retry_History ".format(panda_config.schemaJEDI)
        sqlFJ += "WHERE jediTaskID=:jediTaskID AND newPandaID=:newPandaID "
        varMap = {}
        varMap[':jediTaskID'] = jediTaskID
        varMap[':newPandaID'] = pandaID
        cur.execute(sqlFJ+comment,varMap)
        resT = cur.fetchone()
        retList = []
        if resT == None:
            # origin
            retList.append(pandaID)
        else:
            # use only one origin since tracking the whole tree brings too many origins
            originPandaID, = resT
            if originPandaID == None:
                # origin
                retList.append(pandaID)
            else:
                retList.append(originPandaID)
        # return
        return retList



    # get retry history
    def getRetryHistoryJEDI(self,jediTaskID):
        comment = ' /* DBProxy.getRetryHistoryJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        _logger.debug("{0} start".format(methodName))
        # sql
        sql  = "SELECT oldPandaID,newPandaID FROM {0}.JEDI_Job_Retry_History ".format(panda_config.schemaJEDI)
        sql += "WHERE jediTaskID=:jediTaskID GROUP BY oldPandaID,newPandaID "
        try:
            # set autocommit on
            self.conn.begin()
            self.cur.arraysize = 1000000
            # set
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            self.cur.execute(sql+comment,varMap)
            resG = self.cur.fetchall()
            retMap = {}
            for oldPandaID,newPandaID in resG:
                if not retMap.has_key(oldPandaID):
                    retMap[oldPandaID] = []
                retMap[oldPandaID].append(newPandaID)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} return len={1}".format(methodName,len(retMap)))
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
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
            errMsg = "rollback EC:%s %s" % (oraErrCode,errValue)
            _logger.debug(errMsg)
            # error codes for connection error
            if self.backend == 'oracle':
                error_Codes  = ['ORA-01012','ORA-01033','ORA-01034','ORA-01089',
                                'ORA-03113','ORA-03114','ORA-12203','ORA-12500',
                                'ORA-12571','ORA-03135','ORA-25402']
                # other errors are apperantly given when connection lost contact
                if useOtherError:
                    error_Codes += ['ORA-01861','ORA-01008']
            else:
                # mysql error codes for connection error
                from MySQLdb.constants.ER import ACCESS_DENIED_ERROR, DBACCESS_DENIED_ERROR, \
                    SERVER_SHUTDOWN, ILLEGAL_VALUE_FOR_TYPE
                from MySQLdb.constants.CR import CONNECTION_ERROR, CONN_HOST_ERROR, \
                    LOCALHOST_CONNECTION, SERVER_LOST
                error_Codes = [
                    ACCESS_DENIED_ERROR, DBACCESS_DENIED_ERROR,
                    SERVER_SHUTDOWN,
                    CONNECTION_ERROR, CONN_HOST_ERROR, LOCALHOST_CONNECTION,
                    SERVER_LOST
                    ]
                # other errors are apperantly given when connection lost contact
                if useOtherError:
                    error_Codes += [
                        ILLEGAL_VALUE_FOR_TYPE
                        ]
            if oraErrCode in error_Codes:
                # reconnect
                retFlag = self.connect(reconnect=True)
                _logger.debug("rollback reconnected %s" % retFlag)
        except:
            pass
        # return
        return retVal



    # dump error message
    def dumpErrorMessage(self,tmpLog,methodName):
        # error
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "{0}: {1} {2}".format(methodName,errtype.__name__,errvalue)
        errStr.strip()
        errStr += traceback.format_exc()
        tmpLog.error(errStr)



    # get DEFT schema
    def getSchemaDEFT(self):
        if not hasattr(panda_config,'schemaDEFT'):
            return "ATLAS_DEFT"
        return panda_config.schemaDEFT



    # get working group with production role
    def getWorkingGroup(self,fqans):
        for fqan in fqans:
            # check production role
            match = re.search('/[^/]+/([^/]+)/Role=production',fqan)
            if match != None:
                return match.group(1)
        return None



    # get country working
    def getCountryGroup(self,fqans):
        # extract country group
        for tmpFQAN in fqans:
            match = re.search('^/atlas/([^/]+)/',tmpFQAN)
            if match != None:
                tmpCountry = match.group(1)
                # use country code or usatlas
                if len(tmpCountry) == 2:
                    return tmpCountry
                # usatlas
                if tmpCountry in ['usatlas']:
                    return 'us'
        return None



    # insert TaskParams
    def insertTaskParamsPanda(self,taskParams,dn,prodRole,fqans,parent_tid,properErrorCode=False):
        comment = ' /* JediDBProxy.insertTaskParamsPanda */'
        try:
            methodName = "insertTaskParamsPanda"
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            methodName += ' <{0}>'.format(compactDN)
            _logger.debug('{0} start'.format(methodName))
            # decode json
            taskParamsJson = PrioUtil.decodeJSON(taskParams)
            # set user name and task type
            taskParamsJson['userName'] = compactDN
            if not prodRole or not 'taskType' in taskParamsJson:
                taskParamsJson['taskType']   = 'anal'
                taskParamsJson['taskPriority'] = 1000
                # extract working group
                if taskParamsJson.has_key('official') and taskParamsJson['official'] == True:
                    workingGroup = self.getWorkingGroup(fqans)
                    if workingGroup != None:
                        taskParamsJson['workingGroup'] = workingGroup
                # extract country group
                countryGroup = self.getCountryGroup(fqans)
                if countryGroup != None:
                    taskParamsJson['countryGroup'] = countryGroup
            _logger.debug('{0} taskName={1}'.format(methodName,taskParamsJson['taskName']))
            schemaDEFT = self.getSchemaDEFT()
            # sql to check task duplication
            sqlTD  = "SELECT jediTaskID,status FROM {0}.JEDI_Tasks ".format(panda_config.schemaJEDI)
            sqlTD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND userName=:userName AND taskName=:taskName "
            sqlTD += "ORDER BY jediTaskID DESC FOR UPDATE "
            # sql to check DEFT table
            sqlCD  = "SELECT taskid FROM {0}.T_TASK ".format(schemaDEFT)
            sqlCD += "WHERE vo=:vo AND prodSourceLabel=:prodSourceLabel AND userName=:userName AND taskName=:taskName "
            sqlCD += "ORDER BY taskid DESC FOR UPDATE " 
            # sql to insert task parameters
            sqlT  = "INSERT INTO {0}.T_TASK ".format(schemaDEFT)
            sqlT += "(taskid,status,submit_time,vo,prodSourceLabel,userName,taskName,jedi_task_parameters,priority,current_priority,parent_tid) VALUES "
            varMap = {}
            if self.backend == 'oracle':
                sqlT += "({0}.PRODSYS2_TASK_ID_SEQ.nextval,".format(schemaDEFT)
            else:
                #panda_config.backend == 'mysql':
                ### fake sequence
                sql = " INSERT INTO PRODSYS2_TASK_ID_SEQ (col) VALUES (NULL) "
                self.cur.arraysize = 100
                self.cur.execute(sql + comment, {})
                sql2 = """ SELECT LAST_INSERT_ID() """
                self.cur.execute(sql2 + comment, {})
                nextval, = self.cur.fetchone()
                sqlT += "( :nextval ,".format(schemaDEFT)
                varMap[':nextval'] = nextval
            sqlT += ":status,CURRENT_DATE,:vo,:prodSourceLabel,:userName,:taskName,:param,:priority,:current_priority,"
            if parent_tid == None:
                if self.backend == 'oracle':
                    sqlT += "{0}.PRODSYS2_TASK_ID_SEQ.currval) ".format(schemaDEFT)
                else:
                    #panda_config.backend == 'mysql':
                    ### fake sequence
                    sql = " SELECT MAX(COL) FROM PRODSYS2_TASK_ID_SEQ "
                    self.cur.arraysize = 100
                    self.cur.execute(sql + comment, {})
                    currval, = self.cur.fetchone()
                    sqlT += " :currval ) "
                    varMap[':currval'] = currval
            else:
                sqlT += ":parent_tid) "
            sqlT += "RETURNING TASKID INTO :jediTaskID"
            # sql to delete command
            sqlDC  = "DELETE FROM {0}.PRODSYS_COMM ".format(schemaDEFT)
            sqlDC += "WHERE COMM_TASK=:jediTaskID "
            # sql to insert command
            sqlIC  = "INSERT INTO {0}.PRODSYS_COMM (COMM_TASK,COMM_OWNER,COMM_CMD,COMM_PARAMETERS) ".format(schemaDEFT)
            sqlIC += "VALUES (:jediTaskID,:comm_owner,:comm_cmd,:comm_parameters) "
            # begin transaction
            self.conn.begin()
            # check duplication
            goForward = True
            retFlag = False
            retVal = None
            errorCode = 0
            if taskParamsJson['taskType'] == 'anal' and \
                    (taskParamsJson.has_key('uniqueTaskName') and taskParamsJson['uniqueTaskName'] == True):
                varMap[':vo']       = taskParamsJson['vo']
                varMap[':userName'] = taskParamsJson['userName']
                varMap[':taskName'] = taskParamsJson['taskName']
                varMap[':prodSourceLabel'] = taskParamsJson['prodSourceLabel']
                self.cur.execute(sqlTD+comment,varMap)
                resDT = self.cur.fetchone()
                if resDT == None:
                    # check DEFT table
                    varMap = {}
                    varMap[':vo']       = taskParamsJson['vo']
                    varMap[':userName'] = taskParamsJson['userName']
                    varMap[':taskName'] = taskParamsJson['taskName']
                    varMap[':prodSourceLabel'] = taskParamsJson['prodSourceLabel']
                    self.cur.execute(sqlCD+comment,varMap)
                    resCD = self.cur.fetchone()
                    if resCD != None:
                        # task is already in DEFT
                        jediTaskID, = resCD
                        _logger.debug('{0} old jediTaskID={1} with taskName={2} in DEFT table'.format(methodName,jediTaskID,
                                                                                                      varMap[':taskName']))
                        goForward = False
                        retVal  = 'jediTaskID={0} is already queued for outDS={1}. '.format(jediTaskID,
                                                                                            taskParamsJson['taskName'])
                        retVal += 'You cannot submit duplicated tasks. '
                        _logger.debug('{0} skip since old task is already queued in DEFT'.format(methodName))
                        errorCode = 1
                else:
                    # task is already in JEDI table
                    jediTaskID,taskStatus = resDT
                    _logger.debug('{0} old jediTaskID={1} with taskName={2} in status={3}'.format(methodName,jediTaskID,
                                                                                                  varMap[':taskName'],taskStatus))
                    # check task status
                    if not taskStatus in ['finished','failed','aborted','done']:
                        # still active
                        goForward = False
                        retVal  = 'jediTaskID={0} is in the {1} state for outDS={2}. '.format(jediTaskID,
                                                                                              taskStatus,
                                                                                              taskParamsJson['taskName'])
                        retVal += 'You can re-execute the task with the same or another input once it goes into finished/failed/done'
                        _logger.debug('{0} skip since old task is not yet finalized'.format(methodName))
                        errorCode = 2
                    else:
                        # extract several params for incremental execution
                        newTaskParams = {}
                        for tmpKey,tmpVal in taskParamsJson.iteritems():
                            # dataset names
                            # site limitation
                            # command line parameters
                            # splitting hints
                            # fixed source code
                            if tmpKey.startswith('dsFor') \
                                    or tmpKey in ['site','cloud','includedSite','excludedSite'] \
                                    or tmpKey == 'cliParams' \
                                    or tmpKey in ['nFilesPerJob','nFiles','nEvents'] \
                                    or tmpKey == 'fixedSandbox':
                                newTaskParams[tmpKey] = tmpVal
                                if tmpKey == 'fixedSandbox' and 'sourceURL' in taskParamsJson:
                                    newTaskParams['sourceURL'] = taskParamsJson['sourceURL']
                                continue
                        # delete command just in case
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        self.cur.execute(sqlDC+comment,varMap)
                        # insert command
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':comm_cmd']  = 'incexec'
                        varMap[':comm_owner']  = 'DEFT'
                        varMap[':comm_parameters'] = json.dumps(newTaskParams)
                        self.cur.execute(sqlIC+comment,varMap)
                        _logger.debug('{0} {1} jediTaskID={2} with {3}'.format(methodName,varMap[':comm_cmd'],
                                                                                   jediTaskID,str(newTaskParams)))
                        retVal  = 'reactivation accepted. '
                        retVal += 'jediTaskID={0} (currently in {1} state) will be re-executed with old and/or new input'.format(jediTaskID,
                                                                                                                                taskStatus) 
                        goForward = False
                        retFlag = True
                        errorCode = 3
            if goForward:
                # insert task parameters
                taskParams = json.dumps(taskParamsJson)    
                varMap = {}
                varMap[':param']  = taskParams
                varMap[':status'] = 'waiting'
                varMap[':vo']       = taskParamsJson['vo']
                varMap[':userName'] = taskParamsJson['userName']
                varMap[':taskName'] = taskParamsJson['taskName']
                if parent_tid != None:
                    varMap[':parent_tid']  = parent_tid
                varMap[':prodSourceLabel'] = taskParamsJson['prodSourceLabel']
                varMap[':jediTaskID'] = self.cur.var(varNUMBER)
                if taskParamsJson.has_key('taskPriority'):
                    varMap[':priority'] = taskParamsJson['taskPriority']
                else:
                    varMap[':priority'] = 100
                varMap[':current_priority'] = varMap[':priority']
                self.cur.execute(sqlT+comment,varMap)
                jediTaskID = long(self.cur.getvalue(varMap[':jediTaskID']))
                if properErrorCode:
                    retVal = "succeeded. new jediTaskID={0}".format(jediTaskID)
                else:
                    retVal = jediTaskID
                _logger.debug('{0} inserted new jediTaskID={1}'.format(methodName,jediTaskID))
                retFlag = True
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug('{0} done'.format(methodName))
            if properErrorCode:
                return errorCode,retVal
            return retFlag,retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            errorCode = 4
            retVal = 'failed to register task'
            if properErrorCode:
                return errorCode,retVal
            return False,retVal



    # send command to task through DEFT
    def sendCommandTaskPanda(self,jediTaskID,dn,prodRole,comStr,comComment=None,useCommit=True,properErrorCode=False,
                             comQualifier=None):
        comment = ' /* JediDBProxy.sendCommandTaskPanda */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += ' <jediTaskID={0}>'.format(jediTaskID)
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            _logger.debug("{0} start com={1} DN={2} prod={3} comment={4} qualifier={5}".format(methodName,comStr,compactDN,
                                                                                               prodRole,comComment,
                                                                                               comQualifier))
            # sql to check status and owner
            sqlTC  = "SELECT status,userName,prodSourceLabel FROM {0}.JEDI_Tasks ".format(panda_config.schemaJEDI)
            sqlTC += "WHERE jediTaskID=:jediTaskID FOR UPDATE "
            # sql to delete command
            schemaDEFT = self.getSchemaDEFT()
            sqlT  = "DELETE FROM {0}.PRODSYS_COMM ".format(schemaDEFT)
            sqlT += "WHERE COMM_TASK=:jediTaskID "
            # sql to insert command
            sqlC  = "INSERT INTO {0}.PRODSYS_COMM (COMM_TASK,COMM_OWNER,COMM_CMD,COMM_COMMENT) ".format(schemaDEFT)
            sqlC += "VALUES (:jediTaskID,:comm_owner,:comm_cmd,:comm_comment) "
            goForward = True
            retStr = ''
            retCode = 0
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get task status and owner
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            self.cur.execute(sqlTC+comment,varMap)
            resTC = self.cur.fetchone()
            if resTC == None:
                # task not found
                retStr = 'jediTaskID={0} not found'.format(jediTaskID)
                _logger.debug("{0} : {1}".format(methodName,retStr))
                goForward = False
                retCode = 2
            else:
                taskStatus,userName,prodSourceLabel = resTC
            # check owner
            if goForward:
                if not prodRole and compactDN != userName:
                    retStr = 'Permission denied: not the task owner or no production role'
                    _logger.debug("{0} : {1}".format(methodName,retStr))
                    goForward = False
                    retCode = 3
            # check task status
            if goForward:
                if comStr in ['kill','finish']:
                    if taskStatus in ['finished','done','prepared','broken','aborted','aborted','toabort','aborting','failed','finishing']:
                        goForward = False
                if comStr == 'retry':
                    if not taskStatus in ['finished','failed','aborted','exhausted']:
                        goForward = False
                if comStr == 'incexec':
                    if not taskStatus in ['finished','failed','done','aborted','exhausted']:
                        goForward = False
                if comStr == 'reassign':
                    if not taskStatus in ['registered','defined','ready','running','scouting','scouted','pending','assigning','exhausted']:
                        goForward = False
                if comStr == 'pause':
                    if taskStatus in ['finished','failed','done','aborted','broken','paused']:
                        goForward = False
                if comStr == 'resume':
                    if not taskStatus in ['paused','throttled']:
                        goForward = False
                if not goForward:
                    retStr = 'Command rejected: the {0} command is not accepted if the task is in {1} status'.format(comStr,taskStatus)
                    _logger.debug("{0} : {1}".format(methodName,retStr))
                    retCode = 4
            # retry for failed analysis jobs
            if goForward and properErrorCode and taskStatus in ['running','scouting','pending'] and prodSourceLabel in ['user']:
                retCode = 5
                retStr = taskStatus
            if goForward:
                # delete command just in case
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                self.cur.execute(sqlT+comment,varMap)
                # insert command
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':comm_cmd']  = comStr
                varMap[':comm_owner']  = 'DEFT'
                if comComment == None:
                    tmpStr = ''
                    if not comQualifier in ['',None]:
                        tmpStr += '{0} '.format(comQualifier)
                    tmpStr += '{0} by {1}'.format(comStr,compactDN)
                    varMap[':comm_comment'] = tmpStr
                else:
                    varMap[':comm_comment'] = comComment
                self.cur.execute(sqlC+comment,varMap)
                _logger.debug('{0} done'.format(methodName))
                retStr = 'command is registered. will be executed in a few minutes'
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            if properErrorCode:
                return retCode,retStr
            else:
                if retCode == 0:
                    return True,retStr
                else:
                    return False,retStr
        except:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            if properErrorCode:
                return 1,'failed to register command'
            else:
                return False,'failed to register command'



    # update unmerged jobs
    def updateUnmergedJobs(self,job):
        comment = ' /* JediDBProxy.updateUnmergedJobs */'
        # get PandaID which produced unmerged files
        umPandaIDs = []
        umCheckedIDs = []
        # sql to get PandaIDs
        sqlUMP  = "SELECT PandaID,attemptNr FROM ATLAS_PANDA.filesTable4 "
        sqlUMP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlUMP += "AND type IN (:type1,:type2) ORDER BY attemptNr DESC "
        # sql to check job status
        sqlUMS  = "SELECT jobStatus FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID "
        # look for unmerged files
        for tmpFile in job.Files:
            if tmpFile.isUnMergedInput():
                varMap = {}
                varMap[':jediTaskID'] = tmpFile.jediTaskID
                varMap[':datasetID']  = tmpFile.datasetID
                varMap[':fileID']     = tmpFile.fileID
                varMap[':type1']  = 'output'
                varMap[':type2']  = 'log'
                self.cur.arraysize = 100
                _logger.debug(sqlUMP+comment+str(varMap))
                self.cur.execute(sqlUMP+comment, varMap)
                resUMP = self.cur.fetchall()
                # loop for job in merging state
                for tmpPandaID,tmpAttemptNr in resUMP:
                    # skip checked PandaIDs
                    if tmpPandaID in umCheckedIDs:
                        continue
                    # append to avoid redundant check
                    umCheckedIDs.append(tmpPandaID)
                    # check job status
                    varMap = {}
                    varMap[':PandaID'] = tmpPandaID
                    _logger.debug(sqlUMS+comment+str(varMap))
                    self.cur.execute(sqlUMS+comment, varMap)
                    resUMS = self.cur.fetchone()
                    # unmerged job should be in merging state
                    if resUMS != None and resUMS[0] == 'merging':
                        # append
                        umPandaIDs.append(tmpPandaID)
                        break
        # finish unmerge jobs
        sqlJFJ  = "SELECT %s " % JobSpec.columnNames()
        sqlJFJ += "FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
        sqlJFF  = "SELECT %s FROM ATLAS_PANDA.filesTable4 " % FileSpec.columnNames()
        sqlJFF += "WHERE PandaID=:PandaID"
        for tmpPandaID in umPandaIDs:
            # read job
            varMap = {}
            varMap[':PandaID'] = tmpPandaID
            self.cur.arraysize = 10
            self.cur.execute(sqlJFJ+comment, varMap)
            resJFJ = self.cur.fetchone()
            umJob = JobSpec()
            umJob.pack(resJFJ)
            umJob.jobStatus = job.jobStatus
            if umJob.jobStatus in ['failed','cancelled']:
                umJob.taskBufferErrorCode = ErrorCode.EC_MergeFailed
                umJob.taskBufferErrorDiag = "merge job {0}".format(umJob.jobStatus)
            # read files
            self.cur.arraysize = 10000
            self.cur.execute(sqlJFF+comment, varMap)
            resJFFs = self.cur.fetchall()
            for resJFF in resJFFs:
                umFile = FileSpec()
                umFile.pack(resJFF)
                umFile.status = umJob.jobStatus
                umJob.addFile(umFile)
            # finish
            _logger.debug('updateUnmerged PandaID={0}'.format(umJob.PandaID))
            self.archiveJob(umJob,False,useCommit=False)
        return



    # update unmerged datasets to trigger merging
    def updateUnmergedDatasets(self,job,finalStatusDS):
        comment = ' /* JediDBProxy.updateUnmergedDatasets */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(job.PandaID)
        # get PandaID which produced unmerged files
        umPandaIDs = []
        umCheckedIDs = []
        # sql to get file counts
        sqlGFC  = "SELECT status,COUNT(*) FROM ATLAS_PANDA.JEDI_Dataset_Contents "
        sqlGFC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID GROUP BY status "
        # sql to update nFiles in JEDI datasets
        sqlUNF  = "UPDATE ATLAS_PANDA.JEDI_Datasets "
        sqlUNF += "SET nFilesOnHold=0,nFiles=:nFiles,"
        sqlUNF += "nFilesUsed=:nFilesUsed,nFilesTobeUsed=:nFilesTobeUsed "
        sqlUNF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to check nFiles
        sqlUCF  = "SELECT nFilesTobeUsed,nFilesUsed FROM ATLAS_PANDA.JEDI_Datasets "
        sqlUCF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to update dataset status
        sqlUDS  = "UPDATE ATLAS_PANDA.JEDI_Datasets "
        sqlUDS += "SET status=:status "
        sqlUDS += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
        # sql to update dataset status in panda
        sqlUDP  = "UPDATE ATLAS_PANDA.Datasets "
        sqlUDP += "SET status=:status "
        sqlUDP += "WHERE vuid=:vuid AND NOT status IN (:statusR,:statusD) "
        try:
            _logger.debug('{0} start'.format(methodName))
            # begin transaction
            self.conn.begin()
            # update dataset in panda
            toSkip = False
            for datasetSpec in finalStatusDS:
                varMap = {}
                varMap[':vuid'] = datasetSpec.vuid 
                varMap[':status'] = 'tobeclosed'
                varMap[':statusR'] = 'tobeclosed'
                varMap[':statusD'] = 'completed'
                _logger.debug(methodName+' '+sqlUDP+comment+str(varMap))
                self.cur.execute(sqlUDP+comment, varMap)
                nRow = self.cur.rowcount
                if nRow != 1:
                    toSkip = True
                    _logger.debug('{0} failed to lock {1}'.format(methodName,
                                                                  datasetSpec.name))
            # look for unmerged files
            if not toSkip:
                updatedDS = []
                for tmpFile in job.Files:
                    if tmpFile.isUnMergedOutput():
                        if tmpFile.datasetID in updatedDS:
                            continue
                        updatedDS.append(tmpFile.datasetID)
                        # get file counts
                        varMap = {}
                        varMap[':jediTaskID'] = tmpFile.jediTaskID
                        varMap[':datasetID']  = tmpFile.datasetID
                        self.cur.arraysize = 100
                        _logger.debug(sqlGFC+comment+str(varMap))
                        self.cur.execute(sqlGFC+comment, varMap)
                        resListGFC = self.cur.fetchall()
                        varMap = {}
                        tmpNumFiles = 0
                        tmpNumReady = 0
                        for tmpFileStatus,tmpFileCount in resListGFC:
                            if tmpFileStatus in ['finished','failed','cancelled','notmerged',
                                                 'ready','lost','broken']:
                                tmpNumFiles += tmpFileCount
                                if tmpFileStatus in ['ready']:
                                    tmpNumReady += tmpFileCount
                        # update nFiles
                        varMap = {}
                        varMap[':jediTaskID'] = tmpFile.jediTaskID
                        varMap[':datasetID']  = tmpFile.datasetID
                        varMap[':nFiles'] = tmpNumFiles
                        varMap[':nFilesTobeUsed'] = tmpNumFiles
                        varMap[':nFilesUsed'] = tmpNumFiles-tmpNumReady
                        self.cur.arraysize = 10
                        _logger.debug(sqlUNF+comment+str(varMap))
                        self.cur.execute(sqlUNF+comment, varMap)
                        nRow = self.cur.rowcount
                        if nRow == 1:
                            # check nFilesTobeUsed
                            varMap = {}
                            varMap[':jediTaskID'] = tmpFile.jediTaskID
                            varMap[':datasetID']  = tmpFile.datasetID
                            self.cur.execute(sqlUCF+comment, varMap)
                            resUCF = self.cur.fetchone()
                            if resUCF != None:
                                nFilesTobeUsed,nFilesUsed = resUCF
                                varMap = {}
                                varMap[':jediTaskID'] = tmpFile.jediTaskID
                                varMap[':datasetID']  = tmpFile.datasetID
                                if nFilesTobeUsed-nFilesUsed > 0:
                                    varMap[':status'] = 'ready'
                                else:
                                    varMap[':status'] = 'done'
                                # update dataset status
                                _logger.debug(methodName+' '+sqlUDS+comment+str(varMap))
                                self.cur.execute(sqlUDS+comment, varMap)
                        else:
                            _logger.debug('{0} skip jediTaskID={1} datasetID={2}'.format(methodName,
                                                                                         tmpFile.jediTaskID,
                                                                                         tmpFile.datasetID))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug('{0} done'.format(methodName))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False


    # get active JediTasks in a time range
    def getJediTasksInTimeRange(self,dn,timeRange):
        comment = ' /* DBProxy.getJediTasksInTimeRange */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        _logger.debug("{0} : DN={1} range={2}".format(methodName,dn,timeRange.strftime('%Y-%m-%d %H:%M:%S')))
        try:
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            # make sql
            attrList = ['jediTaskID','modificationTime','status','processingType',
                        'transUses','transHome','architecture','reqID','creationDate',
                        'site','cloud','taskName']
            sql  = 'SELECT '
            for tmpAttr in attrList:
                sql += '{0},'.format(tmpAttr)
            sql  = sql[:-1]
            sql += " FROM {0}.JEDI_Tasks ".format(panda_config.schemaJEDI)
            sql += "WHERE userName=:userName AND modificationTime>=:modificationTime AND prodSourceLabel=:prodSourceLabel "
            varMap = {}
            varMap[':userName'] = compactDN
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
            retTasks = {}
            for tmpRes in resList:
                tmpDict = {}
                for tmpIdx,tmpAttr in enumerate(attrList):
                    tmpDict[tmpAttr] = tmpRes[tmpIdx]
                retTasks[tmpDict['reqID']] = tmpDict
            _logger.debug("{0} : {1}".format(methodName,str(retTasks)))
            return retTasks
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return {}


    # get details of JediTask
    def getJediTaskDetails(self,jediTaskID,fullFlag,withTaskInfo):
        comment = ' /* DBProxy.getJediTaskDetails */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        _logger.debug("{0} : jediTaskID={1} full={2}".format(methodName,jediTaskID,fullFlag))
        try:
            retDict = {'inDS':'','outDS':'','statistics':'','PandaID':[],
                       'mergeStatus':None,'mergePandaID':[]}
            # sql to get task status
            sqlT  = 'SELECT status FROM {0}.JEDI_Tasks WHERE jediTaskID=:jediTaskID '.format(panda_config.schemaJEDI)
            # sql to get datasets
            sqlD  = 'SELECT datasetID,datasetName,containerName,type,nFiles,nFilesTobeUsed,nFilesFinished,nFilesFailed,masterID,nFilesUsed,nFilesOnHold '
            sqlD += 'FROM {0}.JEDI_Datasets '.format(panda_config.schemaJEDI)
            sqlD += "WHERE jediTaskID=:jediTaskID "
            # sql to get PandaIDs
            sqlP  = "SELECT PandaID,COUNT(*) FROM {0}.JEDI_Dataset_Contents ".format(panda_config.schemaJEDI)
            sqlP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND PandaID IS NOT NULL "
            sqlP += "GROUP BY PandaID "
            # sql to get job status
            sqlJS  = "SELECT PandaID,jobStatus,processingType FROM ATLAS_PANDA.jobsDefined4 "
            sqlJS += "WHERE jediTaskID=:jediTaskID AND prodSourceLabel=:prodSourceLabel "
            sqlJS += "UNION "
            sqlJS  = "SELECT PandaID,jobStatus,processingType FROM ATLAS_PANDA.jobsActive4 "
            sqlJS += "WHERE jediTaskID=:jediTaskID AND prodSourceLabel=:prodSourceLabel "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 100000 
            # get task status
            if withTaskInfo:
                self.cur.execute(sqlT+comment, varMap)
                resT = self.cur.fetchone()
                if resT == None:
                    raise RuntimeError, 'No task info'
                retDict['status'] = resT[0]
            # get datasets
            self.cur.execute(sqlD+comment, varMap)
            resList = self.cur.fetchall()
            # get job status
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':prodSourceLabel'] = 'user'
            self.cur.execute(sqlJS+comment, varMap)
            resJS = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # make jobstatus map
            jobStatPandaIDs = {}
            for tmpPandaID,tmpJobStatus,tmpProcessingType in resJS:
                # ignore merge jobs
                if tmpProcessingType == 'pmerge':
                    continue
                jobStatPandaIDs[tmpPandaID] = tmpJobStatus
            # append
            inDSs = []
            outDSs = []
            totalNumFiles = 0
            totalTobeDone = 0
            totalFinished = 0
            totalFailed = 0
            totalStatMap = {}
            for datasetID,datasetName,containerName,datasetType,nFiles,nFilesTobeUsed,nFilesFinished,nFilesFailed,masterID,nFilesUsed,nFilesOnHold in resList:
                # primay input
                if datasetType in ['input','pseudo_input','trn_log'] and masterID == None:
                    # unmerge dataset
                    if datasetType == 'trn_log':
                        unmergeFlag = True
                    else:
                        unmergeFlag = False
                    # collect input dataset names
                    if datasetType == 'input':
                        # use container name if not empty
                        if not containerName in [None,'']:
                            targetName = containerName
                        else:
                            targetName = datasetName
                        if not targetName in inDSs:
                            inDSs.append(targetName)
                            retDict['inDS'] += '{0},'.format(targetName)
                    # statistics
                    if datasetType in ['input','pseudo_input']:
                        totalNumFiles += nFiles
                        totalFinished += nFilesFinished
                        totalFailed   += nFilesFailed
                        totalTobeDone += (nFiles-nFilesUsed)
                    # collect PandaIDs
                    self.conn.begin()
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID'] = datasetID
                    self.cur.execute(sqlP+comment, varMap)
                    resP = self.cur.fetchall()
                    # commit
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                    for tmpPandaID,tmpNumFiles in resP:
                        if not unmergeFlag:
                            if not tmpPandaID in retDict['PandaID']:
                                retDict['PandaID'].append(tmpPandaID)
                        else:
                            if not tmpPandaID in retDict['mergePandaID']:
                                retDict['mergePandaID'].append(tmpPandaID)
                        # map to job status
                        if datasetType in ['input','pseudo_input']:
                            if tmpPandaID in jobStatPandaIDs:
                                tmpJobStatus = jobStatPandaIDs[tmpPandaID]
                                if not tmpJobStatus in totalStatMap:
                                    totalStatMap[tmpJobStatus] = 0 
                                totalStatMap[tmpJobStatus] += tmpNumFiles
                # output
                if datasetType.endswith('output') or datasetType.endswith('log'):
                    # ignore transient datasets
                    if 'trn_' in datasetType:
                        continue
                    # use container name if not empty
                    if not containerName in [None,'']:
                        targetName = containerName
                    else:
                        targetName = datasetName
                    if not targetName in outDSs:
                        outDSs.append(targetName)
                        retDict['outDS'] += '{0},'.format(targetName)
            retDict['inDS'] = retDict['inDS'][:-1]
            retDict['outDS'] = retDict['outDS'][:-1]
            # statistics
            statStr = ''
            nPicked = totalNumFiles
            if totalTobeDone > 0:
                statStr += 'tobedone*{0},'.format(totalTobeDone)
                nPicked -= totalTobeDone
            if totalFinished > 0:
                statStr += 'finished*{0},'.format(totalFinished)
                nPicked -= totalFinished
            if totalFailed > 0:
                statStr += 'failed*{0},'.format(totalFailed)
                nPicked -= totalFailed
            for tmpJobStatus,tmpNumFiles in totalStatMap.iteritems():
                # skip active failed
                if tmpJobStatus == 'failed':
                    continue
                statStr += '{0}*{1},'.format(tmpJobStatus,tmpNumFiles)
                nPicked -= tmpNumFiles
            if nPicked > 0:
                statStr += 'picked*{0},'.format(nPicked)
            retDict['statistics'] = statStr[:-1]
            # command line parameters
            if fullFlag:
                # sql to read task params
                sql = "SELECT taskParams FROM {0}.JEDI_TaskParams WHERE jediTaskID=:jediTaskID ".format(panda_config.schemaJEDI)
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                # begin transaction
                self.conn.begin()
                self.cur.execute(sql+comment,varMap)
                retStr = ''
                for tmpItem, in self.cur:
                    try:
                        retStr = tmpItem.read()
                    except AttributeError:
                        retStr = str(tmpItem)
                    break
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                # decode json
                taskParamsJson = json.loads(retStr)
                if 'cliParams' in taskParamsJson:
                    retDict['cliParams'] = taskParamsJson['cliParams']
                else:
                    retDict['cliParams'] = ''
            retDict['PandaID'].sort()
            _logger.debug("{0} : {1}".format(methodName,str(retDict)))
            return retDict
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return {}



    # make event range ID for event service
    def makeEventRangeID(self,jediTaskID,pandaID,fileID,job_processID,attemptNr):
        return '{0}-{1}-{2}-{3}-{4}'.format(jediTaskID,pandaID,
                                            fileID,job_processID,
                                            attemptNr)

                         

    # get a list of even ranges for a PandaID
    def getEventRanges(self,pandaID,jobsetID,jediTaskID,nRanges):
        comment = ' /* DBProxy.getEventRanges */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0} jobsetID={1} jediTaskID={2}>".format(pandaID,jobsetID,jediTaskID)
        _logger.debug("{0} : start nRanges={1}".format(methodName,nRanges))
        try:
            # sql to get ranges
            sql  = 'SELECT * FROM ('
            sql += 'SELECT jediTaskID,datasetID,fileID,attemptNr,job_processID,def_min_eventID,def_max_eventID '
            sql += "FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sql += "WHERE PandaID=:jobsetID AND status=:eventStatus AND attemptNr>:minAttemptNr "
            sql += "ORDER BY def_min_eventID "
            sql += ") WHERE rownum<={0} ".format(nRanges)
            # sql to get file info
            sqlF  = "SELECT lfn,GUID,scope FROM {0}.JEDI_Dataset_Contents ".format(panda_config.schemaJEDI)
            sqlF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID " 
            # sql to lock range
            sqlU  = "UPDATE {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlU += "SET PandaID=:pandaID,status=:eventStatus "
            sqlU += "WHERE jediTaskID=:jediTaskID AND fileID=:fileID AND PandaID=:jobsetID "
            sqlU += "AND job_processID=:job_processID AND attemptNr=:attemptNr "
            sqlU += "AND status=:oldEventStatus "
            varMap = {}
            varMap[':jobsetID'] = jobsetID
            varMap[':eventStatus']  = EventServiceUtils.ST_ready
            varMap[':minAttemptNr'] = 0
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 100000
            _logger.debug(sql+comment+str(varMap))                
            self.cur.execute(sql+comment, varMap)
            resList = self.cur.fetchall()
            # make dict
            fileInfo = {}
            retRanges = []
            for jediTaskID,datasetID,fileID,attemptNr,job_processID,startEvent,lastEvent in resList:
                # get file info
                if not fileID in fileInfo:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID'] = datasetID
                    varMap[':fileID'] = fileID
                    self.cur.execute(sqlF+comment, varMap)
                    resF = self.cur.fetchone()
                    # not found
                    if resF == None:
                        resF = (None,None)
                        _logger.warning("{0} : file info is not found for fileID={1}".format(methodName,fileID))
                    fileInfo[fileID] = resF
                # get LFN and GUID
                tmpLFN,tmpGUID,tmpScope = fileInfo[fileID]
                if tmpLFN == None:
                    continue
                # make dict
                tmpDict = {'eventRangeID':self.makeEventRangeID(jediTaskID,pandaID,
                                                                fileID,job_processID,
                                                                attemptNr),
                           'startEvent':startEvent,
                           'lastEvent':lastEvent,
                           'LFN':tmpLFN,
                           'GUID':tmpGUID,
                           'scope':tmpScope}
                # lock
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':fileID'] = fileID
                varMap[':job_processID'] = job_processID
                varMap[':pandaID'] = pandaID
                varMap[':jobsetID'] = jobsetID
                varMap[':attemptNr'] = attemptNr
                varMap[':eventStatus'] = EventServiceUtils.ST_sent
                varMap[':oldEventStatus'] = EventServiceUtils.ST_ready
                _logger.debug(sqlU+comment+str(varMap))
                self.cur.execute(sqlU+comment, varMap)
                nRow = self.cur.rowcount
                if nRow != 1:
                    # failed to lock
                    _logger.debug("{0} : failed to lock {1} with nRow={2}".format(methodName,tmpDict['eventRangeID'],
                                                                                  nRow))
                else:
                    # append
                    retRanges.append(tmpDict)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # kill unused consumers
            if retRanges == [] and jediTaskID != None:
                tmpJobSpec = JobSpec()
                tmpJobSpec.PandaID = pandaID
                tmpJobSpec.jobsetID = jobsetID
                tmpJobSpec.jediTaskID = jediTaskID
                self.killUnusedEventServiceConsumers(tmpJobSpec,True)
            _logger.debug("{0} : {1}".format(methodName,str(retRanges)))
            return json.dumps(retRanges)
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # get a list of even ranges for a PandaID
    def updateEventRange(self,eventRangeID,eventStatus,coreCount,cpuConsumptionTime):
        comment = ' /* DBProxy.updateEventRange */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <eventRangeID={0}>".format(eventRangeID)
        _logger.debug("{0} : start status={1} coreCount={2} cpuConsumptionTime={3}".format(methodName,eventStatus,
                                                                                           coreCount,cpuConsumptionTime))
        try:
            # sql to update status
            sqlU  = "UPDATE {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlU += "SET status=:eventStatus"
            sqlU += " WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND fileID=:fileID "
            sqlU += "AND job_processID=:job_processID AND attemptNr=:attemptNr "
            # sql to get event range
            sqlC  = "SELECT def_min_eventID,def_max_eventID FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlC += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND fileID=:fileID "
            sqlC += "AND job_processID=:job_processID "
            # sql to get nEvents
            sqlE  = "SELECT jobStatus,nEvents FROM ATLAS_PANDA.jobsActive4 "
            sqlE += "WHERE PandaID=:pandaID "
            # sql to set nEvents
            sqlS  = "UPDATE ATLAS_PANDA.jobsActive4 "
            sqlS += "SET nEvents=:nEvents "
            sqlS += "WHERE PandaID=:pandaID "
            # sql to set CPU consumption
            sqlT  = "UPDATE ATLAS_PANDA.jobsActive4 "
            sqlT += "SET cpuConsumptionTime=cpuConsumptionTime+:actualCpuTime "
            sqlT += "WHERE PandaID=:PandaID "
            # decompose eventRangeID
            try:
                tmpItems = eventRangeID.split('-')
                jediTaskID,pandaID,fileID,job_processID,attemptNr = tmpItems
                jediTaskID = long(jediTaskID)
                pandaID = long(pandaID)
                fileID = long(fileID)
                job_processID = long(job_processID)
                attemptNr = long(attemptNr)
            except:
                _logger.error("{0} : wrongly formatted eventRangeID".format(methodName))
                return False
            # map string status to int
            if eventStatus == 'running':
                intEventStatus = EventServiceUtils.ST_running
            elif eventStatus == 'finished':
                intEventStatus = EventServiceUtils.ST_finished
            elif eventStatus == 'failed':
                intEventStatus = EventServiceUtils.ST_failed
            else:
                _logger.error("{0} : unknown status".format(methodName))
                return False
            # start transaction
            self.conn.begin()
            nRow = 0
            # get jobStatus and nEvents
            varMap = {}
            varMap[':pandaID'] = pandaID
            self.cur.execute(sqlE+comment, varMap)
            resE = self.cur.fetchone()
            if resE == None:
                _logger.debug("{0} : unknown PandaID".format(methodName))
            else:
                # check job status
                jobStatus,nEventsOld = resE
                if not jobStatus in ['sent','running','starting']:
                    _logger.debug("{0} : wrong jobStatus={1}".format(methodName,jobStatus))
                else:
                    # update event
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':pandaID'] = pandaID
                    varMap[':fileID'] = fileID
                    varMap[':job_processID'] = job_processID
                    varMap[':attemptNr'] = attemptNr
                    varMap[':eventStatus'] = intEventStatus
                    self.cur.execute(sqlU+comment, varMap)
                    nRow = self.cur.rowcount
                    if nRow == 1 and eventStatus in ['finished']:
                        # get event range
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':pandaID'] = pandaID
                        varMap[':fileID'] = fileID
                        varMap[':job_processID'] = job_processID
                        self.cur.execute(sqlC+comment, varMap)
                        resC = self.cur.fetchone()
                        if resC != None:
                            minEventID,maxEventID = resC
                            nEvents = maxEventID-minEventID+1
                            if nEventsOld != None:
                                nEvents += nEventsOld
                            # update nevents
                            varMap = {}
                            varMap[':pandaID'] = pandaID
                            varMap[':nEvents'] = nEvents
                            self.cur.execute(sqlS+comment, varMap)
                    # update cpuConsumptionTime
                    if cpuConsumptionTime != None and eventStatus in ['finished','failed']:
                        varMap = {}
                        varMap[':PandaID'] = pandaID
                        if coreCount == None:
                            varMap[':actualCpuTime'] = long(cpuConsumptionTime)
                        else:
                            varMap[':actualCpuTime'] = long(coreCount) * long(cpuConsumptionTime)
                        self.cur.execute(sqlT+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} : done with nRow={1}".format(methodName,nRow))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False



    # post-process for event service job
    def ppEventServiceJob(self,job,useCommit=True):
        comment = ' /* DBProxy.ppEventServiceJob */'
        pandaID = job.PandaID
        attemptNr = job.attemptNr
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(pandaID)
        _logger.debug("{0} : start attemptNr={1}".format(methodName,attemptNr))
        try:
            # return values
            # 0 : generated a retry job
            # 1 : not retried due to a harmless reason
            # 2 : generated a merge job
            # 3 : max attempts reached
            # 4 : not generated a merge job since other consumers are still running
            # 5 : didn't process any events on WN
            # 6 : didn't process any events on WN and fail since the last one
            # 7 : all event ranges failed
            # None : fatal error
            retValue = 1,None
            # begin transaction
            if useCommit:
                self.conn.begin()
            self.cur.arraysize = 10                
            # make job spec to not change the original
            jobSpec = copy.copy(job)
            jobSpec.Files = []
            # check if event service job
            if not EventServiceUtils.isEventServiceJob(jobSpec):
                _logger.debug("{0} : no event service job".format(methodName))
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                return retValue
            # check if already retried
            if jobSpec.taskBufferErrorCode in [ErrorCode.EC_EventServiceRetried,ErrorCode.EC_EventServiceMerge]:
                _logger.debug("{0} : already post-processed for event service with EC={1}".format(methodName,jobSpec.taskBufferErrorCode))
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                return retValue
            # check if JEDI is used
            if hasattr(panda_config,'useJEDI') and panda_config.useJEDI == True and \
                    jobSpec.lockedby == 'jedi' and self.checkTaskStatusJEDI(jobSpec.jediTaskID,self.cur):
                pass
            else:
                _logger.debug("{0} : JEDI is not used".format(methodName))
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                return retValue
            # change status to done
            sqlED  = "UPDATE {0}.JEDI_Events SET status=:newStatus ".format(panda_config.schemaJEDI)
            sqlED += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND status=:oldStatus "
            varMap = {}
            varMap[':jediTaskID'] = jobSpec.jediTaskID
            varMap[':pandaID']    = pandaID
            varMap[':oldStatus']  = EventServiceUtils.ST_finished
            varMap[':newStatus']  = EventServiceUtils.ST_done
            self.cur.execute(sqlED+comment, varMap)
            nRowDone = self.cur.rowcount
            _logger.debug("{0} : set done to {1} event ranges".format(methodName,nRowDone))
            # release unprocessed event ranges
            sqlEC  = "UPDATE {0}.JEDI_Events SET status=:newStatus,attemptNr=attemptNr-1,pandaID=:jobsetID ".format(panda_config.schemaJEDI)
            sqlEC += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND NOT status IN (:esDone,:esFailed) "
            varMap = {}
            varMap[':jediTaskID']  = jobSpec.jediTaskID
            varMap[':pandaID']     = pandaID
            varMap[':jobsetID']    = jobSpec.jobsetID
            varMap[':esDone']      = EventServiceUtils.ST_done
            varMap[':esFailed']    = EventServiceUtils.ST_failed
            varMap[':newStatus']   = EventServiceUtils.ST_ready
            self.cur.execute(sqlEC+comment, varMap)
            nRowReleased = self.cur.rowcount
            _logger.debug("{0} : released {1} event ranges".format(methodName,nRowReleased))
            # copy failed event ranges
            varMap = {}
            varMap[':jediTaskID']  = jobSpec.jediTaskID
            varMap[':pandaID']     = pandaID
            varMap[':jobsetID']    = jobSpec.jobsetID
            varMap[':esFailed']    = EventServiceUtils.ST_failed
            varMap[':newStatus']   = EventServiceUtils.ST_ready
            sqlEF  = "INSERT INTO {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlEF += "(jediTaskID,datasetID,PandaID,fileID,attemptNr,status,"
            sqlEF += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID) "
            sqlEF += "SELECT jediTaskID,datasetID,:jobsetID,fileID,attemptNr-1,:newStatus,"
            sqlEF += "job_processID,def_min_eventID,def_max_eventID,processed_upto_eventID "
            sqlEF += "FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlEF += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND status=:esFailed "
            self.cur.execute(sqlEF+comment, varMap)
            nRowCopied = self.cur.rowcount
            _logger.debug("{0} : copied {1} failed event ranges".format(methodName,nRowCopied))
            # unset processed_upto for failed events
            sqlUP  = "UPDATE {0}.JEDI_Events SET processed_upto_eventID=NULL ".format(panda_config.schemaJEDI)
            sqlUP += "WHERE jediTaskID=:jediTaskID AND pandaID=:pandaID AND status=:esFailed "
            varMap = {}
            varMap[':jediTaskID']  = jobSpec.jediTaskID
            varMap[':pandaID']     = pandaID
            varMap[':esFailed']    = EventServiceUtils.ST_failed
            self.cur.execute(sqlUP+comment, varMap)
            nRowFailed = self.cur.rowcount
            _logger.debug("{0} : failed {1} event ranges".format(methodName,nRowFailed))
            # look for hopeless event ranges
            sqlEU  = "SELECT COUNT(*) FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlEU += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID AND attemptNr=:minAttempt AND rownum=1 "
            varMap = {}
            varMap[':jediTaskID'] = jobSpec.jediTaskID
            varMap[':jobsetID']   = jobSpec.jobsetID
            varMap[':minAttempt'] = 0
            self.cur.execute(sqlEU+comment, varMap)
            resEU = self.cur.fetchone()
            nRowFatal, = resEU
            # there is hopeless event ranges
            if nRowFatal != 0:
                if not jobSpec.acceptPartialFinish():
                    # fail immediately
                    _logger.debug("{0} : no more retry since reached max number of reattempts for some event ranges".format(methodName))
                    # commit
                    if useCommit:
                        if not self._commit():
                            raise RuntimeError, 'Commit error'
                    retValue = 3,None
                    return retValue
                else:
                    # set fatal to hopeless event ranges
                    sqlFH  = "UPDATE {0}.JEDI_Events SET status=:esFatal ".format(panda_config.schemaJEDI)
                    sqlFH += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID AND attemptNr=:minAttempt AND status<>:esFatal "
                    varMap = {}
                    varMap[':jediTaskID']  = jobSpec.jediTaskID
                    varMap[':jobsetID']    = jobSpec.jobsetID
                    varMap[':esFatal']     = EventServiceUtils.ST_fatal
                    varMap[':minAttempt']  = 0
                    self.cur.execute(sqlFH+comment, varMap)
            # look for event ranges to process
            sqlERP  = "SELECT COUNT(*) FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlERP += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID AND status=:esReady "
            sqlERP += "AND attemptNr>:minAttempt AND rownum=1 "
            varMap = {}
            varMap[':jediTaskID']  = jobSpec.jediTaskID
            varMap[':jobsetID']    = jobSpec.jobsetID
            varMap[':esReady']     = EventServiceUtils.ST_ready
            varMap[':minAttempt']  = 0
            self.cur.execute(sqlERP+comment, varMap)
            resERP = self.cur.fetchone()
            nRow, = resERP
            _logger.debug("{0} : {1} unprocessed event ranges remain".format(methodName,nRow))
            otherRunning = False
            hasDoneRange = False
            if nRow == 0:
                # check if other consumers finished
                sqlEOC  = "SELECT COUNT(*) FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
                sqlEOC += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlEOC += "AND ((NOT status IN (:esDone,:esDiscarded,:esCancelled,:esFatal,:esFailed)) "
                sqlEOC += "OR (status=:esFailed AND processed_upto_eventID IS NOT NULL)) AND rownum=1 "
                # count the number of done ranges
                sqlCDO  = "SELECT COUNT(*) FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
                sqlCDO += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
                sqlCDO += "AND status=:esDone AND rownum=1 "
                for fileSpec in job.Files:
                    if fileSpec.type == 'input':
                        varMap = {}
                        varMap[':jediTaskID']  = fileSpec.jediTaskID
                        varMap[':datasetID']   = fileSpec.datasetID
                        varMap[':fileID']      = fileSpec.fileID
                        varMap[':esDone']      = EventServiceUtils.ST_done
                        varMap[':esDiscarded'] = EventServiceUtils.ST_discarded
                        varMap[':esCancelled'] = EventServiceUtils.ST_cancelled
                        varMap[':esFatal']     = EventServiceUtils.ST_fatal
                        varMap[':esFailed']    = EventServiceUtils.ST_failed
                        _logger.debug(sqlEOC+comment+str(varMap))
                        self.cur.execute(sqlEOC+comment, varMap)
                        resEOC = self.cur.fetchone()
                        nOCRow, = resEOC
                        if nOCRow != 0:
                            # there are unprocessed ranges
                            otherRunning = True
                            _logger.debug("{0} : {1} event ranges still running".format(methodName,nOCRow))
                            break
                        # check if there are done ranges
                        if not hasDoneRange:
                            varMap = {}
                            varMap[':jediTaskID']  = fileSpec.jediTaskID
                            varMap[':datasetID']   = fileSpec.datasetID
                            varMap[':fileID']      = fileSpec.fileID
                            varMap[':esDone']      = EventServiceUtils.ST_done
                            self.cur.execute(sqlCDO+comment, varMap)
                            resCDO = self.cur.fetchone()
                            nCDORow, = resCDO
                            if nCDORow != 0:
                                hasDoneRange = True
                # do merging since all ranges were done
                if not otherRunning:
                    doMerging = True
            else:
                doMerging = False
            # do nothing since other consumers are still running
            if otherRunning:
                _logger.debug("{0} : do nothing as other consumers are still running".format(methodName))
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                retValue = 4,None
                return retValue
            # all failed
            if doMerging and not hasDoneRange:
                # fail immediately
                _logger.debug("{0} : all event ranges failed".format(methodName))
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                retValue = 7,None
                return retValue
            # fail immediately if didn't do anything with the largest attemptNr
            if nRowDone == 0 and nRowReleased == 0 and jobSpec.attemptNr >= jobSpec.maxAttempt and \
                    not (doMerging and hasDoneRange):
                _logger.debug("{0} : no more retry since did't do anything with the largest attemptNr".format(methodName))
                # check if there is active consumer
                sqlAC  = "SELECT COUNT(*) FROM ATLAS_PANDA.jobsActive4 "
                sqlAC += "WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
                varMap = {}
                varMap[':jediTaskID'] = jobSpec.jediTaskID
                varMap[':jobsetID']   = jobSpec.jobsetID
                self.cur.execute(sqlAC+comment, varMap)
                resAC = self.cur.fetchone()
                numActiveEC, = resAC
                _logger.debug("{0} : num of active consumers = {1}".format(methodName,numActiveEC))
                # commit
                if useCommit:
                    if not self._commit():
                        raise RuntimeError, 'Commit error'
                if numActiveEC == 1:
                    # last one
                    retValue = 6,None
                else:
                    # there are active consumers
                    retValue = 5,None
                return retValue
            # check if there is fatal range
            hasFatalRange = False
            if doMerging:
                sqlCFE  = "SELECT COUNT(*) FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
                sqlCFE += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID AND "
                sqlCFE += "status=:esFatal AND rownum=1 "
                varMap = {}
                varMap[':jediTaskID']  = jobSpec.jediTaskID
                varMap[':jobsetID']    = jobSpec.jobsetID
                varMap[':esFatal']     = EventServiceUtils.ST_fatal
                self.cur.execute(sqlCFE+comment, varMap)
                resCFE = self.cur.fetchone()
                nRowCEF, = resCFE
                _logger.debug("{0} : {1} fatal even ranges ".format(methodName,nRowCEF))
                if nRowCEF > 0:
                    hasFatalRange = True
            # reset job attributes
            jobSpec.jobStatus        = 'activated'
            jobSpec.startTime        = None
            jobSpec.creationTime     = datetime.datetime.utcnow()
            jobSpec.modificationTime = jobSpec.creationTime
            jobSpec.attemptNr       += 1
            if doMerging:
                jobSpec.maxAttempt = jobSpec.attemptNr+3
                jobSpec.currentPriority = 5000
            else:
                jobSpec.currentPriority += 1
            jobSpec.endTime          = None
            jobSpec.transExitCode    = None
            jobSpec.jobMetrics       = None
            jobSpec.jobSubStatus     = None
            if hasFatalRange:
                jobSpec.jobSubStatus = 'partial'
            for attr in jobSpec._attributes:
                if attr.endswith('ErrorCode') or attr.endswith('ErrorDiag'):
                    setattr(jobSpec,attr,None)
            # read files
            varMap = {}
            varMap[':PandaID'] = pandaID
            sqlFile  = "SELECT {0} FROM ATLAS_PANDA.filesTable4 ".format(FileSpec.columnNames())
            sqlFile += "WHERE PandaID=:PandaID"
            self.cur.arraysize = 100000
            self.cur.execute(sqlFile+comment, varMap)
            resFs = self.cur.fetchall()
            # loop over all files            
            for resF in resFs:
                # add
                fileSpec = FileSpec()
                fileSpec.pack(resF)
                jobSpec.addFile(fileSpec)
                # reset file status
                if fileSpec.type in ['output','log']:
                    fileSpec.status = 'unknown'
            # read job parameters
            sqlJobP = "SELECT jobParameters FROM ATLAS_PANDA.jobParamsTable WHERE PandaID=:PandaID"
            varMap = {}
            varMap[':PandaID'] = jobSpec.PandaID
            self.cur.execute(sqlJobP+comment, varMap)
            for clobJobP, in self.cur:
                try:
                    jobSpec.jobParameters = clobJobP.read()
                except AttributeError:
                    jobSpec.jobParameters = str(clobJobP)
                break
            # changes some attributes for merging
            if doMerging:
                # extract parameters for merge
                try:
                    tmpMatch = re.search('<PANDA_EVSMERGE>(.*)</PANDA_EVSMERGE>',jobSpec.jobParameters)
                    jobSpec.jobParameters = tmpMatch.group(1)
                except:
                    pass
                # change special handling
                EventServiceUtils.setEventServiceMerge(jobSpec)
                # check where merge is done
                sqlWM  = "SELECT catchAll FROM ATLAS_PANDAMETA.schedconfig WHERE siteid=:siteid "
                varMap = {}
                varMap[':siteid'] = jobSpec.computingSite
                self.cur.execute(sqlWM+comment, varMap)
                resWM = self.cur.fetchone()
                if resWM != None and resWM[0] != None and 'localEsMerge' in resWM[0]:
                    # run merge jobs at the same site
                    pass
                else:
                    # run merge jobs at destination 
                    jobSpec.computingSite = jobSpec.destinationSE
                jobSpec.coreCount = None
            # insert job with new PandaID
            sql1  = "INSERT INTO ATLAS_PANDA.jobsActive4 ({0}) ".format(JobSpec.columnNames())
            sql1 += JobSpec.bindValuesExpression(useSeq=True)
            sql1 += " RETURNING PandaID INTO :newPandaID"
            # set parentID
            jobSpec.parentID = jobSpec.PandaID
            varMap = jobSpec.valuesMap(useSeq=True)
            varMap[':newPandaID'] = self.cur.var(varNUMBER)
            # insert
            retI = self.cur.execute(sql1+comment, varMap)
            # set PandaID
            jobSpec.PandaID = long(self.cur.getvalue(varMap[':newPandaID']))
            msgStr = '{0} Generate new PandaID -> {1}#{2} '.format(methodName,jobSpec.PandaID,jobSpec.attemptNr)
            if doMerging:
                msgStr += "for merge"
            else:
                msgStr += "for retry"
            _logger.debug(msgStr)
            # insert files
            sqlFile = "INSERT INTO ATLAS_PANDA.filesTable4 ({0}) ".format(FileSpec.columnNames())
            sqlFile+= FileSpec.bindValuesExpression(useSeq=True)
            sqlFile+= " RETURNING row_ID INTO :newRowID"
            for fileSpec in jobSpec.Files:
                # reset rowID
                fileSpec.row_ID = None
                # change GUID and LFN for log
                if fileSpec.type == 'log':
                    fileSpec.GUID = commands.getoutput('uuidgen')
                    if doMerging:
                        fileSpec.lfn = re.sub('\.{0}$'.format(pandaID),''.format(jobSpec.PandaID),fileSpec.lfn)
                    else:
                        fileSpec.lfn = re.sub('\.{0}$'.format(pandaID),'.{0}'.format(jobSpec.PandaID),fileSpec.lfn)
                # insert
                varMap = fileSpec.valuesMap(useSeq=True)
                varMap[':newRowID'] = self.cur.var(varNUMBER)
                self.cur.execute(sqlFile+comment, varMap)
                fileSpec.row_ID = long(self.cur.getvalue(varMap[':newRowID']))
            # insert job parameters
            sqlJob = "INSERT INTO ATLAS_PANDA.jobParamsTable (PandaID,jobParameters) VALUES (:PandaID,:param)"
            varMap = {}
            varMap[':PandaID'] = jobSpec.PandaID
            varMap[':param']   = jobSpec.jobParameters
            self.cur.execute(sqlJob+comment, varMap)
            # propagate change to JEDI
            self.updateForPilotRetryJEDI(jobSpec,self.cur,onlyHistory=True)
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            # set return
            if not doMerging:
                retValue = 0,jobSpec.PandaID
            else:
                retValue = 2,jobSpec.PandaID
            # record status change
            try:
                self.recordStatusChange(jobSpec.PandaID,jobSpec.jobStatus,jobInfo=jobSpec,useCommit=useCommit)
            except:
                _logger.error('recordStatusChange in ppEventServiceJob')
            _logger.debug('{0} done for doMergeing={1}'.format(methodName,doMerging))
            return retValue
        except:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None,None



    # change task priority
    def changeTaskPriorityPanda(self,jediTaskID,newPriority):
        comment = ' /* DBProxy.changeTaskPriorityPanda */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        _logger.debug("{0} newPrio={1}".format(methodName,newPriority))
        try:
            # sql to update JEDI task table
            sqlT  = 'UPDATE {0}.JEDI_Tasks SET currentPriority=:newPriority WHERE jediTaskID=:jediTaskID '.format(panda_config.schemaJEDI)
            # sql to update DEFT task table
            schemaDEFT = self.getSchemaDEFT()
            sqlD  = 'UPDATE {0}.T_TASK SET current_priority=:newPriority,timestamp=CURRENT_DATE WHERE taskid=:jediTaskID '.format(schemaDEFT)
            # update job priorities
            sqlJ  = 'UPDATE ATLAS_PANDA.{0} SET currentPriority=:newPriority WHERE jediTaskID=:jediTaskID '
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[':jediTaskID']  = jediTaskID
            varMap[':newPriority'] = newPriority
            # update JEDI
            self.cur.execute(sqlT+comment, varMap)
            nRow = self.cur.rowcount
            if nRow == 1:
                # update jobs
                for tableName in ['jobsActive4','jobsDefined4','jobsWaiting4']:
                    self.cur.execute(sqlJ.format(tableName)+comment, varMap)
            # update DEFT
            self.cur.execute(sqlD+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} done with {1}".format(methodName,nRow))
            return nRow
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # get WAN data flow matrix
    def getWanDataFlowMaxtrix(self):
        comment = ' /* DBProxy.getWanDataFlowMaxtrix */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        _logger.debug("{0} start".format(methodName))
        try:
            # sql to get data flow
            sqlT  = 'SELECT PandaID,jobStatus,prodUserName,computingSite,sourceSite,maxCpuCount,inputFileBytes,currentPriority '
            sqlT += 'FROM ATLAS_PANDA.jobsActive4 '
            sqlT += 'WHERE prodSourceLabel=:prodSourceLabel '
            sqlT += 'AND jobStatus IN (:jobStatus1,:jobStatus2,:jobStatus3,:jobStatus4,:jobStatus5) '
            sqlT += 'AND transferType=:transferType '
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 100000
            varMap = {}
            varMap[':jobStatus1']   = 'activated'
            varMap[':jobStatus2']   = 'running'
            varMap[':jobStatus3']   = 'throttled'
            varMap[':jobStatus4']   = 'sent'
            varMap[':jobStatus5']   = 'starting'
            varMap[':transferType'] = 'fax'
            varMap[':prodSourceLabel'] = 'user'
            # get data
            self.cur.execute(sqlT+comment,varMap)
            resFs = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            # loop over all data
            retMap = {}
            for pandaID,jobStatus,prodUserName,computingSite,sourceSite,maxCpuCount,inputFileBytes,currentPriority in resFs:
                # add sink
                if not retMap.has_key(computingSite):
                    retMap[computingSite] = {}
                # add source
                if not retMap[computingSite].has_key(sourceSite):
                    retMap[computingSite][sourceSite] = {'flow':0,'user':{}}
                # add user
                if not retMap[computingSite][sourceSite]['user'].has_key(prodUserName):
                    retMap[computingSite][sourceSite]['user'][prodUserName] = {'activated':{'nJobs':0,'jobList':{}},
                                                                               'throttled':{'nJobs':0,'jobList':{}}
                                                                               }
                # action for each jobStatus
                if jobStatus in ['activated','throttled']:
                    # collect PandaIDs
                    if not retMap[computingSite][sourceSite]['user'][prodUserName][jobStatus]['jobList'].has_key(currentPriority):
                       retMap[computingSite][sourceSite]['user'][prodUserName][jobStatus]['jobList'][currentPriority] = []
                    retMap[computingSite][sourceSite]['user'][prodUserName][jobStatus]['jobList'][currentPriority].append(pandaID)
                    # number of jobs
                    retMap[computingSite][sourceSite]['user'][prodUserName][jobStatus]['nJobs'] += 1
                else:
                    # calcurate total flow in bps
                    if maxCpuCount in [0,None]:
                        # use the default data flow
                        dataFlow = 1024*8
                    else:
                        dataFlow = inputFileBytes*8/maxCpuCount
                    retMap[computingSite][sourceSite]['flow'] += dataFlow
            _logger.debug("{0} done".format(methodName))
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return {}



    # throttle job
    def throttleJob(self,pandaID):
        comment = ' /* DBProxy.throttleJob */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(pandaID)
        _logger.debug("{0} start".format(methodName))
        try:
            # sql to update job
            sqlT  = 'UPDATE ATLAS_PANDA.jobsActive4 SET currentPriority=assignedPriority,jobStatus=:newJobStatus '
            sqlT += 'WHERE PandaID=:PandaID AND jobStatus=:oldJobStatus '
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[':PandaID']      = pandaID
            varMap[':newJobStatus'] = 'throttled'
            varMap[':oldJobStatus'] = 'activated'
            # get datasets
            self.cur.execute(sqlT+comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            try:
                self.recordStatusChange(pandaID,varMap[':newJobStatus'])
            except:
                _logger.error('recordStatusChange in throttleJob')
            _logger.debug("{0} done with {1}".format(methodName,nRow))
            return nRow
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # unthrottle job
    def unThrottleJob(self,pandaID):
        comment = ' /* DBProxy.unThrottleJob */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(pandaID)
        _logger.debug("{0} start".format(methodName))
        try:
            # sql to update job
            sqlT  = 'UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:newJobStatus '
            sqlT += 'WHERE PandaID=:PandaID AND jobStatus=:oldJobStatus '
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[':PandaID']      = pandaID
            varMap[':newJobStatus'] = 'activated'
            varMap[':oldJobStatus'] = 'throttled'
            # get datasets
            self.cur.execute(sqlT+comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            try:
                self.recordStatusChange(pandaID,varMap[':newJobStatus'])
            except:
                _logger.error('recordStatusChange in unThrottleJob')
            _logger.debug("{0} done with {1}".format(methodName,nRow))
            return nRow
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # kill active consumers related to an ES job
    def killEventServiceConsumers(self,job,killedFlag,useCommit=True):
        comment = ' /* DBProxy.killEventServiceConsumers */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(job.PandaID)
        _logger.debug("{0} : start".format(methodName))
        try:
            # begin transaction
            if useCommit:
                self.conn.begin()
            # sql to get consumers
            sqlCP  = "SELECT distinct PandaID FROM {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlCP += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlCP += "AND NOT status IN (:esDiscarded,:esCancelled) "
            # sql to discard or cancel event ranges
            sqlDE  = "UPDATE {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlDE += "SET status=:status "
            sqlDE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlDE += "AND status IN (:esFinished,:esDone) "
            sqlCE  = "UPDATE {0}.JEDI_Events ".format(panda_config.schemaJEDI)
            sqlCE += "SET status=:status "
            sqlCE += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
            sqlCE += "AND NOT status IN (:esFinished,:esDone,:esDiscarded,:esCancelled,:esFailed,:esFatal) "
            # look for consumers for each input
            killPandaIDs = []
            for fileSpec in job.Files:
                if fileSpec.type != 'input':
                    continue
                # get PandaIDs
                varMap = {}
                varMap[':jediTaskID']  = fileSpec.jediTaskID
                varMap[':datasetID']   = fileSpec.datasetID
                varMap[':fileID']      = fileSpec.fileID
                varMap[':esDiscarded'] = EventServiceUtils.ST_discarded
                varMap[':esCancelled'] = EventServiceUtils.ST_cancelled
                self.cur.arraysize = 100000
                self.cur.execute(sqlCP+comment, varMap)
                resPs = self.cur.fetchall()
                for esPandaID, in resPs:
                    if not esPandaID in killPandaIDs:
                        killPandaIDs.append(esPandaID)
                # discard event ranges
                if resPs != []:
                    varMap = {}
                    varMap[':jediTaskID'] = fileSpec.jediTaskID
                    varMap[':datasetID']  = fileSpec.datasetID
                    varMap[':fileID']     = fileSpec.fileID
                    varMap[':status']     = EventServiceUtils.ST_discarded
                    varMap[':esFinished'] = EventServiceUtils.ST_finished
                    varMap[':esDone']     = EventServiceUtils.ST_done
                    self.cur.execute(sqlDE+comment, varMap)
                    varMap[':status']     = EventServiceUtils.ST_cancelled
                    varMap[':esDiscarded'] = EventServiceUtils.ST_discarded
                    varMap[':esCancelled'] = EventServiceUtils.ST_cancelled
                    varMap[':esFatal']     = EventServiceUtils.ST_fatal
                    varMap[':esFailed']    = EventServiceUtils.ST_failed
                    self.cur.execute(sqlCE+comment, varMap)
            # kill consumers
            sqlDJS = "SELECT %s " % JobSpec.columnNames()
            sqlDJS+= "FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
            sqlDJD = "DELETE FROM ATLAS_PANDA.jobsActive4 WHERE PandaID=:PandaID"
            sqlDJI = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
            sqlDJI+= JobSpec.bindValuesExpression()
            sqlFSF  = "UPDATE ATLAS_PANDA.filesTable4 SET status=:newStatus "
            sqlFSF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
            sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            nKilled = 0
            for pandaID in killPandaIDs:
                # ignore original PandaID since it will be killed by caller
                if pandaID == job.PandaID:
                    continue
                # skip jobsetID
                if pandaID == job.jobsetID:
                    continue
                # read job
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.arraysize = 10
                self.cur.execute(sqlDJS+comment, varMap)
                resJob = self.cur.fetchall()
                if len(resJob) == 0:
                    continue
                _logger.debug("{0} : kill associated consumer {1}".format(methodName,pandaID))
                # instantiate JobSpec
                dJob = JobSpec()
                dJob.pack(resJob[0])
                # delete
                varMap = {}
                varMap[':PandaID'] = pandaID
                self.cur.execute(sqlDJD+comment, varMap)                            
                retD = self.cur.rowcount
                if retD == 0:
                    continue
                # set error code
                dJob.jobStatus = 'cancelled'
                dJob.endTime   = datetime.datetime.utcnow()
                dJob.taskBufferErrorCode = ErrorCode.EC_Kill
                if killedFlag:
                    dJob.taskBufferErrorDiag = 'killed since an associated consumer PandaID={0} was killed'.format(job.PandaID)
                else:
                    dJob.taskBufferErrorDiag = 'killed since an associated consumer PandaID={0} failed'.format(job.PandaID)
                dJob.modificationTime = dJob.endTime
                dJob.stateChangeTime  = dJob.endTime
                # insert
                self.cur.execute(sqlDJI+comment, dJob.valuesMap())
                # set file status
                varMap = {}
                varMap[':PandaID']   = pandaID
                varMap[':type1']     = 'output'
                varMap[':type2']     = 'log'
                varMap[':newStatus'] = 'failed'
                self.cur.execute(sqlFSF+comment,varMap)
                # update files,metadata,parametes
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':modificationTime'] = dJob.modificationTime
                self.cur.execute(sqlFMod+comment,varMap)
                self.cur.execute(sqlMMod+comment,varMap)
                self.cur.execute(sqlPMod+comment,varMap)
                nKilled += 1
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            _logger.debug('{0} killed {1} jobs'.format(methodName,nKilled))
            return True
        except:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False



    # kill unused consumers related to an ES job
    def killUnusedEventServiceConsumers(self,job,useCommit=True,killAll=False):
        comment = ' /* DBProxy.killUnusedEventServiceConsumers */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(job.PandaID)
        _logger.debug("{0} : start".format(methodName))
        try:
            # begin transaction
            if useCommit:
                self.conn.begin()
            # sql to get PandaIDs of consumers
            sqlCP  = "SELECT PandaID,specialHandling FROM ATLAS_PANDA.{0} "
            sqlCP += "WHERE jediTaskID=:jediTaskID AND jobsetID=:jobsetID "
            if killAll:
                sqlCP += "AND jobStatus IN (:st1,:st2,:st3,:st4,:st5,:st6,:st7,:st8) "
            else:
                sqlCP += "AND jobStatus IN (:st1,:st2,:st3,:st4) "
            # get PandaIDs
            varMap = {}
            varMap[':jediTaskID'] = job.jediTaskID
            varMap[':jobsetID']   = job.jobsetID
            varMap[':st1'] = 'activated'
            varMap[':st2'] = 'assigned'
            varMap[':st3'] = 'waiting'
            varMap[':st4'] = 'throttled'
            if killAll:
                varMap[':st5'] = 'running'
                varMap[':st6'] = 'holding'
                varMap[':st7'] = 'sent'
                varMap[':st8'] = 'starting'
            self.cur.arraysize = 100000
            killPandaIDsMap = {}
            for tableName in ['jobsActive4','jobsDefined4','jobsWaiting4']:
                self.cur.execute(sqlCP.format(tableName)+comment, varMap)
                resPs = self.cur.fetchall()
                killPandaIDs = []
                for esPandaID,specialHandling in resPs:
                    # ignore original PandaID since it will be managed by caller
                    if esPandaID == job.PandaID:
                        continue
                    # skip merge
                    if EventServiceUtils.isEventServiceMergeSH(specialHandling):
                        continue
                    # append
                    killPandaIDs.append(esPandaID)
                if killPandaIDs != []:
                    killPandaIDsMap[tableName] = killPandaIDs
            # kill consumers
            nKilled = 0
            sqlDJS = "SELECT %s " % JobSpec.columnNames()
            sqlDJS+= "FROM ATLAS_PANDA.{0} WHERE PandaID=:PandaID"
            sqlDJD = "DELETE FROM ATLAS_PANDA.{0} WHERE PandaID=:PandaID"
            sqlDJI = "INSERT INTO ATLAS_PANDA.jobsArchived4 (%s) " % JobSpec.columnNames()
            sqlDJI+= JobSpec.bindValuesExpression()
            sqlFSF  = "UPDATE ATLAS_PANDA.filesTable4 SET status=:newStatus "
            sqlFSF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
            sqlFMod = "UPDATE ATLAS_PANDA.filesTable4 SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlMMod = "UPDATE ATLAS_PANDA.metaTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            sqlPMod = "UPDATE ATLAS_PANDA.jobParamsTable SET modificationTime=:modificationTime WHERE PandaID=:PandaID"
            for tableName,killPandaIDs in killPandaIDsMap.iteritems():
                for pandaID in killPandaIDs:
                    _logger.debug("{0} : kill unused consumer {1}".format(methodName,pandaID))
                    # read job
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    self.cur.arraysize = 10
                    deletedFlag = False
                    for tableName in ['jobsActive4','jobsDefined4','jobsWaiting4']:
                        self.cur.execute(sqlDJS.format(tableName)+comment, varMap)
                        resJob = self.cur.fetchall()
                        if len(resJob) == 0:
                            continue
                        # instantiate JobSpec
                        dJob = JobSpec()
                        dJob.pack(resJob[0])
                        # delete
                        varMap = {}
                        varMap[':PandaID'] = pandaID
                        self.cur.execute(sqlDJD.format(tableName)+comment, varMap)                            
                        retD = self.cur.rowcount
                        if retD != 0:
                            deletedFlag = True
                            break
                    # not found
                    if not deletedFlag:
                        continue
                    # set error code
                    dJob.jobStatus = 'cancelled'
                    dJob.jobSubStatus = 'finished'
                    dJob.endTime   = datetime.datetime.utcnow()
                    dJob.taskBufferErrorCode = ErrorCode.EC_EventServiceUnused
                    dJob.taskBufferErrorDiag = 'killed since all event ranges were processed by other consumers while waiting in the queue'
                    dJob.modificationTime = dJob.endTime
                    dJob.stateChangeTime  = dJob.endTime
                    # insert
                    self.cur.execute(sqlDJI+comment, dJob.valuesMap())
                    # set file status
                    varMap = {}
                    varMap[':PandaID']   = pandaID
                    varMap[':type1']     = 'output'
                    varMap[':type2']     = 'log'
                    varMap[':newStatus'] = 'failed'
                    self.cur.execute(sqlFSF+comment,varMap)
                    # update files,metadata,parametes
                    varMap = {}
                    varMap[':PandaID'] = pandaID
                    varMap[':modificationTime'] = dJob.modificationTime
                    self.cur.execute(sqlFMod+comment,varMap)
                    self.cur.execute(sqlMMod+comment,varMap)
                    self.cur.execute(sqlPMod+comment,varMap)
                    nKilled += 1
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError, 'Commit error'
            _logger.debug('{0} killed {1} jobs'.format(methodName,nKilled))
            return True
        except:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False



    # kill unused event ranges
    def killUnusedEventRanges(self,jediTaskID,jobsetID):
        comment = ' /* DBProxy.killUnusedEventRanges */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0} jobsetID={1}>".format(jediTaskID,jobsetID)
        # sql to kill event ranges
        varMap = {}
        varMap[':jediTaskID']  = jediTaskID
        varMap[':jobsetID']    = jobsetID
        varMap[':esReady']     = EventServiceUtils.ST_ready
        varMap[':esCancelled'] = EventServiceUtils.ST_cancelled
        sqlCE  = "UPDATE {0}.JEDI_Events ".format(panda_config.schemaJEDI)
        sqlCE += "SET status=:esCancelled "
        sqlCE += "WHERE jediTaskID=:jediTaskID AND pandaID=:jobsetID "
        sqlCE += "AND status=:esReady "
        _logger.debug(sqlCE+comment+str(varMap))
        self.cur.execute(sqlCE, varMap)



    # check attemptNr for more retry
    def checkMoreRetryJEDI(self,job):
        comment = ' /* DBProxy.self.checkMoreRetryJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(job.PandaID)
        _logger.debug("{0} : start".format(methodName))
        # sql to get files
        sqlGF  = "SELECT datasetID,fileID,attemptNr FROM ATLAS_PANDA.filesTable4 "
        sqlGF += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
        # sql to check file
        sqlFJ  = "SELECT attemptNr,maxAttempt,failedAttempt,maxFailure FROM {0}.JEDI_Dataset_Contents ".format(panda_config.schemaJEDI)
        sqlFJ += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID "
        sqlFJ += "AND attemptNr=:attemptNr AND keepTrack=:keepTrack AND PandaID=:PandaID "
        # get files
        varMap = {}
        varMap[':PandaID'] = job.PandaID
        varMap[':type1'] = 'input'
        varMap[':type2'] = 'pseudo_input'
        self.cur.execute(sqlGF+comment,varMap)
        resGF = self.cur.fetchall()
        for datasetID,fileID,attemptNr in resGF:
            # check JEDI contents
            varMap = {}
            varMap[':jediTaskID'] = job.jediTaskID
            varMap[':datasetID']  = datasetID
            varMap[':fileID']     = fileID
            varMap[':attemptNr']  = attemptNr
            varMap[':PandaID']    = job.PandaID
            varMap[':keepTrack']  = 1
            self.cur.execute(sqlFJ+comment,varMap)
            resFJ = self.cur.fetchone()
            if resFJ == None:
                continue
            attemptNr,maxAttempt,failedAttempt,maxFailure = resFJ
            if maxAttempt == None:
                continue
            if attemptNr+1 >= maxAttempt:
                # hit the limit
                _logger.debug("{0} : NG - fileID={1} no more attempt attemptNr({2})+1>=maxAttempt({3})".format(methodName,
                                                                                                               fileID,
                                                                                                               attemptNr,
                                                                                                               maxAttempt))
                return False
            if maxFailure != None and failedAttempt != None and failedAttempt+1 >= maxFailure:
                # hit the limit
                _logger.debug("{0} : NG - fileID={1} no more attempt failedAttempt({2})+1>=maxFailure({3})".format(methodName,
                                                                                                                   fileID,
                                                                                                                   failedAttempt,
                                                                                                                   maxFailure))
                return False
        _logger.debug("{0} : OK".format(methodName))
        return True



    # get the list of jobdefIDs for failed jobs in a task
    def getJobdefIDsForFailedJob(self,jediTaskID):
        comment = ' /* DBProxy.getJobdefIDsForFailedJob */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        _logger.debug("{0} : start".format(methodName))
        try:
            # begin transaction
            self.conn.begin()
            # dql to get jobDefIDs
            sqlGF  = "SELECT distinct jobDefinitionID FROM ATLAS_PANDA.jobsActive4 "
            sqlGF += "WHERE jediTaskID=:jediTaskID AND jobStatus=:jobStatus "
            sqlGF += "AND attemptNr<maxAttempt " 
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':jobStatus']  = 'failed'
            self.cur.execute(sqlGF+comment,varMap)
            resGF = self.cur.fetchall()
            retList = []
            for jobDefinitionID, in resGF:
                retList.append(jobDefinitionID)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} : {1}".format(methodName,str(retList)))
            return retList
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return []



    # change task attribute
    def changeTaskAttributePanda(self,jediTaskID,attrName,attrValue):
        comment = ' /* DBProxy.changeTaskAttributePanda */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        _logger.debug("{0} name={1} value={2}".format(methodName,attrName,attrValue))
        try:
            # sql to update JEDI task table
            sqlT  = 'UPDATE {0}.JEDI_Tasks SET '.format(panda_config.schemaJEDI)
            sqlT += '{0}=:{0} WHERE jediTaskID=:jediTaskID '.format(attrName) 
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[':jediTaskID']  = jediTaskID
            keyName = ':{0}'.format(attrName)
            varMap[keyName] = attrValue
            # update JEDI
            self.cur.execute(sqlT+comment, varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} done with {1}".format(methodName,nRow))
            return nRow
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # increase attempt number for unprocessed files
    def increaseAttemptNrPanda(self,jediTaskID,increasedNr):
        comment = ' /* DBProxy.increaseAttemptNrPanda */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        tmpLog = LogWrapper(_logger,methodName,monToken="<jediTaskID={0}>".format(jediTaskID))
        tmpLog.debug("increasedNr={0}".format(increasedNr))
        try:
            # sql to check task status
            sqlT  = 'SELECT status,oldStatus FROM {0}.JEDI_Tasks '.format(panda_config.schemaJEDI)
            sqlT += 'WHERE jediTaskID=:jediTaskID FOR UPDATE '
            # start transaction
            self.conn.begin()
            # select
            self.cur.arraysize = 10
            varMap = {}
            varMap[':jediTaskID']  = jediTaskID
            # get task status
            self.cur.execute(sqlT+comment, varMap)
            resT = self.cur.fetchone()
            if resT == None:
                tmpMsg = "jediTaskID={0} not found".format(jediTaskID)
                tmpLog.debug(tmpMsg)
                retVal = 1,tmpMsg
            else:
                taskStatus,oldStatus = resT
            # check task status
            okStatusList = ['running','scouting','ready']
            if not taskStatus in okStatusList and not oldStatus in okStatusList:
                tmpMsg = "command rejected since status={0} or oldStatus={1} not in {2}".format(taskStatus,
                                                                                                oldStatus,
                                                                                                str(okStatusList))
                tmpLog.debug(tmpMsg)
                retVal = 2,tmpMsg
            else:
                # sql to get datasetIDs for master
                sqlM  = 'SELECT datasetID FROM {0}.JEDI_Datasets '.format(panda_config.schemaJEDI)
                sqlM += 'WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) '
                # sql to increase attempt numbers
                sqlAB  = "UPDATE {0}.JEDI_Dataset_Contents ".format(panda_config.schemaJEDI)
                sqlAB += "SET maxAttempt=maxAttempt+:increasedNr "
                sqlAB += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND keepTrack=:keepTrack "
                # sql to increase attempt numbers and failure counts
                sqlAF  = "UPDATE {0}.JEDI_Dataset_Contents ".format(panda_config.schemaJEDI)
                sqlAF += "SET maxAttempt=maxAttempt+:increasedNr,maxFailure=maxFailure+:increasedNr "
                sqlAF += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status=:status AND keepTrack=:keepTrack "
                # sql to update datasets
                sqlD  = "UPDATE {0}.JEDI_Datasets ".format(panda_config.schemaJEDI)
                sqlD += "SET nFilesUsed=nFilesUsed-:nFilesReset,nFilesFailed=nFilesFailed-:nFilesReset "
                sqlD += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                # get datasetIDs for master
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':type1'] = 'input'
                varMap[':type2'] = 'pseudo_input'
                self.cur.execute(sqlM+comment, varMap)
                resM = self.cur.fetchall()
                total_nFilesIncreased = 0
                total_nFilesReset = 0
                for datasetID, in resM:
                    # increase attempt numbers
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID'] = datasetID
                    varMap[':status'] = 'ready'
                    varMap[':keepTrack']  = 1
                    varMap[':increasedNr'] = increasedNr
                    nFilesIncreased = 0
                    nFilesReset = 0
                    # still active and maxFailure is undefined
                    sqlA = sqlAB + "AND maxAttempt>attemptNr AND maxFailure IS NULL "
                    self.cur.execute(sqlA+comment, varMap)
                    nRow = self.cur.rowcount
                    nFilesIncreased += nRow
                    # still active and maxFailure is defined
                    sqlA = sqlAF + "AND maxAttempt>attemptNr AND (maxFailure IS NOT NULL AND maxFailure>failedAttempt) "
                    self.cur.execute(sqlA+comment, varMap)
                    nRow = self.cur.rowcount
                    nFilesIncreased += nRow
                    # already done and maxFailure is undefined
                    sqlA = sqlAB + "AND maxAttempt=attemptNr AND maxFailure IS NULL "
                    self.cur.execute(sqlA+comment, varMap)
                    nRow = self.cur.rowcount
                    nFilesReset += nRow
                    nFilesIncreased += nRow
                    # already done and maxFailure is defined
                    sqlA = sqlAF + "AND (maxAttempt=attemptNr OR (maxFailure IS NOT NULL AND maxFailure=failedAttempt)) "
                    self.cur.execute(sqlA+comment, varMap)
                    nRow = self.cur.rowcount
                    nFilesReset += nRow
                    nFilesIncreased += nRow
                    # update dataset
                    if nFilesReset > 0:
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':datasetID'] = datasetID
                        varMap[':nFilesReset'] = nFilesReset
                        tmpLog.debug(sqlD+comment+str(varMap))
                        self.cur.execute(sqlD+comment, varMap)
                    total_nFilesIncreased += nFilesIncreased
                    total_nFilesReset += nFilesReset
                tmpMsg = "increased attemptNr for {0} inputs ({1} reactivated)".format(total_nFilesIncreased,
                                                                                       total_nFilesReset)
                tmpLog.debug(tmpMsg)
                tmpLog.sendMsg(tmpMsg,'jedi','pandasrv')
                retVal = 0,tmpMsg
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug("done")
            return retVal
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(tmpLog,methodName)
            return None,"DB error"



    # get jediTaskID from taskName
    def getTaskIDwithTaskNameJEDI(self,userName,taskName):
        comment = ' /* DBProxy.getTaskIDwithTaskNameJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <userName={0} taskName={1}>".format(userName,taskName)
        _logger.debug("{0} : start".format(methodName))
        try:
            # begin transaction
            self.conn.begin()
            # sql to get jediTaskID
            sqlGF  = "SELECT MAX(jediTaskID) FROM {0}.JEDI_Tasks ".format(panda_config.schemaJEDI)
            sqlGF += "WHERE userName=:userName AND taskName=:taskName "
            varMap = {}
            varMap[':userName'] = userName
            varMap[':taskName'] = taskName
            self.cur.execute(sqlGF+comment,varMap)
            resFJ = self.cur.fetchone()
            if resFJ != None:
                jediTaskID, = resFJ
            else:
                jediTaskID = None
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} : jediTaskID={1}".format(methodName,jediTaskID))
            return jediTaskID
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # update error dialog for a jediTaskID
    def updateTaskErrorDialogJEDI(self,jediTaskID,msg):
        comment = ' /* DBProxy.updateTaskErrorDialogJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        _logger.debug("{0} : start".format(methodName))
        try:
            # begin transaction
            self.conn.begin()
            # get existing dialog
            sqlGF  = "SELECT errorDialog FROM {0}.JEDI_Tasks ".format(panda_config.schemaJEDI)
            sqlGF += "WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            self.cur.execute(sqlGF+comment,varMap)
            resFJ = self.cur.fetchone()
            if resFJ != None:
                # update existing dialog
                errorDialog, = resFJ
                errorDialog = msg
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':errorDialog'] = errorDialog
                sqlUE  = "UPDATE {0}.JEDI_Tasks SET errorDialog=:errorDialog,modificationTime=CURRENT_DATE ".format(panda_config.schemaJEDI)
                sqlUE += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sqlUE+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} : done".format(methodName))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False



    # update modificationtime for a jediTaskID to trigger subsequent process
    def updateTaskModTimeJEDI(self,jediTaskID,newStatus):
        comment = ' /* DBProxy.updateTaskErrorDialogJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        _logger.debug("{0} : start".format(methodName))
        try:
            # begin transaction
            self.conn.begin()
            # update mod time
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            if newStatus != None:
                varMap[':newStatus'] = newStatus
            sqlUE  = "UPDATE {0}.JEDI_Tasks SET ".format(panda_config.schemaJEDI)
            sqlUE += "modificationTime=CURRENT_DATE-1,"
            if newStatus != None:
                sqlUE += "status=:newStatus,oldStatus=NULL,"
            sqlUE = sqlUE[:-1]
            sqlUE += " WHERE jediTaskID=:jediTaskID "
            self.cur.execute(sqlUE+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} : done".format(methodName))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False



    # check input file status 
    def checkInputFileStatusInJEDI(self,jobSpec):
        comment = ' /* DBProxy.checkInputFileStatusInJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(jobSpec.PandaID)
        tmpLog = LogWrapper(_logger,methodName,monToken="<PandaID={0}>".format(jobSpec.PandaID))
        tmpLog.debug("start")
        try:
            # only JEDI
            if jobSpec.lockedby != 'jedi':
                return True
            # sql to check file status
            sqlFileStat  = "SELECT status FROM ATLAS_PANDA.JEDI_Dataset_Contents "
            sqlFileStat += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND attemptNr=:attemptNr "
            # begin transaction
            self.conn.begin()
            # loop over all input files
            allOK = True
            for fileSpec in jobSpec.Files:
                # only input file
                if not fileSpec.type in ['input']:
                    continue
                varMap = {}
                varMap[':jediTaskID'] = fileSpec.jediTaskID
                varMap[':datasetID']  = fileSpec.datasetID
                varMap[':fileID']     = fileSpec.fileID
                varMap[':attemptNr']  = fileSpec.attemptNr
                self.cur.execute(sqlFileStat+comment,varMap)
                resFileStat = self.cur.fetchone()
                if resFileStat != None:
                    fileStatus, = resFileStat
                    if fileStatus in ['cancelled']:
                        tmpLog.debug("jediTaskID={0} datasetID={1} fileID={2} attemptNr={3} is in wrong status ({4})".format(fileSpec.jediTaskID,
                                                                                                                             fileSpec.datasetID,
                                                                                                                             fileSpec.fileID,
                                                                                                                             fileSpec.attemptNr,
                                                                                                                             fileStatus))
                        allOK = False
                        break
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug("done with {0}".format(allOK))
            return allOK
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # increase memory limit
    def increaseRamLimitJEDI(self,jediTaskID,jobRamCount):
        comment = ' /* DBProxy.increaseRamLimitJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0}>".format(jediTaskID)
        _logger.debug("{0} : start".format(methodName))
        try:
            # RAM limit
            limitList = [1000,2000,3000,4000,6000,8000]
            # begin transaction
            self.conn.begin()
            # get currnet limit
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            sqlUE  = "SELECT ramCount FROM {0}.JEDI_Tasks ".format(panda_config.schemaJEDI)
            sqlUE += "WHERE jediTaskID=:jediTaskID "
            self.cur.execute(sqlUE+comment,varMap)
            taskRamCount, = self.cur.fetchone()
            _logger.debug("{0} : RAM limit task={1} job={2}".format(methodName,taskRamCount,jobRamCount))
            # do nothing if the task doesn't define RAM limit
            if taskRamCount in [0,None]:
                _logger.debug("{0} : no change since task RAM limit is {1}".format(methodName,taskRamCount))
            else:
                # skip if already increased or largest limit
                if taskRamCount > jobRamCount:
                    dbgStr = "no change since task RAM limit ({0}) is larger than job limit ({1})".format(taskRamCount,
                                                                                                          jobRamCount)
                    _logger.debug("{0} : {1}".format(methodName,dbgStr))
                elif taskRamCount >= limitList[-1]:
                    dbgStr  = "no change "
                    dbgStr += "since task RAM limit ({0}) is larger than or equal to the highest limit ({1})".format(taskRamCount,
                                                                                                                     limitList[-1])
                    _logger.debug("{0} : {1}".format(methodName,dbgStr))
                else:
                    for nextLimit in limitList:
                        if taskRamCount < nextLimit:
                            break
                    # update RAM limit
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':ramCount'] = nextLimit
                    sqlRL  = "UPDATE {0}.JEDI_Tasks ".format(panda_config.schemaJEDI)
                    sqlRL += "SET ramCount=:ramCount "
                    sqlRL += "WHERE jediTaskID=:jediTaskID "
                    self.cur.execute(sqlRL+comment,varMap)
                    _logger.debug("{0} : increased RAM limit to {1} from {2}".format(methodName,nextLimit,taskRamCount))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            _logger.debug("{0} : done".format(methodName))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False


    # increase memory limit
    def increaseRamLimitJobJEDI(self, job, jobRamCount, jediTaskID):
        """Note that this function only increases the min RAM count for the job,
        not for the entire task (for the latter use increaseRamLimitJEDI)
        """
        comment = ' /* DBProxy.increaseRamLimitJobJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PanDAID={0}>".format(job.PandaID)
        _logger.debug("{0} : start".format(methodName))
        try:
            #If no task associated to job don't take any action
            if job.jediTaskID in [None, 0, 'NULL']:
                _logger.debug("No task(%s) associated to job(%s). Skipping increase of RAM limit"%(job.jediTaskID, job.PandaID))
            else:
                # RAM limit
                limitList = [1000,2000,3000,4000,6000,8000]
                
                # get current task limit
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                sqlUE  = "SELECT ramCount FROM {0}.JEDI_Tasks ".format(panda_config.schemaJEDI)
                sqlUE += "WHERE jediTaskID=:jediTaskID "
                self.cur.execute(sqlUE+comment,varMap)
                taskRamCount, = self.cur.fetchone()
                _logger.debug("{0} : RAM limit task={1} job={2} jobPSS={3}".format(methodName, taskRamCount, jobRamCount, job.maxPSS))
                
                #To increase, we select the highest requirement in job or task
                #e.g. ops could have increased task RamCount through direct DB access
                maxJobTaskRam = max(jobRamCount, taskRamCount)
                
                # do nothing if the job doesn't define RAM limit
                if maxJobTaskRam in [0,None]:
                    _logger.debug("{0} : no change since job RAM limit is {1}".format(methodName, jobRamCount))
                else:
                    # skip if already at largest limit
                    if jobRamCount >= limitList[-1]:
                        dbgStr  = "no change "
                        dbgStr += "since job RAM limit ({0}) is larger than or equal to the highest limit ({1})".format(jobRamCount,
                                                                                                                         limitList[-1])
                        _logger.debug("{0} : {1}".format(methodName,dbgStr))
                    else:
                        #If maxPSS is present, then jump all the levels until the one above
                        if job.maxPSS and job.maxPSS/1024>maxJobTaskRam:
                            minimumRam = job.maxPSS/1024
                        else:
                            minimumRam = maxJobTaskRam

                        for nextLimit in limitList:
                            if minimumRam < nextLimit:
                                break
                        
                        # update RAM limit
                        varMap = {}
                        varMap[':jediTaskID'] = job.jediTaskID
                        varMap[':pandaID'] = job.PandaID
                        varMap[':ramCount'] = nextLimit
                        input_types = ('input', 'pseudo_input', 'pp_input', 'trn_log','trn_output')
                        input_files = filter(lambda pandafile: pandafile.type in input_types, job.Files)
                        input_tuples = [(input_file.datasetID, input_file.fileID, input_file.attemptNr) for input_file in input_files]

                        for entry in input_tuples:
                            datasetID, fileId, attemptNr = entry
                            varMap[':datasetID'] = datasetID
                            varMap[':fileID'] = fileId
                            varMap[':attemptNr'] = attemptNr
                            
                            sqlRL  = "UPDATE {0}.JEDI_Dataset_Contents ".format(panda_config.schemaJEDI)
                            sqlRL += "SET ramCount=:ramCount "
                            sqlRL += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                            sqlRL += "AND pandaID=:pandaID AND fileID=:fileID AND attemptNr=:attemptNr"

                            self.cur.execute(sqlRL+comment,varMap)
                            _logger.debug("{0} : increased RAM limit to {1} from {2} for PandaID {3} fileID {4}".format(methodName, nextLimit, jobRamCount, job.PandaID, fileId))
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'

            _logger.debug("{0} : done".format(methodName))
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False



    # reset files in JEDI
    def resetFileStatusInJEDI(self,dn,prodManager,datasetName,lostFiles,lostInputDatasets):
        comment = ' /* DBProxy.resetFileStatusInJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <datasetName={0}>".format(datasetName)
        tmpLog = LogWrapper(_logger,methodName)
        tmpLog.debug("start")
        try:
            # list of lost input files
            lostInputFiles = set()
            # get compact DN
            compactDN = self.cleanUserID(dn)
            if compactDN in ['','NULL',None]:
                compactDN = dn
            tmpLog.debug("userName={0}".format(compactDN))    
            toSkip = False
            # begin transaction
            self.conn.begin()
            # get jediTaskID
            varMap = {}
            varMap[':type1'] = 'log'
            varMap[':type2'] = 'output'
            varMap[':name1'] = datasetName
            varMap[':name2'] = datasetName.split(':')[-1]
            sqlGI  = 'SELECT jediTaskID,datasetID FROM {0}.JEDI_Datasets '.format(panda_config.schemaJEDI)
            sqlGI += 'WHERE type IN (:type1,:type2) AND datasetName IN (:name1,:name2) '
            self.cur.execute(sqlGI+comment,varMap)
            resGI = self.cur.fetchall()
            # use the largest datasetID since broken tasks might have been retried
            jediTaskID = None
            datasetID = None
            for tmpJediTaskID,tmpDatasetID in resGI:
                if jediTaskID == None or jediTaskID < tmpJediTaskID:
                    jediTaskID = tmpJediTaskID
                    datasetID = tmpDatasetID
                elif datasetID < tmpDatasetID:
                    datasetID =tmpDatasetID
            if jediTaskID == None:
                tmpLog.debug("jediTaskID not found")
                toSkip = True
            if not toSkip:
                # get task status and owner
                tmpLog.debug("jediTaskID={0} datasetID={1}".format(jediTaskID,datasetID))
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                sqlOW  = 'SELECT status,userName FROM {0}.JEDI_Tasks '.format(panda_config.schemaJEDI)
                sqlOW += 'WHERE jediTaskID=:jediTaskID '
                self.cur.execute(sqlOW+comment,varMap)
                resOW = self.cur.fetchone()
                taskStatus,ownerName = resOW
                # check ownership
                if not prodManager and ownerName != compactDN:
                    tmpLog.debug("not the owner = {0}".format(ownerName))
                    toSkip = True
            if not toSkip:
                # get affected PandaIDs
                sqlLP  = 'SELECT pandaID FROM {0}.JEDI_Dataset_Contents '.format(panda_config.schemaJEDI)
                sqlLP += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND lfn=:lfn '
                # sql to update file status
                sqlUFO  = 'UPDATE {0}.JEDI_Dataset_Contents '.format(panda_config.schemaJEDI)
                sqlUFO += 'SET status=:newStatus '
                sqlUFO += 'WHERE jediTaskID=:jediTaskID AND type=:type AND status=:oldStatus AND PandaID=:PandaID '
                # get affected PandaIDs
                lostPandaIDs = set([])
                nDiff = 0
                for lostFile in lostFiles:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID'] = datasetID
                    varMap[':lfn'] = lostFile
                    self.cur.execute(sqlLP+comment,varMap)
                    resLP = self.cur.fetchone()
                    if resLP != None:
                        nDiff += 1
                        pandaID, = resLP
                        lostPandaIDs.add(pandaID)
                        # update the file and coproduced files to lost
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':PandaID'] = pandaID
                        varMap[':type'] = 'output'
                        varMap[':newStatus'] = 'lost'
                        varMap[':oldStatus'] = 'finished'
                        self.cur.execute(sqlUFO+comment,varMap)
                # update output dataset statistics
                sqlUDO  = 'UPDATE {0}.JEDI_Datasets '.format(panda_config.schemaJEDI)
                sqlUDO += 'SET nFilesFinished=nFilesFinished-:nDiff '
                sqlUDO += 'WHERE jediTaskID=:jediTaskID AND type=:type '
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':type'] = 'output'
                varMap[':nDiff'] = nDiff
                tmpLog.debug(sqlUDO+comment+str(varMap))
                self.cur.execute(sqlUDO+comment,varMap)
                # get input datasets
                sqlID  = 'SELECT datasetID,datasetName,masterID FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
                sqlID += 'WHERE jediTaskID=:jediTaskID AND type=:type '
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':type'] = 'input'
                self.cur.execute(sqlID+comment,varMap)
                resID = self.cur.fetchall()
                inputDatasets = {}
                masterID = None
                for tmpDatasetID,tmpDatasetName,tmpMasterID in resID:
                    inputDatasets[tmpDatasetID] = tmpDatasetName
                    if tmpMasterID == None:
                        masterID = tmpDatasetID
                # sql to get affected inputs
                sqlAI  = 'SELECT fileID,datasetID,lfn,outPandaID FROM {0}.JEDI_Dataset_Contents '.format(jedi_config.db.schemaJEDI)
                sqlAI += 'WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) AND PandaID=:PandaID '
                # sql to update input file status
                sqlUFI  = 'UPDATE {0}.JEDI_Dataset_Contents '.format(panda_config.schemaJEDI)
                sqlUFI += 'SET status=:newStatus,attemptNr=attemptNr+1 '
                sqlUFI += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID AND status=:oldStatus '
                # get affected inputs
                datasetCountMap = {}
                for lostPandaID in lostPandaIDs:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':PandaID'] = lostPandaID
                    varMap[':type1'] = 'input'
                    varMap[':type2'] = 'pseudo_input'
                    self.cur.execute(sqlAI+comment,varMap)
                    resAI = self.cur.fetchall()
                    newResAI = []
                    for tmpItem in resAI:
                        tmpFileID,tmpDatasetID,tmpLFN,tmpOutPandaID = tmpItem
                        # input for merged files
                        if tmpOutPandaID != None:
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':PandaID'] = tmpOutPandaID
                            varMap[':type1'] = 'input'
                            varMap[':type2'] = 'pseudo_input'
                            self.cur.execute(sqlAI+comment,varMap)
                            resAI2 = self.cur.fetchall()
                            for tmpItem in resAI2:
                                newResAI.append(tmpItem)
                        else:
                            newResAI.append(tmpItem)
                    for tmpFileID,tmpDatasetID,tmpLFN,tmpOutPandaI in newResAI:
                        # skip if dataset was already deleted
                        if tmpDatasetID in lostInputDatasets:
                            if tmpDatasetID == masterID:
                                lostInputFiles.add(tmpLFN)
                            continue
                        # reset file status
                        if not tmpDatasetID in datasetCountMap:
                            datasetCountMap[tmpDatasetID] = 0
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':datasetID'] = tmpDatasetID
                        varMap[':fileID'] = tmpFileID
                        varMap[':newStatus'] = 'ready'
                        varMap[':oldStatus'] = 'finished'
                        self.cur.execute(sqlUFI+comment,varMap)
                        nRow = self.cur.rowcount
                        if nRow > 0:
                            datasetCountMap[tmpDatasetID] += 1
                # update dataset statistics
                sqlUDI  = 'UPDATE {0}.JEDI_Datasets '.format(panda_config.schemaJEDI)
                sqlUDI += 'SET nFilesUsed=nFilesUsed-:nDiff,nFilesFinished=nFilesFinished-:nDiff '
                sqlUDI += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID '
                for tmpDatasetID,nDiff in datasetCountMap.iteritems():
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID'] = tmpDatasetID
                    varMap[':nDiff'] = nDiff
                    tmpLog.debug(sqlUDI+comment+str(varMap))
                    self.cur.execute(sqlUDI+comment,varMap)
                # update task status
                if taskStatus == 'done':
                    sqlUT  = 'UPDATE {0}.JEDI_Tasks SET status=:newStatus WHERE jediTaskID=:jediTaskID '.format(panda_config.schemaJEDI)
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':newStatus'] = 'finished'
                    self.cur.execute(sqlUT+comment,varMap)
                # some input datasets are lost
                if len(lostInputDatasets) != 0:
                    # rename input datasets since deleted names cannot be reused
                    sqlUN  = 'UPDATE {0}.JEDI_Datasets SET datasetName=:datasetName '.format(panda_config.schemaJEDI)
                    sqlUN += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID '
                    for tmpDatasetID,tmpDatasetName in lostInputDatasets.iteritems():
                        # look for rev number
                        tmpMatch = re.search('\.rcov(\d+)$',tmpDatasetName)
                        if tmpMatch == None:
                            revNum = 0
                        else:
                            revNum = int(tmpMatch.group(1))
                        newDatasetName = re.sub('\.rcov(\d+)$','',tmpDatasetName)
                        newDatasetName += '.rcov{0}'.format(revNum+1)
                        varMap = {}
                        varMap[':jediTaskID'] = jediTaskID
                        varMap[':datasetID'] = tmpDatasetID
                        varMap[':datasetName'] = newDatasetName
                        self.cur.execute(sqlUN+comment,varMap)
                    # get child task
                    sqlGC  = 'SELECT jediTaskID FROM {0}.JEDI_Tasks WHERE parent_tid=:jediTaskID '.format(panda_config.schemaJEDI)
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    self.cur.execute(sqlGC+comment,varMap)
                    resGC = self.cur.fetchone()
                    if resGC != None:
                        childTaskID, = resGC
                        if not childTaskID in [None,jediTaskID]:
                            # get output datasets
                            varMap = {}
                            varMap[':jediTaskID'] = jediTaskID
                            varMap[':type1'] = 'output'
                            varMap[':type2'] = 'log'
                            sqlOD  = 'SELECT datasetID,datasetName FROM {0}.JEDI_Datasets '.format(panda_config.schemaJEDI)
                            sqlOD += 'WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) '
                            self.cur.execute(sqlOD+comment,varMap)
                            resOD = self.cur.fetchall()
                            outputDatasets = {}
                            for tmpDatasetID,tmpDatasetName in resOD:
                                # remove scope and extension
                                bareName = tmpDatasetName.split(':')[-1]
                                bareName = re.sub('\.rcov(\d+)$','',bareName)
                                outputDatasets[bareName] = {'datasetID':tmpDatasetID,
                                                            'datasetName':tmpDatasetName}
                            # get input datasets for child
                            varMap = {}
                            varMap[':jediTaskID'] = childTaskID
                            varMap[':type'] = 'input'
                            sqlGI  = 'SELECT datasetName FROM {0}.JEDI_Datasets '.format(panda_config.schemaJEDI)
                            sqlGI += 'WHERE jediTaskID=:jediTaskID AND type=:type '
                            self.cur.execute(sqlGI+comment,varMap)
                            resGI = self.cur.fetchall()
                            for tmpDatasetName, in resGI:
                                # remove scope and extension
                                bareName = tmpDatasetName.split(':')[-1]
                                bareName = re.sub('\.rcov(\d+)$','',bareName)
                                # check if child renamed them
                                if bareName in outputDatasets and \
                                        tmpDatasetName.split(':')[-1] != outputDatasets[bareName]['datasetName'].split(':')[-1]:
                                    newDatasetName = tmpDatasetName
                                    # remove scope
                                    if not ':' in outputDatasets[bareName]['datasetName']:
                                        newDatasetName = newDatasetName.split(':')[-1]
                                    # rename output datasets if child renamed them
                                    varMap = {}
                                    varMap[':jediTaskID'] = jediTaskID
                                    varMap[':datasetID'] = outputDatasets[bareName]['datasetID']
                                    varMap[':datasetName'] = newDatasetName
                                    self.cur.execute(sqlUN+comment,varMap)
                                    tmpLog.debug("renamed datasetID={0} to {1}".format(varMap[':datasetID'],
                                                                                       varMap[':datasetName']))
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug("done")
            return True,lostInputFiles
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False,None



    # get input datasets for output dataset
    def getInputDatasetsForOutputDatasetJEDI(self,datasetName):
        comment = ' /* DBProxy.getInputDatasetsForOutputDatasetJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <datasetName={0}>".format(datasetName)
        tmpLog = LogWrapper(_logger,methodName)
        tmpLog.debug("start")
        try:
            toSkip = False
            inputDatasets = {}
            # begin transaction
            self.conn.begin()
            # get jediTaskID
            varMap = {}
            varMap[':type1'] = 'log'
            varMap[':type2'] = 'output'
            varMap[':name1'] = datasetName
            varMap[':name2'] = datasetName.split(':')[-1]
            sqlGI  = 'SELECT jediTaskID,datasetID FROM {0}.JEDI_Datasets '.format(panda_config.schemaJEDI)
            sqlGI += 'WHERE type IN (:type1,:type2) AND datasetName IN (:name1,:name2) '
            self.cur.execute(sqlGI+comment,varMap)
            resGI = self.cur.fetchall()
            # use the largest datasetID since broken tasks might have been retried
            jediTaskID = None
            datasetID = None
            for tmpJediTaskID,tmpDatasetID in resGI:
                if jediTaskID == None or jediTaskID < tmpJediTaskID:
                    jediTaskID = tmpJediTaskID
                    datasetID = tmpDatasetID
                elif datasetID < tmpDatasetID:
                    datasetID =tmpDatasetID
            if jediTaskID == None:
                tmpLog.debug("jediTaskID not found")
                toSkip = True
            if not toSkip:
                # get input datasets
                sqlID  = 'SELECT datasetID,datasetName,masterID FROM {0}.JEDI_Datasets '.format(jedi_config.db.schemaJEDI)
                sqlID += 'WHERE jediTaskID=:jediTaskID AND type=:type '
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':type'] = 'input'
                self.cur.execute(sqlID+comment,varMap)
                resID = self.cur.fetchall()
                for tmpDatasetID,tmpDatasetName,tmpMasterID in resID:
                    inputDatasets[tmpDatasetID] = tmpDatasetName
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug("done")
            return True,inputDatasets
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False,None
            


    # record retry history
    def recordRetryHistoryJEDI(self,jediTaskID,newPandaID,oldPandaIDs,relationType):
        comment = ' /* DBProxy.recordRetryHistoryJEDI */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <PandaID={0}>".format(newPandaID)
        tmpLog = LogWrapper(_logger,methodName)
        tmpLog.debug("start")
        # sql to check record
        sqlCK  = "SELECT jediTaskID FROM {0}.JEDI_Job_Retry_History ".format(panda_config.schemaJEDI)
        sqlCK += "WHERE jediTaskID=:jediTaskID AND oldPandaID=:oldPandaID AND newPandaID=:newPandaID AND originPandaID=:originPandaID "
        # sql to insert record
        sqlIN = "INSERT INTO {0}.JEDI_Job_Retry_History ".format(panda_config.schemaJEDI) 
        if relationType == None:
            sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID) "
            sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID) "
        else:
            sqlIN += "(jediTaskID,oldPandaID,newPandaID,originPandaID,relationType) "
            sqlIN += "VALUES(:jediTaskID,:oldPandaID,:newPandaID,:originPandaID,:relationType) "
        for oldPandaID in oldPandaIDs:
            # get origin
            originIDs = self.getOriginPandaIDsJEDI(oldPandaID,jediTaskID,self.cur)
            for originID in originIDs:
                # check
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':oldPandaID'] = oldPandaID
                varMap[':newPandaID'] = newPandaID
                varMap[':originPandaID'] = originID
                self.cur.execute(sqlCK+comment,varMap)
                resCK = self.cur.fetchone()
                # insert
                if resCK == None:
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':oldPandaID'] = oldPandaID
                    varMap[':newPandaID'] = newPandaID
                    varMap[':originPandaID'] = originID
                    if relationType != None:
                        varMap[':relationType'] = relationType
                    self.cur.execute(sqlIN+comment,varMap)
        # return
        tmpLog.debug("done")



    # copy file record
    def copyFileRecord(self,newLFN,fileSpec):
        comment = ' /* DBProxy.copyFileRecord */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <newLFN={0}>".format(newLFN)
        tmpLog = LogWrapper(_logger,methodName)
        tmpLog.debug("start")
        try:
            # reset rowID
            tmpFileSpec = copy.copy(fileSpec)
            tmpFileSpec.row_ID = None
            tmpFileSpec.lfn = newLFN
            # begin transaction
            self.conn.begin()
            # insert file in JEDI
            if not tmpFileSpec.jediTaskID in [None,'NULL']:
                # get fileID
                sqlFileID = "SELECT ATLAS_PANDA.JEDI_DATASET_CONT_FILEID_SEQ.nextval FROM dual "
                self.cur.execute(sqlFileID+comment)
                newFileID, = self.cur.fetchone()
                # read file in JEDI
                varMap = {}
                varMap[':jediTaskID'] = tmpFileSpec.jediTaskID
                varMap[':datasetID']  = tmpFileSpec.datasetID
                varMap[':fileID']     = tmpFileSpec.fileID
                sqlGI  = 'SELECT * FROM {0}.JEDI_Dataset_Contents '.format(panda_config.schemaJEDI)
                sqlGI += 'WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID=:fileID '
                self.cur.execute(sqlGI+comment,varMap)
                resGI = self.cur.fetchone()
                tmpFileSpec.fileID = newFileID
                if resGI != None:
                    # make sql and map
                    sqlJI  = "INSERT INTO {0}.JEDI_Dataset_Contents ".format(panda_config.schemaJEDI)
                    sqlJI += "VALUES ("
                    varMap = {}
                    for columDesc,columVal in zip(self.cur.description,resGI):
                        columName = columDesc[0]
                        # overwrite fileID
                        if columName == 'FILEID':
                            columVal = tmpFileSpec.fileID
                        keyName = ':{0}'.format(columName)
                        varMap[keyName] = columVal
                        sqlJI += '{0},'.format(keyName)
                    sqlJI = sqlJI[:-1]
                    sqlJI += ") "
                    # insert file in JEDI
                    self.cur.execute(sqlJI+comment,varMap)
            # insert file in Panda
            sqlFile = "INSERT INTO ATLAS_PANDA.filesTable4 ({0}) ".format(FileSpec.columnNames())
            sqlFile+= FileSpec.bindValuesExpression(useSeq=True)
            varMap = tmpFileSpec.valuesMap(useSeq=True)
            self.cur.execute(sqlFile+comment, varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug("done")
            return True
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return False

    # get error definitions from DB (values cached for 1 hour)
    @memoize
    def getRetrialRules(self):
        #Logging
        comment = ' /* DBProxy.getRetrialRules */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        _logger.debug("%s start"%methodName)
        
        # SQL to extract the error definitions
        sql  = """
        SELECT re.errorsource, re.errorcode, re.errorDiag, re.parameters, re.architecture, re.release, re.workqueue_id, ra.name, re.active, ra.active
        FROM ATLAS_PANDA.RETRYERRORS re, ATLAS_PANDA.RETRYACTIONS ra
        WHERE re.RetryAction_FK=ra.ID
        AND (CURRENT_TIMESTAMP > re.expiration_date or re.expiration_date IS NULL)
        """
        self.cur.execute(sql+comment, {})
        definitions = self.cur.fetchall()   #example of output: [('pilotErrorCode', 1, None, None, None, None, 'no_retry', 'Y', 'Y'),...]

        # commit
        if not self._commit():
            raise RuntimeError, 'Commit error'

        _logger.debug("definitions %s"%(definitions))
         
        retrial_rules = {} #TODO: Consider if we want a class RetrialRule
        for definition in definitions:
            error_source, error_code, error_diag, parameters, architecture, release, wqid, action, e_active, a_active = definition
                
            #TODO: Need to define a formatting and naming convention for setting the parameters
            #Convert the parameter string into a dictionary
            try:
                #1. Convert a string like "key1=value1&key2=value2" into [[key1, value1],[key2,value2]]
                params_list = map(lambda key_value_pair: key_value_pair.split("="), parameters.split("&"))
                #2. Convert a list [[key1, value1],[key2,value2]] into {key1: value1, key2: value2}
                params_dict = dict((key, value) for (key, value) in params_list)
            except AttributeError:
                params_dict = {}
             
            #Calculate if action and error combination should be active
            if e_active == 'Y' and a_active == 'Y':
                active = True #Apply the action for this error
            else:
                active = False #Do not apply the action for this error, only log 
             
            retrial_rules.setdefault(error_source,{})
            retrial_rules[error_source].setdefault(error_code,[])
            retrial_rules[error_source][error_code].append({'error_diag': error_diag,
                                                            'action': action, 
                                                            'params': params_dict, 
                                                            'architecture': architecture, 
                                                            'release': release,
                                                            'wqid': wqid,
                                                            'active': active})
        _logger.debug("Loaded retrial rules from DB: %s" %retrial_rules)
        return retrial_rules
    
    
    def setMaxAttempt(self, jobID, taskID, files, maxAttempt):
        #Logging
        comment = ' /* DBProxy.setMaxAttempt */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        tmpLog = LogWrapper(_logger,methodName)
        tmpLog.debug("start")
        
        #Update the file entries to avoid JEDI generating new jobs
        input_types = ('input', 'pseudo_input', 'pp_input', 'trn_log','trn_output')
        input_files = filter(lambda pandafile: pandafile.type in input_types and re.search('DBRelease', pandafile.lfn) == None, files)
        input_fileIDs = [input_file.fileID for input_file in input_files]
        input_datasetIDs = [input_file.datasetID for input_file in input_files]
                
        if input_fileIDs:
            #Start transaction
            self.conn.begin()

            
            varMap = {}
            varMap[':taskID'] = taskID
            varMap[':pandaID'] = jobID
            
            #Bind the files
            f = 0
            for fileID in input_fileIDs:
                varMap[':file{0}'.format(f)] = fileID
                f+=1
            file_bindings = ','.join(':file{0}'.format(i) for i in xrange(len(input_fileIDs)))
            
            #Bind the datasets
            d = 0
            for datasetID in input_datasetIDs:
                varMap[':dataset{0}'.format(d)] = datasetID
                d+=1
            dataset_bindings = ','.join(':dataset{0}'.format(i) for i in xrange(len(input_fileIDs)))

            #Get the minimum maxAttempt value of the files
            sql_select = """
            select min(maxattempt) from ATLAS_PANDA.JEDI_Dataset_Contents 
            WHERE JEDITaskID = :taskID
            AND datasetID IN ({0})
            AND fileID IN ({1})
            AND pandaID = :pandaID
            """.format(dataset_bindings, file_bindings)
            self.cur.execute(sql_select+comment, varMap)
            maxAttempt_select = self.cur.fetchone()
            tmpLog.debug("maxAttempt found in DB for jobID {0} is {1}. Target maxAttempt is {2}".format(jobID, maxAttempt_select, maxAttempt))
            
            #Don't update the maxAttempt if the value in the retrial table is lower
            #than the value defined in the task 
            if maxAttempt_select and maxAttempt_select > maxAttempt:
                varMap[':maxAttempt'] = min(maxAttempt, maxAttempt_select)
    
                sql_update  = """
                UPDATE ATLAS_PANDA.JEDI_Dataset_Contents 
                SET maxAttempt=:maxAttempt
                WHERE JEDITaskID = :taskID
                AND datasetID IN ({0})
                AND fileID IN ({1})
                AND pandaID = :pandaID
                """.format(dataset_bindings, file_bindings)
    
                self.cur.execute(sql_update+comment, varMap)
        
            #Commit updates
            if not self._commit():
                raise RuntimeError, 'Commit error'

        tmpLog.debug("done")
        return True

    # throttle jobs for resource shares
    def throttleJobsForResourceShare(self,site):
        comment = ' /* DBProxy.throttleJobsForResourceShare */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <site={0}>".format(site)
        tmpLog = LogWrapper(_logger,methodName)
        tmpLog.debug("start")
        try:
            # sql to throttle jobs
            sql  = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:newStatus "
            sql += "WHERE computingSite=:site AND jobStatus=:oldStatus AND lockedby=:lockedby "
            varMap = {}
            varMap[':site'] = site
            varMap[':lockedby'] = 'jedi'
            varMap[':newStatus'] = 'throttled'
            varMap[':oldStatus'] = 'activated'
            # begin transaction
            self.conn.begin()
            self.cur.execute(sql+comment,varMap)
            nRow = self.cur.rowcount
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug("throttled {0} jobs".format(nRow))
            return nRow
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # activate jobs for resource shares
    def activateJobsForResourceShare(self,site,nJobsPerQueue):
        comment = ' /* DBProxy.activateJobsForResourceShare */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <site={0} nJobsPerQueue={1}>".format(site,nJobsPerQueue)
        tmpLog = LogWrapper(_logger,methodName)
        tmpLog.debug("start")
        try:
            # sql to get jobs
            sqlJ  = "SELECT PandaID,jobStatus FROM ("
            sqlJ += "SELECT PandaID,jobStatus,"
            sqlJ += "ROW_NUMBER() OVER(PARTITION BY workqueue_id ORDER BY currentPriority DESC,PandaID) AS row_number "
            sqlJ += "FROM ATLAS_PANDA.jobsActive4 "
            sqlJ += "WHERE computingSite=:site AND lockedby=:lockedby AND jobStatus IN (:st1,:st2) "
            sqlJ += ") "
            sqlJ += "WHERE row_number<={0} ".format(nJobsPerQueue)
            # sql to activate jobs
            sqlA  = "UPDATE ATLAS_PANDA.jobsActive4 SET jobStatus=:newStatus "
            sqlA += "WHERE PandaID=:PandaID AND jobStatus=:oldStatus "
            varMap = {}
            varMap[':site'] = site
            varMap[':lockedby'] = 'jedi'
            varMap[':st1'] = 'throttled'
            varMap[':st2'] = 'activated'
            # begin transaction
            self.conn.begin()
            self.cur.execute(sqlJ+comment,varMap)
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            resList = self.cur.fetchall()
            nRow = 0
            for pandaID,jobStatus in resList:
                if jobStatus == 'activated':
                    continue
                # activate job
                varMap = {}
                varMap[':PandaID'] = pandaID
                varMap[':newStatus'] = 'activated'
                varMap[':oldStatus'] = 'throttled'
                self.conn.begin()
                self.cur.execute(sqlA+comment,varMap)
                # commit
                if not self._commit():
                    raise RuntimeError, 'Commit error'
                nRow += self.cur.rowcount
            tmpLog.debug("activated {0} jobs".format(nRow))
            return nRow
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return None



    # add associate sub datasets for single consumer job
    def getDestDBlocksWithSingleConsumer(self,jediTaskID,PandaID,ngDatasets):
        comment = ' /* DBProxy.getDestDBlocksWithSingleConsumer */'
        methodName = comment.split(' ')[-2].split('.')[-1]
        methodName += " <jediTaskID={0},PandaID={1}>".format(jediTaskID,PandaID)
        tmpLog = LogWrapper(_logger,methodName)
        tmpLog.debug("start")
        try:
            retMap = {}
            checkedDS = set()
            # sql to get files
            sqlF  = "SELECT datasetID,fileID FROM ATLAS_PANDA.JEDI_Events "
            sqlF += "WHERE jediTaskID=:jediTaskID AND PandaID=:PandaID "
            # sql to get PandaIDs
            sqlP  = "SELECT distinct PandaID FROM ATLAS_PANDA.filesTable4 "
            sqlP += "WHERE jediTaskID=:jediTaskID ANd datasetID=:datasetID AND fileID=:fileID "
            # sql to get sub datasets
            sqlD  = "SELECT destinationDBlock,datasetID FROM ATLAS_PANDA.filesTable4 "
            sqlD += "WHERE PandaID=:PandaID AND type IN (:type1,:type2) "
            # sql to get PandaIDs in merging
            sqlM  = "SELECT distinct PandaID FROM ATLAS_PANDA.filesTable4 "
            sqlM += "WHERE jediTaskID=:jediTaskID ANd datasetID=:datasetID AND status=:status "
            varMap = {}
            varMap[':jediTaskID'] = jediTaskID
            varMap[':PandaID']    = PandaID
            # begin transaction
            self.conn.begin()
            # get files
            self.cur.execute(sqlF+comment,varMap)
            resF = self.cur.fetchall()
            for datasetID,fileID in resF:
                # get parallel jobs
                varMap = {}
                varMap[':jediTaskID'] = jediTaskID
                varMap[':datasetID']  = datasetID
                varMap[':fileID']     = fileID
                self.cur.execute(sqlP+comment,varMap)
                resP = self.cur.fetchall()
                for sPandaID, in resP:
                    if sPandaID == PandaID:
                        continue
                    # get sub datasets of parallel jobs
                    varMap = {}
                    varMap[':PandaID']    = sPandaID
                    varMap[':type1'] = 'output'
                    varMap[':type2'] = 'log'
                    self.cur.execute(sqlD+comment,varMap)
                    resD = self.cur.fetchall()
                    subDatasets = []
                    subDatasetID = None
                    for destinationDBlock,datasetID in resD:
                        if destinationDBlock in ngDatasets:
                            continue
                        if destinationDBlock in checkedDS:
                            continue
                        checkedDS.add(destinationDBlock)
                        subDatasets.append(destinationDBlock)
                        subDatasetID = datasetID
                    if subDatasets == []:
                        continue
                    # get merging PandaID which uses sub dataset
                    varMap = {}
                    varMap[':jediTaskID'] = jediTaskID
                    varMap[':datasetID']  = datasetID
                    varMap[':status']     = 'merging'
                    self.cur.execute(sqlM+comment,varMap)
                    resM = self.cur.fetchone()
                    if resM != None:
                        mPandaID, = resM
                        retMap[mPandaID] = subDatasets
            # commit
            if not self._commit():
                raise RuntimeError, 'Commit error'
            tmpLog.debug("got {0} jobs".format(len(retMap)))
            return retMap
        except:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger,methodName)
            return {}

