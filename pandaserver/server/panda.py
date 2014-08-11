#!/usr/bin/python

"""
entry point

"""

# config file
from config import panda_config

# initialize cx_Oracle using dummy connection
from taskbuffer.Initializer import initializer
initializer.init()

################################################################################
################################################################################
################################################################################
#
#
##### TEST-START
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('pandaservertest')
import os
_logger.debug('PYTHONPATH=%s' % str(os.environ.get("PYTHONPATH")))
#
##_logger.debug('### TEST DIR')
##from taskbuffer.WrappedCursor import WrappedCursor as WPC
##wpc = WPC()
##_logger.debug('dir(wpc)=%s' % str(dir(wpc)))
##
#from taskbuffer.OraDBProxy import DBProxy as DBP
#dbp = DBP()
#_logger.debug('dir(dbp)=%s' % str(dir(dbp)))
#_logger.debug('### END TEST DIR')
##### TEST-END
#
#
#
#
##### OraDBProxyMySQL.connect:
#dbp.connect()
##_logger.debug('dbp.cur=%s' % str(dbp.cur))
##### OraDBProxyMySQL.wakeUp:
#dbp.wakeUp()
#
#
###### OraDBProxyMySQL.insertNewJob
##import datetime
##from taskbuffer.JobSpec import JobSpec as JobSpec
##from taskbuffer.FileSpec import FileSpec as FileSpec
### # FileSpec
##myfile1 = FileSpec()
##myfile1.status = 'status'
##myfile1.lfn = 'AOD_EMBLLUP.$PANDAID._$JOBSETID.pool.root.2'
##myfile1.GUID = '67213601-F952-7E40-886F-DDFB1E14F14D'
##myfile1.type = 'input'
##myfile1.dataset = 'data12_8TeV.00208811.physics_Muons.recon.AOD_EMBLLUP.r4065_p1278_r4232_tid01213138_00'
##myfile1.prodDBlock = 'data12_8TeV.00208811.physics_Muons.recon.AOD_EMBLLUP.r4065_p1278_r4232_tid01213138_00'
##myfile1.fsize = 12345
##myfile1.checksum = 'ad:84f8ee32'
##myfile1.scope = 'data12_8TeV'
##myfile1.attemptNr = 1
##myfile1.datasetID = 1
##
##myfile2 = FileSpec()
##myfile2.status = 'status'
##myfile2.lfn = 'AOD_EMBLLUP.$PANDAID._$JOBSETID.pool.root.3'
##myfile2.GUID = '67213601-F952-7E40-886F-DDFB1E14F143'
##myfile2.type = 'input'
##myfile2.dataset = 'data12_8TeV.00208811.physics_Muons.recon.AOD_EMBLLUP.r4065_p1278_r4232_tid01213138_00'
##myfile2.prodDBlock = 'data12_8TeV.00208811.physics_Muons.recon.AOD_EMBLLUP.r4065_p1278_r4232_tid01213138_00'
##myfile2.fsize = 12645
##myfile2.checksum = 'ad:84f8ee33'
##myfile2.scope = 'data12_8TeV'
##myfile2.attemptNr = 1
##myfile2.datasetID = 1
##
### # JobSpec
##myjob = JobSpec()
##myjob.jobDefinitionID = 1
##myjob.jediTaskID = 1
### myjob.Files = [myfile1]
##myjob.addFile(myfile1)
##myjob.addFile(myfile2)
##myjob.prodSourceLabel = 'user'
##myuser = 'JARKA20130809'
##myserNum = 288
##myuserVO = 'VOPANDATEST'
### dbp.insertNewJob(job, user, serNum, weight=0.0, priorityOffset=0, userVO=None, groupJobSN=0, toPending=False, withModTime=False)
##dbp.insertNewJob(job=myjob, user=myuser, serNum=myserNum, userVO=myuserVO)
##
##dbp.propagateResultToJEDI(myjob, dbp.cur)
#
#
#
#
################################################################################
################################################################################
################################################################################

# initialzie TaskBuffer
from taskbuffer.TaskBuffer import taskBuffer
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, panda_config.nDBConnection, True)

# initialize JobDispatcher
from jobdispatcher.JobDispatcher import jobDispatcher
if panda_config.nDBConnection != 0:
    jobDispatcher.init(taskBuffer)

# initialize DataService
from dataservice.DataService import dataService
if panda_config.nDBConnection != 0:
    dataService.init(taskBuffer)

# initialize UserIF
from userinterface.UserIF import userIF
if panda_config.nDBConnection != 0:
    userIF.init(taskBuffer)

# import web I/F
allowedMethods = []

from taskbuffer.Utils            import isAlive, putFile, deleteFile, getServer, updateLog, fetchLog, \
     touchFile, getVomsAttr, putEventPickingRequest, getAttr, getFile, uploadLog
allowedMethods += ['isAlive', 'putFile', 'deleteFile', 'getServer', 'updateLog', 'fetchLog',
                   'touchFile', 'getVomsAttr', 'putEventPickingRequest', 'getAttr', 'getFile',
                   'uploadLog']

from dataservice.DataService     import datasetCompleted,updateFileStatusInDisp
allowedMethods += ['datasetCompleted','updateFileStatusInDisp']

from jobdispatcher.JobDispatcher import getJob,updateJob,getStatus,genPilotToken,\
    getEventRanges,updateEventRange,getKeyPair
allowedMethods += ['getJob','updateJob','getStatus','genPilotToken',
                   'getEventRanges','updateEventRange','getKeyPair']

from userinterface.UserIF        import submitJobs,getJobStatus,queryPandaIDs,killJobs,reassignJobs,\
     getJobStatistics,getJobStatisticsPerSite,resubmitJobs,queryLastFilesInDataset,getPandaIDsSite,\
     getJobsToBeUpdated,updateProdDBUpdateTimes,runTaskAssignment,getAssigningTask,getSiteSpecs,\
     getCloudSpecs,runBrokerage,seeCloudTask,queryJobInfoPerCloud,registerProxyKey,getProxyKey,\
     getJobIDsInTimeRange,getPandIDsWithJobID,getFullJobStatus,getJobStatisticsForBamboo,\
     getNUserJobs,addSiteAccess,listSiteAccess,getFilesInUseForAnal,updateSiteAccess,\
     getPandaClientVer,getSlimmedFileInfoPandaIDs,runReBrokerage,deleteFilesFromCacheDB,\
     addFilesToCacheDB,flushCacheDB,checkFilesWithCacheDB,getQueuedAnalJobs,getHighestPrioJobStat,\
     getActiveDatasets,setCloudTaskByUser,getSerialNumberForGroupJob,getCachePrefixes,\
     checkMergeGenerationStatus,sendLogInfo,getNumPilots,retryFailedJobsInActive,\
     getJobStatisticsWithLabel,getPandaIDwithJobExeID,getJobStatisticsPerUserSite,\
     getDisInUseForAnal,getLFNsInUseForAnal,getScriptOfflineRunning,setDebugMode,\
     insertSandboxFileInfo,checkSandboxFile,changeJobPriorities,insertTaskParams,\
     killTask,finishTask,getCmtConfigList,getJediTasksInTimeRange,getJediTaskDetails,\
     retryTask,getRetryHistory,changeTaskPriority,reassignTask\
     , checkSandboxFileEC2
allowedMethods += ['submitJobs','getJobStatus','queryPandaIDs','killJobs','reassignJobs',
                   'getJobStatistics','getJobStatisticsPerSite','resubmitJobs','queryLastFilesInDataset','getPandaIDsSite',
                   'getJobsToBeUpdated','updateProdDBUpdateTimes','runTaskAssignment','getAssigningTask','getSiteSpecs',
                   'getCloudSpecs','runBrokerage','seeCloudTask','queryJobInfoPerCloud','registerProxyKey','getProxyKey',
                   'getJobIDsInTimeRange','getPandIDsWithJobID','getFullJobStatus','getJobStatisticsForBamboo',
                   'getNUserJobs','addSiteAccess','listSiteAccess','getFilesInUseForAnal','updateSiteAccess',
                   'getPandaClientVer','getSlimmedFileInfoPandaIDs','runReBrokerage','deleteFilesFromCacheDB',
                   'addFilesToCacheDB','flushCacheDB','checkFilesWithCacheDB','getQueuedAnalJobs','getHighestPrioJobStat',
                   'getActiveDatasets','setCloudTaskByUser','getSerialNumberForGroupJob','getCachePrefixes',
                   'checkMergeGenerationStatus','sendLogInfo','getNumPilots','retryFailedJobsInActive',
                   'getJobStatisticsWithLabel','getPandaIDwithJobExeID','getJobStatisticsPerUserSite',
                   'getDisInUseForAnal','getLFNsInUseForAnal','getScriptOfflineRunning','setDebugMode',
                   'insertSandboxFileInfo','checkSandboxFile','changeJobPriorities','insertTaskParams',
                   'killTask','finishTask','getCmtConfigList','getJediTasksInTimeRange','getJediTaskDetails',
                   'retryTask','getRetryHistory','changeTaskPriority','reassignTask'
                   , 'checkSandboxFileEC2']

## API for HTCondor ... disabled in server, done in BigPanDAmon
#from userinterface.UserIFHTCondor import addHTCondorJobs, updateHTCondorJobs, \
#        removeHTCondorJobs
#allowedMethods += ['addHTCondorJobs', 'updateHTCondorJobs', 'removeHTCondorJobs']

# import error
import taskbuffer.ErrorCode


# FastCGI/WSGI entry
if panda_config.useFastCGI or panda_config.useWSGI:

    import os
    import cgi
    import sys
    from pandalogger.PandaLogger import PandaLogger

    # logger
    _logger = PandaLogger().getLogger('Entry')

    # dummy request object
    class DummyReq:
        def __init__(self, env,):
            # environ
            self.subprocess_env = env
            # header
            self.headers_in = {}
            # content-length
            if self.subprocess_env.has_key('CONTENT_LENGTH'):
                self.headers_in["content-length"] = self.subprocess_env['CONTENT_LENGTH']

        # get remote host    
        def get_remote_host(self):
            if self.subprocess_env.has_key('REMOTE_HOST'):
                return self.subprocess_env['REMOTE_HOST']
            return ""
        

    # application
    def application(environ, start_response):
        # get method name
        methodName = ''
        if environ.has_key('SCRIPT_NAME'):
            methodName = environ['SCRIPT_NAME'].split('/')[-1]
        if panda_config.entryVerbose:
            _logger.debug("PID=%s %s in" % (os.getpid(), methodName))
        # check method name    
        if not methodName in allowedMethods:
            _logger.error("PID=%s %s is forbidden" % (os.getpid(), methodName))
            exeRes = "False : %s is forbidden" % methodName
        else:
            # get method object
            tmpMethod = None
            try:
                exec "tmpMethod = %s" % methodName
            except:
                pass
            # object not found
            if tmpMethod == None:
                _logger.error("PID=%s %s is undefined" % (os.getpid(), methodName))
                exeRes = "False"
            else:
                # get params 
                tmpPars = cgi.FieldStorage(environ['wsgi.input'], environ=environ,
                                           keep_blank_values=1)
                # convert to map
                params = {}
                for tmpKey in tmpPars.keys():
                    if tmpPars[tmpKey].file != None and tmpPars[tmpKey].filename != None:
                        # file
                        params[tmpKey] = tmpPars[tmpKey]
                    else:
                        # string
                        params[tmpKey] = tmpPars.getfirst(tmpKey)
                if panda_config.entryVerbose:
                    _logger.debug("PID=%s %s with %s" % (os.getpid(), methodName, str(params.keys())))
                # dummy request object
                dummyReq = DummyReq(environ)
                try:
                    # exec
                    exeRes = apply(tmpMethod, [dummyReq], params)
                    # convert bool to string
                    if exeRes in [True, False]:
                        exeRes = str(exeRes)
                except:
                    errType, errValue = sys.exc_info()[:2]
                    errStr = ""
                    for tmpKey, tmpVal in environ.iteritems():
                        errStr += "%s : %s\n" % (tmpKey, str(tmpVal))
                    _logger.error("execution failure : %s %s" % (errType, errValue))
                    _logger.error(errStr)
                    # return internal server error
                    start_response('500 INTERNAL SERVER ERROR', [('Content-Type', 'text/plain')]) 
                    return ["%s %s" % (errType, errValue)]
        if panda_config.entryVerbose:
            _logger.debug("PID=%s %s out" % (os.getpid(), methodName))
        # return
        if exeRes == taskbuffer.ErrorCode.EC_NotFound:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['not found']
        elif isinstance(exeRes, taskbuffer.ErrorCode.EC_Redirect):
            start_response('302 Redirect', [('Location', exeRes.url)])
            return ['redirect']
        else:                
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [exeRes]

    # start server
    if panda_config.useFastCGI:
        from flup.server.fcgi import WSGIServer
        WSGIServer(application, multithreaded=False).run()

_logger.debug('### END of pandaserver.server.panda')
