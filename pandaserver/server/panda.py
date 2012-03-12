#!/usr/bin/python2.5

"""
entry point

"""

# config file
from config import panda_config

# initialize cx_Oracle using dummy connection
from taskbuffer.Initializer import initializer
initializer.init()

# initialzie TaskBuffer
from taskbuffer.TaskBuffer import taskBuffer
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,panda_config.nDBConnection,True)

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

from taskbuffer.Utils            import isAlive,putFile,deleteFile,getServer,updateLog,fetchLog,\
     touchFile,getVomsAttr,putEventPickingRequest,getAttr,getFile
allowedMethods += ['isAlive','putFile','deleteFile','getServer','updateLog','fetchLog',
                   'touchFile','getVomsAttr','putEventPickingRequest','getAttr','getFile']

from dataservice.DataService     import datasetCompleted,updateFileStatusInDisp
allowedMethods += ['datasetCompleted','updateFileStatusInDisp']

from jobdispatcher.JobDispatcher import getJob,updateJob,getStatus,genPilotToken
allowedMethods += ['getJob','updateJob','getStatus','genPilotToken']

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
     getDisInUseForAnal,getLFNsInUseForAnal
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
                   'getDisInUseForAnal','getLFNsInUseForAnal']

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
        def __init__(self,env,):
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
            _logger.debug("PID=%s %s in" % (os.getpid(),methodName))
        # check method name    
        if not methodName in allowedMethods:
            _logger.error("PID=%s %s is forbidden" % (os.getpid(),methodName))
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
                _logger.error("PID=%s %s is undefined" % (os.getpid(),methodName))
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
                    _logger.debug("PID=%s %s with %s" % (os.getpid(),methodName,str(params.keys())))
                # dummy request object
                dummyReq = DummyReq(environ)
                try:
                    # exec
                    exeRes = apply(tmpMethod,[dummyReq],params)
                    # convert bool to string
                    if exeRes in [True,False]:
                        exeRes = str(exeRes)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    errStr = ""
                    for tmpKey,tmpVal in environ.iteritems():
                        errStr += "%s : %s\n" % (tmpKey,str(tmpVal))
                    _logger.error("execution failure : %s %s" % (errType,errValue))
                    _logger.error(errStr)
                    # return internal server error
                    start_response('500 INTERNAL SERVER ERROR', [('Content-Type', 'text/plain')]) 
                    return ["%s %s" % (errType,errValue)]
        if panda_config.entryVerbose:
            _logger.debug("PID=%s %s out" % (os.getpid(),methodName))
        # return
        if exeRes == taskbuffer.ErrorCode.EC_NotFound:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['not found']
        elif isinstance(exeRes,taskbuffer.ErrorCode.EC_Redirect):
            start_response('302 Redirect', [('Location', exeRes.url)])
            return ['redirect']
        else:                
            start_response('200 OK', [('Content-Type', 'text/plain')])
            return [exeRes]

    # start server
    if panda_config.useFastCGI:
        from flup.server.fcgi import WSGIServer
        WSGIServer(application,multithreaded=False).run()
