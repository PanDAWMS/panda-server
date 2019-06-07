#!/usr/bin/python

"""
entry point

"""

import datetime
import traceback
import types

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

from taskbuffer.Utils import isAlive, putFile, deleteFile, getServer, updateLog, fetchLog,\
     touchFile, getVomsAttr, putEventPickingRequest, getAttr, getFile, uploadLog
allowedMethods += ['isAlive','putFile','deleteFile','getServer','updateLog','fetchLog',
                   'touchFile','getVomsAttr','putEventPickingRequest','getAttr','getFile',
                   'uploadLog']

from dataservice.DataService import datasetCompleted, updateFileStatusInDisp
allowedMethods += ['datasetCompleted', 'updateFileStatusInDisp']

from jobdispatcher.JobDispatcher import getJob, updateJob, getStatus, genPilotToken,\
    getEventRanges, updateEventRange, getKeyPair, updateEventRanges, getDNsForS3, getProxy, getCommands, ackCommands,\
    checkJobStatus, checkEventsAvailability, updateJobsInBulk, getResourceTypes
allowedMethods += ['getJob', 'updateJob', 'getStatus', 'genPilotToken',
                   'getEventRanges', 'updateEventRange', 'getKeyPair',
                   'updateEventRanges', 'getDNsForS3', 'getProxy', 'getCommands', 'ackCommands',
                   'checkJobStatus', 'checkEventsAvailability', 'updateJobsInBulk', 'getResourceTypes']

from userinterface.UserIF import submitJobs, getJobStatus, queryPandaIDs, killJobs, reassignJobs,\
     getJobStatistics, getJobStatisticsPerSite, resubmitJobs, queryLastFilesInDataset, getPandaIDsSite,\
     getJobsToBeUpdated, updateProdDBUpdateTimes, runTaskAssignment, getAssigningTask, getSiteSpecs,\
     getCloudSpecs, runBrokerage, seeCloudTask, queryJobInfoPerCloud, registerProxyKey, getProxyKey,\
     getJobIDsInTimeRange, getPandIDsWithJobID, getFullJobStatus, getJobStatisticsForBamboo,\
     getNUserJobs, addSiteAccess, listSiteAccess, getFilesInUseForAnal, updateSiteAccess,\
     getPandaClientVer, getSlimmedFileInfoPandaIDs, runReBrokerage, getQueuedAnalJobs, getHighestPrioJobStat,\
     getActiveDatasets, setCloudTaskByUser, getSerialNumberForGroupJob, getCachePrefixes,\
     checkMergeGenerationStatus, sendLogInfo, getNumPilots, retryFailedJobsInActive,\
     getJobStatisticsWithLabel, getPandaIDwithJobExeID, getJobStatisticsPerUserSite,\
     getDisInUseForAnal, getLFNsInUseForAnal, getScriptOfflineRunning, setDebugMode,\
     insertSandboxFileInfo, checkSandboxFile, changeJobPriorities, insertTaskParams,\
     killTask, finishTask, getCmtConfigList, getJediTasksInTimeRange, getJediTaskDetails,\
     retryTask, getRetryHistory, changeTaskPriority, reassignTask, changeTaskAttributePanda,\
     pauseTask, resumeTask, increaseAttemptNrPanda, killUnfinishedJobs, changeTaskSplitRulePanda,\
     changeTaskModTimePanda, avalancheTask, getPandaIDsWithTaskID, reactivateTask, getTaskStatus, \
     reassignShare, listTasksInShare, getTaskParamsMap, updateWorkers, harvesterIsAlive,\
     reportWorkerStats, addHarvesterDialogs, getJobStatisticsPerSiteResource, setNumSlotsForWP,\
     reloadInput, enableJumboJobs, updateServiceMetrics, getUserJobMetadata, getJumboJobDatasets

allowedMethods += ['submitJobs','getJobStatus','queryPandaIDs','killJobs','reassignJobs',
                   'getJobStatistics','getJobStatisticsPerSite','resubmitJobs','queryLastFilesInDataset','getPandaIDsSite',
                   'getJobsToBeUpdated','updateProdDBUpdateTimes','runTaskAssignment','getAssigningTask','getSiteSpecs',
                   'getCloudSpecs','runBrokerage','seeCloudTask','queryJobInfoPerCloud','registerProxyKey','getProxyKey',
                   'getJobIDsInTimeRange','getPandIDsWithJobID','getFullJobStatus','getJobStatisticsForBamboo',
                   'getNUserJobs','addSiteAccess','listSiteAccess','getFilesInUseForAnal','updateSiteAccess',
                   'getPandaClientVer','getSlimmedFileInfoPandaIDs','runReBrokerage','getQueuedAnalJobs','getHighestPrioJobStat',
                   'getActiveDatasets','setCloudTaskByUser','getSerialNumberForGroupJob','getCachePrefixes',
                   'checkMergeGenerationStatus','sendLogInfo','getNumPilots','retryFailedJobsInActive',
                   'getJobStatisticsWithLabel','getPandaIDwithJobExeID','getJobStatisticsPerUserSite',
                   'getDisInUseForAnal','getLFNsInUseForAnal','getScriptOfflineRunning','setDebugMode',
                   'insertSandboxFileInfo','checkSandboxFile','changeJobPriorities','insertTaskParams',
                   'killTask','finishTask','getCmtConfigList','getJediTasksInTimeRange','getJediTaskDetails',
                   'retryTask','getRetryHistory','changeTaskPriority','reassignTask','changeTaskAttributePanda',
                   'pauseTask','resumeTask','increaseAttemptNrPanda','killUnfinishedJobs','changeTaskSplitRulePanda',
                   'changeTaskModTimePanda','avalancheTask','getPandaIDsWithTaskID', 'reactivateTask', 'getTaskStatus',
                   'reassignShare', 'listTasksInShare', 'getTaskParamsMap', 'updateWorkers', 'harvesterIsAlive',
                   'reportWorkerStats', 'addHarvesterDialogs', 'getJobStatisticsPerSiteResource', 'setNumSlotsForWP',
                   'reloadInput', 'enableJumboJobs', 'updateServiceMetrics', 'getUserJobMetadata', 'getJumboJobDatasets']

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

        # accept json
        def acceptJson(self):
            try:
                if self.subprocess_env.has_key('HTTP_ACCEPT'):
                    return 'application/json' in self.subprocess_env['HTTP_ACCEPT']
            except:
                pass
            return False
        

    # application
    def application(environ, start_response):
        # get method name
        methodName = ''
        if environ.has_key('SCRIPT_NAME'):
            methodName = environ['SCRIPT_NAME'].split('/')[-1]
        _logger.debug("PID=%s %s start" % (os.getpid(),methodName))
        regStart = datetime.datetime.utcnow()
        retType = None
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
                try:
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
                    # exec
                    exeRes = apply(tmpMethod,[dummyReq],params)
                    # extract return type
                    if type(exeRes) == types.DictType:
                        retType = exeRes['type']
                        exeRes  = exeRes['content']
                    # convert bool to string
                    if exeRes in [True,False]:
                        exeRes = str(exeRes)
                except:
                    errType,errValue = sys.exc_info()[:2]
                    _logger.error("execution failure : %s %s %s" % (errType,errValue,traceback.format_exc()))
                    errStr = ""
                    for tmpKey,tmpVal in environ.iteritems():
                        errStr += "%s : %s\n" % (tmpKey,str(tmpVal))
                    _logger.error(errStr)
                    # return internal server error
                    start_response('500 INTERNAL SERVER ERROR', [('Content-Type', 'text/plain')]) 
                    return ["%s %s" % (errType,errValue)]
        if panda_config.entryVerbose:
            _logger.debug("PID=%s %s out" % (os.getpid(),methodName))
        regTime = datetime.datetime.utcnow() - regStart
        _logger.info("PID=%s %s exec_time=%s.%03d sec, return len=%s B" % (os.getpid(),
                                                                              methodName,regTime.seconds,
                                                                              regTime.microseconds/1000,
                                                                              len(str(exeRes))))
        # return
        if exeRes == taskbuffer.ErrorCode.EC_NotFound:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['not found']
        elif isinstance(exeRes,taskbuffer.ErrorCode.EC_Redirect):
            start_response('302 Redirect', [('Location', exeRes.url)])
            return ['redirect']
        else:                
            if retType == 'json':
                start_response('200 OK', [('Content-Type', 'application/json')])
            else:
                start_response('200 OK', [('Content-Type', 'text/plain')])
            return [exeRes]

    # start server
    if panda_config.useFastCGI:
        from flup.server.fcgi import WSGIServer
        WSGIServer(application,multithreaded=False).run()
