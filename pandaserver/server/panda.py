#!/usr/bin/python

"""
entry point

"""

import datetime
import types

# config file
from pandaserver.config import panda_config

# initialize cx_Oracle using dummy connection
from pandaserver.taskbuffer.Initializer import initializer
initializer.init()

# initialzie TaskBuffer
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,panda_config.nDBConnection,True)

# initialize JobDispatcher
from pandaserver.jobdispatcher.JobDispatcher import jobDispatcher
if panda_config.nDBConnection != 0:
    jobDispatcher.init(taskBuffer)

# initialize DataService
from pandaserver.dataservice.DataService import dataService
if panda_config.nDBConnection != 0:
    dataService.init(taskBuffer)

# initialize UserIF
from pandaserver.userinterface.UserIF import userIF
if panda_config.nDBConnection != 0:
    userIF.init(taskBuffer)

# import web I/F
allowedMethods = []

from pandaserver.taskbuffer.Utils import isAlive, putFile, deleteFile, getServer, updateLog, fetchLog,\
     touchFile, getVomsAttr, putEventPickingRequest, getAttr, getFile, uploadLog
allowedMethods += ['isAlive','putFile','deleteFile','getServer','updateLog','fetchLog',
                   'touchFile','getVomsAttr','putEventPickingRequest','getAttr','getFile',
                   'uploadLog']

from pandaserver.dataservice.DataService import datasetCompleted, updateFileStatusInDisp
allowedMethods += ['datasetCompleted', 'updateFileStatusInDisp']

from pandaserver.jobdispatcher.JobDispatcher import getJob, updateJob, getStatus, genPilotToken,\
    getEventRanges, updateEventRange, getKeyPair, updateEventRanges, getDNsForS3, getProxy, getCommands, ackCommands,\
    checkJobStatus, checkEventsAvailability, updateJobsInBulk, getResourceTypes
allowedMethods += ['getJob', 'updateJob', 'getStatus', 'genPilotToken',
                   'getEventRanges', 'updateEventRange', 'getKeyPair',
                   'updateEventRanges', 'getDNsForS3', 'getProxy', 'getCommands', 'ackCommands',
                   'checkJobStatus', 'checkEventsAvailability', 'updateJobsInBulk', 'getResourceTypes']

from pandaserver.userinterface.UserIF import submitJobs, getJobStatus, queryPandaIDs, killJobs, reassignJobs,\
     getJobStatistics, getJobStatisticsPerSite, resubmitJobs, queryLastFilesInDataset, getPandaIDsSite,\
     getJobsToBeUpdated, updateProdDBUpdateTimes, runTaskAssignment, getAssigningTask, getSiteSpecs,\
     getCloudSpecs, seeCloudTask, queryJobInfoPerCloud, registerProxyKey, getProxyKey,\
     getJobIDsInTimeRange, getPandIDsWithJobID, getFullJobStatus, getJobStatisticsForBamboo,\
     getNUserJobs, addSiteAccess, listSiteAccess, getFilesInUseForAnal, updateSiteAccess,\
     getPandaClientVer, getSlimmedFileInfoPandaIDs, getQueuedAnalJobs, getHighestPrioJobStat,\
     getActiveDatasets, setCloudTaskByUser, getSerialNumberForGroupJob, getCachePrefixes,\
     checkMergeGenerationStatus, getNumPilots, retryFailedJobsInActive,\
     getJobStatisticsWithLabel, getPandaIDwithJobExeID, getJobStatisticsPerUserSite,\
     getDisInUseForAnal, getLFNsInUseForAnal, getScriptOfflineRunning, setDebugMode,\
     insertSandboxFileInfo, checkSandboxFile, changeJobPriorities, insertTaskParams,\
     killTask, finishTask, getCmtConfigList, getJediTasksInTimeRange, getJediTaskDetails,\
     retryTask, getRetryHistory, changeTaskPriority, reassignTask, changeTaskAttributePanda,\
     pauseTask, resumeTask, increaseAttemptNrPanda, killUnfinishedJobs, changeTaskSplitRulePanda,\
     changeTaskModTimePanda, avalancheTask, getPandaIDsWithTaskID, reactivateTask, getTaskStatus, \
     reassignShare, listTasksInShare, getTaskParamsMap, updateWorkers, harvesterIsAlive,\
     reportWorkerStats, addHarvesterDialogs, getJobStatisticsPerSiteResource, setNumSlotsForWP,\
     reloadInput, enableJumboJobs, updateServiceMetrics, getUserJobMetadata, getJumboJobDatasets, getGShareStatus,\
     sweepPQ

allowedMethods += ['submitJobs','getJobStatus','queryPandaIDs','killJobs','reassignJobs',
                   'getJobStatistics','getJobStatisticsPerSite','resubmitJobs','queryLastFilesInDataset','getPandaIDsSite',
                   'getJobsToBeUpdated','updateProdDBUpdateTimes','runTaskAssignment','getAssigningTask','getSiteSpecs',
                   'getCloudSpecs','seeCloudTask','queryJobInfoPerCloud','registerProxyKey','getProxyKey',
                   'getJobIDsInTimeRange','getPandIDsWithJobID','getFullJobStatus','getJobStatisticsForBamboo',
                   'getNUserJobs','addSiteAccess','listSiteAccess','getFilesInUseForAnal','updateSiteAccess',
                   'getPandaClientVer','getSlimmedFileInfoPandaIDs','getQueuedAnalJobs','getHighestPrioJobStat',
                   'getActiveDatasets','setCloudTaskByUser','getSerialNumberForGroupJob','getCachePrefixes',
                   'checkMergeGenerationStatus','getNumPilots','retryFailedJobsInActive',
                   'getJobStatisticsWithLabel','getPandaIDwithJobExeID','getJobStatisticsPerUserSite',
                   'getDisInUseForAnal','getLFNsInUseForAnal','getScriptOfflineRunning','setDebugMode',
                   'insertSandboxFileInfo','checkSandboxFile','changeJobPriorities','insertTaskParams',
                   'killTask','finishTask','getCmtConfigList','getJediTasksInTimeRange','getJediTaskDetails',
                   'retryTask','getRetryHistory','changeTaskPriority','reassignTask','changeTaskAttributePanda',
                   'pauseTask','resumeTask','increaseAttemptNrPanda','killUnfinishedJobs','changeTaskSplitRulePanda',
                   'changeTaskModTimePanda','avalancheTask','getPandaIDsWithTaskID', 'reactivateTask', 'getTaskStatus',
                   'reassignShare', 'listTasksInShare', 'getTaskParamsMap', 'updateWorkers', 'harvesterIsAlive',
                   'reportWorkerStats', 'addHarvesterDialogs', 'getJobStatisticsPerSiteResource', 'setNumSlotsForWP',
                   'reloadInput', 'enableJumboJobs', 'updateServiceMetrics', 'getUserJobMetadata', 'getJumboJobDatasets',
                   'getGShareStatus', 'sweepPQ']

# import error
import pandaserver.taskbuffer.ErrorCode


# FastCGI/WSGI entry
if panda_config.useFastCGI or panda_config.useWSGI:

    import os
    import cgi
    from pandacommon.pandalogger.PandaLogger import PandaLogger
    from pandacommon.pandalogger.LogWrapper import LogWrapper

    if panda_config.token_authType == 'scitokens':
        import scitokens

    # logger
    _logger = PandaLogger().getLogger('Entry')


    # dummy request object
    class DummyReq:
        def __init__(self, env, tmpLog):
            # environ
            self.subprocess_env = env
            # header
            self.headers_in = {}
            # content-length
            if 'CONTENT_LENGTH' in self.subprocess_env:
                self.headers_in["content-length"] = self.subprocess_env['CONTENT_LENGTH']
            # scitoken
            try:
                if panda_config.token_authType == 'scitokens' and 'HTTP_AUTHORIZATION' in env:
                    serialized_token = env['HTTP_AUTHORIZATION'].split()[1]
                    token = scitokens.SciToken.deserialize(serialized_token, audience=panda_config.token_audience)
                    # check issuer
                    if 'iss' not in token:
                        tmpLog.error('issuer is undefined')
                    elif panda_config.token_issuers != '' and token['iss'] not in panda_config.token_issuers.split(','):
                        tmpLog.error('invalid issuer {0}'.format(token['iss']))
                    else:
                        for c, v in token.claims():
                            self.subprocess_env['SCI_TOKEN_{0}'.format(str(c))] = str(v)
                        # use sub and scope as DN and FQAN
                        if 'SSL_CLIENT_S_DN' not in self.subprocess_env:
                            self.subprocess_env['SSL_CLIENT_S_DN'] = str(token['sub'])
                            i = 0
                            for scope in token['scope'].split():
                                if scope.startswith('role:'):
                                    self.subprocess_env['GRST_CRED_SCI_TOKEN_{0}'.format(i)] = 'VOMS ' + str(scope.split(':')[-1])
                                    i += 1
            except Exception as e:
                tmpLog.error('invalid token: {0}'.format(str(e)))

        # get remote host    
        def get_remote_host(self):
            if 'REMOTE_HOST' in self.subprocess_env:
                return self.subprocess_env['REMOTE_HOST']
            return ""

        # accept json
        def acceptJson(self):
            try:
                if 'HTTP_ACCEPT' in self.subprocess_env:
                    return 'application/json' in self.subprocess_env['HTTP_ACCEPT']
            except Exception:
                pass
            return False
        

    # application
    def application(environ, start_response):
        # get method name
        methodName = ''
        if 'SCRIPT_NAME' in environ:
            methodName = environ['SCRIPT_NAME'].split('/')[-1]
        tmpLog = LogWrapper(_logger, "PID={0} {1}".format(os.getpid(), methodName))
        tmpLog.debug("start")
        regStart = datetime.datetime.utcnow()
        retType = None
        # check method name    
        if not methodName in allowedMethods:
            tmpLog.error("is forbidden")
            exeRes = "False : %s is forbidden" % methodName
        else:
            # get method object
            tmpMethod = None
            try:
                tmpMethod = globals()[methodName]
            except Exception:
                pass
            # object not found
            if tmpMethod is None:
                tmpLog.error("is undefined")
                exeRes = "False"
            else:
                try:
                    # get params 
                    tmpPars = cgi.FieldStorage(environ['wsgi.input'], environ=environ,
                                               keep_blank_values=1)
                    # convert to map
                    params = {}
                    for tmpKey in list(tmpPars):
                        if tmpPars[tmpKey].file is not None and tmpPars[tmpKey].filename is not None:
                            # file
                            params[tmpKey] = tmpPars[tmpKey]
                        else:
                            # string
                            params[tmpKey] = tmpPars.getfirst(tmpKey)
                    if panda_config.entryVerbose:
                        tmpLog.debug("with %s" % str(list(params)))
                    # dummy request object
                    dummyReq = DummyReq(environ, tmpLog)
                    param_list = [dummyReq]
                    # exec
                    exeRes = tmpMethod(*param_list, **params)
                    # extract return type
                    if isinstance(exeRes, dict):
                        retType = exeRes['type']
                        exeRes  = exeRes['content']
                    # convert bool to string
                    if exeRes in [True,False]:
                        exeRes = str(exeRes)
                except Exception as e:
                    tmpLog.error("execution failure : {0}".format(str(e)))
                    errStr = ""
                    for tmpKey in environ:
                        tmpVal = environ[tmpKey]
                        errStr += "%s : %s\n" % (tmpKey,str(tmpVal))
                    tmpLog.error(errStr)
                    # return internal server error
                    start_response('500 INTERNAL SERVER ERROR', [('Content-Type', 'text/plain')]) 
                    return [str(e)]
        if panda_config.entryVerbose:
            tmpLog.debug("done")
        regTime = datetime.datetime.utcnow() - regStart
        tmpLog.info("exec_time=%s.%03d sec, return len=%s B" % (regTime.seconds,
                                                                regTime.microseconds/1000,
                                                                len(str(exeRes))))
        # return
        if exeRes == pandaserver.taskbuffer.ErrorCode.EC_NotFound:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['not found']
        elif isinstance(exeRes, pandaserver.taskbuffer.ErrorCode.EC_Redirect):
            start_response('302 Redirect', [('Location', exeRes.url)])
            return ['redirect']
        else:                
            if retType == 'json':
                start_response('200 OK', [('Content-Type', 'application/json')])
            else:
                start_response('200 OK', [('Content-Type', 'text/plain')])
            if isinstance(exeRes, str):
                exeRes = exeRes.encode()
            return [exeRes]

    # start server
    if panda_config.useFastCGI:
        from flup.server.fcgi import WSGIServer
        WSGIServer(application,multithreaded=False).run()
