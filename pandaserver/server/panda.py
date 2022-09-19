#!/usr/bin/python

"""
entry point

"""

import datetime
import traceback
import six
import tempfile
import io
import signal
import json
import gzip

# config file
from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils

from pandaserver.taskbuffer.Initializer import initializer
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.jobdispatcher.JobDispatcher import jobDispatcher
from pandaserver.dataservice.DataService import dataService
from pandaserver.userinterface.UserIF import userIF
from pandaserver.taskbuffer.Utils import isAlive, putFile, deleteFile, getServer, updateLog, fetchLog,\
     touchFile, getVomsAttr, putEventPickingRequest, getAttr, uploadLog, put_checkpoint, delete_checkpoint,\
     put_file_recovery_request, put_workflow_request
from pandaserver.dataservice.DataService import datasetCompleted, updateFileStatusInDisp
from pandaserver.jobdispatcher.JobDispatcher import getJob, updateJob, getStatus, genPilotToken,\
    getEventRanges, updateEventRange, getKeyPair, updateEventRanges, getDNsForS3, getProxy, getCommands, ackCommands,\
    checkJobStatus, checkEventsAvailability, updateJobsInBulk, getResourceTypes, updateWorkerPilotStatus
from pandaserver.userinterface.UserIF import submitJobs, getJobStatus, queryPandaIDs, killJobs, reassignJobs,\
     getJobStatistics, getJobStatisticsPerSite, resubmitJobs, queryLastFilesInDataset, getPandaIDsSite,\
     getJobsToBeUpdated, updateProdDBUpdateTimes, runTaskAssignment, getAssigningTask, getSiteSpecs,\
     getCloudSpecs, seeCloudTask, queryJobInfoPerCloud, registerProxyKey, getProxyKey,\
     getJobIDsInTimeRange, getPandIDsWithJobID, getFullJobStatus, getJobStatisticsForBamboo,\
     getNUserJobs, addSiteAccess, listSiteAccess, getFilesInUseForAnal, updateSiteAccess,\
     getPandaClientVer, getSlimmedFileInfoPandaIDs, getQueuedAnalJobs, getHighestPrioJobStat,\
     getActiveDatasets, setCloudTaskByUser, getSerialNumberForGroupJob,\
     checkMergeGenerationStatus, getNumPilots, retryFailedJobsInActive,\
     getJobStatisticsWithLabel, getPandaIDwithJobExeID, getJobStatisticsPerUserSite,\
     getDisInUseForAnal, getLFNsInUseForAnal, getScriptOfflineRunning, setDebugMode,\
     insertSandboxFileInfo, checkSandboxFile, changeJobPriorities, insertTaskParams,\
     killTask, finishTask, getJediTasksInTimeRange, getJediTaskDetails,\
     retryTask, getRetryHistory, changeTaskPriority, reassignTask, changeTaskAttributePanda,\
     pauseTask, resumeTask, increaseAttemptNrPanda, killUnfinishedJobs, changeTaskSplitRulePanda,\
     changeTaskModTimePanda, avalancheTask, getPandaIDsWithTaskID, reactivateTask, getTaskStatus, \
     reassignShare, listTasksInShare, getTaskParamsMap, updateWorkers, harvesterIsAlive,\
     reportWorkerStats, reportWorkerStats_jobtype, addHarvesterDialogs, \
     getJobStatisticsPerSiteResource, setNumSlotsForWP, reloadInput, enableJumboJobs, updateServiceMetrics, \
     getUserJobMetadata, getJumboJobDatasets, getGShareStatus,\
     sweepPQ,get_job_statistics_per_site_label_resource, relay_idds_command, send_command_to_job,\
     execute_idds_workflow_command, set_user_secret, get_user_secrets, get_ban_users

from pandaserver.userinterface import Client

# import error
import pandaserver.taskbuffer.ErrorCode


# initialize cx_Oracle using dummy connection
initializer.init()

# initialzie TaskBuffer
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, panda_config.nDBConnection, True)

# initialize JobDispatcher
if panda_config.nDBConnection != 0:
    jobDispatcher.init(taskBuffer)

# initialize DataService
if panda_config.nDBConnection != 0:
    dataService.init(taskBuffer)

# initialize UserIF
if panda_config.nDBConnection != 0:
    userIF.init(taskBuffer)

# import web I/F
allowedMethods = []

allowedMethods += ['isAlive', 'putFile', 'deleteFile', 'getServer', 'updateLog', 'fetchLog',
                   'touchFile', 'getVomsAttr', 'putEventPickingRequest', 'getAttr',
                   'uploadLog', 'put_checkpoint', 'delete_checkpoint', 'put_file_recovery_request',
                   'put_workflow_request']

allowedMethods += ['datasetCompleted', 'updateFileStatusInDisp']

allowedMethods += ['getJob', 'updateJob', 'getStatus', 'genPilotToken',
                   'getEventRanges', 'updateEventRange', 'getKeyPair',
                   'updateEventRanges', 'getDNsForS3', 'getProxy', 'getCommands', 'ackCommands',
                   'checkJobStatus', 'checkEventsAvailability', 'updateJobsInBulk', 'getResourceTypes',
                   'updateWorkerPilotStatus']

allowedMethods += ['submitJobs', 'getJobStatus', 'queryPandaIDs', 'killJobs', 'reassignJobs', 'getJobStatistics',
                   'getJobStatisticsPerSite', 'resubmitJobs', 'queryLastFilesInDataset', 'getPandaIDsSite',
                   'getJobsToBeUpdated', 'updateProdDBUpdateTimes', 'runTaskAssignment', 'getAssigningTask',
                   'getSiteSpecs', 'getCloudSpecs', 'seeCloudTask', 'queryJobInfoPerCloud', 'registerProxyKey',
                   'getProxyKey', 'getJobIDsInTimeRange', 'getPandIDsWithJobID', 'getFullJobStatus',
                   'getJobStatisticsForBamboo', 'getNUserJobs', 'addSiteAccess', 'listSiteAccess',
                   'getFilesInUseForAnal', 'updateSiteAccess', 'getPandaClientVer', 'getSlimmedFileInfoPandaIDs',
                   'getQueuedAnalJobs', 'getHighestPrioJobStat', 'getActiveDatasets', 'setCloudTaskByUser',
                   'getSerialNumberForGroupJob', 'checkMergeGenerationStatus', 'getNumPilots',
                   'retryFailedJobsInActive', 'getJobStatisticsWithLabel', 'getPandaIDwithJobExeID',
                   'getJobStatisticsPerUserSite', 'getDisInUseForAnal', 'getLFNsInUseForAnal', 'getScriptOfflineRunning',
                   'setDebugMode', 'insertSandboxFileInfo', 'checkSandboxFile', 'changeJobPriorities',
                   'insertTaskParams', 'killTask', 'finishTask', 'getJediTasksInTimeRange',
                   'getJediTaskDetails', 'retryTask', 'getRetryHistory', 'changeTaskPriority', 'reassignTask',
                   'changeTaskAttributePanda', 'pauseTask', 'resumeTask', 'increaseAttemptNrPanda',
                   'killUnfinishedJobs', 'changeTaskSplitRulePanda', 'changeTaskModTimePanda', 'avalancheTask',
                   'getPandaIDsWithTaskID', 'reactivateTask', 'getTaskStatus',
                   'reassignShare', 'listTasksInShare', 'getTaskParamsMap', 'updateWorkers', 'harvesterIsAlive',
                   'reportWorkerStats', 'reportWorkerStats_jobtype', 'addHarvesterDialogs',
                   'getJobStatisticsPerSiteResource', 'setNumSlotsForWP', 'reloadInput', 'enableJumboJobs',
                   'updateServiceMetrics', 'getUserJobMetadata', 'getJumboJobDatasets',
                   'getGShareStatus', 'sweepPQ', 'get_job_statistics_per_site_label_resource', 'relay_idds_command',
                   'send_command_to_job','execute_idds_workflow_command', 'set_user_secret', 'get_user_secrets',
                   'get_ban_users']


# FastCGI/WSGI entry
if panda_config.useFastCGI or panda_config.useWSGI:

    import os
    import cgi
    from pandacommon.pandalogger.PandaLogger import PandaLogger
    from pandacommon.pandalogger.LogWrapper import LogWrapper

    if panda_config.token_authType is None:
        pass
    elif panda_config.token_authType == 'scitokens':
        import scitokens
    else:
        from pandaserver.srvcore.oidc_utils import TokenDecoder
        token_decoder = TokenDecoder()

    # logger
    _logger = PandaLogger().getLogger('Entry')

    # ban list
    if panda_config.nDBConnection != 0:
        # get ban list directly from the database
        ban_user_list = CoreUtils.CachedObject('ban_list', 600, taskBuffer.get_ban_users, _logger)
    else:
        # get ban list from remote
        ban_user_list = CoreUtils.CachedObject('ban_list', 600, Client.get_ban_users, _logger)

    # dummy request object
    class DummyReq:
        def __init__(self, env, tmpLog):
            # environ
            self.subprocess_env = env
            # header
            self.headers_in = {}
            # authentication
            self.authenticated = True
            # message
            self.message = None
            # content-length
            if 'CONTENT_LENGTH' in self.subprocess_env:
                self.headers_in["content-length"] = self.subprocess_env['CONTENT_LENGTH']
            # tokens
            try:
                if panda_config.token_authType in ['scitokens', 'oidc'] and 'HTTP_AUTHORIZATION' in env:
                    serialized_token = env['HTTP_AUTHORIZATION'].split()[1]
                    role = None
                    if panda_config.token_authType == 'oidc':
                        self.authenticated = False
                        if 'HTTP_ORIGIN' in env:
                            vo = env['HTTP_ORIGIN']
                            vo_group = vo.replace(':', '.')
                        else:
                            vo = None
                            vo_group = ''
                        token = token_decoder.deserialize_token(serialized_token, panda_config.auth_config,
                                                                vo, tmpLog)
                        # extract role
                        if vo:
                            vo = token["vo"].split(':')[0]
                            vo = vo.split('.')[0]
                            if vo != token["vo"]:
                                role = token["vo"].split(':')[-1]
                                role = role.split('.')[-1]
                        # check vo
                        if vo not in panda_config.auth_policies:
                            self.message = 'Unknown vo : {}'.format(vo)
                            tmpLog.error('{} - {}'.format(self.message, env['HTTP_AUTHORIZATION']))
                        else:
                            # robot
                            if vo_group in  panda_config.auth_vo_dict and \
                                    'robot_ids' in panda_config.auth_vo_dict[vo_group]:
                                robot_ids = [i for i in
                                             panda_config.auth_vo_dict[vo_group].get('robot_ids').split(',') if i]
                                if token['sub'] in robot_ids:
                                    if 'groups' not in token:
                                        if role:
                                            token['groups'] = ['{}/{}'.format(vo, role)]
                                        else:
                                            token['groups'] = ['{}'.format(vo)]
                                    if 'name' not in token:
                                        token['name'] = 'robot {}'.format(role)
                            # check role
                            if role:
                                if '{}/{}'.format(vo, role) not in token["groups"]:
                                    self.message = 'Not a member of the {}/{} group'.format(vo, role)
                                    tmpLog.error('{} - {}'.format(self.message, env['HTTP_AUTHORIZATION']))
                                else:
                                    self.subprocess_env['PANDA_OIDC_VO'] = vo
                                    self.subprocess_env['PANDA_OIDC_GROUP'] = role
                                    self.subprocess_env['PANDA_OIDC_ROLE'] = role
                                    self.authenticated = True
                            else:
                                for memberStr, memberInfo in panda_config.auth_policies[vo]:
                                    if memberStr in token["groups"]:
                                        self.subprocess_env['PANDA_OIDC_VO'] = vo
                                        self.subprocess_env['PANDA_OIDC_GROUP'] = memberInfo['group']
                                        self.subprocess_env['PANDA_OIDC_ROLE'] = memberInfo['role']
                                        self.authenticated = True
                                        break
                            if not self.authenticated:
                                self.message = 'Not a member of the {} group'.format(vo)
                                tmpLog.error('{} - {}'.format(self.message, env['HTTP_AUTHORIZATION']))
                    else:
                        token = scitokens.SciToken.deserialize(serialized_token, audience=panda_config.token_audience)
                    # check issuer
                    if 'iss' not in token:
                        self.message = 'Issuer is undefined in the token'
                        tmpLog.error(self.message)
                    else:
                        if panda_config.token_authType == 'scitokens':
                            items = token.claims()
                        else:
                            items = six.iteritems(token)
                        for c, v in items:
                            self.subprocess_env['PANDA_OIDC_CLAIM_{0}'.format(str(c))] = str(v)
                        # use sub and scope as DN and FQAN
                        if 'SSL_CLIENT_S_DN' not in self.subprocess_env:
                            if 'name' in token:
                                self.subprocess_env['SSL_CLIENT_S_DN'] = ' '.join(
                                    [t[:1].upper() + t[1:].lower() for t in str(token['name']).split()])
                                if 'preferred_username' in token:
                                    self.subprocess_env['SSL_CLIENT_S_DN'] += \
                                        '/CN=nickname:{}'.format(token['preferred_username'])
                            else:
                                self.subprocess_env['SSL_CLIENT_S_DN'] = str(token['sub'])
                            i = 0
                            for scope in token.get('scope', '').split():
                                if scope.startswith('role:'):
                                    self.subprocess_env['GRST_CRED_AUTH_TOKEN_{0}'.format(i)] = \
                                        'VOMS ' + str(scope.split(':')[-1])
                                    i += 1
                            if role:
                                self.subprocess_env['GRST_CRED_AUTH_TOKEN_{0}'.format(i)] = \
                                    'VOMS /{}/Role={}'.format(vo, role)
                                i += 1
            except Exception as e:
                self.message = 'Corrupted token. {}'.format(str(e))
                tmpLog.debug('Origin: {}, Token: {}\n{}'.format(env.get('HTTP_ORIGIN', None),
                                                                env.get('HTTP_AUTHORIZATION', None),
                                                                traceback.format_exc()))

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
        tmpLog = LogWrapper(_logger, "PID={0} {1}".format(os.getpid(), methodName), seeMem=True)
        cont_length = int(environ.get('CONTENT_LENGTH', 0))
        json_body = environ.get('CONTENT_TYPE', None) == 'application/json'
        tmpLog.debug("start content-length={} json={}".format(cont_length, json_body))
        regStart = datetime.datetime.utcnow()
        retType = None
        # check method name
        if methodName not in allowedMethods:
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
                body = b''
                try:
                    # dummy request object
                    dummyReq = DummyReq(environ, tmpLog)
                    if not dummyReq.authenticated:
                        errMsg = "Token authentication failed. {}".format(dummyReq.message)
                        tmpLog.error(errMsg)
                        start_response('403 Forbidden', [('Content-Type', 'text/plain')])
                        return ["ERROR : {}".format(errMsg).encode()]
                    username = dummyReq.subprocess_env.get('SSL_CLIENT_S_DN', None)
                    if username:
                        username = CoreUtils.clean_user_id(username)
                        if username in ban_user_list:
                            errMsg = '{} is banned'.format(username)
                            tmpLog.error(errMsg)
                            start_response('403 Forbidden', [('Content-Type', 'text/plain')])
                            return ["ERROR : {}".format(errMsg).encode()]
                    # read contents
                    while cont_length > 0:
                        chunk = environ['wsgi.input'].read(min(cont_length, 1024*1024))
                        if not chunk:
                            break
                        cont_length -= len(chunk)
                        body += chunk
                    if cont_length > 0:
                        raise OSError('partial read from client. {} bytes remaining'.format(cont_length))
                    if not json_body:
                        # query string
                        environ['wsgi.input'] = io.BytesIO(body)
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
                    else:
                        # json
                        body = gzip.decompress(body)
                        params = json.loads(body)
                        # patch for True/False
                        for k in list(params):
                            if params[k] is True:
                                params[k] = 'True'
                            elif params[k] is False:
                                params[k] = 'False'
                    if panda_config.entryVerbose:
                        tmpLog.debug("with %s" % str(list(params)))
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
                    tmpLog.error("execution failure : {0}\n {1}".format(str(e), traceback.format_exc()))
                    if hasattr(panda_config, 'dumpBadRequest') and panda_config.dumpBadRequest:
                        try:
                            with tempfile.NamedTemporaryFile(delete=False, prefix='req_dump_') as f:
                                environ['WSGI_INPUT_DUMP'] = f.name
                                f.write(body)
                                os.chmod(f.name, 0o775)
                        except Exception:
                            tmpLog.error(traceback.format_exc())
                            pass
                    errStr = ""
                    for tmpKey in environ:
                        tmpVal = environ[tmpKey]
                        errStr += "%s : %s\n" % (tmpKey,str(tmpVal))
                    tmpLog.error(errStr)
                    # return internal server error
                    start_response('500 INTERNAL SERVER ERROR', [('Content-Type', 'text/plain')])
                    # force kill to release memory
                    if type(e) == OSError:
                        tmpLog.warning('force restart due')
                        os.kill(os.getpid(), signal.SIGINT)
                    return [str(e).encode()]
        if panda_config.entryVerbose:
            tmpLog.debug("done")
        regTime = datetime.datetime.utcnow() - regStart
        tmpLog.info("exec_time=%s.%03d sec, return len=%s B" % (regTime.seconds,
                                                                regTime.microseconds/1000,
                                                                len(str(exeRes))))
        # return
        if exeRes == pandaserver.taskbuffer.ErrorCode.EC_NotFound:
            start_response('404 Not Found', [('Content-Type', 'text/plain')])
            return ['not found'.encode()]
        elif isinstance(exeRes, pandaserver.taskbuffer.ErrorCode.EC_Redirect):
            start_response('302 Redirect', [('Location', exeRes.url)])
            return ['redirect'.encode()]
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
