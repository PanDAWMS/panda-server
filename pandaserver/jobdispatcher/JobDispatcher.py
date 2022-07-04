"""
dispatch jobs

"""

import re
import os
import sys
import json
import threading
from pandaserver.jobdispatcher import Protocol
import time
import socket
import datetime
import traceback
from threading import Lock
from pandaserver.config import panda_config
from pandaserver.dataservice.AdderGen import AdderGen
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.jobdispatcher import DispatcherUtils
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.proxycache import panda_proxy_cache
from pandaserver.srvcore import CoreUtils

# logger
_logger = PandaLogger().getLogger('JobDispatcher')
_pilotReqLogger = PandaLogger().getLogger('PilotRequests')

# a wrapper to install timeout into a method
class _TimedMethod:
    def __init__(self,method,timeout):
        self.method  = method
        self.timeout = timeout
        self.result  = Protocol.TimeOutToken

    # method emulation
    def __call__(self,*var):
        self.result = self.method(*var)

    # run
    def run(self,*var):
        #_logger.debug(self.method.__name__ + ' ' + str(var))
        thr = threading.Thread(target=self,args=var)
        # run thread
        thr.start()
        thr.join() #self.timeout)


# cached object
class CachedObject:
    # constructor
    def __init__(self,timeInterval,updateFunc):
        # cached object
        self.cachedObj = None
        # datetime of last updated
        self.lastUpdated = datetime.datetime.utcnow()
        # how frequently update DN/token map
        self.timeInterval = datetime.timedelta(seconds=timeInterval)
        # lock
        self.lock = Lock()
        # function to update object
        self.updateFunc = updateFunc

    # update obj
    def update(self):
        # get current datetime
        current = datetime.datetime.utcnow()
        # lock
        self.lock.acquire()
        # update if old
        if self.cachedObj is None or current-self.lastUpdated > self.timeInterval:
            self.cachedObj = self.updateFunc()
            self.lastUpdated = current
        # release
        self.lock.release()
        # return
        return

    # contains
    def __contains__(self,item):
        contains = False
        try:
            contains = item in self.cachedObj
        except TypeError:
            pass
        return contains

    # get item
    def __getitem__(self,name):
        return self.cachedObj[name]

    # get object
    def getObj(self):
        self.lock.acquire()
        return self.cachedObj

    # release object
    def releaseObj(self):
        self.lock.release()


# job dipatcher
class JobDipatcher:
    # constructor
    def __init__(self):
        # taskbuffer
        self.taskBuffer = None
        # DN/token map
        self.tokenDN = None
        # datetime of last updated
        self.lastUpdated = datetime.datetime.utcnow()
        # how frequently update DN/token map
        self.timeInterval = datetime.timedelta(seconds=180)
        # pilot owners
        self.pilotOwners = None
        # special dispatcher parameters
        self.specialDispatchParams = None
        # site mapper cache
        self.siteMapperCache = None
        # lock
        self.lock = Lock()


    # set task buffer
    def init(self,taskBuffer):
        # lock
        self.lock.acquire()
        # set TB
        if self.taskBuffer is None:
            self.taskBuffer = taskBuffer
        # update DN/token map
        if self.tokenDN is None:
            self.tokenDN = self.taskBuffer.getListSchedUsers()
        # get pilot owners
        if self.pilotOwners is None:
            self.pilotOwners = self.taskBuffer.getPilotOwners()
        # special dipatcher parameters
        if self.specialDispatchParams is None:
            self.specialDispatchParams = CachedObject(60*10, self.taskBuffer.getSpecialDispatchParams)
        # site mapper cache
        if self.siteMapperCache is None:
            self.siteMapperCache = CachedObject(60*10, self.getSiteMapper)
        # release
        self.lock.release()


    # set user proxy
    def setUserProxy(self, response, realDN=None, role=None):
        try:
            if realDN is None:
                realDN = response.data['prodUserID']
            # remove redundant extensions
            realDN = CoreUtils.get_bare_dn(realDN, keep_digits=False)
            pIF = panda_proxy_cache.MyProxyInterface()
            tmpOut = pIF.retrieve(realDN,role=role)
            # not found
            if tmpOut is None:
                tmpMsg = 'proxy not found for {0}'.format(realDN)
                response.appendNode('errorDialog',tmpMsg)
                return False,tmpMsg
            # set
            response.appendNode('userProxy',tmpOut)
            return True,''
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            tmpMsg = "proxy retrieval failed with {0} {1}".format(errtype.__name__,errvalue)
            response.appendNode('errorDialog',tmpMsg)
            return False,tmpMsg


    # get job
    def getJob(self, siteName, prodSourceLabel, cpu, mem, diskSpace, node, timeout, computingElement,
               atlasRelease, prodUserID, getProxyKey, countryGroup, workingGroup, allowOtherCountry,
               realDN, taskID, nJobs, acceptJson, background, resourceType, harvester_id, worker_id,
               schedulerID, jobType, via_topic, tmpLog):

        t_getJob_start = time.time()
        jobs = []
        useProxyCache = False
        try:
            tmpNumJobs = int(nJobs)
        except Exception:
            tmpNumJobs = None
        if tmpNumJobs is None:
            tmpNumJobs = 1

        self.siteMapperCache.update()
        is_gu = self.siteMapperCache.cachedObj.getSite(siteName).is_grandly_unified()
        in_test = self.siteMapperCache.cachedObj.getSite(siteName).status == 'test'

        # change label
        if in_test and prodSourceLabel in ['user', 'managed', 'unified']:
            new_label = 'test'
            tmpLog.debug("prodSourceLabel changed {} -> {}".format(prodSourceLabel, new_label))
            prodSourceLabel = new_label

        # wrapper function for timeout
        tmpWrapper = _TimedMethod(self.taskBuffer.getJobs, timeout)
        tmpWrapper.run(tmpNumJobs, siteName, prodSourceLabel, cpu, mem, diskSpace, node, timeout, computingElement,
                       atlasRelease, prodUserID, getProxyKey, countryGroup, workingGroup, allowOtherCountry,
                       taskID, background, resourceType, harvester_id, worker_id, schedulerID, jobType, is_gu,
                       via_topic)

        if isinstance(tmpWrapper.result, list):
            jobs = jobs + tmpWrapper.result
        # make response
        secrets_map = {}
        if len(jobs) > 0:
            secrets_map = jobs.pop()
            proxyKey = jobs[-1]
            nSent = jobs[-2]
            jobs = jobs[:-2]
        if len(jobs) != 0:
            # succeed
            responseList = []
            # append Jobs
            for tmpJob in jobs:
                try:
                    response=Protocol.Response(Protocol.SC_Success)
                    response.appendJob(tmpJob, self.siteMapperCache)
                except Exception as e:
                    tmpMsg = "failed to get jobs with {0}".format(str(e))
                    tmpLog.error(tmpMsg + '\n' + traceback.format_exc())
                    raise

                # append nSent
                response.appendNode('nSent', nSent)
                # set proxy key
                if getProxyKey:
                    response.setProxyKey(proxyKey)
                # set secrets
                if tmpJob.prodUserName in secrets_map and secrets_map[tmpJob.prodUserName]:
                    response.appendNode('secrets', secrets_map[tmpJob.prodUserName])
                if panda_config.pilot_secrets in secrets_map and secrets_map[panda_config.pilot_secrets]:
                    response.appendNode('pilotSecrets', secrets_map[panda_config.pilot_secrets])
                # check if proxy cache is used
                if hasattr(panda_config,'useProxyCache') and panda_config.useProxyCache is True:
                    self.specialDispatchParams.update()
                    if 'proxyCacheSites' not in self.specialDispatchParams:
                        proxyCacheSites = {}
                    else:
                        proxyCacheSites = self.specialDispatchParams['proxyCacheSites']
                    if siteName in proxyCacheSites:
                        useProxyCache = True
                # set proxy
                if useProxyCache:
                    try:
                        #  get compact
                        compactDN = self.taskBuffer.cleanUserID(realDN)
                        # check permission
                        self.specialDispatchParams.update()
                        if 'allowProxy' not in self.specialDispatchParams:
                            allowProxy = []
                        else:
                            allowProxy = self.specialDispatchParams['allowProxy']
                        if compactDN not in allowProxy:
                            tmpLog.warning("%s %s '%s' no permission to retrieve user proxy" % (siteName, node,
                                                                                                compactDN))
                        else:
                            if useProxyCache:
                                tmpStat,tmpOut = self.setUserProxy(response,
                                                                   proxyCacheSites[siteName]['dn'],
                                                                   proxyCacheSites[siteName]['role'])
                            else:
                                tmpStat,tmpOut = self.setUserProxy(response)
                            if not tmpStat:
                                tmpLog.warning("%s %s failed to get user proxy : %s" % (siteName, node,
                                                                                        tmpOut))
                    except Exception as e:
                        tmpLog.warning("%s %s failed to get user proxy with %s" % (siteName, node, str(e)))
                # panda proxy
                if 'pandaProxySites' in self.specialDispatchParams and siteName in self.specialDispatchParams['pandaProxySites'] \
                        and (EventServiceUtils.isEventServiceJob(tmpJob) or EventServiceUtils.isEventServiceMerge(tmpJob)):
                    # get secret key
                    tmpSecretKey,tmpErrMsg = DispatcherUtils.getSecretKey(tmpJob.PandaID)
                    if tmpSecretKey is None:
                        tmpLog.warning("PandaID=%s site=%s failed to get panda proxy secret key : %s" % (tmpJob.PandaID,
                                                                                                         siteName,
                                                                                                         tmpErrMsg))
                    else:
                        # set secret key
                        tmpLog.debug("PandaID=%s set key=%s" % (tmpJob.PandaID, tmpSecretKey))
                        response.setPandaProxySecretKey(tmpSecretKey)
                # add
                responseList.append(response.data)
            # make response for bulk
            if nJobs is not None:
                try:
                    response = Protocol.Response(Protocol.SC_Success)
                    if not acceptJson:
                        response.appendNode('jobs',json.dumps(responseList))
                    else:
                        response.appendNode('jobs',responseList)
                except Exception as e:
                    tmpMsg = "failed to make response with {0}".format(str(e))
                    tmpLog.error(tmpMsg + '\n' + traceback.format_exc())
                    raise

        else:
            if tmpWrapper.result == Protocol.TimeOutToken:
                # timeout
                if acceptJson:
                    response = Protocol.Response(Protocol.SC_TimeOut, 'database timeout')
                else:
                    response = Protocol.Response(Protocol.SC_TimeOut)
            else:
                # no available jobs
                if acceptJson:
                    response = Protocol.Response(Protocol.SC_NoJobs, 'no jobs in PanDA')
                else:
                    response = Protocol.Response(Protocol.SC_NoJobs)
                _pilotReqLogger.info('method=noJob,site=%s,node=%s,type=%s' % (siteName, node, prodSourceLabel))
        # return
        tmpLog.debug("%s %s ret -> %s" % (siteName, node, response.encode(acceptJson)))

        t_getJob_end = time.time()
        t_getJob_spent = t_getJob_end - t_getJob_start
        tmpLog.info("siteName={0} took timing={1}s in_test={2}".format(siteName, t_getJob_spent,
                                                                       in_test))
        return response.encode(acceptJson)


    # update job status
    def updateJob(self, jobID, jobStatus, timeout, xml, siteName, param, metadata, pilotLog,
                  attemptNr=None, stdout='', acceptJson=False):

        # pilot log
        if pilotLog != '':
            _logger.debug('saving pilot log')
            try:
                self.taskBuffer.storePilotLog(int(jobID), pilotLog)
                _logger.debug('saving pilot log DONE')
            except Exception:
                _logger.debug('saving pilot log FAILED')
        # add metadata
        if metadata != '':
            ret = self.taskBuffer.addMetadata([jobID],[metadata],[jobStatus])
            if len(ret) > 0 and not ret[0]:
                _logger.debug("updateJob : %s failed to add metadata" % jobID)
                # return succeed
                response=Protocol.Response(Protocol.SC_Success)
                return response.encode(acceptJson)
        # add stdout
        if stdout != '':
            self.taskBuffer.addStdOut(jobID,stdout)
        # update
        tmpStatus = jobStatus
        updateStateChange = False
        if jobStatus == 'failed' or jobStatus == 'finished':
            tmpStatus = 'holding'
            # update stateChangeTime to prevent Watcher from finding this job
            updateStateChange = True
            param['jobDispatcherErrorDiag'] = None
        elif jobStatus in ['holding','transferring']:
            param['jobDispatcherErrorDiag'] = 'set to {0} by the pilot at {1}'.format(jobStatus,datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))
        if tmpStatus == 'holding':
            tmpWrapper = _TimedMethod(self.taskBuffer.updateJobStatus,None)
        else:
            tmpWrapper = _TimedMethod(self.taskBuffer.updateJobStatus,timeout)
        tmpWrapper.run(jobID,tmpStatus,param,updateStateChange,attemptNr)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result:
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                result = tmpWrapper.result
                secrets = None
                if isinstance(result, dict):
                    if 'secrets' in result:
                        secrets = result['secrets']
                    result = result['command']
                # set command
                if isinstance(result, str):
                    response.appendNode('command', result)
                    if secrets:
                        response.appendNode('pilotSecrets', secrets)
                else:
                    response.appendNode('command', 'NULL')
                # add output to dataset
                if result not in ["badattemptnr", "alreadydone"] and (jobStatus == 'failed' or jobStatus == 'finished'):
                    adder_gen = AdderGen(self.taskBuffer, jobID, jobStatus, attemptNr)
                    adder_gen.dumpFileReport(xml, attemptNr)
                    del adder_gen
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("updateJob : %s ret -> %s" % (jobID,response.encode(acceptJson)))
        return response.encode(acceptJson)


    # get job status
    def getStatus(self,strIDs,timeout):
        # convert str to list
        ids = strIDs.split()
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.peekJobs,timeout)
        tmpWrapper.run(ids,False,True,True,False)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if isinstance(tmpWrapper.result, list):
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                # make return
                retStr = ''
                attStr = ''
                for job in tmpWrapper.result:
                    if job is None:
                        retStr += '%s+' % 'notfound'
                        attStr += '0+'
                    else:
                        retStr += '%s+' % job.jobStatus
                        attStr += '%s+' % job.attemptNr
                response.appendNode('status',retStr[:-1])
                response.appendNode('attemptNr',attStr[:-1])
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("getStatus : %s ret -> %s" % (strIDs,response.encode()))
        return response.encode()


    # check job status
    def checkJobStatus(self, pandaIDs, timeout):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.checkJobStatus,timeout)
        tmpWrapper.run(pandaIDs)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if isinstance(tmpWrapper.result, list):
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                response.appendNode('data', tmpWrapper.result)
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("checkJobStatus : %s ret -> %s" % (pandaIDs, response.encode(True)))
        return response.encode(True)


    # get a list of event ranges for a PandaID
    def getEventRanges(self, pandaID, jobsetID, jediTaskID, nRanges, timeout, acceptJson, scattered, segment_id):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.getEventRanges,timeout)
        tmpWrapper.run(pandaID,jobsetID,jediTaskID,nRanges,acceptJson,scattered,segment_id)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result is not None:
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                # make return
                response.appendNode('eventRanges',tmpWrapper.result)
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("getEventRanges : %s ret -> %s" % (pandaID,response.encode(acceptJson)))
        return response.encode(acceptJson)


    # update an event range
    def updateEventRange(self, eventRangeID, eventStatus, coreCount, cpuConsumptionTime, objstoreID, timeout):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.updateEventRange,timeout)
        tmpWrapper.run(eventRangeID,eventStatus,coreCount,cpuConsumptionTime,objstoreID)
        # make response
        _logger.debug(str(tmpWrapper.result))
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result[0] is True:
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                response.appendNode('Command',tmpWrapper.result[1])
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("updateEventRange : %s ret -> %s" % (eventRangeID,response.encode()))
        return response.encode()


    # update event ranges
    def updateEventRanges(self, eventRanges, timeout, acceptJson, version):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.updateEventRanges,timeout)
        tmpWrapper.run(eventRanges,version)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            # succeed
            response=Protocol.Response(Protocol.SC_Success)
            # make return
            response.appendNode('Returns',tmpWrapper.result[0])
            response.appendNode('Command',tmpWrapper.result[1])
        _logger.debug("updateEventRanges : ret -> %s" % (response.encode(acceptJson)))
        return response.encode(acceptJson)


    # check event availability
    def checkEventsAvailability(self, pandaID, jobsetID, jediTaskID, timeout):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.checkEventsAvailability,timeout)
        tmpWrapper.run(pandaID, jobsetID, jediTaskID)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result is not None:
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                # make return
                response.appendNode('nEventRanges',tmpWrapper.result)
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("checkEventsAvailability : %s ret -> %s" % (pandaID,response.encode(True)))
        return response.encode(True)


    # get DN/token map
    def getDnTokenMap(self):
        # get current datetime
        current = datetime.datetime.utcnow()
        # lock
        self.lock.acquire()
        # update DN map if old
        if current-self.lastUpdated > self.timeInterval:
            # get new map
            self.tokenDN = self.taskBuffer.getListSchedUsers()
            # reset
            self.lastUpdated = current
        # release
        self.lock.release()
        # return
        return self.tokenDN


    # generate pilot token
    def genPilotToken(self, schedulerhost, scheduleruser, schedulerid):
        retVal = self.taskBuffer.genPilotToken(schedulerhost, scheduleruser, schedulerid)
        # failed
        if retVal is None:
            return "ERROR : failed to generate token"
        return "SUCCEEDED : " + retVal


    # get key pair
    def getKeyPair(self,realDN,publicKeyName,privateKeyName,acceptJson):
        tmpMsg = "getKeyPair {0}/{1} json={2}: ".format(publicKeyName,privateKeyName,acceptJson)
        if realDN is None:
            # cannot extract DN
            tmpMsg += "failed since DN cannot be extracted"
            _logger.debug(tmpMsg)
            response = Protocol.Response(Protocol.SC_Perms,'Cannot extract DN from proxy. not HTTPS?')
        else:
            # get compact DN
            compactDN = self.taskBuffer.cleanUserID(realDN)
            # check permission
            self.specialDispatchParams.update()
            if 'allowKey' not in self.specialDispatchParams:
                allowKey = []
            else:
                allowKey = self.specialDispatchParams['allowKey']
            if compactDN not in allowKey:
                # permission denied
                tmpMsg += "failed since '{0}' not in the authorized user list who have 'k' in {1}.USERS.GRIDPREF".format(compactDN,
                                                                                                                        panda_config.schemaMETA)
                _logger.debug(tmpMsg)
                response = Protocol.Response(Protocol.SC_Perms,tmpMsg)
            else:
                # look for key pair
                if 'keyPair' not in self.specialDispatchParams:
                    keyPair = {}
                else:
                    keyPair = self.specialDispatchParams['keyPair']
                notFound = False
                if publicKeyName not in keyPair:
                    # public key is missing
                    notFound = True
                    tmpMsg += "failed for '{2}' since {0} is missing on {1}".format(publicKeyName,socket.getfqdn(),compactDN)
                elif privateKeyName not in keyPair:
                    # private key is missing
                    notFound = True
                    tmpMsg += "failed for '{2}' since {0} is missing on {1}".format(privateKeyName,socket.getfqdn(),compactDN)
                if notFound:
                    # private or public key is missing
                    _logger.debug(tmpMsg)
                    response = Protocol.Response(Protocol.SC_MissKey,tmpMsg)
                else:
                    # key pair is available
                    response = Protocol.Response(Protocol.SC_Success)
                    response.appendNode('publicKey',keyPair[publicKeyName])
                    response.appendNode('privateKey',keyPair[privateKeyName])
                    tmpMsg += "sent key-pair to '{0}'".format(compactDN)
                    _logger.debug(tmpMsg)
        # return
        return response.encode(acceptJson)


    # get DNs authorized for S3
    def getDNsForS3(self):
        # check permission
        self.specialDispatchParams.update()
        if 'allowKey' not in self.specialDispatchParams:
            allowKey = []
        else:
            allowKey = self.specialDispatchParams['allowKey']
            allowKey = filter(None,allowKey)
        # return
        return json.dumps(allowKey)


    # get site mapper
    def getSiteMapper(self):
        return SiteMapper(self.taskBuffer)

    def getCommands(self, harvester_id, n_commands, timeout, accept_json):
        """
        Get commands for a particular harvester instance
        """
        tmp_wrapper = _TimedMethod(self.taskBuffer.getCommands, timeout)
        tmp_wrapper.run(harvester_id, n_commands)

        # Make response
        if tmp_wrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            # success
            response = Protocol.Response(Protocol.SC_Success)
            response.appendNode('Returns', tmp_wrapper.result[0])
            response.appendNode('Commands', tmp_wrapper.result[1])

        _logger.debug("getCommands : ret -> %s" % (response.encode(accept_json)))
        return response.encode(accept_json)

    def ackCommands(self, command_ids, timeout, accept_json):
        """
        Acknowledge the commands from a list of IDs
        """
        _logger.debug("command_ids : {0}".format(command_ids))
        tmp_wrapper = _TimedMethod(self.taskBuffer.ackCommands, timeout)
        tmp_wrapper.run(command_ids)

        # Make response
        if tmp_wrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            # success
            response = Protocol.Response(Protocol.SC_Success)
            response.appendNode('Returns', tmp_wrapper.result)

        _logger.debug("ackCommands : ret -> %s" % (response.encode(accept_json)))
        return response.encode(accept_json)

    def getResourceTypes(self, timeout, accept_json):
        """
        Get resource types (SCORE, MCORE, SCORE_HIMEM, MCORE_HIMEM) and their definitions
        """
        tmp_wrapper = _TimedMethod(self.taskBuffer.getResourceTypes, timeout)
        tmp_wrapper.run()

        # Make response
        if tmp_wrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            # success
            response = Protocol.Response(Protocol.SC_Success)
            response.appendNode('Returns', 0)
            response.appendNode('ResourceTypes', tmp_wrapper.result)

        _logger.debug("getResourceTypes : ret -> %s" % (response.encode(accept_json)))
        return response.encode(accept_json)

    # get proxy
    def getProxy(self, realDN, role, targetDN):
        if targetDN is None:
            targetDN = realDN
        tmpLog = LogWrapper(_logger, 'getProxy PID={}'.format(os.getpid()))
        tmpMsg = 'start DN="{}" role={} target="{}" '.format(realDN, role, targetDN)
        tmpLog.debug(tmpMsg)
        if realDN is None:
            # cannot extract DN
            tmpMsg += "failed since DN cannot be extracted"
            tmpLog.debug(tmpMsg)
            response = Protocol.Response(Protocol.SC_Perms,'Cannot extract DN from proxy. not HTTPS?')
        else:
            # get compact DN
            compactDN = self.taskBuffer.cleanUserID(realDN)
            # check permission
            self.specialDispatchParams.update()
            if 'allowProxy' not in self.specialDispatchParams:
                allowProxy = []
            else:
                allowProxy = self.specialDispatchParams['allowProxy']
            if compactDN not in allowProxy:
                # permission denied
                tmpMsg += "failed since '{0}' not in the authorized user list who have 'p' in {1}.USERS.GRIDPREF ".format(compactDN,
                                                                                                                          panda_config.schemaMETA)
                tmpMsg += "to get proxy"
                tmpLog.debug(tmpMsg)
                response = Protocol.Response(Protocol.SC_Perms,tmpMsg)
            else:
                # get proxy
                response = Protocol.Response(Protocol.SC_Success,'')
                tmpStat,tmpMsg = self.setUserProxy(response, targetDN, role)
                if not tmpStat:
                    tmpLog.debug(tmpMsg)
                    response.appendNode('StatusCode',Protocol.SC_ProxyError)
                else:
                    tmpMsg = "successful sent proxy"
                    tmpLog.debug(tmpMsg)
        # return
        return response.encode(True)

    # get active job attribute
    def getActiveJobAttributes(self, pandaID, attrs):
        return self.taskBuffer.getActiveJobAttributes(pandaID, attrs)

    # update job status
    def updateWorkerPilotStatus(self, workerID, harvesterID, status, timeout, accept_json):

        tmp_wrapper = _TimedMethod(self.taskBuffer.updateWorkerPilotStatus, timeout)
        tmp_wrapper.run(workerID, harvesterID, status)

        # make response
        if tmp_wrapper.result == Protocol.TimeOutToken:
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmp_wrapper.result:  # success
                response = Protocol.Response(Protocol.SC_Success)
            else:  # error
                response = Protocol.Response(Protocol.SC_Failed)
        _logger.debug("updateWorkerPilotStatus : {0} {1} {2} ret -> {3}"
                      .format(workerID, harvesterID, status, response.encode(accept_json)))
        return response.encode(accept_json)


# Singleton
jobDispatcher = JobDipatcher()
del JobDipatcher


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmpKey in req.subprocess_env:
        tmpVal = req.subprocess_env[tmpKey]
        # compact credentials
        if tmpKey.startswith('GRST_CRED_'):
            # VOMS attribute
            if tmpVal.startswith('VOMS'):
                # FQAN
                fqan = tmpVal.split()[-1]
                # append
                fqans.append(fqan)
        # old style
        elif tmpKey.startswith('GRST_CONN_'):
            tmpItems = tmpVal.split(':')
            # FQAN
            if len(tmpItems)==2 and tmpItems[0]=='fqan':
                fqans.append(tmpItems[-1])
    # return
    return fqans


# check role
def _checkRole(fqans,dn,jdCore,withVomsPatch=True,site='',hostname=''):
    prodManager = False
    try:
        # VOMS attributes of production and pilot roles
        prodAttrs = ['/atlas/usatlas/Role=production',
                     '/atlas/usatlas/Role=pilot',
                     '/atlas/Role=production',
                     '/atlas/Role=pilot',
                     '/osg/Role=pilot',
                     '^/[^/]+/Role=production$',
                     '/ams/Role=pilot',
                     '/Engage/LBNE/Role=pilot',
                     ]
        if withVomsPatch:
            # FIXEME once http://savannah.cern.ch/bugs/?47136 is solved
            prodAttrs += ['/atlas/']
            prodAttrs += ['/osg/','/cms/','/ams/']
            prodAttrs += ['/Engage/LBNE/']
        for fqan in fqans:
            # check atlas/usatlas production role
            for rolePat in prodAttrs:
                if fqan.startswith(rolePat):
                    prodManager = True
                    break
                if re.search(rolePat, fqan):
                    prodManager = True
                    break
            # escape
            if prodManager:
                break
        # service proxy for CERNVM
        if site in ['CERNVM']:
            serviceSubjects = ['/DC=ch/DC=cern/OU=computers/CN=pilot/copilot.cern.ch']
            for tmpSub in serviceSubjects:
                if dn.startswith(tmpSub):
                    prodManager = True
                    break
        # check DN with pilotOwners
        if (not prodManager) and (dn not in [None]):
            if site in jdCore.pilotOwners:
                tmpPilotOwners = jdCore.pilotOwners[None].union(jdCore.pilotOwners[site])
            else:
                tmpPilotOwners = jdCore.pilotOwners[None]
            for owner in set(panda_config.production_dns).union(tmpPilotOwners):
                if not owner:
                    continue
                # check
                if re.search(owner, dn) is not None:
                    prodManager = True
                    break
    except Exception:
        pass
    # return
    return prodManager


# get DN
def _getDN(req):
    realDN = None
    if 'SSL_CLIENT_S_DN' in req.subprocess_env:
        realDN = req.subprocess_env['SSL_CLIENT_S_DN']
        # remove redundant CN
        realDN = CoreUtils.get_bare_dn(realDN, keep_proxy=True)
    # return
    return realDN


# check token
def _checkToken(token,jdCore):
    # not check None until all pilots use tokens
    if token is None:
        return True
    # get map
    tokenDN = jdCore.getDnTokenMap()
    # return
    return token in tokenDN


"""
web service interface

"""

# get job
def getJob(req, siteName, token=None, timeout=60, cpu=None, mem=None, diskSpace=None, prodSourceLabel=None, node=None,
           computingElement=None, AtlasRelease=None, prodUserID=None, getProxyKey=None, countryGroup=None,
           workingGroup=None, allowOtherCountry=None, taskID=None, nJobs=None, background=None, resourceType=None,
           harvester_id=None, worker_id=None, schedulerID=None, jobType=None, viaTopic=None):
    tmpLog = LogWrapper(_logger, 'getJob {}'.format(datetime.datetime.utcnow().isoformat('/')))
    tmpLog.debug(siteName)
    # get DN
    realDN = _getDN(req)
    # get FQANs
    fqans = _getFQAN(req)
    # check production role
    if getProxyKey == 'True':
        # don't use /atlas to prevent normal proxy getting credname
        prodManager = _checkRole(fqans,realDN,jobDispatcher,False,site=siteName)
    else:
        prodManager = _checkRole(fqans,realDN,jobDispatcher,site=siteName,
                                 hostname=req.get_remote_host())
    # check token
    validToken = _checkToken(token,jobDispatcher)
    # set DN for non-production user
    if not prodManager:
        prodUserID = realDN
    # allow getProxyKey for production role
    if getProxyKey == 'True' and prodManager:
        getProxyKey = True
    else:
        getProxyKey = False
    # convert mem and diskSpace
    try:
        mem = int(float(mem))
        if mem < 0:
            mem = 0
    except Exception:
        mem = 0
    try:
        diskSpace = int(float(diskSpace))
        if diskSpace < 0:
            diskSpace = 0
    except Exception:
        diskSpace = 0
    if background == 'True':
        background = True
    else:
        background = False
    if viaTopic == 'True':
        viaTopic = True
    else:
        viaTopic = False
    tmpLog.debug("%s,nJobs=%s,cpu=%s,mem=%s,disk=%s,source_label=%s,node=%s,ce=%s,rel=%s,user=%s,proxy=%s,c_group=%s,"
                 "w_group=%s,%s,taskID=%s,DN:%s,role:%s,token:%s,val:%s,FQAN:%s,json:%s,bg=%s,rt=%s,"
                 "harvester_id=%s,worker_id=%s,schedulerID=%s,jobType=%s,viaTopic=%s"
                 % (siteName, nJobs, cpu, mem, diskSpace, prodSourceLabel, node,
                    computingElement, AtlasRelease, prodUserID, getProxyKey, countryGroup, workingGroup,
                    allowOtherCountry, taskID, realDN, prodManager, token, validToken, str(fqans), req.acceptJson(),
                    background, resourceType, harvester_id, worker_id, schedulerID, jobType, viaTopic))
    try:
        dummyNumSlots = int(nJobs)
    except Exception:
        dummyNumSlots = 1
    if dummyNumSlots > 1:
        for iSlots in range(dummyNumSlots):
            _pilotReqLogger.info('method=getJob,site=%s,node=%s_%s,type=%s' % (siteName, node, iSlots, prodSourceLabel))
    else:
        _pilotReqLogger.info('method=getJob,site=%s,node=%s,type=%s' % (siteName,node,prodSourceLabel))
    # invalid role
    if (not prodManager) and (prodSourceLabel not in ['user']):
        tmpLog.warning("invalid role")
        if req.acceptJson():
            tmpMsg = 'no production/pilot role in VOMS FQANs or non pilot owner'
        else:
            tmpMsg = None
        return Protocol.Response(Protocol.SC_Role, tmpMsg).encode(req.acceptJson())
    # invalid token
    if not validToken:
        tmpLog.warning("invalid token")
        return Protocol.Response(Protocol.SC_Invalid).encode(req.acceptJson())
    # invoke JD
    return jobDispatcher.getJob(siteName, prodSourceLabel, cpu, mem, diskSpace, node, int(timeout),
                                computingElement, AtlasRelease, prodUserID, getProxyKey, countryGroup,
                                workingGroup, allowOtherCountry, realDN, taskID, nJobs, req.acceptJson(),
                                background, resourceType, harvester_id, worker_id, schedulerID, jobType,
                                viaTopic, tmpLog)


# update job status
def updateJob(req, jobId, state, token=None, transExitCode=None, pilotErrorCode=None, pilotErrorDiag=None,
              timestamp=None, timeout=60, xml='', node=None, workdir=None, cpuConsumptionTime=None,
              cpuConsumptionUnit=None, remainingSpace=None, schedulerID=None, pilotID=None, siteName=None,
              messageLevel=None, pilotLog='', metaData='', cpuConversionFactor=None, exeErrorCode=None,
              exeErrorDiag=None, pilotTiming=None, computingElement=None, startTime=None, endTime=None, nEvents=None,
              nInputFiles=None, batchID=None, attemptNr=None, jobMetrics=None, stdout='', jobSubStatus=None,
              coreCount=None, maxRSS=None, maxVMEM=None, maxSWAP=None, maxPSS=None, avgRSS=None, avgVMEM=None,
              avgSWAP=None, avgPSS=None, totRCHAR=None, totWCHAR=None, totRBYTES=None, totWBYTES=None, rateRCHAR=None,
              rateWCHAR=None, rateRBYTES=None, rateWBYTES=None, corruptedFiles=None, meanCoreCount=None):
    tmpLog = LogWrapper(_logger, 'updateJob PandaID={0} PID={1}'.format(jobId,os.getpid()))
    tmpLog.debug('start')
    # get DN
    realDN = _getDN(req)
    # get FQANs
    fqans = _getFQAN(req)
    # check production role
    prodManager = _checkRole(fqans,realDN,jobDispatcher,site=siteName,hostname=req.get_remote_host())
    # check token
    validToken = _checkToken(token,jobDispatcher)
    # accept json
    acceptJson = req.acceptJson()
    _logger.debug("updateJob(%s,%s,%s,%s,%s,%s,%s,"
                  "cpuConsumptionTime=%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"
                  "attemptNr:%s,jobSubStatus:%s,core:%s,DN:%s,role:%s,token:%s,val:%s,FQAN:%s,"
                  "maxRSS=%s,maxVMEM=%s,maxSWAP=%s,maxPSS=%s,avgRSS=%s,avgVMEM=%s,avgSWAP=%s,avgPSS=%s,"
                  "totRCHAR=%s,totWCHAR=%s,totRBYTES=%s,totWBYTES=%s,rateRCHAR=%s,rateWCHAR=%s,rateRBYTES=%s,"
                  "rateWBYTES=%s,meanCoreCount=%s,corruptedFiles=%s\n==XML==\n%s\n==LOG==\n%s\n==Meta==\n%s\n"
                  "==Metrics==\n%s\n==stdout==\n%s)" %
                  (jobId, state, transExitCode, pilotErrorCode, pilotErrorDiag, node, workdir, cpuConsumptionTime,
                   cpuConsumptionUnit, remainingSpace, schedulerID, pilotID, siteName, messageLevel, nEvents,
                   nInputFiles, cpuConversionFactor, exeErrorCode, exeErrorDiag, pilotTiming, computingElement,
                   startTime, endTime, batchID, attemptNr, jobSubStatus, coreCount, realDN, prodManager, token,
                   validToken, str(fqans), maxRSS, maxVMEM, maxSWAP, maxPSS, avgRSS, avgVMEM, avgSWAP, avgPSS,
                   totRCHAR, totWCHAR, totRBYTES, totWBYTES, rateRCHAR, rateWCHAR, rateRBYTES, rateWBYTES,
                   meanCoreCount, corruptedFiles, xml, pilotLog[:1024], metaData[:1024], jobMetrics, stdout))
    _pilotReqLogger.debug('method=updateJob,site=%s,node=%s,type=None' % (siteName,node))
    # invalid role
    if not prodManager:
        _logger.warning("updateJob(%s) : invalid role" % jobId)
        if acceptJson:
            tmpMsg = 'no production/pilot role in VOMS FQANs or non pilot owner'
        else:
            tmpMsg = None
        return Protocol.Response(Protocol.SC_Role, tmpMsg).encode(acceptJson)
    # invalid token
    if not validToken:
        _logger.warning("updateJob(%s) : invalid token" % jobId)
        return Protocol.Response(Protocol.SC_Invalid).encode(acceptJson)
    # aborting message
    if jobId=='NULL':
        return Protocol.Response(Protocol.SC_Success).encode(acceptJson)
    # check status
    if state not in ['running','failed','finished','holding','starting','transferring']:
        _logger.warning("invalid state=%s for updateJob" % state)
        return Protocol.Response(Protocol.SC_Success).encode(acceptJson)
    # create parameter map
    param = {}
    if cpuConsumptionTime not in [None, '']:
        param['cpuConsumptionTime']=cpuConsumptionTime
    if cpuConsumptionUnit is not None:
        param['cpuConsumptionUnit']=cpuConsumptionUnit
    if node is not None:
        param['modificationHost']=node[:128]
    if transExitCode is not None:
        try:
            int(transExitCode)
            param['transExitCode'] = transExitCode
        except Exception:
            pass
    if pilotErrorCode is not None:
        try:
            int(pilotErrorCode)
            param['pilotErrorCode'] = pilotErrorCode
        except Exception:
            pass
    if pilotErrorDiag is not None:
        param['pilotErrorDiag']=pilotErrorDiag[:500]
    if jobMetrics is not None:
        param['jobMetrics']=jobMetrics[:500]
    if schedulerID is not None:
        param['schedulerID']=schedulerID
    if pilotID is not None:
        param['pilotID']=pilotID[:200]
    if batchID is not None:
        param['batchID']=batchID[:80]
    if exeErrorCode is not None:
        param['exeErrorCode']=exeErrorCode
    if exeErrorDiag is not None:
        param['exeErrorDiag']=exeErrorDiag[:500]
    if cpuConversionFactor is not None:
        param['cpuConversion']=cpuConversionFactor
    if pilotTiming is not None:
        param['pilotTiming']=pilotTiming
    # disable pilot CE reporting. We will fill it from harvester table
    #if computingElement is not None:
    #    param['computingElement']=computingElement
    if nEvents is not None:
        param['nEvents']=nEvents
    if nInputFiles is not None:
        param['nInputFiles']=nInputFiles
    if jobSubStatus not in [None,'']:
        param['jobSubStatus']=jobSubStatus
    if coreCount not in [None,'']:
        param['actualCoreCount']=coreCount
    if meanCoreCount:
        try:
            param['meanCoreCount'] = float(meanCoreCount)
        except Exception:
            pass
    if maxRSS is not None:
        param['maxRSS'] = maxRSS
    if maxVMEM is not None:
        param['maxVMEM'] = maxVMEM
    if maxSWAP is not None:
        param['maxSWAP'] = maxSWAP
    if maxPSS is not None:
        param['maxPSS'] = maxPSS
    if avgRSS is not None:
        param['avgRSS'] = int(float(avgRSS))
    if avgVMEM is not None:
        param['avgVMEM'] = int(float(avgVMEM))
    if avgSWAP is not None:
        param['avgSWAP'] = int(float(avgSWAP))
    if avgPSS is not None:
        param['avgPSS'] = int(float(avgPSS))
    if totRCHAR is not None:
        totRCHAR = int(totRCHAR) / 1024 # convert to kByte
        totRCHAR = min(10 ** 10 - 1, totRCHAR)  # limit to 10 digit
        param['totRCHAR'] = totRCHAR
    if totWCHAR is not None:
        totWCHAR = int(totWCHAR) / 1024 # convert to kByte
        totWCHAR = min(10 ** 10 - 1, totWCHAR) # limit to 10 digit
        param['totWCHAR'] = totWCHAR
    if totRBYTES is not None:
        totRBYTES = int(totRBYTES) / 1024 # convert to kByte
        totRBYTES = min(10 ** 10 - 1, totRBYTES)  # limit to 10 digit
        param['totRBYTES'] = totRBYTES
    if totWBYTES is not None:
        totWBYTES = int(totWBYTES) / 1024 # convert to kByte
        totWBYTES = min(10 ** 10 - 1, totWBYTES)  # limit to 10 digit
        param['totWBYTES'] = totWBYTES
    if rateRCHAR is not None:
        rateRCHAR = min(10 ** 10 - 1, int(float(rateRCHAR)))  # limit to 10 digit
        param['rateRCHAR'] = rateRCHAR
    if rateWCHAR is not None:
        rateWCHAR = min(10 ** 10 - 1, int(float(rateWCHAR)))  # limit to 10 digit
        param['rateWCHAR'] = rateWCHAR
    if rateRBYTES is not None:
        rateRBYTES = min(10 ** 10 - 1, int(float(rateRBYTES)))  # limit to 10 digit
        param['rateRBYTES'] = rateRBYTES
    if rateWBYTES is not None:
        rateWBYTES = min(10 ** 10 - 1, int(float(rateWBYTES)))  # limit to 10 digit
        param['rateWBYTES'] = rateWBYTES
    if startTime is not None:
        try:
            param['startTime']=datetime.datetime(*time.strptime(startTime,'%Y-%m-%d %H:%M:%S')[:6])
        except Exception:
            pass
    if endTime is not None:
        try:
            param['endTime']=datetime.datetime(*time.strptime(endTime,'%Y-%m-%d %H:%M:%S')[:6])
        except Exception:
            pass
    if attemptNr is not None:
        try:
            attemptNr = int(attemptNr)
        except Exception:
            attemptNr = None
    if stdout != '':
        stdout = stdout[:2048]
    if corruptedFiles is not None:
        param['corruptedFiles'] = corruptedFiles
    # invoke JD
    tmpLog.debug('executing')
    return jobDispatcher.updateJob(int(jobId), state, int(timeout), xml, siteName, param, metaData, pilotLog,
                                   attemptNr, stdout, acceptJson)


# bulk update jobs
def updateJobsInBulk(req, jobList, harvester_id=None):
    retList = []
    retVal = False
    _logger.debug("updateJobsInBulk %s start" % harvester_id)
    tStart = datetime.datetime.utcnow()
    try:
        jobList = json.loads(jobList)
        for jobDict in jobList:
            jobId = jobDict['jobId']
            del jobDict['jobId']
            state = jobDict['state']
            del jobDict['state']
            if 'metaData' in jobDict:
                jobDict['metaData'] = str(jobDict['metaData'])
            tmpRet = updateJob(req, jobId, state, **jobDict)
            retList.append(tmpRet)
        retVal = True
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        tmpMsg = "updateJobsInBulk {0} failed with {1} {2}".format(harvester_id, errtype.__name__, errvalue)
        retList = tmpMsg
        _logger.error(tmpMsg + '\n' + traceback.format_exc())
    tDelta = datetime.datetime.utcnow() - tStart
    _logger.debug("updateJobsInBulk %s took %s.%03d sec" % (harvester_id, tDelta.seconds, tDelta.microseconds/1000))
    return json.dumps((retVal, retList))


# get job status
def getStatus(req,ids,timeout=60):
    _logger.debug("getStatus(%s)" % ids)
    return jobDispatcher.getStatus(ids,int(timeout))


# check job status
def checkJobStatus(req,ids,timeout=60):
    return jobDispatcher.checkJobStatus(ids,int(timeout))


# get a list of even ranges for a PandaID
def getEventRanges(req, pandaID, jobsetID, taskID=None, nRanges=10, timeout=60, scattered=None, segment_id=None):
    tmpStr = "getEventRanges(PandaID=%s jobsetID=%s taskID=%s,nRanges=%s,segment=%s)" % (
        pandaID, jobsetID, taskID, nRanges, segment_id)
    _logger.debug(tmpStr+' start')
    # get site
    tmpMap = jobDispatcher.getActiveJobAttributes(pandaID, ['computingSite'])
    if tmpMap is None:
        site = ''
    else:
        site = tmpMap['computingSite']
    tmpStat,tmpOut = checkPilotPermission(req, site)
    if not tmpStat:
        _logger.error(tmpStr+'failed with '+tmpOut)
        return tmpOut
    if scattered == 'True':
        scattered = True
    else:
        scattered = False
    if segment_id is not None:
        segment_id = int(segment_id)
    return jobDispatcher.getEventRanges(pandaID, jobsetID, taskID, nRanges, int(timeout), req.acceptJson(),
                                        scattered, segment_id)


# update an event range
def updateEventRange(req, eventRangeID, eventStatus, coreCount=None, cpuConsumptionTime=None,
                     objstoreID=None, timeout=60, pandaID=None):
    tmpStr = "updateEventRange(%s status=%s coreCount=%s cpuConsumptionTime=%s osID=%s)" % \
        (eventRangeID, eventStatus, coreCount, cpuConsumptionTime, objstoreID)
    _logger.debug(tmpStr+' start')
    # get site
    site = ''
    if pandaID is not None:
        tmpMap = jobDispatcher.getActiveJobAttributes(pandaID, ['computingSite'])
        if tmpMap is not None:
            site = tmpMap['computingSite']
    tmpStat,tmpOut = checkPilotPermission(req, site)
    if not tmpStat:
        _logger.error(tmpStr+'failed with '+tmpOut)
        return tmpOut
    return jobDispatcher.updateEventRange(eventRangeID, eventStatus, coreCount, cpuConsumptionTime,
                                          objstoreID, int(timeout))


# update an event ranges
def updateEventRanges(req, eventRanges, timeout=120, version=0, pandaID=None):
    tmpStr = "updateEventRanges(%s)" % eventRanges
    _logger.debug(tmpStr+' start')
    # get site
    site = ''
    if pandaID is not None:
        tmpMap = jobDispatcher.getActiveJobAttributes(pandaID, ['computingSite'])
        if tmpMap is not None:
            site = tmpMap['computingSite']
    tmpStat,tmpOut = checkPilotPermission(req, site)
    if not tmpStat:
        _logger.error(tmpStr+'failed with '+tmpOut)
        return tmpOut
    try:
        version = int(version)
    except Exception:
        version = 0
    return jobDispatcher.updateEventRanges(eventRanges,int(timeout),req.acceptJson(),version)


# check event availability
def checkEventsAvailability(req, pandaID, jobsetID, taskID, timeout=60):
    tmpStr = "checkEventsAvailability(pandaID={0} jobsetID={1} taskID={2})".format(pandaID, jobsetID, taskID)
    _logger.debug(tmpStr+' start')
    tmpStat,tmpOut = checkPilotPermission(req)
    if not tmpStat:
        _logger.error(tmpStr+'failed with '+tmpOut)
        #return tmpOut
    return jobDispatcher.checkEventsAvailability(pandaID, jobsetID, taskID, timeout)


# generate pilot token
def genPilotToken(req, schedulerid, host=None):
    # get DN
    realDN = _getDN(req)
    # get FQANs
    fqans = _getFQAN(req)
    # check production role
    prodManager = _checkRole(fqans,realDN,jobDispatcher,False)
    if not prodManager:
        return "ERROR : production or pilot role is required"
    if realDN is None:
        return "ERROR : failed to retrive DN"
    # hostname
    if host is None:
        host = req.get_remote_host()
    # return
    return jobDispatcher.genPilotToken(host,realDN,schedulerid)


# get key pair
def getKeyPair(req, publicKeyName, privateKeyName):
    # get DN
    realDN = _getDN(req)
    return jobDispatcher.getKeyPair(realDN,publicKeyName,privateKeyName,req.acceptJson())


# get proxy
def getProxy(req, role=None, dn=None):
    # get DN
    realDN = _getDN(req)
    if role == '':
        role = None
    return jobDispatcher.getProxy(realDN, role, dn)


# check pilot permission
def checkPilotPermission(req, site=''):
    # get DN
    realDN = _getDN(req)
    # get FQANs
    fqans = _getFQAN(req)
    # check production role
    prodManager = _checkRole(fqans,realDN,jobDispatcher,True,site)
    if not prodManager:
        return False,"production or pilot role is required"
    if realDN is None:
        return False,"failed to retrive DN"
    return True,None


# get DNs authorized for S3
def getDNsForS3(req):
    return jobDispatcher.getDNsForS3()


def getCommands(req, harvester_id, n_commands, timeout=30):
    """
    Get n commands for a particular harvester instance
    """
    tmp_str = "getCommands"

    # check permissions
    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        _logger.error('{0} failed with {1}'.format(tmp_str, tmp_out))

    accept_json = req.acceptJson()
    # retrieve the commands
    return jobDispatcher.getCommands(harvester_id, n_commands, timeout, accept_json)


def ackCommands(req, command_ids, timeout=30):
    """
    Ack the commands in the list of IDs
    """
    tmp_str = "ackCommands"

    # check permissions
    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        _logger.error('{0} failed with {1}'.format(tmp_str, tmp_out))

    command_ids = json.loads(command_ids)
    accept_json = req.acceptJson()
    # retrieve the commands
    return jobDispatcher.ackCommands(command_ids, timeout, accept_json)


def getResourceTypes(req, timeout=30):
    """
    Get resource types (MCORE, SCORE, etc.)
    """
    tmp_str = "getResourceTypes"

    # check permissions
    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        _logger.error('{0} failed with {1}'.format(tmp_str, tmp_out))

    accept_json = req.acceptJson()
    # retrieve the commands
    return jobDispatcher.getResourceTypes(timeout, accept_json)


# update the status of a worker according to the pilot
def updateWorkerPilotStatus(req, site, workerID, harvesterID, status, timeout=60):

    tmp_log = LogWrapper(_logger, 'updateWorkerPilotStatus workerID={0} harvesterID={1} status={2} PID={3}'
                         .format(workerID, harvesterID, status, os.getpid()))
    tmp_log.debug('start')

    # validate the pilot permissions
    tmp_stat, tmp_out = checkPilotPermission(req, site)
    if not tmp_stat:
        tmp_log.error('failed with {0}'.format(tmp_out))
        return tmp_out

    # validate the state passed by the pilot
    valid_states = ('started', 'finished')
    if status not in valid_states:
        message = 'Invalid worker state. The worker state has to be in {0}'.format(valid_states)
        tmp_log.debug(message)
        return message

    accept_json = req.acceptJson()

    return jobDispatcher.updateWorkerPilotStatus(workerID, harvesterID, status, timeout, accept_json)


