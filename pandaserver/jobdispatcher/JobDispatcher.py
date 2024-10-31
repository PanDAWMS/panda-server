"""
dispatch jobs

"""

import datetime
import json
import os
import re
import socket
import sys
import threading
import time
import traceback
from threading import Lock

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice.adder_gen import AdderGen
from pandaserver.jobdispatcher import DispatcherUtils, Protocol
from pandaserver.proxycache import panda_proxy_cache, token_cache
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import EventServiceUtils

# logger
_logger = PandaLogger().getLogger("JobDispatcher")
_pilotReqLogger = PandaLogger().getLogger("PilotRequests")


# a wrapper to install timeout into a method
class _TimedMethod:
    def __init__(self, method, timeout):
        self.method = method
        self.timeout = timeout
        self.result = Protocol.TimeOutToken

    # method emulation
    def __call__(self, *var):
        self.result = self.method(*var)

    # run
    def run(self, *var):
        thr = threading.Thread(target=self, args=var)
        thr.start()
        thr.join()


# job dispatcher
class JobDispatcher:
    # constructor
    def __init__(self):
        # taskbuffer
        self.taskBuffer = None
        # datetime of last updated
        self.lastUpdated = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        # how frequently update DN/token map
        self.timeInterval = datetime.timedelta(seconds=180)
        # special dispatcher parameters
        self.specialDispatchParams = None
        # site mapper cache
        self.siteMapperCache = None
        # lock
        self.lock = Lock()
        # proxy cacher
        self.proxy_cacher = panda_proxy_cache.MyProxyInterface()
        # token cacher
        self.token_cacher = token_cache.TokenCache()
        # config of token cacher
        try:
            with open(panda_config.token_cache_config) as f:
                self.token_cache_config = json.load(f)
        except Exception:
            self.token_cache_config = {}

    # set task buffer
    def init(self, taskBuffer):
        # lock
        self.lock.acquire()
        # set TB
        if self.taskBuffer is None:
            self.taskBuffer = taskBuffer
        # special dispatcher parameters
        if self.specialDispatchParams is None:
            self.specialDispatchParams = CoreUtils.CachedObject("dispatcher_params", 60 * 10, self.get_special_dispatch_params, _logger)
        # site mapper cache
        if self.siteMapperCache is None:
            self.siteMapperCache = CoreUtils.CachedObject("site_mapper", 60 * 10, self.getSiteMapper, _logger)
        # release
        self.lock.release()

    # get special parameters for dispatcher
    def get_special_dispatch_params(self):
        """
        Wrapper function around taskBuffer.get_special_dispatch_params to convert list to set since task buffer cannot return set
        """
        param = self.taskBuffer.get_special_dispatch_params()
        for client_name in param["tokenKeys"]:
            param["tokenKeys"][client_name]["fullList"] = set(param["tokenKeys"][client_name]["fullList"])
        return True, param

    # set user proxy
    def set_user_proxy(self, response, distinguished_name=None, role=None, tokenized=False) -> tuple[bool, str]:
        """
        Set user proxy to the response

        :param response: response object
        :param distinguished_name: the distinguished name of the user
        :param role: the role of the user
        :param tokenized: whether the response should contain a token instead of a proxy

        :return: a tuple containing a boolean indicating success and a message
        """
        try:
            if distinguished_name is None:
                distinguished_name = response.data["prodUserID"]
            # remove redundant extensions
            distinguished_name = CoreUtils.get_bare_dn(distinguished_name, keep_digits=False)
            if not tokenized:
                # get proxy
                output = self.proxy_cacher.retrieve(distinguished_name, role=role)
            else:
                # get token
                output = self.token_cacher.get_access_token(distinguished_name)
            # not found
            if output is None:
                tmp_msg = f"""{"token" if tokenized else "proxy"} not found for {distinguished_name}"""
                response.appendNode("errorDialog", tmp_msg)
                return False, tmp_msg
            # set
            response.appendNode("userProxy", output)
            return True, ""
        except Exception as e:
            tmp_msg = f"""{"token" if tokenized else "proxy"} retrieval failed with {str(e)}"""
            response.appendNode("errorDialog", tmp_msg)
            return False, tmp_msg

    # get job
    def getJob(
        self,
        siteName,
        prodSourceLabel,
        mem,
        diskSpace,
        node,
        timeout,
        computingElement,
        prodUserID,
        getProxyKey,
        realDN,
        taskID,
        nJobs,
        acceptJson,
        background,
        resourceType,
        harvester_id,
        worker_id,
        schedulerID,
        jobType,
        via_topic,
        tmpLog,
    ):
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
        in_test = self.siteMapperCache.cachedObj.getSite(siteName).status == "test"

        # change label
        if in_test and prodSourceLabel in ["user", "managed", "unified"]:
            new_label = "test"
            tmpLog.debug(f"prodSourceLabel changed {prodSourceLabel} -> {new_label}")
            prodSourceLabel = new_label

        # wrapper function for timeout
        tmpWrapper = _TimedMethod(self.taskBuffer.getJobs, timeout)
        tmpWrapper.run(
            tmpNumJobs,
            siteName,
            prodSourceLabel,
            mem,
            diskSpace,
            node,
            timeout,
            computingElement,
            prodUserID,
            taskID,
            background,
            resourceType,
            harvester_id,
            worker_id,
            schedulerID,
            jobType,
            is_gu,
            via_topic,
        )

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
                    response = Protocol.Response(Protocol.SC_Success)
                    response.appendJob(tmpJob, self.siteMapperCache)
                except Exception as e:
                    tmpMsg = f"failed to get jobs with {str(e)}"
                    tmpLog.error(tmpMsg + "\n" + traceback.format_exc())
                    raise

                # append nSent
                response.appendNode("nSent", nSent)
                # set proxy key
                if getProxyKey:
                    response.setProxyKey(proxyKey)
                # set secrets
                if tmpJob.use_secrets() and tmpJob.prodUserName in secrets_map and secrets_map[tmpJob.prodUserName]:
                    response.appendNode("secrets", secrets_map[tmpJob.prodUserName])
                if panda_config.pilot_secrets in secrets_map and secrets_map[panda_config.pilot_secrets]:
                    response.appendNode("pilotSecrets", secrets_map[panda_config.pilot_secrets])
                # check if proxy cache is used
                if hasattr(panda_config, "useProxyCache") and panda_config.useProxyCache is True:
                    self.specialDispatchParams.update()
                    if "proxyCacheSites" not in self.specialDispatchParams:
                        proxyCacheSites = {}
                    else:
                        proxyCacheSites = self.specialDispatchParams["proxyCacheSites"]
                    if siteName in proxyCacheSites:
                        useProxyCache = True
                # set proxy
                if useProxyCache:
                    try:
                        #  get compact
                        compactDN = self.taskBuffer.cleanUserID(realDN)
                        # check permission
                        self.specialDispatchParams.update()
                        if "allowProxy" not in self.specialDispatchParams:
                            allowProxy = []
                        else:
                            allowProxy = self.specialDispatchParams["allowProxy"]
                        if compactDN not in allowProxy:
                            tmpLog.warning(f"{siteName} {node} '{compactDN}' no permission to retrieve user proxy")
                        else:
                            if useProxyCache:
                                tmpStat, tmpOut = self.set_user_proxy(
                                    response,
                                    proxyCacheSites[siteName]["dn"],
                                    proxyCacheSites[siteName]["role"],
                                )
                            else:
                                tmpStat, tmpOut = self.set_user_proxy(response)
                            if not tmpStat:
                                tmpLog.warning(f"{siteName} {node} failed to get user proxy : {tmpOut}")
                    except Exception as e:
                        tmpLog.warning(f"{siteName} {node} failed to get user proxy with {str(e)}")
                # panda proxy
                if (
                    "pandaProxySites" in self.specialDispatchParams
                    and siteName in self.specialDispatchParams["pandaProxySites"]
                    and (EventServiceUtils.isEventServiceJob(tmpJob) or EventServiceUtils.isEventServiceMerge(tmpJob))
                ):
                    # get secret key
                    tmpSecretKey, tmpErrMsg = DispatcherUtils.getSecretKey(tmpJob.PandaID)
                    if tmpSecretKey is None:
                        tmpLog.warning(f"PandaID={tmpJob.PandaID} site={siteName} failed to get panda proxy secret key : {tmpErrMsg}")
                    else:
                        # set secret key
                        tmpLog.debug(f"PandaID={tmpJob.PandaID} set key={tmpSecretKey}")
                        response.setPandaProxySecretKey(tmpSecretKey)
                # add
                responseList.append(response.data)
            # make response for bulk
            if nJobs is not None:
                try:
                    response = Protocol.Response(Protocol.SC_Success)
                    if not acceptJson:
                        response.appendNode("jobs", json.dumps(responseList))
                    else:
                        response.appendNode("jobs", responseList)
                except Exception as e:
                    tmpMsg = f"failed to make response with {str(e)}"
                    tmpLog.error(tmpMsg + "\n" + traceback.format_exc())
                    raise

        else:
            if tmpWrapper.result == Protocol.TimeOutToken:
                # timeout
                if acceptJson:
                    response = Protocol.Response(Protocol.SC_TimeOut, "database timeout")
                else:
                    response = Protocol.Response(Protocol.SC_TimeOut)
            else:
                # no available jobs
                if acceptJson:
                    response = Protocol.Response(Protocol.SC_NoJobs, "no jobs in PanDA")
                else:
                    response = Protocol.Response(Protocol.SC_NoJobs)
                _pilotReqLogger.info(f"method=noJob,site={siteName},node={node},type={prodSourceLabel}")
        # return
        tmpLog.debug(f"{siteName} {node} ret -> {response.encode(acceptJson)}")

        t_getJob_end = time.time()
        t_getJob_spent = t_getJob_end - t_getJob_start
        tmpLog.info(f"siteName={siteName} took timing={t_getJob_spent}s in_test={in_test}")
        return response.encode(acceptJson)

    # update job status
    def updateJob(
        self,
        jobID,
        jobStatus,
        timeout,
        xml,
        siteName,
        param,
        metadata,
        pilotLog,
        attemptNr=None,
        stdout="",
        acceptJson=False,
    ):
        tmp_logger = LogWrapper(_logger, f"updateJob {jobID}")

        # store the pilot log
        if pilotLog != "":
            tmp_logger.debug("saving pilot log")
            try:
                self.taskBuffer.storePilotLog(int(jobID), pilotLog)
                tmp_logger.debug("saving pilot log DONE")
            except Exception:
                tmp_logger.debug("saving pilot log FAILED")

        # add metadata
        if metadata != "":
            ret = self.taskBuffer.addMetadata([jobID], [metadata], [jobStatus])
            if len(ret) > 0 and not ret[0]:
                tmp_logger.debug(f"failed to add metadata")
                # return succeed
                response = Protocol.Response(Protocol.SC_Success)
                return response.encode(acceptJson)

        # add stdout
        if stdout != "":
            self.taskBuffer.addStdOut(jobID, stdout)

        # update the job
        tmpStatus = jobStatus
        updateStateChange = False
        if jobStatus == "failed" or jobStatus == "finished":
            tmpStatus = "holding"
            # update stateChangeTime to prevent Watcher from finding this job
            updateStateChange = True
            param["jobDispatcherErrorDiag"] = None
        elif jobStatus in ["holding", "transferring"]:
            param[
                "jobDispatcherErrorDiag"
            ] = f"set to {jobStatus} by the pilot at {datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')}"
        if tmpStatus == "holding":
            tmpWrapper = _TimedMethod(self.taskBuffer.updateJobStatus, None)
        else:
            tmpWrapper = _TimedMethod(self.taskBuffer.updateJobStatus, timeout)
        tmpWrapper.run(jobID, tmpStatus, param, updateStateChange, attemptNr)

        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result:
                # succeed
                response = Protocol.Response(Protocol.SC_Success)
                result = tmpWrapper.result
                secrets = None
                if isinstance(result, dict):
                    if "secrets" in result:
                        secrets = result["secrets"]
                    result = result["command"]
                # set command
                if isinstance(result, str):
                    response.appendNode("command", result)
                    if secrets:
                        response.appendNode("pilotSecrets", secrets)
                else:
                    response.appendNode("command", "NULL")

                # add output to dataset
                if result not in ["badattemptnr", "alreadydone"] and (jobStatus == "failed" or jobStatus == "finished"):
                    adder_gen = AdderGen(self.taskBuffer, jobID, jobStatus, attemptNr)
                    adder_gen.dump_file_report(xml, attemptNr)
                    del adder_gen
            else:
                response = Protocol.Response(Protocol.SC_Failed)

        tmp_logger.debug(f"ret -> {response.encode(acceptJson)}")
        return response.encode(acceptJson)

    # get job status
    def getStatus(self, strIDs, timeout):
        # convert str to list
        ids = strIDs.split()
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.peekJobs, timeout)
        tmpWrapper.run(ids, False, True, True, False)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            if isinstance(tmpWrapper.result, list):
                # succeed
                response = Protocol.Response(Protocol.SC_Success)
                # make return
                retStr = ""
                attStr = ""
                for job in tmpWrapper.result:
                    if job is None:
                        retStr += f"notfound+"
                        attStr += "0+"
                    else:
                        retStr += f"{job.jobStatus}+"
                        attStr += f"{job.attemptNr}+"
                response.appendNode("status", retStr[:-1])
                response.appendNode("attemptNr", attStr[:-1])
            else:
                # failed
                response = Protocol.Response(Protocol.SC_Failed)
        _logger.debug(f"getStatus : {strIDs} ret -> {response.encode()}")
        return response.encode()

    # check job status
    def checkJobStatus(self, pandaIDs, timeout):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.checkJobStatus, timeout)
        tmpWrapper.run(pandaIDs)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            if isinstance(tmpWrapper.result, list):
                # succeed
                response = Protocol.Response(Protocol.SC_Success)
                response.appendNode("data", tmpWrapper.result)
            else:
                # failed
                response = Protocol.Response(Protocol.SC_Failed)
        _logger.debug(f"checkJobStatus : {pandaIDs} ret -> {response.encode(True)}")
        return response.encode(True)

    # get a list of event ranges for a PandaID
    def getEventRanges(
        self,
        pandaID,
        jobsetID,
        jediTaskID,
        nRanges,
        timeout,
        acceptJson,
        scattered,
        segment_id,
    ):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.getEventRanges, timeout)
        tmpWrapper.run(pandaID, jobsetID, jediTaskID, nRanges, acceptJson, scattered, segment_id)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result is not None:
                # succeed
                response = Protocol.Response(Protocol.SC_Success)
                # make return
                response.appendNode("eventRanges", tmpWrapper.result)
            else:
                # failed
                response = Protocol.Response(Protocol.SC_Failed)
        _logger.debug(f"getEventRanges : {pandaID} ret -> {response.encode(acceptJson)}")
        return response.encode(acceptJson)

    # update an event range
    def updateEventRange(
        self,
        eventRangeID,
        eventStatus,
        coreCount,
        cpuConsumptionTime,
        objstoreID,
        timeout,
    ):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.updateEventRange, timeout)
        tmpWrapper.run(eventRangeID, eventStatus, coreCount, cpuConsumptionTime, objstoreID)
        # make response
        _logger.debug(str(tmpWrapper.result))
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result[0] is True:
                # succeed
                response = Protocol.Response(Protocol.SC_Success)
                response.appendNode("Command", tmpWrapper.result[1])
            else:
                # failed
                response = Protocol.Response(Protocol.SC_Failed)
        _logger.debug(f"updateEventRange : {eventRangeID} ret -> {response.encode()}")
        return response.encode()

    # update event ranges
    def updateEventRanges(self, eventRanges, timeout, acceptJson, version):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.updateEventRanges, timeout)
        tmpWrapper.run(eventRanges, version)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            # succeed
            response = Protocol.Response(Protocol.SC_Success)
            # make return
            response.appendNode("Returns", tmpWrapper.result[0])
            response.appendNode("Command", tmpWrapper.result[1])
        _logger.debug(f"updateEventRanges : ret -> {response.encode(acceptJson)}")
        return response.encode(acceptJson)

    # check event availability
    def checkEventsAvailability(self, pandaID, jobsetID, jediTaskID, timeout):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.checkEventsAvailability, timeout)
        tmpWrapper.run(pandaID, jobsetID, jediTaskID)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result is not None:
                # succeed
                response = Protocol.Response(Protocol.SC_Success)
                # make return
                response.appendNode("nEventRanges", tmpWrapper.result)
            else:
                # failed
                response = Protocol.Response(Protocol.SC_Failed)
        _logger.debug(f"checkEventsAvailability : {pandaID} ret -> {response.encode(True)}")
        return response.encode(True)

    # get key pair
    def getKeyPair(self, realDN, publicKeyName, privateKeyName, acceptJson):
        tmpMsg = f"getKeyPair {publicKeyName}/{privateKeyName} json={acceptJson}: "
        if realDN is None:
            # cannot extract DN
            tmpMsg += "failed since DN cannot be extracted"
            _logger.debug(tmpMsg)
            response = Protocol.Response(Protocol.SC_Perms, "Cannot extract DN from proxy. not HTTPS?")
        else:
            # get compact DN
            compactDN = self.taskBuffer.cleanUserID(realDN)
            # check permission
            self.specialDispatchParams.update()
            allowKey = self.specialDispatchParams.get("allowKeyPair", [])
            if compactDN not in allowKey:
                # permission denied
                tmpMsg += f"failed since '{compactDN}' not authorized with 'k' in {panda_config.schemaMETA}.USERS.GRIDPREF"
                _logger.debug(tmpMsg)
                response = Protocol.Response(Protocol.SC_Perms, tmpMsg)
            else:
                # look for key pair
                if "keyPair" not in self.specialDispatchParams:
                    keyPair = {}
                else:
                    keyPair = self.specialDispatchParams["keyPair"]
                notFound = False
                if publicKeyName not in keyPair:
                    # public key is missing
                    notFound = True
                    tmpMsg += f"failed for '{compactDN}' since {publicKeyName} is missing on {socket.getfqdn()}"
                elif privateKeyName not in keyPair:
                    # private key is missing
                    notFound = True
                    tmpMsg += f"failed for '{compactDN}' since {privateKeyName} is missing on {socket.getfqdn()}"
                if notFound:
                    # private or public key is missing
                    _logger.debug(tmpMsg)
                    response = Protocol.Response(Protocol.SC_MissKey, tmpMsg)
                else:
                    # key pair is available
                    response = Protocol.Response(Protocol.SC_Success)
                    response.appendNode("publicKey", keyPair[publicKeyName])
                    response.appendNode("privateKey", keyPair[privateKeyName])
                    tmpMsg += f"sent key-pair to '{compactDN}'"
                    _logger.debug(tmpMsg)
        # return
        return response.encode(acceptJson)

    # get a token key
    def get_token_key(self, distinguished_name, client_name, accept_json):
        tmp_log = LogWrapper(_logger, f"get_token_key client={client_name} PID={os.getpid()}")
        if distinguished_name is None:
            # cannot extract DN
            tmp_msg = "failed since DN cannot be extracted. non-HTTPS?"
            tmp_log.debug(tmp_msg)
            response = Protocol.Response(Protocol.SC_Perms, tmp_msg)
        else:
            # get compact DN
            compact_name = self.taskBuffer.cleanUserID(distinguished_name)
            # check permission
            self.specialDispatchParams.update()
            allowed_users = self.specialDispatchParams.get("allowTokenKey", [])
            if compact_name not in allowed_users:
                # permission denied
                tmp_msg = f"denied since '{compact_name}' not authorized with 't' in {panda_config.schemaMETA}.USERS.GRIDPREF"
                tmp_log.debug(tmp_msg)
                response = Protocol.Response(Protocol.SC_Perms, tmp_msg)
            else:
                # get a token key
                if client_name not in self.specialDispatchParams["tokenKeys"]:
                    # token key is missing
                    tmp_msg = f"token key is missing for '{client_name}"
                    tmp_log.debug(tmp_msg)
                    response = Protocol.Response(Protocol.SC_MissKey, tmp_msg)
                else:
                    # token key is available
                    response = Protocol.Response(Protocol.SC_Success)
                    response.appendNode("tokenKey", self.specialDispatchParams["tokenKeys"][client_name]["latest"])
                    tmp_msg = f"sent token key to '{compact_name}'"
                    tmp_log.debug(tmp_msg)
        # return
        return response.encode(accept_json)

    # get site mapper
    def getSiteMapper(self):
        return True, SiteMapper(self.taskBuffer)

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
            response.appendNode("Returns", tmp_wrapper.result[0])
            response.appendNode("Commands", tmp_wrapper.result[1])

        _logger.debug(f"getCommands : ret -> {response.encode(accept_json)}")
        return response.encode(accept_json)

    def ackCommands(self, command_ids, timeout, accept_json):
        """
        Acknowledge the commands from a list of IDs
        """
        _logger.debug(f"command_ids : {command_ids}")
        tmp_wrapper = _TimedMethod(self.taskBuffer.ackCommands, timeout)
        tmp_wrapper.run(command_ids)

        # Make response
        if tmp_wrapper.result == Protocol.TimeOutToken:
            # timeout
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            # success
            response = Protocol.Response(Protocol.SC_Success)
            response.appendNode("Returns", tmp_wrapper.result)

        _logger.debug(f"ackCommands : ret -> {response.encode(accept_json)}")
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
            response.appendNode("Returns", 0)
            response.appendNode("ResourceTypes", tmp_wrapper.result)

        _logger.debug(f"getResourceTypes : ret -> {response.encode(accept_json)}")
        return response.encode(accept_json)

    # get proxy
    def get_proxy(self, real_distinguished_name: str, role: str | None, target_distinguished_name: str | None, tokenized: bool, token_key: str | None) -> dict:
        """
        Get proxy for a user with a role

        :param real_distinguished_name: actual distinguished name of the user
        :param role: role of the user
        :param target_distinguished_name: target distinguished name if the user wants to get proxy for someone else.
                                          This is one of client_name defined in token_cache_config when getting a token
        :param tokenized: whether the response should contain a token instead of a proxy
        :param token_key: key to get the token from the token cache

        :return: response in dictionary
        """
        if target_distinguished_name is None:
            target_distinguished_name = real_distinguished_name
        tmp_log = LogWrapper(_logger, f"get_proxy PID={os.getpid()}")
        tmp_msg = f"""start DN="{real_distinguished_name}" role={role} target="{target_distinguished_name}" tokenized={tokenized} token_key={token_key}"""
        tmp_log.debug(tmp_msg)
        if real_distinguished_name is None:
            # cannot extract DN
            tmp_msg = "failed since DN cannot be extracted"
            tmp_log.debug(tmp_msg)
            response = Protocol.Response(Protocol.SC_Perms, "Cannot extract DN from proxy. not HTTPS?")
        else:
            # get compact DN
            compact_name = self.taskBuffer.cleanUserID(real_distinguished_name)
            # check permission
            self.specialDispatchParams.update()
            if "allowProxy" not in self.specialDispatchParams:
                allowed_names = []
            else:
                allowed_names = self.specialDispatchParams["allowProxy"]
            if compact_name not in allowed_names:
                # permission denied
                tmp_msg = f"failed since '{compact_name}' not in the authorized user list who have 'p' in {panda_config.schemaMETA}.USERS.GRIDPREF "
                if not tokenized:
                    tmp_msg += "to get proxy"
                else:
                    tmp_msg += "to get access token"
                tmp_log.debug(tmp_msg)
                response = Protocol.Response(Protocol.SC_Perms, tmp_msg)
            elif (
                tokenized
                and target_distinguished_name in self.token_cache_config
                and self.token_cache_config[target_distinguished_name].get("use_token_key") is True
                and (
                    target_distinguished_name not in self.specialDispatchParams["tokenKeys"]
                    or token_key not in self.specialDispatchParams["tokenKeys"][target_distinguished_name]["fullList"]
                )
            ):
                # invalid token key
                tmp_msg = f"failed since token key is invalid for {target_distinguished_name}"
                tmp_log.debug(tmp_msg)
                response = Protocol.Response(Protocol.SC_Invalid, tmp_msg)
            else:
                # get proxy
                response = Protocol.Response(Protocol.SC_Success, "")
                tmp_status, tmp_msg = self.set_user_proxy(response, target_distinguished_name, role, tokenized)
                if not tmp_status:
                    tmp_log.debug(tmp_msg)
                    response.appendNode("StatusCode", Protocol.SC_ProxyError)
                else:
                    tmp_msg = "successful sent proxy"
                    tmp_log.debug(tmp_msg)
        # return
        return response.encode(True)

    # get active job attribute
    def getActiveJobAttributes(self, pandaID, attrs):
        return self.taskBuffer.getActiveJobAttributes(pandaID, attrs)

    # update job status
    def updateWorkerPilotStatus(self, workerID, harvesterID, status, timeout, accept_json, node_id):
        tmp_wrapper = _TimedMethod(self.taskBuffer.updateWorkerPilotStatus, timeout)
        tmp_wrapper.run(workerID, harvesterID, status, node_id)

        # make response
        if tmp_wrapper.result == Protocol.TimeOutToken:
            response = Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmp_wrapper.result:  # success
                response = Protocol.Response(Protocol.SC_Success)
            else:  # error
                response = Protocol.Response(Protocol.SC_Failed)
        _logger.debug(f"updateWorkerPilotStatus : {workerID} {harvesterID} {status} ret -> {response.encode(accept_json)}")
        return response.encode(accept_json)

    # get max workerID
    def get_max_worker_id(self, harvester_id):
        id = self.taskBuffer.get_max_worker_id(harvester_id)
        return json.dumps(id)

    # get max workerID
    def get_events_status(self, ids):
        ret = self.taskBuffer.get_events_status(ids)
        return json.dumps(ret)


# Singleton
jobDispatcher = JobDispatcher()
del JobDispatcher


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmp_key in req.subprocess_env:
        tmp_value = req.subprocess_env[tmp_key]
        # Scan VOMS attributes
        # compact style
        if tmp_key.startswith("GRST_CRED_") and tmp_value.startswith("VOMS"):
            fqan = tmp_value.split()[-1]
            fqans.append(fqan)

        # old style
        elif tmp_key.startswith("GRST_CONN_"):
            tmp_items = tmp_value.split(":")
            if len(tmp_items) == 2 and tmp_items[0] == "fqan":
                fqans.append(tmp_items[-1])

    return fqans


# check role
def _checkRole(fqans, dn, withVomsPatch=True):
    production_manager = False
    try:
        # VOMS attributes of production and pilot roles
        production_attributes = [
            "/atlas/usatlas/Role=production",
            "/atlas/usatlas/Role=pilot",
            "/atlas/Role=production",
            "/atlas/Role=pilot",
            "/osg/Role=pilot",
            "^/[^/]+/Role=production$",
            "/ams/Role=pilot",
            "/Engage/LBNE/Role=pilot",
        ]
        if withVomsPatch:
            # FIXME once http://savannah.cern.ch/bugs/?47136 is solved
            production_attributes += ["/atlas/", "/osg/", "/cms/", "/ams/", "/Engage/LBNE/"]

        for fqan in fqans:
            # check atlas/usatlas production role
            if any(fqan.startswith(role_pattern) or re.search(role_pattern, fqan) for role_pattern in production_attributes):
                production_manager = True
                break

            # escape
            if production_manager:
                break

        # check DN with pilot owners
        if not production_manager and dn not in [None]:
            for owner in set(panda_config.production_dns).union(panda_config.pilot_owners):
                if owner and re.search(owner, dn) is not None:
                    production_manager = True
                    break

    except Exception:
        pass

    return production_manager


# get DN
def _getDN(req):
    realDN = None
    if "SSL_CLIENT_S_DN" in req.subprocess_env:
        realDN = req.subprocess_env["SSL_CLIENT_S_DN"]
        # remove redundant CN
        realDN = CoreUtils.get_bare_dn(realDN, keep_proxy=True)
    # return
    return realDN


"""
web service interface

"""


# get job
def getJob(
    req,
    siteName,
    token=None,
    timeout=60,
    cpu=None,
    mem=None,
    diskSpace=None,
    prodSourceLabel=None,
    node=None,
    computingElement=None,
    AtlasRelease=None,
    prodUserID=None,
    getProxyKey=None,
    countryGroup=None,
    workingGroup=None,
    allowOtherCountry=None,
    taskID=None,
    nJobs=None,
    background=None,
    resourceType=None,
    harvester_id=None,
    worker_id=None,
    schedulerID=None,
    jobType=None,
    viaTopic=None,
):
    tmpLog = LogWrapper(_logger, f"getJob {datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None).isoformat('/')}")
    tmpLog.debug(siteName)
    # get DN
    realDN = _getDN(req)
    # get FQANs
    fqans = _getFQAN(req)
    # check production role
    if getProxyKey == "True":
        # don't use /atlas to prevent normal proxy getting credname
        prodManager = _checkRole(fqans, realDN, False)
    else:
        prodManager = _checkRole(fqans, realDN)
    # set DN for non-production user
    if not prodManager:
        prodUserID = realDN
    # allow getProxyKey for production role
    if getProxyKey == "True" and prodManager:
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
    if background == "True":
        background = True
    else:
        background = False
    if viaTopic == "True":
        viaTopic = True
    else:
        viaTopic = False
    tmpLog.debug(
        f"{siteName},nJobs={nJobs},cpu={cpu},mem={mem},disk={diskSpace},source_label={prodSourceLabel},"
        f"node={node},ce={computingElement},rel={AtlasRelease},user={prodUserID},proxy={getProxyKey},"
        f"c_group={countryGroup},w_group={workingGroup},{allowOtherCountry},taskID={taskID},DN={realDN},"
        f"role={prodManager},FQAN={str(fqans)},json={req.acceptJson()},"
        f"bg={background},rt={resourceType},harvester_id={harvester_id},worker_id={worker_id},"
        f"schedulerID={schedulerID},jobType={jobType},viaTopic={viaTopic}"
    )
    try:
        dummyNumSlots = int(nJobs)
    except Exception:
        dummyNumSlots = 1
    if dummyNumSlots > 1:
        for iSlots in range(dummyNumSlots):
            _pilotReqLogger.info(f"method=getJob,site={siteName},node={node}_{iSlots},type={prodSourceLabel}")
    else:
        _pilotReqLogger.info(f"method=getJob,site={siteName},node={node},type={prodSourceLabel}")
    # invalid role
    if (not prodManager) and (prodSourceLabel not in ["user"]):
        tmpLog.warning("invalid role")
        if req.acceptJson():
            tmpMsg = "no production/pilot role in VOMS FQANs or non pilot owner"
        else:
            tmpMsg = None
        return Protocol.Response(Protocol.SC_Role, tmpMsg).encode(req.acceptJson())
    # invoke job dispatcher
    return jobDispatcher.getJob(
        siteName,
        prodSourceLabel,
        mem,
        diskSpace,
        node,
        int(timeout),
        computingElement,
        prodUserID,
        getProxyKey,
        realDN,
        taskID,
        nJobs,
        req.acceptJson(),
        background,
        resourceType,
        harvester_id,
        worker_id,
        schedulerID,
        jobType,
        viaTopic,
        tmpLog,
    )


# update job status
def updateJob(
    req,
    jobId,
    state,
    token=None,
    transExitCode=None,
    pilotErrorCode=None,
    pilotErrorDiag=None,
    timestamp=None,
    timeout=60,
    xml="",
    node=None,
    workdir=None,
    cpuConsumptionTime=None,
    cpuConsumptionUnit=None,
    remainingSpace=None,
    schedulerID=None,
    pilotID=None,
    siteName=None,
    messageLevel=None,
    pilotLog="",
    metaData="",
    cpuConversionFactor=None,
    exeErrorCode=None,
    exeErrorDiag=None,
    pilotTiming=None,
    computingElement=None,
    startTime=None,
    endTime=None,
    nEvents=None,
    nInputFiles=None,
    batchID=None,
    attemptNr=None,
    jobMetrics=None,
    stdout="",
    jobSubStatus=None,
    coreCount=None,
    maxRSS=None,
    maxVMEM=None,
    maxSWAP=None,
    maxPSS=None,
    avgRSS=None,
    avgVMEM=None,
    avgSWAP=None,
    avgPSS=None,
    totRCHAR=None,
    totWCHAR=None,
    totRBYTES=None,
    totWBYTES=None,
    rateRCHAR=None,
    rateWCHAR=None,
    rateRBYTES=None,
    rateWBYTES=None,
    corruptedFiles=None,
    meanCoreCount=None,
    cpu_architecture_level=None,
):
    tmp_log = LogWrapper(_logger, f"updateJob PandaID={jobId} PID={os.getpid()}")
    tmp_log.debug("start")

    # get DN, FQANs and roles
    realDN = _getDN(req)
    fqans = _getFQAN(req)
    prodManager = _checkRole(fqans, realDN)
    acceptJson = req.acceptJson()

    _logger.debug(
        f"updateJob({jobId},{state},{transExitCode},{pilotErrorCode},{pilotErrorDiag},{node},{workdir},"
        f"cpuConsumptionTime={cpuConsumptionTime},{cpuConsumptionUnit},{cpu_architecture_level},{remainingSpace},"
        f"{schedulerID},{pilotID},{siteName},{messageLevel},{nEvents},{nInputFiles},{cpuConversionFactor},"
        f"{exeErrorCode},{exeErrorDiag},{pilotTiming},{computingElement},{startTime},{endTime},{batchID},"
        f"attemptNr:{attemptNr},jobSubStatus:{jobSubStatus},core:{coreCount},DN:{realDN},role:{prodManager},"
        f"FQAN:{fqans},maxRSS={maxRSS},maxVMEM={maxVMEM},maxSWAP={maxSWAP},"
        f"maxPSS={maxPSS},avgRSS={avgRSS},avgVMEM={avgVMEM},avgSWAP={avgSWAP},avgPSS={avgPSS},"
        f"totRCHAR={totRCHAR},totWCHAR={totWCHAR},totRBYTES={totRBYTES},totWBYTES={totWBYTES},rateRCHAR={rateRCHAR},"
        f"rateWCHAR={rateWCHAR},rateRBYTES={rateRBYTES},rateWBYTES={rateWBYTES},meanCoreCount={meanCoreCount},"
        f"corruptedFiles={corruptedFiles}\n==XML==\n{xml}\n==LOG==\n{pilotLog[:1024]}\n==Meta==\n{metaData[:1024]}\n"
        f"==Metrics==\n{jobMetrics}\n==stdout==\n{stdout})"
    )

    _pilotReqLogger.debug(f"method=updateJob,site={siteName},node={node},type=None")

    # invalid role
    if not prodManager:
        tmp_log.warning(f"invalid role")
        tmpMsg = None
        if acceptJson:
            tmpMsg = "no production/pilot role in VOMS FQANs or non pilot owner"
        return Protocol.Response(Protocol.SC_Role, tmpMsg).encode(acceptJson)

    # aborting message
    if jobId == "NULL":
        return Protocol.Response(Protocol.SC_Success).encode(acceptJson)

    # check status
    if state not in [
        "running",
        "failed",
        "finished",
        "holding",
        "starting",
        "transferring",
    ]:
        tmp_log.warning(f"invalid state={state} for updateJob")
        return Protocol.Response(Protocol.SC_Success).encode(acceptJson)

    # create parameter map
    param = {}
    if cpuConsumptionTime not in [None, ""]:
        param["cpuConsumptionTime"] = cpuConsumptionTime
    if cpuConsumptionUnit is not None:
        param["cpuConsumptionUnit"] = cpuConsumptionUnit
    if cpu_architecture_level:
        try:
            param["cpu_architecture_level"] = cpu_architecture_level[:20]
        except Exception:
            tmp_log.error(f"invalid cpu_architecture_level={cpu_architecture_level} for updateJob")
            pass
    if node is not None:
        param["modificationHost"] = node[:128]
    if transExitCode is not None:
        try:
            int(transExitCode)
            param["transExitCode"] = transExitCode
        except Exception:
            pass
    if pilotErrorCode is not None:
        try:
            int(pilotErrorCode)
            param["pilotErrorCode"] = pilotErrorCode
        except Exception:
            pass
    if pilotErrorDiag is not None:
        param["pilotErrorDiag"] = pilotErrorDiag[:500]
    if jobMetrics is not None:
        param["jobMetrics"] = jobMetrics[:500]
    if schedulerID is not None:
        param["schedulerID"] = schedulerID
    if pilotID is not None:
        param["pilotID"] = pilotID[:200]
    if batchID is not None:
        param["batchID"] = batchID[:80]
    if exeErrorCode is not None:
        param["exeErrorCode"] = exeErrorCode
    if exeErrorDiag is not None:
        param["exeErrorDiag"] = exeErrorDiag[:500]
    if cpuConversionFactor is not None:
        param["cpuConversion"] = cpuConversionFactor
    if pilotTiming is not None:
        param["pilotTiming"] = pilotTiming
    if nEvents is not None:
        param["nEvents"] = nEvents
    if nInputFiles is not None:
        param["nInputFiles"] = nInputFiles
    if jobSubStatus not in [None, ""]:
        param["jobSubStatus"] = jobSubStatus
    if coreCount not in [None, ""]:
        param["actualCoreCount"] = coreCount
    if meanCoreCount:
        try:
            param["meanCoreCount"] = float(meanCoreCount)
        except Exception:
            pass
    if maxRSS is not None:
        param["maxRSS"] = maxRSS
    if maxVMEM is not None:
        param["maxVMEM"] = maxVMEM
    if maxSWAP is not None:
        param["maxSWAP"] = maxSWAP
    if maxPSS is not None:
        param["maxPSS"] = maxPSS
    if avgRSS is not None:
        param["avgRSS"] = int(float(avgRSS))
    if avgVMEM is not None:
        param["avgVMEM"] = int(float(avgVMEM))
    if avgSWAP is not None:
        param["avgSWAP"] = int(float(avgSWAP))
    if avgPSS is not None:
        param["avgPSS"] = int(float(avgPSS))
    if totRCHAR is not None:
        totRCHAR = int(totRCHAR) / 1024  # convert to kByte
        totRCHAR = min(10**10 - 1, totRCHAR)  # limit to 10 digit
        param["totRCHAR"] = totRCHAR
    if totWCHAR is not None:
        totWCHAR = int(totWCHAR) / 1024  # convert to kByte
        totWCHAR = min(10**10 - 1, totWCHAR)  # limit to 10 digit
        param["totWCHAR"] = totWCHAR
    if totRBYTES is not None:
        totRBYTES = int(totRBYTES) / 1024  # convert to kByte
        totRBYTES = min(10**10 - 1, totRBYTES)  # limit to 10 digit
        param["totRBYTES"] = totRBYTES
    if totWBYTES is not None:
        totWBYTES = int(totWBYTES) / 1024  # convert to kByte
        totWBYTES = min(10**10 - 1, totWBYTES)  # limit to 10 digit
        param["totWBYTES"] = totWBYTES
    if rateRCHAR is not None:
        rateRCHAR = min(10**10 - 1, int(float(rateRCHAR)))  # limit to 10 digit
        param["rateRCHAR"] = rateRCHAR
    if rateWCHAR is not None:
        rateWCHAR = min(10**10 - 1, int(float(rateWCHAR)))  # limit to 10 digit
        param["rateWCHAR"] = rateWCHAR
    if rateRBYTES is not None:
        rateRBYTES = min(10**10 - 1, int(float(rateRBYTES)))  # limit to 10 digit
        param["rateRBYTES"] = rateRBYTES
    if rateWBYTES is not None:
        rateWBYTES = min(10**10 - 1, int(float(rateWBYTES)))  # limit to 10 digit
        param["rateWBYTES"] = rateWBYTES
    if startTime is not None:
        try:
            param["startTime"] = datetime.datetime(*time.strptime(startTime, "%Y-%m-%d %H:%M:%S")[:6])
        except Exception:
            pass
    if endTime is not None:
        try:
            param["endTime"] = datetime.datetime(*time.strptime(endTime, "%Y-%m-%d %H:%M:%S")[:6])
        except Exception:
            pass
    if attemptNr is not None:
        try:
            attemptNr = int(attemptNr)
        except Exception:
            attemptNr = None
    if stdout != "":
        stdout = stdout[:2048]
    if corruptedFiles is not None:
        param["corruptedFiles"] = corruptedFiles
    # invoke JD
    tmp_log.debug("executing")
    return jobDispatcher.updateJob(
        int(jobId),
        state,
        int(timeout),
        xml,
        siteName,
        param,
        metaData,
        pilotLog,
        attemptNr,
        stdout,
        acceptJson,
    )


# bulk update jobs
def updateJobsInBulk(req, jobList, harvester_id=None):
    ret_list = []
    ret_val = False
    prefix = f"updateJobsInBulk {harvester_id}"
    tmp_logger = LogWrapper(_logger, prefix)
    tmp_logger.debug("start")
    t_start = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    try:
        job_list = json.loads(jobList)
        for job_dict in job_list:
            job_id = job_dict["jobId"]
            del job_dict["jobId"]
            state = job_dict["state"]
            del job_dict["state"]
            if "metaData" in job_dict:
                job_dict["metaData"] = str(job_dict["metaData"])
            tmp_ret = updateJob(req, job_id, state, **job_dict)
            ret_list.append(tmp_ret)
        ret_val = True
    except Exception:
        err_type, err_value = sys.exc_info()[:2]
        tmp_msg = f"failed with {err_type.__name__} {err_value}"
        ret_list = f"{prefix} {tmp_msg}"
        tmp_logger.error(f"{tmp_msg}\n{traceback.format_exc()}")

    t_delta = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - t_start
    tmp_logger.debug(f"took {t_delta.seconds}.{t_delta.microseconds // 1000:03d} sec")
    return json.dumps((ret_val, ret_list))


# get job status
def getStatus(req, ids, timeout=60):
    _logger.debug(f"getStatus({ids})")
    return jobDispatcher.getStatus(ids, int(timeout))


# check job status
def checkJobStatus(req, ids, timeout=60):
    return jobDispatcher.checkJobStatus(ids, int(timeout))


# get a list of even ranges for a PandaID
def getEventRanges(
    req,
    pandaID,
    jobsetID,
    taskID=None,
    nRanges=10,
    timeout=60,
    scattered=None,
    segment_id=None,
):
    """
    Check the permissions and eetrieve a list of event ranges for a given PandaID.

    Args:
        req: The request object containing the environment variables.
        pandaID (str): The ID of the Panda job.
        jobsetID (str): The ID of the job set.
        taskID (str, optional): The ID of the task. Defaults to None.
        nRanges (int, optional): The number of event ranges to retrieve. Defaults to 10.
        timeout (int, optional): The timeout value. Defaults to 60.
        scattered (str, optional): Whether the event ranges are scattered. Defaults to None.
        segment_id (int, optional): The segment ID. Defaults to None.
    Returns:
        dict: The response from the job dispatcher.
    """
    tmp_log = LogWrapper(_logger, f"getEventRanges(PandaID={pandaID} jobsetID={jobsetID} taskID={taskID},nRanges={nRanges},segment={segment_id})")
    tmp_log.debug("start")

    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        tmp_log.error(f"failed with {tmp_out}")
        return tmp_out

    if scattered == "True":
        scattered = True
    else:
        scattered = False

    if segment_id is not None:
        segment_id = int(segment_id)

    return jobDispatcher.getEventRanges(
        pandaID,
        jobsetID,
        taskID,
        nRanges,
        int(timeout),
        req.acceptJson(),
        scattered,
        segment_id,
    )


def updateEventRange(
    req,
    eventRangeID,
    eventStatus,
    coreCount=None,
    cpuConsumptionTime=None,
    objstoreID=None,
    timeout=60,
    pandaID=None,
):
    """
    Check the permissions and update the status of a specific event range.

    Args:
        req: The request object containing the environment variables.
        eventRangeID (str): The ID of the event range to update.
        eventStatus (str): The new status of the event range.
        coreCount (int, optional): The number of cores used. Defaults to None.
        cpuConsumptionTime (float, optional): The CPU consumption time. Defaults to None.
        objstoreID (int, optional): The object store ID. Defaults to None.
        timeout (int, optional): The timeout value. Defaults to 60.
        pandaID (str, optional): The PandaID. Defaults to None.

    Returns:
        dict: The response from the job dispatcher.
    """
    tmp_log = LogWrapper(
        _logger, f"updateEventRange({eventRangeID} status={eventStatus} coreCount={coreCount} cpuConsumptionTime={cpuConsumptionTime} osID={objstoreID})"
    )
    tmp_log.debug("start")

    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        tmp_log.error(f"failed with {tmp_out}")
        return tmp_out

    return jobDispatcher.updateEventRange(
        eventRangeID,
        eventStatus,
        coreCount,
        cpuConsumptionTime,
        objstoreID,
        int(timeout),
    )


def updateEventRanges(req, eventRanges, timeout=120, version=0, pandaID=None):
    """
    This function checks the permissions, converts the version to an integer, and updates the event ranges.

    Args:
        req: The request object containing the environment variables.
        eventRanges (str): A JSON string containing the list of event ranges to update.
        timeout (int, optional): The timeout value. Defaults to 120.
        version (int, optional): The version of the event ranges. Defaults to 0.
        pandaID (str, optional): The PandaID. Defaults to None.
    Returns:
        dict: The response from the job dispatcher.
    """
    tmp_log = LogWrapper(_logger, f"updateEventRanges({eventRanges})")
    tmp_log.debug("start")

    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        tmp_log.error(f"failed with {tmp_out}")
        return tmp_out

    try:
        version = int(version)
    except Exception:
        version = 0

    return jobDispatcher.updateEventRanges(eventRanges, int(timeout), req.acceptJson(), version)


def checkEventsAvailability(req, pandaID, jobsetID, taskID, timeout=60):
    """
    This function checks the availability of events for a given PandaID, jobsetID, and taskID.

    Args:
        req: The request object containing the environment variables.
        pandaID (str): The PandaID.
        jobset_id (str): The jobsetID.
        task_id (str): The taskID.
        timeout (int, optional): The timeout value. Defaults to 60.
    Returns:
        bool: The availability status of the events.
    """
    tmp_log = LogWrapper(_logger, f"check_events_availability(panda_id={pandaID} jobset_id={jobsetID} task_id={taskID})")
    tmp_log.debug("start")
    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        tmp_log.error(f"failed with {tmp_out}")

    return jobDispatcher.checkEventsAvailability(pandaID, jobsetID, taskID, timeout)


def getKeyPair(req, publicKeyName, privateKeyName):
    """
    This function retrieves the distinguished name (DN) from the request and uses it to get a key pair.

    Args:
        req: The request object containing the environment variables.
        publicKeyName (str): The name of the public key.
        privateKeyName (str): The name of the private key.

    Returns:
        dict: The key pair for the user.
    """
    real_dn = _getDN(req)
    return jobDispatcher.getKeyPair(real_dn, publicKeyName, privateKeyName, req.acceptJson())


def getProxy(req, role=None, dn=None):
    """
    Get proxy for a user with a role.

    Args:
        req: The request object containing the environment variables.
        role (str, optional): The role of the user. Defaults to None.
        dn (str, optional): The distinguished name of the user. Defaults to None.
    Returns:
        dict: The proxy for the user.
    """
    real_dn = _getDN(req)
    if role == "":
        role = None
    return jobDispatcher.get_proxy(real_dn, role, dn, False, None)


def get_access_token(req, client_name, token_key=None):
    """
    This function retrieves the distinguished name (DN) from the request and uses it to get an access token for the specified client.

    Args:
        req: The request object containing the environment variables.
        client_name (str): The name of the client requesting the access token.
        token_key (str, optional): The key to get the token from the token cache. Defaults to None.
    Returns:
        dict: The access token for the client.
    """
    # get DN
    real_dn = _getDN(req)
    return jobDispatcher.get_proxy(real_dn, None, client_name, True, token_key)


def get_token_key(req, client_name):
    """
    This function retrieves the distinguished name (DN) from the request and uses it to get a token key for the specified client.

    Args:
        req: The request object containing the environment variables.
        client_name (str): The name of the client requesting the token key.
    Returns:
        str: The token key for the client.
    """
    # get DN
    real_dn = _getDN(req)
    return jobDispatcher.get_token_key(real_dn, client_name, req.acceptJson())


def checkPilotPermission(req):
    """
    This function retrieves the distinguished name (DN) and Fully Qualified Attribute Names (FQANs) from the request,
    checks if the user has a production role, and verifies the DN.

    Args:
        req: The request object containing the environment variables.
    Returns:
        tuple: A tuple containing a boolean indicating success and a message.
    """
    # get DN
    real_dn = _getDN(req)
    if real_dn is None:
        return False, "failed to retrieve DN"

    # get FQANs and check production role
    fqans = _getFQAN(req)
    prod_manager = _checkRole(fqans, real_dn, True)
    if not prod_manager:
        return False, "production or pilot role is required"

    return True, None


def getCommands(req, harvester_id, n_commands, timeout=30):
    """
    This function checks the permissions and retrieves the commands for a specified harvester instance.

    Args:
        req: The request object containing the environment variables.
        harvester_id (str): The ID of the harvester instance.
        n_commands (int): The number of commands to retrieve.
        timeout (int, optional): The timeout value. Defaults to 30.
    Returns:
        dict: The response from the job dispatcher.
    """
    tmp_str = "getCommands"

    # check permissions
    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        _logger.error(f"{tmp_str} failed with {tmp_out}")

    accept_json = req.acceptJson()
    # retrieve the commands
    return jobDispatcher.getCommands(harvester_id, n_commands, timeout, accept_json)


def ackCommands(req, command_ids, timeout=30):
    """
    This function checks the permissions, parses the command IDs from JSON, and acknowledges the list of commands.

    Args:
        req: The request object containing the environment variables.
        command_ids (str): A JSON string containing the list of command IDs to acknowledge.
        timeout (int, optional): The timeout value. Defaults to 30.
    Returns:
        dict: The response from the job dispatcher.
    """
    tmp_str = "ackCommands"

    # check permissions
    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        _logger.error(f"{tmp_str} failed with {tmp_out}")

    command_ids = json.loads(command_ids)
    accept_json = req.acceptJson()

    # retrieve the commands
    return jobDispatcher.ackCommands(command_ids, timeout, accept_json)


def getResourceTypes(req, timeout=30):
    """
    This function retrieves the resource types (MCORE, SCORE, etc.) and their definitions.

    Args:
        req: The request object containing the environment variables.
        timeout (int, optional): The timeout value. Defaults to 30.
    Returns:
        dict: The resource types and their definitions.
    """
    tmp_str = "getResourceTypes"

    # check permissions
    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        _logger.error(f"{tmp_str} failed with {tmp_out}")

    accept_json = req.acceptJson()

    # retrieve the resource types
    return jobDispatcher.getResourceTypes(timeout, accept_json)


def updateWorkerPilotStatus(req, site, workerID, harvesterID, status, timeout=60, node_id=None):
    """
    Update the status of a worker according to the pilot.

    This function validates the pilot permissions and the state passed by the pilot, then updates the worker status.

    Args:
        req: The request object containing the environment variables.
        site (str): The site name.
        workerID (str): The worker ID.
        harvesterID (str): The harvester ID.
        status (str): The status of the worker. Must be either 'started' or 'finished'.
        timeout (int, optional): The timeout value. Defaults to 60.
        node_id (str, optional): The node ID. Defaults to None.

    Returns:
        str: The result of the status update or an error message.
    """
    tmp_log = LogWrapper(
        _logger,
        f"updateWorkerPilotStatus workerID={workerID} harvesterID={harvesterID} status={status} nodeID={node_id} PID={os.getpid()}",
    )
    tmp_log.debug("start")

    # validate the pilot permissions
    tmp_stat, tmp_out = checkPilotPermission(req)
    if not tmp_stat:
        tmp_log.error(f"failed with {tmp_out}")
        return tmp_out

    # validate the state passed by the pilot
    valid_states = ("started", "finished")
    if status not in valid_states:
        message = f"Invalid worker state. The worker state has to be in {valid_states}"
        tmp_log.debug(message)
        return message

    accept_json = req.acceptJson()

    return jobDispatcher.updateWorkerPilotStatus(workerID, harvesterID, status, timeout, accept_json, node_id)


# get max workerID
def get_max_worker_id(req, harvester_id):
    return jobDispatcher.get_max_worker_id(harvester_id)


# get events status
def get_events_status(req, ids):
    return jobDispatcher.get_events_status(ids)
