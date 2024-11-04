"""
client methods
"""

import gzip
import json
import os
import pickle
import socket
import sys
import tempfile
from cgi import logfile

import requests
from pandacommon.pandautils.net_utils import replace_hostname_in_url_randomly

# PanDA server configuration
baseURL = os.environ.get("PANDA_URL", "http://pandaserver.cern.ch:25080/server/panda")
baseURLSSL = os.environ.get("PANDA_URL_SSL", "https://pandaserver.cern.ch:25443/server/panda")

# exit code
EC_Failed = 255


def is_https(url):
    # check if https is used
    return url.startswith("https://")


def pickle_dumps(obj):
    # wrapper for pickle with python 3
    return pickle.dumps(obj, protocol=0)


def pickle_loads(obj_string):
    try:
        return pickle.loads(obj_string.encode())
    except Exception:
        return pickle.loads(obj_string)


class HttpClient:
    def __init__(self):
        # verification of the host certificate
        if "PANDA_VERIFY_HOST" in os.environ and os.environ["PANDA_VERIFY_HOST"] == "off":
            self.verifyHost = False
        else:
            self.verifyHost = True

        # request a compressed response
        self.compress = True

        # SSL cert/key
        self.ssl_certificate = self._x509()
        self.ssl_key = self._x509()

        self.use_json = False

        # OIDC
        self.oidc = os.getenv("PANDA_AUTH") == "oidc"
        self.auth_vo = os.getenv("PANDA_AUTH_VO") if self.oidc else None
        self.id_token = os.getenv("PANDA_AUTH_ID_TOKEN") if self.oidc else None

    def _x509(self):
        # retrieve the X509_USER_PROXY from the environment variables
        try:
            return os.environ["X509_USER_PROXY"]
        except Exception:
            pass

        # look for the default place
        x509 = f"/tmp/x509up_u{os.getuid()}"
        if os.access(x509, os.R_OK):
            return x509

        # no valid proxy certificate
        print("No valid grid proxy certificate found")
        return ""

    def _prepare_url(self, url):
        """Modify URL with HTTPS check and hostname replacement."""
        use_https = is_https(url)
        modified_url = replace_hostname_in_url_randomly(url)
        return modified_url, use_https

    def _prepare_headers(self):
        """Prepare headers based on authentication and JSON settings."""
        headers = {}

        if self.oidc:
            headers["Authorization"] = f"Bearer {self.id_token}"
            headers["Origin"] = self.auth_vo

        if self.use_json:
            headers["Accept"] = "application/json"

        return headers

    def _prepare_ssl(self, use_https):
        """Prepare SSL configuration based on HTTPS usage and verification settings."""
        cert = None
        verify = True
        if use_https:
            cert = (self.ssl_certificate, self.ssl_key)

            if not self.verifyHost:
                verify = False
            elif "X509_CERT_DIR" in os.environ:
                verify = os.environ["X509_CERT_DIR"]
            elif os.path.exists("/etc/grid-security/certificates"):
                verify = "/etc/grid-security/certificates"

        return cert, verify

    def get(self, url, data):
        url, use_https = self._prepare_url(url)
        headers = self._prepare_headers()
        cert, verify = self._prepare_ssl(use_https)

        try:
            response = requests.get(url, headers=headers, params=data, timeout=600, cert=cert, verify=verify)
            response.raise_for_status()
            return 0, response.text
        except requests.RequestException as e:
            return 255, str(e)

    def post(self, url, data):
        url, use_https = self._prepare_url(url)
        headers = self._prepare_headers()
        cert, verify = self._prepare_ssl(use_https)

        try:
            response = requests.post(url, headers=headers, data=data, timeout=600, cert=cert, verify=verify)
            response.raise_for_status()
            return 0, response.text
        except requests.RequestException as e:
            return 255, str(e)

    def post_files(self, url, data):
        url, use_https = self._prepare_url(url)
        headers = self._prepare_headers()
        cert, verify = self._prepare_ssl(use_https)

        files = {}
        try:
            for key, value in data.items():
                if type(data[key]) == str:
                    # we got a file to upload without specifying the destination name
                    files[key] = open(data[key], "rb")
                else:
                    # we got a file to upload which specifies the destination name
                    files[key] = (data[key][0], open(data[key][1], "rb"))
            print(f"cert: {cert}, verify: {verify}")
            response = requests.post(url, headers=headers, files=files, timeout=600, cert=cert, verify=verify)
            response.raise_for_status()
            return 0, response.text
        except requests.RequestException as e:
            return 255, str(e)
        finally:
            for file in files.values():
                if type(file) == tuple:
                    file_handler = file[1]
                else:
                    file_handler = file
                file_handler.close()


"""
Client API
"""


def submitJobs(jobs, toPending=False):
    """
    Submit jobs

    args:
        jobs: the list of JobSpecs
        toPending: set True if jobs need to be pending state for the
                   two-staged submission mechanism
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              True: request is processed
              False: not processed
    """
    # set hostname
    hostname = socket.getfqdn()
    for job in jobs:
        job.creationHost = hostname
    # serialize
    str_jobs = pickle_dumps(jobs)

    http_client = HttpClient()

    url = f"{baseURLSSL}/submitJobs"
    data = {"jobs": str_jobs}
    if toPending:
        data["toPending"] = True
    status, output = http_client.post(url, data)
    if status != 0:
        print(output)
        return status, output
    try:
        return status, pickle_loads(output)
    except Exception:
        err_type, err_value, _ = sys.exc_info()
        err_str = f"ERROR submitJobs : {err_type} {err_value}"
        print(err_str)
        return EC_Failed, f"{output}\n{err_str}"


def getJobStatus(panda_ids):
    """
    Get job status

    args:
        ids: the list of PandaIDs
        use_json: using json instead of pickle
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        the list of JobSpecs (or Nones for non-existing PandaIDs)
    """
    # Serialize the panda IDs
    str_ids = json.dumps(panda_ids)

    http_client = HttpClient()
    http_client.use_json = True

    # Execute
    url = f"{baseURL}/getJobStatus"
    data = {"ids": str_ids}
    status, output = http_client.post(url, data)
    try:
        return status, json.loads(output)
    except Exception as e:
        err_str = f"ERROR getJobStatus: {str(e)}"
        print(err_str)
        return EC_Failed, f"{output}\n{err_str}"


def killJobs(
    ids,
    code=None,
    useMailAsID=False,
    keepUnmerged=False,
    jobSubStatus=None,
):
    """
    Kill jobs. Normal users can kill only their own jobs.
    People with production VOMS role can kill any jobs.
    Running jobs are killed when next heartbeat comes from the pilot.
    Set code=9 if running jobs need to be killed immediately.

       args:
           ids: the list of PandaIDs
           code: specify why the jobs are killed
                 2: expire
                 3: aborted
                 4: expire in waiting
                 7: retry by server
                 8: re-brokerage
                 9: force kill
                 10: fast re-brokerage on overloaded PQs
                 50: kill by JEDI
                 91: kill user jobs with prod role
           useMailAsID: obsolete
           keepUnmerged: set True not to cancel unmerged jobs when pmerge is killed.
           jobSubStatus: set job sub status if any
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of clouds (or Nones if tasks are not yet assigned)
    """
    # serialize
    str_panda_ids = pickle_dumps(ids)

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/killJobs"
    data = {"ids": str_panda_ids, "code": code, "useMailAsID": useMailAsID}
    kill_options = ""
    if keepUnmerged:
        kill_options += "keepUnmerged,"
    if jobSubStatus is not None:
        kill_options += f"jobSubStatus={jobSubStatus},"
    data["killOpts"] = kill_options[:-1]
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR killJobs : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"


def reassignJobs(ids, forPending=False, firstSubmission=None):
    """
    Triggers reassignment of jobs. This is not effective if jobs were preassigned to sites before being submitted.

    args:
        ids: the list of taskIDs
        forPending: set True if pending jobs are reassigned
        firstSubmission: set True if first jobs are submitted for a task, or False if not
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              True: request is processed
              False: not processed

    """
    # serialize
    str_task_ids = pickle_dumps(ids)

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/reassignJobs"
    data = {"ids": str_task_ids}
    if forPending:
        data["forPending"] = True
    if firstSubmission is not None:
        data["firstSubmission"] = firstSubmission
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR reassignJobs : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"stat={status} err={output} {error_str}"


def getJobStatistics(sourcetype=None):
    """
    Get job statistics

    args:
        sourcetype: type of jobs
            all: all jobs
            analysis: analysis jobs
            production: production jobs
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of the number jobs per job status in each site

    """

    http_client = HttpClient()
    # execute
    ret = {}

    url = f"{baseURL}/getJobStatistics"
    data = {}
    if sourcetype is not None:
        data["sourcetype"] = sourcetype
    status, output = http_client.get(url, data)
    try:
        tmp_return = status, pickle_loads(output)
        if status != 0:
            return tmp_return
    except Exception:
        print(output)
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR getJobStatistics : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"
    # gather
    for tmpCloud in tmp_return[1]:
        tmpVal = tmp_return[1][tmpCloud]
        if tmpCloud not in ret:
            # append cloud values
            ret[tmpCloud] = tmpVal
        else:
            # sum statistics
            for tmpStatus in tmpVal:
                tmpCount = tmpVal[tmpStatus]
                if tmpStatus in ret[tmpCloud]:
                    ret[tmpCloud][tmpStatus] += tmpCount
                else:
                    ret[tmpCloud][tmpStatus] = tmpCount

    return 0, ret


def getJobStatisticsForBamboo(useMorePG=False):
    """
    Get job statistics for Bamboo (used by TRIUMF panglia monitoring)

    args:
        useMorePG: set True if fine-grained classification is required
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of the number jobs per job status in each site

    """

    http_client = HttpClient()
    # execute
    ret = {}
    url = f"{baseURL}/getJobStatisticsForBamboo"
    data = {}
    if useMorePG is not False:
        data["useMorePG"] = useMorePG
    status, output = http_client.get(url, data)
    try:
        tmp_return = status, pickle_loads(output)
        if status != 0:
            return tmp_return
    except Exception:
        print(output)
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR getJobStatisticsForBamboo : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"
    # gather
    for tmpCloud in tmp_return[1]:
        tmpMap = tmp_return[1][tmpCloud]
        if tmpCloud not in ret:
            # append cloud values
            ret[tmpCloud] = tmpMap
        else:
            # sum statistics
            for tmpPType in tmpMap:
                tmpVal = tmpMap[tmpPType]
                if tmpPType not in ret[tmpCloud]:
                    ret[tmpCloud][tmpPType] = tmpVal
                else:
                    for tmpStatus in tmpVal:
                        tmpCount = tmpVal[tmpStatus]
                        if tmpStatus in ret[tmpCloud][tmpPType]:
                            ret[tmpCloud][tmpPType][tmpStatus] += tmpCount
                        else:
                            ret[tmpCloud][tmpPType][tmpStatus] = tmpCount
    return 0, ret


def getJobStatisticsPerSite(
    predefined=False,
    workingGroup="",
    countryGroup="",
    jobType="",
    minPriority=None,
    readArchived=None,
):
    """
    Get job statistics with job attributes

    args:
        predefined: get jobs which are assigned to sites before being submitted
        workingGroup: comma-separated list of workingGroups
        countryGroup: comma-separated list of countryGroups
        jobType: type of jobs
            all: all jobs
            analysis: analysis jobs
            production: production jobs
        minPriority: get jobs with higher priorities than this value
        readArchived: get jobs with finished/failed/cancelled state in addition
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of the number jobs per job status in each site

    """

    http_client = HttpClient()
    # execute
    ret = {}
    url = f"{baseURL}/getJobStatisticsPerSite"
    data = {"predefined": predefined}
    if workingGroup not in ["", None]:
        data["workingGroup"] = workingGroup
    if countryGroup not in ["", None]:
        data["countryGroup"] = countryGroup
    if jobType not in ["", None]:
        data["jobType"] = jobType
    if minPriority not in ["", None]:
        data["minPriority"] = minPriority
    if readArchived not in ["", None]:
        data["readArchived"] = readArchived
    status, output = http_client.get(url, data)
    try:
        tmp_return = status, pickle_loads(output)
        if status != 0:
            return tmp_return
    except Exception:
        print(output)
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR getJobStatisticsPerSite : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"

    # gather
    for tmp_site, tmp_value in tmp_return[1].items():
        if tmp_site not in ret:
            # append site values
            ret[tmp_site] = tmp_value
        else:
            # sum statistics
            for tmp_status, tmp_count in tmp_value.items():
                ret[tmp_site][tmp_status] = ret[tmp_site].get(tmp_status, 0) + tmp_count

    return 0, ret


def getJobStatisticsPerSiteResource(timeWindow=None):
    """
    Get job statistics per site and resource. This is used by panglia (TRIUMF monitoring)

    args:
       timeWindow: to count number of jobs that finish/failed/cancelled for last N minutes. 12*60 by default
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of the number jobs per job status in each site and resource

    """

    http_client = HttpClient()
    # execute
    url = f"{baseURL}/getJobStatisticsPerSiteResource"
    data = {}
    if timeWindow is not None:
        data["timeWindow"] = timeWindow
    status, output = http_client.get(url, data)
    try:
        return status, json.loads(output)
    except Exception:
        print(output)
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR getJobStatisticsPerSiteResource : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"


def get_job_statistics_per_site_label_resource(time_window=None):
    """
    Get job statistics per site, label, and resource

    args:
       timeWindow: to count number of jobs that finish/failed/cancelled for last N minutes. 12*60 by default
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of the number jobs per job status in each site and resource

    """

    http_client = HttpClient()
    # execute
    url = f"{baseURL}/get_job_statistics_per_site_label_resource"
    data = {}
    if time_window is not None:
        data["time_window"] = time_window
    status, output = http_client.get(url, data)
    try:
        return status, json.loads(output)
    except Exception as e:
        print(output)
        error_str = f"ERROR get_job_statistics_per_site_label_resource : {str(e)}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"


def insertSandboxFileInfo(userName, fileName, fileSize, checkSum):
    """
    Insert information of input sandbox

    args:
        userName: the name of the user
        fileName: the file name
        fileSize: the file size
        fileSize: md5sum of the file
    returns:
        status code
              0: communication succeeded to the panda server
              else: communication failure

    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/insertSandboxFileInfo"
    data = {
        "userName": userName,
        "fileName": fileName,
        "fileSize": fileSize,
        "checkSum": checkSum,
    }
    return http_client.post(url, data)


def putFile(file):
    """
    Upload input sandbox

    args:
        file: the file name
    returns:
        status code
              0: communication succeeded to the panda server
              else: communication failure

    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/putFile"
    data = {"file": file}
    return http_client.post_files(url, data)


# delete file (obsolete)
# TODO: is this really obsolete? I think it's used in panda cache
def deleteFile(file):
    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/deleteFile"
    data = {"file": file}
    return http_client.post(url, data)


# touch file (obsolete)
# TODO: is this really obsolete? I think it's used in panda cache
def touchFile(source_url, filename):
    http_client = HttpClient()

    # execute
    url = f"{source_url}/server/panda/touchFile"
    data = {"filename": filename}
    return http_client.post(url, data)


def getSiteSpecs(siteType=None):
    """
    Get list of site specifications

    args:
        siteType: type of sites
            None: all sites
            analysis: analysis sites
            production: production sites
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of site and attributes

    """

    http_client = HttpClient()
    # execute
    url = f"{baseURL}/getSiteSpecs"
    data = {}
    if siteType is not None:
        data = {"siteType": siteType}
    status, output = http_client.get(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR getSiteSpecs : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"


def insertTaskParams(taskParams):
    """
    Insert task parameters

    args:
        taskParams: a dictionary of task parameters
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and JediTaskID
              True: request is processed
              False: not processed
    """
    # serialize
    taskParamsStr = json.dumps(taskParams)

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/insertTaskParams"
    data = {"taskParams": taskParamsStr}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR insertTaskParams : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def killTask(jediTaskID, broadcast=False):
    """
    Kill a task

    args:
        jediTaskID: jediTaskID of the task to be killed
        broadcast: True to push the message to the pilot subscribing the MB
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/killTask"
    data = {"jediTaskID": jediTaskID, "properErrorCode": True, "broadcast": broadcast}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR killTask : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def finishTask(jediTaskID, soft=False, broadcast=False):
    """
    Finish a task

    args:
        jediTaskID: jediTaskID of the task to be finished
        soft: If True, new jobs are not generated and the task is
              finihsed once all remaining jobs are done.
              If False, all remaining jobs are killed and then the
              task is finished
        broadcast: True to push the message to the pilot subscribing the MB
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/finishTask"
    data = {"jediTaskID": jediTaskID, "properErrorCode": True, "broadcast": broadcast}
    if soft:
        data["soft"] = True
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR finishTask : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def reassignTaskToSite(jediTaskID, site, mode=None):
    """
    Reassign a task to a site. Existing jobs are killed and new jobs are generated at the site

    args:
        jediTaskID: jediTaskID of the task to be reassigned
        site: the site name where the task is reassigned
        mode: If soft, only defined/waiting/assigned/activated jobs are killed. If nokill, no jobs are killed. All jobs are killed by default.
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """
    maxSite = 60
    if site is not None and len(site) > maxSite:
        return EC_Failed, f"site parameter is too long > {maxSite}chars"

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/reassignTask"
    data = {"jediTaskID": jediTaskID, "site": site}
    if mode is not None:
        data["mode"] = mode
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR reassignTaskToSite : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def reassignTaskToCloud(jediTaskID, cloud, mode=None):
    """
    Reassign a task to a cloud. Existing jobs are killed and new jobs are generated in the cloud

    args:
        jediTaskID: jediTaskID of the task to be reassigned
        cloud: the cloud name where the task is reassigned
        mode: If soft, only defined/waiting/assigned/activated jobs are killed. If nokill, no jobs are killed. All jobs are killed by default.
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/reassignTask"
    data = {"jediTaskID": jediTaskID, "cloud": cloud}
    if mode is not None:
        data["mode"] = mode
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR reassignTaskToCloud : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def reassignTaskToNucleus(jediTaskID, nucleus, mode=None):
    """
    Reassign a task to a nucleus. Existing jobs are killed and new jobs are generated in the cloud

    args:
        jediTaskID: jediTaskID of the task to be reassigned
        nucleus: the nucleus name where the task is reassigned
        mode: If soft, only defined/waiting/assigned/activated jobs are killed. If nokill, no jobs are killed. All jobs are killed by default.
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/reassignTask"
    data = {"jediTaskID": jediTaskID, "nucleus": nucleus}
    if mode is not None:
        data["mode"] = mode
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR reassignTaskToCloud : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def uploadLog(logStr, logFileName):
    """
    Upload log

    args:
        logStr: log message
        logFileName: name of log file
    returns:
        status code
              0: communication succeeded to the panda server
              else: communication failure

    """

    http_client = HttpClient()

    # write log to a tmp file
    fh = tempfile.NamedTemporaryFile(delete=False)
    gfh = gzip.open(fh.name, mode="wb")
    if sys.version_info[0] >= 3:
        logStr = logStr.encode("utf-8")
    gfh.write(logStr)
    gfh.close()
    # execute
    url = f"{baseURLSSL}/uploadLog"
    # sometimes the destination file name (=logFileName) comes as an integer (e.g. a JEDI task ID) and it needs to be converted to a string
    logFileName = str(logFileName)
    data = {"file": (logFileName, fh.name)}
    return_value = http_client.post_files(url, data)
    os.unlink(fh.name)
    return return_value


def changeTaskPriority(jediTaskID, newPriority):
    """
    Change the task priority

    args:
        jediTaskID: jediTaskID of the task to change the priority
        newPriority: new task priority
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              0: unknown task
              1: succeeded
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/changeTaskPriority"
    data = {"jediTaskID": jediTaskID, "newPriority": newPriority}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR changeTaskPriority : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def setDebugMode(pandaID, modeOn):
    """
    Turn debug mode for a job on/off

    args:
        pandaID: PandaID of the job
        modeOn: True to turn it on. Oppositely, False
    returns:
        status code
              0: communication succeeded to the panda server
              another: communication failure
        error message
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/setDebugMode"
    data = {"pandaID": pandaID, "modeOn": modeOn}
    return http_client.post(url, data)


def retryTask(jediTaskID, noChildRetry=False, discardEvents=False, disable_staging_mode=False, keep_gshare_priority=False):
    """
    Retry a task

    args:
        jediTaskID: jediTaskID of the task to retry
        noChildRetry: True not to retry child tasks
        discardEvents: discard events
        disable_staging_mode: disable staging mode
        keep_gshare_priority: keep current gshare and priority
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/retryTask"
    data = {"jediTaskID": jediTaskID, "properErrorCode": True}
    if noChildRetry:
        data["noChildRetry"] = True
    if discardEvents:
        data["discardEvents"] = True
    if disable_staging_mode:
        data["disable_staging_mode"] = True
    if keep_gshare_priority:
        data["keep_gshare_priority"] = True
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR retryTask : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def reloadInput(jediTaskID):
    """
    Reload the input for a task

    args:
        jediTaskID: jediTaskID of the task to retry
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/reloadInput"
    data = {"jediTaskID": jediTaskID}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR reloadInput : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def changeTaskWalltime(jediTaskID, wallTime):
    """
    Change task walltime

    args:
        jediTaskID: jediTaskID of the task to change the priority
        wallTime: new walltime for the task
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              0: unknown task
              1: succeeded
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/changeTaskAttributePanda"
    data = {"jediTaskID": jediTaskID, "attrName": "wallTime", "attrValue": wallTime}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR changeTaskWalltime : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def changeTaskCputime(jediTaskID, cpuTime):
    """
    Change task CPU time

    args:
        jediTaskID: jediTaskID of the task to change the priority
        cpuTime: new cputime for the task
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              0: unknown task
              1: succeeded
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/changeTaskAttributePanda"
    data = {"jediTaskID": jediTaskID, "attrName": "cpuTime", "attrValue": cpuTime}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR changeTaskCputime : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def changeTaskRamCount(jediTaskID, ramCount):
    """
    Change task RAM count

    args:
        jediTaskID: jediTaskID of the task to change the priority
        ramCount: new ramCount for the task
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              0: unknown task
              1: succeeded
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/changeTaskAttributePanda"
    data = {"jediTaskID": jediTaskID, "attrName": "ramCount", "attrValue": ramCount}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR changeTaskRamCount : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def changeTaskAttribute(jediTaskID, attrName, attrValue):
    """
    Change task attribute

    args:
        jediTaskID: jediTaskID of the task to change the attribute
        attrName: attribute name
        attrValue: new value for the attribute
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message
              0: unknown task
              1: succeeded
              2: disallowed to update the attribute
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/changeTaskAttributePanda"
    data = {"jediTaskID": jediTaskID, "attrName": attrName, "attrValue": attrValue}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR changeTaskAttributePanda : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def changeTaskSplitRule(jediTaskID, ruleName, ruleValue):
    """
    Change split rule fo task

    args:
        jediTaskID: jediTaskID of the task to change the rule
        ruleName: rule name
        ruleValue: new value for the rule
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message
              0: unknown task
              1: succeeded
              2: disallowed to update the attribute
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/changeTaskSplitRulePanda"
    data = {"jediTaskID": jediTaskID, "attrName": ruleName, "attrValue": ruleValue}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR changeTaskSplitRule : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def pauseTask(jediTaskID):
    """
    Pause task

    args:
        jediTaskID: jediTaskID of the task to pause
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/pauseTask"
    data = {"jediTaskID": jediTaskID}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR pauseTask : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def resumeTask(jediTaskID):
    """
    Resume task

    args:
        jediTaskID: jediTaskID of the task to release
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/resumeTask"
    data = {"jediTaskID": jediTaskID}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR resumeTask : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def avalancheTask(jediTaskID):
    """
    Force avalanche for task

    args:
        jediTaskID: jediTaskID of the task to avalanche
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/avalancheTask"
    data = {"jediTaskID": jediTaskID}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR resumeTask : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def increaseAttemptNr(jediTaskID, increase):
    """
    Change task priority

    args:
        jediTaskID: jediTaskID of the task to increase attempt numbers
        increase: increase for attempt numbers
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              0: succeeded
              1: unknown task
              2: invalid task status
              3: permission denied
              4: wrong parameter
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/increaseAttemptNrPanda"
    data = {"jediTaskID": jediTaskID, "increasedNr": increase}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR increaseAttemptNr : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def killUnfinishedJobs(jediTaskID, code=None, useMailAsID=False):
    """
    Kill unfinished jobs in a task. Normal users can kill only their own jobs.
    People with production VOMS role can kill any jobs.
    Running jobs are killed when next heartbeat comes from the pilot.
    Set code=9 if running jobs need to be killed immediately.

       args:
           jediTaskID: the taskID of the task
           code: specify why the jobs are killed
                 2: expire
                 3: aborted
                 4: expire in waiting
                 7: retry by server
                 8: re-brokerage
                 9: force kill
                 50: kill by JEDI
                 91: kill user jobs with prod role
           useMailAsID: obsolete
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of clouds (or Nones if tasks are not yet assigned)
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/killUnfinishedJobs"
    data = {"jediTaskID": jediTaskID, "code": code, "useMailAsID": useMailAsID}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR killUnfinishedJobs : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"


def triggerTaskBrokerage(jediTaskID):
    """
    Trigger task brokerage

    args:
        jediTaskID: jediTaskID of the task to change the attribute
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message
              0: unknown task
              1: succeeded
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/changeTaskModTimePanda"
    data = {"jediTaskID": jediTaskID, "diffValue": -12}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR triggerTaskBrokerage : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def getPandaIDsWithTaskID(jediTaskID):
    """
    Get PanDA IDs with TaskID

    args:
        jediTaskID: jediTaskID of the task to get lit of PanDA IDs
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        the list of PanDA IDs
    """

    http_client = HttpClient()
    # execute
    url = f"{baseURL}/getPandaIDsWithTaskID"
    data = {"jediTaskID": jediTaskID}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR getPandaIDsWithTaskID : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"


def reactivateTask(jediTaskID, keep_attempt_nr=False, trigger_job_generation=False):
    """
    Reactivate task

    args:
        jediTaskID: jediTaskID of the task to be reactivated
        keep_attempt_nr: not to reset attempt numbers when being reactivated
        trigger_job_generation: trigger job generation once being reactivated
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message
              0: unknown task
              1: succeeded
              None: database error
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/reactivateTask"
    data = {"jediTaskID": jediTaskID}
    if keep_attempt_nr:
        data["keep_attempt_nr"] = True
    if trigger_job_generation:
        data["trigger_job_generation"] = True
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR reactivateTask : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def getTaskStatus(jediTaskID):
    """
    Get task status for a particular task ID

    args:
        jediTaskID: jediTaskID of the task to get lit of PanDA IDs
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        the status string
    """

    http_client = HttpClient()
    # execute
    url = f"{baseURL}/getTaskStatus"
    data = {"jediTaskID": jediTaskID}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR getTaskStatus : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"


def reassignShare(jedi_task_ids, share, reassign_running=False):
    """
    Reassign specified tasks (and their jobs) to a new share

    args:
        jedi_task_ids: task ids to act on
        share: share to be applied to jedi task ids
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message
              1: logical error
              0: success
              None: database error
    """

    http_client = HttpClient()

    jedi_task_ids_pickle = pickle_dumps(jedi_task_ids)
    change_running_pickle = pickle_dumps(reassign_running)
    # execute
    url = f"{baseURLSSL}/reassignShare"
    data = {
        "jedi_task_ids_pickle": jedi_task_ids_pickle,
        "share": share,
        "reassign_running": change_running_pickle,
    }
    status, output = http_client.post(url, data)

    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR reassignShare : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def getTaskParamsMap(jediTaskID):
    """
    Get task parameter map for a certain task ID

    args:
        jediTaskID: jediTaskID of the task to get taskParamsMap
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and taskParamsMap
              1: logical error
              0: success
              None: database error
    """

    http_client = HttpClient()
    # execute
    url = f"{baseURL}/getTaskParamsMap"
    data = {"jediTaskID": jediTaskID}
    status, output = http_client.post(url, data)
    try:
        return status, pickle_loads(output)
    except Exception:
        error_type, error_value, _ = sys.exc_info()
        error_str = f"ERROR getTaskParamsMap : {error_type} {error_value}"
        print(error_str)
        return EC_Failed, f"{output}\n{error_str}"


def setNumSlotsForWP(pandaQueueName, numSlots, gshare=None, resourceType=None, validPeriod=None):
    """
    Set num slots for workload provisioning

    args:
        pandaQueueName: Panda Queue name
        numSlots: the number of slots. 0 to dynamically set based on the number of starting jobs
        gshare: global share. None to set for any global share (default)
        resourceType: resource type. None to set for any resource type (default)
        validPeriod: How long the rule is valid in days. None if no expiration (default)
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: succeeded
              1: server error
            100: non SSL connection
            101: missing production role
            102: type error for some parameters
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/setNumSlotsForWP"
    data = {"pandaQueueName": pandaQueueName, "numSlots": numSlots}
    if gshare is not None:
        data["gshare"] = gshare
    if resourceType is not None:
        data["resourceType"] = resourceType
    if validPeriod is not None:
        data["validPeriod"] = validPeriod
    status, output = http_client.post(url, data)
    try:
        return status, json.loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR setNumSlotsForWP : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


# enable jumbo jobs
def enableJumboJobs(jediTaskID, totalJumboJobs=1, nJumboPerSite=1):
    """
    Enable jumbo jobs for a task

    args:
        jediTaskID: jediTaskID of the task
        totalJumboJobs: The total number of active jumbo jobs produced for the task. Use 0 to disable jumbo jobs for the task
        nJumboPerSite: The number of active jumbo jobs per site
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: succeeded
              1: server error
            100: non SSL connection
            101: missing production role
            102: type error for some parameters
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/enableJumboJobs"
    data = {
        "jediTaskID": jediTaskID,
        "nJumboJobs": totalJumboJobs,
        "nJumboPerSite": nJumboPerSite,
    }
    status, output = http_client.post(url, data)
    try:
        return status, json.loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR /enableJumboJobs : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def sweepPQ(panda_queue, status_list, ce_list, submission_host_list):
    """
    Send a harvester command to panda server in order sweep a panda queue

    args:
        panda_queue: panda queue name
        status_list: list with statuses to sweep, e.g. ['submitted']
        ce_list: list of CEs belonging to the site or 'ALL'
        submission_host_list: list of submission hosts this applies or 'ALL'
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message
              False: logical error
              True: success
    """

    http_client = HttpClient()

    panda_queue_json = json.dumps(panda_queue)
    status_list_json = json.dumps(status_list)
    ce_list_json = json.dumps(ce_list)
    submission_host_list_json = json.dumps(submission_host_list)

    # execute
    url = f"{baseURLSSL}/sweepPQ"
    data = {
        "panda_queue": panda_queue_json,
        "status_list": status_list_json,
        "ce_list": ce_list_json,
        "submission_host_list": submission_host_list_json,
    }
    status, output = http_client.post(url, data)

    try:
        return status, json.loads(output)
    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        error_str = f"ERROR sweepPQ : {error_type} {error_value}"
        return EC_Failed, f"{output}\n{error_str}"


def send_command_to_job(panda_id, com):
    """
    Send a command to a job

    args:
        panda_id: PandaID of the job
        com: a command string passed to the pilot. max 250 chars
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return: a tuple of return code and message
              False: failed
              True: the command received
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/send_command_to_job"
    data = {"panda_id": panda_id, "com": com}
    status, output = http_client.post(url, data)

    try:
        return status, json.loads(output)
    except Exception as e:
        error_str = f"ERROR send_command_to_job : {str(e)}"
        return EC_Failed, f"{output}\n{error_str}"


def get_ban_users():
    """
    Get list of banned users

    returns:
        status code
              True: communication succeeded to the panda server
              False: communication failure


    """

    http_client = HttpClient()

    # execute
    url = f"{baseURL}/get_ban_users"
    output = None
    try:
        status, output = http_client.post(url, {})
        if status == 0:
            return json.loads(output)
        else:
            return False, f"bad response: {output}"
    except Exception:
        return False, f"broken response: {output}"


def release_task(jedi_task_id):
    """
    Release task from staging

    args:
        jedi_task_id: jediTaskID of the task to avalanche
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        tuple of return code and diagnostic message
              0: request is registered
              1: server error
              2: task not found
              3: permission denied
              4: irrelevant task status
            100: non SSL connection
            101: irrelevant taskID
    """

    http_client = HttpClient()

    # execute
    url = f"{baseURLSSL}/release_task"
    data = {"jedi_task_id": jedi_task_id}
    status, output = http_client.post(url, data)
    try:
        return status, json.loads(output)
    except Exception as e:
        error_str = f"ERROR release_task : failed with {str(e)}"
        return EC_Failed, f"{output}\n{error_str}"
