"""
client methods
"""

import gzip
import os
import pickle
import socket
import sys
import tempfile

import requests
from pandacommon.pandautils.net_utils import replace_hostname_in_url_randomly

from pandaserver.api.v1.http_client import HttpClient as HttpClientV1
from pandaserver.api.v1.http_client import api_url_ssl as api_url_ssl_v1
from pandaserver.taskbuffer.JobUtils import dump_jobs_json

DEFAULT_CERT_PATH = "/etc/grid-security/certificates"

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
        if self.id_token and self.id_token.startswith("file:"):
            with open(self.id_token[5:], "r") as f:
                self.id_token = f.read().strip()

    def _x509(self):
        # retrieve the X509_USER_PROXY from the environment variables and check if it is readable
        try:
            if "X509_USER_PROXY" in os.environ and os.access(os.environ["X509_USER_PROXY"], os.R_OK):
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
        if "PANDA_BEHIND_REAL_LB" in os.environ:
            modified_url = url
        else:
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
        cert = None  # no certificate by default when no HTTS or using oidc headers
        verify = True  # validate against default system CA certificates

        if use_https:
            # oidc tokens are added to the headers, we don't need to provide a certificate
            if not self.oidc:
                cert = (self.ssl_certificate, self.ssl_key)

            # the host verification has been disabled in the configuration
            if not self.verifyHost:
                verify = False
            # there is a path to the CA certificate folder and it exists
            elif "X509_CERT_DIR" in os.environ and os.path.exists(os.environ["X509_CERT_DIR"]):
                verify = os.environ["X509_CERT_DIR"]
            # the CA certificate folder is available in the standard location
            elif os.path.exists(DEFAULT_CERT_PATH):
                verify = DEFAULT_CERT_PATH

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


def submit_jobs(jobs):
    """
    Submit jobs

    args:
        jobs: the list of JobSpecs
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              True: request is processed
              False: not processed
    """
    # set hostname to jobs
    hostname = socket.getfqdn()
    for job in jobs:
        job.creationHost = hostname

    # serialize the jobs to json
    jobs = dump_jobs_json(jobs)

    http_client = HttpClientV1()

    url = f"{api_url_ssl_v1}/job/submit"
    data = {"jobs": jobs}

    status, output = http_client.post(url, data)
    return status, output


def get_job_status(job_ids):
    """
    Get job status

    args:
        job_ids: the list of PandaIDs
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        the list of JobSpecs (or Nones for non-existing PandaIDs)
    """
    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/pilot/get_job_status"
    data = {"job_ids": job_ids}
    status, output = http_client.post(url, data)

    return status, output


def kill_jobs(
    job_ids,
    code=None,
    keep_unmerged=False,
    job_sub_status=None,
):
    """
    Kill jobs. Normal users can kill only their own jobs.
    People with production VOMS role can kill any jobs.
    Running jobs are killed when next heartbeat comes from the pilot.
    Set code=9 if running jobs need to be killed immediately.

       args:
           job_ids: the list of PandaIDs
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
           keep_unmerged: set True not to cancel unmerged jobs when pmerge is killed.
           job_sub_status: set job sub status if any
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of clouds (or Nones if tasks are not yet assigned)
    """

    http_client = HttpClientV1()

    url = f"{api_url_ssl_v1}/job/kill"
    data = {"job_ids": job_ids}

    if code:
        data["code"] = code

    kill_options = []
    if keep_unmerged == True:
        kill_options.append("keepUnmerged")
    if job_sub_status:
        kill_options.append(f"jobSubStatus={job_sub_status}")
    if kill_options:
        data["kill_options"] = kill_options

    status, output = http_client.post(url, data)
    return status, output


def reassign_jobs(job_ids):
    """
    Triggers reassignment of jobs.

    args:
        ids: the list of taskIDs
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        return code
              True: request is processed
              False: not processed

    """
    http_client = HttpClientV1()

    url = f"{api_url_ssl_v1}/job/reassign"
    data = {"job_ids": job_ids}
    status, output = http_client.post(url, data)
    return status, output


def job_stats_by_cloud(job_type=None):
    """
    Get job statistics by cloud. Used by panglia monitor in TRIUMF

    args:
        job_type: string with the type of jobs to consider
            analysis: analysis jobs
            production: production jobs
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of the number jobs per job status in each site

    """
    if job_type not in [None, "analysis", "production"]:
        print("Invalid job type, must be one of [None, 'analysis', 'production'']")
        return EC_Failed, "Invalid job type"

    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/statistics/job_stats_by_cloud"

    data = {}
    if job_type:
        data["type"] = job_type

    status, output = http_client.get(url, data)
    if status != 0 or not output.get("success"):
        print(f"Failed to retrieve job_stats_by_cloud for {job_type}. Status: {status}, Output: {output}")
        return status, output

    statistics = output["data"]
    ret = {}
    for cloud, values in statistics.items():
        if cloud not in ret:
            # append cloud values (make a shallow copy to avoid mutating original)
            ret[cloud] = dict(values)
        else:
            # sum statistics per status
            for job_status, count in values.items():
                ret[cloud][job_status] = ret[cloud].get(job_status, 0) + count

    return 0, ret


# alias the old name to the new function for backwards compatibility
def getJobStatistics(sourcetype=None):
    return job_stats_by_cloud(sourcetype)


def production_job_stats_by_cloud_and_processing_type():
    """
    Get job statistics by cloud and processing type. Used by panglia monitor in TRIUMF

    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of the number jobs per job status in each site

    """

    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/statistics/production_job_stats_by_cloud_and_processing_type"
    status, output = http_client.get(url, {})

    if status != 0 or not output.get("success"):
        print(f"Failed to retrieve production_job_stats_by_cloud_and_processing_type. Status: {status}, Output: {output}")
        return status, output

    statistics = output["data"]
    aggregated = {}

    for cloud, processing_type_map in statistics.items():
        # ensure we work with a shallow copy of the incoming mappings
        processing_type_map = dict(processing_type_map)

        if cloud not in aggregated:
            # copy nested structures to avoid mutating the original
            aggregated[cloud] = {processing_type: dict(status_map) for processing_type, status_map in processing_type_map.items()}
            continue

        # merge into existing cloud entry
        for processing_type, status_map in processing_type_map.items():
            status_map = dict(status_map)
            if processing_type not in aggregated[cloud]:
                aggregated[cloud][processing_type] = status_map
                continue

            # sum counts per status
            for status, count in status_map.items():
                aggregated[cloud][processing_type][status] = aggregated[cloud][processing_type].get(status, 0) + count
    return 0, aggregated


# alias the old name to the new function for backwards compatibility
def getJobStatisticsForBamboo(useMorePG=False):
    return production_job_stats_by_cloud_and_processing_type()


def job_stats_by_site_and_resource_type(time_window=None):
    """
    Get job statistics per site and resource. Used by panglia monitor in TRIUMF

    args:
       time_window: to count number of jobs that finish/failed/cancelled for last N minutes. 12*60 by default
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of the number jobs per job status in each site and resource

    """

    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/statistics/job_stats_by_site_and_resource_type"
    data = {}
    if time_window:
        data["time_window"] = time_window

    status, output = http_client.get(url, data)

    if status != 0 or not output.get("success"):
        print(f"Failed to retrieve job_stats_by_site_and_resource_type. Status: {status}, Output: {output}")
        return status, output

    return 0, output.get("data")


# alias the old name to the new function for backwards compatibility
def getJobStatisticsPerSiteResource(timeWindow=None):
    return job_stats_by_site_and_resource_type(timeWindow)


def job_stats_by_site_share_and_resource_type(time_window=None):
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

    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/statistics/job_stats_by_site_share_and_resource_type"
    data = {}
    if time_window:
        data["time_window"] = time_window

    status, output = http_client.get(url, data)

    if status != 0 or not output.get("success"):
        print(f"Failed to retrieve job_stats_by_site_share_and_resource_type. Status: {status}, Output: {output}")
        return status, output

    return 0, output.get("data")


# alias the old name to the new function for backwards compatibility
def get_job_statistics_per_site_label_resource(time_window=None):
    return job_stats_by_site_share_and_resource_type(time_window)


def get_site_specs(site_type=None):
    """
    Get list of site specifications. Used by panglia monitor in TRIUMF

    args:
        site_type: type of sites
            all: all sites
            None: defaults to analysis sites
            analysis: analysis sites
            production: production sites
            unified: unified sites
    returns:
        status code
              0: communication succeeded to the panda server
              255: communication failure
        map of site and attributes

    """
    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/metaconfig/get_site_specs"
    if site_type not in [None, "all", "analysis", "production", "unified"]:
        return EC_Failed, "Invalid site type"

    data = {}
    if site_type:
        data = {"type": site_type}

    status, output = http_client.get(url, data)

    if status != 0 or not output.get("success"):
        print(f"Failed to retrieve get_site_specs. Status: {status}, Output: {output}")
        return status, output

    return 0, output.get("data")


# alias the old name to the new function for backwards compatibility
def getSiteSpecs(siteType=None):
    return get_site_specs(siteType)


def register_cache_file(user_name: str, file_name: str, file_size: int, checksum: str):
    """
    Register information about the input sandbox that is being stored in PanDA cache

    args:
        user_name: the name of the user
        file_name: the file name
        file_size: the file size
        checksum: md5sum of the file
    returns:
        status code
              0: communication succeeded to the panda server
              else: communication failure

    """
    http_client = HttpClientV1()

    url = f"{api_url_ssl_v1}/file_server/register_cache_file"

    data = {
        "user_name": user_name,
        "file_name": file_name,
        "file_size": file_size,
        "checksum": str(checksum),
    }
    return http_client.post(url, data)


def put_file(file):
    """
    Upload input sandbox to PanDA cache

    args:
        file: the file name
    returns:
        status code
              0: communication succeeded to the panda server
              else: communication failure

    """

    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/file_server/upload_cache_file"
    data = {"file": file}
    return http_client.post_files(url, data)


def touch_file(source_url, file_name):
    http_client = HttpClientV1()
    # Note the special construction of the URL here, since it is not going through the api_url_ssl_v1,
    # but directly to the source_url pointing at the concrete instance provided
    url = f"{source_url}/api/v1/file_server/touch_cache_file"
    data = {"file_name": file_name}
    return http_client.post(url, data)


def kill_task(task_id, broadcast=False):
    """
    Kill a task

    args:
        task_id: task ID of the task to be killed
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

    http_client = HttpClientV1()

    url = f"{api_url_ssl_v1}/task/kill"
    data = {"task_id": task_id, "broadcast": broadcast}

    status, output = http_client.post(url, data)

    return status, output


def finish_task(task_id, soft=False, broadcast=False):
    """
    Finish a task

    args:
        jediTaskID: jediTaskID of the task to be finished
        soft: If True, new jobs are not generated and the task is
              finished once all remaining jobs are done.
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
    http_client = HttpClientV1()

    url = f"{api_url_ssl_v1}/task/finish"
    data = {"task_id": task_id, "soft": soft, "broadcast": broadcast}

    status, output = http_client.post(url, data)

    return status, output


def uploadLog(log_string, log_file_name):
    """
    Upload log

    args:
        logStr: log message
        log_file_name: name of log file
    returns:
        status code
              0: communication succeeded to the panda server
              else: communication failure

    """

    http_client = HttpClientV1()

    # write log to a tmp file
    fh = tempfile.NamedTemporaryFile(delete=False)
    gfh = gzip.open(fh.name, mode="wb")
    if sys.version_info[0] >= 3:
        log_string = log_string.encode("utf-8")
    gfh.write(log_string)
    gfh.close()
    # execute
    url = f"{api_url_ssl_v1}/file_server/upload_jedi_log"

    # sometimes the destination file name (=logFileName) comes as an integer (e.g. a JEDI task ID) and it needs to be converted to a string
    log_file_name = str(log_file_name)
    data = {"file": (log_file_name, fh.name)}
    return_value = http_client.post_files(url, data, encoding="gzip")
    os.unlink(fh.name)
    return return_value


def set_debug_mode(job_id, mode):
    """
    Turn debug mode for a job on/off

    args:
        job_id: job_id of the job
        mode: True to turn it on. Oppositely, False
    returns:
        status code
              0: communication succeeded to the panda server
              another: communication failure
        error message
    """

    http_client = HttpClientV1()

    url = f"{api_url_ssl_v1}/job/set_debug_mode"
    data = {"job_id": job_id, "mode": mode}

    status, output = http_client.post(url, data)

    return status, output


def retry_task(task_id, no_child_retry=False, discard_events=False, disable_staging_mode=False, keep_gshare_priority=False):
    """
    Retry a task

    args:
        task_id: jediTaskID of the task to retry
        no_child_retry: True not to retry child tasks
        discard_events: discard events
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

    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/task/retry"

    data = {"task_id": task_id}
    if no_child_retry:
        data["no_child_retry"] = True
    if discard_events:
        data["discard_events"] = True
    if disable_staging_mode:
        data["disable_staging_mode"] = True
    if keep_gshare_priority:
        data["keep_gshare_priority"] = True

    status, output = http_client.post(url, data)
    return status, output


def reload_input(task_id):
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
    http_client = HttpClientV1()

    url = f"{api_url_ssl_v1}/task/reload_input"
    data = {"task_id": task_id}

    status, output = http_client.post(url, data)

    return status, output


def send_command_to_job(panda_id, command):
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

    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/job/set_command"
    data = {"job_id": panda_id, "command": command}

    status, output = http_client.post(url, data)

    return status, output


def get_banned_users():
    """
    Get list of banned users

    returns:
        status code
              True: communication succeeded to the panda server
              False: communication failure


    """

    http_client = HttpClientV1()
    url = f"{api_url_ssl_v1}/metaconfig/get_banned_users"

    status, output = http_client.get(url, {})
    if status != 0:
        return False, f"bad response: {output}"

    success = output["success"]
    message = output.get("message", "")
    data = output.get("data", None)

    if success:
        return True, data
    else:
        return False, f"error message: {message}"
