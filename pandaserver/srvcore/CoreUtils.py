import copy
import datetime
import json
import math
import os
import re
import subprocess
from threading import Lock

from pandacommon.pandautils.PandaUtils import naive_utcnow


# replacement for commands
def commands_get_status_output(com):
    data = ""
    try:
        p = subprocess.Popen(
            com,
            shell=True,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        data, unused_err = p.communicate()
        retcode = p.poll()
        if retcode:
            ex = subprocess.CalledProcessError(retcode, com)
            raise ex
        status = 0
    except subprocess.CalledProcessError as ex:
        status = ex.returncode
    if data[-1:] == "\n":
        data = data[:-1]
    return status, data


# extract name from DN
def clean_user_id(id):
    try:
        up = re.compile("/(DC|O|OU|C|L)=[^\/]+")
        username = up.sub("", id)
        up2 = re.compile("/CN=[0-9]+")
        username = up2.sub("", username)
        up3 = re.compile(" [0-9]+")
        username = up3.sub("", username)
        up4 = re.compile("_[0-9]+")
        username = up4.sub("", username)
        username = username.replace("/CN=proxy", "")
        username = username.replace("/CN=limited proxy", "")
        username = username.replace("limited proxy", "")
        username = re.sub("/CN=Robot:[^/]+", "", username)
        username = re.sub("/CN=nickname:[^/]+", "", username)
        pat = re.compile(".*/CN=([^\/]+)/CN=([^\/]+)")
        mat = pat.match(username)
        if mat:
            username = mat.group(2)
        else:
            username = username.replace("/CN=", "")
        if username.lower().find("/email") > 0:
            username = username[: username.lower().find("/email")]
        pat = re.compile(".*(limited.*proxy).*")
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace("(", "")
        username = username.replace(")", "")
        username = username.replace("'", "")
        name_wo_email = re.sub(r" [a-z][\w\.-]+@[\w\.-]+(?:\.\w+)+", "", username).strip()
        if " " in name_wo_email:
            username = name_wo_email
        return username
    except Exception:
        return id


# extract bare string from DN
def get_bare_dn(dn, keep_proxy=False, keep_digits=True):
    dn = re.sub("/CN=limited proxy", "", dn)
    if keep_proxy:
        dn = re.sub("/CN=proxy(/CN=proxy)+", "/CN=proxy", dn)
    else:
        dn = re.sub("(/CN=proxy)+", "", dn)
    if not keep_digits:
        dn = re.sub(r"(/CN=\d+)+$", "", dn)
    return dn


# extract id string from DN
def get_id_from_dn(dn, keep_proxy=False, keep_digits=True):
    m = re.search("/CN=nickname:([^/]+)", dn)
    if m:
        return m.group(1)
    return get_bare_dn(dn, keep_proxy, keep_digits)


def get_distinguished_name_list(distinguished_name: str) -> list:
    """
    Get a list of possible distinguished names from a string, including legacy and RFC formats.

    Args:
        distinguished_name (str): The distinguished name string.

    Returns:
        list: A list of possible distinguished names.
    """
    name_list = []
    # characters to be escaped in RFC format
    trans_table = str.maketrans({",": r"\,", "+": r"\+", '"': r"\"", "\\": r"\\", "<": r"\<", ">": r"\>", ";": r"\;"})
    # loop over distinguished name and without-CN form
    for tmp_name in [distinguished_name, get_bare_dn(distinguished_name, keep_digits=False)]:
        name_list.append(tmp_name)
        # replace backslashes with commas and reverse substrings to be converted to RFC format
        tmp_list = [s.translate(trans_table) for s in tmp_name.split("/") if s]
        name_list.append(",".join(tmp_list[::-1]))
    return name_list


def normalize_cpu_model(cpu_model):
    cpu_model = cpu_model.upper()
    # Remove GHz, cache sizes, and redundant words
    cpu_model = re.sub(r"@\s*\d+(\.\d+)?\s*GHZ", "", cpu_model)
    cpu_model = re.sub(r"\d+\s*KB", "", cpu_model)
    cpu_model = re.sub(r"CORE PROCESSOR\s*\([^)]*\)", "", cpu_model)
    cpu_model = re.sub(r"\b\d+-CORE\b", "", cpu_model)
    cpu_model = re.sub(r"CPU|CORE|PROCESSOR|SCALABLE|\(R\)|\(TM\)", "", cpu_model)
    cpu_model = re.sub(r"\s+", " ", cpu_model)
    cpu_model = cpu_model.strip()
    return cpu_model


def clean_host_name(host_name):
    # If the worker node comes in the slot1@worker1.example.com format, we remove the slot1@ part
    match = re.search(r"@(.+)", host_name)
    host_name = match.group(1) if match else host_name

    # Special handling for ATLAS worker nodes to extract the third field of the hostname, since the first 2 fields are not unique
    # e.g. atlprd55-xyz-<third_field>.cern.ch
    match = re.match(r"^atlprd\d+-[^-]+-([^.]+\.cern\.ch)$", host_name)
    host_name = match.group(1) if match else host_name

    return host_name


# resolve string bool
def resolve_bool(param):
    if isinstance(param, bool):
        return param
    if param == "True":
        return True
    if param == "False":
        return False
    return param


# cached object
class CachedObject:
    # constructor
    def __init__(self, name, time_interval, update_func, log_stream):
        # name
        self.name = name
        # cached object
        self.cachedObj = None
        # datetime of last updated
        self.lastUpdated = naive_utcnow()
        # update frequency
        self.timeInterval = datetime.timedelta(seconds=time_interval)
        # lock
        self.lock = Lock()
        # function to update object
        self.updateFunc = update_func
        # log
        self.log_stream = log_stream

    # update obj
    def update(self):
        # lock
        self.lock.acquire()
        # get current datetime
        current = naive_utcnow()
        # update if old
        if self.cachedObj is None or current - self.lastUpdated > self.timeInterval:
            self.log_stream.debug(f"PID={os.getpid()} renewing {self.name} cache")
            try:
                tmp_stat, tmp_out = self.updateFunc()
                self.log_stream.debug(f"PID={os.getpid()} got for {self.name} {tmp_stat} {str(tmp_out)}")
                if tmp_stat:
                    self.cachedObj = tmp_out
            except Exception as e:
                self.log_stream.error(f"PID={os.getpid()} failed to renew {self.name} due to {str(e)}")
            self.lastUpdated = current
        # release
        self.lock.release()
        # return
        return

    # contains
    def __contains__(self, item):
        self.update()
        contains = False
        try:
            contains = item in self.cachedObj
        except TypeError:
            pass
        return contains

    # get item
    def __getitem__(self, name):
        self.update()
        return self.cachedObj[name]

    # get method
    def get(self, *var):
        return self.cachedObj.get(*var)

    # get object
    def get_object(self):
        self.lock.acquire()
        return self.cachedObj

    # release object
    def release_object(self):
        self.lock.release()


# dictionary of caches
class CacheDict:
    """
    Dictionary of caches with periodic cleanup
    """

    # constructor
    def __init__(self, update_interval=10, cleanup_interval=60):
        self.idx = 0
        self.lock = Lock()
        self.cache_dict = {}
        self.update_interval = datetime.timedelta(minutes=update_interval)
        self.cleanup_interval = datetime.timedelta(minutes=cleanup_interval)
        self.last_cleanup = naive_utcnow()

    def cleanup(self, tmp_log):
        """
        Cleanup caches
        :param tmp_log: logger
        """
        with self.lock:
            current = naive_utcnow()
            if current - self.last_cleanup > self.cleanup_interval:
                for name in list(self.cache_dict):
                    cache = self.cache_dict[name]
                    if current - cache["last_updated"] > self.cleanup_interval:
                        tmp_log.debug(f"""deleting cache #{self.cache_dict[name]["idx"]}""")
                        del self.cache_dict[name]
                self.last_cleanup = current

    def get(self, name, tmp_log, update_func, *update_args, **update_kwargs):
        """
        Get updated object
        :param name: name of cache
        :param tmp_log: logger
        :param update_func: function to update object
        :param update_args: arguments for update function
        :param update_kwargs: keyword arguments for update function
        :return: object or None
        """
        self.cleanup(tmp_log)
        with self.lock:
            obj = self.cache_dict.get(name)
            current = naive_utcnow()
            if not obj:
                tmp_log.debug(f"creating new cache #{self.idx}")
                # create new cache
                obj = {
                    "obj": update_func(*update_args, **update_kwargs),
                    "last_updated": current,
                    "update_func": update_func,
                    "update_args": update_args,
                    "update_kwargs": update_kwargs,
                    "idx": self.idx,
                }
                self.cache_dict[name] = obj
                self.idx += 1
            else:
                # update if old
                if current - obj["last_updated"] > self.update_interval:
                    tmp_log.debug(f"""updating cache #{obj["idx"]}""")
                    obj["obj"] = obj["update_func"](*obj["update_args"], **obj["update_kwargs"])
                    obj["last_updated"] = current
                else:
                    tmp_log.debug(f"""reusing cache #{obj["idx"]}""")
            return obj["obj"]


# convert datetime to string
class NonJsonObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return {"_datetime_object": obj.strftime("%Y-%m-%d %H:%M:%S.%f")}
        return json.JSONEncoder.default(self, obj)


# hook for json decoder
def as_python_object(dct):
    if "_datetime_object" in dct:
        return datetime.datetime.strptime(str(dct["_datetime_object"]), "%Y-%m-%d %H:%M:%S.%f")
    return dct


# get effective file size
def getEffectiveFileSize(fsize, startEvent, endEvent, nEvents):
    inMB = 1024 * 1024
    if fsize in [None, 0]:
        # use dummy size for pseudo input
        effectiveFsize = inMB
    elif nEvents is not None and startEvent is not None and endEvent is not None:
        # take event range into account
        effectiveFsize = int(float(fsize) * float(endEvent - startEvent + 1) / float(nEvents))
    else:
        effectiveFsize = fsize
    # use dummy size if input is too small
    if effectiveFsize == 0:
        effectiveFsize = inMB
    # in MB
    effectiveFsize = float(effectiveFsize) / inMB
    # return
    return effectiveFsize


# get effective number of events
def getEffectiveNumEvents(startEvent, endEvent, nEvents):
    if endEvent is not None and startEvent is not None:
        evtCounts = endEvent - startEvent + 1
        if evtCounts > 0:
            return evtCounts
        return 1
    if nEvents is not None:
        return nEvents
    return 1


# get memory usage
def getMemoryUsage():
    try:
        t = open(f"/proc/{os.getpid()}/status")
        v = t.read()
        t.close()
        value = 0
        for line in v.split("\n"):
            if line.startswith("VmRSS"):
                items = line.split()
                value = int(items[1])
                if items[2] in ["kB", "KB"]:
                    value /= 1024
                elif items[2] in ["mB", "MB"]:
                    pass
                break
        return int(value)
    except Exception:
        return None


# check process
def checkProcess(pid):
    return os.path.exists(f"/proc/{pid}/status")


# offset for walltime
wallTimeOffset = 10 * 60


# convert config parameters
def convert_config_params(itemStr):
    items = itemStr.split(":")
    newItems = []
    for item in items:
        if item == "":
            newItems.append(None)
        elif "," in item:
            newItems.append(item.split(","))
        else:
            try:
                newItems.append(int(item))
            except Exception:
                newItems.append(item)
    return newItems


# parse init params
def parse_init_params(par):
    if isinstance(par, list):
        return par
    try:
        return par.split("|")
    except Exception:
        return [par]


# get config param for vo and prodSourceLabel
def getConfigParam(configStr, vo, sourceLabel):
    try:
        for _ in configStr.split(","):
            items = configStr.split(":")
            vos = items[0].split("|")
            sourceLabels = items[1].split("|")
            if vo not in ["", "any"] and vo not in vos and None not in vos and "any" not in vos and "" not in vos:
                continue
            if (
                sourceLabel not in ["", "any"]
                and sourceLabel not in sourceLabels
                and None not in sourceLabels
                and "any" not in sourceLabels
                and "" not in sourceLabels
            ):
                continue
            return ",".join(items[2:])
    except Exception:
        pass
    return None


# get percentile until numpy 1.5.X becomes available
def percentile(inList, percent, idMap):
    inList = sorted(copy.copy(inList))
    k = (len(inList) - 1) * float(percent) / 100
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        retVal = inList[int(f)]
        return retVal, [retVal]
    val0 = inList[int(f)]
    val1 = inList[int(c)]
    d0 = val0 * (c - k)
    d1 = val1 * (k - f)
    retVal = d0 + d1
    return retVal, [val0, val1]


# get max walltime and cpu count
def getJobMaxWalltime(taskSpec, inputChunk, totalMasterEvents, jobSpec, siteSpec):
    try:
        if taskSpec.getCpuTime() is None:
            # use PQ maxtime when CPU time is not defined
            jobSpec.maxWalltime = siteSpec.maxtime
            jobSpec.maxCpuCount = siteSpec.maxtime
        else:
            jobSpec.maxWalltime = taskSpec.getCpuTime()
            if jobSpec.maxWalltime is not None and jobSpec.maxWalltime > 0:
                jobSpec.maxWalltime *= totalMasterEvents
                if siteSpec.coreCount > 0:
                    jobSpec.maxWalltime /= float(siteSpec.coreCount)
                if siteSpec.corepower not in [0, None]:
                    jobSpec.maxWalltime /= siteSpec.corepower
            if taskSpec.cpuEfficiency not in [None, 0]:
                jobSpec.maxWalltime /= float(taskSpec.cpuEfficiency) / 100.0
            if taskSpec.baseWalltime is not None:
                jobSpec.maxWalltime += taskSpec.baseWalltime
            jobSpec.maxWalltime = int(jobSpec.maxWalltime)
            if taskSpec.useHS06():
                jobSpec.maxCpuCount = jobSpec.maxWalltime
    except Exception:
        pass


# use direct IO for job
def use_direct_io_for_job(task_spec, site_spec, input_chunk):
    # not for merging
    if input_chunk and input_chunk.isMerging:
        return False
    # always
    if site_spec.always_use_direct_io():
        return True
    # force copy-to-scratch
    if task_spec.useLocalIO():
        return False
    # depends on task and site specs
    if task_spec.allowInputLAN() is not None and site_spec.isDirectIO():
        return True
    return False


# stopwatch
class StopWatch:
    """Utility class to measure timing information."""

    def __init__(self, identifier: str = None):
        self.start_time = datetime.datetime.now()
        self.checkpoint = self.start_time
        self.step_name = None
        self.identifier = identifier

    def reset(self):
        """Reset the stopwatch."""
        self.start_time = datetime.datetime.now()
        self.checkpoint = self.start_time
        self.step_name = None

    def get_elapsed_time(self, new_step_name: str) -> str:
        """Get the elapsed time since the stopwatch was started and the duration since the last checkpoint.
        :param new_step_name: The name of the next step.
        Returns:
            str: A string with the elapsed time and the duration since the last checkpoint.
        """
        now = datetime.datetime.now()
        total_delta = now - self.start_time
        duration_delta = now - self.checkpoint
        return_str = ""
        if self.identifier:
            return_str += f"{self.identifier}: "
        return_str += f"elapsed {total_delta.seconds}.{int(total_delta.microseconds / 1000):03d} sec. "
        if self.step_name is not None:
            return_str += f"{self.step_name} took {duration_delta.seconds}.{int(duration_delta.microseconds / 1000):03d} sec. "
        if new_step_name:
            return_str += f"{new_step_name} started."
        else:
            return_str += "done."
        self.checkpoint = now
        self.step_name = new_step_name
        return return_str
