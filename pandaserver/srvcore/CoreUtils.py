import datetime
import json
import os
import re
import subprocess
from threading import Lock


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
        self.lastUpdated = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
        current = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
        self.last_cleanup = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)

    def cleanup(self, tmp_log):
        """
        Cleanup caches
        :param tmp_log: logger
        """
        with self.lock:
            current = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
            current = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
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
