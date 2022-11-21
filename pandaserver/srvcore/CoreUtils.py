import re
import os
import datetime
import subprocess
from threading import Lock


# replacement for commands
def commands_get_status_output(com):
    data = ''
    try:
        # not to use check_output for python 2.6
        # data = subprocess.check_output(com, shell=True, universal_newlines=True, stderr=subprocess.STDOUT)
        p = subprocess.Popen(com, shell=True, universal_newlines=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        data, unused_err = p.communicate()
        retcode = p.poll()
        if retcode:
            ex = subprocess.CalledProcessError(retcode, com)
            raise ex
        status = 0
    except subprocess.CalledProcessError as ex:
        # commented out for python 2.6
        # data = ex.output
        status = ex.returncode
    if data[-1:] == '\n':
        data = data[:-1]
    return status, data


# extract name from DN
def clean_user_id(id):
    try:
        up = re.compile('/(DC|O|OU|C|L)=[^\/]+')
        username = up.sub('', id)
        up2 = re.compile('/CN=[0-9]+')
        username = up2.sub('', username)
        up3 = re.compile(' [0-9]+')
        username = up3.sub('', username)
        up4 = re.compile('_[0-9]+')
        username = up4.sub('', username)
        username = username.replace('/CN=proxy', '')
        username = username.replace('/CN=limited proxy', '')
        username = username.replace('limited proxy', '')
        username = re.sub('/CN=Robot:[^/]+', '', username)
        username = re.sub('/CN=nickname:[^/]+', '', username)
        pat = re.compile('.*/CN=([^\/]+)/CN=([^\/]+)')
        mat = pat.match(username)
        if mat:
            username = mat.group(2)
        else:
            username = username.replace('/CN=', '')
        if username.lower().find('/email') > 0:
            username = username[:username.lower().find('/email')]
        pat = re.compile('.*(limited.*proxy).*')
        mat = pat.match(username)
        if mat:
            username = mat.group(1)
        username = username.replace('(', '')
        username = username.replace(')', '')
        username = username.replace("'", '')
        name_wo_email = re.sub(r' [a-z][\w\.-]+@[\w\.-]+(?:\.\w+)+', '', username).strip()
        if ' ' in name_wo_email:
            username = name_wo_email
        return username
    except Exception:
        return id


# extract bare string from DN
def get_bare_dn(dn, keep_proxy=False, keep_digits=True):
    dn = re.sub('/CN=limited proxy', '', dn)
    if keep_proxy:
        dn = re.sub('/CN=proxy(/CN=proxy)+', '/CN=proxy', dn)
    else:
        dn = re.sub('(/CN=proxy)+', '', dn)
    if not keep_digits:
        dn = re.sub(r'(/CN=\d+)+$', '', dn)
    return dn


# extract id string from DN
def get_id_from_dn(dn, keep_proxy=False, keep_digits=True):
    m = re.search('/CN=nickname:([^/]+)', dn)
    if m:
        return m.group(1)
    return get_bare_dn(dn, keep_proxy, keep_digits)


# resolve string bool
def resolve_bool(param):
    if isinstance(param, bool):
        return param
    if param == 'True':
        return True
    if param == 'False':
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
        self.lastUpdated = datetime.datetime.utcnow()
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
        current = datetime.datetime.utcnow()
        # update if old
        if self.cachedObj is None or current-self.lastUpdated > self.timeInterval:
            self.log_stream.debug('PID={} renewing {} cache'.format(os.getpid(), self.name))
            try:
                tmp_stat, tmp_out = self.updateFunc()
                self.log_stream.debug('PID={} got for {} {} {}'.format(os.getpid(),
                                                                       self.name, tmp_stat, str(tmp_out)))
                if tmp_stat:
                    self.cachedObj = tmp_out
            except Exception as e:
                self.log_stream.error('PID={} failed to renew {} due to {}'.format(os.getpid(),
                                                                                   self.name, str(e)))
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
    def __getitem__(self,name):
        self.update()
        return self.cachedObj[name]
