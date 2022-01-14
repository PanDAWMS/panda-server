import re
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
        return username
    except Exception:
        return id


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
            self.log_stream.debug('renewing {} cache'.format(self.name))
            try:
                tmp_stat, tmp_out = self.updateFunc()
                self.log_stream.debug('got for {} {} {}'.format(self.name, tmp_stat, str(tmp_out)))
                if tmp_stat:
                    self.cachedObj = tmp_out
            except Exception as e:
                self.log_stream.error('failed to renew {} due to {}'.format(self.name, str(e)))
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
