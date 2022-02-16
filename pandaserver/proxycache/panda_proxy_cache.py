import subprocess
import hashlib
import os
import datetime

import six

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper

# logger
_logger = PandaLogger().getLogger('ProxyCache')


def execute(program, log_stream):
    """Run a program on the command line. Return stderr, stdout and status."""
    log_stream.info("executable: %s" % program)
    pipe = subprocess.Popen(program, bufsize=-1, shell=True, close_fds=False,
                            cwd='/tmp',
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = pipe.communicate()
    return stdout, stderr, pipe.wait()

def cat(filename):
    """Given filename, print its text contents."""
    f = open(filename, 'r')
    out = f.read()
    f.close()
    return out

class MyProxyInterface(object):
    """Class to store and retrieve proxies from my proxies."""
    def __init__(self):
        self.__target_path = '/tmp/proxies'
        self.__cred_name = 'panda'
        if not os.path.exists(self.__target_path):
            os.makedirs(self.__target_path)

    def store(self, user_dn, cred_name, production=False, server_name='myproxy.cern.ch', role=None, force=False,
              log_stream=None):
        log_stream.info('store proxy')
        """Retrieve proxy from myproxy."""
        proxy_path = os.path.join(self.__target_path, hashlib.sha1(six.b(user_dn + '.plain')).hexdigest())
        # check if empty dummy file
        if os.path.exists(proxy_path) and os.stat(proxy_path).st_size == 0:
            if datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(os.path.getctime(proxy_path)) < datetime.timedelta(hours=1):
                log_stream.info('skip too early to try again according to timestamp of {}'.format(proxy_path))
                return 2
        cmd = "myproxy-logon -s %s --no_passphrase --out %s -l '%s' -k %s -t 0" % (server_name, proxy_path, user_dn, cred_name)
        # if myproxy.cern.ch fails, try myproxy on bnl as well
        stdout, stderr, status = execute(cmd, log_stream)
        if stdout:
            log_stream.info('stdout is %s ' % stdout)
        if stderr:
            log_stream.info('stderr is %s ' % stderr)
            # make a dummy to avoid too early reattempt
            open(proxy_path, 'w').close()
        log_stream.info('test the status of plain... %s' %status)
        if status == 1:
            return status
        #proxyValidity = checkValidity(proxy_path)
        if role is not None:
            log_stream.info('proxy needs {0} - need to add voms attributes and store it in the cache'.format(role))
            tmpExtension = self.getExtension(role)
            prodproxy_path = os.path.join(self.__target_path, str(hashlib.sha1(six.b(user_dn + tmpExtension)).hexdigest()))
            prodcmd = "voms-proxy-init -vomses /etc/vomses -valid 96:00 -rfc -cert %s -key %s -out %s -n -voms %s" % (
                proxy_path,proxy_path, prodproxy_path,role)
            stdout, stderr, status = execute(prodcmd, log_stream)
            if stdout:
                log_stream.info('stdout is %s ' % stdout)
            if stderr:
                log_stream.info('stderr is %s ' % stderr)
            log_stream.debug('test the status of production... %s' %status)
        elif production:
            log_stream.info('production proxy needed - need to add voms attributes and store it in the cache')
            prodproxy_path = os.path.join(self.__target_path, str(hashlib.sha1(six.b(user_dn + '.prod')).hexdigest()))
            log_stream.info(prodproxy_path)
            prodcmd = "voms-proxy-init -vomses /etc/vomses -valid 96:00 -rfc -cert %s -key %s -out %s -n -voms atlas:/atlas/Role=production" % (proxy_path, proxy_path, prodproxy_path)
            stdout, stderr, status = execute(prodcmd, log_stream)
            if stdout:
                log_stream.info('stdout is %s ' % stdout)
            if stderr:
                log_stream.info('stderr is %s ' % stderr)
            log_stream.info('test the status of production... %s' %status)
        else:
            # Now we need to add atlas roles and store it
            atlasproxy_path = os.path.join(self.__target_path, hashlib.sha1(six.b(user_dn)).hexdigest())
            atlasrolescmd = "voms-proxy-init -vomses /etc/vomses -valid 96:00 -rfc -cert %s -key %s -out %s -n -voms atlas" % (proxy_path, proxy_path, atlasproxy_path)
            stdout, stderr, status = execute(atlasrolescmd, log_stream)
            if stdout:
                log_stream.info('stdout is %s ' % stdout)
            if stderr:
                log_stream.info('stderr is %s ' % stderr)
            log_stream.info('test the status of atlas... %s' %status)
        # make dummy to avoid too early attempts
        if status != 0 and not os.path.exists(proxy_path):
            open(proxy_path, 'w').close()
        return status

    def retrieve(self, user_dn, production=False, role=None):
        """Retrieve proxy from proxy cache."""
        if role is not None:
            tmpExtension = self.getExtension(role)
            proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(six.b(user_dn + tmpExtension)).hexdigest()))
        elif production:
            proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(six.b(user_dn + '.prod')).hexdigest()))
        else:
            proxy_path = os.path.join(self.__target_path, hashlib.sha1(six.b(user_dn)).hexdigest())
        if os.path.isfile(proxy_path):
            return cat(proxy_path)
        else:
            _logger.warning('proxy file does not exist : DN:{0} role:{1} file:{2}'.format(user_dn,role,proxy_path))


    def checkProxy(self, user_dn, production=False, role=None, name=None):
        log_stream = LogWrapper(_logger, '< name="{}" role={} >'.format(name, role))
        log_stream.info('check proxy for {}'.format(user_dn))
        """Check the validity of a proxy."""
        if role is not None:
            tmpExtension = self.getExtension(role)
            proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(six.b(user_dn + tmpExtension)).hexdigest()))
        elif production:
            proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(six.b(user_dn + '.prod')).hexdigest()))
        else:
            proxy_path = os.path.join(self.__target_path, hashlib.sha1(six.b(user_dn)).hexdigest())
        isOK = False
        if os.path.isfile(proxy_path):
            log_stream.info('proxy is there. Need to check validity')
            cmd = "voms-proxy-info -exists -hours 94 -file %s" % proxy_path
            stdout, stderr, status = execute(cmd, log_stream)
            if stdout:
                log_stream.info('stdout is %s ' % stdout)
            if stderr:
                log_stream.info('stderr is %s ' %stderr)
            if status == 1:
                log_stream.info('proxy expires in 94h or less. We need to renew proxy!')
                ret = self.store(user_dn, self.__cred_name, production, role=role, log_stream=log_stream)
                if ret == 0:
                    log_stream.info('proxy retrieval successful')
                    isOK = True
                elif ret == 2:
                    log_stream.info('proxy retrieval on hold')
                else:
                    log_stream.error('proxy retrieval failed')
            else:
                log_stream.info('proxy is valid for more than 3 days')
                isOK = True
        else:
            log_stream.info('proxy is not in the cache repo. will try to get it from myproxy')
            ret = self.store(user_dn, self.__cred_name, production, role=role, log_stream=log_stream)
            if ret == 0:
                log_stream.info('proxy stored successfully')
                isOK = True
            elif ret == 2:
                log_stream.info('proxy retrieval on hold')
            else:
                log_stream.error('proxy retrieval failed')
        if isOK:
            plain_path = os.path.join(self.__target_path, hashlib.sha1(six.b(user_dn + '.plain')).hexdigest())
            if os.path.isfile(plain_path):
                return self.checkValidity(plain_path, log_stream)
            else:
                log_stream.error('plain proxy not there at the moment!')

    def checkValidity(self, proxy_path, log_stream):
        log_stream.info('Need to check validity and expiry!')
        datechecks = [24, 94, 168]
        #datechecks = [1,2,3,4]
        status = 0
        for i in datechecks:
            cmd = "voms-proxy-info -exists -hours %s -file %s" % (i, proxy_path)
            stdout, stderr, status = execute(cmd, log_stream)
            if status == 1:
                log_stream.warning('proxy expires in %s hours' %i)
                return i
        return status


    # get extension
    def getExtension(self, role):
        if role is not None:
            return '.' + role.split('=')[-1]
        return None
