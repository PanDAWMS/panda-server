import subprocess
import hashlib
import os
import datetime

from pandacommon.pandalogger.PandaLogger import PandaLogger
# logger
_logger = PandaLogger().getLogger('ProxyCache')


def execute(program):
    """Run a program on the command line. Return stderr, stdout and status."""
    _logger.debug("executable: %s" % program)
    pipe = subprocess.Popen(program, bufsize=-1, shell=True, close_fds=False,
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

    def store(self, user_dn, cred_name, production=False, server_name='myproxy.cern.ch', role=None, force=False):
        """Retrieve proxy from myproxy."""
        proxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn + '.plain').hexdigest())
        # check if empty dummy file
        if os.path.exists(proxy_path) and os.stat(proxy_path).st_size == 0:
            if datetime.datetime.utcnow() - datetime.datetime.utcfromtimestamp(os.path.getctime(proxy_path)) < datetime.timedelta(hours=1):
                _logger.debug('skip too eary to try again')
                return 2
        cmd = "myproxy-logon -s %s --no_passphrase --out %s -l '%s' -k %s -t 0" % (server_name, proxy_path, user_dn, cred_name)
        # if myproxy.cern.ch fails, try myproxy on bnl as well
        stdout, stderr, status = execute(cmd)
        if stdout:
            _logger.debug('stdout is %s ' % stdout)
        if stderr:
            _logger.debug('stderr is %s ' % stderr)
            # make a dummy to avoid too early reattempt
            open(proxy_path, 'w').close()
        _logger.debug('test the status of plain... %s' %status)
        if status == 1:
            return status
        #proxyValidity = checkValidity(proxy_path)
        if role is not None:
            _logger.debug('proxy needs {0} - need to add voms attributes and store it in the cache'.format(role))
            tmpExtension = self.getExtension(role)
            prodproxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + tmpExtension).hexdigest()))
            _logger.debug(prodproxy_path)
            prodcmd = "voms-proxy-init -valid 96:00 -rfc -cert %s -key %s -out %s -voms %s" % (proxy_path,proxy_path,
                                                                                               prodproxy_path,role)
            stdout, stderr, status = execute(prodcmd)
            if stdout:
                _logger.debug('stdout is %s ' % stdout)
            if stderr:
                _logger.debug('stderr is %s ' % stderr)
            _logger.debug('test the status of production... %s' %status)
        elif production:
            _logger.debug('production proxy needed - need to add voms attributes and store it in the cache')
            prodproxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + '.prod').hexdigest()))
            _logger.debug(prodproxy_path)
            prodcmd = "voms-proxy-init -valid 96:00 -rfc -cert %s -key %s -out %s -voms atlas:/atlas/Role=production" % (proxy_path, proxy_path, prodproxy_path)
            stdout, stderr, status = execute(prodcmd)
            if stdout:
                _logger.debug('stdout is %s ' % stdout)
            if stderr:
                _logger.debug('stderr is %s ' % stderr)
            _logger.debug('test the status of production... %s' %status)
        else:
            # Now we need to add atlas roles and store it
            atlasproxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn).hexdigest())
            atlasrolescmd = "voms-proxy-init -valid 96:00 -rfc -cert %s -key %s -out %s -voms atlas" % (proxy_path, proxy_path, atlasproxy_path)
            stdout, stderr, status = execute(atlasrolescmd)
            if stdout:
                _logger.debug('stdout is %s ' % stdout)
            if stderr:
                _logger.debug('stderr is %s ' % stderr)
            _logger.debug('test the status of atlas... %s' %status)
        # will remove the proxy later on as I need to check the actual validity in order to send notification emails
        #if os.path.exists(proxy_path):
        #    print 'will now remove the plain proxy from the cache'
        #    os.remove(proxy_path)

        return status

    def retrieve(self, user_dn, production=False, role=None):
        """Retrieve proxy from proxy cache."""
        if role is not None:
            tmpExtension = self.getExtension(role)
            proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + tmpExtension).hexdigest()))
        elif production:
            proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + '.prod').hexdigest()))
        else:
            proxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn).hexdigest())
        if os.path.isfile(proxy_path):
            return cat(proxy_path)
        else:
            _logger.debug('proxy file does not exist : DN:{0} role:{1} file:{2}'.format(user_dn,role,proxy_path))


    def checkProxy(self, user_dn, production=False, role=None):
        """Check the validity of a proxy."""
        if role is not None:
            tmpExtension = self.getExtension(role)
            proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + tmpExtension).hexdigest()))
        elif production:
            proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + '.prod').hexdigest()))
        else:
            proxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn).hexdigest())
        if os.path.isfile(proxy_path):
            _logger.info('Proxy is there. Need to check validity')
            cmd = "voms-proxy-info -exists -hours 72 -file %s" % proxy_path
            stdout, stderr, status = execute(cmd)
            if stdout:
                _logger.debug('stdout is %s ' % stdout)
            if stderr:
                _logger.debug('stderr is %s ' %stderr)
            if status == 1:
                _logger.info('Proxy expires in 3 days or less. We need to renew proxy!')
                if self.store(user_dn, self.__cred_name, production, role=role) == 0:
                    _logger.info('Proxy stored successfully')
                else:
                    _logger.info('Proxy retrieval failed')
            else:
                _logger.info('Proxy is valid for more than 3 days')

        else:
            _logger.info('Proxy is not in the cache repo. Will try to get it from myproxy')
            if self.store(user_dn, self.__cred_name, production, role=role) == 0:
                _logger.info('proxy stored successfully')
            else:
                _logger.info('proxy retrieval failed')
        plain_path = os.path.join(self.__target_path, hashlib.sha1(user_dn + '.plain').hexdigest())
        if os.path.isfile(plain_path):
            return self.checkValidity(plain_path)
        else:
            _logger.info('plain proxy not there at the moment!')

    def checkValidity(self, proxy_path):
        _logger.debug('Need to check validity and expiry!')
        datechecks = [24, 72, 168, 730.484]
        #datechecks = [1,2,3,4]
        status = 0
        for i in datechecks:
            cmd = "voms-proxy-info -exists -hours %s -file %s" % (i, proxy_path)
            stdout, stderr, status = execute(cmd)
            if status == 1:
                _logger.debug('Proxy expires in %s hours. We need to send a notification!' %i)
                return i
        return status


    # get extension
    def getExtension(self, role):
        if role is not None:
            return '.' + role.split('=')[-1]
        return None
