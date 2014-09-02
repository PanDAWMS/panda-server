import subprocess
import hashlib
import os

def execute(program):
    """Run a program on the command line. Return stderr, stdout and status."""
    print("executable: %s" % program)
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

    def store(self, user_dn, cred_name, production=False, server_name='myproxy.cern.ch'):
        """Retrieve proxy from myproxy."""
	proxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn + '.plain').hexdigest())
        cmd = "myproxy-logon -s %s --no_passphrase --out %s -l '%s' -k %s -t 0" % (server_name, proxy_path, user_dn, cred_name)
	# if myproxy.cern.ch fails, try myproxy on bnl as well
        stdout, stderr, status = execute(cmd)
        if stdout:
            print 'stdout is %s ' % stdout
        if stderr:
            print 'stderr is %s ' % stderr
        print('test the status of plain... %s' %status)
	#proxyValidity = checkValidity(proxy_path)
        if production:
	    print 'production proxy needed - need to add voms attributes and store it in the cache'
	    prodproxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + '.prod').hexdigest()))
	    print prodproxy_path
            prodcmd = "voms-proxy-init -valid 96:00 -cert %s -key %s -out %s -voms atlas:/atlas/Role=production" % (proxy_path, proxy_path, prodproxy_path)
	    stdout, stderr, status = execute(prodcmd)
	    if stdout:
		print 'stdout is %s ' % stdout
            if stderr:
		print 'stderr is %s ' % stderr
            print('test the status of production... %s' %status)
        else:
	    # Now we need to add atlas roles and store it
	    atlasproxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn).hexdigest())
            atlasrolescmd = "voms-proxy-init -valid 96:00 -cert %s -key %s -out %s -voms atlas" % (proxy_path, proxy_path, atlasproxy_path)
            stdout, stderr, status = execute(atlasrolescmd)
            if stdout:
                print 'stdout is %s ' % stdout
            if stderr:
                print 'stderr is %s ' % stderr
            print('test the status of atlas... %s' %status)
	# will remove the proxy later on as I need to check the actual validity in order to send notification emails
        #if os.path.exists(proxy_path):
        #    print 'will now remove the plain proxy from the cache'
        #    os.remove(proxy_path)

        return status

    def retrieve(self, user_dn, production=False):
        """Retrieve proxy from proxy cache."""
	if production:
		proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + '.prod').hexdigest()))
	else:
        	proxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn).hexdigest())
	if os.path.isfile(proxy_path):
        	return cat(proxy_path)
	else:
		print 'proxy file does not exist'

    def checkProxy(self, user_dn, production=False):
	"""Check the validity of a proxy."""
        if production:
           proxy_path = os.path.join(self.__target_path, str(hashlib.sha1(user_dn + '.prod').hexdigest()))
	else:
           proxy_path = os.path.join(self.__target_path, hashlib.sha1(user_dn).hexdigest())
	if os.path.isfile(proxy_path):
		print 'Proxy is there. Need to check validity'
		cmd = "voms-proxy-info -exists -hours 72 -file %s" % proxy_path
		stdout, stderr, status = execute(cmd)
		if stdout:
			print 'stdout is %s ' % stdout
		if stderr:
			print 'stderr is %s ' %stderr
		if status == 1:
			print 'Proxy expires in 3 days or less. We need to renew proxy!'	
	                if self.store(user_dn, self.__cred_name, production) == 0:
        	                print 'Proxy stored successfully'
	                else:
        	                print 'Proxy retrieval failed'
		else:
			print 'Proxy is valid for more than 3 days'
	
	else:
		print 'Proxy is not in the cache repo. Will try to get it from myproxy'
		if self.store(user_dn, self.__cred_name, production) == 0:
			print 'proxy stored successfully'
		else:
			print 'proxy retrieval failed'
	plain_path = os.path.join(self.__target_path, hashlib.sha1(user_dn + '.plain').hexdigest())
	if os.path.isfile(plain_path):
		return self.checkValidity(plain_path)
	else:
		print 'plain proxy not there at the moment!'

    def checkValidity(self, proxy_path):
        print 'Need to check validity and expiry!'
	datechecks = [24, 72, 168, 730.484]	
	#datechecks = [1,2,3,4]
	status = 0
	for i in datechecks:
	        cmd = "voms-proxy-info -exists -hours %s -file %s" % (i, proxy_path)
	        stdout, stderr, status = execute(cmd)
	        if status == 1:
        	        print 'Proxy expires in %s hours. We need to send a notification!' %i
			return i
	return status
