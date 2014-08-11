"""
client methods for PanDA - HTCondor API

"""
import os
import re
import sys
import gzip
import urllib
import commands
import tempfile
import cPickle as pickle

try:
    import json
except:
    import simplejson as json

# configuration
try:
    baseURL = os.environ['PANDA_URL']
except:
    baseURL = 'http://pandawms.org:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    baseURLSSL = 'https://pandawms.org:25443/server/panda'


# exit code
EC_Failed = 255


# panda server URLs
if os.environ.has_key('PANDA_URL_MAP'):
    serverURLs = {'default' : {'URL'    : baseURL,
                               'URLSSL' : baseURLSSL},
                  }
    # decode envvar to map
    try:
        for tmpCompStr in os.environ['PANDA_URL_MAP'].split('|'):
            tmpKey,tmpURL,tmpURLSSL = tmpCompStr.split(',')
            # append
            serverURLs[tmpKey] = {'URL'    : tmpURL,
                                  'URLSSL' : tmpURLSSL}
    except:
        pass
else:
    # default
    serverURLs = {'default' : {'URL'    : baseURL,
                               'URLSSL' : baseURLSSL},
                  'EC2'    : {'URL'    : 'http://pandawms.org:25080/server/panda',
                               'URLSSL' : 'https://pandawms.org:25443/server/panda'},
                  }

# get URL
def _getURL(type,srvID=None):
    if serverURLs.has_key(srvID):
        urls = serverURLs[srvID]
    else:
        urls = serverURLs['default']
    return urls[type]
    

# get Panda srvIDs
def getPandas():
    srvs = serverURLs.keys()
    # remove 'default'
    try:
        srvs.remove('default')
    except:
        pass
    return srvs


# look for a grid proxy certificate
def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    print "No valid grid proxy certificate found"
    return ''


# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl'
        # verification of the host certificate
        self.verifyHost = True
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # verbose
        self.verbose = False
#        self.verbose = True

    # GET method
    def get(self,url,data):
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        elif os.environ.has_key('X509_CERT_DIR'):
            com += ' --capath %s' % os.environ['X509_CERT_DIR']
        elif os.path.exists('/etc/grid-security/certificates'):
            com += ' --capath /etc/grid-security/certificates'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # timeout
        com += ' -m 600' 
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        try:
            tmpName = os.environ['PANDA_TMP']
        except:
            tmpName = '/tmp'
        tmpName += '/%s_%s' % (commands.getoutput('whoami'),commands.getoutput('uuidgen'))
        tmpFile = open(tmpName,'w')
        tmpFile.write(strData)
        tmpFile.close()
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print commands.getoutput('cat %s' % tmpName)
        ret = commands.getstatusoutput(com)
        # remove temporary file
        os.remove(tmpName)
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        if self.verbose:
            print ret
        return ret


    # POST method
    def post(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        elif os.environ.has_key('X509_CERT_DIR'):
            com += ' --capath %s' % os.environ['X509_CERT_DIR']
        elif os.path.exists('/etc/grid-security/certificates'):
            com += ' --capath /etc/grid-security/certificates'
        if self.compress:
            com += ' --compressed'
        if os.environ.has_key('CURL_CA_BUNDLE'):
            com += ' --cacert %s' % os.environ['CURL_CA_BUNDLE']
        else:
            com += ' --cacert %s' % self.sslCert
        if os.environ.has_key('CURL_SSLCERT'):
            com += ' --cert %s' % os.environ['CURL_SSLCERT']
        elif self.sslCert != '':
            com += ' --cert %s' % self.sslCert
        if os.environ.has_key('CURL_SSLCERT'):
            com += ' --key %s' % os.environ['CURL_SSLKEY']
        elif self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # timeout
        com += ' -m 600' 
        # data
        strData = ''
        for key in data.keys():
            strData += 'data="%s"\n' % urllib.urlencode({key:data[key]})
        # write data to temporary config file
        try:
            tmpName = os.environ['PANDA_TMP']
        except:
            tmpName = '/tmp'
        tmpName += '/%s_%s' % (commands.getoutput('whoami'),commands.getoutput('uuidgen'))
        tmpFile = open(tmpName,'w')
        tmpFile.write(strData)
        tmpFile.close()
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
            print commands.getoutput('cat %s' % tmpName)
        ret = commands.getstatusoutput(com)
        # remove temporary file
        os.remove(tmpName)
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        if self.verbose:
            print ret
        return ret


    # PUT method
    def put(self,url,data):
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        elif os.environ.has_key('X509_CERT_DIR'):
            com += ' --capath %s' % os.environ['X509_CERT_DIR']
        elif os.path.exists('/etc/grid-security/certificates'):
            com += ' --capath /etc/grid-security/certificates'
        if self.compress:
            com += ' --compressed'
        if self.sslCert != '':
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
        if self.sslKey != '':
            com += ' --key %s' % self.sslKey
        # emulate PUT 
        for key in data.keys():
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        # execute
        if self.verbose:
            print com
        ret = commands.getstatusoutput(com)
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        if self.verbose:
            print ret
        return ret
            

"""
Client API

"""

# add HTCondor jobs
def addHTCondorJobs(jobs, srvID=None):
    """
        add HTCondor jobs
        args:
            jobs: the list of HTCondorJobSpecs
            srvID: obsoleted
        returns:
            status code
                0: communication succeeded to the panda server 
                255: communication failure
            return code
                True: request is processed
                False: not processed
    """
    # serialize
    strJobs = pickle.dumps(jobs)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = _getURL('URLSSL', srvID) + '/addHTCondorJobs'
    data = {'jobs':strJobs}
    status,output = curl.post(url,data)
    if status != 0:
        print output
        return status,output
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR addHTCondorJobs : %s %s" % (type, value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# update HTCondor jobs
def updateHTCondorJobs(jobs, srvID=None):
    """
        update HTCondor jobs
        args:
            jobs: the list of dictionaries with HTCondorJobSpecs properties 
                    to be updated. 
                    GlobalJobID key has to be present in every dictionary.
            srvID: obsoleted  
        returns:
            status code
                0: communication succeeded to the panda server 
                255: communication failure
            return code
                True: request is processed
                False: not processed
    """
    # serialize
    strJobs = pickle.dumps(jobs)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()
    # execute
    url = _getURL('URLSSL', srvID) + '/updateHTCondorJobs'
    data = {'jobs':strJobs}
    status, output = curl.post(url, data)
    if status != 0:
        print output
        return status, output
    try:
        return status, pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR updateHTCondorJobs : %s %s" % (type, value)
        print errStr
        return EC_Failed, output + '\n' + errStr


# remove HTCondor jobs
def removeHTCondorJobs(jobs, srvID=None):
    """
        remove HTCondor jobs
        args:
            jobs: the list of GlobalJobIDs of HTCondor jobs to be removed
            srvID: obsoleted
        returns:
            status code
                0: communication succeeded to the panda server 
                255: communication failure
            return code
                True: request is processed
                False: not processed
    """
    # serialize
    strJobs = pickle.dumps(jobs)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()
    # execute
    url = _getURL('URLSSL', srvID) + '/removeHTCondorJobs'
    data = {'jobs':strJobs}
    status, output = curl.post(url, data)
    if status != 0:
        print output
        return status, output
    try:
        return status, pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR removeHTCondorJobs : %s %s" % (type, value)
        print errStr
        return EC_Failed, output + '\n' + errStr


