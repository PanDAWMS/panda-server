'''
client methods

'''

import os
import re
import sys
import gzip
import uuid
try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode
import socket
import getpass
import tempfile
try:
    import cPickle as pickle
except ImportError:
    import pickle

import json

from pandaserver.srvcore.CoreUtils import commands_get_status_output

# configuration
try:
    baseURL = os.environ['PANDA_URL']
except Exception:
    baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except Exception:
    baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'


# exit code
EC_Failed = 255


# panda server URLs
if 'PANDA_URL_MAP' in os.environ:
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
    except Exception:
        pass
else:
    # default
    serverURLs = {'default' : {'URL'    : baseURL,
                               'URLSSL' : baseURLSSL},
                  'CERN'    : {'URL'    : 'http://pandaserver.cern.ch:25080/server/panda',
                               'URLSSL' : 'https://pandaserver.cern.ch:25443/server/panda'},
                  }

# bamboo
baseURLBAMBOO = 'http://pandabamboo.cern.ch:25070/bamboo/bamboo'


# wrapper for pickle with python 3
def pickle_dumps(obj):
    return pickle.dumps(obj, protocol=0)


def pickle_loads(obj_string):
    try:
        return pickle.loads(obj_string.encode())
    except Exception:
        return pickle.loads(obj_string)


# get URL
def _getURL(type,srvID=None):
    if srvID in serverURLs:
        urls = serverURLs[srvID]
    else:
        urls = serverURLs['default']
    return urls[type]


# get Panda srvIDs
def getPandas():
    srvs = list(serverURLs)
    # remove 'default'
    try:
        srvs.remove('default')
    except Exception:
        pass
    return srvs


# look for a grid proxy certificate
def _x509():
    # see X509_USER_PROXY
    try:
        return os.environ['X509_USER_PROXY']
    except Exception:
        pass
    # see the default place
    x509 = '/tmp/x509up_u%s' % os.getuid()
    if os.access(x509,os.R_OK):
        return x509
    # no valid proxy certificate
    # FIXME
    print("No valid grid proxy certificate found")
    return ''


# check if https
def is_https(url):
    return url.startswith('https://')


# curl class
class _Curl:
    # constructor
    def __init__(self):
        # path to curl
        self.path = 'curl'
        # verification of the host certificate
        if 'PANDA_VERIFY_HOST' in os.environ and os.environ['PANDA_VERIFY_HOST'] == 'off':
            self.verifyHost = False
        else:
            self.verifyHost = True
        # request a compressed response
        self.compress = True
        # SSL cert/key
        self.sslCert = ''
        self.sslKey  = ''
        # verbose
        self.verbose = False
        # use json
        self.use_json = False
        # OIDC
        if 'PANDA_AUTH' in os.environ and os.environ['PANDA_AUTH'] == 'oidc':
            self.oidc = True
            if 'PANDA_AUTH_VO' in os.environ:
                self.authVO = os.environ['PANDA_AUTH_VO']
            else:
                self.authVO = None
            if 'PANDA_AUTH_ID_TOKEN' in os.environ:
                self.idToken = os.environ['PANDA_AUTH_ID_TOKEN']
            else:
                self.idToken = None
        else:
            self.oidc = False

    # GET method
    def get(self, url, data):
        use_https = is_https(url)
        # make command
        com = '%s --silent --get' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        elif 'X509_CERT_DIR' in os.environ:
            com += ' --capath %s' % os.environ['X509_CERT_DIR']
        elif os.path.exists('/etc/grid-security/certificates'):
            com += ' --capath /etc/grid-security/certificates'
        if self.compress:
            com += ' --compressed'
        if self.oidc:
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
            if not self.sslKey:
                self.sslKey = _x509()
            com += ' --key %s' % self.sslKey
        # timeout
        com += ' -m 600'
        # json
        if self.use_json:
            com += ' -H "Accept: application/json"'
        # data
        strData = ''
        for key in data:
            strData += 'data="%s"\n' % urlencode({key:data[key]})
        # write data to temporary config file
        try:
            tmpName = os.environ['PANDA_TMP']
        except Exception:
            tmpName = '/tmp'
        tmpName += '/%s_%s' % (getpass.getuser(), str(uuid.uuid4()))
        tmpFile = open(tmpName,'w')
        tmpFile.write(strData)
        tmpFile.close()
        com += ' --config %s' % tmpName
        com += ' %s' % url
        # execute
        if self.verbose:
            print(com)
            print(strData)
        ret = commands_get_status_output(com)
        # remove temporary file
        os.remove(tmpName)
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        if self.verbose:
            print(ret)
        return ret

    # POST method
    def post(self, url, data, via_file=False):
        use_https = is_https(url)
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        elif 'X509_CERT_DIR' in os.environ:
            com += ' --capath %s' % os.environ['X509_CERT_DIR']
        elif os.path.exists('/etc/grid-security/certificates'):
            com += ' --capath /etc/grid-security/certificates'
        if self.compress:
            com += ' --compressed'
        if self.oidc:
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
            if not self.sslKey:
                self.sslKey = _x509()
            com += ' --key %s' % self.sslKey
        # timeout
        com += ' -m 600'
        # json
        if self.use_json:
            com += ' -H "Accept: application/json"'
        # data
        strData = ''
        for key in data:
            strData += 'data="%s"\n' % urlencode({key:data[key]})
        # write data to temporary config file
        try:
            tmpName = os.environ['PANDA_TMP']
        except Exception:
            tmpName = '/tmp'
        tmpName += '/%s_%s' % (getpass.getuser(), str(uuid.uuid4()))
        tmpNameOut = '{0}.out'.format(tmpName)
        tmpFile = open(tmpName,'w')
        tmpFile.write(strData)
        tmpFile.close()
        com += ' --config %s' % tmpName
        if via_file:
            com += ' -o {0}'.format(tmpNameOut)
        com += ' %s' % url
        # execute
        if self.verbose:
            print(com)
            print(strData)
        s,o = commands_get_status_output(com)
        if via_file:
            with open(tmpNameOut, 'rb') as f:
                ret = (s, f.read())
            os.remove(tmpNameOut)
        else:
            ret = (s, o)
        # remove temporary file
        os.remove(tmpName)
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        if self.verbose:
            print(ret)
        return ret

    # PUT method
    def put(self, url, data):
        use_https = is_https(url)
        # make command
        com = '%s --silent' % self.path
        if not self.verifyHost:
            com += ' --insecure'
        elif 'X509_CERT_DIR' in os.environ:
            com += ' --capath %s' % os.environ['X509_CERT_DIR']
        elif os.path.exists('/etc/grid-security/certificates'):
            com += ' --capath /etc/grid-security/certificates'
        if self.compress:
            com += ' --compressed'
        if self.oidc:
            com += ' -H "Authorization: Bearer {0}"'.format(self.idToken)
            com += ' -H "Origin: {0}"'.format(self.authVO)
        elif use_https:
            if not self.sslCert:
                self.sslCert = _x509()
            com += ' --cert %s' % self.sslCert
            com += ' --cacert %s' % self.sslCert
            if not self.sslKey:
                self.sslKey = _x509()
            com += ' --key %s' % self.sslKey
        # emulate PUT
        for key in data:
            com += ' -F "%s=@%s"' % (key,data[key])
        com += ' %s' % url
        # execute
        if self.verbose:
            print(com)
        ret = commands_get_status_output(com)
        if ret[0] != 0:
            ret = (ret[0]%255,ret[1])
        if self.verbose:
            print(ret)
        return ret


'''
Client API

'''

# use web cache
def useWebCache():
    """Switch to use web cache for some read-only requests so that the number
    of hits to the back-end database is reduced.

       args:
       returns:
    """
    global baseURL
    baseURL = re.sub('25080','25085',baseURL)
    global serverURLs
    for tmpKey in serverURLs:
        tmpVal = serverURLs[tmpKey]
        tmpVal['URL'] = baseURL


# submit jobs
def submitJobs(jobs,srvID=None,toPending=False):
    """Submit jobs

       args:
           jobs: the list of JobSpecs
           srvID: obsolete
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
    strJobs = pickle_dumps(jobs)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = _getURL('URLSSL',srvID) + '/submitJobs'
    data = {'jobs':strJobs}
    if toPending:
        data['toPending'] = True
    status,output = curl.post(url,data)
    if status!=0:
        print(output)
        return status,output
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr =  "ERROR submitJobs : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# run task assignment
def runTaskAssignment(jobs):
    """Run the task brokerage

       args:
           ids: list of typical JobSpecs for tasks to be assigned
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
    strJobs = pickle_dumps(jobs)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/runTaskAssignment'
    data = {'jobs':strJobs}
    status,output = curl.post(url,data)
    if status!=0:
        print(output)
        return status,output
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr =  "ERROR runTaskAssignment : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get job status
def getJobStatus(ids, use_json=False):
    """Get job status

       args:
           ids: the list of PandaIDs
           use_json: using json instead of pickle
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of JobSpecs (or Nones for non-existing PandaIDs)
    """
    # serialize
    if use_json:
        strIDs = json.dumps(ids)
    else:
        strIDs = pickle_dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.use_json = use_json
    # execute
    url = _getURL('URL') + '/getJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url, data, via_file=True)
    try:
        if use_json:
            return status, json.loads(output)
        return status,pickle_loads(output)
    except Exception as e:
        errStr = "ERROR getJobStatus : %s" % str(e)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get PandaID with jobexeID
def getPandaIDwithJobExeID(ids):
    """Get the list of PandaIDs corresponding to a given jobExecutionIDs

       args:
           ids: list of jobExecutionIDs
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of PandaIDs (or Nones for non-existing IDs)
    """
    # serialize
    strIDs = pickle_dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = _getURL('URL') + '/getPandaIDwithJobExeID'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getPandaIDwithJobExeID : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get assigning task
def getAssigningTask():
    """Get the list of IDs of tasks which are being assigned by the
    task brokerage

       args:
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of taskIDs
    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getAssigningTask'
    status,output = curl.get(url,{})
    try:
        return status,pickle_loads(output)
    except Exception:
        print(output)
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getAssigningTask : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get assigned cloud for tasks
def seeCloudTask(ids):
    """Check to which clouds the tasks are assigned

       args:
           ids: the list of taskIDs
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of clouds (or Nones if tasks are not yet assigned)
        raises:
           EC_Failed: if communication failure to the panda server

    """
    # serialize
    strIDs = pickle_dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/seeCloudTask'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR seeCloudTask : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# kill jobs
def killJobs(ids,code=None,verbose=False,srvID=None,useMailAsID=False,keepUnmerged=False, jobSubStatus=None):
    """Kill jobs. Normal users can kill only their own jobs.
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
                 8: rebrokerage
                 9: force kill
                 10: fast rebrokerage on overloaded PQs
                 50: kill by JEDI
                 91: kill user jobs with prod role
           verbose: set True to see what's going on
           srvID: obsolete
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
    strIDs = pickle_dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = _getURL('URLSSL',srvID) + '/killJobs'
    data = {'ids':strIDs,'code':code,'useMailAsID':useMailAsID}
    killOpts = ''
    if keepUnmerged:
        killOpts += 'keepUnmerged,'
    if jobSubStatus is not None:
        killOpts += 'jobSubStatus={0},'.format(jobSubStatus)
    data['killOpts'] = killOpts[:-1]
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR killJobs : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# reassign jobs
def reassignJobs(ids,forPending=False,firstSubmission=None):
    """Triggers reassignment of jobs. This is not effective if jobs were preassigned to sites before being submitted.

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
    strIDs = pickle_dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/reassignJobs'
    data = {'ids':strIDs}
    if forPending:
        data['forPending'] = True
    if firstSubmission is not None:
        data['firstSubmission'] = firstSubmission
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR reassignJobs : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,"stat=%s err=%s %s" % (status,output,errStr)


# query PandaIDs (obsolete)
def queryPandaIDs(ids):
    # serialize
    strIDs = pickle_dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/queryPandaIDs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR queryPandaIDs : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# query job info per cloud (obsolete)
def queryJobInfoPerCloud(cloud,schedulerID=None):
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/queryJobInfoPerCloud'
    data = {'cloud':cloud}
    if schedulerID is not None:
        data['schedulerID'] = schedulerID
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR queryJobInfoPerCloud : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get job statistics
def getJobStatistics(sourcetype=None):
    """Get job statistics

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
    # instantiate curl
    curl = _Curl()
    # execute
    ret = {}
    for srvID in getPandas():
        url = _getURL('URL',srvID) + '/getJobStatistics'
        data = {}
        if sourcetype is not None:
            data['sourcetype'] = sourcetype
        status,output = curl.get(url,data)
        try:
            tmpRet = status,pickle_loads(output)
            if status != 0:
                return tmpRet
        except Exception:
            print(output)
            type, value, traceBack = sys.exc_info()
            errStr = "ERROR getJobStatistics : %s %s" % (type,value)
            print(errStr)
            return EC_Failed,output+'\n'+errStr
        # gather
        for tmpCloud in tmpRet[1]:
            tmpVal = tmpRet[1][tmpCloud]
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
    return 0,ret


# get job statistics for Bamboo
def getJobStatisticsForBamboo(useMorePG=False):
    """Get job statistics for Bamboo

       args:
           useMorePG: set True if fine-grained classification is required
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           map of the number jobs per job status in each site

    """
    # instantiate curl
    curl = _Curl()
    # execute
    ret = {}
    for srvID in getPandas():
        url = _getURL('URL',srvID) + '/getJobStatisticsForBamboo'
        data = {}
        if useMorePG is not False:
            data['useMorePG'] = useMorePG
        status,output = curl.get(url,data)
        try:
            tmpRet = status,pickle_loads(output)
            if status != 0:
                return tmpRet
        except Exception:
            print(output)
            type, value, traceBack = sys.exc_info()
            errStr = "ERROR getJobStatisticsForBamboo : %s %s" % (type,value)
            print(errStr)
            return EC_Failed,output+'\n'+errStr
        # gather
        for tmpCloud in tmpRet[1]:
            tmpMap = tmpRet[1][tmpCloud]
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
    return 0,ret


# get highest prio jobs
def getHighestPrioJobStat(perPG=False,useMorePG=False):
    """Get the number of jobs with the highest priorities in each combination of cloud and processingType

       args:
           perPG: set True if grouped by processingGroup instead of processingType
           useMorePG: set True if fine-grained classification is required
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           map of the number jobs and priorities in each combination of cloud and processingType (or processingGroup)

    """
    # instantiate curl
    curl = _Curl()
    # execute
    ret = {}
    url = baseURL + '/getHighestPrioJobStat'
    data = {'perPG':perPG}
    if useMorePG is not False:
        data['useMorePG'] = useMorePG
    status,output = curl.get(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        print(output)
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getHighestPrioJobStat : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get jobs updated recently
def getJobsToBeUpdated(limit=5000,lockedby='',srvID=None):
    """Get the list of jobs which have been recently updated.

       args:
           limit: the maximum number of jobs
           lockedby: name of the machinery which submitted jobs
           srvID: obsolete
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the lit of PandaIDs

    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = _getURL('URL',srvID) + '/getJobsToBeUpdated'
    status,output = curl.get(url,{'limit':limit,'lockedby':lockedby})
    try:
        return status,pickle_loads(output)
    except Exception:
        print(output)
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getJobsToBeUpdated : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# update prodDBUpdateTimes
def updateProdDBUpdateTimes(params,verbose=False,srvID=None):
    """Update timestamp of jobs when update info is propagated to another database

       args:
           params: map of PandaID and jobStatus and timestamp
           verbose: set True to see what's going on
           srvID: obsolete
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return code
                 True: request is processed
                 False: not processed

    """
    # serialize
    strPar = pickle_dumps(params)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = _getURL('URLSSL',srvID) + '/updateProdDBUpdateTimes'
    data = {'params':strPar}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR updateProdDBUpdateTimes : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get PandaID at site
def getPandaIDsSite(site,status,limit=500):
    """Get the list of jobs in a job status at at a site

       args:
           site: site name
           status: job status
           limit: maximum number of jobs
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of PandaIDs

    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getPandaIDsSite'
    status,output = curl.get(url,{'site':site,'status':status,'limit':limit})
    try:
        return status,pickle_loads(output)
    except Exception:
        print(output)
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getPandaIDsSite : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get job statistics per site
def getJobStatisticsPerSite(predefined=False,workingGroup='',countryGroup='',jobType='',minPriority=None,
                            readArchived=None):
    """Get job statistics with job attributes

       args:
           predefined: get jobs which are assiggned to sites before being submitted
           workingGroup: commna-separated list of workingGroups
           countryGroup: commna-separated list of countryGroups
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
    # instantiate curl
    curl = _Curl()
    # execute
    ret = {}
    for srvID in getPandas():
        url = _getURL('URL',srvID) + '/getJobStatisticsPerSite'
        data = {'predefined':predefined}
        if workingGroup not in ['',None]:
            data['workingGroup'] = workingGroup
        if countryGroup not in ['',None]:
            data['countryGroup'] = countryGroup
        if jobType not in ['',None]:
            data['jobType'] = jobType
        if minPriority not in ['',None]:
            data['minPriority'] = minPriority
        if readArchived not in ['',None]:
            data['readArchived'] = readArchived
        status,output = curl.get(url,data)
        try:
            tmpRet = status,pickle_loads(output)
            if status != 0:
                return tmpRet
        except Exception:
            print(output)
            type, value, traceBack = sys.exc_info()
            errStr = "ERROR getJobStatisticsPerSite : %s %s" % (type,value)
            print(errStr)
            return EC_Failed,output+'\n'+errStr
        # gather
        for tmpSite in tmpRet[1]:
            tmpVal = tmpRet[1][tmpSite]
            if tmpSite not in ret:
                # append site values
                ret[tmpSite] = tmpVal
            else:
                # sum statistics
                for tmpStatus in tmpVal:
                    tmpCount = tmpVal[tmpStatus]
                    if tmpStatus in ret[tmpSite]:
                        ret[tmpSite][tmpStatus] += tmpCount
                    else:
                        ret[tmpSite][tmpStatus] = tmpCount
    return 0,ret


# get job statistics per site with label
def getJobStatisticsWithLabel(site=''):
    """Get job statistics per prodSourceLabel

       args:
           site: commna-separated list of sites. An empty string for all sites.
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           map of the number jobs per job status and prodSourceLabel in each site

    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getJobStatisticsWithLabel'
    data = {}
    if site not in ['',None]:
        data['site'] = site
    status,output = curl.get(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        print(output)
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getJobStatisticsWithLabel : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get the number of waiting jobs per site and user (obsolete)
def getJobStatisticsPerUserSite():
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getJobStatisticsPerUserSite'
    data = {}
    status,output = curl.get(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        print(output)
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getJobStatisticsPerUserSite : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr



# get job statistics per site and resource
def getJobStatisticsPerSiteResource(timeWindow=None):
    """Get job statistics with job attributes

       args:
          timeWindow: to count number of jobs that finish/failed/cancelled for last N minutes. 12*60 by default
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           map of the number jobs per job status in each site and resource

    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getJobStatisticsPerSiteResource'
    data = {}
    if timeWindow is not None:
        data['timeWindow'] = timeWindow
    status,output = curl.get(url,data)
    try:
        return status,json.loads(output)
    except Exception:
        print(output)
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getJobStatisticsPerSiteResource : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get job statistics per site, label, and resource
def get_job_statistics_per_site_label_resource(time_window=None):
    """Get job statistics per site, label, and resource

       args:
          timeWindow: to count number of jobs that finish/failed/cancelled for last N minutes. 12*60 by default
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           map of the number jobs per job status in each site and resource

    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/get_job_statistics_per_site_label_resource'
    data = {}
    if time_window is not None:
        data['time_window'] = time_window
    status,output = curl.get(url,data)
    try:
        return status,json.loads(output)
    except Exception as e:
        print(output)
        errStr = "ERROR get_job_statistics_per_site_label_resource : %s" % str(e)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# query last files in datasets
def queryLastFilesInDataset(datasets):
    """Get names of files which have the largest serial number in each dataset

       args:
           datasets: the list of dataset names
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           map of the dataset name and the file name

    """
    # serialize
    strDSs = pickle_dumps(datasets)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/queryLastFilesInDataset'
    data = {'datasets':strDSs}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        print("ERROR queryLastFilesInDataset : %s %s" % (type,value))
        return EC_Failed,None


# insert sandbox file info
def insertSandboxFileInfo(userName,fileName,fileSize,checkSum,verbose=False):
    """Insert infomation of input sandbox

       args:
           userName: the name of the user
           fileName: the file name
           fileSize: the file size
           fileSize: md5sum of the file
           verbose: set True to see what's going on
       returns:
           status code
                 0: communication succeeded to the panda server
                 else: communication failure

    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/insertSandboxFileInfo'
    data = {'userName':userName,'fileName':fileName,'fileSize':fileSize,'checkSum':checkSum}
    return curl.post(url,data)


# upload input sandbox file
def putFile(file):
    """Upload input sandbox

       args:
           file: the file name
       returns:
           status code
                 0: communication succeeded to the panda server
                 else: communication failure

    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/putFile'
    data = {'file':file}
    return curl.put(url,data)


# delete file (obsolete)
def deleteFile(file):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/deleteFile'
    data = {'file':file}
    return curl.post(url,data)


# touch file (obsolete)
def touchFile(sourceURL,filename):
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = sourceURL + '/server/panda/touchFile'
    data = {'filename':filename}
    return curl.post(url,data)


# get site specs
def getSiteSpecs(siteType=None):
    """Get list of site specifications

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
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getSiteSpecs'
    data = {}
    if siteType is not None:
        data = {'siteType':siteType}
    status,output = curl.get(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getSiteSpecs : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get cloud specs
def getCloudSpecs():
    """Get list of cloud specifications

       args:
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           map of cloud and attributes

    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getCloudSpecs'
    status,output = curl.get(url,{})
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getCloudSpecs : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# get nPilots (obsolete)
def getNumPilots():
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getNumPilots'
    status,output = curl.get(url,{})
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getNumPilots : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr



# get a list of DN/myproxy pass phrase/queued job count at a site
def getNUserJobs(siteName):
    """Get a list of DN/myproxy pass phrase/queued job count at a site. production or pilot role is required

       args:
           siteName: the site name
       returns:
           status code
                 0: communication succeeded to the panda server
                 else: communication failure
           a dictionary of DN, myproxy pass phrase, queued job count, hostname of myproxy server

    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/getNUserJobs'
    data = {'siteName':siteName}
    status,output = curl.get(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getNUserJobs : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# run brokerage
def runBrokerage(sites,atlasRelease,cmtConfig=None):
    """Run brokerage

       args:
           sites: the list of candidate sites
           atlasRelease: version number of SW release
           cmtConfig: cmt config
       returns:
           status code
                 0: communication succeeded to the panda server
                 else: communication failure
           the name of the selected site

    """
    # serialize
    strSites = pickle_dumps(sites)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/runBrokerage'
    data = {'sites':strSites,
            'atlasRelease':atlasRelease}
    if cmtConfig is not None:
        data['cmtConfig'] = cmtConfig
    return curl.get(url,data)


# get RW
def getRW(priority=0):
    """Get the amount of workload queued in each cloud

       args:
           priority: workload with higher priorities than this value
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           map of cloud and the amount of workload

    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURLBAMBOO + '/getRW'
    # get RWs for high priority tasks
    data = {'priority':priority}
    status,output = curl.get(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getRW : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# change job priorities (obsolete)
def changeJobPriorities(newPrioMap):
    # serialize
    newPrioMapStr = pickle_dumps(newPrioMap)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeJobPriorities'
    data = {'newPrioMap':newPrioMapStr}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR changeJobPriorities : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr


# insert task params
def insertTaskParams(taskParams):
    """Insert task parameters

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/insertTaskParams'
    data = {'taskParams':taskParamsStr}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR insertTaskParams : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# kill task
def killTask(jediTaskID, broadcast=False):
    """Kill a task

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/killTask'
    data = {'jediTaskID':jediTaskID}
    data['properErrorCode'] = True
    data['broadcast'] = broadcast
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR killTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr


# finish task
def finishTask(jediTaskID, soft=False, broadcast=False):
    """Finish a task

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/finishTask'
    data = {'jediTaskID':jediTaskID}
    data['properErrorCode'] = True
    data['broadcast'] = broadcast
    if soft:
        data['soft'] = True
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR finishTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# reassign task to a site
def reassignTaskToSite(jediTaskID,site,mode=None):
    """Reassign a task to a site. Existing jobs are killed and new jobs are generated at the site

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
        return EC_Failed,'site parameter is too long > {0}chars'.format(maxSite)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/reassignTask'
    data = {'jediTaskID':jediTaskID,'site':site}
    if mode is not None:
        data['mode'] = mode
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR reassignTaskToSite : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# reassign task to a cloud
def reassignTaskToCloud(jediTaskID,cloud,mode=None):
    """Reassign a task to a cloud. Existing jobs are killed and new jobs are generated in the cloud

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/reassignTask'
    data = {'jediTaskID':jediTaskID,'cloud':cloud}
    if mode is not None:
        data['mode'] = mode
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR reassignTaskToCloud : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# reassign task to a nucleus
def reassignTaskToNucleus(jediTaskID,nucleus,mode=None):
    """Reassign a task to a nucleus. Existing jobs are killed and new jobs are generated in the cloud

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/reassignTask'
    data = {'jediTaskID':jediTaskID,'nucleus':nucleus}
    if mode is not None:
        data['mode'] = mode
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR reassignTaskToCloud : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# upload log
def uploadLog(logStr,logFileName):
    """Upload sandbox

       args:
           logStr: log message
           logFileName: name of log file
       returns:
           status code
                 0: communication succeeded to the panda server
                 else: communication failure

    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # write log to a tmp file
    fh = tempfile.NamedTemporaryFile(delete=False)
    gfh = gzip.open(fh.name,mode='wb')
    if sys.version_info[0] >= 3:
        logStr = logStr.encode('utf-8')
    gfh.write(logStr)
    gfh.close()
    # execute
    url = baseURLSSL + '/uploadLog'
    data = {'file':'{0};filename={1}'.format(fh.name,logFileName)}
    retVal = curl.put(url,data)
    os.unlink(fh.name)
    return retVal



# change task priority
def changeTaskPriority(jediTaskID,newPriority):
    """Change task priority

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeTaskPriority'
    data = {'jediTaskID':jediTaskID,
            'newPriority':newPriority}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR changeTaskPriority : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# set debug mode
def setDebugMode(pandaID,modeOn):
    """Turn debug mode on/off for a job

       args:
           pandaID: PandaID of the job
           modeOn: True to turn it on. Oppositely, False
       returns:
           status code
                 0: communication succeeded to the panda server
                 another: communication failure
           error message
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/setDebugMode'
    data = {'pandaID':pandaID,
            'modeOn':modeOn}
    return curl.post(url,data)



# retry task
def retryTask(jediTaskID, verbose=False, noChildRetry=False, discardEvents=False, disable_staging_mode=False):
    """Retry task

       args:
           jediTaskID: jediTaskID of the task to retry
           noChildRetry: True not to retry child tasks
           discardEvents: discard events
           disable_staging_mode: disable staging mode
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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/retryTask'
    data = {'jediTaskID':jediTaskID}
    data['properErrorCode'] = True
    if noChildRetry:
        data['noChildRetry'] = True
    if discardEvents:
        data['discardEvents'] = True
    if disable_staging_mode:
        data['disable_staging_mode'] = True
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR retryTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# reload input
def reloadInput(jediTaskID,verbose=False):
    """Retry task

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/reloadInput'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR reloadInput : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# change task walltime
def changeTaskWalltime(jediTaskID,wallTime):
    """Change task priority

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeTaskAttributePanda'
    data = {'jediTaskID':jediTaskID,
            'attrName':'wallTime',
            'attrValue':wallTime}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR changeTaskWalltime : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# change task cputime
def changeTaskCputime(jediTaskID,cpuTime):
    """Change task cpuTime

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeTaskAttributePanda'
    data = {'jediTaskID':jediTaskID,
            'attrName':'cpuTime',
            'attrValue':cpuTime}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR changeTaskCputime : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# change task RAM count
def changeTaskRamCount(jediTaskID,ramCount):
    """Change task priority

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeTaskAttributePanda'
    data = {'jediTaskID':jediTaskID,
            'attrName':'ramCount',
            'attrValue':ramCount}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR changeTaskRamCount : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# change task attribute
def changeTaskAttribute(jediTaskID,attrName,attrValue):
    """Change task attribute

       args:
           jediTaskID: jediTaskID of the task to change the attribute
           attrName: attribute name
           attrValue: new value for the attribute
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tupple of return code and message
                 0: unknown task
                 1: succeeded
                 2: disallowed to update the attribute
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeTaskAttributePanda'
    data = {'jediTaskID':jediTaskID,
            'attrName':attrName,
            'attrValue':attrValue}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR changeTaskAttributePanda : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# change split rule for task
def changeTaskSplitRule(jediTaskID,ruleName,ruleValue):
    """Change split rule fo task

       args:
           jediTaskID: jediTaskID of the task to change the rule
           ruleName: rule name
           ruleValue: new value for the rule
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tupple of return code and message
                 0: unknown task
                 1: succeeded
                 2: disallowed to update the attribute
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeTaskSplitRulePanda'
    data = {'jediTaskID':jediTaskID,
            'attrName':ruleName,
            'attrValue':ruleValue}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR changeTaskSplitRule : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# pause task
def pauseTask(jediTaskID,verbose=False):
    """Pause task

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/pauseTask'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR pauseTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# resume task
def resumeTask(jediTaskID,verbose=False):
    """Resume task

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/resumeTask'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR resumeTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# avalanche task
def avalancheTask(jediTaskID,verbose=False):
    """force avalanche for task

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = baseURLSSL + '/avalancheTask'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR resumeTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# increase attempt number for unprocessed files
def increaseAttemptNr(jediTaskID,increase):
    """Change task priority

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/increaseAttemptNrPanda'
    data = {'jediTaskID':jediTaskID,
            'increasedNr':increase}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR increaseAttemptNr : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# kill unfinished jobs
def killUnfinishedJobs(jediTaskID,code=None,verbose=False,srvID=None,useMailAsID=False):
    """Kill unfinished jobs in a task. Normal users can kill only their own jobs.
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
                 8: rebrokerage
                 9: force kill
                 50: kill by JEDI
                 91: kill user jobs with prod role
           verbose: set True to see what's going on
           srvID: obsolete
           useMailAsID: obsolete
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of clouds (or Nones if tasks are not yet assigned)
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = _getURL('URLSSL',srvID) + '/killUnfinishedJobs'
    data = {'jediTaskID':jediTaskID,'code':code,'useMailAsID':useMailAsID}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR killUnfinishedJobs : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr



# trigger task brokerage
def triggerTaskBrokerage(jediTaskID):
    """Trigger task brokerge

       args:
           jediTaskID: jediTaskID of the task to change the attribute
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tupple of return code and message
                 0: unknown task
                 1: succeeded
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeTaskModTimePanda'
    data = {'jediTaskID':jediTaskID,
            'diffValue':-12}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR triggerTaskBrokerage : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# get PanDA IDs with TaskID
def getPandaIDsWithTaskID(jediTaskID):
    """Get PanDA IDs with TaskID

       args:
           jediTaskID: jediTaskID of the task to get lit of PanDA IDs
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the list of PanDA IDs
    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getPandaIDsWithTaskID'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getPandaIDsWithTaskID : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr



# reactivate task
def reactivateTask(jediTaskID, keep_attempt_nr=False, trigger_job_generation=False):
    """Reactivate task

       args:
           jediTaskID: jediTaskID of the task to be reactivated
           keep_attempt_nr: not to reset attempt numbers when being reactivated
           trigger_job_generation: trigger job generation once being reactivated
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tupple of return code and message
                 0: unknown task
                 1: succeeded
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/reactivateTask'
    data = {'jediTaskID':jediTaskID}
    if keep_attempt_nr:
        data['keep_attempt_nr'] = True
    if trigger_job_generation:
        data['trigger_job_generation'] = True
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR reactivateTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# get task status TaskID
def getTaskStatus(jediTaskID):
    """Get task status

       args:
           jediTaskID: jediTaskID of the task to get lit of PanDA IDs
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           the status string
    """
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getTaskStatus'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getTaskStatus : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr



# reassign specified tasks (and their jobs) to a new share
def reassignShare(jedi_task_ids, share, reassign_running=False):
    """
       args:
           jedi_task_ids: task ids to act on
           share: share to be applied to jeditaskids
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tuple of return code and message
                 1: logical error
                 0: success
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()

    jedi_task_ids_pickle = pickle_dumps(jedi_task_ids)
    change_running_pickle = pickle_dumps(reassign_running)
    # execute
    url = baseURLSSL + '/reassignShare'
    data = {'jedi_task_ids_pickle': jedi_task_ids_pickle,
            'share': share,
            'reassign_running': change_running_pickle}
    status, output = curl.post(url, data)

    try:
        return status, pickle_loads(output)
    except Exception:
        err_type, err_value = sys.exc_info()[:2]
        err_str = "ERROR reassignShare : {0} {1}".format(err_type, err_value)
        return EC_Failed, '{0}\n{1}'.format(output, err_str)


# list tasks in a particular share and optionally status
def listTasksInShare(gshare, status='running'):
    """
       args:
           gshare: global share
           status: task status, running by default
       returns:
           status code
                 0: communication succeeded to the panda server
                 255: communication failure
           return: a tuple of return code and jedi_task_ids
                 1: logical error
                 0: success
                 None: database error
    """
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()

    # execute
    url = baseURLSSL + '/listTasksInShare'
    data = {'gshare': gshare,
            'status': status}
    status, output = curl.post(url, data)

    try:
        return status, pickle_loads(output)
    except Exception:
        err_type, err_value = sys.exc_info()[:2]
        err_str = "ERROR listTasksInShare : {0} {1}".format(err_type, err_value)
        return EC_Failed, '{0}\n{1}'.format(output, err_str)


# get taskParamsMap with TaskID
def getTaskParamsMap(jediTaskID):
    """Get task status

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
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getTaskParamsMap'
    data = {'jediTaskID':jediTaskID}
    status,output = curl.post(url,data)
    try:
        return status,pickle_loads(output)
    except Exception:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getTaskParamsMap : %s %s" % (type,value)
        print(errStr)
        return EC_Failed,output+'\n'+errStr


# set num slots for workload provisioning
def setNumSlotsForWP(pandaQueueName, numSlots, gshare=None, resourceType=None, validPeriod=None):
    """Set num slots for workload provisioning

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/setNumSlotsForWP'
    data = {'pandaQueueName': pandaQueueName,
            'numSlots': numSlots}
    if gshare is not None:
        data['gshare'] = gshare
    if resourceType is not None:
        data['resourceType'] = resourceType
    if validPeriod is not None:
        data['validPeriod'] = validPeriod
    status,output = curl.post(url, data)
    try:
        return status, json.loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR setNumSlotsForWP : %s %s" % (errtype,errvalue)
        return EC_Failed, output+'\n'+errStr



# enable jumbo jobs
def enableJumboJobs(jediTaskID, totalJumboJobs=1, nJumboPerSite=1):
    """Enable jumbo jobs

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/enableJumboJobs'
    data = {'jediTaskID': jediTaskID,
            'nJumboJobs': totalJumboJobs,
            'nJumboPerSite': nJumboPerSite}
    status,output = curl.post(url, data)
    try:
        return status, json.loads(output)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR /enableJumboJobs : %s %s" % (errtype,errvalue)
        return EC_Failed, output+'\n'+errStr


# get Global Share status
def getGShareStatus():
    """

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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/getGShareStatus'

    status, output = curl.post(url, {})
    try:
        return status, json.loads(output)
    except Exception:
        err_type,err_value = sys.exc_info()[:2]
        err_str = "ERROR /getGShareStatus : %s %s" % (err_type, err_value)
        return EC_Failed, output+'\n' + err_str

# send a harvester command to panda server in order sweep a panda queue
def sweepPQ(panda_queue, status_list, ce_list, submission_host_list):
    """
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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()

    panda_queue_json = json.dumps(panda_queue)
    status_list_json = json.dumps(status_list)
    ce_list_json = json.dumps(ce_list)
    submission_host_list_json = json.dumps(submission_host_list)

    # execute
    url = baseURLSSL + '/sweepPQ'
    data = {'panda_queue': panda_queue_json,
            'status_list': status_list_json,
            'ce_list': ce_list_json,
            'submission_host_list': submission_host_list_json
            }
    status, output = curl.post(url, data)

    try:
        return status, json.loads(output)
    except Exception:
        err_type, err_value = sys.exc_info()[:2]
        err_str = "ERROR sweepPQ : {0} {1}".format(err_type, err_value)
        return EC_Failed, '{0}\n{1}'.format(output, err_str)

# send a command to a job
def send_command_to_job(panda_id, com):
    """
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
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey = _x509()

    # execute
    url = baseURLSSL + '/send_command_to_job'
    data = {'panda_id': panda_id,
            'com': com
            }
    status, output = curl.post(url, data)

    try:
        return status, json.loads(output)
    except Exception as e:
        err_str = "ERROR send_command_to_job : {}".format(str(e))
        return EC_Failed, '{0}\n{1}'.format(output, err_str)


# get ban list
def get_ban_users(verbose=False):
    """Get ban user list

       args:
           verbose: set True to see what's going on
       returns:
           status code
                 True: communication succeeded to the panda server
                 False: communication failure


    """
    # instantiate curl
    curl = _Curl()
    curl.verbose = verbose
    # execute
    url = baseURL + '/get_ban_users'
    output = None
    try:
        status, output = curl.post(url, {})
        if status == 0:
            return json.loads(output)
        else:
            return False, "bad response: {}".format(output)
    except Exception:
        return False, "broken response: {}".format(output)
