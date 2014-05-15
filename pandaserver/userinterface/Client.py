'''
client methods

'''

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
    baseURL = 'http://pandaserver.cern.ch:25080/server/panda'
try:
    baseURLSSL = os.environ['PANDA_URL_SSL']
except:
    baseURLSSL = 'https://pandaserver.cern.ch:25443/server/panda'


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
                  'CERN'    : {'URL'    : 'http://pandaserver.cern.ch:25080/server/panda',
                               'URLSSL' : 'https://pandaserver.cern.ch:25443/server/panda'},
                  }

# bamboo
baseURLBAMBOO = 'http://pandabamboo.cern.ch:25070/bamboo/bamboo'


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
    for tmpKey,tmpVal in serverURLs.iteritems():
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
    hostname = commands.getoutput('hostname')
    for job in jobs:
        job.creationHost = hostname
    # serialize
    strJobs = pickle.dumps(jobs)
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
        print output
        return status,output
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr =  "ERROR submitJobs : %s %s" % (type,value)
        print errStr
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
    hostname = commands.getoutput('hostname')
    for job in jobs:
        job.creationHost = hostname
    # serialize
    strJobs = pickle.dumps(jobs)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/runTaskAssignment'
    data = {'jobs':strJobs}
    status,output = curl.post(url,data)
    if status!=0:
        print output
        return status,output
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr =  "ERROR runTaskAssignment : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# get job status
def getJobStatus(ids,srvID=None):
    """Get job status

       args:
           ids: the list of PandaIDs
       returns:
           status code
                 0: communication succeeded to the panda server 
                 255: communication failure
           the list of JobSpecs (or Nones for non-existing PandaIDs)
    """     
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = _getURL('URL',srvID) + '/getJobStatus'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getJobStatus : %s %s" % (type,value)
        print errStr
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
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = _getURL('URL') + '/getPandaIDwithJobExeID'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getPandaIDwithJobExeID : %s %s" % (type,value)
        print errStr
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
        return status,pickle.loads(output)
    except:
        print output
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getAssigningTask : %s %s" % (type,value)
        print errStr
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
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/seeCloudTask'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR seeCloudTask : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# kill jobs
def killJobs(ids,code=None,verbose=False,srvID=None,useMailAsID=False):
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
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    curl.verbose = verbose
    # execute
    url = _getURL('URLSSL',srvID) + '/killJobs'
    data = {'ids':strIDs,'code':code,'useMailAsID':useMailAsID}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR killJobs : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# reassign jobs
def reassignJobs(ids,forPending=False):
    """Triggers reassignment of jobs. This is not effective if jobs were preassigned to sites before being submitted. 

       args:
           ids: the list of taskIDs
           forPending: set True if pending jobs are reassigned
       returns:
           status code
                 0: communication succeeded to the panda server 
                 255: communication failure
           return code
                 True: request is processed
                 False: not processed

    """     
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/reassignJobs'
    data = {'ids':strIDs}
    if forPending:
        data['forPending'] = True
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR reassignJobs : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# query PandaIDs (obsolete)
def queryPandaIDs(ids):
    # serialize
    strIDs = pickle.dumps(ids)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/queryPandaIDs'
    data = {'ids':strIDs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR queryPandaIDs : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# query job info per cloud (obsolete)
def queryJobInfoPerCloud(cloud,schedulerID=None):
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/queryJobInfoPerCloud'
    data = {'cloud':cloud}
    if schedulerID != None:
        data['schedulerID'] = schedulerID
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR queryJobInfoPerCloud : %s %s" % (type,value)
        print errStr
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
        if sourcetype != None:
            data['sourcetype'] = sourcetype            
        status,output = curl.get(url,data)
        try:
            tmpRet = status,pickle.loads(output)
            if status != 0:
                return tmpRet
        except:
            print output
            type, value, traceBack = sys.exc_info()
            errStr = "ERROR getJobStatistics : %s %s" % (type,value)
            print errStr
            return EC_Failed,output+'\n'+errStr
        # gather
        for tmpCloud,tmpVal in tmpRet[1].iteritems():
            if not ret.has_key(tmpCloud):
                # append cloud values
                ret[tmpCloud] = tmpVal
            else:
                # sum statistics
                for tmpStatus,tmpCount in tmpVal.iteritems():
                    if ret[tmpCloud].has_key(tmpStatus):
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
        if useMorePG != False:
            data['useMorePG'] = useMorePG
        status,output = curl.get(url,data)
        try:
            tmpRet = status,pickle.loads(output)
            if status != 0:
                return tmpRet
        except:
            print output
            type, value, traceBack = sys.exc_info()
            errStr = "ERROR getJobStatisticsForBamboo : %s %s" % (type,value)
            print errStr
            return EC_Failed,output+'\n'+errStr
        # gather
        for tmpCloud,tmpMap in tmpRet[1].iteritems():
            if not ret.has_key(tmpCloud):
                # append cloud values
                ret[tmpCloud] = tmpMap
            else:
                # sum statistics
                for tmpPType,tmpVal in tmpMap.iteritems():
                    if not ret[tmpCloud].has_key(tmpPType):
                        ret[tmpCloud][tmpPType] = tmpVal
                    else:
                        for tmpStatus,tmpCount in tmpVal.iteritems():
                            if ret[tmpCloud][tmpPType].has_key(tmpStatus):
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
    if useMorePG != False:
        data['useMorePG'] = useMorePG
    status,output = curl.get(url,data)
    try:
        return status,pickle.loads(output)
    except:
        print output
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getHighestPrioJobStat : %s %s" % (type,value)
        print errStr
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
        return status,pickle.loads(output)
    except:
        print output
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getJobsToBeUpdated : %s %s" % (type,value)
        print errStr
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
    strPar = pickle.dumps(params)
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
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR updateProdDBUpdateTimes : %s %s" % (type,value)
        print errStr
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
        return status,pickle.loads(output)
    except:
        print output
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getPandaIDsSite : %s %s" % (type,value)
        print errStr
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
        if not workingGroup in ['',None]:
            data['workingGroup'] = workingGroup
        if not countryGroup in ['',None]:
            data['countryGroup'] = countryGroup
        if not jobType in ['',None]:
            data['jobType'] = jobType
        if not minPriority in ['',None]:
            data['minPriority'] = minPriority
        if not readArchived in ['',None]:    
            data['readArchived'] = readArchived    
        status,output = curl.get(url,data)
        try:
            tmpRet = status,pickle.loads(output)
            if status != 0:
                return tmpRet
        except:
            print output
            type, value, traceBack = sys.exc_info()
            errStr = "ERROR getJobStatisticsPerSite : %s %s" % (type,value)
            print errStr
            return EC_Failed,output+'\n'+errStr
        # gather
        for tmpSite,tmpVal in tmpRet[1].iteritems():
            if not ret.has_key(tmpSite):
                # append site values
                ret[tmpSite] = tmpVal
            else:
                # sum statistics
                for tmpStatus,tmpCount in tmpVal.iteritems():
                    if ret[tmpSite].has_key(tmpStatus):
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
    if not site in ['',None]:
        data['site'] = site
    status,output = curl.get(url,data)
    try:
        return status,pickle.loads(output)
    except:
        print output
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getJobStatisticsWithLabel : %s %s" % (type,value)
        print errStr
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
        return status,pickle.loads(output)
    except:
        print output
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getJobStatisticsPerUserSite : %s %s" % (type,value)
        print errStr
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
    strDSs = pickle.dumps(datasets)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/queryLastFilesInDataset'
    data = {'datasets':strDSs}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        print "ERROR queryLastFilesInDataset : %s %s" % (type,value)
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
    if siteType != None:
        data = {'siteType':siteType}
    status,output = curl.get(url,data)
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getSiteSpecs : %s %s" % (type,value)
        print errStr
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
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getCloudSpecs : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# get nPilots (obsolete)
def getNumPilots():
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/getNumPilots'
    status,output = curl.get(url,{})
    try:
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getNumPilots : %s %s" % (type,value)
        print errStr
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
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getNUserJobs : %s %s" % (type,value)
        print errStr
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
    strSites = pickle.dumps(sites)
    # instantiate curl
    curl = _Curl()
    # execute
    url = baseURL + '/runBrokerage'
    data = {'sites':strSites,
            'atlasRelease':atlasRelease}
    if cmtConfig != None:
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
        return status,pickle.loads(output)
    except:
        type, value, traceBack = sys.exc_info()
        errStr = "ERROR getRW : %s %s" % (type,value)
        print errStr
        return EC_Failed,output+'\n'+errStr


# change job priorities (obsolete)
def changeJobPriorities(newPrioMap):
    # serialize
    newPrioMapStr = pickle.dumps(newPrioMap)
    # instantiate curl
    curl = _Curl()
    curl.sslCert = _x509()
    curl.sslKey  = _x509()
    # execute
    url = baseURLSSL + '/changeJobPriorities'
    data = {'newPrioMap':newPrioMapStr}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
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
        return status,pickle.loads(output)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR insertTaskParams : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# kill task
def killTask(jediTaskID):
    """Kill a task

       args:
           jediTaskID: jediTaskID of the task to be killed 
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
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR killTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# finish task
def finishTask(jediTaskID):
    """Finish a task

       args:
           jediTaskID: jediTaskID of the task to be finished
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
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR finishTask : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# reassign task to a site
def reassignTaskToSite(jediTaskID,site):
    """Reassign a task to a site

       args:
           jediTaskID: jediTaskID of the task to be reassigned
           site: the site name where the task is reassigned 
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
    data = {'jediTaskID':jediTaskID,'site':site}
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR reassignTaskToSite : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr



# reassign task to a cloud
def reassignTaskToCloud(jediTaskID,cloud):
    """Reassign a task to a cloud

       args:
           jediTaskID: jediTaskID of the task to be reassigned
           cloud: the cloud name where the task is reassigned 
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
    status,output = curl.post(url,data)
    try:
        return status,pickle.loads(output)
    except:
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
                 0: unkown task
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
        return status,pickle.loads(output)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        errStr = "ERROR changeTaskPriority : %s %s" % (errtype,errvalue)
        return EC_Failed,output+'\n'+errStr

