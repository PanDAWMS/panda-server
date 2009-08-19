'''
provide web interface to users

'''

import re
import sys
import time
import types
import cPickle as pickle
import jobdispatcher.Protocol as Protocol
import brokerage.broker
from config import panda_config
from taskbuffer.JobSpec import JobSpec
from brokerage.SiteMapper import SiteMapper
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('UserIF')


class UserIF:
    # constructor
    def __init__(self):
        self.taskBuffer = None
        

    # initialize
    def init(self,taskBuffer):
        self.taskBuffer = taskBuffer


    # submit jobs
    def submitJobs(self,jobsStr,user,host,userFQANs):
        try:
            # deserialize jobspecs
            jobs = pickle.loads(jobsStr)
            _logger.debug("submitJobs %s len:%s FQAN:%s" % (user,len(jobs),str(userFQANs)))
            maxJobs = 2000
            if len(jobs) > maxJobs:
                _logger.error("too may jobs more than %s" % maxJobs)
                jobs = jobs[:maxJobs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("submitJobs : %s %s" % (type,value))
            jobs = []
        # store jobs
        ret = self.taskBuffer.storeJobs(jobs,user,forkSetupper=True,fqans=userFQANs)
        _logger.debug("submitJobs %s ->:%s" % (user,len(ret)))
        # logging
        try:
            # make message
            message = '%s - PandaID =' % host
            for iret in ret:
                message += ' %s' % iret[0]
            # get logger
            _pandaLogger = PandaLogger()            
            _pandaLogger.lock()
            _pandaLogger.setParams({'Type':'submitJobs','User':user})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            logger.info(message)
            # release HTTP handler
            _pandaLogger.release()
        except:
            pass
        # serialize 
        return pickle.dumps(ret)


    # run task assignment
    def runTaskAssignment(self,jobsStr):
        try:
            # deserialize jobspecs
            jobs = pickle.loads(jobsStr)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("runTaskAssignment : %s %s" % (type,value))
            jobs = []
        # run
        ret = self.taskBuffer.runTaskAssignment(jobs)
        # serialize 
        return pickle.dumps(ret)


    # get job status
    def getJobStatus(self,idsStr):
        try:
            # deserialize jobspecs
            ids = pickle.loads(idsStr)
            _logger.debug("getJobStatus len   : %s" % len(ids))
            maxIDs = 2500
            if len(ids) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getJobStatus : %s %s" % (type,value))
            ids = []
        _logger.debug("getJobStatus start : %s" % ids)       
        # peek jobs
        ret = self.taskBuffer.peekJobs(ids)
        _logger.debug("getJobStatus end")
        # serialize 
        return pickle.dumps(ret)


    # get assigned cloud for tasks
    def seeCloudTask(self,idsStr):
        try:
            # deserialize jobspecs
            ids = pickle.loads(idsStr)
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("seeCloudTask : %s %s" % (type,value))
            ids = []
        _logger.debug("seeCloudTask start : %s" % ids)       
        # peek jobs
        ret = {}
        for id in ids:
            tmpRet = self.taskBuffer.seeCloudTask(id)
            ret[id] = tmpRet
        _logger.debug("seeCloudTask end")
        # serialize 
        return pickle.dumps(ret)


    # get assigning task
    def getAssigningTask(self):
        # run
        ret = self.taskBuffer.getAssigningTask()
        # serialize 
        return pickle.dumps(ret)

    
    # get job statistics
    def getJobStatistics(self,sourcetype=None):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForExtIF(sourcetype)
        # serialize 
        return pickle.dumps(ret)


    # get job statistics for Bamboo
    def getJobStatisticsForBamboo(self):
        # get job statistics
        ret = self.taskBuffer.getJobStatisticsForBamboo()
        # serialize 
        return pickle.dumps(ret)
        

    # get job statistics per site
    def getJobStatisticsPerSite(self,predefined=False,workingGroup='',countryGroup='',jobType=''):
        # get job statistics
        ret = self.taskBuffer.getJobStatistics(True,predefined,workingGroup,countryGroup,jobType)
        # serialize 
        return pickle.dumps(ret)


    # query PandaIDs
    def queryPandaIDs(self,idsStr):
        # deserialize IDs
        ids = pickle.loads(idsStr)
        # query PandaIDs 
        ret = self.taskBuffer.queryPandaIDs(ids)
        # serialize 
        return pickle.dumps(ret)


    # get number of analysis jobs per user  
    def getNUserJobs(self,siteName,nJobs):
        # get 
        ret = self.taskBuffer.getNUserJobs(siteName,nJobs)
        # serialize 
        return pickle.dumps(ret)


    # query job info per cloud
    def queryJobInfoPerCloud(self,cloud,schedulerID):
        # query PandaIDs 
        ret = self.taskBuffer.queryJobInfoPerCloud(cloud,schedulerID)
        # serialize 
        return pickle.dumps(ret)

    
    # query PandaIDs at site
    def getPandaIDsSite(self,site,status,limit):
        # query PandaIDs 
        ret = self.taskBuffer.getPandaIDsSite(site,status,limit)
        # serialize 
        return pickle.dumps(ret)


    # get PandaIDs to be updated in prodDB
    def getJobsToBeUpdated(self,limit,lockedby):
        # query PandaIDs 
        ret = self.taskBuffer.getPandaIDsForProdDB(limit,lockedby)
        # serialize 
        return pickle.dumps(ret)


    # update prodDBUpdateTimes
    def updateProdDBUpdateTimes(self,paramsStr):
        # deserialize IDs
        params = pickle.loads(paramsStr)
        # get jobs
        ret = self.taskBuffer.updateProdDBUpdateTimes(params)
        # serialize 
        return pickle.dumps(True)


    # query last files in datasets
    def queryLastFilesInDataset(self,datasetStr):
        # deserialize names
        datasets = pickle.loads(datasetStr)
        # get files
        ret = self.taskBuffer.queryLastFilesInDataset(datasets)
        # serialize 
        return pickle.dumps(ret)


    # get input files currently in used for analysis
    def getFilesInUseForAnal(self,outDataset):
        # get files
        ret = self.taskBuffer.getFilesInUseForAnal(outDataset)
        # serialize 
        return pickle.dumps(ret)


    # kill jobs
    def killJobs(self,idsStr,user,host,code,prodManager):
        # deserialize IDs
        ids = pickle.loads(idsStr)
        if not isinstance(ids,types.ListType):
            ids = [ids]
        _logger.debug("killJob : %s %s %s %s" % (user,code,prodManager,ids))
        # kill jobs
        ret = self.taskBuffer.killJobs(ids,user,code,prodManager)
        # logging
        try:
            # make message
            message = '%s - PandaID =' % host
            for id in ids:
                message += ' %s' % id
            # get logger
            _pandaLogger = PandaLogger()            
            _pandaLogger.lock()
            _pandaLogger.setParams({'Type':'killJobs','User':user})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            logger.info(message)
            # release HTTP handler
            _pandaLogger.release()
        except:
            pass
        # serialize 
        return pickle.dumps(ret)


    # reassign jobs
    def reassignJobs(self,idsStr,user,host):
        # deserialize IDs
        ids = pickle.loads(idsStr)
        # reassign jobs
        ret = self.taskBuffer.reassignJobs(ids,forkSetupper=True)
        # logging
        try:
            # make message
            message = '%s - PandaID =' % host
            for id in ids:
                message += ' %s' % id
            # get logger
            _pandaLogger = PandaLogger()            
            _pandaLogger.lock()
            _pandaLogger.setParams({'Type':'reassignJobs','User':user})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            logger.info(message)
            # release HTTP handler
            _pandaLogger.release()
        except:
            pass
        # serialize 
        return pickle.dumps(ret)
        

    # resubmit jobs
    def resubmitJobs(self,idsStr):
        # deserialize IDs
        ids = pickle.loads(idsStr)
        # kill jobs
        ret = self.taskBuffer.resubmitJobs(ids)
        # serialize 
        return pickle.dumps(ret)


    # get list of site spec
    def getSiteSpecs(self,siteType='analysis'):
        # get analysis site list
        specList = {}
        siteMapper = SiteMapper(self.taskBuffer)
        for id,spec in siteMapper.siteSpecList.iteritems():
            if siteType == 'all' or spec.type == siteType:
                # convert to map
                tmpSpec = {}
                for attr in spec._attributes:
                    tmpSpec[attr] = getattr(spec,attr)
                specList[id] = tmpSpec
        # serialize
        return pickle.dumps(specList)


    # get list of cloud spec
    def getCloudSpecs(self):
        # get cloud list
        siteMapper = SiteMapper(self.taskBuffer)
        # serialize
        return pickle.dumps(siteMapper.cloudSpec)


    # run brokerage
    def runBrokerage(self,sitesStr,cmtConfig,atlasRelease,trustIS=False):
        ret = 'NULL'
        try:
            # deserialize sites
            sites = pickle.loads(sitesStr)
            # instantiate siteMapper
            siteMapper = SiteMapper(self.taskBuffer)
            # instantiate job
            job = JobSpec()
            job.AtlasRelease = atlasRelease
            job.cmtConfig    = cmtConfig
            # run brokerage
            brokerage.broker.schedule([job],self.taskBuffer,siteMapper,True,sites,trustIS)
            # get computingSite
            ret = job.computingSite
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("runBrokerage : %s %s" % (type,value))
        return ret


    # register proxy key
    def registerProxyKey(self,params):
        # register
        ret = self.taskBuffer.registerProxyKey(params)
        # return
        return ret

    
    # get client version
    def getPandaClientVer(self):
        # get
        ret = self.taskBuffer.getPandaClientVer()
        # return
        return ret


    # get proxy key
    def getProxyKey(self,dn):
        # get files
        ret = self.taskBuffer.getProxyKey(dn)
        # serialize 
        return pickle.dumps(ret)


    # get slimmed file info with PandaIDs
    def getSlimmedFileInfoPandaIDs(self,pandaIDsStr,dn):
        try:
            # deserialize IDs
            pandaIDs = pickle.loads(pandaIDsStr)
            # truncate
            maxIDs = 2500
            if len(pandaIDs) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                pandaIDs = pandaIDs[:maxIDs]
            # get
            _logger.debug("getSlimmedFileInfoPandaIDs start : %s %s" % (dn,len(pandaIDs)))            
            ret = self.taskBuffer.getSlimmedFileInfoPandaIDs(pandaIDs)
            _logger.debug("getSlimmedFileInfoPandaIDs end")            
        except:
            ret = {}
        # serialize 
        return pickle.dumps(ret)


    # get JobIDs in a time range
    def getJobIDsInTimeRange(self,dn,timeRange):
        # get IDs
        ret = self.taskBuffer.getJobIDsInTimeRange(dn,timeRange)
        # serialize 
        return pickle.dumps(ret)


    # get PandaIDs for a JobID
    def getPandIDsWithJobID(self,dn,jobID,nJobs):
        # get IDs
        ret = self.taskBuffer.getPandIDsWithJobID(dn,jobID,nJobs)
        # serialize 
        return pickle.dumps(ret)


    # get full job status
    def getFullJobStatus(self,idsStr,dn):
        try:
            # deserialize jobspecs
            ids = pickle.loads(idsStr)
            # truncate
            maxIDs = 2500
            if len(ids) > maxIDs:
                _logger.error("too long ID list more than %s" % maxIDs)
                ids = ids[:maxIDs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("getFullJobStatus : %s %s" % (type,value))
            ids = []
        _logger.debug("getFullJobStatus start : %s %s" % (dn,str(ids)))
        # peek jobs
        ret = self.taskBuffer.getFullJobStatus(ids)
        _logger.debug("getFullJobStatus end")
        # serialize 
        return pickle.dumps(ret)


    # add account to siteaccess
    def addSiteAccess(self,siteID,dn):
        # add
        ret = self.taskBuffer.addSiteAccess(siteID,dn)
        # serialize 
        return pickle.dumps(ret)


    # list site access
    def listSiteAccess(self,siteID,dn,longFormat=False):
        # list
        ret = self.taskBuffer.listSiteAccess(siteID,dn,longFormat)
        # serialize 
        return pickle.dumps(ret)


    # update site access
    def updateSiteAccess(self,method,siteid,requesterDN,userName,attrValue):
        # list
        ret = self.taskBuffer.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)
        # serialize 
        return str(ret)


# Singleton
userIF = UserIF()
del UserIF


# get FQANs
def _getFQAN(req):
    fqans = []
    for tmpKey,tmpVal in req.subprocess_env.iteritems():
        # compact credentials
        if tmpKey.startswith('GRST_CRED_'):
            # VOMS attribute
            if tmpVal.startswith('VOMS'):
                # FQAN
                fqan = tmpVal.split()[-1]
                # append
                fqans.append(fqan)
        # old style         
        elif tmpKey.startswith('GRST_CONN_'):
            tmpItems = tmpVal.split(':')
            # FQAN
            if len(tmpItems)==2 and tmpItems[0]=='fqan':
                fqans.append(tmpItems[-1])
    # return
    return fqans


# get DN
def _getDN(req):
    realDN = ''
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        realDN = req.subprocess_env['SSL_CLIENT_S_DN']
        # remove redundant CN
        realDN = re.sub('/CN=limited proxy','',realDN)
        realDN = re.sub('/CN=proxy(/CN=proxy)+','/CN=proxy',realDN)
    return realDN
                                        


"""
web service interface

"""

# submit jobs
def submitJobs(req,jobs):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # get FQAN
    fqans = _getFQAN(req)
    # hostname
    host = req.get_remote_host()
    return userIF.submitJobs(jobs,user,host,fqans)


# run task assignment
def runTaskAssignment(req,jobs):
    # check security
    if not Protocol.isSecure(req):
        return "False"
    return userIF.runTaskAssignment(jobs)


# get job status
def getJobStatus(req,ids):
    return userIF.getJobStatus(ids)


# get assigning task
def getAssigningTask(req):
    return userIF.getAssigningTask()


# get assigned cloud for tasks
def seeCloudTask(req,ids):
    return userIF.seeCloudTask(ids)


# query PandaIDs
def queryPandaIDs(req,ids):
    return userIF.queryPandaIDs(ids)


# query job info per cloud
def queryJobInfoPerCloud(req,cloud,schedulerID=None):
    return userIF.queryJobInfoPerCloud(cloud,schedulerID)


# get PandaIDs at site
def getPandaIDsSite(req,site,status,limit=500):
    return userIF.getPandaIDsSite(site,status,limit)


# get PandaIDs to be updated in prodDB
def getJobsToBeUpdated(req,limit=5000,lockedby=''):
    limit = int(limit)
    return userIF.getJobsToBeUpdated(limit,lockedby)


# update prodDBUpdateTimes
def updateProdDBUpdateTimes(req,params):
    # check security
    if not Protocol.isSecure(req):
        return False
    return userIF.updateProdDBUpdateTimes(params)


# get job statistics
def getJobStatistics(req,sourcetype=None):
    return userIF.getJobStatistics(sourcetype)


# get job statistics for Babmoo
def getJobStatisticsForBamboo(req):
    return userIF.getJobStatisticsForBamboo()


# get job statistics per site
def getJobStatisticsPerSite(req,predefined='False',workingGroup='',countryGroup='',jobType=''):
    if predefined=='True':
        predefined=True
    else:
        predefined=False
    return userIF.getJobStatisticsPerSite(predefined,workingGroup,countryGroup,jobType)


# query last files in datasets
def queryLastFilesInDataset(req,datasets):
    return userIF.queryLastFilesInDataset(datasets)


# get input files currently in used for analysis
def getFilesInUseForAnal(req,outDataset):
    return userIF.getFilesInUseForAnal(outDataset)


# kill jobs
def killJobs(req,ids,code=None):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)        
    # check role
    prodManager = False
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production','/atlas/Role=production']:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # hostname
    host = req.get_remote_host()
    return userIF.killJobs(ids,user,host,code,prodManager)


# reassign jobs
def reassignJobs(req,ids):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # hostname
    host = req.get_remote_host()
    return userIF.reassignJobs(ids,user,host)


# resubmit jobs
def resubmitJobs(req,ids):
    # check security
    if not Protocol.isSecure(req):
        return False
    return userIF.resubmitJobs(ids)


# get list of site spec
def getSiteSpecs(req,siteType=None):
    if siteType != None:
        return userIF.getSiteSpecs(siteType)
    else:
        return userIF.getSiteSpecs()

# get list of cloud spec
def getCloudSpecs(req):
    return userIF.getCloudSpecs()

# get client version
def getPandaClientVer(req):
    return userIF.getPandaClientVer()

# run brokerage
def runBrokerage(req,sites,cmtConfig=None,atlasRelease=None,trustIS=False):
    if trustIS=='True':
        trustIS = True
    else:
        trustIS = False
    return userIF.runBrokerage(sites,cmtConfig,atlasRelease,trustIS)


# register proxy key
def registerProxyKey(req,credname,origin,myproxy):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    # get expiration date
    if not req.subprocess_env.has_key('SSL_CLIENT_V_END'):
        return False
    params = {}
    params['dn'] = _getDN(req)
    # set parameters
    params['credname'] = credname
    params['origin']   = origin
    params['myproxy']  = myproxy
    # convert SSL_CLIENT_V_END
    try:
        expTime = req.subprocess_env['SSL_CLIENT_V_END']
        # remove redundant white spaces
        expTime = re.sub('\s+',' ',expTime)
        # convert to timestamp
        expTime = time.strptime(expTime,'%b %d %H:%M:%S %Y %Z')
        params['expires']  = time.strftime('%Y-%m-%d %H:%M:%S',expTime)
    except:
        _logger.error("registerProxyKey : failed to convert %s" % \
                      req.subprocess_env['SSL_CLIENT_V_END'])
    # execute
    return userIF.registerProxyKey(params)


# register proxy key
def getProxyKey(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    # execute
    return userIF.getProxyKey(dn)


# get JobIDs in a time range
def getJobIDsInTimeRange(req,timeRange,dn=None):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("getJobIDsInTimeRange %s %s" % (dn,timeRange))
    # execute
    return userIF.getJobIDsInTimeRange(dn,timeRange)


# get PandaIDs for a JobID
def getPandIDsWithJobID(req,jobID,nJobs,dn=None):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    if dn == None:
        dn = _getDN(req)
    _logger.debug("getPandIDsWithJobID %s JobID=%s nJobs=%s" % (dn,jobID,nJobs))
    # execute
    return userIF.getPandIDsWithJobID(dn,jobID,nJobs)


# get slimmed file info with PandaIDs
def getSlimmedFileInfoPandaIDs(req,ids):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    return userIF.getSlimmedFileInfoPandaIDs(ids,dn)


# get full job status
def getFullJobStatus(req,ids):
    # check security
    if not Protocol.isSecure(req):
        return False
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    dn = _getDN(req)
    return userIF.getFullJobStatus(ids,dn)


# get number of analysis jobs per user  
def getNUserJobs(req,siteName,nJobs=100):
    # check security
    prodManager = False
    if not Protocol.isSecure(req):
        return "Failed : HTTPS connection is required"
    # get FQANs
    fqans = _getFQAN(req)
    # loop over all FQANs
    for fqan in fqans:
        # check production role
        for rolePat in ['/atlas/usatlas/Role=production',
                        '/atlas/Role=production',
                        '/atlas/usatlas/Role=pilot',                        
                        '/atlas/Role=pilot',
                        ]:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # only prod managers can use this method
    if not prodManager:
        return "Failed : VOMS authorization failure"
    # convert nJobs to int
    try:
        nJobs = int(nJobs)
    except:
        nJobs = 100
    # execute
    return userIF.getNUserJobs(siteName,nJobs)


# add account to siteaccess
def addSiteAccess(req,siteID):
    # check security
    if not Protocol.isSecure(req):
        return "False"        
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "False"        
    dn = req.subprocess_env['SSL_CLIENT_S_DN']
    return userIF.addSiteAccess(siteID,dn)


# list site access
def listSiteAccess(req,siteID=None,longFormat=False):
    # check security
    if not Protocol.isSecure(req):
        return "False"
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "False"
    # set DN if siteID is none
    dn = None
    if siteID==None:
        dn = req.subprocess_env['SSL_CLIENT_S_DN']
    # convert longFormat option
    if longFormat == 'True':
        longFormat = True
    else:
        longFormat = False
    return userIF.listSiteAccess(siteID,dn,longFormat)


# update site access
def updateSiteAccess(req,method,siteid,userName,attrValue=''):
    # check security
    if not Protocol.isSecure(req):
        return "non HTTPS"
    # get DN
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return "invalid DN"
    # set requester's DN
    requesterDN = req.subprocess_env['SSL_CLIENT_S_DN']
    # update
    return userIF.updateSiteAccess(method,siteid,requesterDN,userName,attrValue)
