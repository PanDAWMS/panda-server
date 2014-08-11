'''
provide web interface to users

'''

import re
import sys
import time
import json
import types
import cPickle as pickle
import jobdispatcher.Protocol as Protocol
import brokerage.broker
import taskbuffer.ProcessGroups
from config import panda_config
#from taskbuffer.JobSpecHTCondor import JobSpecHTCondor
from taskbuffer.WrappedPickle import WrappedPickle
from brokerage.SiteMapper import SiteMapper
from pandalogger.PandaLogger import PandaLogger
from RbLauncher import RbLauncher
from ReBroker import ReBroker
from taskbuffer import PrioUtil
from dataservice.DDM import dq2Info

from taskbuffer.TaskBuffer import taskBuffer


# logger
_logger = PandaLogger().getLogger('UserIFHTCondor')


# main class     
class UserIFHTCondor:
    # constructor
    def __init__(self):
        self.taskBuffer = None
        self.taskBuffer = taskBuffer
        self.taskBuffer.init('dbname', 'dbpass')
        self.init(self.taskBuffer)


    # initialize
    def init(self,taskBuffer):
        self.taskBuffer = taskBuffer


    # add jobs
    def addHTCondorJobs(self, jobsStr, user, host, userFQANs):
        """
            addHTCondorJobs
            args:
#                jobsStr: list of HTCondorJobSpecs
                jobsStr: list of dictionaries with HTCondor job properties
                user: DN of the user adding HTCondor job via this API
                host: remote host of the request
                userFQANs: FQANs of the user's proxy
            returns:
                pickle of list of tuples with GlobalJobID and WmsID
        """
        try:
            # deserialize list of dictionaries
            jobs = pickle.loads(jobsStr)
            _logger.debug("addHTCondorJobs %s len:%s FQAN:%s" % (user, len(jobs), str(userFQANs)))
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error("too many jobs %s, more than %s" % (len(jobs), maxJobs))
                jobs = jobs[:maxJobs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("addHTCondorJobs : %s %s" % (type, value))
            jobs = []
        _logger.debug('jobs= %s' % str(jobs))
        # store jobs
        ret = self.taskBuffer.storeHTCondorJobs(jobs, user, fqans=userFQANs)
        _logger.debug("addHTCondorJobs %s ->:%s" % (user, len(ret)))
        # serialize 
        return pickle.dumps(ret)


    # update jobs
    def updateHTCondorJobs(self, jobsStr, user, host, userFQANs):
        """
            updateHTCondorJobs
            args:
                jobsStr: the list of dictionaries with HTCondorJobSpecs properties 
                    to be updated. 
                    GlobalJobID key has to be present in every dictionary.
                user: DN of the user adding HTCondor job via this API
                host: remote host of the request
                userFQANs: FQANs of the user's proxy
            returns:
                pickle of list of tuples with GlobalJobID and WmsID
        """
        try:
            # deserialize list of dictionaries
            jobs = pickle.loads(jobsStr)
            _logger.debug("updateHTCondorJobs %s len:%s FQAN:%s" % (user, len(jobs), str(userFQANs)))
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error("too many jobs %s, more than %s" % (len(jobs), maxJobs))
                jobs = jobs[:maxJobs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("updateHTCondorJobs : %s %s" % (type, value))
            jobs = []
        _logger.debug('jobs= %s' % str(jobs))
        # store jobs
        ret = self.taskBuffer.updateHTCondorJobs(jobs, user, fqans=userFQANs)
        _logger.debug("updateHTCondorJobs %s ->:%s" % (user, len(ret)))
        # serialize
        return pickle.dumps(ret)


    # remove jobs
    def removeHTCondorJobs(self, jobsStr, user, host, userFQANs):
        """
            removeHTCondorJobs
            args:
                jobsStr: the list of dict with GlobalJobIDs of HTCondor jobs to be removed
                user: DN of the user adding HTCondor job via this API
                host: remote host of the request
                userFQANs: FQANs of the user's proxy
            returns:
                pickle of list of tuples with GlobalJobID and WmsID
        """
        try:
            # deserialize list of dict
            jobs = pickle.loads(jobsStr)
            _logger.debug("removeHTCondorJobs %s len:%s FQAN:%s" % (user, len(jobs), str(userFQANs)))
            maxJobs = 5000
            if len(jobs) > maxJobs:
                _logger.error("too many jobs %s, more than %s" % (len(jobs), maxJobs))
                jobs = jobs[:maxJobs]
        except:
            type, value, traceBack = sys.exc_info()
            _logger.error("removeHTCondorJobs : %s %s" % (type, value))
            jobs = []
        _logger.debug('jobs= %s' % str(jobs))
        # store jobs
        ret = self.taskBuffer.removeHTCondorJobs(jobs, user, fqans=userFQANs)
        _logger.debug("removeHTCondorJobs %s ->:%s" % (user, len(ret)))
        # serialize
        return pickle.dumps(ret)


# Singleton
userIF = UserIFHTCondor()
del UserIFHTCondor


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

# security check
def isSecure(req):
    # check security
    if not Protocol.isSecure(req):
        return False
    # disable limited proxy
    if '/CN=limited proxy' in req.subprocess_env['SSL_CLIENT_S_DN']:
        _logger.warning("access via limited proxy : %s" % req.subprocess_env['SSL_CLIENT_S_DN'])
        return False
    return True


# add jobs
def addHTCondorJobs(req, jobs):
    """
        addHTCondorJobs
        args:
            jobs: the list of HTCondorJobSpecs
        returns:
            response of userIF.addHTCondorJobs
    """
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # get FQAN
    fqans = _getFQAN(req)
    # hostname
    host = req.get_remote_host()
    return userIF.addHTCondorJobs(jobs, user, host, fqans)


# update jobs
def updateHTCondorJobs(req, jobs):
    """
        updateHTCondorJobs
        args:
            jobs: the list of dictionaries with HTCondorJobSpecs properties 
                    to be updated. 
                    GlobalJobID key has to be present in every dictionary.
        returns:
            response of userIF.updateHTCondorJobs
    """
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # get FQAN
    fqans = _getFQAN(req)
    # hostname
    host = req.get_remote_host()
    return userIF.updateHTCondorJobs(jobs, user, host, fqans)


# remove jobs
def removeHTCondorJobs(req, jobs):
    """
        removeHTCondorJobs
        args:
            jobs: the list of GlobalJobIDs of HTCondor jobs to be removed
        returns:
            response of userIF.removeHTCondorJobs
    """
    # check security
    if not isSecure(req):
        return False
    # get DN
    user = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        user = _getDN(req)
    # get FQAN
    fqans = _getFQAN(req)
    # hostname
    host = req.get_remote_host()
    return userIF.removeHTCondorJobs(jobs, user, host, fqans)


