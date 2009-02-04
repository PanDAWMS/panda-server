"""
dispatch jobs

"""

import re
import types
import threading
import Protocol
import datetime
import commands
from config import panda_config
from dataservice.Adder import Adder
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('JobDispatcher')


# a wrapper to install timpout into a method
class _TimedMethod:
    def __init__(self,method,timeout):
        self.method  = method
        self.timeout = timeout
        self.result  = Protocol.TimeOutToken

    # method emulation    
    def __call__(self,*var):
        self.result = apply(self.method,var)

    # run
    def run(self,*var):
        thr = threading.Thread(target=self,args=var)
        # run thread
        thr.start()
        thr.join(self.timeout)


# job dipatcher
class JobDipatcher:
    # constructor
    def __init__(self):
        # taskbuffer
        self.taskBuffer = None
        # number of activated analysis jobs
        self.nAnalysisJobs = {}
        # datetime of last updated
        self.lastUpdated = datetime.datetime.utcnow()
        # how frequently update nAnalysisJobs
        self.timeInterval = datetime.timedelta(seconds=180)

    
    # set task buffer
    def init(self,taskBuffer):
        self.taskBuffer = taskBuffer


    # get job
    def getJob(self,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
               atlasRelease,prodUserID,getProxyKey):
        jobs = []
        # wrapper function for timeout
        tmpWrapper = _TimedMethod(self.taskBuffer.getJobs,timeout)
        tmpWrapper.run(1,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                       atlasRelease,prodUserID,getProxyKey)
        if isinstance(tmpWrapper.result,types.ListType):
            jobs = jobs + tmpWrapper.result
        # make response
        if len(jobs) > 0:
            proxyKey = jobs[-1]
            nSent    = jobs[-2]
            jobs     = jobs[:-2]
        if len(jobs) != 0:
            # succeed
            response=Protocol.Response(Protocol.SC_Success)
            # append Job
            response.appendJob(jobs[0])
            # append nSent
            response.appendNode('nSent',nSent)
            # set proxy key
            if getProxyKey:
                response.setProxyKey(proxyKey)
        else:
            if tmpWrapper.result == Protocol.TimeOutToken:
                # timeout
                response=Protocol.Response(Protocol.SC_TimeOut)
            else:
                # no available jobs
                response=Protocol.Response(Protocol.SC_NoJobs)
        # return
        _logger.debug("getJob : %s %s ret -> %s" % (siteName,node,response.encode()))
        return response.encode()


    # update job status
    def updateJob(self,jobID,jobStatus,timeout,xml,siteName,param,metadata):
        # retry failed analysis job and ddm job
        #if jobStatus=='failed' and siteName != None and siteName.startswith('ANALY_') \
        if jobStatus=='failed' \
               and ((param.has_key('pilotErrorCode') and param['pilotErrorCode'] in ['1200','1201']) \
                    or (siteName != None and siteName.find('DDM') != -1)):
            # retry
            ret = self.taskBuffer.retryJob(jobID,param)
            if ret:
                # return succeed
                response=Protocol.Response(Protocol.SC_Success)
                return response.encode()
        # add metadata
        if metadata != '':
            self.taskBuffer.addMetadata([jobID],[metadata])
        # update
        tmpStatus = jobStatus
        if jobStatus == 'failed' or jobStatus == 'finished':
            tmpStatus = 'holding'
        if tmpStatus == 'holding':
            tmpWrapper = _TimedMethod(self.taskBuffer.updateJobStatus,None)
        else:
            tmpWrapper = _TimedMethod(self.taskBuffer.updateJobStatus,timeout)            
        tmpWrapper.run(jobID,tmpStatus,param)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result:
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                # set command
                if isinstance(tmpWrapper.result,types.StringType):
                    response.appendNode('command',tmpWrapper.result)
                else:
                    response.appendNode('command','NULL')
                # add output to dataset
                if jobStatus == 'failed' or jobStatus == 'finished':
                    Adder(self.taskBuffer,jobID,xml,jobStatus).start()
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("updateJob : %s ret -> %s" % (jobID,response.encode()))                
        return response.encode()


    # get job status
    def getStatus(self,strIDs,timeout):
        # convert str to list
        ids = strIDs.split()
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.peekJobs,timeout)            
        tmpWrapper.run(ids,False,True,True,False)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if isinstance(tmpWrapper.result,types.ListType):
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                # make return
                retStr = ''
                attStr = ''
                for job in tmpWrapper.result:
                    if job == None:
                        retStr += '%s+' % 'notfound'
                        attStr += '0+'
                    else:
                        retStr += '%s+' % job.jobStatus
                        attStr += '%s+' % job.attemptNr
                response.appendNode('status',retStr[:-1])
                response.appendNode('attemptNr',attStr[:-1])
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("getStatus : %s ret -> %s" % (strIDs,response.encode()))                
        return response.encode()


    # check if there is activated analysis job
    def isThereAnalysisJob(self,siteName):
        # get current datetime
        current = datetime.datetime.utcnow()
        if current-self.lastUpdated > self.timeInterval:
            # get number of processes
            nProcesses = float(commands.getoutput(
                'ps aux|grep httpd|grep %s|grep -v grep|wc -l' % panda_config.loggername))-1.0
            # get numbers of analysis jobs
            self.nAnalysisJobs = self.taskBuffer.getNAnalysisJobs(nProcesses)
            # set datetime
            self.lastUpdated = current
        # no jobs at the site
        if not self.nAnalysisJobs.has_key(siteName):
            return False
        nJob = self.nAnalysisJobs[siteName]
        # no remaining jobs
        if nJob < 0.0:
            return False
        # decrement
        nJob -= 1.0
        self.nAnalysisJobs[siteName] = nJob
        return True
    
        
# Singleton
jobDispatcher = JobDipatcher()
del JobDipatcher


"""
web service interface

"""

# get job
def getJob(req,siteName,timeout=60,cpu=None,mem=None,diskSpace=None,prodSourceLabel=None,node=None,
           computingElement=None,AtlasRelease=None,prodUserID=None,getProxyKey=None):
    # get FQANs
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
    # check production role
    prodManager = False
    for fqan in fqans:
        # check atlas/usatlas production role
        for rolePat in ['/atlas/usatlas/Role=production',
                        '/atlas/usatlas/Role=pilot',                        
                        '/atlas/Role=production',
                        '/atlas/Role=pilot']:
            if fqan.startswith(rolePat):
                prodManager = True
                break
        # escape
        if prodManager:
            break
    # get DN
    realDN = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        realDN = req.subprocess_env['SSL_CLIENT_S_DN']
        # remove redundant CN
        realDN = re.sub('/CN=limited proxy','',realDN)
        realDN = re.sub('/CN=proxy(/CN=proxy)+','/CN=proxy',realDN)        
    # set DN for non-production user
    if not prodManager:
        prodUserID = realDN
    # allow getProxyKey for production role
    if getProxyKey == 'True' and prodManager:
        getProxyKey = True
    else:
        getProxyKey = False
    # convert mem and diskSpace
    try:
        mem = int(float(mem))
        if mem < 0:
            mem = 0
    except:
        mem = 0        
    try:
        diskSpace = int(float(diskSpace))
        if diskSpace < 0:
            diskSpace = 0
    except:
        diskSpace = 0        
    _logger.debug("getJob(%s) %s" % (siteName,realDN))
    _logger.debug("getJob(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (siteName,cpu,mem,diskSpace,prodSourceLabel,node,
                                                             computingElement,AtlasRelease,prodUserID,getProxyKey))
    # logging
    try:
        # make message
        message = '%s - siteID:%s cpu:%s mem:%s label:%s' % (node,siteName,cpu,mem,prodSourceLabel)
        # get logger
        _pandaLogger = PandaLogger()
        _pandaLogger.lock()
        _pandaLogger.setParams({'Type':'getJob'})
        logger = _pandaLogger.getHttpLogger(panda_config.loggername)
        # add message
        logger.info(message)
        # release HTTP handler
        _pandaLogger.release()
    except:
        pass
    # invoke JD
    return jobDispatcher.getJob(siteName,prodSourceLabel,cpu,mem,diskSpace,node,int(timeout),
                                computingElement,AtlasRelease,prodUserID,getProxyKey)
    

# update job status
def updateJob(req,jobId,state,transExitCode=None,pilotErrorCode=None,pilotErrorDiag=None,timestamp=None,timeout=60,
              xml='',node=None,workdir=None,cpuConsumptionTime=None,cpuConsumptionUnit=None,remainingSpace=None,
              schedulerID=None,pilotID=None,siteName=None,messageLevel=None,pilotLog='',metaData='',
              cpuConversionFactor=None,exeErrorCode=None,exeErrorDiag=None,pilotTiming=None,computingElement=None):
    _logger.debug("updateJob(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n==XML==\n%s\n==LOG==\n%s\n==Meta==\n%s)" %
                  (jobId,state,transExitCode,pilotErrorCode,pilotErrorDiag,node,workdir,cpuConsumptionTime,
                   cpuConsumptionUnit,remainingSpace,schedulerID,pilotID,siteName,messageLevel,
                   cpuConversionFactor,exeErrorCode,exeErrorDiag,pilotTiming,computingElement,
                   xml,pilotLog,metaData))
    # remaining space
    if remainingSpace != None and state != 'running':
        try:
            # make message
            message = '%s - %s GB' % (siteName,remainingSpace)
            # get logger
            _pandaLogger = PandaLogger()
            _pandaLogger.lock()
            _pandaLogger.setParams({'Type':'remainingSpace'})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            if messageLevel == 'warning':
                logger.warning(message)
            elif messageLevel == 'critical':
                logger.critical(message)
            else:
                logger.info(message)                
            # release HTTP handler
            _pandaLogger.release()
        except:
            pass
    # aborting message
    if jobId=='NULL':
        return Protocol.Response(Protocol.SC_Success).encode()
    # pilot log
    if pilotLog != '':
        try:
            # make message
            message = pilotLog
            # get logger
            _pandaLogger = PandaLogger()
            _pandaLogger.lock()
            _pandaLogger.setParams({'Type':'pilotLog','PandaID':int(jobId)})
            logger = _pandaLogger.getHttpLogger(panda_config.loggername)
            # add message
            logger.info(message)                
            # release HTTP handler
            _pandaLogger.release()
        except:
            pass
    # logging
    try:
        # make message
        message = '%s - siteID:%s state:%s' % (node,siteName,state)
        if transExitCode != None:
            message += ' exitcode:%s' % transExitCode
        if cpuConversionFactor != None:
            message += ' cpuConversionFactor:%s' % cpuConversionFactor
        # get logger
        _pandaLogger = PandaLogger()
        _pandaLogger.lock()
        _pandaLogger.setParams({'Type':'updateJob','PandaID':int(jobId)})
        logger = _pandaLogger.getHttpLogger(panda_config.loggername)
        # add message
        logger.info(message)
        # release HTTP handler
        _pandaLogger.release()
    except:
        pass
    # create parameter map
    param = {}
    if cpuConsumptionTime != None:
        param['cpuConsumptionTime']=cpuConsumptionTime
    if cpuConsumptionUnit != None:
        param['cpuConsumptionUnit']=cpuConsumptionUnit
    if node != None:
        param['modificationHost']=node
    if transExitCode != None:
        param['transExitCode']=transExitCode
    if pilotErrorCode != None:
        param['pilotErrorCode']=pilotErrorCode
    if pilotErrorDiag != None:
        param['pilotErrorDiag']=pilotErrorDiag
    if schedulerID != None:
        param['schedulerID']=schedulerID
    if pilotID != None:
        param['pilotID']=pilotID
    if exeErrorCode != None:
        param['exeErrorCode']=exeErrorCode
    if exeErrorDiag != None:
        param['exeErrorDiag']=exeErrorDiag
    if cpuConversionFactor != None:
        param['cpuConversion']=cpuConversionFactor
    if pilotTiming != None:
        param['pilotTiming']=pilotTiming
    if computingElement != None:
        param['computingElement']=computingElement
    # invoke JD
    return jobDispatcher.updateJob(int(jobId),state,int(timeout),xml,siteName,param,metaData)


# get job status
def getStatus(req,ids,timeout=60):
    _logger.debug("getStatus(%s)" % ids)
    return jobDispatcher.getStatus(ids,int(timeout))


# check if there is activated analysis job
def isThereAnalysisJob(req,siteName):
    # invoke JD    
    return str(jobDispatcher.isThereAnalysisJob(siteName))
        
