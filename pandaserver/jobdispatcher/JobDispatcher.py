"""
dispatch jobs

"""

import re
import types
import threading
import Protocol
import time
import datetime
import commands
from threading import Lock
from config import panda_config
from dataservice.Adder import Adder
from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('JobDispatcher')
_pilotReqLogger = PandaLogger().getLogger('PilotRequests')


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
        thr.join() #self.timeout)



# cached object
class CachedObject:
    # constructor
    def __init__(self,timeInterval,updateFunc):
        # cached object
        self.cachedObj = None
        # datetime of last updated
        self.lastUpdated = datetime.datetime.utcnow()
        # how frequently update DN/token map
        self.timeInterval = datetime.timedelta(seconds=timeInterval)
        # lock
        self.lock = Lock()
        # function to update object
        self.updateFunc = updateFunc

    # update obj
    def update(self):
        # get current datetime
        current = datetime.datetime.utcnow()
        # lock
        self.lock.acquire()
        # update if old
        if self.cachedObj == None or current-self.lastUpdated > self.timeInterval:
            self.cachedObj = self.updateFunc()
            self.lastUpdated = current
        # release
        self.lock.release()
        # return
        return 

    # contains
    def __contains__(self,item):
        try:
            self.update()
            return item in self.cachedObj
        except:
            return False

    # get item
    def __getitem__(self,name):
        return self.cachedObj[name]



# job dipatcher
class JobDipatcher:
    # constructor
    def __init__(self):
        # taskbuffer
        self.taskBuffer = None
        # DN/token map
        self.tokenDN = None
        # datetime of last updated
        self.lastUpdated = datetime.datetime.utcnow()
        # how frequently update DN/token map
        self.timeInterval = datetime.timedelta(seconds=180)
        # pilot owners
        self.pilotOwners = None
        # hostnames for authorization at grid-free sites
        self.allowedNodes = None
        # sites with glexec
        self.glexecSites = None
        # lock
        self.lock = Lock()

    
    # set task buffer
    def init(self,taskBuffer):
        # lock
        self.lock.acquire()
        # set TB
        if self.taskBuffer == None:
            self.taskBuffer = taskBuffer
        # update DN/token map
        if self.tokenDN == None:
            self.tokenDN = self.taskBuffer.getListSchedUsers()
        # get pilot owners
        if self.pilotOwners == None:
            self.pilotOwners = self.taskBuffer.getPilotOwners()
        # get allowed nodes
        if self.allowedNodes == None:
            self.allowedNodes = self.taskBuffer.getAllowedNodes()
        # sites with glexec
        if self.glexecSites == None:
            self.glexecSites = CachedObject(60*10,self.taskBuffer.getGlexecSites)
        # release
        self.lock.release()
        

    # get job
    def getJob(self,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
               atlasRelease,prodUserID,getProxyKey,countryGroup,workingGroup,allowOtherCountry):
        jobs = []
        useGLEXEC = False
        # wrapper function for timeout
        tmpWrapper = _TimedMethod(self.taskBuffer.getJobs,timeout)
        tmpWrapper.run(1,siteName,prodSourceLabel,cpu,mem,diskSpace,node,timeout,computingElement,
                       atlasRelease,prodUserID,getProxyKey,countryGroup,workingGroup,allowOtherCountry)
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
            # check if glexec is used
            if hasattr(panda_config,'useProxyCache') and panda_config.useProxyCache == True:
                if siteName in self.glexecSites:
                    if self.glexecSites[siteName] == 'True':
                        useGLEXEC = True
                    elif self.glexecSites[siteName] == 'test' and \
                            (prodSourceLabel in ['test','prod_test'] or \
                                 (jobs[0].processingType in ['gangarobot'])):
                        useGLEXEC = True
            # set proxy
            if useGLEXEC:
                tmpStat,tmpOut = response.setUserProxy()
                if not tmpStat:
                    _logger.warning("getJob : %s %s failed to get user proxy : %s" % (siteName,node,
                                                                                      tmpOut))
        else:
            if tmpWrapper.result == Protocol.TimeOutToken:
                # timeout
                response=Protocol.Response(Protocol.SC_TimeOut)
            else:
                # no available jobs
                response=Protocol.Response(Protocol.SC_NoJobs)
        # return
        _logger.debug("getJob : %s %s useGLEXEC=%s ret -> %s" % (siteName,node,useGLEXEC,response.encode()))
        return response.encode()


    # update job status
    def updateJob(self,jobID,jobStatus,timeout,xml,siteName,param,metadata,attemptNr=None,stdout=''):
        # retry failed analysis job and ddm job
        if jobStatus=='failed' \
           and ((param.has_key('pilotErrorCode') and (param['pilotErrorCode'] in ['1200','1201'] \
                                                      or param['pilotErrorCode'].startswith('-'))) \
                 or (siteName != None and siteName.find('DDM') != -1)):
            # retry
            if param.has_key('pilotErrorCode') and param['pilotErrorCode'].startswith('-'):
                # pilot retry with new PandaID
                ret = self.taskBuffer.retryJob(jobID,param,getNewPandaID=True,attemptNr=attemptNr)
            else:
                # old style
                ret = self.taskBuffer.retryJob(jobID,param,attemptNr=attemptNr)                
            if ret:
                # return succeed
                response=Protocol.Response(Protocol.SC_Success)
                return response.encode()
        # add metadata
        if metadata != '':
            self.taskBuffer.addMetadata([jobID],[metadata])
        # add stdout
        if stdout != '':
            self.taskBuffer.addStdOut(jobID,stdout)
        # update
        tmpStatus = jobStatus
        updateStateChange = False
        if jobStatus == 'failed' or jobStatus == 'finished':
            tmpStatus = 'holding'
            # update stateChangeTime to prevent Watcher from finding this job
            updateStateChange = True
        if tmpStatus == 'holding':
            tmpWrapper = _TimedMethod(self.taskBuffer.updateJobStatus,None)
        else:
            tmpWrapper = _TimedMethod(self.taskBuffer.updateJobStatus,timeout)            
        tmpWrapper.run(jobID,tmpStatus,param,updateStateChange,attemptNr)
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
                if tmpWrapper.result != "badattemptnr" and (jobStatus == 'failed' or jobStatus == 'finished'):
                    Adder(self.taskBuffer,jobID,xml,jobStatus,attemptNr=attemptNr).start()
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


    # get a list of even ranges for a PandaID
    def getEventRanges(self,pandaID,nRanges,timeout):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.getEventRanges,timeout)
        tmpWrapper.run(pandaID,nRanges)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if isinstance(tmpWrapper.result,types.StringType):
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
                # make return
                response.appendNode('eventRanges',tmpWrapper.result)
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("getEventRanges : %s ret -> %s" % (pandaID,response.encode()))
        return response.encode()


    # update an event range
    def updateEventRange(self,eventRangeID,eventStatus,timeout):
        # peek jobs
        tmpWrapper = _TimedMethod(self.taskBuffer.updateEventRange,timeout)
        tmpWrapper.run(eventRangeID,eventStatus)
        # make response
        if tmpWrapper.result == Protocol.TimeOutToken:
            # timeout
            response=Protocol.Response(Protocol.SC_TimeOut)
        else:
            if tmpWrapper.result:
                # succeed
                response=Protocol.Response(Protocol.SC_Success)
            else:
                # failed
                response=Protocol.Response(Protocol.SC_Failed)
        _logger.debug("updateEventRange : %s ret -> %s" % (eventRangeID,response.encode()))
        return response.encode()


    # get DN/token map
    def getDnTokenMap(self):
        # get current datetime
        current = datetime.datetime.utcnow()
        # lock
        self.lock.acquire()
        # update DN map if old 
        if current-self.lastUpdated > self.timeInterval:
            # get new map
            self.tokenDN = self.taskBuffer.getListSchedUsers()
            # reset
            self.lastUpdated = current
        # release
        self.lock.release()
        # return
        return self.tokenDN


    # generate pilot token
    def genPilotToken(self,schedulerhost,scheduleruser,schedulerid):
        retVal = self.taskBuffer.genPilotToken(schedulerhost,scheduleruser,schedulerid)
        # failed
        if retVal == None:
            return "ERROR : failed to generate token"
        return "SUCCEEDED : " + retVal
    
        
# Singleton
jobDispatcher = JobDipatcher()
del JobDipatcher


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


# check role
def _checkRole(fqans,dn,jdCore,withVomsPatch=True,site='',hostname=''):
    prodManager = False
    try:
        # VOMS attributes of production and pilot roles
        prodAttrs = ['/atlas/usatlas/Role=production',
                     '/atlas/usatlas/Role=pilot',                        
                     '/atlas/Role=production',
                     '/atlas/Role=pilot',
                     '/osg/Role=pilot',
                     '/cms/Role=pilot',
                     '/ams/Role=pilot',                     
                     '/Engage/LBNE/Role=pilot',
                     ]
        if withVomsPatch:
            # FIXEME once http://savannah.cern.ch/bugs/?47136 is solved
            prodAttrs += ['/atlas/']
            prodAttrs += ['/osg/','/cms/','/ams/']
            prodAttrs += ['/Engage/LBNE/']            
        for fqan in fqans:
            # check atlas/usatlas production role
            for rolePat in prodAttrs:
                if fqan.startswith(rolePat):
                    prodManager = True
                    break
            # escape
            if prodManager:
                break
        # service proxy for CERNVM
        if site in ['CERNVM']:
            serviceSubjects = ['/DC=ch/DC=cern/OU=computers/CN=pilot/copilot.cern.ch']
            for tmpSub in serviceSubjects:
                if dn.startswith(tmpSub):
                    prodManager = True
                    break
        # grid-free authorization
        if not prodManager:
            if hostname != '' and jdCore.allowedNodes.has_key(site):
                for tmpPat in jdCore.allowedNodes[site]:
                    if re.search(tmpPat,hostname) != None:
                        prodManager = True
                        break
        # check DN with pilotOwners
        if (not prodManager) and (not dn in [None]):
            for owner in jdCore.pilotOwners:
                # check
                if re.search(owner,dn) != None:
                    prodManager = True
                    break
    except:
        pass
    # return
    return prodManager


# get DN
def _getDN(req):
    realDN = None
    if req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        realDN = req.subprocess_env['SSL_CLIENT_S_DN']
        # remove redundant CN
        realDN = re.sub('/CN=limited proxy','',realDN)
        realDN = re.sub('/CN=proxy(/CN=proxy)+','/CN=proxy',realDN)
    # return
    return realDN


# check token
def _checkToken(token,jdCore):
    # not check None until all pilots use tokens
    if token == None:
        return True
    # get map
    tokenDN = jdCore.getDnTokenMap()
    # return
    return tokenDN.has_key(token)
    


"""
web service interface

"""

# get job
def getJob(req,siteName,token=None,timeout=60,cpu=None,mem=None,diskSpace=None,prodSourceLabel=None,node=None,
           computingElement=None,AtlasRelease=None,prodUserID=None,getProxyKey=None,countryGroup=None,
           workingGroup=None,allowOtherCountry=None):
    _logger.debug("getJob(%s)" % siteName)
    # get DN
    realDN = _getDN(req)
    # get FQANs
    fqans = _getFQAN(req)
    # check production role
    if getProxyKey == 'True':
        # don't use /atlas to prevent normal proxy getting credname
        prodManager = _checkRole(fqans,realDN,jobDispatcher,False,site=siteName)
    else:
        prodManager = _checkRole(fqans,realDN,jobDispatcher,site=siteName,
                                 hostname=req.get_remote_host())        
    # check token
    validToken = _checkToken(token,jobDispatcher)
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
    _logger.debug("getJob(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,DN:%s,role:%s,token:%s,val:%s,FQAN:%s)" \
                  % (siteName,cpu,mem,diskSpace,prodSourceLabel,node,
                     computingElement,AtlasRelease,prodUserID,getProxyKey,countryGroup,workingGroup,
                     allowOtherCountry,realDN,prodManager,token,validToken,str(fqans)))
    _pilotReqLogger.info('method=getJob,site=%s,node=%s,type=%s' % (siteName,node,prodSourceLabel))    
    # invalid role
    if (not prodManager) and (not prodSourceLabel in ['user']):
        _logger.warning("getJob(%s) : invalid role" % siteName)
        return Protocol.Response(Protocol.SC_Role).encode()        
    # invalid token
    if not validToken:
        _logger.warning("getJob(%s) : invalid token" % siteName)    
        return Protocol.Response(Protocol.SC_Invalid).encode()        
    # invoke JD
    return jobDispatcher.getJob(siteName,prodSourceLabel,cpu,mem,diskSpace,node,int(timeout),
                                computingElement,AtlasRelease,prodUserID,getProxyKey,countryGroup,
                                workingGroup,allowOtherCountry)
    

# update job status
def updateJob(req,jobId,state,token=None,transExitCode=None,pilotErrorCode=None,pilotErrorDiag=None,timestamp=None,timeout=60,
              xml='',node=None,workdir=None,cpuConsumptionTime=None,cpuConsumptionUnit=None,remainingSpace=None,
              schedulerID=None,pilotID=None,siteName=None,messageLevel=None,pilotLog='',metaData='',
              cpuConversionFactor=None,exeErrorCode=None,exeErrorDiag=None,pilotTiming=None,computingElement=None,
              startTime=None,endTime=None,nEvents=None,nInputFiles=None,batchID=None,attemptNr=None,jobMetrics=None,
              stdout='',jobSubStatus=None):
    _logger.debug("updateJob(%s)" % jobId)
    # get DN
    realDN = _getDN(req)
    # get FQANs
    fqans = _getFQAN(req)
    # check production role
    prodManager = _checkRole(fqans,realDN,jobDispatcher,site=siteName,hostname=req.get_remote_host())
    # check token
    validToken = _checkToken(token,jobDispatcher)
    _logger.debug("updateJob(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,attemptNr:%s,jobSubStatus:%s,DN:%s,role:%s,token:%s,val:%s,FQAN:%s\n==XML==\n%s\n==LOG==\n%s\n==Meta==\n%s\n==Metrics==\n%s\n==stdout==\n%s)" %
                  (jobId,state,transExitCode,pilotErrorCode,pilotErrorDiag,node,workdir,cpuConsumptionTime,
                   cpuConsumptionUnit,remainingSpace,schedulerID,pilotID,siteName,messageLevel,nEvents,nInputFiles,
                   cpuConversionFactor,exeErrorCode,exeErrorDiag,pilotTiming,computingElement,startTime,endTime,
                   batchID,attemptNr,jobSubStatus,realDN,prodManager,token,validToken,str(fqans),xml,pilotLog,metaData,jobMetrics,
                   stdout))
    _pilotReqLogger.info('method=updateJob,site=%s,node=%s,type=None' % (siteName,node))
    # invalid role
    if not prodManager:
        _logger.warning("updateJob(%s) : invalid role" % jobId)
        return Protocol.Response(Protocol.SC_Role).encode()        
    # invalid token
    if not validToken:
        _logger.warning("updateJob(%s) : invalid token" % jobId)
        return Protocol.Response(Protocol.SC_Invalid).encode()        
    # aborting message
    if jobId=='NULL':
        return Protocol.Response(Protocol.SC_Success).encode()
    # check status
    if not state in ['running','failed','finished','holding','starting','transferring']:
        _logger.warning("invalid state=%s for updateJob" % state)
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
        param['pilotErrorDiag']=pilotErrorDiag[:500]
    if jobMetrics != None:
        param['jobMetrics']=jobMetrics[:500]
    if schedulerID != None:
        param['schedulerID']=schedulerID
    if pilotID != None:
        param['pilotID']=pilotID[:200]
    if batchID != None:
        param['batchID']=batchID
    if exeErrorCode != None:
        param['exeErrorCode']=exeErrorCode
    if exeErrorDiag != None:
        param['exeErrorDiag']=exeErrorDiag[:500]
    if cpuConversionFactor != None:
        param['cpuConversion']=cpuConversionFactor
    if pilotTiming != None:
        param['pilotTiming']=pilotTiming
    if computingElement != None:
        param['computingElement']=computingElement
    if nEvents != None:
        param['nEvents']=nEvents
    if nInputFiles != None:
        param['nInputFiles']=nInputFiles
    if not jobSubStatus in [None,'']:
        param['jobSubStatus']=jobSubStatus
    if startTime != None:
        try:
            param['startTime']=datetime.datetime(*time.strptime(startTime,'%Y-%m-%d %H:%M:%S')[:6])
        except:
            pass
    if endTime != None:
        try:
            param['endTime']=datetime.datetime(*time.strptime(endTime,'%Y-%m-%d %H:%M:%S')[:6])
        except:
            pass
    if attemptNr != None:
        try:
            attemptNr = int(attemptNr)
        except:
            attemptNr = None
    if stdout != '':
        stdout = stdout[:2048]
    # invoke JD
    return jobDispatcher.updateJob(int(jobId),state,int(timeout),xml,siteName,
                                   param,metaData,attemptNr,stdout)


# get job status
def getStatus(req,ids,timeout=60):
    _logger.debug("getStatus(%s)" % ids)
    return jobDispatcher.getStatus(ids,int(timeout))


# get a list of even ranges for a PandaID
def getEventRanges(req,pandaID,nRanges=10,timeout=60):
    _logger.debug("getEventRanges(%s)" % pandaID)
    return jobDispatcher.getEventRanges(pandaID,nRanges,int(timeout))


# update an event range
def updateEventRange(req,eventRangeID,eventStatus,timeout=60):
    _logger.debug("updateEventRange(%s status=%s)" % (eventRangeID,eventStatus))
    return jobDispatcher.updateEventRange(eventRangeID,eventStatus,int(timeout))


# generate pilot token
def genPilotToken(req,schedulerid,host=None):
    # get DN
    realDN = _getDN(req)
    # get FQANs
    fqans = _getFQAN(req)
    # check production role
    prodManager = _checkRole(fqans,realDN,jobDispatcher,False)
    if not prodManager:
        return "ERROR : production or pilot role is required"
    if realDN == None:
        return "ERROR : failed to retrive DN"
    # hostname
    if host == None:
        host = req.get_remote_host()
    # return
    return jobDispatcher.genPilotToken(host,realDN,schedulerid)
        
