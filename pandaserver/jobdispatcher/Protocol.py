import re
import json
try:
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlencode
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.dataservice import DataServiceUtils

# constants
TimeOutToken = "TimeOut"
NoJobsToken  = "NoJobs"       

########### status codes
# succeeded
SC_Success   =  0
# timeout
SC_TimeOut   = 10
# no available jobs
SC_NoJobs    = 20
# failed
SC_Failed    = 30
# Not secure connection
SC_NonSecure = 40
# invalid token
SC_Invalid   = 50
# invalid role
SC_Role      = 60
# permission denied
SC_Perms     = 70
# key missing
SC_MissKey   = 80
# failure of proxy retrieval
SC_ProxyError = 90


# response
class Response:
    # constructor
    def __init__(self,statusCode,errorDialog=None):
        # create data object
        self.data = {'StatusCode':statusCode}
        if errorDialog is not None:
            self.data['errorDialog'] = errorDialog


    # URL encode
    def encode(self,acceptJson=False):
        if not acceptJson:
            return urlencode(self.data)
        else:
            return {'type':'json','content':json.dumps(self.data)}


    # append Node
    def appendNode(self,name,value):
        self.data[name]=value
            
                   
    # append job
    def appendJob(self,job,siteMapperCache=None):
        # event service merge
        if EventServiceUtils.isEventServiceMerge(job):
            isEventServiceMerge = True
        else:
            isEventServiceMerge = False
        # PandaID
        self.data['PandaID'] = job.PandaID
        # prodSourceLabel
        self.data['prodSourceLabel'] = job.prodSourceLabel
        # swRelease
        self.data['swRelease'] = job.AtlasRelease
        # homepackage
        self.data['homepackage'] = job.homepackage
        # transformation
        self.data['transformation'] = job.transformation
        # job name
        self.data['jobName'] = job.jobName
        # job definition ID
        self.data['jobDefinitionID'] = job.jobDefinitionID
        # cloud
        self.data['cloud'] = job.cloud
        # files
        strIFiles = ''
        strOFiles = ''
        strDispatch = ''
        strDisToken = ''
        strDisTokenForOutput = ''                
        strDestination = ''
        strRealDataset = ''
        strRealDatasetIn = ''
        strProdDBlock = ''
        strDestToken = ''
        strProdToken = ''
        strProdTokenForOutput = ''
        strGUID = ''
        strFSize = ''
        strCheckSum = ''
        strFileDestinationSE = ''
        strScopeIn  = ''
        strScopeOut = ''
        strScopeLog = ''        
        logFile = ''
        logGUID = ''        
        ddmEndPointIn = []
        ddmEndPointOut = []
        noOutput = []
        siteSpec = None
        inDsLfnMap = {}
        inLFNset = set()
        if siteMapperCache is not None:
            siteMapper = siteMapperCache.getObj()
            siteSpec = siteMapper.getSite(job.computingSite)
            # resove destSE
            try:
                job.destinationSE = siteMapper.resolveNucleus(job.destinationSE)
                for tmpFile in job.Files:
                    tmpFile.destinationSE = siteMapper.resolveNucleus(tmpFile.destinationSE)
            except Exception:
                pass
            siteMapperCache.releaseObj()
        for file in job.Files:
            if file.type == 'input':
                if EventServiceUtils.isJumboJob(job) and file.lfn in inLFNset:
                    pass
                else:
                    inLFNset.add(file.lfn)
                    if strIFiles != '':
                        strIFiles += ','
                    strIFiles += file.lfn
                    if strDispatch != '':
                        strDispatch += ','
                    strDispatch += file.dispatchDBlock
                    if strDisToken != '':
                        strDisToken += ','
                    strDisToken += file.dispatchDBlockToken
                    strProdDBlock += '%s,' % file.prodDBlock 
                    if not isEventServiceMerge:
                        strProdToken += '%s,' % file.prodDBlockToken
                    else:
                        strProdToken += '%s,' % job.metadata[1][file.lfn]
                    if strGUID != '':
                        strGUID += ','
                    strGUID += file.GUID
                    strRealDatasetIn += '%s,' % file.dataset
                    strFSize += '%s,' % file.fsize
                    if not file.checksum in ['','NULL',None]:
                        strCheckSum += '%s,' % file.checksum
                    else:
                        strCheckSum += '%s,' % file.md5sum
                    strScopeIn += '%s,' % file.scope
                    ddmEndPointIn.append(self.getDdmEndpoint(siteSpec,file.dispatchDBlockToken, 'input'))
                    if not file.dataset in inDsLfnMap:
                        inDsLfnMap[file.dataset] = []
                    inDsLfnMap[file.dataset].append(file.lfn)
            if file.type == 'output' or file.type == 'log':
                if strOFiles != '':
                    strOFiles += ','
                strOFiles += file.lfn
                if strDestination != '':
                    strDestination += ','
                strDestination += file.destinationDBlock
                if strRealDataset != '':
                    strRealDataset += ','
                strRealDataset += file.dataset
                strFileDestinationSE += '%s,' % file.destinationSE
                if file.type == 'log':
                    logFile = file.lfn
                    logGUID = file.GUID
                    strScopeLog = file.scope
                else:
                    strScopeOut += '%s,' % file.scope                        
                if strDestToken != '':
                    strDestToken += ','
                strDestToken += re.sub('^ddd:','dst:',file.destinationDBlockToken.split(',')[0])
                strDisTokenForOutput += '%s,' % file.dispatchDBlockToken
                strProdTokenForOutput += '%s,' % file.prodDBlockToken
                ddmEndPointOut.append(self.getDdmEndpoint(siteSpec,file.destinationDBlockToken.split(',')[0], 'output'))
                if file.isAllowedNoOutput():
                    noOutput.append(file.lfn)
        # inFiles
        self.data['inFiles'] = strIFiles
        # dispatch DBlock
        self.data['dispatchDblock'] = strDispatch
        # dispatch DBlock space token
        self.data['dispatchDBlockToken'] = strDisToken
        # dispatch DBlock space token for output
        self.data['dispatchDBlockTokenForOut'] = strDisTokenForOutput[:-1]
        # outFiles
        self.data['outFiles'] = strOFiles
        # destination DBlock
        self.data['destinationDblock'] = strDestination
        # destination DBlock space token
        self.data['destinationDBlockToken'] = strDestToken
        # prod DBlocks
        self.data['prodDBlocks'] = strProdDBlock[:-1]
        # prod DBlock space token
        self.data['prodDBlockToken'] = strProdToken[:-1]
        # real output datasets
        self.data['realDatasets'] = strRealDataset
        # real output datasets
        self.data['realDatasetsIn'] = strRealDatasetIn[:-1]
        # file's destinationSE
        self.data['fileDestinationSE'] = strFileDestinationSE[:-1]
        # log filename
        self.data['logFile'] = logFile
        # log GUID
        self.data['logGUID'] = logGUID
        # jobPars
        self.data['jobPars'] = job.jobParameters
        # attempt number
        self.data['attemptNr'] = job.attemptNr
        # GUIDs
        self.data['GUID'] = strGUID
        # checksum
        self.data['checksum'] = strCheckSum[:-1]
        # fsize
        self.data['fsize'] = strFSize[:-1]
        # scope
        self.data['scopeIn']  = strScopeIn[:-1]
        self.data['scopeOut'] = strScopeOut[:-1]
        self.data['scopeLog'] = strScopeLog
        # DDM endpoints
        try:
            self.data['ddmEndPointIn'] = ','.join(ddmEndPointIn)
        except TypeError:
            self.data['ddmEndPointIn'] = ''
        try:
            self.data['ddmEndPointOut'] = ','.join(ddmEndPointOut)
        except TypeError:
            self.data['ddmEndPointOut'] = ''
        # destinationSE
        self.data['destinationSE'] = job.destinationSE
        # user ID
        self.data['prodUserID'] = job.prodUserID
        # CPU count
        self.data['maxCpuCount'] = job.maxCpuCount
        # RAM count
        self.data['minRamCount'] = job.minRamCount
        # disk count
        self.data['maxDiskCount'] = job.maxDiskCount
        # cmtconfig
        self.data['cmtConfig'] = job.cmtConfig
        # processingType
        self.data['processingType'] = job.processingType
        # transferType
        self.data['transferType'] = job.transferType
        # sourceSite
        self.data['sourceSite'] = job.sourceSite
        # current priority
        self.data['currentPriority'] = job.currentPriority
        # taskID
        if job.lockedby == 'jedi':
            self.data['taskID'] = job.jediTaskID
        else:
            self.data['taskID'] = job.taskID
        # core count
        if job.coreCount in ['NULL', None]:
            self.data['coreCount'] = 1
        else:
            self.data['coreCount'] = job.coreCount
        # jobsetID
        self.data['jobsetID'] = job.jobsetID
        # nucleus
        self.data['nucleus'] = job.nucleus
        # walltime
        self.data['maxWalltime'] = job.maxWalltime
        # debug mode
        if job.specialHandling is not None and 'debug' in job.specialHandling:
            self.data['debug'] = 'True'
        # event service or job cloning
        if EventServiceUtils.isJobCloningJob(job):
            self.data['cloneJob'] = EventServiceUtils.getJobCloningType(job)
        elif EventServiceUtils.isEventServiceJob(job) or EventServiceUtils.isJumboJob(job):
            self.data['eventService'] = 'True'
            # prod DBlock space token for pre-merging output
            self.data['prodDBlockTokenForOutput'] = strProdTokenForOutput[:-1]
        # event service merge
        if isEventServiceMerge:
            self.data['eventServiceMerge'] = 'True'
            # write to file for ES merge
            writeToFileStr = ''
            try:
                for outputName in job.metadata[0]:
                    inputList = job.metadata[0][outputName]
                    writeToFileStr += 'inputFor_{0}:'.format(outputName)
                    for tmpInput in inputList:
                        writeToFileStr += '{0},'.format(tmpInput)
                    writeToFileStr = writeToFileStr[:-1]
                    writeToFileStr += '^'
                writeToFileStr = writeToFileStr[:-1]
            except Exception:
                pass
            self.data['writeToFile'] = writeToFileStr
        elif job.writeInputToFile():
            try:
                # write input to file
                writeToFileStr = ''
                for inDS in inDsLfnMap:
                    inputList = inDsLfnMap[inDS]
                    inDS = re.sub('/$','',inDS)
                    inDS = inDS.split(':')[-1]
                    writeToFileStr += 'tmpin_{0}:'.format(inDS)
                    writeToFileStr += ','.join(inputList)
                    writeToFileStr += '^'
                writeToFileStr = writeToFileStr[:-1]
                self.data['writeToFile'] = writeToFileStr
            except Exception:
                pass
        # replace placeholder
        if EventServiceUtils.isJumboJob(job) or EventServiceUtils.isCoJumboJob(job):
            try:
                for inDS in inDsLfnMap:
                    inputList = inDsLfnMap[inDS]
                    inDS = re.sub('/$','',inDS)
                    inDS = inDS.split(':')[-1]
                    srcStr = 'tmpin__cnt_{0}'.format(inDS)
                    dstStr = ','.join(inputList)
                    self.data['jobPars'] = self.data['jobPars'].replace(srcStr, dstStr)
            except Exception:
                pass
        # no output
        if noOutput != []:
            self.data['allowNoOutput'] = ','.join(noOutput)
        # alternative stage-out
        if job.getAltStgOut() is not None:
            self.data['altStageOut'] = job.getAltStgOut()
        # log to OS
        if job.putLogToOS():
            self.data['putLogToOS'] = 'True'
        # suppress execute string conversion
        if job.noExecStrCnv():
            self.data['noExecStrCnv'] = 'True'
        # in-file positional event number
        if job.inFilePosEvtNum():
            self.data['inFilePosEvtNum'] = 'True'
        # use prefetcher
        if job.usePrefetcher():
            self.data['usePrefetcher'] = 'True'
        # IO
        self.data['ioIntensity'] = job.get_task_attribute('ioIntensity')
        self.data['ioIntensityUnit'] = job.get_task_attribute('ioIntensityUnit')


    # set proxy key
    def setProxyKey(self,proxyKey):
        names = ['credname','myproxy']
        for name in names:
            if name in proxyKey:
                self.data[name] = proxyKey[name]
            else:
                self.data[name] = ''


    # set secret key for panda proxy
    def setPandaProxySecretKey(self,secretKey):
        self.data['pandaProxySecretKey'] = secretKey


    # get ddm endpoint
    def getDdmEndpoint(self, siteSpec, spaceToken, mode):
        if siteSpec is None or mode not in ['input', 'output']:
            return ''

        if mode == 'input':
            connected_endpoints = siteSpec.ddm_endpoints_input
        elif mode == 'output':
            connected_endpoints = siteSpec.ddm_endpoints_output

        endPoint = DataServiceUtils.getDestinationSE(spaceToken)
        if endPoint is not None and connected_endpoints.isAssociated(endPoint):
            return endPoint

        endPoint = DataServiceUtils.getDistributedDestination(spaceToken)
        if endPoint is not None and connected_endpoints.isAssociated(endPoint):
            return endPoint

        if mode == 'input':
            setokens = siteSpec.setokens_input
            ddm = siteSpec.ddm_input
        elif mode == 'output':
            setokens = siteSpec.setokens_output
            ddm = siteSpec.ddm_output
        if spaceToken in setokens:
            return setokens[spaceToken]

        # Protection against misconfigured sites
        if not ddm:
            ddm = ''

        return ddm
                

# check if secure connection
def isSecure(req):
    if 'SSL_CLIENT_S_DN' not in req.subprocess_env:
        return False
    return True


# get user DN
def getUserDN(req):
    try:
        return req.subprocess_env['SSL_CLIENT_S_DN']
    except Exception:
        return 'None'
