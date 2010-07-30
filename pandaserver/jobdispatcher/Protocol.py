import urllib


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


# response
class Response:
    # constructor
    def __init__(self,statusCode):
        # create data object
        self.data = {'StatusCode':statusCode}


    # URL encode
    def encode(self):
        return urllib.urlencode(self.data)


    # append Node
    def appendNode(self,name,value):
        self.data[name]=value
            
                   
    # append job
    def appendJob(self,job):
        # PandaID
        self.data['PandaID'] = job.PandaID
        # swRelease
        self.data['swRelease'] = job.AtlasRelease
        # homepackage
        self.data['homepackage'] = job.homepackage
        # transformation
        self.data['transformation'] = job.transformation
        # job name
        self.data['jobName'] = job.jobName
        # files
        strIFiles = ''
        strOFiles = ''
        strDispatch = ''
        strDisToken = ''
        strDisTokenForOutput = ''                
        strDestination = ''
        strRealDataset = ''
        strRealDatasetIn = ''        
        strDestToken = ''
        strProdToken = ''        
        strGUID = ''
        logFile = ''
        logGUID = ''        
        for file in job.Files:
            if file.type == 'input':
                if strIFiles != '':
                    strIFiles += ','
                strIFiles += file.lfn
                if strDispatch != '':
                    strDispatch += ','
                strDispatch += file.dispatchDBlock
                if strDisToken != '':
                    strDisToken += ','
                strDisToken += file.dispatchDBlockToken
                if strProdToken != '':
                    strProdToken += ','
                strProdToken += file.prodDBlockToken
                if strGUID != '':
                    strGUID += ','
                strGUID += file.GUID
                strRealDatasetIn += '%s,' % file.dataset                
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
                if file.type == 'log':
                    logFile = file.lfn
                    logGUID = file.GUID
                if strDestToken != '':
                    strDestToken += ','
                strDestToken += file.destinationDBlockToken.split(',')[0]
                strDisTokenForOutput += '%s,' % file.dispatchDBlockToken
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
        # prod DBlock space token
        self.data['prodDBlockToken'] = strProdToken
        # real output datasets
        self.data['realDatasets'] = strRealDataset
        # real output datasets
        self.data['realDatasetsIn'] = strRealDatasetIn[:-1]
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
            

    # set proxy key
    def setProxyKey(self,proxyKey):
        names = ['credname','myproxy']
        for name in names:
            if proxyKey.has_key(name):
                self.data[name] = proxyKey[name]
            else:
                self.data[name] = ''
                

# check if secure connection
def isSecure(req):
    if not req.subprocess_env.has_key('SSL_CLIENT_S_DN'):
        return False
    return True


# get user DN
def getUserDN(req):
    try:
        return req.subprocess_env['SSL_CLIENT_S_DN']
    except:
        return 'None'

                
            
