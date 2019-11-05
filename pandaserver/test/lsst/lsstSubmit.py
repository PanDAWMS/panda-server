import sys
import time
import uuid

import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.FileSpec import FileSpec

aSrvID = None
prodUserNameDefault = 'unknown-user'
prodUserName = None
prodUserNameDP = None
prodUserNamePipeline = None
site = 'ANALY_BNL-LSST'
PIPELINE_TASK = None
PIPELINE_PROCESSINSTANCE = None
PIPELINE_EXECUTIONNUMBER = None
PIPELINE_STREAM = None
lsstJobParams = ""

for idx,argv in enumerate(sys.argv):
    if argv == '--site':
        try:
            site = sys.argv[idx + 1]
        except Exception:
            site = 'ANALY_BNL-LSST'
    if argv == '-DP_USER':
        try:
            prodUserNameDP = sys.argv[idx + 1]
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += "%(key)s=%(value)s" % \
                {'key': 'DP_USER', \
                 'value': str(prodUserNameDP)}
        except Exception:
            prodUserNameDP = None
    if argv == '-PIPELINE_USER':
        try:
            prodUserNamePipeline = sys.argv[idx + 1]
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += "%(key)s=%(value)s" % \
                {'key': 'PIPELINE_USER', \
                 'value': str(prodUserNamePipeline)}
        except Exception:
            prodUserNamePipeline = None
    if argv == '-PIPELINE_TASK':
        try:
            PIPELINE_TASK = sys.argv[idx + 1]
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += "%(key)s=%(value)s" % \
                {'key': 'PIPELINE_TASK', \
                 'value': str(PIPELINE_TASK)}
        except Exception:
            PIPELINE_TASK = None
    if argv == '-PIPELINE_PROCESSINSTANCE':
        try:
            PIPELINE_PROCESSINSTANCE = int(sys.argv[idx + 1])
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += "%(key)s=%(value)s" % \
                {'key': 'PIPELINE_PROCESSINSTANCE', \
                 'value': str(PIPELINE_PROCESSINSTANCE)}
        except Exception:
            PIPELINE_PROCESSINSTANCE = None
    if argv == '-PIPELINE_EXECUTIONNUMBER':
        try:
            PIPELINE_EXECUTIONNUMBER = int(sys.argv[idx + 1])
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += "%(key)s=%(value)s" % \
                {'key': 'PIPELINE_EXECUTIONNUMBER', \
                 'value': str(PIPELINE_EXECUTIONNUMBER)}
        except Exception:
            PIPELINE_EXECUTIONNUMBER = None
    if argv == '-PIPELINE_STREAM':
        try:
            PIPELINE_STREAM = int(sys.argv[idx + 1])
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += "%(key)s=%(value)s" % \
                {'key': 'PIPELINE_STREAM', \
                 'value': str(PIPELINE_STREAM)}
        except Exception:
            PIPELINE_STREAM = None
    if argv == '-s':
        aSrvID = sys.argv[idx+1]
        sys.argv = sys.argv[:idx]
        break


### DP_USER and PIPELINE_USER preference
if prodUserNameDP is not None:
    prodUserName = prodUserNameDP
elif prodUserNamePipeline is not None:
    prodUserName = prodUserNamePipeline


#site = sys.argv[1]
#site = 'ANALY_BNL-LSST'  #orig
#site = 'BNL-LSST'
#site = 'SWT2_CPB-LSST'
#site = 'UTA_SWT2-LSST'
#site = 'ANALY_SWT2_CPB-LSST'

destName    = None

if prodUserName is not None \
    and PIPELINE_TASK is not None \
    and PIPELINE_PROCESSINSTANCE is not None:
    datasetName = 'panda.lsst.user.%(PIPELINE_PROCESSINSTANCE)s.%(PIPELINE_TASK)s.%(prodUserName)s' % \
    {'prodUserName': str(prodUserName), \
     'PIPELINE_TASK': str(PIPELINE_TASK), \
     'PIPELINE_PROCESSINSTANCE': str(PIPELINE_PROCESSINSTANCE) \
     }
else:
    datasetName = 'panda.lsst.user.jschovan.%s' % str(uuid.uuid4())

if prodUserName is not None \
    and PIPELINE_TASK is not None \
    and PIPELINE_EXECUTIONNUMBER is not None \
    and PIPELINE_STREAM is not None:
    jobName = 'job.%(PIPELINE_PROCESSINSTANCE)s.%(PIPELINE_TASK)s.%(PIPELINE_EXECUTIONNUMBER)s.%(prodUserName)s.%(PIPELINE_STREAM)s' % \
    {'prodUserName': str(prodUserName), \
     'PIPELINE_TASK': str(PIPELINE_TASK), \
     'PIPELINE_EXECUTIONNUMBER': str(PIPELINE_EXECUTIONNUMBER), \
     'PIPELINE_STREAM': str(PIPELINE_STREAM), \
     'PIPELINE_PROCESSINSTANCE': str(PIPELINE_PROCESSINSTANCE) \
     }
else:
    jobName = "%s" % str(uuid.uuid4())

if PIPELINE_STREAM is not None:
    jobDefinitionID = PIPELINE_STREAM
else:
    jobDefinitionID = int(time.time()) % 10000
job = JobSpec()
job.jobDefinitionID = jobDefinitionID
job.jobName = jobName
job.transformation    = 'http://pandawms.org/pandawms-jobcache/lsst-trf.sh'
job.destinationDBlock = datasetName
job.destinationSE     = 'local' 
job.currentPriority   = 1000
job.prodSourceLabel = 'panda'
job.jobParameters = ' --lsstJobParams="%s" ' % lsstJobParams
if prodUserName is not None:
    job.prodUserName = prodUserName
else:
    job.prodUserName = prodUserNameDefault
if PIPELINE_PROCESSINSTANCE is not None:
    job.taskID = PIPELINE_PROCESSINSTANCE
if PIPELINE_EXECUTIONNUMBER is not None:
    job.attemptNr = PIPELINE_EXECUTIONNUMBER
if PIPELINE_TASK is not None:
    job.processingType = PIPELINE_TASK
job.computingSite = site
job.VO = "lsst"

fileOL = FileSpec()
fileOL.lfn = "%s.job.log.tgz" % job.jobName
fileOL.destinationDBlock = job.destinationDBlock
fileOL.destinationSE     = job.destinationSE
fileOL.dataset           = job.destinationDBlock
fileOL.type = 'log'
job.addFile(fileOL)


s,o = Client.submitJobs([job],srvID=aSrvID)
print(s)
for x in o:
    print("PandaID=%s" % x[0])
