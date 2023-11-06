import sys
import time
import uuid

import pandaserver.userinterface.Client as Client
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec

aSrvID = None
prodUserNameDefault = "jschovan"
prodUserName = None
prodUserNameDP = None
prodUserNamePipeline = None
# site = 'ANALY_BNL-LSST-OSGRPMtest'
# site = 'ANALY_BNL-LSST-gridgk01-mergetest'
site = "ANALY_BNL-LSST-mergetest"
PIPELINE_TASK = None
PIPELINE_PROCESSINSTANCE = None
PIPELINE_EXECUTIONNUMBER = None
PIPELINE_STREAM = None
lsstJobParams = ""

for idx, argv in enumerate(sys.argv):
    if argv == "--site":
        try:
            site = sys.argv[idx + 1]
        except Exception:
            site = "ANALY_BNL-LSST-mergetest"
    if argv == "-DP_USER":
        try:
            prodUserNameDP = sys.argv[idx + 1]
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += f"DP_USER={str(prodUserNameDP)}"
        except Exception:
            prodUserNameDP = None
    if argv == "-PIPELINE_USER":
        try:
            prodUserNamePipeline = sys.argv[idx + 1]
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += f"PIPELINE_USER={str(prodUserNamePipeline)}"
        except Exception:
            prodUserNamePipeline = None
    if argv == "-PIPELINE_TASK":
        try:
            PIPELINE_TASK = sys.argv[idx + 1]
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += f"PIPELINE_TASK={str(PIPELINE_TASK)}"
        except Exception:
            PIPELINE_TASK = None
    if argv == "-PIPELINE_PROCESSINSTANCE":
        try:
            PIPELINE_PROCESSINSTANCE = int(sys.argv[idx + 1])
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += f"PIPELINE_PROCESSINSTANCE={str(PIPELINE_PROCESSINSTANCE)}"
        except Exception:
            PIPELINE_PROCESSINSTANCE = None
    if argv == "-PIPELINE_EXECUTIONNUMBER":
        try:
            PIPELINE_EXECUTIONNUMBER = int(sys.argv[idx + 1])
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += f"PIPELINE_EXECUTIONNUMBER={str(PIPELINE_EXECUTIONNUMBER)}"
        except Exception:
            PIPELINE_EXECUTIONNUMBER = None
    if argv == "-PIPELINE_STREAM":
        try:
            PIPELINE_STREAM = int(sys.argv[idx + 1])
            if len(lsstJobParams):
                lsstJobParams += "|"
            lsstJobParams += f"PIPELINE_STREAM={str(PIPELINE_STREAM)}"
        except Exception:
            PIPELINE_STREAM = None
    if argv == "-s":
        aSrvID = sys.argv[idx + 1]
        sys.argv = sys.argv[:idx]
        break


# DP_USER and PIPELINE_USER preference
if prodUserNameDP is not None:
    prodUserName = prodUserNameDP
elif prodUserNamePipeline is not None:
    prodUserName = prodUserNamePipeline


# site = sys.argv[1]
# site = 'ANALY_BNL-LSST'  #orig
# site = 'BNL-LSST'
# site = 'SWT2_CPB-LSST'
# site = 'UTA_SWT2-LSST'
# site = 'ANALY_SWT2_CPB-LSST'

destName = None

if prodUserName is not None and PIPELINE_TASK is not None and PIPELINE_PROCESSINSTANCE is not None:
    datasetName = f"panda.lsst.user.{str(PIPELINE_PROCESSINSTANCE)}.{str(PIPELINE_TASK)}.{str(prodUserName)}"
else:
    datasetName = f"panda.lsst.user.jschovan.{str(uuid.uuid4())}"

if prodUserName is not None and PIPELINE_TASK is not None and PIPELINE_EXECUTIONNUMBER is not None and PIPELINE_STREAM is not None:
    jobName = f"job.{str(PIPELINE_PROCESSINSTANCE)}.{str(PIPELINE_TASK)}.{str(PIPELINE_EXECUTIONNUMBER)}.{str(prodUserName)}.{str(PIPELINE_STREAM)}"
else:
    jobName = f"{str(uuid.uuid4())}"

if PIPELINE_STREAM is not None:
    jobDefinitionID = PIPELINE_STREAM
else:
    jobDefinitionID = int(time.time()) % 10000
job = JobSpec()
job.jobDefinitionID = jobDefinitionID
job.jobName = jobName
job.transformation = "http://rpm-test.pandawms.org/pandawms-jobcache/lsst-trf.sh"
job.destinationDBlock = datasetName
job.destinationSE = "local"
job.currentPriority = 1000
job.prodSourceLabel = "panda"
job.jobParameters = f' --lsstJobParams="{lsstJobParams}" '
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
fileOL.lfn = f"{job.jobName}.job.log.tgz"
fileOL.destinationDBlock = job.destinationDBlock
fileOL.destinationSE = job.destinationSE
fileOL.dataset = job.destinationDBlock
fileOL.type = "log"
job.addFile(fileOL)


s, o = Client.submitJobs([job], srvID=aSrvID)
print(s)
for x in o:
    print(f"PandaID={x[0]}")
