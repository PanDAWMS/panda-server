import sys
import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

# extract pacball and site
argStr   = ""
pacball  = None
pacFlag  = False
siteName = None
siteFlag = False
for arg in sys.argv[1:]:
    if arg == '--pacball':
        pacFlag = True
        continue
    if pacFlag:
        pacball = arg
        pacFlag = False
        continue
    if arg == '--sitename':
        siteFlag = True
        continue
    if siteFlag:
        siteName = arg
        siteFlag = False
        continue
    argStr += "%s " % arg

# check site
if siteName == None:
    print "ERROR : --sitename needs to be specified"
    sys.exit(1)
# append sitename
argStr += "--sitename %s " % siteName

# check pacball format
if pacball != None and pacball.find(':') != -1:
    pacDS   = pacball.split(':')[0]    
    pacFile = pacball.split(':')[-1]
else:
    pacDS   = None
    pacFile = pacball

# append pacball to arg
if pacFile != None:
    argStr += "--pacball %s " % pacFile

job = JobSpec()
job.jobDefinitionID   = int(time.time()) % 10000
job.jobName           = "%s_%s" % (siteName,commands.getoutput('uuidgen'))
job.transformation    = 'http://www.usatlas.bnl.gov/svn/panda/apps/sw/installAtlasSW'
job.destinationDBlock = 'testpanda.%s' % job.jobName
job.currentPriority   = 10000
job.prodSourceLabel   = 'software'
job.computingSite     = siteName
job.cloud             = 'US'
    
fileOL = FileSpec()
fileOL.lfn = "%s.job.log.tgz" % job.jobName
fileOL.destinationDBlock = job.destinationDBlock
fileOL.dataset           = job.destinationDBlock
fileOL.type = 'log'
job.addFile(fileOL)

# pacball
if pacDS != None:
    job.prodDBlock   = pacDS
    fileP = FileSpec()
    fileP.dataset    = pacDS
    fileP.prodDBlock = pacDS
    fileP.lfn = pacFile
    fileP.type = 'input'
    job.addFile(fileP)
    
job.jobParameters = argStr

s,o = Client.submitJobs([job])
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
