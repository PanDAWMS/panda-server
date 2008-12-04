import sys
import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

# extract site
sites    = []
argStr   = ""
siteFlag = False
for arg in sys.argv[1:]:
    if arg == '--site':
        siteFlag = True
        continue
    if siteFlag:
        sites = arg.split(',') 
        siteFlag = False
        continue
    argStr += "%s " % arg
    
if sites == []:
    sites =  ['BNL_ATLAS_1', 'BU_ATLAS_Tier2', 'UC_ATLAS_MWT2', 'SLACXRD', 'IU_OSG',
              'MWT2_UC', 'UTD-HEP', 'LTU_CCT', 'MWT2_IU', 'UTA-DPCC', 'OU_OCHEP_SWT2',
              'AGLT2']

jobList = []

for site in sites:
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = "%s_%s" % (site,commands.getoutput('uuidgen'))
    job.transformation    = 'http://www.usatlas.bnl.gov/svn/panda/apps/sw/installAtlasSW'
    job.destinationDBlock = 'testpanda.%s' % job.jobName
    job.currentPriority   = 10000
    job.prodSourceLabel   = 'software'
    job.computingSite     = site
    
    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)
    
    job.jobParameters = argStr

    jobList.append(job)

if jobList != []:    
    s,o = Client.submitJobs(jobList)
    print "---------------------"
    print s
    for x in o:
        print "PandaID=%s" % x[0]
