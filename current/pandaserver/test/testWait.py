import sys
import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

if len(sys.argv)>1:
    site = sys.argv[1]
else:
    site = None

datasetName = 'testpanda.destDB.%s' % commands.getoutput('uuidgen')
destName    = 'BNL_SE'

jobListE = []
lfnListE = []

for i in range(2):
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = commands.getoutput('uuidgen') 
    job.AtlasRelease      = 'Atlas-11.0.3'
    job.homepackage       = 'JobTransforms-11-00-03-03'
    job.transformation    = 'share/csc.evgen.trf'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = 1000
    job.prodSourceLabel   = 'test'
    job.computingSite     = site
    
    file = FileSpec()
    file.lfn = "%s.evgen.pool.root" % commands.getoutput('uuidgen')
    lfnListE.append(file.lfn)
    file.lfn += ('.%d' % (i+1))
    file.destinationDBlock = job.destinationDBlock
    file.destinationSE     = job.destinationSE
    file.dataset           = job.destinationDBlock
    file.type = 'output'
    job.addFile(file)

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % commands.getoutput('uuidgen') 
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)
    
    job.jobParameters="5056 %s NONE 81000 9000 10 DC3.005056.PythiaPhotonJet2.py NONE" % file.lfn
    jobListE.append(job)
    
s,o = Client.submitJobs(jobListE)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]

time.sleep(20)

datasetNameS = 'testpanda.simu.%s' % commands.getoutput('uuidgen')

jobListS = []

for lfn in lfnListE:
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = commands.getoutput('uuidgen') 
    job.AtlasRelease      = 'Atlas-11.0.3'
    job.homepackage       = 'JobTransforms-11-00-03-04'
    job.transformation    = 'share/csc.simul.trf'
    job.destinationDBlock = datasetNameS
    job.destinationSE     = destName
    job.prodDBlock        = datasetName
    
    job.prodSourceLabel   = 'test'    
    job.currentPriority   = 1000

    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = 'input'
    job.addFile(fileI)

    fileOE = FileSpec()
    fileOE.lfn = "%s.HITS.pool.root" % commands.getoutput('uuidgen') 
    fileOE.destinationDBlock = job.destinationDBlock
    fileOE.destinationSE     = job.destinationSE
    fileOE.dataset           = job.destinationDBlock
    fileOE.type = 'output'
    job.addFile(fileOE)

    fileOA = FileSpec()
    fileOA.lfn = "%s.RDO.pool.root" % commands.getoutput('uuidgen') 
    fileOA.destinationDBlock = job.destinationDBlock
    fileOA.destinationSE     = job.destinationSE
    fileOA.dataset           = job.destinationDBlock
    fileOA.type = 'output'
    job.addFile(fileOA)

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % commands.getoutput('uuidgen') 
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)

    job.jobParameters="%s %s %s 100 4900 400" % (fileI.lfn,fileOE.lfn,fileOA.lfn)

    jobListS.append(job)
    
s,o = Client.submitJobs(jobListS)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]

