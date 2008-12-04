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
destName    = 'BNL_ATLAS_2'

jobList = []
for i in range(20):
    
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = commands.getoutput('uuidgen') 
    job.AtlasRelease      = 'Atlas-11.0.41'
    #job.AtlasRelease      = 'Atlas-11.0.3'
    job.homepackage       = 'AnalysisTransforms'
    job.transformation    = 'https://gridui01.usatlas.bnl.gov:24443/dav/test/runAthena'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = 100
    job.prodSourceLabel   = 'user'
    job.computingSite     = site
    #job.prodDBlock        = "pandatest.b1599dfa-cd36-4fc5-92f6-495781a94c66"
    job.prodDBlock        = "pandatest.f228b051-077b-4f81-90bf-496340644379"
    
    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = "lib.f228b051-077b-4f81-90bf-496340644379.tgz"
    fileI.type = 'input'
    job.addFile(fileI)
    
    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % commands.getoutput('uuidgen') 
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)

    fileOZ = FileSpec()
    fileOZ.lfn = "%s.pool.root" % commands.getoutput('uuidgen') 
    fileOZ.destinationDBlock = job.destinationDBlock
    fileOZ.destinationSE     = job.destinationSE
    fileOZ.dataset           = job.destinationDBlock
    fileOZ.type = 'output'
    job.addFile(fileOZ)

    job.jobParameters="""-l %s -r PhysicsAnalysis/AnalysisCommon/UserAnalysis/UserAnalysis-00-05-11/run -j " jobOptions.pythia.py" -i "[]" -o "{'Stream1': '%s'}" """ % (fileI.lfn,fileOZ.lfn)

    jobList.append(job)
    
    
s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
