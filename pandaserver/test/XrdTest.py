import sys
import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

if len(sys.argv)>1:
    site = sys.argv[1]
else:
    site = "ANALY_BNL_ATLAS_1"

datasetName = 'testpanda.destDB.%s' % commands.getoutput('uuidgen')
destName    = 'BNL_SE'

jobDefinitionID = int(time.time()) % 10000

jobList = []

for i in range(2):
    job = JobSpec()
    job.jobDefinitionID   = jobDefinitionID
    job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),i)
    job.AtlasRelease      = 'Atlas-12.0.6'
    job.homepackage       = 'AnalysisTransforms'
    job.transformation    = 'https://gridui01.usatlas.bnl.gov:24443/dav/test/runAthenaXrd'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = 3000
    job.assignedPriority  = 3000    
    job.prodSourceLabel   = 'user'
    job.computingSite     = site
    
    file = FileSpec()
    file.lfn = "%s.AANT._%05d.root" % (job.jobName,i)
    file.destinationDBlock = job.destinationDBlock
    file.destinationSE     = job.destinationSE
    file.dataset           = job.destinationDBlock
    file.type = 'output'
    job.addFile(file)
    
    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)

    fileL = FileSpec()
    fileL.dataset    = 'user.TadashiMaeno.acas0003.lib._000134'
    fileL.prodDBlock = fileL.dataset
    fileL.lfn        = 'user.TadashiMaeno.acas0003.lib._000134.lib.tgz'
    fileL.type       = 'input'
    fileL.status     = 'ready'
    job.addFile(fileL)
    
    job.jobParameters=("-l %s " % fileL.lfn) + """-r run/ -j "%20AnalysisSkeleton_topOptions.py" -i "[]" -m "[]" -n "[]" -o "{'AANT': [('AANTupleStream', 'AANT', """ + ("""'%s')]}" -c""" % file.lfn) 
    jobList.append(job)

s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
