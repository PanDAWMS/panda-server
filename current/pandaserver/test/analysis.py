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

jobList = []
for i in range(2):
    datasetName = 'panda.destDB.%s' % commands.getoutput('uuidgen')
    destName    = 'ANALY_BNL_ATLAS_1'
    
    job = JobSpec()
    job.jobDefinitionID   = 1
    job.jobName           = commands.getoutput('uuidgen') 
    job.AtlasRelease      = 'Atlas-12.0.2'
    job.homepackage       = 'AnalysisTransforms'
    job.transformation    = 'https://gridui01.usatlas.bnl.gov:24443/dav/test/runAthena2'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.currentPriority   = 3000
    job.prodSourceLabel   = 'user'
    job.computingSite     = site
    job.prodDBlock        = 'testIdeal_06.005001.pythia_minbias.recon.AOD.v12000103'
    
    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % commands.getoutput('uuidgen') 
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)

    fileOZ = FileSpec()
    fileOZ.lfn = "AANT.%s.root" % commands.getoutput('uuidgen') 
    fileOZ.destinationDBlock = job.destinationDBlock
    fileOZ.destinationSE     = job.destinationSE
    fileOZ.dataset           = job.destinationDBlock
    fileOZ.type = 'output'
    job.addFile(fileOZ)

    files = [
        'testIdeal_06.005001.pythia_minbias.recon.AOD.v12000103._00001.pool.root.1',
        'testIdeal_06.005001.pythia_minbias.recon.AOD.v12000103._00002.pool.root.1',
        'testIdeal_06.005001.pythia_minbias.recon.AOD.v12000103._00003.pool.root.1',
        ]
    for lfn in files:
        fileI = FileSpec()
        fileI.dataset    = job.prodDBlock
        fileI.prodDBlock = job.prodDBlock
        fileI.lfn = lfn
        fileI.type = 'input'
        fileI.status     = 'ready'
        job.addFile(fileI)

    fileL = FileSpec()
    fileL.dataset    = 'user.TadashiMaeno.lib._000157'
    fileL.prodDBlock = 'user.TadashiMaeno.lib._000157'
    fileL.lfn = 'user.TadashiMaeno.lib._000157.lib.tgz'
    fileL.type = 'input'
    fileL.status     = 'ready'
    job.addFile(fileL)

    job.jobParameters=""" -l user.TadashiMaeno.lib._000157.lib.tgz -r run/ -j " AnalysisSkeleton_jobOptions.py" -i "['testIdeal_06.005001.pythia_minbias.recon.AOD.v12000103._00001.pool.root.1', 'testIdeal_06.005001.pythia_minbias.recon.AOD.v12000103._00002.pool.root.1', 'testIdeal_06.005001.pythia_minbias.recon.AOD.v12000103._00003.pool.root.1']" -o "{'AANT': [('AANTupleStream', 'AANT', '%s')]}" """ % fileOZ.lfn 

    jobList.append(job)
    
    
s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
