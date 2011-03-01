import sys
import time
import random
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

if len(sys.argv)>1:
    site = sys.argv[1]
else:
    site = None

datasetName = 'panda.destDB.%s' % commands.getoutput('uuidgen')
destName    = 'BNL_ATLAS_2'

files = {
    'EVNT.019128._00011.pool.root.1':None,
    }

jobList = []

index = 0
for lfn in files.keys():
    index += 1
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),index)
    job.AtlasRelease      = 'Atlas-13.0.40'
    job.homepackage       = 'AtlasProduction/13.0.40.3'
    job.transformation    = 'csc_simul_trf.py'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.computingSite     = site
    job.prodDBlock        = 'valid1.005001.pythia_minbias.evgen.EVNT.e306_tid019128'
    
    job.prodSourceLabel   = 'test'    
    job.currentPriority   = 10000
    job.cloud             = 'IT'

    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = 'input'
    job.addFile(fileI)

    fileD = FileSpec()
    fileD.dataset    = 'ddo.000001.Atlas.Ideal.DBRelease.v040701'
    fileD.prodDBlock = 'ddo.000001.Atlas.Ideal.DBRelease.v030101'
    fileD.lfn = 'DBRelease-4.7.1.tar.gz'
    fileD.type = 'input'
    job.addFile(fileD)

    fileOE = FileSpec()
    fileOE.lfn = "%s.HITS.pool.root" % job.jobName
    fileOE.destinationDBlock = job.destinationDBlock
    fileOE.destinationSE     = job.destinationSE
    fileOE.dataset           = job.destinationDBlock
    fileOE.destinationDBlockToken = 'ATLASDATADISK'
    fileOE.type = 'output'
    job.addFile(fileOE)

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.destinationDBlockToken = 'ATLASDATADISK'
    fileOL.type = 'log'
    job.addFile(fileOL)

    job.jobParameters="%s %s NONE 1 3250 55866 ATLAS-CSC-02-01-00 55866 55866 QGSP_EMV None %s DEFAULT" % \
                       (fileI.lfn,fileOE.lfn,fileD.lfn)
    jobList.append(job)
    
s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
