import sys
import time
import random
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

site  = None
cloud = 'DE'
prodDBlock = 'mc09_10TeV.105807.JF35_pythia_jet_filter.evgen.EVNT.e469_tid095268'
inputFile = 'EVNT.095268._000110.pool.root.1'

if len(sys.argv)==5:
    site       = sys.argv[1]
    cloud      = sys.argv[2]
    prodDBlock = sys.argv[3]
    inputFile  = sys.argv[4]

datasetName = 'testpanda.destDB.%s' % commands.getoutput('uuidgen')

files = {
    inputFile:None,
    }

jobList = []

index = 0
for lfn in files.keys():
    index += 1
    job = JobSpec()
    job.jobDefinitionID   = (time.time()) % 10000
    job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),index)
    job.AtlasRelease      = 'Atlas-15.3.1'
    job.homepackage       = 'AtlasProduction/15.3.1.5'
    job.transformation    = 'csc_atlasG4_trf.py'
    job.destinationDBlock = datasetName
    job.computingSite     = site
    job.prodDBlock        = prodDBlock
    
    job.prodSourceLabel   = 'test'
    job.processingType    = 'test'
    job.currentPriority   = 10000
    job.cloud             = cloud
    job.cmtConfig         = 'i686-slc4-gcc34-opt'

    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = 'input'
    job.addFile(fileI)

    fileD = FileSpec()
    fileD.dataset    = 'ddo.000001.Atlas.Ideal.DBRelease.v070302'
    fileD.prodDBlock = fileD.dataset
    fileD.lfn = 'DBRelease-7.3.2.tar.gz'
    fileD.type = 'input'
    job.addFile(fileD)

    fileOA = FileSpec()
    fileOA.lfn = "%s.HITS.pool.root" % job.jobName
    fileOA.destinationDBlock = job.destinationDBlock
    fileOA.destinationSE     = job.destinationSE
    fileOA.dataset           = job.destinationDBlock
    fileOA.type = 'output'
    job.addFile(fileOA)

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)

    job.jobParameters="%s %s 5 1850 8738 ATLAS-GEO-08-00-01 QGSP_BERT VertexPos.py %s OFLCOND-SIM-01-00-00 False s595" % \
                       (fileI.lfn,fileOA.lfn,fileD.lfn)
    jobList.append(job)
    
s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
