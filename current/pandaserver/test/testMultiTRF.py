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

index = 0

job = JobSpec()
job.jobDefinitionID   = int(time.time()) % 10000
job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),index)
job.AtlasRelease      = 'Atlas-14.1.0\nAtlas-14.1.0'
job.homepackage       = 'AtlasProduction/14.1.0.3\nAtlasProduction/14.1.0.3'
job.transformation    = 'csc_digi_trf.py\ncsc_reco_trf.py'
job.destinationDBlock = datasetName

job.computingSite     = site

job.prodDBlock        = 'valid1.005200.T1_McAtNlo_Jimmy.simul.HITS.e322_s429_tid022081'
    
job.prodSourceLabel   = 'test'    
job.currentPriority   = 10000
job.cloud             = 'US'

for lfn in ['HITS.022081._00001.pool.root','HITS.022081._00002.pool.root']:
    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = 'input'
    job.addFile(fileI)

fileD1 = FileSpec()
fileD1.dataset    = 'ddo.000001.Atlas.Ideal.DBRelease.v050001'
fileD1.prodDBlock = fileD1.dataset
fileD1.lfn = 'DBRelease-5.0.1.tar.gz'
fileD1.type = 'input'
job.addFile(fileD1)

fileD2 = FileSpec()
fileD2.dataset    = 'ddo.000001.Atlas.Ideal.DBRelease.v050101'
fileD2.prodDBlock = fileD2.dataset
fileD2.lfn = 'DBRelease-5.1.1.tar.gz'
fileD2.type = 'input'
job.addFile(fileD2)

fileOE = FileSpec()
fileOE.lfn = "%s.ESD.pool.root" % job.jobName
fileOE.destinationDBlock = job.destinationDBlock
fileOE.destinationSE     = job.destinationSE
fileOE.dataset           = job.destinationDBlock
fileOE.type = 'output'
job.addFile(fileOE)

fileOA = FileSpec()
fileOA.lfn = "%s.AOD.pool.root" % job.jobName
fileOA.destinationDBlock = job.destinationDBlock
fileOA.destinationSE     = job.destinationSE
fileOA.dataset           = job.destinationDBlock
fileOA.type = 'output'
job.addFile(fileOA)

fileOC = FileSpec()
fileOC.lfn = "%s.NTUP.root" % job.jobName
fileOC.destinationDBlock = job.destinationDBlock
fileOC.destinationSE     = job.destinationSE
fileOC.dataset           = job.destinationDBlock
fileOC.type = 'output'
job.addFile(fileOC)
    
fileOL = FileSpec()
fileOL.lfn = "%s.job.log.tgz" % job.jobName
fileOL.destinationDBlock = job.destinationDBlock
fileOL.destinationSE     = job.destinationSE
fileOL.dataset           = job.destinationDBlock
fileOL.type = 'log'
job.addFile(fileOL)

job.jobParameters="HITS.022081._[00001,00002].pool.root RDO.TMP._00001_tmp.pool.root 250 0 ATLAS-CSC-05-00-00 1 1 NONE NONE None %s AtRndmGenSvc QGSP_EMV DEFAULT NONE NONE NONE NONE NONE\n RDO.TMP._00001_tmp.pool.root %s %s %s 250 0 ATLAS-CSC-05-00-00 DEFAULT None %s NONE" % \
                   (fileD1.lfn,fileOE.lfn,fileOA.lfn,fileOC.lfn,fileD2.lfn)

s,o = Client.submitJobs([job])
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
