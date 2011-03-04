import sys
import time
import random
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

site  = sys.argv[1]
cloud = sys.argv[2]

prodDBlock = 'mc10_7TeV.105001.pythia_minbias.evgen.EVNT.e574_tid153937_00'
inputFile = 'EVNT.153937._000184.pool.root.1'

if len(sys.argv)==5:
    site       = sys.argv[1]
    cloud      = sys.argv[2]
    prodDBlock = sys.argv[3]
    inputFile  = sys.argv[4]

datasetName = 'panda.destDB.%s' % commands.getoutput('uuidgen')

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
    job.AtlasRelease      = 'Atlas-16.6.2'
    job.homepackage       = 'AtlasProduction/16.6.2.1'
    job.transformation    = 'AtlasG4_trf.py'
    job.destinationDBlock = datasetName
    job.computingSite     = site
    job.prodDBlock        = prodDBlock
    
    job.prodSourceLabel   = 'test'
    job.processingType    = 'test'
    job.currentPriority   = 10000
    job.cloud             = cloud
    job.cmtConfig         = 'i686-slc5-gcc43-opt'

    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = 'input'
    job.addFile(fileI)

    fileD = FileSpec()
    fileD.dataset    = 'ddo.000001.Atlas.Ideal.DBRelease.v140201'
    fileD.prodDBlock = fileD.dataset
    fileD.lfn = 'DBRelease-14.2.1.tar.gz'
    fileD.type = 'input'
    job.addFile(fileD)

    fileOA = FileSpec()
    fileOA.lfn = "%s.HITS.pool.root" % job.jobName
    fileOA.destinationDBlock = job.destinationDBlock
    fileOA.destinationSE     = job.destinationSE
    fileOA.dataset           = job.destinationDBlock
    fileOA.destinationDBlockToken = 'ATLASDATADISK'
    fileOA.type = 'output'
    job.addFile(fileOA)

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.destinationDBlockToken = 'ATLASDATADISK'    
    fileOL.type = 'log'
    job.addFile(fileOL)

    job.jobParameters='inputEvgenFile=%s outputHitsFile=%s maxEvents=3 skipEvents=1700 DBRelease=%s preInclude=SimuJobTransforms/VertexFromCondDB.py postExec="from InDetBeamSpotService.InDetBeamSpotServiceConf import BeamCondSvc;ServiceMgr+=BeamCondSvc();ServiceMgr.BeamCondSvc.useDB=False;ServiceMgr.BeamCondSvc.posX=0.1352;ServiceMgr.BeamCondSvc.posY=1.1621;ServiceMgr.BeamCondSvc.posZ=2.87;ServiceMgr.BeamCondSvc.sigmaX=0;ServiceMgr.BeamCondSvc.sigmaY=0;ServiceMgr.BeamCondSvc.sigmaZ=0" geometryVersion=ATLAS-GEO-16-00-00 conditionsTag=OFLCOND-SDR-BS7T-02 AMITag=s1019 randomSeed=568 physicsList=QGSP_BERT firstEvent=1701 RunNumber=106047' % \
                       (fileI.lfn,fileOA.lfn,fileD.lfn)
    jobList.append(job)
    
s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
