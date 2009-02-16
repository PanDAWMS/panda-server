#
# eg. python cl_testG4sim.py SACLAY FR
#

import sys
import time
import random
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

if len(sys.argv)==2:
    site = sys.argv[1]
    cloud='CA'
elif  len(sys.argv)==3:
    site = sys.argv[1]
    cloud=sys.argv[2]
else:
    site = None
    cloud = None

datasetName = 'testpanda.rod2.%s_tid999990' % commands.getoutput('uuidgen')
#destName    = 'BNL_SE'

files={'daq.m5_combined.0028997.Default.L1TT-b00000110.LB0000.SFO-1._0001.data':'M5.0028997.Default.L1TT-b00000110.RAW.v010803',}

if cloud=='IT':
  files={'daq.m5_combined.0029118.Default.L1TT-b00000010.LB0000.SFO-1._0001.data':'M5.0029118.Default.L1TT-b00000010.RAW.v010803'}


jobList = []

for i in range(1):
  for lfn in files.keys():
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = commands.getoutput('uuidgen') 
    job.AtlasRelease      = 'Atlas-13.0.35'
    job.homepackage       = 'AtlasPoint1/13.0.35.1'
    job.transformation    = 'csc_cosmics_trf.py'
    job.destinationDBlock = datasetName
    job.cloud = cloud
    job.computingSite     = site
    job.prodDBlock        = files[lfn]
    job.prodSourceLabel   = 'test'
    job.currentPriority   = 1001

    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = 'input'
    job.addFile(fileI)

    fileD = FileSpec()
    fileD.dataset    = 'ddo.000001.Atlas.Ideal.DBRelease.v030101'
    fileD.prodDBlock = 'ddo.000001.Atlas.Ideal.DBRelease.v030101'
    fileD.lfn = 'DBRelease-3.1.1.tar.gz'
    fileD.type = 'input'
#    job.addFile(fileD)


    fileO1 = FileSpec()
    fileO1.lfn = "%s.ESD.pool.root" % job.jobName
    fileO1.destinationDBlock = job.destinationDBlock
    fileO1.destinationSE     = job.destinationSE
    fileO1.dataset           = job.destinationDBlock
    fileO1.type = 'output'
    job.addFile(fileO1)

    fileO2 = FileSpec()
    fileO2.lfn = "%s.ESDF.pool.root" %  job.jobName
    fileO2.destinationDBlock = job.destinationDBlock
    fileO2.destinationSE     = job.destinationSE
    fileO2.dataset           = job.destinationDBlock
    fileO2.type = 'output'
#    job.addFile(fileO2)

    fileO3 = FileSpec()
    fileO3.lfn = "%s.NTUP.pool.root" %  job.jobName
    fileO3.destinationDBlock = job.destinationDBlock
    fileO3.destinationSE     = job.destinationSE
    fileO3.dataset           = job.destinationDBlock
    fileO3.type = 'output'
    job.addFile(fileO3)

    fileO4 = FileSpec()
    fileO4.lfn = "%s.HIST.pool.root" %  job.jobName
    fileO4.destinationDBlock = job.destinationDBlock
    fileO4.destinationSE     = job.destinationSE
    fileO4.dataset           = job.destinationDBlock
    fileO4.type = 'output'
    job.addFile(fileO4)

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)

    job.jobParameters="%s LAR_TILE_MUONS_LVL1C 10 %s NONE %s %s COMCOND-002-00 NONE" % (fileI.lfn,fileO1.lfn,fileO3.lfn,fileO4.lfn)

    jobList.append(job)
    
s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
