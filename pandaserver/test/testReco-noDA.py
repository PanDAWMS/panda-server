# This script is used for testing RUCIO transfers and running short reconstruction job

import commands
import sys
import time

import userinterface.Client as Client

from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

if len(sys.argv)>2:
    site = sys.argv[1]
    cloud = sys.argv[2]
else:
    site = "UTA_PAUL_TEST"
    cloud = "US"

job = JobSpec()
job.jobDefinitionID   = int(time.time()) % 10000
job.jobName           = "%s_1" % commands.getoutput('uuidgen')
job.AtlasRelease      = 'Atlas-20.1.4'
job.homepackage       = 'AtlasProduction/20.1.4.14'
#job.AtlasRelease      = 'Atlas-20.20.8'
#job.homepackage       = 'AtlasProduction/20.20.8.4'
job.transformation    = 'Reco_tf.py'
job.destinationDBlock = 'panda.destDB.%s' % commands.getoutput('uuidgen')
job.destinationSE     = 'AGLT2_TEST'
job.prodDBlock        = 'user.mlassnig:user.mlassnig.pilot.test.single.hits'
job.currentPriority   = 1000
#job.prodSourceLabel   = 'ptest'
job.prodSourceLabel   = 'user'
job.computingSite     = site
job.cloud             = cloud
job.cmtConfig         = 'x86_64-slc6-gcc48-opt'
job.specialHandling   = 'ddm:rucio'
#job.transferType      = 'direct'

ifile = 'HITS.06828093._000096.pool.root.1'
fileI = FileSpec()
fileI.GUID = 'AC5B3759-B606-BA42-8681-4BD86455AE02'
fileI.checksum = 'ad:5d000974'
fileI.dataset = 'user.mlassnig:user.mlassnig.pilot.test.single.hits'
fileI.fsize = 94834717
fileI.lfn = ifile
fileI.prodDBlock = job.prodDBlock
fileI.scope = 'mc15_13TeV'
fileI.type = 'input'
job.addFile(fileI)

ofile = 'RDO_%s.root' % commands.getoutput('uuidgen')
fileO = FileSpec()
fileO.dataset = job.destinationDBlock
fileO.destinationDBlock = job.destinationDBlock
fileO.destinationSE = job.destinationSE
fileO.lfn = ofile
fileO.type = 'output'
job.addFile(fileO)

fileOL = FileSpec()
fileOL.lfn = "%s.job.log.tgz" % job.jobName
fileOL.destinationDBlock = job.destinationDBlock
fileOL.destinationSE = job.destinationSE
fileOL.dataset = job.destinationDBlock
fileOL.type = 'log'
job.addFile(fileOL)

job.jobParameters = '--maxEvents=3 --inputHITSFile %s --outputRDOFile %s' % (ifile, ofile)

print 'calling submitJobs'
s,o = Client.submitJobs([job])
print 'http://bigpanda.cern.ch/job?pandaid=%s' % o[0][0]
