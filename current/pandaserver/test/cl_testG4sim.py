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

datasetName = 'panda.rod2.%s_tid999990' % commands.getoutput('uuidgen')
#destName    = 'BNL_SE'

if cloud=='UK':
  files = {
    'mc12.005802.JF17_pythia_jet_filter.evgen.EVNT.v12003105_tid004541._01035.pool.root.1':'mc12.005802.JF17_pythia_jet_filter.evgen.EVNT.v12003105_tid004541',
  }
# or mc12.005802.JF17_pythia_jet_filter.evgen.EVNT.v12003105_tid004541._01174.pool.root.1, mc12.005802.JF17_pythia_jet_filter.evgen.EVNT.v12003105_tid004541._01035.pool.root.1  
elif cloud=='CA':
  files={'EVNT.012303._00901.pool.root.1':'mc12.005001.pythia_minbias.evgen.EVNT.v12000701_tid012303',}
elif cloud=='FR':
  files={'EVNT.010822._00007.pool.root.1':'mc12.006873.PythiaWH140lnugamgam.evgen.EVNT.v12000701_tid010822',}           
elif cloud in ['ES']:
  files={'EVNT.016869._00187.pool.root.1':'mc12.005001.pythia_minbias.evgen.EVNT.v12000601_tid016869',}  
elif cloud in ['DE']:
  files={'EVNT.016869._00177.pool.root.2':'mc12.005001.pythia_minbias.evgen.EVNT.v12000601_tid016869',}
else:
  print 'Cloud not known: %s'%cloud
  cloud = None
  files={'EVNT.012303._00545.pool.root.1':'rod.cloudtest1'}

# UK
#'mc12.005802.JF17_pythia_jet_filter.evgen.EVNT.v12003105_tid004541._01035.pool.root.1':'mc12.005802.JF17_pythia_jet_filter.evgen.EVNT.v12003105_tid004541',
# CA
#    'EVNT.012303._00901.pool.root.1':'mc12.005001.pythia_minbias.evgen.EVNT.v12000701_tid012303',



jobList = []

for i in range(1):
  for lfn in files.keys():
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = commands.getoutput('uuidgen') 
    job.AtlasRelease      = 'Atlas-12.0.7'
    job.homepackage       = 'AtlasProduction/12.0.7.1'
# Need different args too
#    job.AtlasRelease      = 'Atlas-13.0.30'
#    job.homepackage       = 'AtlasProduction/13.0.30.2'
    job.transformation    = 'csc_simul_trf.py'
    job.destinationDBlock = datasetName
    job.cloud = cloud
    job.computingSite     = site
#    job.prodDBlock        = 'mc12.005001.pythia_minbias.evgen.EVNT.v12000701_tid012303'
    job.prodDBlock        = files[lfn]
    job.prodSourceLabel   = 'test'
#    job.prodSourceLabel   = 'cloudtest'
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
    job.addFile(fileD)


    fileOE = FileSpec()
    fileOE.lfn = "%s.HITS.pool.root" % job.jobName
    fileOE.destinationDBlock = job.destinationDBlock
    fileOE.destinationSE     = job.destinationSE
    fileOE.dataset           = job.destinationDBlock
    fileOE.type = 'output'
    job.addFile(fileOE)

    fileOA = FileSpec()
    fileOA.lfn = "%s.RDO.pool.root" %  job.jobName
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

    job.jobParameters="%s %s %s 1 4000 153781 ATLAS-CSC-01-02-00 NONE %s" % (fileI.lfn,fileOE.lfn,fileOA.lfn,fileD.lfn)

    jobList.append(job)
    
s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
