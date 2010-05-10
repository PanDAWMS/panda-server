import re
import sys
import time
import random
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

cloud = sys.argv[1]
if len(sys.argv)>2:
    site = sys.argv[2]
else:
    site = None

datasetName = 'panda.destDB.%s' % commands.getoutput('uuidgen')
destName    = None

files = {
    'daq.ATLAS.0092045.physics.RPCwBeam.LB0016.SFO-2._0009.data':None,
    }

jobList = []

index = 0
for lfn in files.keys():
    index += 1
    job = JobSpec()
    job.jobDefinitionID   = int(time.time()) % 10000
    job.jobName           = "%s_%d" % (commands.getoutput('uuidgen'),index)
    job.AtlasRelease      = 'Atlas-14.4.0'
    job.homepackage       = 'AtlasTier0/14.4.0.2'
    job.transformation    = 'Reco_trf.py'
    job.destinationDBlock = datasetName
    job.destinationSE     = destName
    job.computingSite     = site
    job.prodDBlock        = 'data08_cos.00092045.physics_RPCwBeam.daq.RAW.o4_T1224560091' 
    
    job.prodSourceLabel   = 'test'
    job.processingType    = 'reprocessing'        
    job.currentPriority   = 10000
    job.cloud = cloud
    job.cmtConfig         = 'i686-slc4-gcc34-opt'

    origParams = """inputBSFile=daq.ATLAS.0092045.physics.RPCwBeam.LB0016.SFO-2._0009.data maxEvents=5 skipEvents=0 autoConfiguration=FieldAndGeo preInclude=RecExCommission/RecExCommission.py,RecExCommission/MinimalCommissioningSetup.py,RecJobTransforms/UseOracle.py preExec="jetFlags.Enabled.set_Value_and_Lock(False)" DBRelease=DBRelease-6.2.1.5.tar.gz conditionsTag=COMCOND-ES1C-000-00 RunNumber=92045 beamType=cosmics AMITag=r595 projectName=data08_cos trigStream=physics_RPCwBeam outputTypes=DPDCOMM outputESDFile=ESD.029868._01110.pool.root outputTAGComm=TAG_COMM.029868._01110.pool.root outputAODFile=AOD.029868._01110.pool.root outputMergedDQMonitorFile=DQM_MERGED.029868._01110.root DPD_PIXELCOMM=DPD_PIXELCOMM.029868._01110.pool.root DPD_SCTCOMM=DPD_SCTCOMM.029868._01110.pool.root DPD_IDCOMM=DPD_IDCOMM.029868._01110.pool.root DPD_IDPROJCOMM=DPD_IDPROJCOMM.029868._01110.pool.root DPD_CALOCOMM=DPD_CALOCOMM.029868._01110.pool.root DPD_TILECOMM=DPD_TILECOMM.029868._01110.pool.root DPD_EMCLUSTCOMM=DPD_EMCLUSTCOMM.029868._01110.pool.root DPD_EGAMMACOMM=DPD_EGAMMACOMM.029868._01110.pool.root DPD_RPCCOMM=DPD_RPCCOMM.029868._01110.pool.root DPD_TGCCOMM=DPD_TGCCOMM.029868._01110.pool.root --ignoreunknown"""

    match = re.findall("([^\s]+=[^\s]+)",origParams)
    outMap = {}
    for item in match:
        arg = item.split('=')[0]
        var = item.split('=')[-1]
        # output
        if arg.startswith('output') or arg.startswith('DPD_'):
            # skip some keys
            if arg in ['outputTypes']:
                continue
            prefix = var.split('.')[0]
            sumatch = re.search('(\.[^\.]+\.[^\.]+)(\.\d+)*$',var)
            suffix = sumatch.group(1)
            newName = '%s.%s%s' % (job.jobName,prefix,suffix)
            outMap[arg] = (var,newName)
        # DBRelease
        elif arg == 'DBRelease':
            dbrMap = (arg,var)
        # input
        elif arg.startswith('input') and arg.endswith('File'):
            inputMap = (arg,var)


    fileI = FileSpec()
    fileI.dataset    = job.prodDBlock
    fileI.prodDBlock = job.prodDBlock
    fileI.lfn = lfn
    fileI.type = 'input'
    job.addFile(fileI)

    fileD = FileSpec()
    fileD.dataset    = 'ddo.000001.Atlas.Ideal.DBRelease.v06020105'
    fileD.prodDBlock = fileD.dataset
    fileD.lfn = 'DBRelease-6.2.1.5.tar.gz'
    fileD.type = 'input'
    job.addFile(fileD)

    newParams = origParams
    newParams = newParams.replace(dbrMap[0]+'='+dbrMap[1],dbrMap[0]+'='+fileD.lfn)
    newParams = newParams.replace(inputMap[0]+'='+inputMap[1],inputMap[0]+'='+fileI.lfn)    

    for arg,vars in outMap.iteritems():
        fileO = FileSpec()
        fileO.lfn = vars[1]
        fileO.destinationDBlock = job.destinationDBlock
        fileO.destinationSE     = job.destinationSE
        fileO.dataset           = job.destinationDBlock
        fileO.type = 'output'
        job.addFile(fileO)
        newParams = newParams.replace(arg+'='+vars[0],arg+'='+fileO.lfn)            

    fileOL = FileSpec()
    fileOL.lfn = "%s.job.log.tgz" % job.jobName
    fileOL.destinationDBlock = job.destinationDBlock
    fileOL.destinationSE     = job.destinationSE
    fileOL.dataset           = job.destinationDBlock
    fileOL.type = 'log'
    job.addFile(fileOL)
    
    job.jobParameters=newParams

    jobList.append(job)

s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
