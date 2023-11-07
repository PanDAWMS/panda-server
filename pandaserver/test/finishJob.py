import hashlib
import json
import os
import re
import sys
import uuid
from http.client import HTTPSConnection
from urllib.parse import urlencode

from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.config import panda_config
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.userinterface.Client import baseURLSSL

# instantiate TB
requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1, requester=requester_id)
# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)

id = sys.argv[1]
job = taskBuffer.peekJobs([id])[0]

if job is None:
    print("got None")
    sys.exit(0)

xml = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- ATLAS file meta-data catalog -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>
"""
try:
    att = sys.argv[2]
except Exception:
    att = job.attemptNr

if job.computingSite in ["", None, "NULL"]:
    print("computingSite is not yet defined")
    sys.exit(0)

siteSpec = siteMapper.getSite(job.computingSite)
scope_input, scope_output = select_scope(siteSpec, job.prodSourceLabel, job.job_label)

with open("/cvmfs/atlas.cern.ch/repo/sw/local/etc/cric_ddmendpoints.json") as f:
    rseDict = json.load(f)

hash = hashlib.md5()

iOut = 0
outFileName = []
fileDict = {}
for tmpFile in job.Files:
    if tmpFile.type in ["output"]:
        if False:  # outFileName is None:
            outFileName.append(tmpFile.lfn)
    if tmpFile.type in ["output", "log"]:
        fileList = []
        if False:  # tmpFile.type == 'output':# and iOut > 0:
            for i in range(8):
                newFile = copy.copy(tmpFile)
                newFile.lfn += f"._00{i}"
                fileList.append(newFile)
            # continue
        else:
            fileList.append(tmpFile)
        iOut += 1
        for file in fileList:
            file.GUID = str(uuid.uuid4())
            if DataServiceUtils.getDistributedDestination(file.destinationDBlockToken) is not None:
                tmpSrcDDM = DataServiceUtils.getDistributedDestination(file.destinationDBlockToken)
            elif job.computingSite == file.destinationSE and file.destinationDBlockToken in siteSpec.setokens_output[scope_output]:
                tmpSrcDDM = siteSpec.setokens_output[scope_output][file.destinationDBlockToken]
            elif file.lfn in outFileName:
                tmpSrcDDM = DataServiceUtils.getDestinationSE(file.destinationDBlockToken)
                if tmpSrcDDM is None:
                    tmpSrcSite = siteMapper.getSite(file.destinationSE)
                    tmp_scope_input, tmp_scope_output = select_scope(siteSpec, job.prodSourceLabel, job.job_label)
                    tmpSrcDDM = tmpSrcSite.ddm_output[tmp_scope_output]
            else:
                tmpSrcSite = siteMapper.getSite(job.computingSite)
                tmp_scope_input, tmp_scope_output = select_scope(siteSpec, job.prodSourceLabel, job.job_label)
                tmpSrcDDM = tmpSrcSite.ddm_output[tmp_scope_output]
            srm, dummy, root = rseDict[tmpSrcDDM]["aprotocols"]["w"][0]
            srm = re.sub("^token:[^:]+:", "", srm)
            srm += root
            srm = re.sub("/$", "", srm)
            hash = hashlib.md5()
            sl = f"{file.scope}:{file.lfn}"
            hash.update(sl.encode())
            hash_hex = hash.hexdigest()
            correctedscope = "/".join(file.scope.split("."))
            path = f"{correctedscope}/{hash_hex[0:2]}/{hash_hex[2:4]}/{file.lfn}"

            strDDM = ""
            if tmpFile.type == "log":
                strDDM += f"<endpoint>{tmpSrcDDM}</endpoint>"
                strDDM += "<endpoint>CERN-PROD_LOGS</endpoint>"
            xml += f"""
      <File ID="{file.GUID}">
        <logical>
          <lfn name="{file.lfn}"/>
        </logical>
        {strDDM}
        <metadata att_name="surl" att_value="{srm}/{path}"/>
        <metadata att_name="fsize" att_value="1273400000"/>
        <metadata att_name="adler32" att_value="0d2a9dc9"/>
       </File>"""
            fileDict[file.lfn] = {
                "guid": file.GUID,
                "fsize": 1234,
                "adler32": "0d2a9dc9",
                "surl": f"{srm}/{path}",
            }

xml += """
</POOLFILECATALOG>
"""

xml = json.dumps(fileDict)

meta = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?> <!-- ATLAS file meta-data catalog --> <!DOCTYPE POOLFILECATALOG SYSTEM "InMemory"> <POOLFILECATALOG> <META type="string" name="size" value="82484969"/> <META type="string" name="conditionsTag" value="COMCOND-BLKPA-006-11"/> <META type="string" name="beamType" value="collisions"/> <META type="string" name="fileType" value="aod"/> <META type="string" name="autoConfiguration" value="['everything']"/> <META type="string" name="dataset" value=""/> <META type="string" name="maxEvents" value="200"/> <META type="string" name="AMITag" value="r5475"/> <META type="string" name="postInclude" value="['EventOverlayJobTransforms/Rt_override_BLKPA-006-11.py', 'EventOverlayJobTransforms/muAlign_reco.py']"/> <META type="string" name="preExec" value="['from CaloRec.CaloCellFlags import jobproperties;jobproperties.CaloCellFlags.doLArHVCorr.set_Value_and_Lock(False);muonRecFlags.writeSDOs=True', 'TriggerFlags.AODEDMSet=AODSLIM;rec.Commissioning.set_Value_and_Lock(True);jobproperties.Beam.numberOfCollisions.set_Value_and_Lock(20.0)']"/> <META type="string" name="triggerConfig" value="MCRECO:DBF:TRIGGERDBMC:325,142,266"/> <META type="string" name="preInclude" value="['EventOverlayJobTransforms/UseOracle.py', 'EventOverlayJobTransforms/custom.py', 'EventOverlayJobTransforms/recotrfpre.py']"/> <META type="string" name="geometryVersion" value="ATLAS-GEO-20-00-01"/> <META type="string" name="events"/> <META type="string" name="postExec" value="['from IOVDbSvc.CondDB import conddb"/> """

meta = """{ "argValues": { "AMITag": "p1815", "athenaopts": [ "--preloadlib=/cvmfs/atlas.cern.ch/repo/sw/software/x86_64-slc6-gcc47-opt/19.1.4/sw/IntelSoftware/linux/x86_64/xe2013/composer_xe_2013.3.163/compiler/lib/intel64/libintlc.so.5:/cvmfs/atlas.cern.ch/repo/sw/software/x86_64-slc6-gcc47-opt/19.1.4/sw/IntelSoftware/linux/x86_64/xe2013/composer_xe_2013.3.163/compiler/lib/intel64/libimf.so" ], "digiSeedOffset1": 8, "digiSeedOffset2": 8, "inputAODFile": [ "AOD.01598144._000008.pool.root.3" ], "jobNumber": 108, "reductionConf": [ "SUSY2" ], "runNumber": 167775 }, "cmdLine": "'/cvmfs/atlas.cern.ch/repo/sw/software/x86_64-slc6-gcc47-opt/19.1.4/AtlasDerivation/19.1.4.4/InstallArea/share/bin/Reco_tf.py' '--inputAODFile=AOD.01598144._000008.pool.root.3' '--AMITag' 'p1815' '--digiSeedOffset1' '8' '--digiSeedOffset2' '8' '--jobNumber' '108' '--outputDAODFile' '04553369._000108.pool.root.1.panda.um' '--reductionConf' 'SUSY2' '--runNumber' '167775'", "created": "2015-01-10T02:51:03", "executor": [ { "asetup": null, "errMsg": "", "exeConfig": { "inputs": [ "AOD" ], "outputs": [ "DAOD_SUSY2" ], "script": "athena.py", "substep": "a2da" }, "logfileReport": { "countSummary": { "CRITICAL": 0, "DEBUG": 1, "ERROR": 0, "FATAL": 0, "IGNORED": 0, "INFO": 6166, "UNKNOWN": 2114, "VERBOSE": 1, "WARNING": 95 }, "details": {} }, "metaData": {}, "name": "AODtoDAOD", "rc": 0, "resource": { "cpuTime": 2478, "wallTime": 2649 }, "statusOK": true, "validation": true } ], "exitAcronym": "OK", "exitCode": 0, "exitMsg": "OK", "exitMsgExtra": "", "files": { "input": [ { "dataset": null, "nentries": 5000, "subFiles": [ { "file_guid": "727E5251-EE36-1F44-8EFB-C8FBA7D9BE52", "name": "AOD.01598144._000008.pool.root.3" } ], "type": "AOD" } ], "output": [ { "argName": "SUSY2", "dataset": null, "subFiles": [ """

for file in job.Files:
    if file.type in ["output"]:
        #        meta += """{ "AODFixVersion": "", "beam_energy": [ 6500000.0 ], "beam_type": [ "collisions" ], "conditions_tag": "OFLCOND-RUN12-SDR-14", "file_guid": "%s", "file_size": 643794695, "file_type": "pool", "geometry": "ATLAS-R2-2015-01-01-00", "integrity": true, "lumi_block": [ 15 ], "name": "%s", "nentries": 2563, "run_number": [ 222222 ] } ,""" % (file.GUID,file.lfn)
        meta += (
            """{ "AODFixVersion": "", "beam_energy": [ 6500000.0 ], "beam_type": [ "collisions" ], "conditions_tag": "OFLCOND-RUN12-SDR-14", "file_guid": "%s", "file_size": 643794695, "file_type": "pool", "geometry": "ATLAS-R2-2015-01-01-00", "integrity": true, "lumi_block": [ 15 ], "name": "%s", "run_number": [ 222222 ] } ,"""
            % (file.GUID, file.lfn)
        )
meta = meta[:-1]
meta += """], "type": "aod" } ] }, "name": "Reco_tf", "reportVersion": "1.0.0", "resource": { "cpuUnit": "seconds", "externalsCpuTime": 24, "memUnit": "kB", "transformCpuTime": 50, "wallTime": 2782 } }"""

node = {}
node["jobId"] = id
node["state"] = "finished"
# node['state']='failed'
# node['batchID']='aaax'
node["pilotErrorCode"] = 2
node["pilotErrorCode"] = 1099
# node['pilotErrorDiag']='aaaaaaaaaaaaaaaaaaaaaaa'
# node['metaData']=meta
node["corruptedFiles"] = "4003029-1800232404-8696089-2-2.zip"
node["siteName"] = "BNL_ATLAS_test"
node["attemptNr"] = att
node["jobMetrics"] = f"aaaaa=2 bbbb=3 alt:{','.join(outFileName)}"
# node['jobSubStatus']='pilot_killed'
# node['coreCount']=10
node["cpuConsumptionTime"] = 12340
node["maxRSS"] = 1
node["maxVMEM"] = 2
node["maxSWAP"] = 3
node["maxPSS"] = 5 * 1024 * 1024
node["avgRSS"] = 11
node["avgVMEM"] = 12
node["avgSWAP"] = 13
node["avgPSS"] = 14
node["rateWCHAR"] = 1400

node["xml"] = xml
url = f"{baseURLSSL}/updateJob"

match = re.search("[^:/]+://([^/]+)(/.+)", url)
host = match.group(1)
path = match.group(2)

if "X509_USER_PROXY" in os.environ:
    certKey = os.environ["X509_USER_PROXY"]
else:
    certKey = f"/tmp/x509up_u{os.getuid()}"

rdata = urlencode(node)

conn = HTTPSConnection(host, key_file=certKey, cert_file=certKey)
conn.request("POST", path, rdata)
resp = conn.getresponse()
data = resp.read()

print(data)
