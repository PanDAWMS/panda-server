import os
import re
import sys
import urllib
from dq2.info import TiersOfATLAS

import userinterface.Client as Client
from userinterface.Client import baseURLSSL

from taskbuffer.TaskBuffer import taskBuffer
from dataservice.DataServiceUtils import select_scope
from brokerage.SiteMapper import SiteMapper
from config import panda_config

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
# instantiate sitemapper
siteMapper = SiteMapper(taskBuffer)


import httplib
import commands

id = sys.argv[1]
s,o = Client.getJobStatus([id])

if s != 0:
    print "failed to get job with:%s" % s
    sys.exit(0)

job = o[0]

if job == None:
    print "got None"
    sys.exit(0)

xml = """<?xml version="1.0" encoding="UTF-8" standalone="no" ?>
<!-- ATLAS file meta-data catalog -->
<!DOCTYPE POOLFILECATALOG SYSTEM "InMemory">
<POOLFILECATALOG>
"""
try:
    att = sys.argv[2]
except:
    att = job.attemptNr

if job.computingSite in ['',None,'NULL']:
    print 'computingSite is not yet defined'
    sys.exit(0)

siteSpec = siteMapper.getSite(job.computingSite)
scope_input, scope_output = select_scope(siteSpec, job.prodSourceLabel)

for file in job.Files:
    if file.type in ['output','log']:
        file.GUID = commands.getoutput('uuidgen')
        if job.computingSite == file.destinationSE and \
                siteSpec.setokens_output[scope_output].has_key(file.destinationDBlockToken):
            tmpSrcDDM = siteSpec.setokens_output[scope_output][file.destinationDBlockToken]
        else:
            tmpSrcDDM = siteSpec.ddm_output[scope_output]
        srm = TiersOfATLAS.getSiteProperty(tmpSrcDDM,'srm')
        srm = re.sub('^token:[^:]+:','',srm)
        xml += """
  <File ID="%s">
    <logical>
      <lfn name="%s"/>
    </logical>
    <metadata att_name="surl" att_value="%s/user.elmsheus/user.elmsheus.hc10006029.ANALY_LONG_BNL_ATLAS.1312433204.e0b.8181.ANALY_LONG_BNL_ATLAS/%s"/>    
    <metadata att_name="fsize" att_value="127340"/>
    <metadata att_name="md5sum" att_value="03cea4013bdb9f2e44050449b6ebf079"/>
   </File>""" % (file.GUID,file.lfn,srm,file.lfn)

xml += """
</POOLFILECATALOG>
"""

meta = \
"""<?xml version="1.0" encoding="UTF-8" standalone="no" ?> <!-- ATLAS file meta-data catalog --> <!DOCTYPE POOLFILECATALOG SYSTEM "InMemory"> <POOLFILECATALOG> <META type="string" name="size" value="82484969"/> <META type="string" name="conditionsTag" value="COMCOND-BLKPA-006-11"/> <META type="string" name="beamType" value="collisions"/> <META type="string" name="fileType" value="aod"/> <META type="string" name="autoConfiguration" value="['everything']"/> <META type="string" name="dataset" value=""/> <META type="string" name="maxEvents" value="200"/> <META type="string" name="AMITag" value="r5475"/> <META type="string" name="postInclude" value="['EventOverlayJobTransforms/Rt_override_BLKPA-006-11.py', 'EventOverlayJobTransforms/muAlign_reco.py']"/> <META type="string" name="preExec" value="['from CaloRec.CaloCellFlags import jobproperties;jobproperties.CaloCellFlags.doLArHVCorr.set_Value_and_Lock(False);muonRecFlags.writeSDOs=True', 'TriggerFlags.AODEDMSet=AODSLIM;rec.Commissioning.set_Value_and_Lock(True);jobproperties.Beam.numberOfCollisions.set_Value_and_Lock(20.0)']"/> <META type="string" name="triggerConfig" value="MCRECO:DBF:TRIGGERDBMC:325,142,266"/> <META type="string" name="preInclude" value="['EventOverlayJobTransforms/UseOracle.py', 'EventOverlayJobTransforms/custom.py', 'EventOverlayJobTransforms/recotrfpre.py']"/> <META type="string" name="geometryVersion" value="ATLAS-GEO-20-00-01"/> <META type="string" name="events"/> <META type="string" name="postExec" value="['from IOVDbSvc.CondDB import conddb"/> """

for file in job.Files:
    if file.type in ['output','log']:
        meta += """<File ID="{guid}"> <logical> <lfn name="{lfn}"/> </logical> <metadata att_name="fileType" att_value="esd"/> <metadata att_name="preExec" att_value="['from CaloRec.CaloCellFlags "/> <metadata att_name="postExec" att_value="['from IOVDbSvc.CondDB import conddb; "/> <metadata att_name="events" att_value="200"/> <metadata att_name="size" att_value="644128836"/> </File> """.format(guid=file.GUID,lfn=file.lfn)

meta += """ </POOLFILECATALOG>"""

import json

node={}
node['jobId']=id
node['state']='finished'
#node['state']='failed'
#node['pilotErrorCode']=1200
#node['pilotErrorCode']=-1202
node['pilotErrorDiag']='aaaaaaaaaaaaaaaaaaaaaaa'
node['metaData']=meta
node['siteName']='BNL_ATLAS_test'
node['attemptNr']=att
node['jobMetrics']="aaaaa=2 bbbb=3"
node['coreCount']=10

node['xml']=xml
url='%s/updateJob' % baseURLSSL

match = re.search('[^:/]+://([^/]+)(/.+)',url)
host = match.group(1)
path = match.group(2)

if os.environ.has_key('X509_USER_PROXY'):
    certKey = os.environ['X509_USER_PROXY']
else:
    certKey = '/tmp/x509up_u%s' % os.getuid()

rdata=urllib.urlencode(node)

conn = httplib.HTTPSConnection(host,key_file=certKey,cert_file=certKey)
conn.request('POST',path,rdata)
resp = conn.getresponse()
data = resp.read()

print data
