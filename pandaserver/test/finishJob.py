import os
import re
import sys
import urllib2,urllib

import userinterface.Client as Client
from userinterface.Client import baseURLSSL

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

for file in job.Files:
    if file.type in ['output','log']:
        xml += """
  <File ID="%s">
    <logical>
      <lfn name="%s"/>
    </logical>
    <metadata att_name="surl" att_value="srm://dcsrm.usatlas.bnl.gov:8443/srm/managerv2?SFN=/pnfs/usatlas.bnl.gov/atlasuserdisk/user.elmsheus/user.elmsheus.hc10006029.ANALY_LONG_BNL_ATLAS.1312433204.e0b.8181.ANALY_LONG_BNL_ATLAS/%s"/>    
    <metadata att_name="fsize" att_value="127340"/>
    <metadata att_name="md5sum" att_value="03cea4013bdb9f2e44050449b6ebf079"/>
   </File>""" % (commands.getoutput('uuidgen'),file.lfn,file.lfn)

xml += """
</POOLFILECATALOG>
"""

node={}
node['jobId']=id
node['state']='finished'
node['metaData']='finished'
#node['state']='failed'
#node['pilotErrorCode']=1200
node['siteName']='BNL_ATLAS_test'

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
