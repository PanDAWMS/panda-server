import sys
import urllib2,urllib

import userinterface.Client as Client

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
    <metadata att_name="fsize" att_value="127340"/>
    <metadata att_name="md5sum" att_value="03cea4013bdb9f2e44050449b6ebf079"/>
   </File>""" % (commands.getoutput('uuidgen'),file.lfn)

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
url='https://localhost:26443/server/panda/updateJob'
rdata=urllib.urlencode(node)
req=urllib2.Request(url)
fd=urllib2.urlopen(req,rdata)
data = fd.read()

print data
