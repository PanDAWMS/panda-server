import sys
import urllib2,urllib

node={}
node['vuid']=sys.argv[1]
url='https://gridui01.usatlas.bnl.gov:25443/server/panda/datasetCompleted'
rdata=urllib.urlencode(node)
req=urllib2.Request(url)
fd=urllib2.urlopen(req,rdata)
data = fd.read()

print data
