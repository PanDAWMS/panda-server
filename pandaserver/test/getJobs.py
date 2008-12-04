import sys
import time
import threading
import urllib2,urllib

node={}
node['siteName']=sys.argv[1]
node['mem']=1000
node['prodSourceLabel']='user'
url='https://gridui01.usatlas.bnl.gov:25443/server/panda/getJob'
rdata=urllib.urlencode(node)

class Thr(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        req=urllib2.Request(url)
        fd=urllib2.urlopen(req,rdata)
        print time.localtime()
        data = fd.read()
        import cgi
        print cgi.parse_qs(data)


nThr = 1
thrs = []
for i in range(nThr):
    thrs.append(Thr())

for thr in thrs:
    thr.start()
