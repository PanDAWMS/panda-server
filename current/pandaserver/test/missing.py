import re
import commands

stMap = []
tmpMap = {}
nLog = 30
for i in range(0,nLog):
    if i == 0:
        out = commands.getoutput('cat /data/sm/prod/httpd/logs/panda-Adder.log')
    else:
        out = commands.getoutput('zcat /data/sm/prod/httpd/logs/panda-Adder.log.%s.gz' % (nLog-i))        
    for line in out.split('\n'):
        stStr  = re.search('start: finished',line)
        idsStr = re.search('ids = .*$',line)
        mapStr = re.search('idMap = .*$',line)
        if stStr == None and idsStr == None and mapStr == None:
            continue
        items = line.split()
        try:
            pandaID = int(items[4])
        except:
            continue
        if stStr != None:
            stMap.append(pandaID)
        if idsStr != None:
            exec idsStr.group(0)
            tmpMap[pandaID] = ids
        if mapStr != None:
            exec mapStr.group(0)
            if (pandaID in stMap) and idMap == {} and tmpMap[pandaID] != ([], []):
                print pandaID
                print tmpMap[pandaID]
            try:
                del tmpMap[pandaID]
            except:
                pass
            try:
                stMap.remove(pandaID)
            except:
                pass
if tmpMap != {}:
    print tmpMap
    
