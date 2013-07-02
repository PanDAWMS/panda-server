import os
import re
import sys
import commands

tarList = []
realTime = []
timeStamps = {}
for item in os.listdir('.'):
    if item.endswith('log.tgz'):
        commands.getoutput('tar xvfz %s' % item)
        for dirItem in os.listdir('.'):
            if os.path.isdir(dirItem):
                foundTime = False
                file = open('%s/pilot_child.stdout' % dirItem)
                event = -1
                for line in file:
                    line = re.sub('\n','',line)
                    if line.startswith('AthenaEventLoopMgr   INFO   ===>>>  start of event') \
                           or line.startswith('Init Time :') or line.startswith('Wake Time :'):
                        #event = line.split()[-2]
                        event += 1
                    match = re.search('Wake Time : \d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2}\.\d{3})',line)
                    if line.startswith('Exec Time :') or line.startswith('Init Time :') \
                           or match != None:
                        if match != None:
                            timeVal = match.group(1)
                        else:
                            timeVal = line.split()[-1]
                        if not (int(event) < 10 or int(event) % 10 == 0):
                            continue
                        if not timeStamps.has_key(event):
                            timeStamps[event] = [] 
                        timeStamps[event].append(timeVal)
                    if line.startswith('real'):
                        rT = re.sub('m',':',line.split()[-1])
                        rT = re.sub('s','',rT)
                        realTime.append(rT)
                file.close()
                commands.getoutput('rm -rf %s' % dirItem)
outReal = open('real.txt','w')
for rT in realTime:
    outReal.write('%s\n' % rT)
outReal.close()
nStamp = 0
events = timeStamps.keys()
events.sort()
outStamp = open('stamp.txt','w')
for event in events:
    stamps = timeStamps[event]
    if nStamp == 0:
        nStamp = len(stamps)
    if nStamp != len(stamps):
        print "ERROR : invalid nStamp %s %s" % (nStamp,len(stamps))
    str = '%s' % event
    for s in stamps:
        str += ',%s' % s 
    outStamp.write(str+'\n')
outStamp.close()
