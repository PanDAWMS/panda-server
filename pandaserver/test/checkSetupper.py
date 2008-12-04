import re
import time
import datetime
f = open("../../httpd/logs/panda-Setupper.log")
session = []
timeList = {}
for line in f:
    match = re.search('DEBUG (.*) startRun',line)
    if match:
        stamp = match.group(1)
        stamp = stamp.strip()
        session.append(stamp)
        timeM = re.search('^(\d+-\d+-\d+ \d+:\d+:\d+),',line)
        startTime = datetime.datetime(*time.strptime(timeM.group(1),'%Y-%m-%d %H:%M:%S')[:6])
        timeList[stamp] = startTime
        continue
    match = re.search('DEBUG (.*) endRun',line)
    if match:
        stamp = match.group(1)
        stamp = stamp.strip()
        session.remove(stamp)
        timeM = re.search('^(\d+-\d+-\d+ \d+:\d+:\d+),',line)
        endTime = datetime.datetime(*time.strptime(timeM.group(1),'%Y-%m-%d %H:%M:%S')[:6])
        if timeList.has_key(stamp):
            delta = endTime - timeList[stamp]
            if delta > datetime.timedelta(minutes = 10):
                print "Start : %s " % stamp
                print "   took -> %02d:%02d:%02d" % (delta.seconds/(60*60),(delta.seconds%(60*60))/60,delta.seconds%60)
        continue

print session
