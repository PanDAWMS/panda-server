import sys
import re
import time
import datetime
timeLimit  = datetime.timedelta(seconds=10)
f = open("../../httpd/logs/panda-DBProxy.log")
for line in f:
    match = re.search('unlock',line)
    if match:
        timeM = re.search('^(\d+-\d+-\d+ \d+:\d+:\d+),(\d+)',line)
        endTime = datetime.datetime(*time.strptime(timeM.group(1),'%Y-%m-%d %H:%M:%S')[:6])
        endTime = endTime.replace(microsecond = 1000*int(timeM.group(2)))
        timeM = re.search('getJobs : (\d+-\d+-\d+T\d+:\d+:\d+)\.(\d+)',line)
        startTime = datetime.datetime(*time.strptime(timeM.group(1),'%Y-%m-%dT%H:%M:%S')[:6])
        startTime = startTime.replace(microsecond = int(timeM.group(2)))
        if (endTime-startTime) > timeLimit:
            print '%s      %s' % (startTime,endTime-startTime)
f.close()
