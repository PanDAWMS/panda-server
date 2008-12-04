import re
import time
import datetime
import pylab
file = open('panda-DBProxy.log')
datesMap  = {}
valuesMap = {}
for line in file:
    items = re.findall('countPilotRequests[^\']+\'([^\']+)\': (\d+)',line)
    if len(items) != 0:
        # statistics
        site  = items[0][0]
        count = float(items[0][1])
        # date
        items = re.split(' |,',line)
        if len(items) >= 2:
            strDate = '%s %s' % tuple(items[:2])
            datetimeTime = datetime.datetime(*time.strptime(strDate,'%Y-%m-%d %H:%M:%S')[:6])
            # assign
            if not datesMap.has_key(site):
                datesMap[site]  = []
                valuesMap[site] = []                
            datesMap[site].append(pylab.date2num(datetimeTime))
            valuesMap[site].append(count)
# close file
file.close()
# plot
nRow = 1 #len(datesMap.keys())
nCol = 1
nFig = 1
tFig = 1
sites = datesMap.keys()
sites.sort()
for site in sites:
    if nFig == (nRow*nCol+1):
        pylab.savefig('pilot%d.png' % tFig)
        tFig += 1
        pylab.figure(tFig)
        nFig = 1
    pylab.subplot(int('%d%d%d' % (nRow,nCol,nFig)))
    pylab.title('Number of pilots @%s' % site)
    pylab.plot_date(datesMap[site],valuesMap[site])
    nFig += 1
# save the last figure
pylab.savefig('pilot%d.png' % tFig)
# show
#pylab.show()



                             
