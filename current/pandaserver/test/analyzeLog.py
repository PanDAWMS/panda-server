import re
from config import panda_config

# analyze Setupper log
logSetupper  = open('%s/panda-Setupper.log' % panda_config.logdir)
# extract subscriptions
mapSub = {}
mapDataset = {}
for line in logSetupper:
    items = re.findall("'registerDatasetSubscription', '(.+_dis\d+)', '([^']+)'",line)
    if len(items) != 0:
        dataset = items[0][0]
        siteID  = items[0][1]
        date    = '%s %s' % tuple(re.split(' |,',line)[:2])
        if not mapSub.has_key(siteID):
            mapSub[siteID] = []
        # append
        mapSub[siteID].append(dataset)
        mapDataset[dataset] = (date,False)
logSetupper.close()

# analyze Activator log
logActivator = open('%s/panda-Activator.log' % panda_config.logdir)
# extract callbacks
for line in logActivator:
    items = re.findall("start: (\S+_dis\d+)$",line)
    if len(items) != 0:
        dataset = items[0]
        if dataset in mapDataset.keys():
            mapDataset[dataset] = mapDataset[dataset][:-1]+(True,)
logActivator.close()

# print
for siteID in mapSub.keys():
    print "ID : %s" % siteID
    nSucceed = 0
    failedSubs = []
    for dataset in mapSub[siteID]:
        # succeeded
        if mapDataset[dataset][-1:][0]:
            nSucceed += 1
        # failed
        else:
            failedSubs.append((mapDataset[dataset][0],dataset))
    # statistics
    print " Total:%d  Succeeded:%d" % (len(mapSub[siteID]),nSucceed)
    # not completed subscriptions
    print "  Not completed"
    for item in failedSubs:
        print "   %s" % item[0]
        print "     %s" % item[1]
    print
        
                
        
