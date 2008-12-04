import sys
import time
import datetime

from taskbuffer.DBProxy import DBProxy
# password
from config import panda_config

proxyS = DBProxy()
proxyS.connect(panda_config.dbhost,panda_config.dbpasswd,panda_config.dbuser,panda_config.dbname)

site = sys.argv[1]
import userinterface.Client as Client

# erase dispatch datasets
def eraseDispDatasets(ids):
    print "eraseDispDatasets"
    datasets = []
    # get jobs
    status,jobs = Client.getJobStatus(ids)
    if status != 0:
        return
    # gather dispDBlcoks
    for job in jobs:
        # dispatchDS is not a DQ2 dataset in US
        if job.cloud == 'US':
            continue
        # erase disp datasets for production jobs only
        if job.prodSourceLabel != 'managed':
            continue
        for file in job.Files:
            if file.dispatchDBlock == 'NULL':
                continue
            if (not file.dispatchDBlock in datasets) and \
               re.search('_dis\d+$',file.dispatchDBlock) != None:
                datasets.append(file.dispatchDBlock)
    # erase
    for dataset in datasets:
        print 'erase %s' % dataset
        status,out = ddm.DQ2.main('eraseDataset',dataset)
        print out

timeLimit = datetime.datetime.utcnow() - datetime.timedelta(hours=4)
status,res = proxyS.querySQLS("SELECT PandaID FROM jobsActive4 WHERE jobStatus='activated' AND prodSourceLabel='managed' AND computingSite='%s' AND modificationTime<'%s' ORDER BY PandaID"
                              % (site,timeLimit.strftime('%Y-%m-%d %H:%M:%S')))
jobs = []
if res != None:
    for (id,) in res:
        jobs.append(id)
if len(jobs):
    nJob = 100
    iJob = 0
    while iJob < len(jobs):
        print 'reassign  %s' % str(jobs[iJob:iJob+nJob])
        eraseDispDatasets(jobs[iJob:iJob+nJob])        
        Client.reassignJobs(jobs[iJob:iJob+nJob])
        iJob += nJob
        time.sleep(10)

