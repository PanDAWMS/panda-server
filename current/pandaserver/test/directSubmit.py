import re
import sys
import time
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

if len(sys.argv) != 2:
    print "task file is missing"
    sys.exit(0)

# open task file
taskFile = open(sys.argv[1])

# read common parameters
line = taskFile.readline()
items = line.split()

# common parameters
taskID     = items[0]
inTaskName = items[1]
taskName   = items[2]
formats    = items[3].split('.')
lparams    = items[4].split(',')
vparams    = items[5].split(',')
trf        = items[7]
trfVer     = items[8]
grid       = items[10]
priority   = items[11]
totalJob   = items[14]
cpu        = items[15]
memory     = items[16]


# input dataset   
iDataset = 'NULL'
m = re.search('(.+)\.([^\.]+)\.([^\.]+)$',inTaskName)
if m != None:
    step = m.group(2)
    if step == 'evgen':
        format = 'EVENT'
    elif step == 'digit':
        format = 'RDO'
    else:
        format = 'AOO'
    #### FIXME : _tidXXXX is missing
    iDataset = '%s.%s.%s.%s' % (m.group(1),step,format,m.group(3))


# output datasets
m = re.search('(.+)\.([^\.]+)\.([^\.]+)$',taskName)
oDatasets = []
for format in formats:
    step = m.group(2)
    if format=='HITS':
        step = 'simul'
    # append
    oDatasets.append('%s.%s.%s.%s_tid%06d' % (m.group(1),step,format,m.group(3),int(taskID)))

# log dataset
lDataset = '%s.%s.%s.%s_tid%06d' % (m.group(1),m.group(2),'log',m.group(3),int(taskID))


# instantiate JobSpecs
iJob = 0
jobList = []
for line in taskFile:
    iJob += 1
    job = JobSpec()
    # job ID  ###### FIXME
    job.jobDefinitionID   = int(time.time()) % 10000
    # job name
    job.jobName           = "%s_%05d.job" % (taskName,iJob)
    # AtlasRelease
    if len(re.findall('\.',trfVer)) > 2:
        match = re.search('^(\d+\.\d+\.\d+)',trfVer)
        job.AtlasRelease  = 'Atlas-%s' % match.group(1)
    else:
        job.AtlasRelease  = 'Atlas-%s' % trfVer
    # homepackage
    vers = trfVer.split('.')
    if int(vers[0]) <= 11:
        job.homepackage = 'JobTransforms'
        for ver in vers:
            job.homepackage += "-%02d" % int(ver)
    else:
        job.homepackage = 'AtlasProduction/%s' % trfVer
    # trf
    job.transformation    = trf
    job.destinationDBlock = oDatasets[0]
    # prod DBlock
    job.prodDBlock        = iDataset
    # souce lavel
    job.prodSeriesLabel   = 'pandatest'
    job.prodSourceLabel   = 'managed'
    # priority
    job.assignedPriority  = priority
    job.currentPriority   = priority
    # CPU, memory,disk ### FIXME

    # attempt number   ### FIXME

    # input files
    if iDataset != 'NULL':
        # remove _tidXXX
        pat = re.sub('_tid\d+$','',iDataset)
        # search
        m = re.search('('+pat+'\S+)',line)
        if m != None:
            file = FileSpec()
            file.lfn  = m.group(1)
            file.type = 'input'
            file.dataset    = iDataset
            file.prodDBlock = iDataset
            job.addFile(file)
    # DB release
    for i,lpar in enumerate(lparams):
        if lpar == 'DBRelease':
            file = FileSpec()
            file.lfn  = "%s-%s.tgz" % (lpar,vparams[i])
            file.type = 'input'
            file.dataset    = iDataset
            file.prodDBlock = iDataset
            job.addFile(file)
            break
    # output files
    for oDataset in oDatasets:
        # remove _tidXXX
        pat = re.sub('_tid\d+$','',oDataset)
        # search
        m = re.search('('+pat+'\S+)',line)
        if m != None:
            file = FileSpec()
            file.lfn  = m.group(1)
            file.type = 'output'
            file.dataset           = oDataset
            file.destinationDBlock = oDataset
            job.addFile(file)
    # log
    file = FileSpec()
    file.lfn  = "%s._%05d.log.tgz" % (lDataset,iJob)
    file.type = 'log'
    file.dataset           = lDataset
    file.destinationDBlock = lDataset
    job.addFile(file)

    # job par
    job.jobParameters = line[:-1]

    """
    print job.values()
    for file in job.Files:
        print file.values()
    sys.exit(0)
    """    
    jobList.append(job)
    

s,o = Client.submitJobs(jobList)
print "---------------------"
print s
for x in o:
    print "PandaID=%s" % x[0]
