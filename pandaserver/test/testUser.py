import time
import commands
import userinterface.Client as Client
from taskbuffer.JobSpec import JobSpec
from taskbuffer.FileSpec import FileSpec

job = JobSpec()
job.jobDefinitionID   = int(time.time()) % 10000
job.jobName           = commands.getoutput('/usr/bin/uuidgen') 
job.AtlasRelease      = 'Atlas-9.0.4'
job.prodDBlock        = 'pandatest.000003.dd.input'
job.destinationDBlock = 'panda.destDB.%s' % commands.getoutput('/usr/bin/uuidgen')
job.destinationSE     = 'BNL_SE'

ids = {'pandatest.000003.dd.input._00028.junk':'6c19e1fc-ee8c-4bae-bd4c-c9e5c73aca27',
       'pandatest.000003.dd.input._00033.junk':'98f79ba1-1793-4253-aac7-bdf90a51d1ee',
       'pandatest.000003.dd.input._00039.junk':'33660dd5-7cef-422a-a7fc-6c24cb10deb1'}
for lfn in ids.keys():
    file = FileSpec()
    file.lfn = lfn
    file.GUID = ids[file.lfn]
    file.dataset = 'pandatest.000003.dd.input'
    file.type = 'input'
    job.addFile(file)

s,o = Client.submitJobs([job])
print "---------------------"
print s
print o
print "---------------------"
s,o = Client.getJobStatus([4934, 4766, 4767, 4768, 4769])
print s
if s == 0:
    for job in o:
        if job == None:
            continue
        print job.PandaID
        for file in job.Files:
            print file.lfn,file.type
print "---------------------"
s,o = Client.queryPandaIDs([0])
print s
print o

