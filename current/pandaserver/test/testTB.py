"""
test TaskBuffer and JobDispatcher on local PC

$ python -i testTB.py
>>> testGetJobs(10)
>>> testGetJobStatus(1)
>>> testUpdateJob(1,'running')
>>> testGetJobStatus(1)
>>> testUpdateJob(1,'finished')
>>> testGetJobStatus(1)
>>> taskBuffer.peekJobs([1,])
>>> taskBuffer.queryPandaIDs([0,])


"""


import time
import commands
import threading

from taskbuffer.JobSpec  import JobSpec
from taskbuffer.FileSpec import FileSpec

class TestThread (threading.Thread):
    def __init__(self,tb,i,n,siteName):
        threading.Thread.__init__(self)
        self.taskbuffer = tb
        self.interval = i
        self.jobDefinitionID = n        
        self.siteName = siteName

    def run(self):
        for i in range(1):
            prodDBlock        = 'rome.004201.evgen.ZeeJimmy'
            destinationDBlock = 'pandatest.000123.test.simul'
            destinationSE = 'BNL_SE'
            jobs = []
            #for i in range(self.interval):
            for i in range(2):
                job = JobSpec()
                job.jobDefinitionID=self.jobDefinitionID
                job.AtlasRelease='Atlas-11.0.1'
                job.prodDBlock=prodDBlock
                job.destinationDBlock=destinationDBlock
                job.destinationSE=destinationSE
                job.currentPriority=i 

                lfnI = 'rome.004201.evgen.ZeeJimmy._00001.pool.root'
                file = FileSpec()
                file.lfn = lfnI
                file.dataset = 'rome.004201.evgen.ZeeJimmy'
                file.type = 'input'
                file.prodDBlock = prodDBlock
                file.dataset = prodDBlock
                job.addFile(file)

                lfnO ='%s.pool.root.1' % commands.getoutput('uuidgen')
                file = FileSpec()
                file.lfn = lfnO
                file.type = 'output'
                file.destinationDBlock = destinationDBlock
                file.dataset = destinationDBlock
                file.destinationSE     = destinationSE
                job.addFile(file)

                job.homepackage='JobTransforms-11-00-01-01'
                job.transformation='share/rome.g4sim.standard.trf'
                job.jobParameters='%s %s 1 2 14268' % (lfnI,lfnO)
                jobs.append(job)
            self.taskbuffer.storeJobs(jobs,None)
            time.sleep(self.interval)

from taskbuffer.TaskBuffer import taskBuffer
from jobdispatcher.JobDispatcher import jobDispatcher
from userinterface.UserIF import userIF

import getpass
passwd = getpass.getpass()

taskBuffer.init('adbpro.usatlas.bnl.gov',passwd,nDBConnection=3)

jobDispatcher.init(taskBuffer)
userIF.init(taskBuffer)

jobDefID = int(time.time()) % 10000
thr1 = TestThread(taskBuffer,4,jobDefID,"myhost")
thr2 = TestThread(taskBuffer,3,jobDefID+1,"testsite")

thr1.start()
#thr2.start()

from jobdispatcher.JobDispatcher import getJob,updateJob
from userinterface.UserIF import submitJobs,getJobStatus,queryPandaIDs


### emulate HTTP requests

class Request:
    def __init__(self):
        self.subprocess_env = {}
        self.subprocess_env['SSL_CLIENT_S_DN'] = "aaa"
        self.subprocess_env['HTTPS'] = "on"

req = Request()        

def testGetJob():
    print getJob(req,"BNL_ATLAS_2")

def testGetJobStatus(arg):
    print getJobStatus(req,arg)

def testSubmitJobs(arg):
    print submitJobs(req,arg)

def testUpdateJob(arg0,arg1):
    print updateJob(req,arg0,arg1)

def testQueryPandaIDs(arg):
    print queryPandaIDs(req,arg)

"""

import cPickle as pickle
ids=[3023,3414]
testGetJobStatus(pickle.dumps(ids))

job = JobSpec()
job.jobDefinitionID='user.%s' % commands.getoutput('/usr/bin/uuidgen')
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

testSubmitJobs(pickle.dumps([job]))

testQueryPandaIDs(pickle.dumps([10]))

"""
