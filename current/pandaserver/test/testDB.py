#!/usr/bin/python

"""
test DB access

"""

from taskbuffer.JobSpec     import JobSpec
from taskbuffer.FileSpec    import FileSpec
from taskbuffer.DatasetSpec import DatasetSpec
from taskbuffer.DBProxyPool import DBProxyPool

import getpass
passwd = getpass.getpass()

pool = DBProxyPool('adbpro.usatlas.bnl.gov',passwd,2)

proxy = pool.getProxy()

import sys
import commands

job1 = JobSpec()
job1.PandaID='NULL'
job1.jobStatus='unknown'
job1.computingSite="aaa"
f11 = FileSpec()
f11.lfn = 'in1.pool.root'
f11.type = 'input'
job1.addFile(f11)
f12 = FileSpec()
f12.lfn = 'out1.pool.root'
f12.type = 'output'
job1.addFile(f12)

job2 = JobSpec()
job2.PandaID='NULL'
job2.jobStatus='unknown'
job2.computingSite="bbb"
f21 = FileSpec()
f21.lfn = 'in2.pool.root'
f21.type = 'input'
job2.addFile(f21)
f22 = FileSpec()
f22.lfn = 'out2.pool.root'
f22.type = 'output'
job2.addFile(f22)

proxy.insertNewJob(job1)
proxy.insertNewJob(job2)
print "Inserted %d %d" % (job1.PandaID,job2.PandaID)
proxy.activateJob(job1)
proxy.activateJob(job2)
print "activated"
ret = proxy.getJobs(1,"aaa")
print "Got Jobs"
for j in ret:
    print j.PandaID
print proxy.peekJob(job1.PandaID).jobStatus
proxy.updateJobStatus(job1.PandaID,"unknown")
print " ->" ,proxy.peekJob(job1.PandaID).jobStatus

print proxy.peekJob(job2.PandaID).jobStatus
job2.jobStatus = "running"
proxy.updateJob(job2,False)
print " ->" ,proxy.peekJob(job2.PandaID).jobStatus
print "Updated"
proxy.archiveJob(job1,False)
proxy.archiveJobLite(job2.PandaID,job2.jobStatus)
print "Archived"
proxy.querySQL("DELETE FROM jobsArchived3 WHERE PandaID=%d" % job1.PandaID)
proxy.querySQL("DELETE FROM jobsArchived3 WHERE PandaID=%d" % job2.PandaID)
print "job Deleted"

print "dataset"
dataset = DatasetSpec()
dataset.vuid = commands.getoutput('/usr/bin/uuidgen')
dataset.name = 'test.%s' % dataset.vuid

proxy.insertDataset(dataset)
print dataset.vuid
dataset2 = proxy.queryDataset(dataset.vuid)
print dataset2.values()
dataset2.type = 'test'
proxy.updateDataset(dataset2)
dataset3 = proxy.queryDataset(dataset.vuid)
print dataset3.values()
proxy.querySQL("DELETE FROM Datasets WHERE vuid='%s'" % dataset.vuid)
