'''
setup dataset

'''

import re
import sys
import datetime
import commands
import threading


from config import panda_config
from pandalogger.PandaLogger import PandaLogger
from pandalogger.LogWrapper import LogWrapper

# logger
_logger = PandaLogger().getLogger('Setupper')

panda_config.setupPlugin()


# main class
class Setupper (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,jobs,resubmit=False,pandaDDM=False,ddmAttempt=0,forkRun=False,onlyTA=False,
                 resetLocation=False,firstSubmission=True):
        threading.Thread.__init__(self)
        self.jobs       = jobs
        self.taskBuffer = taskBuffer
        # resubmission or not
        self.resubmit = resubmit
        # use PandaDDM
        self.pandaDDM = pandaDDM
        # priority for ddm job
        self.ddmAttempt = ddmAttempt
        # fork another process because python doesn't release memory
        self.forkRun = forkRun
        # run task assignment only
        self.onlyTA = onlyTA
        # reset locations
        self.resetLocation = resetLocation
        # first submission
        self.firstSubmission = firstSubmission

        
        
    # main
    def run(self):
        try:
            # make a message instance
            tmpLog = LogWrapper(_logger,None)
            # run main procedure in the same process
            if not self.forkRun:
                tmpLog.debug('main start')
                tmpLog.debug('firstSubmission={0}'.format(self.firstSubmission))
                # group jobs per VO
                voJobsMap = {}
                ddmFreeJobs = []
                tmpLog.debug('{0} jobs in total'.format(len(self.jobs)))
                for tmpJob in self.jobs:
                    # set VO=local for DDM free 
                    if tmpJob.destinationSE == 'local':
                        tmpVO = 'local'
                    else:
                        tmpVO = tmpJob.VO
                    # make map
                    if not voJobsMap.has_key(tmpVO):
                        voJobsMap[tmpVO] = []
                    voJobsMap[tmpVO].append(tmpJob)
                # loop over all VOs
                for tmpVO,tmpJobList in voJobsMap.iteritems():
                    tmpLog.debug('vo={0} has {1} jobs'.format(tmpVO,len(tmpJobList)))
                    # get plugin
                    setupperPluginClass = panda_config.getPlugin('setupper_plugins',tmpVO)
                    if setupperPluginClass == None:
                        # use ATLAS plug-in by default
                        from SetupperAtlasPlugin import SetupperAtlasPlugin
                        setupperPluginClass = SetupperAtlasPlugin
                    tmpLog.debug('plugin name -> {0}'.format(setupperPluginClass.__name__))
                    try:
                        # make plugin
                        setupperPlugin = setupperPluginClass(self.taskBuffer,self.jobs,tmpLog,
                                                             resubmit=self.resubmit,
                                                             pandaDDM=self.pandaDDM,
                                                             ddmAttempt=self.ddmAttempt,
                                                             onlyTA=self.onlyTA,
                                                             firstSubmission=self.firstSubmission)
                        # run plugin
                        tmpLog.debug('run plugin')
                        setupperPlugin.run()
                        # go forward if not TA
                        if not self.onlyTA:
                            # update jobs
                            tmpLog.debug('update jobs')
                            self.updateJobs(setupperPlugin.jobs+setupperPlugin.jumboJobs,tmpLog)
                            # execute post process
                            tmpLog.debug('post execute plugin')
                            setupperPlugin.postRun()
                        tmpLog.debug('done plugin')
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('plugin failed with {0}:{1}'.format(errtype,errvalue))
                tmpLog.debug('main end')
            else:
                tmpLog.debug('fork start')
                # write jobs to file
                import os
                import cPickle as pickle
                outFileName = '%s/set.%s_%s' % (panda_config.logdir,self.jobs[0].PandaID,commands.getoutput('uuidgen'))
                outFile = open(outFileName,'w')
                pickle.dump(self.jobs,outFile)
                outFile.close()
                # run main procedure in another process because python doesn't release memory
                com =  'cd %s > /dev/null 2>&1; export HOME=%s; ' % (panda_config.home_dir_cwd,panda_config.home_dir_cwd)
                com += 'source %s; ' % panda_config.glite_source
                com += 'env PYTHONPATH=%s:%s %s/python -Wignore %s/dataservice/forkSetupper.py -i %s' % \
                       (panda_config.pandaCommon_dir,panda_config.pandaPython_dir,panda_config.native_python,
                        panda_config.pandaPython_dir,outFileName)
                if self.onlyTA:
                    com += " -t"
                if not self.firstSubmission:
                    com += " -f"
                tmpLog.debug(com)
                # exeute
                status,output = self.taskBuffer.processLimiter.getstatusoutput(com)
                tmpLog.debug("return from main process: %s %s" % (status,output))                
                tmpLog.debug('fork end')
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('master failed with {0}:{1}'.format(errtype,errvalue))



    #  update jobs
    def updateJobs(self,jobList,tmpLog):
        updateJobs   = []
        failedJobs   = []
        activateJobs = []
        waitingJobs  = []
        # sort out jobs
        for job in jobList:
            # failed jobs
            if job.jobStatus in ['failed','cancelled']:
                failedJobs.append(job)
            # waiting
            elif job.jobStatus == 'waiting':
                waitingJobs.append(job)
            # no input jobs
            elif job.dispatchDBlock=='NULL':
                activateJobs.append(job)
            # normal jobs
            else:
                # change status
                job.jobStatus = "assigned"
                updateJobs.append(job)
        # update DB
        tmpLog.debug('# of activated jobs : {0}'.format(len(activateJobs)))
        self.taskBuffer.activateJobs(activateJobs)
        tmpLog.debug('# of updated jobs : {0}'.format(len(updateJobs)))
        self.taskBuffer.updateJobs(updateJobs,True)
        tmpLog.debug('# of failed jobs : {0}'.format(len(failedJobs)))
        self.taskBuffer.updateJobs(failedJobs,True)
        tmpLog.debug('# of waiting jobs : {0}'.format(len(waitingJobs)))
        self.taskBuffer.keepJobs(waitingJobs)
        # delete local values
        del updateJobs
        del failedJobs
        del activateJobs
        del waitingJobs
