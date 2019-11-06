'''
setup dataset

'''

import sys
import uuid
import traceback
import threading

from pandaserver.config import panda_config
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandaserver.taskbuffer import EventServiceUtils

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
                    voJobsMap.setdefault(tmpVO, [])
                    voJobsMap[tmpVO].append(tmpJob)
                # loop over all VOs
                for tmpVO in voJobsMap:
                    tmpJobList = voJobsMap[tmpVO]
                    tmpLog.debug('vo={0} has {1} jobs'.format(tmpVO,len(tmpJobList)))
                    # get plugin
                    setupperPluginClass = panda_config.getPlugin('setupper_plugins',tmpVO)
                    if setupperPluginClass is None:
                        # use ATLAS plug-in by default
                        from pandaserver.dataservice.SetupperAtlasPlugin import SetupperAtlasPlugin
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
                    except Exception:
                        errtype,errvalue = sys.exc_info()[:2]
                        tmpLog.error('plugin failed with {0}:{1}'.format(errtype, errvalue))
                tmpLog.debug('main end')
            else:
                tmpLog.debug('fork start')
                # write jobs to file
                import os
                try:
                    import cPickle as pickle
                except ImportError:
                    import pickle
                outFileName = '%s/set.%s_%s' % (panda_config.logdir,self.jobs[0].PandaID,str(uuid.uuid4()))
                outFile = open(outFileName, 'wb')
                pickle.dump(self.jobs, outFile, protocol=0)
                outFile.close()
                # run main procedure in another process because python doesn't release memory
                com =  'cd %s > /dev/null 2>&1; export HOME=%s; ' % (panda_config.home_dir_cwd,panda_config.home_dir_cwd)
                com += 'env PYTHONPATH=%s:%s %s/python -Wignore %s/dataservice/forkSetupper.py -i %s' % \
                       (panda_config.pandaCommon_dir,panda_config.pandaPython_dir,panda_config.native_python,
                        panda_config.pandaPython_dir,outFileName)
                if self.onlyTA:
                    com += " -t"
                if not self.firstSubmission:
                    com += " -f"
                tmpLog.debug(com)
                # execute
                status,output = self.taskBuffer.processLimiter.getstatusoutput(com)
                tmpLog.debug("return from main process: %s %s" % (status,output))                
                tmpLog.debug('fork end')
        except Exception as e:
            tmpLog.error('master failed with {0} {1}'.format(str(e), traceback.format_exc()))



    #  update jobs
    def updateJobs(self,jobList,tmpLog):
        updateJobs   = []
        failedJobs   = []
        activateJobs = []
        waitingJobs  = []
        closeJobs  = []
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
        # trigger merge generation if all events are done
        newActivateJobs = []
        nFinished = 0
        for job in activateJobs:
            if job.notDiscardEvents() and job.allOkEvents() and not EventServiceUtils.isEventServiceMerge(job):
                self.taskBuffer.activateJobs([job])
                # change status
                job.jobStatus = "finished"
                self.taskBuffer.updateJobs([job], False)
                nFinished += 1
            else:
                newActivateJobs.append(job)
        activateJobs = newActivateJobs
        tmpLog.debug('# of finished jobs in activated : {0}'.format(nFinished))
        newUpdateJobs = []
        nFinished = 0
        for job in updateJobs:
            if job.notDiscardEvents() and job.allOkEvents() and not EventServiceUtils.isEventServiceMerge(job):
                self.taskBuffer.updateJobs([job], True)
                # change status
                job.jobStatus = "finished"
                self.taskBuffer.updateJobs([job], True)
                nFinished += 1
            else:
                newUpdateJobs.append(job)
        updateJobs = newUpdateJobs
        tmpLog.debug('# of finished jobs in defined : {0}'.format(nFinished))
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
