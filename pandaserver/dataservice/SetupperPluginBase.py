from pandaserver.taskbuffer import EventServiceUtils


class SetupperPluginBase(object):
    def __init__(self, taskBuffer, jobs, logger, params, defaultMap):
        self.jobs = []
        self.jumboJobs = []
        # separate normal and jumbo jobs
        for tmpJob in jobs:
            if EventServiceUtils.isJumboJob(tmpJob):
                self.jumboJobs.append(tmpJob)
            else:
                self.jobs.append(tmpJob)
        self.taskBuffer = taskBuffer
        self.logger = logger
        # set named parameters
        for tmpKey in params:
            tmpVal = params[tmpKey]
            setattr(self, tmpKey, tmpVal)
        # set defaults
        for tmpKey in defaultMap:
            tmpVal = defaultMap[tmpKey]
            if not hasattr(self, tmpKey):
                setattr(self, tmpKey, tmpVal)

    # abstracts
    def run(self):
        pass

    def postRun(self):
        pass

    # update failed jobs
    def updateFailedJobs(self, jobs):
        for tmpJob in jobs:
            # set file status
            for tmpFile in tmpJob.Files:
                if tmpFile.type in ["output", "log"]:
                    if tmpFile.status not in ["missing"]:
                        tmpFile.status = "failed"
        self.taskBuffer.updateJobs(jobs, True)
