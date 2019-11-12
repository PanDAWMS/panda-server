'''
activate job

'''

from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Activator')


class Activator:
    # constructor
    def __init__(self,taskBuffer,dataset,enforce=False):
        self.dataset = dataset
        self.taskBuffer = taskBuffer
        self.enforce = enforce


    # to keep backward compatibility
    def start(self):
        self.run()
    def join(self):
        pass


    # main
    def run(self):
        _logger.debug("start: %s" % self.dataset.name)
        if self.dataset.status in ['completed','deleting','deleted'] and not self.enforce:
            _logger.debug("   skip: %s" % self.dataset.name)
        else:
            # update input files
            ids = self.taskBuffer.updateInFilesReturnPandaIDs(self.dataset.name,'ready')
            _logger.debug("IDs: %s" % ids)
            if len(ids) != 0:
                # get job
                jobs = self.taskBuffer.peekJobs(ids,fromActive=False,fromArchived=False,fromWaiting=False)
                # remove None and unknown
                acJobs = []
                for job in jobs:
                    if job is None or job.jobStatus == 'unknown':
                        continue
                    acJobs.append(job)
                # activate
                self.taskBuffer.activateJobs(acJobs)
            # update dataset in DB
            if self.dataset.type == 'dispatch':
                self.dataset.status = 'completed'        
                self.taskBuffer.updateDatasets([self.dataset])
        _logger.debug("end: %s" % self.dataset.name)
