from AdderPluginBase import AdderPluginBase
from CouchAPI.CMSCouch import CouchServer
import time, datetime, traceback, logging
import hashlib

def getHashLfn(lfn):
    """
    Provide a hashed lfn from an lfn.
    """
    return hashlib.sha224(lfn).hexdigest()

class AdderCmsPlugin(AdderPluginBase):

    def __init__(self, job, **params):
        """
        Get the config params and define the logger attribute.  
	"""
        AdderPluginBase.__init__(self, job, params)

        # TODO: Get the following parameters from a config file
        server = CouchServer("http://admin:rootroot@dashboard73.cern.ch:5184")
        #server = CouchServer("http://admin:rootroot@crab.pg.infn.it:5184")
        self.db = server.connectDatabase("asynctransfer")

        # Define the logger attribute
        self.logger = logging.getLogger('ASOPlugin')
        hdlr = logging.FileHandler('/data/atlpan/srv/var/log/panda/ASOPlugin.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        # TODO: Get the log verbosity from a config file 
        self.logger.setLevel(logging.DEBUG)
        self.logger.info("ASO plugin starts")

    def execute(self):
#      test
        # check job status
##        if self.job.jobStatus == 'failed':
            # job failed
            # the outputs of the job are not there so nothing to inject into ASO
##            pass

##        else:
            # job succeeded
        # do nothing in plugin for transferring jobs
        if self.job.jobStatus == 'transferring':
            self.result.setSucceeded()
            return
        last_update = int(time.time())
        now = str(datetime.datetime.now())
        doc = {}
        state = 'new'
        end_time = "" 

#        if self.job.jobStatus == 'failed':
#            state = "done"
        # *** file info
        for tmpFile in self.job.Files:
            destination = tmpFile.destinationSE
            if self.job.jobStatus == 'failed': 
                if tmpFile.type == 'output':
                    continue
                else:
                    destination = self.job.computingSite.split("ANALY_")[1] 
                    state = "done"
                    end_time = now
            self.logger.debug("the new lfn %s type %s params %s status %s type %s" %(tmpFile.lfn, tmpFile.type, self.job.jobParameters, self.job.jobStatus, tmpFile.type ) )
#                 if tmpFile.type == 'output':
                 #user = self.extraInfo['surl'][tmpFile.lfn].split('/')[4]
                 #lfn = self.extraInfo['surl'][tmpFile.lfn]
            lfn = tmpFile.lfn 
            if tmpFile.lfn.split('/')[2] == 'temp':
                user = tmpFile.lfn.split('/')[4]
            else:      
                user = tmpFile.lfn.split('/')[3]
                destination = self.job.computingSite.split("ANALY_")[1]
                state = "done"
                end_time = now 
            workflow = self.job.jobParameters.split(" ")[ len(self.job.jobParameters.split(" ")) - 1 ]
            if not workflow:
                workflow = self.job.jobParameters.split(" ")[ len(self.job.jobParameters.split(" ")) - 2 ]
            # Prepare the file document and queue it
            doc = { "_id": getHashLfn( lfn ),
                    "inputdataset": self.job.prodDBlock,
                    "group": "",
                    "lfn": lfn,
                    "checksums": {'adler32': tmpFile.checksum},
                    "size": tmpFile.fsize,
                    "user": user,
                    "source": self.job.computingSite.split("ANALY_")[1],
                    "destination": destination,
                    "last_update": last_update,
                    "state": state,
                    "role": "",
                    "dbSource_url": "Panda",
                    "dn": self.job.prodUserID,
                    "workflow": workflow, 
                    "start_time": now,
                    "end_time": end_time,
                    "job_end_time": str(self.job.endTime),
                    "jobid": self.job.PandaID,
                    "retry_count": [ ],
                    "publication_state": 'not_published',
                    "publication_retry_count": [],
                    "type" : tmpFile.type 
                 }   
            self.logger.debug("Trying to commit %s" % doc)
            try:
                self.db.queue(doc, True)
                self.db.commit()
                # append LFN to the list of transferring files,
                # which gets the job status to change to transferring
                self.result.transferringFiles.append(lfn)
            except Exception, ex:
                msg =  "Error queuing document in asyncdb"
                msg += str(ex)
                msg += str(traceback.format_exc())
                self.logger.error(msg)
                self.result.statusCode = 1
                return self.result
                 
            # Bulk commit of documents
#            if doc: 
#                try:
#                    self.db.commit()
#                except Exception, ex:
#                    msg =  "Error commiting documents in asyncdb"
#                    msg += str(ex)
#                    msg += str(traceback.format_exc())
#                    self.logger.error(msg)
#                    self.result.statusCode = 1
#                    return self.result

        # set 0 to self.result.statusCode when interactions succeeded
        # set 1 to self.result.statusCode when interactions failed due to a temporary error
        # set 2 to self.result.statusCode when interactions failed due to a fatal error
        self.result.statusCode = 0
        self.logger.info("ASOPlugin ends.")
        return self.result
