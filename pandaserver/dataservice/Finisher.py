'''
finish transferring jobs

'''

import re
import sys
import commands
import threading
from DDM import ddm
from config import panda_config

from pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('Finisher')


class Finisher (threading.Thread):
    # constructor
    def __init__(self,taskBuffer,dataset,job=None):
        threading.Thread.__init__(self)
        self.dataset = dataset
        self.taskBuffer = taskBuffer
        self.job = job


    # main
    def run(self):
        # start
        if self.job == None:
            _logger.debug("start: %s" % self.dataset.name)
            # update input files
            ids = self.taskBuffer.updateOutFilesReturnPandaIDs(self.dataset.name)
        else:
            _logger.debug("start: %s" % self.job.PandaID)
            # update input files
            ids = [self.job.PandaID]
        _logger.debug("IDs: %s" % ids)
        if len(ids) != 0:
            # get job
            if self.job == None:
                jobs = self.taskBuffer.peekJobs(ids,fromDefined=False,fromArchived=False,fromWaiting=False)
            else:
                jobs = [self.job]
            # loop over all jobs
            for job in jobs:
                if job == None:
                    continue
                _logger.debug("Job: %s" % job.PandaID)
                if job.jobStatus == 'transferring':
                    jobReady = True
                    # check file status
                    for file in job.Files:
                        if file.type == 'output' or file.type == 'log':
                            if file.status != 'ready':
                                jobReady = False
                                break
                    # finish job
                    if jobReady:
                        # create XML
                        try:
                            import xml.dom.minidom
                            dom = xml.dom.minidom.getDOMImplementation()
                            doc = dom.createDocument(None,'xml',None)
                            topNode = doc.createElement("POOLFILECATALOG")
                            for file in job.Files:
                                if file.type in ['output','log']:
                                    # File
                                    fileNode = doc.createElement("File")
                                    fileNode.setAttribute("ID",file.GUID)
                                    # LFN
                                    logNode = doc.createElement("logical")
                                    lfnNode = doc.createElement("lfn")
                                    lfnNode.setAttribute('name',file.lfn)
                                    # metadata
                                    fsizeNode    = doc.createElement("metadata")
                                    fsizeNode.setAttribute("att_name","fsize")
                                    fsizeNode.setAttribute("att_value",str(file.fsize))
                                    # checksum
                                    if file.checksum.startswith('ad:'):
                                        # adler32
                                        chksumNode    = doc.createElement("metadata")
                                        chksumNode.setAttribute("att_name","adler32")
                                        chksumNode.setAttribute("att_value",re.sub('^ad:','',file.checksum))
                                    else:
                                        # md5sum
                                        chksumNode    = doc.createElement("metadata")
                                        chksumNode.setAttribute("att_name","md5sum")
                                        chksumNode.setAttribute("att_value",re.sub('^md5:','',file.checksum))
                                    # append nodes
                                    logNode.appendChild(lfnNode)
                                    fileNode.appendChild(logNode)
                                    fileNode.appendChild(fsizeNode)
                                    fileNode.appendChild(chksumNode)
                                    topNode.appendChild(fileNode)
                            # write to file
                            xmlFile = '%s/%s_%s_%s' % (panda_config.logdir,job.PandaID,'finished',commands.getoutput('uuidgen'))
                            oXML = open(xmlFile,"w")
                            oXML.write(topNode.toxml())
                            oXML.close()
                        except:
                            type, value, traceBack = sys.exc_info()
                            _logger.error("%s : %s %s" % (job.PandaID,type,value))
                _logger.debug("Job: %s status: %s" % (job.PandaID,job.jobStatus))                
        # end
        if self.job == None:        
            _logger.debug("end: %s" % self.dataset.name)
        else:
            _logger.debug("end: %s" % self.job.PandaID)
