import os
import sys
import time
import signal
import commands

from config import panda_config
from dq2.common import log as logging
import stomp

# keep PID
pidFile = '%s/file_callback_listener.pid' % panda_config.logdir


# create daemon
def createDaemon():
    try:
        pid = os.fork()
        # terminate master process
        if pid != 0:
            os._exit(0)
    except OSError, e:
        raise Exception,"%s [%d]" % (e.strerror, e.errno)
    # runs in a new session
    os.setsid()
    os.umask(0)


# kill whole process
def catch_sig(sig, frame):
    try:
        os.remove(pidFile)
    except:
        pass
    # kill
    commands.getoutput('kill -9 -- -%s' % os.getpgrp())
    # exit
    sys.exit(0)
                                        

# callback listener
class FileCallbackListener(stomp.ConnectionListener):

    def __init__(self,conn,tb):
        # connection
        self.conn = conn
        # task buffer
        self.tb = tb
        # logger
        from pandalogger.PandaLogger import PandaLogger
        self.log = PandaLogger().getLogger('fileCallbackListener')
        

    def on_message(self, headers, message):
        id = headers['message-id']
        print message
        self.conn.ack({'message-id':id})
        # update file
        ids = self.tb.updateInFilesReturnPandaIDs(dsName,'ready',fileGUID)
        if len(ids) != 0:
            # remove None and unknown
            acJobs = []
            for tmpID in ids:
                # count the number of pending files
                nPending = self.tb.countPendingFiles(tmpID)
                if nPending != 0:
                    continue
                # get job
                jobs = self.tb.peekJobs(ids,fromActive=False,fromArchived=False,fromWaiting=False)
                if len(jobs) == 0 or jobs[0] == None or job[0].jobStatus == 'unknown':
                    continue
                acJobs.append(job[0])
            # activate
            self.tb.activateJobs(acJobs)


# main
def main():
    # make daemon
    createDaemon()
    # write PID
    pidFH = open(pidFile,'w')
    pidFH.write("%d" % os.getpid())
    pidFH.close()
    # register signal handler
    signal.signal(signal.SIGTERM,catch_sig)
    # make child to be properly killed via signal
    while True:
        pid = os.fork()
        if pid != 0:
            # watch child process
            os.wait()
            time.sleep(1)
        else:    
            # main loop
            from taskbuffer.TaskBuffer import taskBuffer
            # initialize cx_Oracle using dummy connection
            from taskbuffer.Initializer import initializer
            initializer.init()
            # instantiate TB
            taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)
            # ActiveMQ params
            clientid = 'PANDA'
            queue = '/queue/Consumer.PANDA.atlas.ddm.siteservices'
            ssl_opts = {'use_ssl' : True,
                        'ssl_cert_file' : '/data/atlpan/x509up_u25606',
                        'ssl_key_file' :  '/data/atlpan/x509up_u25606'}
            # resolve multiple brokers
            import socket
            brokerList = socket.gethostbyname_ex('atlas-ddm.msg.cern.ch')[-1]
            # set listener
            for tmpBroker in brokerList:
                conn = stomp.Connection(host_and_ports = [(tmpBroker, 6162)])
                conn.set_listener('FileCallbackListener', FileCallbackListener(conn,taskBuffer))
                conn.start()
                conn.connect(headers = {'client-id': clientid})
                conn.subscribe(destination=queue, ack='client-individual')
            # exit
            os._exit(0)

# entry
if __name__ == "__main__":
    main()


                                                                                    
            

