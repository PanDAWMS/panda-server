from config import panda_config
from taskbuffer.TaskBuffer import taskBuffer

# initialize cx_Oracle using dummy connection
from taskbuffer.Initializer import initializer
initializer.init()

# instantiate TB
taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

from dq2.common import stomp

class DummyListener(stomp.ConnectionListener):

    def __init__(self, conn):
        self.conn = conn

    def on_message(self, headers, message):
        id = headers['message-id']
        print message
        self.conn.ack({'message-id':id})
        # update file
        ids = taskBuffer.updateInFilesReturnPandaIDs(dsName,'ready',fileGUID)
        if len(ids) != 0:
            jobs = taskBuffer.peekJobs(ids,fromActive=False,fromArchived=False,fromWaiting=False)
            # remove None and unknown
            acJobs = []
            for job in jobs:
                if job == None or job.jobStatus == 'unknown':
                    continue
                acJobs.append(job)
            # activate
            taskBuffer.activateJobs(acJobs)
                                                                                    
            

clientid = 'MyPandaClientID'
topic = '/topic/atlas.ddm.siteservices'
queue = '/queue/Consumer.pandatest.atlas.ddm.siteservices'

conn = stomp.Connection(host_and_ports = [('atlddmmsgprod.cern.ch', 6163)])
conn.set_listener('DummyListener', DummyListener(conn))
conn.start()
conn.connect(headers = {'client-id': clientid})
conn.subscribe(destination=queue, ack='client-individual')
