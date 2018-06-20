import argparse
from taskbuffer.TaskBuffer import taskBuffer
from config import panda_config
from userinterface import Client
from rucio.client import Client as RucioClient
from rucio.common.exception import DataIdentifierNotFound
from dataservice.DDM import rucioAPI

taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# parse option
parser = argparse.ArgumentParser()
parser.add_argument('--ds',action='store',dest='ds',default=None,required=True,
                    help='dataset name')
parser.add_argument('--files',action='store',dest='files',default=None,required=True,
                    help='comma-separated list of lost file names')
parser.add_argument('--noChildRetry',action='store_const',const=True,dest='noChildRetry',default=False,
                    help='not retry child tasks')
parser.add_argument('--resurrectDS',action='store_const',const=True,dest='resurrectDS',default=False,
                    help='resurrect output and log datasets if they were already deleted')

options = parser.parse_args()

files = options.files.split(',')

print 

s,jediTaskID = taskBuffer.resetFileStatusInJEDI('',True,options.ds,files,[])
if s:
    if options.resurrectDS:
        sd,so = taskBuffer.querySQLS('SELECT datasetName FROM ATLAS_PANDA.JEDI_Datasets WHERE jediTaskID=:id AND type IN (:t1,:t2)',
                                     {':id': jediTaskID, ':t1': 'output', ':t2': 'log'})
        rc = RucioClient()
        for datasetName, in so:
            for i in range(3):
                try:
                    scope, name = rucioAPI.extract_scope(datasetName)
                    rc.get_did(scope, name)
                    break
                except DataIdentifierNotFound:
                    print 'resurrect {0}'.format(datasetName)
                    rc.resurrect([{'scope': scope, 'name': name}])
                    try:
                        rc.set_metadata(scope, name, 'lifetime', None)
                    except:
                        pass
    print Client.retryTask(jediTaskID, noChildRetry=options.noChildRetry)[-1][-1]
    print 'done for jediTaskID={0}'.format(jediTaskID)
else:
    print 'failed'

