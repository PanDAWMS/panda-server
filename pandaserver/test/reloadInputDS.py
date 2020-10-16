import sys
import argparse
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.config import panda_config
from pandaserver.userinterface import Client
from rucio.client import Client as RucioClient
from rucio.common.exception import DataIdentifierNotFound
from pandaserver.dataservice.DDM import rucioAPI

taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

# parse option
parser = argparse.ArgumentParser()
parser.add_argument('--tid',action='store',dest='tid',default=None,required=True,
                    help='task ID')
parser.add_argument('--resurrectDS',action='store_const',const=True,dest='resurrectDS',default=False,
                    help='resurrect output and log datasets if they were already deleted')

options = parser.parse_args()

jediTaskID = int(options.tid)

if True:
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
                    print ('resurrect {0}'.format(datasetName))
                    rc.resurrect([{'scope': scope, 'name': name}])
                    try:
                        rc.set_metadata(scope, name, 'lifetime', None)
                    except:
                        pass
    print (Client.reloadInput(jediTaskID)[-1])
    print ('done for jediTaskID={0}'.format(jediTaskID))
else:
    print ('failed')
