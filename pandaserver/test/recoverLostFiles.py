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
parser.add_argument('--ds',action='store',dest='ds',default=None,required=True,
                    help='dataset name')
parser.add_argument('--files',action='store',dest='files',default=None,
                    help='comma-separated list of lost file names. The list is dedeuced if this option is omitted')
parser.add_argument('--noChildRetry',action='store_const',const=True,dest='noChildRetry',default=False,
                    help='not retry child tasks')
parser.add_argument('--resurrectDS',action='store_const',const=True,dest='resurrectDS',default=False,
                    help='resurrect output and log datasets if they were already deleted')
parser.add_argument('--dryRun',action='store_const',const=True,dest='dryRun',default=False,
                    help='dry run')

options = parser.parse_args()

if options.files is not None:
    files = options.files.split(',')
else:
    # get files from rucio
    rc = RucioClient()
    scope, name = rucioAPI.extract_scope(options.ds)
    files_rucio = set()
    for i in rc.list_files(scope, name):
        files_rucio.add(i['name'])
    # get files from panda
    dsName = options.ds.split(':')[-1]
    fd,fo = taskBuffer.querySQLS('SELECT c.lfn FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.JEDI_Dataset_Contents c WHERE d.jediTaskID=c.jediTaskID AND d.datasetID=c.datasetID AND d.type IN (:t1,:t2) AND c.status=:s AND d.datasetName=:name ',
                                     {':s': 'finished', ':t1': 'output', ':t2': 'log', ':name': dsName})
    files = []
    for tmpLFN, in fo:
        if tmpLFN not in files_rucio:
            files.append(tmpLFN)
    print('')
    print('found {0} lost files -> {1}'.format(len(files), ','.join(files)))

s,jediTaskID = taskBuffer.resetFileStatusInJEDI('',True,options.ds,files,[],options.dryRun)
if options.dryRun:
    sys.exit(0)
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
                    print('resurrect {0}'.format(datasetName))
                    rc.resurrect([{'scope': scope, 'name': name}])
                    try:
                        rc.set_metadata(scope, name, 'lifetime', None)
                    except Exception:
                        pass
    print(Client.retryTask(jediTaskID, noChildRetry=options.noChildRetry)[-1][-1])
    print('done for jediTaskID={0}'.format(jediTaskID))
else:
    print('failed')
