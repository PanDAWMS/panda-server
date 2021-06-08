import sys
import argparse

from pandaserver.config import panda_config
from pandaserver.userinterface import Client
from rucio.client import Client as RucioClient
from rucio.common.exception import DataIdentifierNotFound
from pandaserver.dataservice.DDM import rucioAPI


# get files form rucio
def get_files_from_rucio(ds_name, log_stream):
    # get files from rucio
    try:
        rc = RucioClient()
        scope, name = rucioAPI.extract_scope(ds_name)
        if name.endswith('/'):
            name = name[:-1]
        files_rucio = set()
        for i in rc.list_files(scope, name):
            files_rucio.add(i['name'])
        return True, files_rucio
    except DataIdentifierNotFound:
        return False, "unknown dataset {}".format(ds_name)
    except Exception as e:
        msgStr = "failed to get files from rucio: {}".format(str(e))
        return None, msgStr


# main
def main(taskBuffer=None, exec_options=None, log_stream=None):
    # options
    parser = argparse.ArgumentParser()
    if taskBuffer:
        parser.add_argument('--ds',action='store',dest='ds',default=None,
                            help='dataset name')
    else:
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
    # parse options
    options = parser.parse_args()

    # executed via command-line
    givenTaskID = None
    dn = None
    if taskBuffer is None:
        # instantiate TB
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer
        taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

    else:
        # set options from dict
        if exec_options is None:
            exec_options = {}
        keys = set(vars(options).keys())
        for k in exec_options:
            if k in keys:
                setattr(options, k, exec_options[k])
        if 'jediTaskID' in exec_options:
            givenTaskID = exec_options['jediTaskID']
        if 'userName' in exec_options:
            dn = exec_options['userName']

    ds_files = {}
    if options.files is not None:
        files = options.files.split(',')
        ds_files[options.ds] = files
    else:
        # look for lost files
        if not givenTaskID:
            # get files from rucio
            st, files_rucio = get_files_from_rucio(options.ds, log_stream)
            if st is not True:
                return st, files_rucio
            # get files from panda
            dsName = options.ds.split(':')[-1]
            fd, fo = taskBuffer.querySQLS(
                'SELECT c.lfn FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.JEDI_Dataset_Contents c '
                'WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND '
                'd.type IN (:t1,:t2) AND c.status=:s AND d.datasetName=:name ',
                {':s': 'finished', ':t1': 'output', ':t2': 'log', ':name': dsName})
            for tmpLFN, in fo:
                if tmpLFN not in files_rucio:
                    ds_files.setdefault(options.ds, [])
                    ds_files[options.ds].append(tmpLFN)
        else:
            # get dataset names
            dd, do = taskBuffer.querySQLS(
                'SELECT datasetName FROM ATLAS_PANDA.JEDI_Datasets '
                'WHERE jediTaskID=:jediTaskID AND type IN (:t1,:t2) ',
                {':t1': 'output', ':t2': 'log', ':jediTaskID': givenTaskID})
            # get files from rucio
            files_rucio = set()
            for tmpDS, in do:
                st, tmp_files_rucio = get_files_from_rucio(tmpDS, log_stream)
                if st is None:
                    return st, tmp_files_rucio
                # ignore unknown dataset
                if st:
                    files_rucio = files_rucio.union(tmp_files_rucio)
            # get files from rucio
            fd, fo = taskBuffer.querySQLS(
                'SELECT d.datasetName,c.lfn FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.JEDI_Dataset_Contents c '
                'WHERE d.jediTaskID=:jediTaskID AND c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND '
                'd.type IN (:t1,:t2) AND c.status=:s ',
                {':s': 'finished', ':t1': 'output', ':t2': 'log', ':jediTaskID': givenTaskID})
            for tmpDS, tmpLFN in fo:
                if tmpLFN not in files_rucio:
                    ds_files.setdefault(tmpDS, [])
                    ds_files[tmpDS].append(tmpLFN)
        for tmpDS in ds_files:
            files = ds_files[tmpDS]
            msgStr = '{} has {} lost files -> {}'.format(tmpDS, len(files), ','.join(files))
            if log_stream:
                log_stream.debug(msgStr)
            else:
                print(msgStr)

    # no lost files
    if not ds_files:
        return True, "no lost files"

    # reset file status
    s = False
    for tmpDS in ds_files:
        files = ds_files[tmpDS]
        if dn:
            ts, jediTaskID = taskBuffer.resetFileStatusInJEDI(dn, False, tmpDS, files, [], options.dryRun)
        else:
            ts, jediTaskID = taskBuffer.resetFileStatusInJEDI('', True, tmpDS, files, [], options.dryRun)
        msgStr = 'reset file status for {} in the DB: done with {} for jediTaskID={}'.format(tmpDS, ts, jediTaskID)
        if log_stream:
            log_stream.debug(msgStr)
        else:
            print(msgStr)
        s |= ts

    # go ahead
    if options.dryRun:
        return True, 'done in the dry-run mode'
    if s:
        if options.resurrectDS:
            sd,so = taskBuffer.querySQLS(
                'SELECT datasetName FROM ATLAS_PANDA.JEDI_Datasets WHERE jediTaskID=:id AND type IN (:t1,:t2)',
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
        msgStr = Client.retryTask(jediTaskID, noChildRetry=options.noChildRetry)[-1][-1]
        if log_stream:
            log_stream.debug("retried task: done with {}".format(msgStr))
            log_stream.debug("done")
        else:
            print("retried task: done with {}".format(msgStr))
        return True, msgStr
    else:
        msgStr = 'failed'
        if log_stream:
            log_stream.error(msgStr)
        else:
            print(msgStr)
        return False, msgStr
