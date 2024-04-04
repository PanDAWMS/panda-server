import argparse
import sys

from pandacommon.pandautils.thread_utils import GenericThread
from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.userinterface import Client
from rucio.client import Client as RucioClient
from rucio.common.exception import DataIdentifierNotFound


# get files form rucio
def get_files_from_rucio(ds_name, log_stream):
    # get files from rucio
    try:
        rc = RucioClient()
        scope, name = rucioAPI.extract_scope(ds_name)
        if name.endswith("/"):
            name = name[:-1]
        files_rucio = set()
        for i in rc.list_files(scope, name):
            files_rucio.add(i["name"])
        return True, files_rucio
    except DataIdentifierNotFound:
        return False, f"unknown dataset {ds_name}"
    except Exception as e:
        msgStr = f"failed to get files from rucio: {str(e)}"
        return None, msgStr


# main
def main(taskBuffer=None, exec_options=None, log_stream=None, args_list=None):
    # options
    parser = argparse.ArgumentParser()
    if taskBuffer:
        parser.add_argument("--ds", action="store", dest="ds", default=None, help="dataset name")
    else:
        parser.add_argument(
            "--ds",
            action="store",
            dest="ds",
            default=None,
            required=True,
            help="dataset name",
        )
    parser.add_argument(
        "--files",
        action="store",
        dest="files",
        default=None,
        help="comma-separated list of lost file names. The list is dedeuced if this option is omitted",
    )
    parser.add_argument(
        "--noChildRetry",
        action="store_const",
        const=True,
        dest="noChildRetry",
        default=False,
        help="not retry child tasks",
    )
    parser.add_argument(
        "--resurrectDS",
        action="store_const",
        const=True,
        dest="resurrectDS",
        default=False,
        help="resurrect output and log datasets if they were already deleted",
    )
    parser.add_argument(
        "--dryRun",
        action="store_const",
        const=True,
        dest="dryRun",
        default=False,
        help="dry run",
    )
    parser.add_argument(
        "--force",
        action="store_const",
        const=True,
        dest="force",
        default=False,
        help="force retry even if no lost files",
    )
    parser.add_argument(
        "--reproduceParent",
        action="store_const",
        const=True,
        dest="reproduceParent",
        default=False,
        help="reproduce the input files from which the lost files were produced. "
        "Typically useful to recover merged files when unmerged files were already deleted",
    )
    parser.add_argument(
        "--reproduceUptoNthGen",
        action="store",
        dest="reproduceUptoNthGen",
        type=int,
        default=0,
        help="reproduce files up to N-th upper generation. --reproduceUptoNthGen=1 corresponds "
        "to --reproduceParent. 0 by default so that any provenance files are not reproduced",
    )
    # parse options
    if taskBuffer:
        if args_list:
            options = parser.parse_args(args_list)
        else:
            options, unknown = parser.parse_known_args()
    else:
        if args_list:
            options = parser.parse_args(args_list)
        else:
            options = parser.parse_args()

    # executed via command-line
    givenTaskID = None
    dn = None

    requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)

    if taskBuffer is None:
        # instantiate TB
        from pandaserver.taskbuffer.TaskBuffer import taskBuffer

        taskBuffer.init(
            panda_config.dbhost,
            panda_config.dbpasswd,
            nDBConnection=1,
            requester=requester_id,
        )

    else:
        # set options from dict
        if exec_options is None:
            exec_options = {}
        keys = set(vars(options).keys())
        for k in exec_options:
            if k in keys:
                setattr(options, k, exec_options[k])
        if "jediTaskID" in exec_options:
            givenTaskID = exec_options["jediTaskID"]
        if "userName" in exec_options:
            dn = exec_options["userName"]

    if options.reproduceUptoNthGen > 0:
        options.reproduceUptoNthGen -= 1
        options.reproduceParent = True

    ds_files = {}
    if options.files is not None:
        files = options.files.split(",")
        ds_files[options.ds] = files
    else:
        # look for lost files
        if not givenTaskID:
            # get files from rucio
            st, files_rucio = get_files_from_rucio(options.ds, log_stream)
            if st is not True:
                return st, files_rucio
            # get files from panda
            dsName = options.ds.split(":")[-1]
            fd, fo = taskBuffer.querySQLS(
                "SELECT c.lfn FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.JEDI_Dataset_Contents c "
                "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND "
                "d.type IN (:t1,:t2) AND c.status=:s AND d.datasetName=:name ",
                {":s": "finished", ":t1": "output", ":t2": "log", ":name": dsName},
            )
            for (tmpLFN,) in fo:
                if tmpLFN not in files_rucio:
                    ds_files.setdefault(options.ds, [])
                    ds_files[options.ds].append(tmpLFN)
            # get taskID
            td, to = taskBuffer.querySQLS(
                "SELECT jediTaskID FROM ATLAS_PANDA.JEDI_Datasets " "WHERE datasetName=:datasetName AND type IN (:t1,:t2) ",
                {":t1": "output", ":t2": "log", ":datasetName": dsName},
            )
            (jediTaskID,) = to[0]
        else:
            # get dataset names
            dd, do = taskBuffer.querySQLS(
                "SELECT datasetName FROM ATLAS_PANDA.JEDI_Datasets " "WHERE jediTaskID=:jediTaskID AND type IN (:t1,:t2) ",
                {":t1": "output", ":t2": "log", ":jediTaskID": givenTaskID},
            )
            # get files from rucio
            files_rucio = set()
            for (tmpDS,) in do:
                st, tmp_files_rucio = get_files_from_rucio(tmpDS, log_stream)
                if st is None:
                    return st, tmp_files_rucio
                # ignore unknown dataset
                if st:
                    files_rucio = files_rucio.union(tmp_files_rucio)
            # get files from rucio
            fd, fo = taskBuffer.querySQLS(
                "SELECT d.datasetName,c.lfn FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.JEDI_Dataset_Contents c "
                "WHERE d.jediTaskID=:jediTaskID AND c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND "
                "d.type IN (:t1,:t2) AND c.status=:s ",
                {
                    ":s": "finished",
                    ":t1": "output",
                    ":t2": "log",
                    ":jediTaskID": givenTaskID,
                },
            )
            for tmpDS, tmpLFN in fo:
                if tmpLFN not in files_rucio:
                    ds_files.setdefault(tmpDS, [])
                    ds_files[tmpDS].append(tmpLFN)
        for tmpDS in ds_files:
            files = ds_files[tmpDS]
            msgStr = f"{tmpDS} has {len(files)} lost files -> {','.join(files)}"
            if log_stream:
                log_stream.info(msgStr)
            else:
                print(msgStr)

    # no lost files
    if not ds_files and not options.force:
        return True, "No lost files. Use --force to ignore this check"

    # reset file status
    s = False
    for tmpDS in ds_files:
        files = ds_files[tmpDS]
        if dn:
            ts, jediTaskID, lostInputFiles = taskBuffer.resetFileStatusInJEDI(dn, False, tmpDS, files, options.reproduceParent, options.dryRun)
        else:
            ts, jediTaskID, lostInputFiles = taskBuffer.resetFileStatusInJEDI("", True, tmpDS, files, options.reproduceParent, options.dryRun)
        msgStr = f"reset file status for {tmpDS} in the DB: done with {ts} for jediTaskID={jediTaskID}"
        if log_stream:
            log_stream.info(msgStr)
        else:
            print(msgStr)
        s |= ts
        # recover provenance
        if options.reproduceParent:
            # reproduce input
            for lostDS in lostInputFiles:
                com_args = ["--ds", lostDS, "--noChildRetry", "--resurrectDS"]
                if options.reproduceUptoNthGen > 0:
                    com_args += [
                        "--reproduceUptoNthGen",
                        str(options.reproduceUptoNthGen),
                    ]
                if options.dryRun:
                    com_args.append("--dryRun")
                com_args += ["--files", ",".join(lostInputFiles[lostDS])]
            main(taskBuffer=taskBuffer, log_stream=log_stream, args_list=com_args)

    # go ahead
    if options.dryRun:
        return True, f"Done in the dry-run mode with {s}"
    if s or options.force:
        if options.resurrectDS:
            sd, so = taskBuffer.querySQLS(
                "SELECT datasetName FROM ATLAS_PANDA.JEDI_Datasets WHERE jediTaskID=:id AND type IN (:t1,:t2)",
                {":id": jediTaskID, ":t1": "output", ":t2": "log"},
            )
            rc = RucioClient()
            for (datasetName,) in so:
                for i in range(3):
                    try:
                        scope, name = rucioAPI.extract_scope(datasetName)
                        rc.get_did(scope, name)
                        break
                    except DataIdentifierNotFound:
                        print(f"resurrect {datasetName}")
                        rc.resurrect([{"scope": scope, "name": name}])
                        try:
                            rc.set_metadata(scope, name, "lifetime", None)
                        except Exception:
                            pass
        if not options.reproduceParent:
            msgStr = Client.retryTask(jediTaskID, noChildRetry=options.noChildRetry)[-1][-1]
        else:
            msgStr = Client.reloadInput(jediTaskID)[-1][-1]
        if log_stream:
            log_stream.info(f"Retried task {jediTaskID} with {msgStr}")
            log_stream.info("Done")
        else:
            print(f"Retried task {jediTaskID}: done with {msgStr}")
        return True, msgStr
    else:
        msgStr = "failed"
        if log_stream:
            log_stream.error(msgStr)
        else:
            print(msgStr)
        return False, msgStr
