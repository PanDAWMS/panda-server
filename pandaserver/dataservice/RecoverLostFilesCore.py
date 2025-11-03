import argparse
import os
import sys

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.thread_utils import GenericThread
from rucio.client import Client as RucioClient
from rucio.common.exception import DataIdentifierNotFound

from pandaserver.config import panda_config
from pandaserver.dataservice.ddm import rucioAPI
from pandaserver.userinterface import Client


# get files form rucio
def get_files_from_rucio(ds_name):
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
        msg_str = f"failed to get files from rucio: {str(e)}"
        return None, msg_str


# print a message
def print_msg(message: str, log_stream: LogWrapper | None, is_error: bool = False, put_log: str | None = None):
    """
    Print a message to log stream or stdout.

    :param message: Message string.
    :param log_stream: LogWrapper object or None.
    :param is_error: If True, log as error.
    :param put_log: Log filename to dump log to this file in cache_dir.
    """
    if log_stream:
        if is_error:
            log_stream.error("Error: " + message)
        else:
            log_stream.info(message)
        if put_log:
            with open(os.path.join(panda_config.cache_dir, put_log), "w") as f:
                f.write(log_stream.dumpToString())
                f.write("done\n")
    else:
        if is_error:
            print("ERROR: ", message)
        else:
            print(message)


# main
def main(taskBuffer=None, exec_options=None, log_stream=None, args_list=None):
    # options
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ds",
        action="store",
        dest="ds",
        default=None,
        help="dataset name",
    )
    parser.add_argument(
        "--files",
        action="store",
        dest="files",
        default=None,
        help="comma-separated list of lost file names. The list is deduced if this option is omitted",
    )
    parser.add_argument(
        "--jediTaskID",
        action="store",
        dest="jediTaskID",
        type=int,
        default=None,
        help="JEDI task ID that produced the dataset",
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

    # set options from dict
    if exec_options is None:
        exec_options = {}
    keys = set(vars(options).keys())
    for k in exec_options:
        if k in keys:
            setattr(options, k, exec_options[k])

    dn = exec_options.get("userName")
    is_production_manager = exec_options.get("isProductionManager", False)
    log_filename = exec_options.get("logFilename")
    print_msg(f"User='{dn}' is_prod={is_production_manager} log_name={log_filename}", log_stream)

    if options.reproduceUptoNthGen > 0:
        options.reproduceUptoNthGen -= 1
        options.reproduceParent = True

    # check if dataset name or taskID is given
    if options.jediTaskID is None and options.ds is None:
        msg_str = "Dataset name or jediTaskID is required"
        print_msg(msg_str, log_stream, is_error=True, put_log=log_filename)
        return False, msg_str

    ds_files = {}
    if options.files is not None:
        files = options.files.split(",")
        ds_files[options.ds] = files
    else:
        # look for lost files
        if not options.jediTaskID:
            # get files from rucio
            st, files_rucio = get_files_from_rucio(options.ds)
            if st is not True:
                print_msg(files_rucio, log_stream, is_error=True, put_log=log_filename)
                return st, files_rucio
            # get files from panda
            dsName = options.ds.split(":")[-1]
            fd, fo = taskBuffer.querySQLS(
                "SELECT c.lfn FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.JEDI_Dataset_Contents c "
                "WHERE c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND "
                "d.type=:t1 AND c.status=:s AND d.datasetName=:name ",
                {":s": "finished", ":t1": "output", ":name": dsName},
            )
            for (tmpLFN,) in fo:
                if tmpLFN not in files_rucio:
                    ds_files.setdefault(options.ds, [])
                    ds_files[options.ds].append(tmpLFN)
            # get taskID
            td, to = taskBuffer.querySQLS(
                "SELECT jediTaskID FROM ATLAS_PANDA.JEDI_Datasets " "WHERE datasetName=:datasetName AND type=:t1 ",
                {":t1": "output", ":datasetName": dsName},
            )
            (jediTaskID,) = to[0]
        else:
            # get dataset names
            dd, do = taskBuffer.querySQLS(
                "SELECT datasetName FROM ATLAS_PANDA.JEDI_Datasets " "WHERE jediTaskID=:jediTaskID AND type=:t1 ",
                {":t1": "output", ":jediTaskID": options.jediTaskID},
            )
            # get files from rucio
            files_rucio = set()
            for (tmpDS,) in do:
                st, tmp_files_rucio = get_files_from_rucio(tmpDS)
                if st is None:
                    print_msg(tmp_files_rucio, log_stream, is_error=True, put_log=log_filename)
                    return st, tmp_files_rucio
                # ignore unknown dataset
                if st:
                    files_rucio = files_rucio.union(tmp_files_rucio)
            # get files from PanDA
            fd, fo = taskBuffer.querySQLS(
                "SELECT d.datasetName,c.lfn FROM ATLAS_PANDA.JEDI_Datasets d,ATLAS_PANDA.JEDI_Dataset_Contents c "
                "WHERE d.jediTaskID=:jediTaskID AND c.jediTaskID=d.jediTaskID AND c.datasetID=d.datasetID AND "
                "d.type=:t1 AND c.status=:s ",
                {
                    ":s": "finished",
                    ":t1": "output",
                    ":jediTaskID": options.jediTaskID,
                },
            )
            for tmpDS, tmpLFN in fo:
                if tmpLFN not in files_rucio:
                    ds_files.setdefault(tmpDS, [])
                    ds_files[tmpDS].append(tmpLFN)
        for tmpDS in ds_files:
            files = ds_files[tmpDS]
            msg_str = f"{tmpDS} has {len(files)} lost files -> {','.join(files)}"
            print_msg(msg_str, log_stream)

    # no lost files
    if not ds_files and not options.force:
        msg_str = "No lost files. Use --force to ignore this check"
        print_msg(msg_str, log_stream, put_log=log_filename)
        return True, msg_str

    # reset file status
    s = False
    for tmpDS in ds_files:
        files = ds_files[tmpDS]
        if not is_production_manager:
            ts, jediTaskID, lostInputFiles, error_message = taskBuffer.resetFileStatusInJEDI(dn, False, tmpDS, files, options.reproduceParent, options.dryRun)
        else:
            ts, jediTaskID, lostInputFiles, error_message = taskBuffer.resetFileStatusInJEDI("", True, tmpDS, files, options.reproduceParent, options.dryRun)
        if error_message:
            msg_str = f"Failed to reset file status in the DB since {error_message}"
            print_msg(msg_str, log_stream, is_error=True)
        else:
            msg_str = f"Reset file status for {tmpDS} in the DB: done with {ts} for jediTaskID={jediTaskID}"
            print_msg(msg_str, log_stream)
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
        msg_str = f"Done in the dry-run mode with {s}"
        print_msg(msg_str, log_stream, put_log=log_filename)
        return True, msg_str
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
                        msg_str = f"resurrect {datasetName}"
                        print_msg(msg_str, log_stream)
                        rc.resurrect([{"scope": scope, "name": name}])
                        try:
                            rc.set_metadata(scope, name, "lifetime", None)
                        except Exception:
                            pass
        if not options.reproduceParent:
            tmp_ret = Client.retry_task(jediTaskID, no_child_retry=options.noChildRetry)
        else:
            tmp_ret = Client.reload_input(jediTaskID)
        msg_str = f"Retried task {jediTaskID}: done with {tmp_ret}"
        print_msg(msg_str, log_stream, put_log=log_filename)
        return True, msg_str
    else:
        msg_str = "Failed"
        print_msg(msg_str, log_stream, is_error=True, put_log=log_filename)
        return False, msg_str
