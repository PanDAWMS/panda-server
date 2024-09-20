#! /usr/bin/env python
#
# Dump schedconfig on a per-queue basis into cache files
#
#

import json
import os
import shutil
import sys
import traceback
from argparse import ArgumentParser
from copy import deepcopy
from pathlib import Path

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.thread_utils import GenericThread

from pandaserver.config import panda_config

# logger
_logger = PandaLogger().getLogger("cache_schedconfig")

# default destination directory
default_dest_dir = getattr(panda_config, "schedconfig_cache_dir", "/var/cache/pandaserver/schedconfig")


class cacheSchedConfig:
    """
    Class to dump schedconfig on a per-queue basis into cache files
    """

    def __init__(self, tbuf):
        self.tbuf = tbuf
        self.queueData = None
        self.cloudStatus = None
        # Define this here, but could be more flexible...
        self.queueDataFields = {
            # Note that json dumps always use sort_keys=True; for pilot format
            # the order defined here is respected
            "pilot": [
                "appdir",
                "allowdirectaccess",
                "cloud",
                "datadir",
                "dq2url",
                "copytool",
                "copytoolin",
                "copysetup",
                "copysetupin",
                "ddm",
                "se",
                "sepath",
                "seprodpath",
                "envsetup",
                "envsetupin",
                "region",
                "copyprefix",
                "copyprefixin",
                "lfcpath",
                "lfcprodpath",
                "lfchost",
                "lfcregister",
                "sein",
                "wntmpdir",
                "proxy",
                "retry",
                "recoverdir",
                "space",
                "memory",
                "cmtconfig",
                "status",
                "setokens",
                "glexec",
                "seopt",
                "gatekeeper",
                "pcache",
                "maxinputsize",
                "timefloor",
                "corecount",
                "faxredirector",
                "allowfax",
                "maxtime",
                "maxwdir",
            ],
            "factory": [
                "site",
                "siteid",
                "nickname",
                "cloud",
                "status",
                "jdl",
                "queue",
                "localqueue",
                "nqueue",
                "environ",
                "proxy",
                "glexec",
                "depthboost",
                "idlepilotsupression",
                "pilotlimit",
                "transferringlimit",
                "memory",
                "maxtime",
                "system",
                "fairsharepolicy",
                "autosetup_pre",
                "autosetup_post",
            ],
            # None is magic here and really means "all"
            "all": None,
        }

    def query_column_sql(self, sql, varMap=None, arraySize=100):
        res = self.tbuf.querySQL(sql, varMap, arraySize=arraySize)
        retList = []
        for (
            panda_queue,
            data,
        ) in res:
            if isinstance(data, str):
                dictData = json.loads(data)
            elif isinstance(data, dict):
                dictData = data
            else:
                dictData = json.loads(data.read())
            dictData["siteid"] = panda_queue
            retList.append(dictData)
        return retList

    def getQueueData(self, site=None, queue=None):
        # Dump schedconfig in a single query (it's not very big)
        varDict = {}
        sql = f"SELECT panda_queue, data from {panda_config.schemaPANDA}.SCHEDCONFIG_JSON"
        if site:
            sql += " where panda_queue=:site"
            varDict[":site"] = site
            self.queueData = self.query_column_sql(sql, varDict)
        elif queue:
            sql += " where panda_queue=:queue"
            varDict[":queue"] = queue
            self.queueData = self.query_column_sql(sql, varDict)
        else:
            self.queueData = self.query_column_sql(sql)

    def dumpSingleQueue(self, queueDict, dest="/tmp", outputSet="all", format="txt"):
        try:
            file = os.path.join(dest, queueDict["nickname"] + "." + outputSet + "." + format)
            output = open(file, "w")
            outputFields = self.queueDataFields[outputSet]
            if outputFields is None:
                outputFields = queueDict.keys()
            if format == "txt":
                for outputField in outputFields:
                    output.write(outputField + "=" + str(queueDict[outputField]))
            if format == "pilot":
                outputStr = ""
                for outputField in outputFields:
                    if outputField in queueDict and queueDict[outputField]:
                        outputStr += outputField + "=" + str(queueDict[outputField]) + "|"
                    else:
                        outputStr += outputField + "=|"
                output.write(outputStr[:-1])
            if format == "json":
                dumpMe = {}
                for outputField in outputFields:
                    if outputField in queueDict:
                        val = queueDict[outputField]
                    else:
                        val = ""
                    dumpMe[outputField] = val
                json.dump(self.queueDictPythonise(dumpMe), output, sort_keys=True, indent=4)
            output.close()
            # a copy of the file, when makes sense, with filename based on siteid
            newfile = os.path.join(dest, queueDict["siteid"] + "." + outputSet + "." + format)
            if newfile != file:
                shutil.copy(file, newfile)
        except Exception:
            raise

    def queueDictPythonise(self, queueDict, deepCopy=True):
        """Turn queue dictionary with SQL text fields into a more stuctured python representation"""
        if deepCopy:
            structDict = deepcopy(queueDict)
        else:
            structDict = queueDict

        if "releases" in structDict and structDict["releases"] is not None:
            if isinstance(structDict["releases"], str):
                structDict["releases"] = structDict["releases"].split("|")

        for timeKey in "lastmod", "tspace":
            if timeKey in structDict:
                structDict[timeKey] = structDict[timeKey].isoformat()
        return structDict

    def dumpAllSchedConfig(self, queueArray=None, dest="/tmp"):
        """Dumps all of schedconfig into a single json file - allows clients to retrieve a
        machine readable version of schedconfig efficiently"""
        file = os.path.join(dest, "schedconfig.all.json")
        if queueArray is None:
            queueArray = self.queueData
        output = open(file, "w")
        dumpMe = {}
        for queueDict in queueArray:
            dumpMe[queueDict["nickname"]] = {}
            for k in queueDict:
                v = queueDict[k]
                dumpMe[queueDict["nickname"]][k] = v
            dumpMe[queueDict["nickname"]] = self.queueDictPythonise(dumpMe[queueDict["nickname"]])
        json.dump(dumpMe, output, sort_keys=True, indent=4)
        self.dump_pilot_gdp_config(dest)

    def dump_pilot_gdp_config(self, dest="/tmp"):
        app = "pilot"
        dump_me = {}
        sql = f"SELECT key, component, vo from {panda_config.schemaPANDA}.config where app=:app"
        r = self.tbuf.querySQL(sql, {":app": app})
        for key, component, vo in r:
            dump_me.setdefault(vo, {})
            value = self.tbuf.getConfigValue(component, key, app, vo)
            dump_me[vo][key] = value
        # dump
        _logger.debug(f"pilot GDP config: {str(dump_me)}")
        with open(os.path.join(dest, "pilot_gdp_config.json"), "w") as f:
            json.dump(dump_me, f, sort_keys=True, indent=4)


def main(argv=tuple(), tbuf=None, **kwargs):
    _logger.debug("start")
    try:
        # parse arguments
        parser = ArgumentParser()
        parser.add_argument(
            "-o",
            "--output",
            dest="dirname",
            default=default_dest_dir,
            help="write cache outputs to DIR",
            metavar="DIR",
        )
        args = parser.parse_args(list(argv)[1:])
        # ensure destination directory
        dest_dir_path = Path(args.dirname)
        dest_dir_path.mkdir(mode=0o755, exist_ok=True)
        # instantiate TB
        requester_id = GenericThread().get_full_id(__name__, sys.modules[__name__].__file__)
        if tbuf is None:
            from pandaserver.taskbuffer.TaskBuffer import taskBuffer

            taskBuffer.init(
                panda_config.dbhost,
                panda_config.dbpasswd,
                nDBConnection=1,
                useTimeout=True,
                requester=requester_id,
            )
        else:
            taskBuffer = tbuf

        # initialize
        cacher = cacheSchedConfig(tbuf=taskBuffer)
        cacher.getQueueData()

        # dump
        for queue in cacher.queueData:
            cacher.dumpSingleQueue(queue, dest=args.dirname, outputSet="pilot", format="pilot")
            cacher.dumpSingleQueue(queue, dest=args.dirname, outputSet="pilot", format="json")
            cacher.dumpSingleQueue(queue, dest=args.dirname, outputSet="all", format="json")
            cacher.dumpSingleQueue(queue, dest=args.dirname, outputSet="factory", format="json")
        _logger.debug("dumped json files for each queue")
        # Big dumper
        cacher.dumpAllSchedConfig(dest=args.dirname)
        _logger.debug("dumped schedconfig.all.json")
        # stop taskBuffer if created inside this script
        if tbuf is None:
            taskBuffer.cleanup(requester=requester_id)
    except Exception as e:
        err_str = traceback.format_exc()
        _logger.error(f"failed to copy files: {err_str}")
    # done
    _logger.debug("done")


if __name__ == "__main__":
    main(argv=sys.argv)
