"""
proxy for database connection

"""
import re
import warnings

from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.config import panda_config
from pandaserver.taskbuffer import OraDBProxy

warnings.filterwarnings("ignore")

# logger
_logger = PandaLogger().getLogger("EiDBProxy")


# proxy
class EiDBProxy(OraDBProxy.DBProxy):
    # constructor
    def __init__(self, useOtherError=False):
        OraDBProxy.DBProxy.__init__(self, useOtherError)

    # connect to DB (just for INTR)
    def connect(
        self,
        dbhost=panda_config.ei_dbhost,
        dbpasswd=panda_config.ei_dbpasswd,
        dbuser=panda_config.ei_dbuser,
        dbname=panda_config.ei_dbname,
        dbtimeout=panda_config.ei_dbtimeout,
        reconnect=False,
    ):
        return OraDBProxy.DBProxy.connect(
            self,
            dbhost=dbhost,
            dbpasswd=dbpasswd,
            dbuser=dbuser,
            dbname=dbname,
            dbtimeout=dbtimeout,
            reconnect=reconnect,
        )

    # get index of AMI tag
    def getIndexAmiTag(self, tagList, amiTag):
        for idxTag, tagPattern in enumerate(tagList):
            if re.search("^" + tagPattern + "$", amiTag) is not None:
                return idxTag
        return None

    # get GUIDs from EventIndex
    def getGUIDsFromEventIndex(self, runEventList, streamName, amiTags, dataType):
        comment = " /* DBProxy.getGUIDsFromEventIndex */"
        methodName = comment.split(" ")[-2].split(".")[-1]
        tmpLog = LogWrapper(
            _logger,
            methodName + f" <streamName={streamName} amiTags={amiTags} dataType={dataType}>",
        )
        try:
            # change to list
            if amiTags not in [None, ""]:
                amiTags = amiTags.replace("*", ".*").split(",")
            tmpLog.debug(f"start for {len(runEventList)} events")
            # check data type
            if dataType not in ["RAW", "ESD", "AOD"]:
                return False, f"dataType={dataType} is unsupported"
            # sql to insert runs and events
            sqlRE = f"INSERT INTO {panda_config.schemaEI}.TMP_RUN_EVENT_PAIRS (runNumber,eventNumber) "
            sqlRE += "VALUES (:runNumber,:eventNumber) "
            varMaps = []
            for runNumber, eventNumber in runEventList:
                varMap = {}
                varMap[":runNumber"] = runNumber
                varMap[":eventNumber"] = eventNumber
                varMaps.append(varMap)
            # begin transaction
            self.conn.begin()
            self.cur.arraysize = 100000
            # insert runs and events
            self.cur.executemany(sqlRE + comment, varMaps)
            # read GUIDs
            varMap = {}
            if amiTags in [None, ""]:
                sqlRG = f"SELECT runNumber,eventNumber,guid_{dataType} "
                sqlRG += f"FROM {panda_config.schemaEI}.V_PANDA_EVPICK_NOAMITAG_MANY "
            else:
                sqlRG = f"SELECT runNumber,eventNumber,guid_{dataType},amiTag "
                sqlRG += f"FROM {panda_config.schemaEI}.V_PANDA_EVPICK_AMITAG_MANY "
            if streamName not in [None, ""]:
                sqlRG += "WHERE streamName=:streamName "
                varMap[":streamName"] = streamName
            self.cur.execute(sqlRG + comment, varMap)
            resRG = self.cur.fetchall()
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            retValue = {}
            keyAmiIdxMap = {}
            for tmpItem in resRG:
                if amiTags in [None, ""]:
                    runNumber, eventNumber, guid = tmpItem
                    # dummy
                    idxTag = 0
                else:
                    runNumber, eventNumber, guid, amiTag = tmpItem
                    # get index number for the AMI tag in the list
                    idxTag = self.getIndexAmiTag(amiTags, amiTag)
                    # didn't match
                    if idxTag is None:
                        continue
                tmpKey = (runNumber, eventNumber)
                # use AMI tag in a preference orde
                if tmpKey in keyAmiIdxMap and keyAmiIdxMap[tmpKey] < idxTag:
                    continue
                keyAmiIdxMap[tmpKey] = idxTag
                retValue[tmpKey] = [guid]
            tmpLog.debug(f"found {len(retValue)} events")
            return True, retValue
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dumpErrorMessage(_logger, methodName)
            return False, None
