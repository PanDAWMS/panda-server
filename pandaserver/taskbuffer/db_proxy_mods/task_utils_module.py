import datetime
import json
import math
import random
import re
import sys
import traceback
import uuid
from statistics import mean

import numpy
from pandacommon.pandalogger.LogWrapper import LogWrapper
from pandacommon.pandautils.PandaUtils import (
    batched,
    get_sql_IN_bind_variables,
    naive_utcnow,
)

from pandaserver.config import panda_config
from pandaserver.srvcore import CoreUtils
from pandaserver.taskbuffer import EventServiceUtils, JobUtils
from pandaserver.taskbuffer.db_proxy_mods.base_module import BaseModule, varNUMBER
from pandaserver.taskbuffer.InputChunk import InputChunk
from pandaserver.taskbuffer.JediDatasetSpec import (
    INPUT_TYPES_var_map,
    INPUT_TYPES_var_str,
    JediDatasetSpec,
    MERGE_TYPES_var_map,
    MERGE_TYPES_var_str,
    PROCESS_TYPES_var_map,
    PROCESS_TYPES_var_str,
)
from pandaserver.taskbuffer.JediFileSpec import JediFileSpec
from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec, is_msg_driven
from pandaserver.taskbuffer.JobSpec import JobSpec, get_task_queued_time


# Module class to define task related methods that are used by TaskComplex methods
class TaskUtilsModule(BaseModule):
    # constructor
    def __init__(self, log_stream: LogWrapper):
        super().__init__(log_stream)

    # check if item is matched with one of list items
    def isMatched(self, itemName, pattList):
        for tmpName in pattList:
            # normal pattern
            if re.search(tmpName, itemName) is not None or tmpName == itemName:
                return True
        # return
        return False

    # fix associated files in staging
    def fix_associated_files_in_staging(self, jeditaskid, primary_id=None, secondary_id=None):
        comment = " /* JediDBProxy.fix_associated_files_in_staging */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jeditaskid}")
        tmpLog.debug("start")
        # get primary dataset
        if primary_id is None:
            sqlGD = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type=:type AND masterID IS NULL "
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type"] = "input"
            self.cur.execute(sqlGD + comment, varMap)
            resGD = self.cur.fetchone()
            if resGD is None:
                return
            (primary_id,) = resGD
        # get secondary dataset
        if secondary_id is not None:
            secondary_id_list = [secondary_id]
        else:
            sqlGS = f"SELECT datasetID FROM {panda_config.schemaJEDI}.JEDI_Datasets WHERE jediTaskID=:jediTaskID AND type=:type AND masterID IS NOT NULL "
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type"] = "pseudo_input"
            self.cur.execute(sqlGS + comment, varMap)
            resGDA = self.cur.fetchall()
            secondary_id_list = [tmpID for tmpID, in resGDA]
        if len(secondary_id_list) == 0:
            return
        # get primary files
        sqlGP = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents  WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID ORDER BY lfn "
        varMap = dict()
        varMap[":jediTaskID"] = jeditaskid
        varMap[":datasetID"] = primary_id
        self.cur.execute(sqlGP + comment, varMap)
        resFP = self.cur.fetchall()
        primaryList = [status for status, in resFP]
        # sql to get secondary files
        sqlGS = ("SELECT fileID,status FROM {0}.JEDI_Dataset_Contents " " WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID " "ORDER BY fileID ").format(
            panda_config.schemaJEDI
        )
        # sql to update files
        sqlUS = (
            "UPDATE {0}.JEDI_Dataset_Contents "
            "SET status=:new_status "
            "WHERE jediTaskID=:jediTaskID "
            "AND datasetID=:datasetID "
            "AND fileID=:fileID "
            "AND status=:old_status "
        ).format(panda_config.schemaJEDI)
        # sql to update dataset
        sqlUD = (
            "UPDATE {0}.JEDI_Datasets "
            "SET nFilesToBeUsed="
            "(SELECT COUNT(*) FROM {0}.JEDI_Dataset_Contents "
            "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND status<>:status) "
            "WHERE jediTaskID=:jediTaskID "
            "AND datasetID=:datasetID "
        ).format(panda_config.schemaJEDI)
        # loop over secondary datasets
        for secondaryID in secondary_id_list:
            # get secondary files
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":datasetID"] = secondaryID
            self.cur.execute(sqlGS + comment, varMap)
            resFS = self.cur.fetchall()
            # check files
            n = 0
            for priStatus, (secFileID, secStatus) in zip(primaryList, resFS):
                if priStatus != "staging" and secStatus == "staging":
                    # update files
                    varMap = dict()
                    varMap[":jediTaskID"] = jeditaskid
                    varMap[":datasetID"] = secondaryID
                    varMap[":fileID"] = secFileID
                    varMap[":old_status"] = "staging"
                    varMap[":new_status"] = "ready"
                    self.cur.execute(sqlUS + comment, varMap)
                    n += self.cur.rowcount
            # update dataset
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":datasetID"] = secondaryID
            varMap[":status"] = "staging"
            self.cur.execute(sqlUD + comment, varMap)
            tmpLog.debug(f"updated {n} files for datasetID={secondaryID}")

    # enable jumbo jobs in a task
    def enableJumboInTask_JEDI(self, jediTaskID, eventService, site, useJumbo, splitRule):
        comment = " /* JediDBProxy.enableJumboInTask_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug(f"eventService={eventService} site={site} useJumbo={useJumbo}")
        if eventService == 1 and site is None and useJumbo is None:
            taskSpec = JediTaskSpec()
            taskSpec.splitRule = splitRule
            # go to scouting
            if taskSpec.useScout() and not taskSpec.isPostScout():
                return
            # check if should enable jumbo
            toEnable = self.toEnableJumbo_JEDI(jediTaskID)
            if not toEnable:
                return
            # get nJumbo jobs
            sqlLK = f"SELECT value, type FROM {panda_config.schemaPANDA}.CONFIG "
            sqlLK += "WHERE component=:component AND key=:key AND app=:app "
            varMap = dict()
            varMap[":component"] = "taskrefiner"
            varMap[":app"] = "jedi"
            varMap[":key"] = "AES_NUM_JUMBO_PER_TASK"
            self.cur.execute(sqlLK + comment, varMap)
            resLK = self.cur.fetchone()
            try:
                (nJumboJobs,) = resLK
                nJumboJobs = int(nJumboJobs)
            except Exception:
                nJumboJobs = 1
            # enable jumbo
            # self.enableJumboJobs(jediTaskID, nJumboJobs, False, False)

    # check if should enable jumbo
    def toEnableJumbo_JEDI(self, jediTaskID):
        comment = " /* JediDBProxy.toEnableJumbo_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        try:
            # sql to get thresholds
            sqlLK = f"SELECT value FROM {panda_config.schemaPANDA}.CONFIG "
            sqlLK += "WHERE component=:component AND key=:key AND app=:app "
            # sql to get nevents
            sqlAV = f"SELECT nEvents,nFilesToBeUsed,nFilesUsed FROM {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlAV += "WHERE jediTaskID=:jediTaskID "
            sqlAV += f"AND type IN ({INPUT_TYPES_var_str}) "
            sqlAV += "AND masterID IS NULL "
            # sql to get # of active jumbo jobs
            sqlAJ = "SELECT COUNT(*) "
            sqlAJ += "FROM {0}.JEDI_Tasks tabT,{0}.JEDI_AUX_Status_MinTaskID tabA ".format(panda_config.schemaJEDI)
            sqlAJ += "WHERE tabT.status=tabA.status AND tabT.jediTaskID>=tabA.min_jediTaskID "
            sqlAJ += "AND tabT.eventService=:eventService AND tabT.useJumbo IS NOT NULL AND tabT.useJumbo<>:useJumbo "
            sqlAJ += "AND tabT.site IS NULL AND tabT.status IN (:st1,:st2,:st3) "
            # get thresholds
            configMaxJumbo = "AES_MAX_NUM_JUMBO_TASKS"
            varMap = dict()
            varMap[":component"] = "taskrefiner"
            varMap[":app"] = "jedi"
            varMap[":key"] = configMaxJumbo
            self.cur.execute(sqlLK + comment, varMap)
            resLK = self.cur.fetchone()
            if resLK is None:
                tmpLog.debug(f"False since {configMaxJumbo} is not defined")
                return False
            try:
                (maxJumbo,) = resLK
            except Exception:
                tmpLog.debug(f"False since {configMaxJumbo} is not an int")
                return False
            varMap = dict()
            varMap[":component"] = "taskrefiner"
            varMap[":app"] = "jedi"
            varMap[":key"] = "AES_MIN_EVENTS_PER_JUMBO_TASK"
            self.cur.execute(sqlLK + comment, varMap)
            resLK = self.cur.fetchone()
            try:
                (minEvents,) = resLK
                minEvents = int(minEvents)
            except Exception:
                minEvents = 100 * 1000 * 1000
            # get nevents
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap.update(INPUT_TYPES_var_map)
            self.cur.execute(sqlAV + comment, varMap)
            resAV = self.cur.fetchone()
            if resAV is None:
                tmpLog.debug("False since cannot get nEvents")
                return False
            nEvents, nFilesToBeUsed, nFilesUsed = resAV
            try:
                nEvents = nEvents * nFilesToBeUsed // (nFilesToBeUsed - nFilesUsed)
            except Exception:
                tmpLog.debug(f"False since cannot get effective nEvents from nEvents={nEvents} nFilesToBeUsed={nFilesToBeUsed} nFilesUsed={nFilesUsed}")
                return False
            if nEvents < minEvents:
                tmpLog.debug(f"False since effective nEvents={nEvents} < minEventsJumbo={minEvents}")
                return False
            # get num jombo tasks
            varMap = dict()
            varMap[":eventService"] = 1
            varMap[":useJumbo"] = "D"
            varMap[":st1"] = "ready"
            varMap[":st2"] = "pending"
            varMap[":st3"] = "running"
            self.cur.execute(sqlAJ + comment, varMap)
            resAJ = self.cur.fetchone()
            nJumbo = 0
            if resAJ is not None:
                (nJumbo,) = resAJ
            if nJumbo > maxJumbo:
                tmpLog.debug(f"False since nJumbo={nJumbo} > maxJumbo={maxJumbo}")
                return False
            tmpLog.debug("True since nJumbo={0} < maxJumbo={1} and nEvents={0} > minEventsJumbo={1}".format(nJumbo, maxJumbo, nEvents, minEvents))
            return True
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # get scout job data
    def getScoutJobData_JEDI(
        self,
        jediTaskID,
        useTransaction=False,
        scoutSuccessRate=None,
        mergeScout=False,
        flagJob=False,
        setPandaID=None,
        site_mapper=None,
        task_spec=None,
        task_params_map=None,
    ):
        comment = " /* JediDBProxy.getScoutJobData_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug(f"start mergeScout={mergeScout}")
        returnMap = {}
        extraInfo = {}

        # get percentile rank and margin for memory
        ramCountRank = self.getConfigValue("dbproxy", "SCOUT_RAMCOUNT_RANK", "jedi")
        if ramCountRank is None:
            ramCountRank = 75
        ramCountMargin = self.getConfigValue("dbproxy", "SCOUT_RAMCOUNT_MARGIN", "jedi")
        if ramCountMargin is None:
            ramCountMargin = 10
        # get percentile rank for cpuTime
        cpuTimeRank = self.getConfigValue("dbproxy", "SCOUT_CPUTIME_RANK", "jedi")
        if cpuTimeRank is None:
            cpuTimeRank = 95

        # sql to get preset values
        sqlGPV = (
            "SELECT prodSourceLabel, outDiskUnit, walltime, ramUnit, baseRamCount, workDiskCount, cpuEfficiency, baseWalltime, "
            "splitRule, memory_leak_core, memory_leak_x2 "
            f"FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            "WHERE jediTaskID = :jediTaskID "
        )

        # sql to get scout job data from JEDI
        sqlSCF = (
            "SELECT tabF.PandaID, tabF.fsize, tabF.startEvent, tabF.endEvent, tabF.nEvents, tabF.type "
            f"FROM {panda_config.schemaJEDI}.JEDI_Datasets tabD, {panda_config.schemaJEDI}.JEDI_Dataset_Contents tabF "
            "WHERE tabD.jediTaskID = tabF.jediTaskID "
            "AND tabD.jediTaskID = :jediTaskID "
            "AND tabF.status = :status "
            "AND tabD.datasetID = tabF.datasetID "
        )
        if not mergeScout:
            sqlSCF += f"AND tabF.type IN ({INPUT_TYPES_var_str}) "
        else:
            sqlSCF += f"AND tabD.type IN ({MERGE_TYPES_var_str}) "
        sqlSCF += "AND tabD.masterID IS NULL "
        if setPandaID is not None:
            sqlSCF += "AND tabF.PandaID=:usePandaID "

        # sql to check scout success rate
        sqlCSSR = "SELECT COUNT(*),SUM(is_finished),SUM(is_failed) FROM "
        sqlCSSR += (
            "(SELECT DISTINCT tabF.PandaID,CASE WHEN tabF.status='finished' THEN 1 ELSE 0 END is_finished,"
            "CASE WHEN tabF.status='ready' AND "
            "(tabF.maxAttempt<=tabF.attemptNr OR "
            "(tabF.maxfailure IS NOT NULL AND tabF.maxFailure<=tabF.failedAttempt)) THEN 1 ELSE 0 END "
            "is_failed "
        )
        sqlCSSR += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(panda_config.schemaJEDI)
        sqlCSSR += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.PandaID IS NOT NULL "
        sqlCSSR += "AND tabD.datasetID=tabF.datasetID "
        sqlCSSR += f"AND tabF.type IN ({INPUT_TYPES_var_str}) "
        sqlCSSR += "AND tabD.masterID IS NULL "
        sqlCSSR += ") tmp_sub "

        # sql to get normal scout job data from Panda
        sqlSCDN = (
            "SELECT eventService, jobsetID, PandaID, jobStatus, outputFileBytes, jobMetrics, cpuConsumptionTime, "
            "actualCoreCount, coreCount, startTime, endTime, computingSite, maxPSS, specialHandling, nEvents, "
            "totRBYTES, totWBYTES, inputFileBytes, memory_leak, memory_leak_x2, modificationhost "
            f"FROM {panda_config.schemaPANDA}.jobsArchived4 "
            "WHERE PandaID=:pandaID AND jobStatus=:jobStatus AND jediTaskID=:jediTaskID "
            "UNION "
            "SELECT eventService, jobsetID, PandaID, jobStatus, outputFileBytes, jobMetrics, cpuConsumptionTime, "
            "actualCoreCount, coreCount, startTime, endTime, computingSite, maxPSS, specialHandling, nEvents, "
            "totRBYTES, totWBYTES, inputFileBytes, memory_leak, memory_leak_x2, modificationhost "
            f"FROM {panda_config.schemaPANDAARCH}.jobsArchived "
            "WHERE PandaID=:pandaID AND jobStatus=:jobStatus AND jediTaskID=:jediTaskID "
            "AND modificationTime>(CURRENT_DATE-30) "
        )

        # sql to get ES scout job data from Panda
        sqlSCDE = (
            "SELECT eventService, jobsetID, PandaID, jobStatus, outputFileBytes, jobMetrics, cpuConsumptionTime, "
            "actualCoreCount, coreCount, startTime, endTime, computingSite, maxPSS, specialHandling, nEvents, "
            "totRBYTES, totWBYTES, inputFileBytes, memory_leak, memory_leak_x2, modificationhost "
            f"FROM {panda_config.schemaPANDA}.jobsArchived4 "
            "WHERE jobsetID=:pandaID AND jobStatus=:jobStatus AND jediTaskID=:jediTaskID "
            "UNION "
            "SELECT eventService, jobsetID, PandaID, jobStatus, outputFileBytes, jobMetrics, cpuConsumptionTime, "
            "actualCoreCount, coreCount, startTime, endTime, computingSite, maxPSS, specialHandling, nEvents, "
            "totRBYTES, totWBYTES, inputFileBytes, memory_leak, memory_leak_x2, modificationhost "
            f"FROM {panda_config.schemaPANDAARCH}.jobsArchived "
            "WHERE jobsetID=:pandaID AND jobStatus=:jobStatus AND jediTaskID=:jediTaskID "
            "AND modificationTime>(CURRENT_DATE-14) "
        )

        # get size of lib
        sqlLIB = "SELECT MAX(fsize) "
        sqlLIB += "FROM {0}.JEDI_Datasets tabD, {0}.JEDI_Dataset_Contents tabF WHERE ".format(panda_config.schemaJEDI)
        sqlLIB += "tabD.jediTaskID=tabF.jediTaskID AND tabD.jediTaskID=:jediTaskID AND tabF.status=:status AND "
        sqlLIB += "tabD.type=:type AND tabF.type=:type "

        # get core power
        sqlCore = f"SELECT /* use_json_type */ scj.data.corepower FROM {panda_config.schemaJEDI}.schedconfig_json scj "
        sqlCore += "WHERE panda_queue=:site "

        # get num of new jobs
        sqlNumJobs = f"SELECT SUM(nFiles),SUM(nFilesFinished),SUM(nFilesUsed) FROM {panda_config.schemaJEDI}.JEDI_Datasets "
        sqlNumJobs += "WHERE jediTaskID=:jediTaskID "
        sqlNumJobs += f"AND type IN ({INPUT_TYPES_var_str}) "
        sqlNumJobs += "AND masterID IS NULL "

        # get num of new jobs with event
        sql_num_jobs_event = (
            "SELECT SUM(n_events), SUM(CASE WHEN status='finished' THEN n_events ELSE 0 END) FROM ("
            "SELECT (CASE WHEN tabF.endEvent IS NULL THEN tabF.nEvents ELSE tabF.endEvent-tabF.startEvent+1 END) n_events,tabF.status "
            f"FROM {panda_config.schemaJEDI}.JEDI_Datasets tabD, "
            f"{panda_config.schemaJEDI}.JEDI_Dataset_Contents tabF "
            "WHERE tabD.jediTaskID=:jediTaskID AND tabF.jediTaskID=tabD.jediTaskID "
        )
        sql_num_jobs_event += f"AND tabF.datasetID=tabD.datasetID AND tabD.type IN ({INPUT_TYPES_var_str}) "
        sql_num_jobs_event += "AND tabD.masterID IS NULL) tmp_tab "

        if useTransaction:
            # begin transaction
            self.conn.begin()
        self.cur.arraysize = 100000

        # get preset values to the task
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        self.cur.execute(sqlGPV + comment, varMap)
        resGPV = self.cur.fetchone()
        if resGPV is not None:
            (
                prodSourceLabel,
                preOutDiskUnit,
                preWalltime,
                preRamUnit,
                preBaseRamCount,
                preWorkDiskCount,
                preCpuEfficiency,
                preBaseWalltime,
                splitRule,
                memory_leak_core,
                memory_leak_x2,
            ) = resGPV
        else:
            prodSourceLabel = None
            preOutDiskUnit = None
            preWalltime = 0
            preRamUnit = None
            preBaseRamCount = 0
            preWorkDiskCount = 0
            preCpuEfficiency = None
            preBaseWalltime = None
            splitRule = None
        if preOutDiskUnit is not None and preOutDiskUnit.endswith("PerEvent"):
            preOutputScaleWithEvents = True
        else:
            preOutputScaleWithEvents = False
        if preCpuEfficiency is None:
            preCpuEfficiency = 100
        if preBaseWalltime is None:
            preBaseWalltime = 0

        # don't use baseRamCount for pmerge
        if mergeScout:
            preBaseRamCount = 0

        # use original ramCount if available
        if task_params_map is not None:
            preCpuTime = task_params_map.get("cpuTime")
            preCpuTimeUnit = task_params_map.get("cpuTimeUnit")
            if preCpuTime and not preCpuTimeUnit:
                preCpuTimeUnit = "HS06sPerEvent"
            if mergeScout:
                preRamCount = task_params_map.get("mergeRamCount")
            else:
                preRamCount = task_params_map.get("ramCount")
            if preRamCount:
                preRamCount = int(preRamCount)
        else:
            preCpuTime = None
            preRamCount = None
            preCpuTimeUnit = None
        # get preCpuTime in sec
        try:
            if preCpuTime and preCpuTimeUnit and preCpuTimeUnit.startswith("m"):
                preCpuTime = float(preCpuTime) / 1000.0
        except Exception:
            pass
        extraInfo["oldCpuTime"] = preCpuTime
        extraInfo["oldRamCount"] = preRamCount

        # get minimum ram count
        minRamCount = self.getConfigValue("dbproxy", "SCOUT_RAMCOUNT_MIN", "jedi")

        # get limit for short jobs
        shortExecTime = self.getConfigValue("dbproxy", f"SCOUT_SHORT_EXECTIME_{prodSourceLabel}", "jedi")
        if shortExecTime is None:
            shortExecTime = 0

        # get limit for cpu-inefficient jobs
        lowCpuEff = self.getConfigValue("dbproxy", f"SCOUT_LOW_CPU_EFFICIENCY_{prodSourceLabel}", "jedi")
        if lowCpuEff is None:
            lowCpuEff = 0

        # cap on diskIO
        capOnDiskIO = self.getConfigValue("dbproxy", "SCOUT_DISK_IO_CAP", "jedi")
        extraInfo["shortExecTime"] = shortExecTime
        extraInfo["cpuEfficiencyCap"] = lowCpuEff

        # get the size of lib
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":type"] = "lib"
        varMap[":status"] = "finished"
        self.cur.execute(sqlLIB + comment, varMap)
        resLIB = self.cur.fetchone()
        libSize = None
        if resLIB is not None:
            try:
                (libSize,) = resLIB
                libSize /= 1024 * 1024
            except Exception:
                pass

        # get files
        varMap = {}
        varMap[":jediTaskID"] = jediTaskID
        varMap[":status"] = "finished"
        if not mergeScout:
            varMap.update(INPUT_TYPES_var_map)
        else:
            varMap.update(MERGE_TYPES_var_map)
        if setPandaID is not None:
            varMap[":usePandaID"] = setPandaID
        self.cur.execute(sqlSCF + comment, varMap)
        resList = self.cur.fetchall()
        # scout succeeded or not
        scoutSucceeded = True
        if not resList:
            scoutSucceeded = False
            tmpLog.debug("no scouts succeeded")
            extraInfo["successRate"] = 0
        elif not mergeScout and task_spec and task_spec.useScout():
            # check scout success rate
            varMap = {":jediTaskID": jediTaskID}
            varMap.update(INPUT_TYPES_var_map)
            self.cur.execute(sqlCSSR + comment, varMap)
            scTotal, scOK, scNG = self.cur.fetchone()

            if scTotal > 0 and scOK + scNG > 0:
                extraInfo["successRate"] = scOK / (scOK + scNG)
            else:
                extraInfo["successRate"] = 0
            tmpLog.debug(
                f"""scout total={scTotal} finished={scOK} failed={scNG} target_rate={None if scoutSuccessRate is None else scoutSuccessRate/10} actual_rate={extraInfo["successRate"]}"""
            )
            if scoutSuccessRate and scTotal and extraInfo["successRate"] < scoutSuccessRate / 10:
                tmpLog.debug("not enough scouts succeeded")
                scoutSucceeded = False

        # upper limit
        limitWallTime = 999999999

        # loop over all files
        outSizeList = []
        outSizeDict = {}
        walltimeList = []
        walltimeDict = {}
        memSizeList = []
        memSizeDict = {}
        leak_list = []
        leak_dict = {}
        leak_x2_list = []
        leak_x2_dict = {}
        workSizeList = []
        cpuTimeList = []
        cpuTimeDict = {}
        ioIntentList = []
        ioIntentDict = {}
        cpuEffList = []
        cpuEffDict = {}
        cpuEffMap = {}
        finishedJobs = []
        inFSizeList = []
        inFSizeMap = {}
        inEventsMap = {}
        corePowerMap = {}
        jMetricsMap = {}
        execTimeMap = {}
        siteMap = {}
        diskIoList = []
        pandaIDList = set()
        totInSizeMap = {}
        masterInSize = {}
        coreCountMap = {}
        pseudoInput = set()
        total_actual_input_size = 0
        for pandaID, fsize, startEvent, endEvent, nEvents, fType in resList:
            pandaIDList.add(pandaID)
            if pandaID not in inFSizeMap:
                inFSizeMap[pandaID] = 0
            # get effective file size
            effectiveFsize = CoreUtils.getEffectiveFileSize(fsize, startEvent, endEvent, nEvents)
            inFSizeMap[pandaID] += effectiveFsize
            # events
            if pandaID not in inEventsMap:
                inEventsMap[pandaID] = 0
            inEventsMap[pandaID] += CoreUtils.getEffectiveNumEvents(startEvent, endEvent, nEvents)
            # master input size
            if pandaID not in masterInSize:
                masterInSize[pandaID] = 0
            masterInSize[pandaID] += fsize
            total_actual_input_size += fsize
            if fType == "pseudo_input":
                pseudoInput.add(pandaID)

        # get nFiles
        totalJobs = 0
        totFiles = 0
        totFinished = 0
        nNewJobs = 0
        total_jobs_with_event = 0
        if not mergeScout:
            # estimate the number of new jobs with the number of files
            varMap = dict()
            varMap[":jediTaskID"] = jediTaskID
            varMap.update(INPUT_TYPES_var_map)
            self.cur.execute(sqlNumJobs + comment, varMap)
            resNumJobs = self.cur.fetchone()
            if resNumJobs is not None:
                totFiles, totFinished, totUsed = resNumJobs
                if totFinished > 0:
                    totalJobs = int(totFiles * len(pandaIDList) // totFinished)
                    nNewJobs = int((totFiles - totUsed) * len(pandaIDList) // totFinished)
                    # take into account the size limits coming from scouts, dataset boundaries, etc
                    if task_spec and not task_spec.getNumFilesPerJob() and not task_spec.getNumEventsPerJob() and not task_spec.getMaxSizePerJob():
                        # average input size
                        avg_actual_input_size = total_actual_input_size / 1024 / 1024 / len(pandaIDList)
                        # scale with actual size / allowed size
                        nNewJobs = int(nNewJobs * avg_actual_input_size / InputChunk.maxInputSizeAvalanche)
                        totalJobs = int(totalJobs * avg_actual_input_size / InputChunk.maxInputSizeAvalanche)
                    # estimate the number of new jobs with size
                    var_map = dict()
                    var_map[":jediTaskID"] = jediTaskID
                    var_map.update(INPUT_TYPES_var_map)
                    self.cur.execute(sql_num_jobs_event + comment, var_map)
                    res_num_jobs_event = self.cur.fetchone()
                    if res_num_jobs_event:
                        total_in_event, total_finished_event = res_num_jobs_event
                        if total_finished_event is not None and total_finished_event > 0:
                            total_jobs_with_event = int(total_in_event * len(pandaIDList) // total_finished_event)
                            # take into account size limit for scouts
                            if (
                                task_spec
                                and task_spec.useScout()
                                and not task_spec.getNumFilesPerJob()
                                and not task_spec.getNumEventsPerJob()
                                and not task_spec.getMaxSizePerJob()
                            ):
                                total_jobs_with_event = int(total_jobs_with_event * InputChunk.maxInputSizeScouts / InputChunk.maxInputSizeAvalanche)
        extraInfo["expectedNumJobs"] = totalJobs
        extraInfo["numFinishedJobs"] = len(pandaIDList)
        extraInfo["nFiles"] = totFiles
        extraInfo["nFilesFinished"] = totFinished
        extraInfo["nNewJobs"] = nNewJobs
        extraInfo["expectedNumJobsWithEvent"] = total_jobs_with_event

        # loop over all jobs
        loopPandaIDs = list(inFSizeMap.keys())
        random.shuffle(loopPandaIDs)
        loopPandaIDs = loopPandaIDs[:1000]
        for loopPandaID in loopPandaIDs:
            totalFSize = inFSizeMap[loopPandaID]
            # get job data
            varMap = {}
            varMap[":pandaID"] = loopPandaID
            varMap[":jobStatus"] = "finished"
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlSCDN + comment, varMap)
            resData = self.cur.fetchone()
            if resData is not None:
                eventServiceJob = resData[0]
                jobsetID = resData[1]
                resDataList = [resData]
                # get all ES jobs since input is associated to only one consumer
                if eventServiceJob == EventServiceUtils.esJobFlagNumber:
                    varMap = {}
                    varMap[":pandaID"] = jobsetID
                    varMap[":jobStatus"] = "finished"
                    varMap[":jediTaskID"] = jediTaskID
                    self.cur.execute(sqlSCDE + comment, varMap)
                    resDataList = self.cur.fetchall()
                # loop over all jobs
                for oneResData in resDataList:
                    (
                        eventServiceJob,
                        jobsetID,
                        pandaID,
                        jobStatus,
                        outputFileBytes,
                        jobMetrics,
                        cpuConsumptionTime,
                        actualCoreCount,
                        defCoreCount,
                        startTime,
                        endTime,
                        computingSite,
                        maxPSS,
                        specialHandling,
                        nEvents,
                        totRBYTES,
                        totWBYTES,
                        inputFileByte,
                        memory_leak,
                        memory_leak_x2,
                        modificationhost,
                    ) = oneResData

                    # event service job
                    is_event_service = eventServiceJob == EventServiceUtils.esJobFlagNumber and not EventServiceUtils.isJobCloningSH(specialHandling)
                    # add inputSize and nEvents
                    if pandaID not in inFSizeMap:
                        inFSizeMap[pandaID] = totalFSize
                    if pandaID not in inEventsMap or is_event_service:
                        inEventsMap[pandaID] = nEvents
                    totInSizeMap[pandaID] = inputFileByte

                    siteMap[pandaID] = computingSite

                    # --- core power by host (CPU model) ---
                    benchmarks = []
                    atlas_site = "NO_SITE"  # in case of no match
                    if site_mapper:
                        atlas_site = site_mapper.getSite(computingSite).pandasite
                        benchmarks = self.get_cpu_benchmarks_by_host(atlas_site, modificationhost) or []

                    vals = [v for _, v in benchmarks]
                    benchmark_specific = next(
                        (v for s, v in benchmarks if s and atlas_site and (s.upper() in atlas_site.upper() or atlas_site.upper() in s.upper())), 0
                    )
                    benchmark_average = mean(vals) if vals else 0

                    # --- core power by site (fallback) ---
                    if not benchmark_specific and not benchmark_average:
                        if computingSite not in corePowerMap:
                            self.cur.execute(sqlCore + comment, {":site": computingSite})
                            row = self.cur.fetchone()
                            corePowerMap[computingSite] = float(row[0]) if row and row[0] is not None else None
                        corePower = corePowerMap[computingSite]
                    else:
                        corePower = benchmark_specific or benchmark_average

                    # --- final fallback default ---
                    if not corePower or corePower == 0:
                        corePower = 10

                    finishedJobs.append(pandaID)
                    inFSizeList.append(totalFSize)
                    jMetricsMap[pandaID] = jobMetrics

                    # core count
                    coreCount = JobUtils.getCoreCount(actualCoreCount, defCoreCount, jobMetrics)
                    coreCountMap[pandaID] = coreCount

                    # output size
                    tmpWorkSize = 0
                    if not is_event_service:
                        try:
                            try:
                                # add size of intermediate files
                                if jobMetrics is not None:
                                    tmpMatch = re.search("workDirSize=(\d+)", jobMetrics)
                                    tmpWorkSize = int(tmpMatch.group(1))
                                    tmpWorkSize /= 1024 * 1024
                            except Exception:
                                pass
                            if preOutDiskUnit is None or "Fixed" not in preOutDiskUnit:
                                if preOutputScaleWithEvents:
                                    # scale with events
                                    if pandaID in inEventsMap and inEventsMap[pandaID] > 0:
                                        tmpVal = int(math.ceil(float(outputFileBytes) / inEventsMap[pandaID]))
                                    if pandaID not in inEventsMap or inEventsMap[pandaID] >= 10:
                                        outSizeList.append(tmpVal)
                                        outSizeDict[tmpVal] = pandaID
                                else:
                                    # scale with input size
                                    tmpVal = int(math.ceil(float(outputFileBytes) / totalFSize))
                                    if pandaID not in inEventsMap or inEventsMap[pandaID] >= 10:
                                        outSizeList.append(tmpVal)
                                        outSizeDict[tmpVal] = pandaID
                        except Exception:
                            pass

                    # execution time
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            tmpVal = cpuConsumptionTime
                            walltimeList.append(tmpVal)
                            walltimeDict[tmpVal] = pandaID
                        except Exception:
                            pass
                        try:
                            execTimeMap[pandaID] = endTime - startTime
                        except Exception:
                            pass

                    # CPU time
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            if preCpuTimeUnit not in ["HS06sPerEventFixed", "mHS06sPerEventFixed"]:
                                tmpVal = JobUtils.getHS06sec(
                                    startTime, endTime, corePower, coreCount, baseWalltime=preBaseWalltime, cpuEfficiency=preCpuEfficiency
                                )
                                if pandaID in inEventsMap and inEventsMap[pandaID] > 0:
                                    tmpVal /= float(inEventsMap[pandaID])
                                if (
                                    pandaID not in inEventsMap
                                    or inEventsMap[pandaID] >= (10 * coreCount)
                                    or pandaID in pseudoInput
                                    or (
                                        inEventsMap[pandaID] < (10 * coreCount)
                                        and pandaID in execTimeMap
                                        and execTimeMap[pandaID] > datetime.timedelta(seconds=6 * 3600)
                                    )
                                ):
                                    cpuTimeList.append(tmpVal)
                                    cpuTimeDict[tmpVal] = pandaID
                        except Exception:
                            pass

                    # IO
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            tmpTimeDelta = endTime - startTime
                            tmpVal = totalFSize * 1024.0 + float(outputFileBytes) / 1024.0
                            tmpVal = tmpVal / float(tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600)
                            tmpVal /= float(coreCount)
                            ioIntentList.append(tmpVal)
                            ioIntentDict[tmpVal] = pandaID
                        except Exception:
                            pass

                    # disk IO
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            tmpTimeDelta = endTime - startTime
                            tmpVal = totRBYTES + totWBYTES
                            tmpVal /= float(tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600)
                            diskIoList.append(tmpVal)
                        except Exception:
                            pass

                    # memory leak
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            memory_leak_core_tmp = float(memory_leak) / float(coreCount)
                            memory_leak_core_tmp = int(math.ceil(memory_leak_core_tmp))
                            leak_list.append(memory_leak_core_tmp)
                            leak_dict[memory_leak_core_tmp] = pandaID
                        except Exception:
                            pass
                        # memory leak chi2 measurement
                        try:
                            memory_leak_x2_tmp = int(memory_leak_x2)
                            leak_x2_list.append(memory_leak_x2_tmp)
                            leak_x2_dict[memory_leak_x2_tmp] = pandaID
                        except Exception:
                            pass

                    # RAM size
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            if preRamUnit == "MBPerCoreFixed":
                                pass
                            elif preRamUnit == "MBPerCore":
                                if maxPSS > 0:
                                    tmpPSS = maxPSS
                                    if preBaseRamCount not in [0, None]:
                                        tmpPSS -= preBaseRamCount * 1024
                                    tmpPSS = float(tmpPSS) / float(coreCount)
                                    tmpPSS = int(math.ceil(tmpPSS))
                                    memSizeList.append(tmpPSS)
                                    memSizeDict[tmpPSS] = pandaID
                            else:
                                if maxPSS > 0:
                                    tmpMEM = maxPSS
                                else:
                                    tmpMatch = re.search("vmPeakMax=(\d+)", jobMetrics)
                                    tmpMEM = int(tmpMatch.group(1))
                                memSizeList.append(tmpMEM)
                                memSizeDict[tmpMEM] = pandaID
                        except Exception:
                            pass

                    # use lib size as workdir size
                    if tmpWorkSize is None or (libSize is not None and libSize > tmpWorkSize):
                        tmpWorkSize = libSize
                    if tmpWorkSize is not None:
                        workSizeList.append(tmpWorkSize)

                    # CPU efficiency
                    if eventServiceJob != EventServiceUtils.esMergeJobFlagNumber:
                        try:
                            tmpTimeDelta = endTime - startTime
                            float(tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600)
                            tmpCpuEff = int(
                                math.ceil(float(cpuConsumptionTime) / (coreCount * float(tmpTimeDelta.seconds + tmpTimeDelta.days * 24 * 3600)) * 100)
                            )
                            cpuEffList.append(tmpCpuEff)
                            cpuEffDict[tmpCpuEff] = pandaID
                            cpuEffMap[pandaID] = tmpCpuEff
                        except Exception:
                            pass

        # add tags
        def addTag(jobTagMap, idDict, value, tagStr):
            if value in idDict:
                tmpPandaID = idDict[value]
                if tmpPandaID not in jobTagMap:
                    jobTagMap[tmpPandaID] = []
                if tagStr not in jobTagMap[tmpPandaID]:
                    jobTagMap[tmpPandaID].append(tagStr)

        # calculate values
        jobTagMap = {}
        if outSizeList != []:
            median, origValues = CoreUtils.percentile(outSizeList, 75, outSizeDict)
            for origValue in origValues:
                addTag(jobTagMap, outSizeDict, origValue, "outDiskCount")
            median /= 1024
            # upper limit 10MB output per 1MB input
            upperLimit = 10 * 1024
            if median > upperLimit:
                median = upperLimit
            returnMap["outDiskCount"] = int(median)
            if preOutputScaleWithEvents:
                returnMap["outDiskUnit"] = "kBPerEvent"
            else:
                returnMap["outDiskUnit"] = "kB"

        if walltimeList != []:
            maxWallTime = max(walltimeList)
            extraInfo["maxCpuConsumptionTime"] = maxWallTime
            extraInfo["maxExecTime"] = execTimeMap[walltimeDict[maxWallTime]]
            extraInfo["defCoreCount"] = coreCountMap[walltimeDict[maxWallTime]]
            addTag(jobTagMap, walltimeDict, maxWallTime, "walltime")
            # cut off of 60min
            if maxWallTime < 60 * 60:
                maxWallTime = 0
            median = float(maxWallTime) / float(max(inFSizeList)) * 1.5
            median = math.ceil(median)
            returnMap["walltime"] = int(median)
            # use preset value if larger
            if preWalltime is not None and (preWalltime > returnMap["walltime"] or preWalltime < 0):
                returnMap["walltime"] = preWalltime
            # upper limit
            if returnMap["walltime"] > limitWallTime:
                returnMap["walltime"] = limitWallTime
        returnMap["walltimeUnit"] = "kSI2kseconds"

        if cpuTimeList != []:
            maxCpuTime, origValues = CoreUtils.percentile(cpuTimeList, cpuTimeRank, cpuTimeDict)
            for origValue in origValues:
                addTag(jobTagMap, cpuTimeDict, origValue, "cpuTime")
                try:
                    extraInfo["execTime"] = execTimeMap[cpuTimeDict[origValue]]
                except Exception:
                    pass
            maxCpuTime *= 1.5
            if maxCpuTime < 10:
                maxCpuTime *= 1000
                returnMap["cpuTimeUnit"] = "mHS06sPerEvent"
                if extraInfo["oldCpuTime"]:
                    extraInfo["oldCpuTime"] = int(extraInfo["oldCpuTime"] * 1000)
            elif preCpuTimeUnit is not None:
                # for mHS06sPerEvent -> HS06sPerEvent
                returnMap["cpuTimeUnit"] = "HS06sPerEvent"
            maxCpuTime = int(math.ceil(maxCpuTime))
            returnMap["cpuTime"] = maxCpuTime

        if ioIntentList != []:
            maxIoIntent = max(ioIntentList)
            addTag(jobTagMap, ioIntentDict, maxIoIntent, "ioIntensity")
            maxIoIntent = int(math.ceil(maxIoIntent))
            returnMap["ioIntensity"] = maxIoIntent
            returnMap["ioIntensityUnit"] = "kBPerS"

        if diskIoList != []:
            aveDiskIo = sum(diskIoList) // len(diskIoList)
            aveDiskIo = int(math.ceil(aveDiskIo))
            if capOnDiskIO is not None:
                aveDiskIo = min(aveDiskIo, capOnDiskIO)
            returnMap["diskIO"] = aveDiskIo
            returnMap["diskIOUnit"] = "kBPerS"

        if leak_list:
            ave_leak = int(math.ceil(sum(leak_list) / len(leak_list)))
            returnMap["memory_leak_core"] = ave_leak

        if leak_x2_list:
            ave_leak_x2 = int(math.ceil(sum(leak_x2_list) / len(leak_x2_list)))
            returnMap["memory_leak_x2"] = ave_leak_x2

        if memSizeList != []:
            memVal, origValues = CoreUtils.percentile(memSizeList, ramCountRank, memSizeDict)
            for origValue in origValues:
                addTag(jobTagMap, memSizeDict, origValue, "ramCount")
            memVal = memVal * (100 + ramCountMargin) // 100
            memVal /= 1024
            memVal = int(memVal)
            if memVal < 0:
                memVal = 1
            if minRamCount is not None and minRamCount > memVal:
                memVal = minRamCount
            if preRamUnit == "MBPerCore":
                returnMap["ramUnit"] = preRamUnit
                returnMap["ramCount"] = memVal
            elif preRamUnit == "MBPerCoreFixed":
                returnMap["ramUnit"] = preRamUnit
            else:
                returnMap["ramUnit"] = "MB"
                returnMap["ramCount"] = memVal

        if workSizeList != []:
            median = max(workSizeList)
            returnMap["workDiskCount"] = int(median)
            returnMap["workDiskUnit"] = "MB"
            # use preset value if larger
            if preWorkDiskCount is not None and preWorkDiskCount > returnMap["workDiskCount"]:
                returnMap["workDiskCount"] = preWorkDiskCount

        if cpuEffList != []:
            minCpuEfficiency = int(numpy.median(cpuEffList))
            addTag(jobTagMap, cpuEffDict, minCpuEfficiency, "cpuEfficiency")
            extraInfo["minCpuEfficiency"] = minCpuEfficiency

        nShortJobs = 0
        nShortJobsWithCtoS = 0
        nTotalForShort = 0
        longestShortExecTime = 0
        for tmpPandaID, tmpExecTime in execTimeMap.items():
            if tmpExecTime <= datetime.timedelta(minutes=shortExecTime):
                longestShortExecTime = max(longestShortExecTime, tmpExecTime.total_seconds())
                if site_mapper and task_spec:
                    # ignore if the site enforces to use copy-to-scratch
                    tmpSiteSpec = site_mapper.getSite(siteMap[tmpPandaID])
                    if not task_spec.useLocalIO() and not CoreUtils.use_direct_io_for_job(task_spec, tmpSiteSpec, None):
                        nShortJobsWithCtoS += 1
                        continue
                nShortJobs += 1
            nTotalForShort += 1
        extraInfo["nShortJobs"] = nShortJobs
        extraInfo["nShortJobsWithCtoS"] = nShortJobsWithCtoS
        extraInfo["nTotalForShort"] = nTotalForShort
        extraInfo["longestShortExecTime"] = longestShortExecTime
        nInefficientJobs = 0
        for tmpPandaID, tmpCpuEff in cpuEffMap.items():
            if tmpCpuEff < lowCpuEff:
                nInefficientJobs += 1
        extraInfo["nInefficientJobs"] = nInefficientJobs
        extraInfo["nTotalForIneff"] = len(cpuEffMap)
        # tag jobs
        if flagJob:
            for tmpPandaID, tmpTags in jobTagMap.items():
                self.updateJobMetrics_JEDI(jediTaskID, tmpPandaID, jMetricsMap[tmpPandaID], tmpTags)
        # reset NG
        taskSpec = JediTaskSpec()
        taskSpec.splitRule = splitRule
        if not mergeScout and taskSpec.getTgtMaxOutputForNG() is not None and "outDiskCount" in returnMap:
            # look for PandaID for outDiskCount
            for tmpPandaID, tmpTags in jobTagMap.items():
                if "outDiskCount" in tmpTags:
                    # get total and the largest output fsize
                    sqlBig = f"SELECT SUM(fsize) FROM {panda_config.schemaPANDA}.filesTable4 WHERE PandaID=:PandaID AND type=:type GROUP BY dataset "
                    varMap = dict()
                    varMap[":PandaID"] = tmpPandaID
                    varMap[":type"] = "output"
                    self.cur.execute(sqlBig + comment, varMap)
                    resBig = self.cur.fetchall()
                    outTotal = 0
                    outBig = 0
                    for (tmpFsize,) in resBig:
                        outTotal += tmpFsize
                        if tmpFsize > outBig:
                            outBig = tmpFsize
                    if outTotal * outBig > 0:
                        # get NG
                        taskSpec.outDiskCount = returnMap["outDiskCount"]
                        taskSpec.outDiskUnit = returnMap["outDiskUnit"]
                        expectedOutSize = outTotal * taskSpec.getTgtMaxOutputForNG() * 1024 * 1024 * 1024 // outBig
                        outDiskCount = taskSpec.getOutDiskSize()
                        if "workDiskCount" in returnMap:
                            taskSpec.workDiskCount = returnMap["workDiskCount"]
                        else:
                            taskSpec.workDiskCount = preWorkDiskCount
                        taskSpec.workDiskUnit = "MB"
                        workDiskCount = taskSpec.getWorkDiskSize()
                        if outDiskCount == 0:
                            # to avoid zero-division
                            outDiskCount = 1
                        scaleFactor = expectedOutSize // outDiskCount
                        if preOutputScaleWithEvents:
                            # scaleFactor is num of events
                            try:
                                expectedInSize = (
                                    (inFSizeMap[tmpPandaID] + totInSizeMap[tmpPandaID] - masterInSize[tmpPandaID]) * scaleFactor // inEventsMap[tmpPandaID]
                                )
                                newNG = expectedOutSize + expectedInSize + workDiskCount - InputChunk.defaultOutputSize
                            except Exception:
                                newNG = None
                        else:
                            # scaleFactor is input size
                            newNG = expectedOutSize + scaleFactor * (1024 * 1024) - InputChunk.defaultOutputSize
                        if newNG is not None:
                            newNG /= 1024 * 1024 * 1024
                            if newNG <= 0:
                                newNG = 1
                            maxNG = 100
                            if newNG > maxNG:
                                newNG = maxNG
                            returnMap["newNG"] = int(newNG)
        if useTransaction:
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
        # filtered dump
        if scoutSucceeded and not mergeScout and len(returnMap) > 0:
            tmpMsg = f"scouts got for jediTaskID={jediTaskID} "
            tmpKeys = sorted(returnMap.keys())
            for tmpKey in tmpKeys:
                tmpMsg += f"{tmpKey}={returnMap[tmpKey]} "
            for tmpPandaID, tmpTags in jobTagMap.items():
                for tmpTag in tmpTags:
                    tmpMsg += f"{tmpTag}_by={tmpPandaID} "
            tmpLog.info(tmpMsg[:-1])
        # return
        tmpLog.debug(f"succeeded={scoutSucceeded} data={str(returnMap)} extra={str(extraInfo)} tag={jobTagMap}")
        return scoutSucceeded, returnMap, extraInfo

    # set scout job data
    def setScoutJobData_JEDI(self, taskSpec, useCommit, useExhausted, site_mapper):
        comment = " /* JediDBProxy.setScoutJobData_JEDI */"
        jediTaskID = taskSpec.jediTaskID
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID} label={taskSpec.prodSourceLabel}")
        tmpLog.debug("start")
        # get thresholds for exausted
        ramThr = self.getConfigValue("dbproxy", "RAM_THR_EXAUSTED", "jedi")
        if ramThr is None:
            ramThr = 4
        ramThr *= 1000
        # send tasks to exhausted when task.successRate > rate >= thr
        minNumOkScoutsForExhausted = self.getConfigValue("dbproxy", f"SCOUT_MIN_OK_RATE_EXHAUSTED_{taskSpec.prodSourceLabel}", "jedi")
        scoutSuccessRate = taskSpec.getScoutSuccessRate()
        if scoutSuccessRate and minNumOkScoutsForExhausted:
            if scoutSuccessRate > minNumOkScoutsForExhausted * 10:
                scoutSuccessRate = minNumOkScoutsForExhausted * 10
            else:
                minNumOkScoutsForExhausted = None
        if useCommit:
            # begin transaction
            self.conn.begin()
        task_params_str = self.getTaskParamsWithID_JEDI(jediTaskID, use_commit=False)
        task_params_map = json.loads(task_params_str)
        # set average job data
        scoutSucceeded, scoutData, extraInfo = self.getScoutJobData_JEDI(
            jediTaskID,
            scoutSuccessRate=scoutSuccessRate,
            flagJob=True,
            site_mapper=site_mapper,
            task_spec=taskSpec,
            task_params_map=task_params_map,
        )
        # sql to update task data
        if scoutData != {}:
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            sqlTSD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET "
            for scoutKey, scoutVal in scoutData.items():
                # skip new NG
                if scoutKey in ["newNG"]:
                    continue
                tmpScoutKey = f":{scoutKey}"
                varMap[tmpScoutKey] = scoutVal
                sqlTSD += f"{scoutKey}={tmpScoutKey},"
            sqlTSD = sqlTSD[:-1]
            sqlTSD += " WHERE jediTaskID=:jediTaskID "
            tmpLog.debug(sqlTSD + comment + str(varMap))
            self.cur.execute(sqlTSD + comment, varMap)
            # update NG
            if "newNG" in scoutData:
                taskSpec.setSplitRule("nGBPerJob", str(scoutData["newNG"]))
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                varMap[":splitRule"] = taskSpec.splitRule
                sqlTSL = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET splitRule=:splitRule "
                sqlTSL += " WHERE jediTaskID=:jediTaskID "
                tmpLog.debug(sqlTSL + comment + str(varMap))
                self.cur.execute(sqlTSL + comment, varMap)

        # set average merge job data
        mergeScoutSucceeded = None
        if taskSpec.mergeOutput():
            mergeScoutSucceeded, mergeScoutData, mergeExtraInfo = self.getScoutJobData_JEDI(jediTaskID, mergeScout=True, task_params_map=task_params_map)
            if mergeScoutData != {}:
                varMap = {}
                varMap[":jediTaskID"] = jediTaskID
                sqlTSD = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET "
                for mergeScoutKey, mergeScoutVal in mergeScoutData.items():
                    # only walltime and ramCount
                    if not mergeScoutKey.startswith("walltime") and not mergeScoutKey.startswith("ram"):
                        continue
                    tmpScoutKey = f":{mergeScoutKey}"
                    varMap[tmpScoutKey] = mergeScoutVal
                    sqlTSD += f"merge{mergeScoutKey}={tmpScoutKey},"
                sqlTSD = sqlTSD[:-1]
                sqlTSD += " WHERE jediTaskID=:jediTaskID "
                tmpLog.debug(sqlTSD + comment + str(varMap))
                self.cur.execute(sqlTSD + comment, varMap)

        # go to exhausted if necessary
        nNewJobsCutoff = 20
        if useExhausted and scoutSucceeded and extraInfo["nNewJobs"] > nNewJobsCutoff:
            # check cpuTime
            if taskSpec.useHS06() and "cpuTime" in scoutData and "execTime" in extraInfo:
                minExecTime = 24
                wrong_cputime_thr = self.getConfigValue("dbproxy", "SCOUT_WRONG_CPUTIME_THRESHOLD", "jedi")
                if wrong_cputime_thr is None:
                    wrong_cputime_thr = 2
                if (
                    wrong_cputime_thr > 0
                    and extraInfo["oldCpuTime"] not in [0, None]
                    and scoutData["cpuTime"] > wrong_cputime_thr * extraInfo["oldCpuTime"]
                    and extraInfo["execTime"] > datetime.timedelta(hours=minExecTime)
                ):
                    errMsg = f"""#KV #ATM action=set_exhausted reason=scout_cpuTime ({scoutData["cpuTime"]}) is larger than {wrong_cputime_thr}*task_cpuTime ({extraInfo["oldCpuTime"]}) and execTime ({extraInfo["execTime"]}) > {minExecTime} hours"""
                    tmpLog.info(errMsg)
                    taskSpec.setErrDiag(errMsg)
                    taskSpec.status = "exhausted"

            # check ramCount
            if taskSpec.status != "exhausted":
                if (
                    taskSpec.ramPerCore()
                    and "ramCount" in scoutData
                    and extraInfo["oldRamCount"] is not None
                    and extraInfo["oldRamCount"] < ramThr < scoutData["ramCount"]
                ):
                    errMsg = f"#KV #ATM action=set_exhausted reason=scout_ramCount {scoutData['ramCount']} MB is larger than {ramThr} MB "
                    errMsg += f"while requested task_ramCount {extraInfo['oldRamCount']} MB is less than {ramThr} MB"
                    tmpLog.info(errMsg)
                    taskSpec.setErrDiag(errMsg)
                    taskSpec.status = "exhausted"

            # check memory leak
            if taskSpec.status != "exhausted":
                memory_leak_core_max = self.getConfigValue("dbproxy", f"SCOUT_MEM_LEAK_PER_CORE_{taskSpec.prodSourceLabel}", "jedi")
                memory_leak_core = scoutData.get("memory_leak_core")
                memory_leak_x2 = scoutData.get("memory_leak_x2")  # TODO: decide what to do with it
                if memory_leak_core and memory_leak_core_max and memory_leak_core > memory_leak_core_max:
                    errMsg = f"#ATM #KV action=set_exhausted since reason=scout_memory_leak {memory_leak_core} is larger than {memory_leak_core_max}"
                    tmpLog.info(errMsg)
                    taskSpec.setErrDiag(errMsg)
                    # taskSpec.status = 'exhausted'

            # short job check
            sl_changed = False
            if taskSpec.status != "exhausted":
                # get exectime threshold for exhausted
                maxShortJobs = self.getConfigValue("dbproxy", f"SCOUT_NUM_SHORT_{taskSpec.prodSourceLabel}", "jedi")
                shortJobCutoff = self.getConfigValue("dbproxy", f"SCOUT_THR_SHORT_{taskSpec.prodSourceLabel}", "jedi")
                if maxShortJobs and shortJobCutoff:
                    # many short jobs w/o copy-to-scratch
                    manyShortJobs = extraInfo["nTotalForShort"] > 0 and extraInfo["nShortJobs"] / extraInfo["nTotalForShort"] >= maxShortJobs / 10
                    if manyShortJobs:
                        toExhausted = True
                        # check expected number of jobs
                        if shortJobCutoff and min(extraInfo["expectedNumJobs"], extraInfo["expectedNumJobsWithEvent"]) < shortJobCutoff:
                            tmpLog.debug(
                                "not to set exhausted or change split rule since expect num of jobs "
                                "min({} file-based, {} event-based) is less than {}".format(
                                    extraInfo["expectedNumJobs"], extraInfo["expectedNumJobsWithEvent"], shortJobCutoff
                                )
                            )
                            toExhausted = False
                        # remove wrong rules
                        if toExhausted and self.getConfigValue("dbproxy", f"SCOUT_CHANGE_SR_{taskSpec.prodSourceLabel}", "jedi"):
                            updateSL = []
                            removeSL = []
                            if taskSpec.getNumFilesPerJob() is not None:
                                taskSpec.removeNumFilesPerJob()
                                removeSL.append("nFilesPerJob")
                            if taskSpec.getMaxSizePerJob() is not None:
                                taskSpec.removeMaxSizePerJob()
                                removeSL.append("nGBPerJob")
                            MAX_NUM_FILES = 200
                            if taskSpec.getMaxNumFilesPerJob() is not None and taskSpec.getMaxNumFilesPerJob() < MAX_NUM_FILES:
                                taskSpec.setMaxNumFilesPerJob(str(MAX_NUM_FILES))
                                updateSL.append("MF")
                            if updateSL or removeSL:
                                sl_changed = True
                                tmpMsg = "#KV #ATM action=change_split_rule reason=many_shorter_jobs"
                                if removeSL:
                                    tmpMsg += f" removed {','.join(removeSL)},"
                                if updateSL:
                                    tmpMsg += f" changed {','.join(updateSL)},"
                                tmpMsg = tmpMsg[:-1]
                                tmpLog.info(tmpMsg)
                                varMap = {}
                                varMap[":jediTaskID"] = jediTaskID
                                varMap[":splitRule"] = taskSpec.splitRule
                                sqlTSL = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET splitRule=:splitRule "
                                sqlTSL += " WHERE jediTaskID=:jediTaskID "
                                tmpLog.debug(sqlTSL + comment + str(varMap))
                                self.cur.execute(sqlTSL + comment, varMap)
                                toExhausted = False
                        # check scaled walltime
                        if toExhausted:
                            scMsg = ""
                            if taskSpec.useScout():
                                scaled_max_walltime = extraInfo["longestShortExecTime"]
                                scaled_max_walltime *= InputChunk.maxInputSizeAvalanche / InputChunk.maxInputSizeScouts
                                scaled_max_walltime = int(scaled_max_walltime / 60)
                                if scaled_max_walltime > extraInfo["shortExecTime"]:
                                    tmpLog.debug(
                                        "not to set exhausted since scaled execution time ({}) is longer "
                                        "than {} min".format(scaled_max_walltime, extraInfo["shortExecTime"])
                                    )
                                    toExhausted = False
                                else:
                                    scMsg = " and scaled execution time ({} = walltime * {}/{}) less than {} min".format(
                                        scaled_max_walltime, InputChunk.maxInputSizeAvalanche, InputChunk.maxInputSizeScouts, extraInfo["shortExecTime"]
                                    )
                        # go to exhausted
                        if toExhausted:
                            errMsg = "#ATM #KV action=set_exhausted since reason=many_shorter_jobs "
                            errMsg += (
                                "{}/{} jobs (greater than {}0%, excluding {} jobs forced "
                                "to run with copy-to-scratch) ran faster than {} min, "
                                "and the expected num of jobs min({} file-based, {} event-based) exceeds {} {}".format(
                                    extraInfo["nShortJobs"],
                                    extraInfo["nTotalForShort"],
                                    maxShortJobs,
                                    extraInfo["nShortJobsWithCtoS"],
                                    extraInfo["shortExecTime"],
                                    extraInfo["expectedNumJobs"],
                                    extraInfo["expectedNumJobsWithEvent"],
                                    shortJobCutoff,
                                    scMsg,
                                )
                            )
                            tmpLog.info(errMsg)
                            taskSpec.setErrDiag(errMsg)
                            taskSpec.status = "exhausted"

            # CPU efficiency
            if taskSpec.status != "exhausted" and not sl_changed:
                # OK if minCpuEfficiency is satisfied
                if taskSpec.getMinCpuEfficiency() and extraInfo["minCpuEfficiency"] >= taskSpec.getMinScoutEfficiency():
                    pass
                else:
                    # get inefficiency threshold for exhausted
                    maxIneffJobs = self.getConfigValue("dbproxy", f"SCOUT_NUM_CPU_INEFFICIENT_{taskSpec.prodSourceLabel}", "jedi")
                    ineffJobCutoff = self.getConfigValue("dbproxy", f"SCOUT_THR_CPU_INEFFICIENT_{taskSpec.prodSourceLabel}", "jedi")
                    tmp_skip = False
                    errMsg = "#ATM #KV action=set_exhausted since reason=low_efficiency "
                    if taskSpec.getMinCpuEfficiency() and extraInfo["minCpuEfficiency"] < taskSpec.getMinCpuEfficiency():
                        tmp_skip = True
                        errMsg += f"lowest CPU efficiency {extraInfo['minCpuEfficiency']} is less than getMinCpuEfficiency={taskSpec.getMinCpuEfficiency()}"
                    elif (
                        maxIneffJobs
                        and extraInfo["nTotalForIneff"] > 0
                        and extraInfo["nInefficientJobs"] / extraInfo["nTotalForIneff"] >= maxIneffJobs / 10
                        and ineffJobCutoff
                        and max(extraInfo["expectedNumJobs"], extraInfo["expectedNumJobsWithEvent"]) > ineffJobCutoff
                    ):
                        tmp_skip = True
                        errMsg += (
                            "{}/{} jobs (greater than {}/10) had lower CPU efficiencies than {} "
                            "and expected num of jobs max({} file-based est, {} event-based est) is larger than {}".format(
                                extraInfo["nInefficientJobs"],
                                extraInfo["nTotalForIneff"],
                                maxIneffJobs,
                                extraInfo["cpuEfficiencyCap"],
                                extraInfo["expectedNumJobs"],
                                extraInfo["expectedNumJobsWithEvent"],
                                ineffJobCutoff,
                            )
                        )
                    if tmp_skip:
                        tmpLog.info(errMsg)
                        taskSpec.setErrDiag(errMsg)
                        taskSpec.status = "exhausted"

            # cpu abuse
            if taskSpec.status != "exhausted":
                try:
                    abuseOffset = 2
                    if extraInfo["maxCpuConsumptionTime"] > extraInfo["maxExecTime"].total_seconds() * extraInfo["defCoreCount"] * abuseOffset:
                        errMsg = f"#ATM #KV action=set_exhausted since reason=over_cpu_consumption {extraInfo['maxCpuConsumptionTime']} sec "
                        errMsg += "is larger than jobDuration*coreCount*safety ({0}*{1}*{2}). ".format(
                            extraInfo["maxExecTime"].total_seconds(), extraInfo["defCoreCount"], abuseOffset
                        )
                        errMsg += "Running multi-core payload on single core queues? #ATM"
                        tmpLog.info(errMsg)
                        taskSpec.setErrDiag(errMsg)
                        taskSpec.status = "exhausted"
                except Exception:
                    tmpLog.error("failed to check CPU abuse")
                    pass

            # low success rate
            if taskSpec.status != "exhausted" and minNumOkScoutsForExhausted:
                if taskSpec.getScoutSuccessRate() and "successRate" in extraInfo and extraInfo["successRate"] < taskSpec.getScoutSuccessRate() / 10:
                    errMsg = "#ATM #KV action=set_exhausted since reason=low_success_rate between {} and {} ".format(
                        minNumOkScoutsForExhausted, taskSpec.getScoutSuccessRate() / 10
                    )
                    tmpLog.info(errMsg)
                    taskSpec.setErrDiag(errMsg)
                    taskSpec.status = "exhausted"

        if useCommit:
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
        # reset the task resource type
        try:
            self.reset_resource_type_task(jediTaskID, useCommit)
        except Exception:
            tmpLog.error(f"reset_resource_type_task excepted with: {traceback.format_exc()}")

        return scoutSucceeded, mergeScoutSucceeded

    # update input datasets stage-in done (according to message from iDDS, called by other methods, etc.)
    def updateInputDatasetsStaged_JEDI(self, jeditaskid, scope, dsnames_dict=None, use_commit=True, by=None):
        comment = " /* JediDBProxy.updateInputDatasetsStaged_JEDI */"
        tmp_tag = f"jediTaskID={jeditaskid}"
        if by:
            tmp_tag += f" by={by}"
        tmpLog = self.create_tagged_logger(comment, tmp_tag)
        tmpLog.debug("start")
        try:
            # update all files when scope is None
            if scope is None:
                dsnames_dict = [None]
            retVal = 0
            # varMap
            varMap = dict()
            varMap[":jediTaskID"] = jeditaskid
            varMap[":type1"] = "input"
            varMap[":type2"] = "pseudo_input"
            varMap[":old_status"] = "staging"
            varMap[":new_status"] = "pending"
            # sql with dataset name
            sqlUD = (
                "UPDATE {0}.JEDI_Dataset_Contents "
                "SET status=:new_status "
                "WHERE jediTaskID=:jediTaskID "
                "AND datasetID IN ("
                "SELECT datasetID FROM {0}.JEDI_Datasets "
                "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2) AND datasetName=:datasetName) "
                "AND status=:old_status "
            ).format(panda_config.schemaJEDI)
            # sql without dataset name
            sql_wo_dataset_name = (
                "UPDATE {0}.JEDI_Dataset_Contents "
                "SET status=:new_status "
                "WHERE jediTaskID=:jediTaskID "
                "AND datasetID IN ("
                "SELECT datasetID FROM {0}.JEDI_Datasets "
                "WHERE jediTaskID=:jediTaskID AND type IN (:type1,:type2)) "
                "AND status=:old_status "
            ).format(panda_config.schemaJEDI)
            # begin transaction
            if use_commit:
                self.conn.begin()
            for dsname in dsnames_dict:
                if scope:
                    varMap[":datasetName"] = f"{scope}:{dsname}"
                    sql = sqlUD
                else:
                    sql = sql_wo_dataset_name
                tmpLog.debug(f"running sql: {sql} {str(varMap)}")
                self.cur.execute(sql + comment, varMap)
                retVal += self.cur.rowcount
            self.fix_associated_files_in_staging(jeditaskid)
            # update task to trigger CF immediately
            if retVal:
                sqlUT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET modificationTime=CURRENT_DATE-1 WHERE jediTaskID=:jediTaskID AND lockedBy IS NULL "
                varMap = dict()
                varMap[":jediTaskID"] = jeditaskid
                self.cur.execute(sqlUT + comment, varMap)
                tmpLog.debug(f"unlocked task with {self.cur.rowcount}")
            # commit
            if use_commit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            tmpLog.debug(f"updated {retVal} files")
            return retVal
        except Exception:
            if use_commit:
                # roll back
                self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return None

    # kill child tasks
    def killChildTasks_JEDI(self, jediTaskID, taskStatus, useCommit=True):
        comment = " /* JediDBProxy.killChildTasks_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug("start")
        retTasks = []
        try:
            # sql to get child tasks
            sqlGT = f"SELECT jediTaskID,status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlGT += "WHERE parent_tid=:jediTaskID AND parent_tid<>jediTaskID "
            # sql to change status
            sqlCT = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks "
            sqlCT += "SET status=:status,errorDialog=:errorDialog,stateChangeTime=CURRENT_DATE "
            sqlCT += "WHERE jediTaskID=:jediTaskID "
            # begin transaction
            if useCommit:
                self.conn.begin()
            # get tasks
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            self.cur.execute(sqlGT + comment, varMap)
            resList = self.cur.fetchall()
            for cJediTaskID, cTaskStatus in resList:
                # no more changes
                if cTaskStatus in JediTaskSpec.statusToRejectExtChange():
                    continue
                # change status
                cTaskStatus = "toabort"
                varMap = {}
                varMap[":jediTaskID"] = cJediTaskID
                varMap[":status"] = cTaskStatus
                varMap[":errorDialog"] = f"parent task is {taskStatus}"
                self.cur.execute(sqlCT + comment, varMap)
                tmpLog.debug(f"set {cTaskStatus} to jediTaskID={cJediTaskID}")
                # add missing record_task_status_change and push_task_status_message updates
                self.record_task_status_change(cJediTaskID)
                self.push_task_status_message(None, cJediTaskID, cTaskStatus)
                # kill child
                tmpStat = self.killChildTasks_JEDI(cJediTaskID, cTaskStatus, useCommit=False)
                if not tmpStat:
                    raise RuntimeError("Failed to kill child tasks")
            # commit
            if useCommit:
                if not self._commit():
                    raise RuntimeError("Commit error")
            # return
            tmpLog.debug("done")
            return True
        except Exception:
            # roll back
            if useCommit:
                self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return False

    # task attempt start logging
    def log_task_attempt_start(self, jedi_task_id):
        comment = " /* JediDBProxy.log_task_attempt_start */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmpLog.debug("start")
        # sql
        sqlGLTA = f"SELECT MAX(attemptnr) FROM {panda_config.schemaJEDI}.TASK_ATTEMPTS WHERE jediTaskID=:jediTaskID "
        sqlELTA = (
            "UPDATE {0}.TASK_ATTEMPTS "
            "SET (endtime, endstatus) = ( "
            "SELECT CURRENT_DATE,status "
            "FROM {0}.JEDI_Tasks "
            "WHERE jediTaskID=:jediTaskID "
            ") "
            "WHERE jediTaskID=:jediTaskID "
            "AND attemptnr=:last_attemptnr "
            "AND endtime IS NULL "
        ).format(panda_config.schemaJEDI)
        sqlITA = (
            "INSERT INTO {0}.TASK_ATTEMPTS "
            "(jeditaskid, attemptnr, starttime, startstatus) "
            "SELECT jediTaskID, GREATEST(:grandAttemptNr, COALESCE(attemptNr, 0)), CURRENT_DATE, status "
            "FROM {0}.JEDI_Tasks "
            "WHERE jediTaskID=:jediTaskID "
        ).format(panda_config.schemaJEDI)
        # get grand attempt number
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        self.cur.execute(sqlGLTA + comment, varMap)
        (last_attemptnr,) = self.cur.fetchone()
        grand_attemptnr = 0
        if last_attemptnr is not None:
            grand_attemptnr = last_attemptnr + 1
            # end last attempt in case log_task_attempt_end is not called
            varMap = dict()
            varMap[":jediTaskID"] = jedi_task_id
            varMap[":last_attemptnr"] = last_attemptnr
            self.cur.execute(sqlELTA + comment, varMap)
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        varMap[":grandAttemptNr"] = grand_attemptnr
        # insert task attempt
        self.cur.execute(sqlITA + comment, varMap)
        tmpLog.debug("done")

    # task attempt end logging
    def log_task_attempt_end(self, jedi_task_id):
        comment = " /* JediDBProxy.log_task_attempt_end */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id}")
        tmpLog.debug("start")
        varMap = dict()
        varMap[":jediTaskID"] = jedi_task_id
        # sql
        sqlUTA = (
            "UPDATE {0}.TASK_ATTEMPTS "
            "SET (endtime, endstatus) = ( "
            "SELECT CURRENT_DATE,status "
            "FROM {0}.JEDI_Tasks "
            "WHERE jediTaskID=:jediTaskID "
            ") "
            "WHERE jediTaskID=:jediTaskID "
            "AND endtime IS NULL "
        ).format(panda_config.schemaJEDI)
        self.cur.execute(sqlUTA + comment, varMap)
        tmpLog.debug("done")

    # duplicate files for reuse
    def duplicateFilesForReuse_JEDI(self, datasetSpec):
        comment = " /* JediDBProxy.duplicateFilesForReuse_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={datasetSpec.jediTaskID} datasetID={datasetSpec.datasetID}")
        try:
            tmpLog.debug(f"start random={datasetSpec.isRandom()}")
            # sql to get unique files
            sqlCT = "SELECT COUNT(*) FROM ("
            sqlCT += "SELECT distinct lfn,startEvent,endEvent "
            sqlCT += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlCT += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlCT += ") "
            # sql to read file spec
            defaultVales = {}
            defaultVales["status"] = "ready"
            defaultVales["PandaID"] = None
            defaultVales["attemptNr"] = 0
            defaultVales["failedAttempt"] = 0
            defaultVales["ramCount"] = 0
            sqlFR = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlFR += f"SELECT {JediFileSpec.columnNames(useSeq=True, defaultVales=defaultVales)} FROM ( "
            sqlFR += f"SELECT {JediFileSpec.columnNames(defaultVales=defaultVales, skipDefaultAttr=True)} "
            sqlFR += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID AND fileID IN ( "
            sqlFR += "SELECT /*+ UNNEST */ MIN(fileID) minFileID "
            sqlFR += f"FROM {panda_config.schemaJEDI}.JEDI_Dataset_Contents "
            sqlFR += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            sqlFR += "GROUP BY lfn,startEvent,endEvent) "
            if not datasetSpec.isRandom():
                sqlFR += "ORDER BY fileID) "
            else:
                sqlFR += "ORDER BY DBMS_RANDOM.value) "
            # sql to update dataset record
            sqlDU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDU += "SET nFiles=nFiles+:iFiles,nFilesTobeUsed=nFilesTobeUsed+:iFiles "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            # read unique files
            varMap = {}
            varMap[":jediTaskID"] = datasetSpec.jediTaskID
            varMap[":datasetID"] = datasetSpec.datasetID
            self.cur.execute(sqlCT + comment, varMap)
            resCT = self.cur.fetchone()
            (iFile,) = resCT
            # insert files
            varMap = {}
            varMap[":jediTaskID"] = datasetSpec.jediTaskID
            varMap[":datasetID"] = datasetSpec.datasetID
            self.cur.execute(sqlFR + comment, varMap)
            # update dataset
            if iFile > 0:
                varMap = {}
                varMap[":jediTaskID"] = datasetSpec.jediTaskID
                varMap[":datasetID"] = datasetSpec.datasetID
                varMap[":iFiles"] = iFile
                self.cur.execute(sqlDU + comment, varMap)
            tmpLog.debug(f"inserted {iFile} files")
            return iFile
        except Exception:
            # error
            self.dump_error_message(tmpLog)
            return 0

    # increase seq numbers
    def increaseSeqNumber_JEDI(self, datasetSpec, n_records):
        comment = " /* JediDBProxy.increaseSeqNumber_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={datasetSpec.jediTaskID} datasetID={datasetSpec.datasetID}")
        tmpLog.debug("start")
        try:
            # sql to get max LFN
            sqlCT = (
                "SELECT lfn,maxAttempt,maxFailure FROM {0}.JEDI_Dataset_Contents "
                "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
                "AND fileID=("
                "SELECT MAX(fileID) FROM {0}.JEDI_Dataset_Contents "
                "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID"
                ") "
            ).format(panda_config.schemaJEDI)
            varMap = {}
            varMap[":jediTaskID"] = datasetSpec.jediTaskID
            varMap[":datasetID"] = datasetSpec.datasetID
            self.cur.execute(sqlCT + comment, varMap)
            resCT = self.cur.fetchone()
            baseLFN, maxAttempt, maxFailure = resCT
            baseLFN = int(baseLFN) + 1
            # current date
            timeNow = naive_utcnow()
            # make files
            varMaps = []
            n_records = math.ceil(n_records)
            for i in range(n_records):
                fileSpec = JediFileSpec()
                fileSpec.jediTaskID = datasetSpec.jediTaskID
                fileSpec.datasetID = datasetSpec.datasetID
                fileSpec.GUID = str(uuid.uuid4())
                fileSpec.type = datasetSpec.type
                fileSpec.status = "ready"
                fileSpec.proc_status = "ready"
                fileSpec.lfn = baseLFN + i
                fileSpec.scope = None
                fileSpec.fsize = 0
                fileSpec.checksum = None
                fileSpec.creationDate = timeNow
                fileSpec.attemptNr = 0
                fileSpec.failedAttempt = 0
                fileSpec.maxAttempt = maxAttempt
                fileSpec.maxFailure = maxFailure
                fileSpec.ramCount = 0
                # make vars
                varMap = fileSpec.valuesMap(useSeq=True)
                varMaps.append(varMap)
            # sql for insert
            sqlIn = f"INSERT INTO {panda_config.schemaJEDI}.JEDI_Dataset_Contents ({JediFileSpec.columnNames()}) "
            sqlIn += JediFileSpec.bindValuesExpression()
            self.cur.executemany(sqlIn + comment, varMaps)
            # sql to update dataset record
            sqlDU = f"UPDATE {panda_config.schemaJEDI}.JEDI_Datasets "
            sqlDU += "SET nFiles=nFiles+:iFiles,nFilesTobeUsed=nFilesTobeUsed+:iFiles "
            sqlDU += "WHERE jediTaskID=:jediTaskID AND datasetID=:datasetID "
            varMap = {}
            varMap[":jediTaskID"] = datasetSpec.jediTaskID
            varMap[":datasetID"] = datasetSpec.datasetID
            varMap[":iFiles"] = n_records
            self.cur.execute(sqlDU + comment, varMap)
            tmpLog.debug(f"inserted {n_records} files")
            return n_records
        except Exception:
            # error
            self.dump_error_message(tmpLog)
            return 0

    # get JEDI task with ID
    def getTaskWithID_JEDI(self, jediTaskID, fullFlag, lockTask=False, pid=None, lockInterval=None, clearError=False):
        comment = " /* JediDBProxy.getTaskWithID_JEDI */"
        tmpLog = self.create_tagged_logger(comment, f"jediTaskID={jediTaskID}")
        tmpLog.debug(f"start lockTask={lockTask}")
        # return value for failure
        failedRet = False, None
        try:
            # sql
            sql = f"SELECT {JediTaskSpec.columnNames()} "
            sql += f"FROM {panda_config.schemaJEDI}.JEDI_Tasks WHERE jediTaskID=:jediTaskID "
            if lockInterval is not None:
                sql += "AND (lockedTime IS NULL OR lockedTime<:timeLimit) "
            if lockTask:
                sql += "AND lockedBy IS NULL FOR UPDATE NOWAIT"
            sqlLock = f"UPDATE {panda_config.schemaJEDI}.JEDI_Tasks SET lockedBy=:lockedBy,lockedTime=CURRENT_DATE"
            if clearError:
                sqlLock += ",errorDialog=NULL"
            sqlLock += " WHERE jediTaskID=:jediTaskID "
            varMap = {}
            varMap[":jediTaskID"] = jediTaskID
            if lockInterval is not None:
                varMap[":timeLimit"] = naive_utcnow() - datetime.timedelta(minutes=lockInterval)
            # begin transaction
            self.conn.begin()
            # select
            res = None
            try:
                self.cur.execute(sql + comment, varMap)
                res = self.cur.fetchone()
                if res is not None:
                    # template to generate job parameters
                    jobParamsTemplate = None
                    if fullFlag:
                        # sql to read template
                        sqlJobP = f"SELECT jobParamsTemplate FROM {panda_config.schemaJEDI}.JEDI_JobParams_Template "
                        sqlJobP += "WHERE jediTaskID=:jediTaskID "
                        self.cur.execute(sqlJobP + comment, varMap)
                        for (clobJobP,) in self.cur:
                            if clobJobP is not None:
                                jobParamsTemplate = clobJobP
                                break
                    if lockTask:
                        varMap = {}
                        varMap[":lockedBy"] = pid
                        varMap[":jediTaskID"] = jediTaskID
                        self.cur.execute(sqlLock + comment, varMap)
            except Exception:
                errType, errValue = sys.exc_info()[:2]
                if self.isNoWaitException(errValue):
                    # resource busy and acquire with NOWAIT specified
                    tmpLog.debug("skip locked")
                else:
                    # failed with something else
                    raise errType(errValue)
            # commit
            if not self._commit():
                raise RuntimeError("Commit error")
            if res is not None:
                taskSpec = JediTaskSpec()
                taskSpec.pack(res)
                if jobParamsTemplate is not None:
                    taskSpec.jobParamsTemplate = jobParamsTemplate
            else:
                taskSpec = None
            if taskSpec is None:
                tmpLog.debug("done with skip")
            else:
                tmpLog.debug("done with got")
            return True, taskSpec
        except Exception:
            # roll back
            self._rollback()
            # error
            self.dump_error_message(tmpLog)
            return failedRet

    # check parent task status
    def checkParentTask_JEDI(self, parent_task_id: int, jedi_task_id: int = None, use_commit: bool = True) -> str | None:
        """
        Check the status of parent task
        Args:
            parent_task_id: JEDI task ID of parent task
            jedi_task_id: JEDI task ID of child task (for logging purpose)
            use_commit: whether to use commit/rollback
        Returns:
          "completed": parent task is done/finished
          "corrupted": parent task is broken/aborted/failed
          "running": parent task is still running
          "unknown": parent task is not found
          None: error
        """
        comment = " /* JediDBProxy.checkParentTask_JEDI */"
        tmp_log = self.create_tagged_logger(comment, f"jediTaskID={jedi_task_id} parent={parent_task_id}")
        tmp_log.debug("start")
        ret_val = None
        try:

            sql = f"SELECT status FROM {panda_config.schemaJEDI}.JEDI_Tasks "
            sql += "WHERE jediTaskID=:jediTaskID "
            var_map = {":jediTaskID": parent_task_id}
            # start transaction
            if use_commit:
                self.conn.begin()
            self.cur.execute(sql + comment, var_map)
            res_tk = self.cur.fetchone()
            if use_commit:
                # commit
                if not self._commit():
                    raise RuntimeError("Commit error")
            if res_tk is None:
                tmp_log.error("parent not found")
                ret_val = "unknown"
            else:
                # task status
                (task_status,) = res_tk
                tmp_log.debug(f"parent status = {task_status}")
                if task_status in ["done", "finished"]:
                    # parent is completed
                    ret_val = "completed"
                elif task_status in ["broken", "aborted", "failed"]:
                    # parent is corrupted
                    ret_val = "corrupted"
                else:
                    # parent is running
                    ret_val = "running"
            # return
            tmp_log.debug(f"done with {ret_val}")
            return ret_val
        except Exception:
            if use_commit:
                # roll back
                self._rollback()
            # error
            self.dump_error_message(tmp_log)
            return ret_val


# get module
def get_task_utils_module(base_mod) -> TaskUtilsModule:
    return base_mod.get_composite_module("task_utils")
