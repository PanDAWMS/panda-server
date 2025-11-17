import datetime
import json
import os
import re
import socket
import sys
import time
import traceback
from typing import Any

from packaging import version
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jedicore import Interaction
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandaserver.brokerage.SiteMapper import SiteMapper
from pandaserver.dataservice import DataServiceUtils
from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.taskbuffer import JobUtils, ProcessGroups, SiteSpec


# get nuclei where data is available
def getNucleiWithData(siteMapper, ddmIF, datasetName, candidateNuclei, deepScan=False):
    # get replicas
    try:
        replicaMap = ddmIF.listReplicasPerDataset(datasetName, deepScan)
    except Exception:
        errtype, errvalue = sys.exc_info()[:2]
        return errtype, f"ddmIF.listReplicasPerDataset failed with {errvalue}", None
    # check if remote source is available at any sites (not only nuclei)
    remote_source_available = False
    for tmp_replica_data in replicaMap.values():
        for tmp_location, tmp_stat_data in tmp_replica_data.items():
            if tmp_stat_data[0]["total"] in [None, 0]:
                continue
            if tmp_stat_data[0]["total"] != tmp_stat_data[0]["found"]:
                continue
            if siteMapper.is_readable_remotely(tmp_location):
                remote_source_available = True
                break
    # loop over all clouds
    retMap = {}
    for tmpNucleus in candidateNuclei:
        tmpNucleusSpec = siteMapper.getNucleus(tmpNucleus)
        # loop over all datasets
        totalNum = 0
        totalSize = 0
        avaNumDisk = 0
        avaNumAny = 0
        avaSizeDisk = 0
        avaSizeAny = 0
        can_be_remote_source = False
        for tmpDataset, tmpRepMap in replicaMap.items():
            tmpTotalNum = 0
            tmpTotalSize = 0
            tmpAvaNumDisk = 0
            tmpAvaNumAny = 0
            tmpAvaSizeDisk = 0
            tmpAvaSizeAny = 0
            # loop over all endpoints
            for tmpLoc, locData in tmpRepMap.items():
                # get total
                if tmpTotalNum == 0:
                    tmpTotalNum = locData[0]["total"]
                    tmpTotalSize = locData[0]["tsize"]
                # check if the endpoint is associated
                if tmpNucleusSpec.is_associated_for_input(tmpLoc):
                    # check blacklist
                    if siteMapper.is_readable_remotely(tmpLoc):
                        can_be_remote_source = True
                    # sum
                    tmpEndpoint = tmpNucleusSpec.getEndpoint(tmpLoc)
                    tmpAvaNum = locData[0]["found"]
                    tmpAvaSize = locData[0]["asize"]
                    # disk
                    if tmpEndpoint["is_tape"] != "Y":
                        # complete replica is available at DISK
                        if tmpTotalNum == tmpAvaNum and tmpTotalNum > 0:
                            tmpAvaNumDisk = tmpAvaNum
                            tmpAvaNumAny = tmpAvaNum
                            tmpAvaSizeDisk = tmpAvaSize
                            tmpAvaSizeAny = tmpAvaSize
                            break
                        if tmpAvaNum > tmpAvaNumDisk:
                            tmpAvaNumDisk = tmpAvaNum
                            tmpAvaSizeDisk = tmpAvaSize
                    # tape
                    if tmpAvaNumAny < tmpAvaNum:
                        tmpAvaNumAny = tmpAvaNum
                        tmpAvaSizeAny = tmpAvaSize
            # total
            totalNum = tmpTotalNum
            totalSize = tmpTotalSize
            avaNumDisk += tmpAvaNumDisk
            avaNumAny += tmpAvaNumAny
            avaSizeDisk += tmpAvaSizeDisk
            avaSizeAny += tmpAvaSizeAny
        # append
        if tmpNucleus in candidateNuclei or avaNumAny > 0:
            retMap[tmpNucleus] = {
                "tot_num": totalNum,
                "tot_size": totalSize,
                "ava_num_disk": avaNumDisk,
                "ava_num_any": avaNumAny,
                "ava_size_disk": avaSizeDisk,
                "ava_size_any": avaSizeAny,
                "can_be_remote_source": can_be_remote_source,
            }
    # return
    return Interaction.SC_SUCCEEDED, retMap, remote_source_available


# get sites where data is available and check if complete replica is available at online RSE
def get_sites_with_data(
    site_list: list,
    site_mapper: SiteMapper,
    ddm_if: DDMInterface,
    dataset_name: str,
    element_list: list,
    max_missing_input_files: int,
    min_input_completeness: int,
) -> tuple[Any, dict | str, bool | None, bool | None, bool | None, bool | None, bool | None, list]:
    """
    Get sites where data is available and check if complete replica is available at online RSE
    1) regarded_as_complete_disk: True if a replica is regarded as complete at disk (missing files within threshold)
    2) complete_tape: True if a complete replica is available at tape
    3) truly_complete_disk: True if a truly complete replica is available at disk (no missing files)
    4) can_be_local_source: True if the site can read the replica locally over LAN
    5) can_be_remote_source: True if the site can read the replica remotely over WAN
    6) list_of_complete_replica_locations : list of RSEs where truly complete replica is available at disk
    Note that VP replicas are not taken into account for the above flags

    :param site_list: list of site names to be checked
    :param site_mapper: SiteMapper object
    :param ddm_if: DDMInterface object
    :param dataset_name: dataset name
    :param element_list: list of constituent datasets
    :param max_missing_input_files: maximum number of missing files to be regarded as complete
    :param min_input_completeness: minimum completeness (%) to be regarded as complete

    :return: tuple of (status code or exception type, dict of sites with data availability info, regarded_as_complete_disk, complete_tape, truly_complete_disk, can_be_local_source, can_be_remote_source, list_of_complete_replica_locations)
    """
    # get replicas
    try:
        replica_map = {
            dataset_name: ddm_if.listDatasetReplicas(dataset_name, use_vp=True, skip_incomplete_element=True, element_list=element_list, use_deep=True)
        }
    except Exception:
        errtype, errvalue = sys.exc_info()[:2]
        return errtype, f"ddmIF.listDatasetReplicas failed with {errvalue}", None, None, None, None, None, []

    # check completeness and storage availability
    is_tape = {}
    replica_availability_info = {}
    list_of_complete_replica_locations = []
    for tmp_rse, tmp_data_list in replica_map[dataset_name].items():
        # check tape attribute
        try:
            is_tape[tmp_rse] = ddm_if.getSiteProperty(tmp_rse, "is_tape")
        except Exception:
            is_tape[tmp_rse] = None
        # look for complete replicas
        tmp_regarded_as_complete_disk = False
        tmp_truly_complete_disk = False
        tmp_complete_tape = False
        tmp_can_be_local_source = False
        tmp_can_be_remote_source = False
        for tmp_data in tmp_data_list:
            if not tmp_data.get("vp"):
                if tmp_data["found"] == tmp_data["total"]:
                    # truly complete
                    truly_complete = True
                elif (
                    tmp_data["total"]
                    and tmp_data["found"]
                    and (
                        tmp_data["total"] - tmp_data["found"] <= max_missing_input_files
                        or tmp_data["found"] / tmp_data["total"] * 100 >= min_input_completeness
                    )
                ):
                    # regarded as complete
                    truly_complete = False
                else:
                    continue
                list_of_complete_replica_locations.append(tmp_rse)
                if is_tape[tmp_rse]:
                    tmp_complete_tape = True
                else:
                    tmp_regarded_as_complete_disk = True
                    if truly_complete:
                        tmp_truly_complete_disk = True
                # check if it is locally accessible over LAN
                if site_mapper.is_readable_locally(tmp_rse):
                    tmp_can_be_local_source = True
                # check if it is remotely accessible over WAN
                if site_mapper.is_readable_remotely(tmp_rse):
                    tmp_can_be_remote_source = True
        replica_availability_info[tmp_rse] = {
            "regarded_as_complete_disk": tmp_regarded_as_complete_disk,
            "truly_complete_disk": tmp_truly_complete_disk,
            "complete_tape": tmp_complete_tape,
            "can_be_local_source": tmp_can_be_local_source,
            "can_be_remote_source": tmp_can_be_remote_source,
        }

    # loop over all candidate sites
    regarded_as_complete_disk = False
    truly_complete_disk = False
    complete_tape = False
    can_be_local_source = False
    can_be_remote_source = False
    return_map = {}
    if not site_list:
        # make sure at least one loop to set the flags
        site_list = [None]
    for tmp_site_name in site_list:
        if not site_mapper.checkSite(tmp_site_name):
            continue
        # get associated DDM endpoints
        tmp_site_spec = site_mapper.getSite(tmp_site_name)
        scope_input, scope_output = select_scope(tmp_site_spec, JobUtils.ANALY_PS, JobUtils.ANALY_PS)
        try:
            input_endpoints = tmp_site_spec.ddm_endpoints_input[scope_input].all.keys()
        except Exception:
            input_endpoints = {}
        # loop over all associated endpoints
        for tmp_rse in input_endpoints:
            if tmp_rse in replica_map[dataset_name]:
                # check completeness
                tmp_statistics = replica_map[dataset_name][tmp_rse][-1]
                if tmp_statistics["found"] is None:
                    tmp_dataset_completeness = "unknown"
                    # refresh request
                    pass
                elif tmp_statistics["total"] == tmp_statistics["found"]:
                    tmp_dataset_completeness = "complete"
                else:
                    tmp_dataset_completeness = "incomplete"
                # append
                if tmp_site_name not in return_map:
                    return_map[tmp_site_name] = {}
                return_map[tmp_site_name][tmp_rse] = {"tape": is_tape[tmp_rse], "state": tmp_dataset_completeness}
                if "vp" in tmp_statistics:
                    return_map[tmp_site_name][tmp_rse]["vp"] = tmp_statistics["vp"]
    # set flags
    for tmp_rse in replica_availability_info:
        if replica_availability_info[tmp_rse]["can_be_local_source"] or replica_availability_info[tmp_rse]["can_be_remote_source"]:
            if replica_availability_info[tmp_rse]["regarded_as_complete_disk"]:
                regarded_as_complete_disk = True
            if replica_availability_info[tmp_rse]["truly_complete_disk"]:
                truly_complete_disk = True
            if replica_availability_info[tmp_rse]["complete_tape"]:
                complete_tape = True
            if replica_availability_info[tmp_rse]["can_be_local_source"]:
                can_be_local_source = True
            if replica_availability_info[tmp_rse]["can_be_remote_source"]:
                can_be_remote_source = True
    # return
    return (
        Interaction.SC_SUCCEEDED,
        return_map,
        regarded_as_complete_disk,
        complete_tape,
        truly_complete_disk,
        can_be_local_source,
        can_be_remote_source,
        list_of_complete_replica_locations,
    )


# get analysis sites where data is available at disk
def getAnalSitesWithDataDisk(dataSiteMap, includeTape=False, use_vp=True, use_incomplete=False):
    sites_with_complete_replicas = []
    sites_with_incomplete_replicas = []
    sites_with_non_vp_disk_replicas = set()
    sites_using_vp = set()
    for tmpSiteName, tmpSeValMap in dataSiteMap.items():
        for tmpSE, tmpValMap in tmpSeValMap.items():
            # VP
            if tmpValMap.get("vp"):
                sites_using_vp.add(tmpSiteName)
                if not use_vp:
                    continue
            # on disk or tape
            if includeTape or not tmpValMap["tape"]:
                # complete replica available at disk, tape, or VP
                if tmpValMap["state"] == "complete":
                    # has complete replica
                    if tmpSiteName not in sites_with_complete_replicas:
                        sites_with_complete_replicas.append(tmpSiteName)
                    # has non-VP disk replica
                    if not tmpValMap["tape"] and not tmpValMap.get("vp"):
                        sites_with_non_vp_disk_replicas.add(tmpSiteName)
                else:
                    # incomplete replica at disk
                    if tmpSiteName not in sites_with_incomplete_replicas:
                        sites_with_incomplete_replicas.append(tmpSiteName)
    # remove VP if complete non-VP disk replica is unavailable
    if sites_with_non_vp_disk_replicas:
        for tmpSiteNameVP in sites_using_vp:
            if tmpSiteNameVP in sites_with_complete_replicas:
                sites_with_complete_replicas.remove(tmpSiteNameVP)
            if tmpSiteNameVP in sites_with_incomplete_replicas:
                sites_with_incomplete_replicas.remove(tmpSiteNameVP)
    # return sites with complete
    if sites_with_complete_replicas:
        if not use_incomplete or sites_with_non_vp_disk_replicas:
            return sites_with_complete_replicas
        else:
            return sites_with_complete_replicas + sites_with_incomplete_replicas
    # return sites with incomplete if complete is unavailable
    return sites_with_incomplete_replicas


# get the number of jobs in a status
def getNumJobs(jobStatMap, computingSite, jobStatus, cloud=None, workQueue_tag=None):
    if computingSite not in jobStatMap:
        return 0
    nJobs = 0
    # loop over all workQueues
    for tmpWorkQueue, tmpWorkQueueVal in jobStatMap[computingSite].items():
        # workQueue is defined
        if workQueue_tag is not None and workQueue_tag != tmpWorkQueue:
            continue
        # loop over all job status
        for tmpJobStatus, tmpCount in tmpWorkQueueVal.items():
            if tmpJobStatus == jobStatus:
                nJobs += tmpCount
    # return
    return nJobs


def get_total_nq_nr_ratio(job_stat_map, work_queue_tag=None):
    """
    Get the ratio of number of queued jobs to number of running jobs
    """
    nRunning = 0
    nQueue = 0

    # loop over all workQueues
    for siteVal in job_stat_map.values():
        for tmpWorkQueue in siteVal:
            # workQueue is defined
            if work_queue_tag is not None and work_queue_tag != tmpWorkQueue:
                continue
            tmpWorkQueueVal = siteVal[tmpWorkQueue]
            # loop over all job status
            for tmpJobStatus in ["defined", "assigned", "activated", "starting"]:
                if tmpJobStatus in tmpWorkQueueVal:
                    nQueue += tmpWorkQueueVal[tmpJobStatus]
            if "running" in tmpWorkQueueVal:
                nRunning += tmpWorkQueueVal["running"]
    try:
        ratio = float(nQueue) / float(nRunning)
    except Exception:
        ratio = None

    return ratio


def hasZeroShare(site_spec, task_spec, ignore_priority, tmp_log):
    """
    Check if the site has a zero share for the given task. Zero share means there is a policy preventing the site to be used for the task.

    :param site_spec: SiteSpec object describing the site being checked in brokerage
    :param task_spec: TaskSpec object describing the task being brokered
    :param ignore_priority: Merging job chunks will skip the priority check
    :param tmp_log: Logger object

    :return: False means there is no policy defined and the site can be used.
             True means the site has a fair share policy that prevents the site to be used for the task
    """

    # there is no per-site share defined (CRIC "fairsharePolicy" field), the site can be used
    if site_spec.fairsharePolicy in ["", None]:
        return False

    try:
        # get the group of processing types from a pre-defined mapping
        processing_group = ProcessGroups.getProcessGroup(task_spec.processingType)

        # don't suppress test tasks - the site can be used
        if processing_group in ["test"]:
            return False

        # loop over all policies
        for policy in site_spec.fairsharePolicy.split(","):
            # Examples of policies are:
            # type=evgen:100%,type=simul:100%,type=any:0%
            # type=evgen:100%,type=simul:100%,type=any:0%,group=(AP_Higgs|AP_Susy|AP_Exotics|Higgs):0%
            # gshare=Express:100%,gshare=any:0%
            # priority>400:0
            tmp_processing_type = None
            tmp_working_group = None
            tmp_priority = None
            tmp_gshare = None
            tmp_fair_share = policy.split(":")[-1]

            # break down each fair share policy into its fields
            for tmp_field in policy.split(":"):
                if tmp_field.startswith("type="):
                    tmp_processing_type = tmp_field.split("=")[-1]
                elif tmp_field.startswith("group="):
                    tmp_working_group = tmp_field.split("=")[-1]
                elif tmp_field.startswith("gshare="):
                    tmp_gshare = tmp_field.split("=")[-1]
                elif tmp_field.startswith("priority"):
                    tmp_priority = re.sub("priority", "", tmp_field)

            # check for a matching processing type
            if tmp_processing_type not in ["any", None]:
                if "*" in tmp_processing_type:
                    tmp_processing_type = tmp_processing_type.replace("*", ".*")
                # if there is no match between the site's fair share policy and the task's processing type,
                # so continue looking for other policies that trigger the zero share condition
                if re.search(f"^{tmp_processing_type}$", processing_group) is None:
                    continue

            # check for matching working group
            if tmp_working_group not in ["any", None] and task_spec.workingGroup is not None:
                if "*" in tmp_working_group:
                    tmp_working_group = tmp_working_group.replace("*", ".*")
                # if there is no match between the site's fair share policy and the task's working group
                # continue looking for other policies that trigger the zero share condition
                if re.search(f"^{tmp_working_group}$", task_spec.workingGroup) is None:
                    continue

            # check for matching gshare. Note that this only works for "leave gshares" in the fairsharePolicy,
            # i.e. the ones that have no sub-gshares, since the task only gets "leave gshares" assigned
            if tmp_gshare not in ["any", None] and task_spec.gshare is not None:
                if "*" in tmp_gshare:
                    tmp_gshare = tmp_gshare.replace("*", ".*")
                # if there is no match between the site's fair share policy and the task's gshare
                # continue looking for other policies that trigger the zero share condition
                if re.search(f"^{tmp_gshare}$", task_spec.gshare) is None:
                    continue

            # check priority
            if tmp_priority is not None and not ignore_priority:
                try:
                    exec(f"tmpStat = {task_spec.currentPriority}{tmp_priority}", globals())
                    tmp_log.debug(
                        f"Priority check for {site_spec.sitename}, {task_spec.currentPriority}): " f"{task_spec.currentPriority}{tmp_priority} = {tmpStat}"
                    )
                    if not tmpStat:
                        continue
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    tmp_log.error(f"Priority check for {site_spec.sitename} failed with {error_type}:{error_value}")

            # check fair share value
            # if 0, we need to skip the site
            # if different than 0, the site can be used
            if tmp_fair_share in ["0", "0%"]:
                return True
            else:
                return False

    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        tmp_log.error(f"hasZeroShare failed with {error_type}:{error_value}")

    # if we reach this point, it means there is no policy preventing the site to be used
    return False


# check if site name is matched with one of list items
def isMatched(siteName, nameList):
    for tmpName in nameList:
        # ignore empty
        if tmpName == "":
            continue
        # wild card
        if "*" in tmpName:
            tmpName = tmpName.replace("*", ".*")
            if re.search(tmpName, siteName) is not None:
                return True
        else:
            # normal pattern
            if tmpName == siteName:
                return True
    # return
    return False


# get dict to set nucleus
def getDictToSetNucleus(nucleusSpec, tmpDatasetSpecs):
    # get destinations
    retMap = {"datasets": [], "nucleus": nucleusSpec.name}
    for datasetSpec in tmpDatasetSpecs:
        # skip distributed datasets
        if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None:
            continue
        # get endpoint relevant to token
        endPoint = nucleusSpec.getAssociatedEndpoint(datasetSpec.storageToken)
        if endPoint is None and not nucleusSpec.is_nucleus() and nucleusSpec.get_default_endpoint_out():
            # use default endpoint for satellite that doesn't have relevant endpoint
            endPoint = nucleusSpec.get_default_endpoint_out()
        if endPoint is None:
            continue
        token = endPoint["ddm_endpoint_name"]
        # add original token
        if datasetSpec.storageToken not in ["", None]:
            token += f"/{datasetSpec.storageToken.split('/')[-1]}"
        retMap["datasets"].append({"datasetID": datasetSpec.datasetID, "token": f"dst:{token}", "destination": f"nucleus:{nucleusSpec.name}"})
    return retMap


# remove problematic sites
def skipProblematicSites(candidateSpecList, ngSites, sitesUsedByTask, preSetSiteSpec, maxNumSites, tmpLog):
    skippedSites = set()
    usedSitesGood = []
    newSitesGood = []
    # collect sites already used by the task
    for candidateSpec in candidateSpecList:
        # check if problematic
        if (candidateSpec.siteName in ngSites or candidateSpec.unifiedName in ngSites) and (
            preSetSiteSpec is None or candidateSpec.siteName != preSetSiteSpec.siteName
        ):
            skippedSites.add(candidateSpec.siteName)
        else:
            if candidateSpec.siteName in sitesUsedByTask or candidateSpec.unifiedName in sitesUsedByTask:
                usedSitesGood.append(candidateSpec)
            else:
                newSitesGood.append(candidateSpec)
    # set number of sites if undefined
    if maxNumSites in [0, None]:
        maxNumSites = len(candidateSpecList)
    newcandidateSpecList = usedSitesGood + newSitesGood
    newcandidateSpecList = newcandidateSpecList[:maxNumSites]
    # dump
    for skippedSite in skippedSites:
        tmpLog.debug(f"getting rid of problematic site {skippedSite}")
    return newcandidateSpecList


# get mapping between sites and input storage endpoints
def getSiteInputStorageEndpointMap(site_list, site_mapper, prod_source_label, job_label):
    # make a map of panda sites to ddm endpoints
    ret_map = {}
    for site_name in site_list:
        tmp_site_spec = site_mapper.getSite(site_name)
        scope_input, scope_output = select_scope(tmp_site_spec, prod_source_label, job_label)

        # skip if scope not available
        if scope_input not in tmp_site_spec.ddm_endpoints_input:
            continue

        # add the schedconfig.ddm endpoints
        ret_map[site_name] = list(tmp_site_spec.ddm_endpoints_input[scope_input].all.keys())

    return ret_map


# get to-running rate of sites from various resources
CACHE_SiteToRunRateStats = {}


def getSiteToRunRateStats(tbIF, vo, time_window=21600, cutoff=300, cache_lifetime=600):
    # initialize
    ret_val = False
    ret_map = {}
    # DB cache keys
    dc_main_key = "AtlasSites"
    dc_sub_key = "SiteToRunRate"
    # arguments for process lock
    this_prodsourcelabel = "user"
    this_pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}_{os.getpgrp()}-broker"
    this_component = "Cache.SiteToRunRate"
    # timestamps
    current_time = naive_utcnow()
    starttime_max = current_time - datetime.timedelta(seconds=cutoff)
    starttime_min = current_time - datetime.timedelta(seconds=time_window)
    # rounded with 10 minutes
    starttime_max_rounded = starttime_max.replace(minute=starttime_max.minute // 10 * 10, second=0, microsecond=0)
    starttime_min_rounded = starttime_min.replace(minute=starttime_min.minute // 10 * 10, second=0, microsecond=0)
    real_interval_hours = (starttime_max_rounded - starttime_min_rounded).total_seconds() / 3600
    # local cache key
    local_cache_key = (starttime_min_rounded, starttime_max_rounded)
    # condition of query
    if local_cache_key in CACHE_SiteToRunRateStats and current_time <= CACHE_SiteToRunRateStats[local_cache_key]["exp"]:
        # query from local cache
        ret_val = True
        ret_map = CACHE_SiteToRunRateStats[local_cache_key]["data"]
    else:
        # try some times
        for _ in range(99):
            # skip if too long after original current time
            if naive_utcnow() - current_time > datetime.timedelta(seconds=min(10, cache_lifetime / 4)):
                # break trying
                break
            try:
                # query from DB cache
                cache_spec = tbIF.getCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key)
                if cache_spec is not None:
                    expired_time = cache_spec.last_update + datetime.timedelta(seconds=cache_lifetime)
                    if current_time <= expired_time:
                        # valid DB cache
                        ret_val = True
                        ret_map = json.loads(cache_spec.data)
                        # fill local cache
                        CACHE_SiteToRunRateStats[local_cache_key] = {"exp": expired_time, "data": ret_map}
                        # break trying
                        break
                # got process lock
                got_lock = tbIF.lockProcess_JEDI(
                    vo=vo,
                    prodSourceLabel=this_prodsourcelabel,
                    cloud=None,
                    workqueue_id=None,
                    resource_name=None,
                    component=this_component,
                    pid=this_pid,
                    timeLimit=5,
                )
                if not got_lock:
                    # not getting lock, sleep and query cache again
                    time.sleep(1)
                    continue
                # query from PanDA DB directly
                ret_val, ret_map = tbIF.getSiteToRunRateStats(vo=vo, exclude_rwq=False, starttime_min=starttime_min, starttime_max=starttime_max)
                if ret_val:
                    # expired time
                    expired_time = current_time + datetime.timedelta(seconds=cache_lifetime)
                    # fill local cache
                    CACHE_SiteToRunRateStats[local_cache_key] = {"exp": expired_time, "data": ret_map}
                    # json of data
                    data_json = json.dumps(ret_map)
                    # fill DB cache
                    tbIF.updateCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key, data=data_json)
                # unlock process
                tbIF.unlockProcess_JEDI(
                    vo=vo, prodSourceLabel=this_prodsourcelabel, cloud=None, workqueue_id=None, resource_name=None, component=this_component, pid=this_pid
                )
                # break trying
                break
            except Exception as e:
                # dump error message
                err_str = f"AtlasBrokerUtils.getSiteToRunRateStats got {e.__class__.__name__}: {e} \n"
                sys.stderr.write(err_str)
                # break trying
                break
        # delete outdated entries in local cache
        for lc_key in list(CACHE_SiteToRunRateStats.keys()):
            lc_time_min, lc_time_max = lc_key
            if lc_time_max < starttime_max_rounded - datetime.timedelta(seconds=cache_lifetime):
                try:
                    del CACHE_SiteToRunRateStats[lc_key]
                except Exception as e:
                    err_str = f"AtlasBrokerUtils.getSiteToRunRateStats when deleting outdated entries got {e.__class__.__name__}: {e} \n"
                    sys.stderr.write(err_str)
    # return
    return ret_val, ret_map


# get users jobs stats from various resources
CACHE_UsersJobsStats = {}


def getUsersJobsStats(tbIF, vo, prod_source_label, cache_lifetime=60):
    # initialize
    ret_val = False
    ret_map = {}
    # DB cache keys
    dc_main_key = "AtlasSites"
    dc_sub_key = "UsersJobsStats"
    # arguments for process lock
    this_prodsourcelabel = prod_source_label
    this_pid = this_pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}_{os.getpgrp()}-broker"
    this_component = "Cache.UsersJobsStats"
    # local cache key; a must if not using global variable
    local_cache_key = "_main"
    # timestamps
    current_time = naive_utcnow()
    # condition of query
    if local_cache_key in CACHE_UsersJobsStats and current_time <= CACHE_UsersJobsStats[local_cache_key]["exp"]:
        # query from local cache
        ret_val = True
        ret_map = CACHE_UsersJobsStats[local_cache_key]["data"]
    else:
        # try some times
        for _ in range(99):
            # skip if too long after original current time
            if naive_utcnow() - current_time > datetime.timedelta(seconds=min(15, cache_lifetime / 4)):
                # break trying
                break
            try:
                # query from DB cache
                cache_spec = tbIF.getCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key)
                if cache_spec is not None:
                    expired_time = cache_spec.last_update + datetime.timedelta(seconds=cache_lifetime)
                    if current_time <= expired_time:
                        # valid DB cache
                        ret_val = True
                        ret_map = json.loads(cache_spec.data)
                        # fill local cache
                        CACHE_UsersJobsStats[local_cache_key] = {"exp": expired_time, "data": ret_map}
                        # break trying
                        break
                # got process lock
                got_lock = tbIF.lockProcess_JEDI(
                    vo=vo,
                    prodSourceLabel=this_prodsourcelabel,
                    cloud=None,
                    workqueue_id=None,
                    resource_name=None,
                    component=this_component,
                    pid=this_pid,
                    timeLimit=(cache_lifetime * 0.75 / 60),
                )
                if not got_lock:
                    # not getting lock, sleep and query cache again
                    time.sleep(1)
                    continue
                # query from PanDA DB directly
                ret_map = tbIF.getUsersJobsStats_JEDI(prod_source_label=this_prodsourcelabel)
                if ret_map is not None:
                    # expired time
                    expired_time = current_time + datetime.timedelta(seconds=cache_lifetime)
                    # fill local cache
                    CACHE_UsersJobsStats[local_cache_key] = {"exp": expired_time, "data": ret_map}
                    # json of data
                    data_json = json.dumps(ret_map)
                    # fill DB cache
                    tbIF.updateCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key, data=data_json)
                    # make True return
                    ret_val = True
                # unlock process
                tbIF.unlockProcess_JEDI(
                    vo=vo, prodSourceLabel=this_prodsourcelabel, cloud=None, workqueue_id=None, resource_name=None, component=this_component, pid=this_pid
                )
                # break trying
                break
            except Exception as e:
                # dump error message
                err_str = f"AtlasBrokerUtils.getUsersJobsStats got {e.__class__.__name__}: {e} \n"
                sys.stderr.write(err_str)
                # break trying
                break
    # return
    return ret_val, ret_map


# get gshare usage
def getGShareUsage(tbIF, gshare, fresher_than_minutes_ago=15):
    # initialize
    ret_val = False
    ret_map = {}
    # timestamps
    current_time = naive_utcnow()
    # try some times
    for _ in range(99):
        now_time = naive_utcnow()
        # skip if too long after original current time
        if now_time - current_time > datetime.timedelta(seconds=max(3, fresher_than_minutes_ago / 3)):
            # break trying
            break
        try:
            # query from PanDA DB directly
            sql_get_gshare = (
                """SELECT m.value_json """
                """FROM ATLAS_PANDA.Metrics m """
                """WHERE m.metric=:metric """
                """AND m.gshare=:gshare """
                """AND m.timestamp>=:min_timestamp """
            )
            # varMap
            varMap = {
                ":metric": "gshare_preference",
                ":gshare": gshare,
                ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
            }
            # result
            res = tbIF.querySQL(sql_get_gshare, varMap)
            if res:
                value_json = res[0][0]
                # json of data
                ret_map = json.loads(value_json)
                # make True return
                ret_val = True
            # break trying
            break
        except Exception as e:
            # dump error message
            err_str = f"AtlasBrokerUtils.getGShareUsage got {e.__class__.__name__}: {e} \n"
            sys.stderr.write(err_str)
            # break trying
            break
    # return
    return ret_val, ret_map


# get user evaluation
def getUserEval(tbIF, user, fresher_than_minutes_ago=20):
    # initialize
    ret_val = False
    ret_map = {}
    # timestamps
    current_time = naive_utcnow()
    # try some times
    for _ in range(99):
        now_time = naive_utcnow()
        # skip if too long after original current time
        if now_time - current_time > datetime.timedelta(seconds=max(3, fresher_than_minutes_ago / 3)):
            # break trying
            break
        try:
            # query from PanDA DB directly
            sql_get_user_eval = f'SELECT m.value_json."{user}" FROM ATLAS_PANDA.Metrics m WHERE m.metric=:metric AND m.timestamp>=:min_timestamp '
            # varMap
            varMap = {
                ":metric": "analy_user_eval",
                ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
            }
            # result
            res = tbIF.querySQL(sql_get_user_eval, varMap)
            if res:
                value_json = res[0][0]
                # json of data
                ret_map = json.loads(value_json) if value_json else None
                # make True return
                ret_val = True
            # break trying
            break
        except Exception as e:
            # dump error message
            err_str = f"AtlasBrokerUtils.getUserEval got {e.__class__.__name__}: {e} \n"
            sys.stderr.write(err_str)
            # break trying
            break
    # return
    return ret_val, ret_map


# get user task evaluation
def getUserTaskEval(tbIF, taskID, fresher_than_minutes_ago=15):
    # initialize
    ret_val = False
    ret_map = {}
    # timestamps
    current_time = naive_utcnow()
    # try some times
    for _ in range(99):
        now_time = naive_utcnow()
        # skip if too long after original current time
        if now_time - current_time > datetime.timedelta(seconds=max(3, fresher_than_minutes_ago / 3)):
            # break trying
            break
        try:
            # query from PanDA DB directly
            sql_get_task_eval = (
                """SELECT tev.value_json """
                """FROM ATLAS_PANDA.Task_Evaluation tev """
                """WHERE tev.metric=:metric """
                """AND tev.jediTaskID=:taskID """
                """AND tev.timestamp>=:min_timestamp """
            )
            # varMap
            varMap = {
                ":metric": "analy_task_eval",
                ":taskID": taskID,
                ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
            }
            # result
            res = tbIF.querySQL(sql_get_task_eval, varMap)
            if res:
                value_json = res[0][0]
                # json of data
                ret_map = json.loads(value_json) if value_json else None
                # make True return
                ret_val = True
            # break trying
            break
        except Exception as e:
            # dump error message
            err_str = f"AtlasBrokerUtils.getUserTaskEval got {e.__class__.__name__}: {e} \n"
            sys.stderr.write(err_str)
            # break trying
            break
    # return
    return ret_val, ret_map


# get analysis sites class
def getAnalySitesClass(tbIF, fresher_than_minutes_ago=60):
    # initialize
    ret_val = False
    ret_map = {}
    # timestamps
    current_time = naive_utcnow()
    # try some times
    for _ in range(99):
        now_time = naive_utcnow()
        # skip if too long after original current time
        if now_time - current_time > datetime.timedelta(seconds=max(3, fresher_than_minutes_ago / 3)):
            # break trying
            break
        try:
            # query from PanDA DB directly
            sql_get_task_eval = (
                """SELECT m.computingSite, m.value_json.class """
                """FROM ATLAS_PANDA.Metrics m """
                """WHERE m.metric=:metric """
                """AND m.timestamp>=:min_timestamp """
            )
            # varMap
            varMap = {
                ":metric": "analy_site_eval",
                ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
            }
            # result
            res = tbIF.querySQL(sql_get_task_eval, varMap)
            if res:
                for site, class_value in res:
                    ret_map[site] = int(class_value)
                    # make True return
                ret_val = True
            # break trying
            break
        except Exception as e:
            # dump error message
            err_str = f"AtlasBrokerUtils.getAnalySitesEval got {e.__class__.__name__}: {e} \n"
            sys.stderr.write(err_str)
            # break trying
            break
    # return
    return ret_val, ret_map


def compare_version_string(version_string, comparison_string):
    """
    Compares a version string with another string composed of a comparison operator and a version string.

    Args:
        version_string (str): The version string to compare.
        comparison_string (str): The string containing the comparison operator and version string (e.g., ">=2.0").

    Returns:
        bool or None: True if the version string satisfies the comparison, False if it doesn't,
                       or None if the comparison string is invalid.
    """
    match = re.match(r"([=><]+)(.+)", comparison_string)
    if not match:
        return None

    operator = match.group(1).strip()
    version_to_compare = match.group(2).strip()

    try:
        version1 = version.parse(version_string)
        version2 = version.parse(version_to_compare)
    except version.InvalidVersion:
        return None

    if operator == "==":
        return version1 == version2
    elif operator == "!=":
        return version1 != version2
    elif operator == ">=":
        return version1 >= version2
    elif operator == "<=":
        return version1 <= version2
    elif operator == ">":
        return version1 > version2
    elif operator == "<":
        return version1 < version2
    else:
        return None


# check SW with json
class JsonSoftwareCheck:
    # constructor
    def __init__(self, site_mapper, sw_map):
        self.siteMapper = site_mapper
        self.sw_map = sw_map

    # get lists
    def check(
        self,
        site_list,
        cvmfs_tag,
        sw_project,
        sw_version,
        cmt_config,
        need_cvmfs,
        cmt_config_only,
        need_container=False,
        container_name=None,
        only_tags_fc=False,
        host_cpu_specs=None,
        host_gpu_spec=None,
        log_stream=None,
    ):
        okSite = []
        noAutoSite = []
        for tmpSiteName in site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.releases == ["AUTO"] and tmpSiteName in self.sw_map:
                go_ahead = False
                try:
                    # convert to a dict
                    architecture_map = {}
                    if "architectures" in self.sw_map[tmpSiteName]:
                        for arch_spec in self.sw_map[tmpSiteName]["architectures"]:
                            if "type" in arch_spec:
                                architecture_map[arch_spec["type"]] = arch_spec
                    # check if need CPU
                    if "cpu" in architecture_map:
                        need_cpu = False
                        for k in architecture_map["cpu"]:
                            if isinstance(architecture_map["cpu"][k], list):
                                if "excl" in architecture_map["cpu"][k]:
                                    need_cpu = True
                                    break
                        if need_cpu and host_cpu_specs is None:
                            continue
                    # check if need GPU
                    if "gpu" in architecture_map:
                        need_gpu = False
                        for k in architecture_map["gpu"]:
                            if isinstance(architecture_map["gpu"][k], list):
                                if "excl" in architecture_map["gpu"][k]:
                                    need_gpu = True
                                    break
                        if need_gpu and host_gpu_spec is None:
                            continue
                    if host_cpu_specs or host_gpu_spec:
                        # skip since the PQ doesn't describe HW spec
                        if not architecture_map:
                            continue
                        # check CPU
                        if host_cpu_specs:
                            host_ok = False
                            for host_cpu_spec in host_cpu_specs:
                                # CPU not specified
                                if "cpu" not in architecture_map:
                                    continue
                                # check architecture
                                if host_cpu_spec["arch"] == "*":
                                    if "excl" in architecture_map["cpu"]["arch"]:
                                        continue
                                else:
                                    if "any" not in architecture_map["cpu"]["arch"]:
                                        if host_cpu_spec["arch"] not in architecture_map["cpu"]["arch"]:
                                            # check with regex
                                            if not [True for iii in architecture_map["cpu"]["arch"] if re.search("^" + host_cpu_spec["arch"] + "$", iii)]:
                                                continue
                                # check vendor
                                if host_cpu_spec["vendor"] == "*":
                                    # task doesn't specify a vendor and PQ explicitly requests a specific vendor
                                    if "vendor" in architecture_map["cpu"] and "excl" in architecture_map["cpu"]["vendor"]:
                                        continue
                                else:
                                    # task specifies a vendor and PQ doesn't request any specific vendor
                                    if "vendor" not in architecture_map["cpu"]:
                                        continue
                                    # task specifies a vendor and PQ doesn't accept any vendor or the specific vendor
                                    if "any" not in architecture_map["cpu"]["vendor"] and host_cpu_spec["vendor"] not in architecture_map["cpu"]["vendor"]:
                                        continue
                                # check instruction set
                                if host_cpu_spec["instr"] == "*":
                                    if "instr" in architecture_map["cpu"] and "excl" in architecture_map["cpu"]["instr"]:
                                        continue
                                else:
                                    if "instr" not in architecture_map["cpu"]:
                                        continue
                                    if "any" not in architecture_map["cpu"]["instr"] and host_cpu_spec["instr"] not in architecture_map["cpu"]["instr"]:
                                        continue
                                host_ok = True
                                break
                            if not host_ok:
                                continue
                        # check GPU
                        if host_gpu_spec:
                            # GPU not specified
                            if "gpu" not in architecture_map:
                                continue
                            # check vendor
                            if host_gpu_spec["vendor"] == "*":
                                # task doesn't specify CPU vendor and PQ explicitly requests a specific vendor
                                if "vendor" in architecture_map["gpu"] and "excl" in architecture_map["gpu"]["vendor"]:
                                    continue
                            else:
                                # task specifies a vendor and PQ doesn't request any specific vendor
                                if "vendor" not in architecture_map["gpu"]:
                                    continue
                                # task specifies a vendor and PQ doesn't accept any vendor or the specific vendor
                                if "any" not in architecture_map["gpu"]["vendor"] and host_gpu_spec["vendor"] not in architecture_map["gpu"]["vendor"]:
                                    continue
                            # check model
                            if host_gpu_spec["model"] == "*":
                                if "model" in architecture_map["gpu"] and "excl" in architecture_map["gpu"]["model"]:
                                    continue
                            else:
                                if "model" not in architecture_map["gpu"] or (
                                    "any" not in architecture_map["gpu"]["model"] and host_gpu_spec["model"] not in architecture_map["gpu"]["model"]
                                ):
                                    continue
                            # check version
                            if "version" in host_gpu_spec:
                                if "version" not in architecture_map["gpu"]:
                                    # PQ doesn't specify version
                                    continue
                                elif "any" == architecture_map["gpu"]["version"]:
                                    # PQ accepts any version
                                    pass
                                else:
                                    # check version at PQ
                                    if not compare_version_string(architecture_map["gpu"]["version"], host_gpu_spec["version"]):
                                        continue
                    go_ahead = True
                except Exception as e:
                    if log_stream:
                        log_stream.error(f"json check {str(architecture_map)} failed for {tmpSiteName} {str(e)} {traceback.format_exc()} ")
                if not go_ahead:
                    continue
                # only HW check
                if not (cvmfs_tag or cmt_config or sw_project or sw_version or container_name) and (host_cpu_specs or host_gpu_spec):
                    okSite.append(tmpSiteName)
                    continue
                # check for fat container
                if container_name:
                    # check for container
                    if not only_tags_fc and ("any" in self.sw_map[tmpSiteName]["containers"] or "/cvmfs" in self.sw_map[tmpSiteName]["containers"]):
                        # any in containers
                        okSite.append(tmpSiteName)
                    elif container_name in set([t["container_name"] for t in self.sw_map[tmpSiteName]["tags"] if t["container_name"]]):
                        # logical name in tags or any in containers
                        okSite.append(tmpSiteName)
                    elif container_name in set([s for t in self.sw_map[tmpSiteName]["tags"] for s in t["sources"] if t["sources"]]):
                        # full path in sources
                        okSite.append(tmpSiteName)
                    elif not only_tags_fc:
                        # get sources in all tag list
                        if "ALL" in self.sw_map:
                            source_list_in_all_tag = [s for t in self.sw_map["ALL"]["tags"] for s in t["sources"] if t["container_name"] == container_name]
                        else:
                            source_list_in_all_tag = []
                        # prefix with full path
                        for tmp_prefix in self.sw_map[tmpSiteName]["containers"]:
                            if container_name.startswith(tmp_prefix):
                                okSite.append(tmpSiteName)
                                break
                            toBreak = False
                            for source_in_all_tag in source_list_in_all_tag:
                                if source_in_all_tag.startswith(tmp_prefix):
                                    okSite.append(tmpSiteName)
                                    toBreak = True
                                    break
                            if toBreak:
                                break
                    continue
                # only cmt config check
                if cmt_config_only:
                    okSite.append(tmpSiteName)
                    continue
                # check if CVMFS is available
                if "any" in self.sw_map[tmpSiteName]["cvmfs"] or cvmfs_tag in self.sw_map[tmpSiteName]["cvmfs"]:
                    # check if container is available
                    if "any" in self.sw_map[tmpSiteName]["containers"] or "/cvmfs" in self.sw_map[tmpSiteName]["containers"]:
                        okSite.append(tmpSiteName)
                    # check cmt config
                    elif not need_container and cmt_config in self.sw_map[tmpSiteName]["cmtconfigs"]:
                        okSite.append(tmpSiteName)
                elif not need_cvmfs:
                    if not need_container or "any" in self.sw_map[tmpSiteName]["containers"]:
                        # check tags
                        for tag in self.sw_map[tmpSiteName]["tags"]:
                            if tag["cmtconfig"] == cmt_config and tag["project"] == sw_project and tag["release"] == sw_version:
                                okSite.append(tmpSiteName)
                                break
                # don't pass to subsequent check if AUTO is enabled
                continue
            # use only AUTO for container or HW
            if container_name is not None or host_cpu_specs is not None or host_gpu_spec is not None:
                continue
            noAutoSite.append(tmpSiteName)
        return (okSite, noAutoSite)


# resolve cmt_config
def resolve_cmt_config(queue_name: str, cmt_config: str, base_platform, sw_map: dict) -> str | None:
    """
    resolve cmt config at a given queue_name
    :param queue_name: queue name
    :param cmt_config: cmt confing to resolve
    :param base_platform: base platform
    :param sw_map: software map
    :return: resolved cmt config or None if unavailable or valid
    """
    # return None if queue_name is unavailable
    if queue_name not in sw_map:
        return None
    # return None if cmt_config is valid
    if cmt_config in sw_map[queue_name]["cmtconfigs"]:
        return None
    # check if cmt_config matches with any of the queue's cmt_configs
    for tmp_cmt_config in sw_map[queue_name]["cmtconfigs"]:
        if re.search("^" + cmt_config + "$", tmp_cmt_config):
            if base_platform:
                # add base_platform if necessary
                tmp_cmt_config = tmp_cmt_config + "@" + base_platform
            return tmp_cmt_config
    # return None if cmt_config is unavailable
    return None


def check_endpoints_with_blacklist(
    site_spec: SiteSpec.SiteSpec, scope_input: str, scope_output: str, sites_in_nucleus: list, remote_source_available: bool
) -> str | None:
    """
    Check if site's endpoints are in the blacklist

    :param site_spec: site spec
    :param scope_input: input scope
    :param scope_output: output scope
    :param sites_in_nucleus: list of sites in nucleus
    :param remote_source_available: if remote source is available

    :return: description of blacklisted reason or None
    """
    tmp_msg = None
    receive_input_over_wan = False
    read_input_over_lan = False
    write_output_over_lan = False
    send_output_over_wan = False
    tmpSiteName = site_spec.sitename
    if scope_input in site_spec.ddm_endpoints_input:
        for tmp_input_endpoint in site_spec.ddm_endpoints_input[scope_input].all.values():
            tmp_read_input_over_lan = tmp_input_endpoint["detailed_status"].get("read_lan")
            tmp_receive_input_over_wan = tmp_input_endpoint["detailed_status"].get("write_wan")
            # can read input from local
            if tmp_read_input_over_lan not in ["OFF", "TEST"]:
                read_input_over_lan = True
            # can receive input from remote to local
            if tmpSiteName not in sites_in_nucleus:
                # satellite sites
                if tmp_receive_input_over_wan not in ["OFF", "TEST"]:
                    receive_input_over_wan = True
            else:
                # NA for nucleus sites
                receive_input_over_wan = True
                remote_source_available = True
    if scope_output in site_spec.ddm_endpoints_output:
        for tmp_output_endpoint in site_spec.ddm_endpoints_output[scope_output].all.values():
            tmp_write_output_over_lan = tmp_output_endpoint["detailed_status"].get("write_lan")
            tmp_send_output_over_wan = tmp_output_endpoint["detailed_status"].get("read_wan")
            # can write output to local
            if tmp_write_output_over_lan not in ["OFF", "TEST"]:
                write_output_over_lan = True
            # can send output from local to remote
            if tmpSiteName not in sites_in_nucleus:
                # satellite sites
                if tmp_send_output_over_wan not in ["OFF", "TEST"]:
                    send_output_over_wan = True
            else:
                # NA for nucleus sites
                send_output_over_wan = True
                remote_source_available = True
    # take the status for logging
    if not read_input_over_lan:
        tmp_msg = f"  skip site={tmpSiteName} since input endpoints cannot read over LAN, read_lan is not ON criteria=-read_lan_blacklist"
    elif not write_output_over_lan:
        tmp_msg = f"  skip site={tmpSiteName} since output endpoints cannot write over LAN, write_lan is not ON criteria=-write_lan_blacklist"
    elif not receive_input_over_wan:
        tmp_msg = f"  skip site={tmpSiteName} since input endpoints cannot receive files over WAN, write_wan is not ON criteria=-write_wan_blacklist"
    elif not send_output_over_wan:
        tmp_msg = f"  skip site={tmpSiteName} since output endpoints cannot send out files over WAN, read_wan is not ON criteria=-read_wan_blacklist"
    elif not remote_source_available:
        tmp_msg = f"  skip site={tmpSiteName} since source endpoints cannot transfer files over WAN and it is satellite criteria=-source_blacklist"
    return tmp_msg
