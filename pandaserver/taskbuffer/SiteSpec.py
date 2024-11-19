"""
site specification

"""

import re


class SiteSpec(object):
    # attributes
    _attributes = (
        "sitename",
        "nickname",
        "dq2url",
        "cloud",
        "ddm",
        "ddm_input",
        "ddm_output",
        "type",
        "releases",
        "memory",
        "maxtime",
        "status",
        "setokens_input",
        "setokens_output",
        "defaulttoken",
        "validatedreleases",
        "maxinputsize",
        "comment",
        "statusmodtime",
        "pledgedCPU",
        "coreCount",
        "reliabilityLevel",
        "iscvmfs",
        "transferringlimit",
        "maxwdir",
        "fairsharePolicy",
        "mintime",
        "allowfax",
        "pandasite",
        "corepower",
        "wnconnectivity",
        "catchall",
        "role",
        "pandasite_state",
        "ddm_endpoints_input",
        "ddm_endpoints_output",
        "maxrss",
        "minrss",
        "direct_access_lan",
        "direct_access_wan",
        "tier",
        "objectstores",
        "is_unified",
        "unified_name",
        "jobseed",
        "capability",
        "num_slots_map",
        "workflow",
        "maxDiskio",
    )

    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            setattr(self, attr, None)

    # serialize
    def __str__(self):
        str = ""
        for attr in self._attributes:
            str += f"{attr}:{getattr(self, attr)} "
        return str

    # check if direct IO is used when tasks allow it
    def isDirectIO(self):
        if self.direct_access_lan is True:
            return True
        return False

    # check what type of jobs are allowed
    def getJobSeed(self):
        tmpVal = self.jobseed
        if tmpVal is None:
            return "std"
        return tmpVal

    # get value from catchall
    def getValueFromCatchall(self, key):
        if self.catchall is None:
            return None
        for tmpItem in self.catchall.split(","):
            tmpMatch = re.search(f"^{key}=(.+)", tmpItem)
            if tmpMatch is not None:
                return tmpMatch.group(1)
        return None

    # has value in catchall
    def hasValueInCatchall(self, key):
        if self.catchall is None:
            return False
        for tmpItem in self.catchall.split(","):
            tmpMatch = re.search(f"^{key}(=|)*", tmpItem)
            if tmpMatch is not None:
                return True
        return False

    # allow WAN input access
    def allowWanInputAccess(self):
        return self.direct_access_lan is True and self.direct_access_wan is True

    # use jumbo jobs
    def useJumboJobs(self):
        return self.hasValueInCatchall("useJumboJobs")

    # GPU
    def isGPU(self):
        return self.hasValueInCatchall("gpu")

    def is_grandly_unified(self):
        if self.hasValueInCatchall("grandly_unified") or self.type == "unified":
            return True
        return False

    def runs_production(self):
        if self.type == "production" or self.is_grandly_unified():
            return True
        return False

    def runs_analysis(self):
        if self.type == "analysis" or self.is_grandly_unified():
            return True
        return False

    # get unified name
    def get_unified_name(self):
        if self.unified_name is None:
            return self.sitename
        return self.unified_name

    # get number of simulated events for dynamic number of events
    def get_n_sim_events(self):
        tmpVal = self.getValueFromCatchall("nSimEvents")
        if tmpVal is None:
            return None
        return int(tmpVal)

    # get minimum of remaining events for jumbo jobs
    def getMinEventsForJumbo(self):
        tmpVal = self.getValueFromCatchall("minEventsForJumbo")
        if tmpVal is None:
            return None
        return int(tmpVal)

    # check if opportunistic
    def is_opportunistic(self):
        return self.pledgedCPU == -1

    # get number of jobs for standby
    def getNumStandby(self, sw_id, resource_type):
        numMap = self.num_slots_map
        # neither gshare or workqueue is definied
        if sw_id not in numMap:
            if None in numMap:
                sw_id = None
            else:
                return None
        # give the total if resource type is undefined
        if resource_type is None:
            return sum(numMap[sw_id].values())
        # give the number for the resource type
        if resource_type in numMap[sw_id]:
            return numMap[sw_id][resource_type]
        elif None in numMap[sw_id]:
            return numMap[sw_id][None]
        return None

    # get max disk per core
    def get_max_disk_per_core(self):
        tmpVal = self.getValueFromCatchall("maxDiskPerCore")
        try:
            return int(tmpVal)
        except Exception:
            pass
        return None

    # use local data only
    def use_only_local_data(self):
        return self.hasValueInCatchall("use_only_local_data")

    # check if use VP
    def use_vp(self, scope):
        # use default scope if missing
        if scope not in self.ddm_endpoints_input:
            scope = "default"
        # check if VP_DISK is associated
        if scope in self.ddm_endpoints_input and [i for i in self.ddm_endpoints_input[scope].getAllEndPoints() if i.endswith("_VP_DISK")]:
            return True
        return False

    # check if always uses direct IO
    def always_use_direct_io(self):
        return self.maxinputsize == -1

    # disable reassign
    def disable_reassign(self):
        if self.hasValueInCatchall("disableReassign"):
            return True
        self.status == "paused"

    # get job chunk size
    def get_job_chunk_size(self):
        try:
            return int(self.getValueFromCatchall("jobChunkSize"))
        except Exception:
            return None

    # get WN connectivity
    def get_wn_connectivity(self):
        if self.wnconnectivity is None:
            return None
        items = self.wnconnectivity.split("#")
        if not items or not items[0]:
            return None
        else:
            return items[0]

    # get IP stack
    def get_ipstack(self):
        if self.wnconnectivity is None:
            return None
        items = self.wnconnectivity.split("#")
        if len(items) == 2 and items[-1]:
            return items[-1]
        else:
            return None

    # get bare nucleus mode
    def bare_nucleus_mode(self):
        mode = self.getValueFromCatchall("bareNucleus")
        if mode in ["only", "allow"]:
            return mode
        return None

    # get secondary nucleus
    def secondary_nucleus(self):
        n = self.getValueFromCatchall("secondaryNucleus")
        if n:
            return n
        return None

    # get allowed processing types
    def get_allowed_processing_types(self):
        """
        Get allowed processing types for processing type-based job brokerage to access only tasks with specific processing types.
        They are defined in the catchall field as a pipe-separated list with the key "allowed_processing".
        """
        n = self.getValueFromCatchall("allowed_processing")
        if n:
            return n.split("|")
        return None

    # get excluded process types
    def get_excluded_processing_types(self):
        """
        Get excluded processing types for processing type-based job brokerage to exclude tasks with specific processing types.
        They are defined in the catchall field as a pipe-separated list with the key "excluded_processing".
        """
        n = self.getValueFromCatchall("excluded_processing")
        if n:
            return n.split("|")
        return None
