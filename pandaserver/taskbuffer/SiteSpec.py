"""
site specification

"""

import datetime
import re
from typing import Any

from pandaserver.taskbuffer.DdmSpec import DdmSpec

catchall_keys = {
    k: k
    for k in [
        "useJumboJobs",
        "gpu",
        "grandly_unified",
        "nSimEvents",
        "minEventsForJumbo",
        "maxDiskPerCore",
        "use_only_local_data",
        "disableReassign",
        "jobChunkSize",
        "bareNucleus",
        "secondaryNucleus",
        "allowed_processing",
        "excluded_processing",
        "per_core_attr",
        "allow_no_pilot",
    ]
}


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
        "space",
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
        "extra_queue_params",
    )

    # Column types. Unlike the other specs these do not come from the panda-database DDL:
    # the columns are the CRIC schedconfig fields, and the single place that fills them is
    # DBProxy.getSiteInfo() in db_proxy_mods/entity_module.py, so the types below are read
    # off that method. The columns are installed by __init__ via setattr, so a type checker
    # sees none of them without these declarations. They carry no value, which keeps them
    # out of the class dict. Everything is Optional because __init__ starts them at None
    # and getSiteInfo() leaves a column None whenever CRIC has no value for it; the ones
    # declared non-Optional are those getSiteInfo() always assigns unconditionally.
    sitename: str
    # assigned unconditionally, but from a lookup with no default, so CRIC omitting it
    # leaves None here like the columns below
    nickname: str | None
    dq2url: str | None
    cloud: str
    ddm: str
    # scope ("default", "user", ...) -> the default endpoint of that scope
    ddm_input: dict[str, str | None]
    ddm_output: dict[str, str | None]
    type: str
    releases: list[str]
    memory: int | None
    maxtime: int | None
    status: str | None
    # free space in the SE, in GB. Declared here and listed in _attributes above because
    # getSiteInfo() was the only thing installing it: a SiteSpec built any other way -- the
    # DEFAULT_SITE that SiteMapper.getSite() returns for an unknown site, among others --
    # had no such attribute at all, and GenJobBroker reads it on whatever getSite() returns.
    space: int | None
    # scope -> {space token -> endpoint name}
    setokens_input: dict[str, dict[str, str]]
    setokens_output: dict[str, dict[str, str]]
    defaulttoken: str | None
    validatedreleases: list[str]
    maxinputsize: int | None
    comment: str | None
    statusmodtime: datetime.datetime | None
    pledgedCPU: int
    coreCount: int
    reliabilityLevel: int | None
    iscvmfs: bool
    transferringlimit: int
    maxwdir: int
    fairsharePolicy: str | None
    mintime: int
    allowfax: bool
    pandasite: str
    corepower: float
    wnconnectivity: str | None
    catchall: str | None
    role: str
    pandasite_state: str
    # scope -> endpoints of that scope
    ddm_endpoints_input: dict[str, DdmSpec]
    ddm_endpoints_output: dict[str, DdmSpec]
    # CRIC gives these as integers, but SiteMapper.get_child_site_spec scales them by a core
    # ratio when it derives a child queue, so a spec in the map can hold either
    maxrss: float | None
    minrss: float | None
    direct_access_lan: bool
    direct_access_wan: bool
    tier: str | None
    objectstores: list[Any]
    is_unified: bool
    # set by SiteMapper when the queue belongs to a unified queue, None otherwise
    unified_name: str | None
    jobseed: str | None
    capability: str | None
    # gshare -> {resource type -> number of slots}; both keys are NULL-able in the DB
    num_slots_map: dict[str | None, dict[str | None, int]]
    workflow: str | None
    maxDiskio: float | None
    extra_queue_params: dict[str, Any]

    # constructor
    def __init__(self) -> None:
        # install attributes
        for attr in self._attributes:
            setattr(self, attr, None)

    # serialize
    def __str__(self) -> str:
        str = ""
        for attr in self._attributes:
            str += f"{attr}:{getattr(self, attr)} "
        return str

    # check if direct IO is used when tasks allow it
    def isDirectIO(self) -> bool:
        if self.direct_access_lan is True:
            return True
        return False

    # check what type of jobs are allowed
    def getJobSeed(self) -> str:
        tmpVal = self.jobseed
        if tmpVal is None:
            return "std"
        return tmpVal

    # get value from catchall
    def getValueFromCatchall(self, key: str) -> str | None:
        # check if the key is valid
        if key not in catchall_keys:
            return None
        key = catchall_keys[key]
        # first get the value if the key is defined as an extra queue parameter
        has_value, value = self.get_extra_queue_param(key)
        if has_value:
            return value
        # next get the value if the key is defined in the catchall field
        if self.catchall is None:
            return None
        for tmpItem in self.catchall.split(","):
            tmpMatch = re.search(f"^{key}=(.+)", tmpItem)
            if tmpMatch is not None:
                return tmpMatch.group(1)
        return None

    # has value in catchall
    def hasValueInCatchall(self, key: str) -> bool:
        # check if the key is valid
        if key not in catchall_keys:
            return False
        key = catchall_keys[key]
        # first check if the key is defined as an extra queue parameter
        has_value, _ = self.get_extra_queue_param(key)
        if has_value:
            return True
        # next check if the key is defined in the catchall field
        if self.catchall is None:
            return False
        for tmpItem in self.catchall.split(","):
            tmpMatch = re.search(f"^{key}(=|)*", tmpItem)
            if tmpMatch is not None:
                return True
        return False

    # get extra queue parameter
    def get_extra_queue_param(self, name: str) -> tuple[bool, None | Any]:
        """
        Get an extra queue parameter by name.
        Arguments:
            name: The name of the extra queue parameter to retrieve.
        Returns:
            A tuple containing a boolean indicating whether the parameter exists and its value (or None if it does not exist).
        """
        if not self.extra_queue_params or name not in self.extra_queue_params:
            return False, None
        return True, self.extra_queue_params[name]

    # allow WAN input access
    def allowWanInputAccess(self) -> bool:
        return self.direct_access_lan is True and self.direct_access_wan is True

    # use jumbo jobs
    def useJumboJobs(self) -> bool:
        return self.hasValueInCatchall("useJumboJobs")

    # GPU
    def isGPU(self) -> bool:
        return self.hasValueInCatchall("gpu")

    def is_grandly_unified(self) -> bool:
        if self.hasValueInCatchall("grandly_unified") or self.type == "unified":
            return True
        return False

    def runs_production(self) -> bool:
        if self.type == "production" or self.is_grandly_unified():
            return True
        return False

    def runs_analysis(self) -> bool:
        if self.type == "analysis" or self.is_grandly_unified():
            return True
        return False

    # get unified name
    def get_unified_name(self) -> str:
        if self.unified_name is None:
            return self.sitename
        return self.unified_name

    # get number of simulated events for dynamic number of events
    def get_n_sim_events(self) -> int | None:
        tmpVal = self.getValueFromCatchall("nSimEvents")
        if tmpVal is None:
            return None
        return int(tmpVal)

    # get minimum of remaining events for jumbo jobs
    def getMinEventsForJumbo(self) -> int | None:
        tmpVal = self.getValueFromCatchall("minEventsForJumbo")
        if tmpVal is None:
            return None
        return int(tmpVal)

    # check if opportunistic
    def is_opportunistic(self) -> bool:
        return self.pledgedCPU == -1

    # get number of jobs for standby
    def getNumStandby(self, sw_id: str | None, resource_type: str | None) -> int | None:
        numMap = self.num_slots_map
        # neither gshare or workqueue is defined
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
    def get_max_disk_per_core(self) -> int | None:
        tmpVal = self.getValueFromCatchall("maxDiskPerCore")
        if tmpVal is None:
            return None
        try:
            return int(tmpVal)
        except Exception:
            pass
        return None

    # use local data only
    def use_only_local_data(self) -> bool:
        return self.hasValueInCatchall("use_only_local_data")

    # check if use VP
    def use_vp(self, scope: str) -> bool:
        # use default scope if missing
        if scope not in self.ddm_endpoints_input:
            scope = "default"
        # check if VP_DISK is associated
        if scope in self.ddm_endpoints_input and [i for i in self.ddm_endpoints_input[scope].getAllEndPoints() if i.endswith("_VP_DISK")]:
            return True
        return False

    # check if always uses direct IO
    def always_use_direct_io(self) -> bool:
        return self.maxinputsize == -1

    # disable reassign
    def disable_reassign(self) -> bool:
        if self.hasValueInCatchall("disableReassign"):
            return True
        return self.status == "paused"

    # get job chunk size
    def get_job_chunk_size(self) -> int | None:
        value = self.getValueFromCatchall("jobChunkSize")
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    # get WN connectivity
    def get_wn_connectivity(self) -> str | None:
        if self.wnconnectivity is None:
            return None
        items = self.wnconnectivity.split("#")
        if not items or not items[0]:
            return None
        else:
            return items[0]

    # get IP stack
    def get_ipstack(self) -> str | None:
        if self.wnconnectivity is None:
            return None
        items = self.wnconnectivity.split("#")
        if len(items) == 2 and items[-1]:
            return items[-1]
        else:
            return None

    # get bare nucleus mode
    def bare_nucleus_mode(self) -> str | None:
        mode = self.getValueFromCatchall("bareNucleus")
        if mode in ["only", "allow"]:
            return mode
        return None

    # get secondary nucleus
    def secondary_nucleus(self) -> str | None:
        n = self.getValueFromCatchall("secondaryNucleus")
        if n:
            return n
        return None

    # get allowed processing types
    def get_allowed_processing_types(self) -> list[str] | None:
        """
        Get allowed processing types for processing type-based job brokerage to access only tasks with specific processing types.
        They are defined in the catchall field as a pipe-separated list with the key "allowed_processing".
        """
        n = self.getValueFromCatchall("allowed_processing")
        if n:
            return n.split("|")
        return None

    # get excluded process types
    def get_excluded_processing_types(self) -> list[str] | None:
        """
        Get excluded processing types for processing type-based job brokerage to exclude tasks with specific processing types.
        They are defined in the catchall field as a pipe-separated list with the key "excluded_processing".
        """
        n = self.getValueFromCatchall("excluded_processing")
        if n:
            return n.split("|")
        return None

    # use per-core attributes
    def use_per_core_attr(self) -> bool:
        return self.hasValueInCatchall("per_core_attr")

    # max IO intensity
    def get_max_io_intensity(self) -> Any:
        s, v = self.get_extra_queue_param("max_io_intensity")
        if s:
            return v
        return None
