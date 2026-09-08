"""
Resource type specification for JEDI

"""

from typing import TYPE_CHECKING

from . import JobUtils

if TYPE_CHECKING:
    from pandaserver.taskbuffer.JediTaskSpec import JediTaskSpec
    from pandaserver.taskbuffer.JobSpec import JobSpec

HIMEM_THRESHOLD = 2000  # MB per core
BASIC_RESOURCE_TYPE = "SCORE"


class ResourceSpecMapper(object):
    def __init__(self, resource_types: list["ResourceSpec"]) -> None:
        """
        :param resource_types: list of ResourceSpec objects
        """
        self.resource_types = resource_types

    def is_single_core(self, resource_name: str | None) -> bool:
        for resource_type in self.resource_types:
            if resource_type.resource_name == resource_name:
                return resource_type.is_single_core()
        return False

    def is_multi_core(self, resource_name: str | None) -> bool:
        for resource_type in self.resource_types:
            if resource_type.resource_name == resource_name:
                return resource_type.is_multi_core()
        return False

    def is_high_memory(self, resource_name: str | None, memory_threshold: int = HIMEM_THRESHOLD) -> bool:
        for resource_type in self.resource_types:
            if resource_type.resource_name == resource_name:
                if resource_type.maxrampercore is None or resource_type.maxrampercore > memory_threshold:
                    return True
        return False

    def translate_resourcetype_to_cores(self, resource_name: str | None, cores_queue: int) -> int:
        # if the resource type is multi-core, return the number of cores in the queue
        if self.is_multi_core(resource_name):
            return cores_queue

        return 1

    def filter_out_high_memory_resourcetypes(self, memory_threshold: int = HIMEM_THRESHOLD) -> list[str | None]:
        resource_names = list(
            map(
                lambda resource_type: resource_type.resource_name,
                filter(lambda resource_type: not self.is_high_memory(resource_type.resource_name, memory_threshold=memory_threshold), self.resource_types),
            )
        )
        return resource_names


class ResourceSpec(object):
    # attributes
    attributes = (
        "resource_name",
        "mincore",
        "maxcore",
        "minrampercore",
        "maxrampercore",
    )

    # Column types, taken from the Oracle schema of ATLAS_PANDA.RESOURCE_TYPES (panda-database
    # repo, schema/oracle). The columns are installed by __init__ via setattr, so a type
    # checker sees none of them without these declarations. They carry no value, which
    # both keeps them out of the class dict and keeps __slots__ classes importable.
    # Unset columns really are None here -- this class has no "NULL" sentinel.
    resource_name: str | None
    mincore: int | None
    maxcore: int | None
    minrampercore: int | None
    maxrampercore: int | None

    def __init__(self, resource_name: str | None, mincore: int | None, maxcore: int | None, minrampercore: int | None, maxrampercore: int | None) -> None:
        object.__setattr__(self, "resource_name", resource_name)
        object.__setattr__(self, "mincore", mincore)
        object.__setattr__(self, "maxcore", maxcore)
        object.__setattr__(self, "minrampercore", minrampercore)
        object.__setattr__(self, "maxrampercore", maxrampercore)

    def match_task(self, task_spec: "JediTaskSpec") -> bool:
        return self.match_task_basic(
            task_spec.coreCount,
            task_spec.ramCount,
            task_spec.baseRamCount,
            task_spec.ramUnit,
        )

    def match_task_basic(self, corecount: int | None, ramcount: int | None, base_ramcount: int | None, ram_unit: str | None) -> bool:
        # Default parameters
        if corecount is None:  # corecount None is also used for 1
            corecount = 1
        elif corecount == 0:  # corecount 0 means it can be anything. We will use 8 as a standard MCORE default
            corecount = 8

        if ramcount is None:
            ramcount = 0

        if base_ramcount is None:
            base_ramcount = 0

        # check min cores
        if self.mincore is not None and corecount < self.mincore:
            return False

        # check max cores
        if self.maxcore is not None and corecount > self.maxcore:
            return False

        if ram_unit in ("MBPerCore", "MBPerCoreFixed"):
            ram_per_core = (ramcount * corecount + base_ramcount) / corecount
        else:
            ram_per_core = (ramcount + base_ramcount) / corecount
        # the count was just computed, so the compensation cannot answer the None it keeps
        # for a NULL or a missing one
        compensated_ram_per_core = JobUtils.compensate_ram_count(ram_per_core)
        if compensated_ram_per_core is not None:
            ram_per_core = compensated_ram_per_core

        # check min ram
        if self.minrampercore is not None and ram_per_core < self.minrampercore:
            return False

        # check max ram
        if self.maxrampercore is not None and ram_per_core > self.maxrampercore:
            return False

        return True

    def match_job(self, job_spec: "JobSpec") -> bool:
        # Default parameters
        if job_spec.coreCount in (None, "NULL"):  # corecount None is also used for 1
            corecount = 1
        elif job_spec.coreCount == 0:  # corecount 0 means it can be anything. We will use 8 as a standard MCORE default
            corecount = 8
        else:
            corecount = job_spec.coreCount  # type: ignore[assignment]  # "NULL" sentinel, see spec_column.py

        if job_spec.minRamCount in (
            None,
            "NULL",
        ):  # jobs come with ram already pre-calculated and in MB
            ramcount = 0
        else:
            ramcount = job_spec.minRamCount  # type: ignore[assignment]  # "NULL" sentinel, see spec_column.py

        # check min cores
        if self.mincore is not None and corecount < self.mincore:
            return False

        # check max cores
        if self.maxcore is not None and corecount > self.maxcore:
            return False

        # We assume ram unit is always MB
        ram_per_core = round(ramcount / corecount)
        # check min ram
        if self.minrampercore is not None and ram_per_core < self.minrampercore:
            return False

        # check max ram
        if self.maxrampercore is not None and ram_per_core > self.maxrampercore:
            return False

        return True

    def is_single_core(self) -> bool:
        if self.mincore is not None and self.mincore == 1 and self.maxcore is not None and self.maxcore == 1:
            return True
        return False

    def is_multi_core(self) -> bool:
        if self.mincore is not None and self.mincore > 1:
            return True
        return False

    @classmethod
    def column_names(cls, prefix: str | None = None) -> str:
        """
        return column names for DB interactions
        """
        ret = ""
        for attr in cls.attributes:
            if prefix is not None:
                ret += f"{prefix}."
            ret += f"{attr},"
        ret = ret[:-1]
        return ret
