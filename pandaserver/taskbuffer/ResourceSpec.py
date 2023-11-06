"""
Resource type specification for JEDI

"""

from . import JobUtils


class ResourceSpec(object):
    # attributes
    attributes = (
        "resource_name",
        "mincore",
        "maxcore",
        "minrampercore",
        "maxrampercore",
    )

    def __init__(self, resource_name, mincore, maxcore, minrampercore, maxrampercore):
        object.__setattr__(self, "resource_name", resource_name)
        object.__setattr__(self, "mincore", mincore)
        object.__setattr__(self, "maxcore", maxcore)
        object.__setattr__(self, "minrampercore", minrampercore)
        object.__setattr__(self, "maxrampercore", maxrampercore)

    def match_task(self, task_spec):
        return self.match_task_basic(
            task_spec.coreCount,
            task_spec.ramCount,
            task_spec.baseRamCount,
            task_spec.ramUnit,
        )

    def match_task_basic(self, corecount, ramcount, base_ramcount, ram_unit):
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
        ram_per_core = JobUtils.compensate_ram_count(ram_per_core)

        # check min ram
        if self.minrampercore is not None and ram_per_core < self.minrampercore:
            return False

        # check max ram
        if self.maxrampercore is not None and ram_per_core > self.maxrampercore:
            return False

        return True

    def match_job(self, job_spec):
        # Default parameters
        if job_spec.coreCount in (None, "NULL"):  # corecount None is also used for 1
            corecount = 1
        elif job_spec.coreCount == 0:  # corecount 0 means it can be anything. We will use 8 as a standard MCORE default
            corecount = 8
        else:
            corecount = job_spec.coreCount

        if job_spec.minRamCount in (
            None,
            "NULL",
        ):  # jobs come with ram already pre-calculated and in MB
            ramcount = 0
        else:
            ramcount = job_spec.minRamCount

        # check min cores
        if self.mincore is not None and corecount < self.mincore:
            return False

        # check max cores
        if self.maxcore is not None and corecount > self.maxcore:
            return False

        # We assume ram unit is always MB
        ram_per_core = ramcount / corecount
        # check min ram
        if self.minrampercore is not None and ram_per_core < self.minrampercore:
            return False

        # check max ram
        if self.maxrampercore is not None and ram_per_core > self.maxrampercore:
            return False

        return True

    def column_names(cls, prefix=None):
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

    column_names = classmethod(column_names)
