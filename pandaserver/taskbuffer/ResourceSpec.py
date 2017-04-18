"""
Resource type specification for JEDI

"""

class ResourceSpec(object):
    # attributes
    attributes = ('resource_name', 'mincore', 'maxcore', 'minrampercore', 'maxrampercore')

    def __init__(self, resource_name, mincore, maxcore, minrampercore, maxrampercore):
        object.__setattr__(self, 'resource_name', resource_name)
        object.__setattr__(self, 'mincore', mincore)
        object.__setattr__(self, 'maxcore', maxcore)
        object.__setattr__(self, 'minrampercore', minrampercore)
        object.__setattr__(self, 'maxrampercore', maxrampercore)

    def match_task(self, task_spec):
        return self.match_task_basic(task_spec.corecount, task_spec.ramcount)

    def match_task_basic(self, corecount, ramcount):
        # check min cores
        if self.mincore is not None and corecount < self.mincore:
            return False

        # check max cores
        if self.maxcore is not None and corecount > self.maxcore:
            return False

        # check min ram
        if self.minrampercore is not None and ramcount < self.minrampercore:
            return False

        # check max ram
        if self.maxrampercore is not None and ramcount > self.maxrampercore:
            return False

        return True

    def column_names(cls, prefix=None):
        """
        return column names for DB interactions
        """
        ret = ''
        for attr in cls.attributes:
            if prefix is not None:
                ret += '{0}.'.format(prefix)
            ret += '{0},'.format(attr)
        ret = ret[:-1]
        return ret
    columnNames = classmethod(column_names)
