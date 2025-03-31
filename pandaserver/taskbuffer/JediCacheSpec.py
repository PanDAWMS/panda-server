"""
cache specification for JEDI

"""


class JediCacheSpec(object):
    # attributes
    attributes = (
        "main_key",
        "sub_key",
        "data",
        "last_update",
    )

    # constructor
    def __init__(self):
        # install attributes
        for attr in self.attributes:
            object.__setattr__(self, attr, None)

    # return map of values
    def valuesMap(self):
        ret = {}
        for attr in self.attributes:
            val = getattr(self, attr)
            ret[f":{attr}"] = val
        return ret

    # pack tuple into JediCacheSpec
    def pack(self, values):
        for i, attr in enumerate(self.attributes):
            val = values[i]
            object.__setattr__(self, attr, val)

    # return column names for INSERT
    @classmethod
    def columnNames(cls, prefix=None):
        ret = ""
        if prefix is None:
            ret = ",".join(cls.attributes)
        else:
            ret = ",".join([f"{prefix}.{attr}" for attr in cls.attributes])
        return ret

    # return expression of bind variables for INSERT
    @classmethod
    def bindValuesExpression(cls):
        values_str = ",".join([f":{attr}" for attr in cls.attributes])
        ret = f"VALUES({values_str})"
        return ret

    # return an expression of bind variables for UPDATE
    @classmethod
    def bindUpdateChangesExpression(cls):
        ret = ",".join(["{0}=:{0}".format(attr) for attr in cls.attributes])
        return ret
