"""
cloud specification

"""


class CloudSpec(object):
    # attributes
    _attributes = (
        "name",
        "tier1",
        "tier1SE",
        "relocation",
        "weight",
        "server",
        "status",
        "transtimelo",
        "transtimehi",
        "waittime",
        "validation",
        "mcshare",
        "countries",
        "fasttrack",
        "nprestage",
        "pilotowners",
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
