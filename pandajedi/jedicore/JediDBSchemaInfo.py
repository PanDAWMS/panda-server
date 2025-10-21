"""
Minimum schema version required for panda-jedi to work properly.
Please always keep this version number up to date.
"""

from pandacommon.pandalogger.PandaLogger import PandaLogger

_logger = PandaLogger().getLogger("DBSchema")


class JediDBSchemaInfo:
    schema_version = None

    def method(self):
        schema_version = "0.0.30"
        _logger.debug(f"PanDA schema version required for JEDI is : {schema_version}")
        return schema_version
