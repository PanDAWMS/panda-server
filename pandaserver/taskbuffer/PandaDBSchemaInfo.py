"""
Minimum schema version required for the panda-server to work properly.
Please always keep this version number up to date.
"""

from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger("DBSchema")


class PandaDBSchemaInfo:
    schema_version = None

    def method(self):
        schema_version = "0.0.20"
        _logger.debug(f"PanDA schema version required for Server is : {schema_version}")
        return schema_version
