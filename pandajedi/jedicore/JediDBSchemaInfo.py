"""
Minimum schema version required for panda-jedi to work properly.
Please always keep this version number up to date.
"""

from pandacommon.pandalogger.PandaLogger import PandaLogger

_logger = PandaLogger().getLogger("DBSchema")


class JediDBSchemaInfo:
    # never read: method() below shadows it with a local of the same name. Kept because
    # pandaserver/taskbuffer/PandaDBSchemaInfo.py is the same class with the same unused
    # attribute, and the two are meant to be read side by side
    schema_version: str | None = None

    def method(self) -> str:
        schema_version = "0.1.1"
        _logger.debug(f"PanDA schema version required for JEDI is : {schema_version}")
        return schema_version
