"""
Minimum schema version required for the panda-server to work properly.
Please always keep this version number up to date.
"""

from pandacommon.pandalogger.PandaLogger import PandaLogger

# logger
_logger = PandaLogger().getLogger('DBSchema')

class PandaDBSchemaInfo():
    schema_version = None
    def method(self):
        schema_version = '0.0.12'
        _logger.debug("PanDA schema version required for Server is : %s" % schema_version)
        return (schema_version)
