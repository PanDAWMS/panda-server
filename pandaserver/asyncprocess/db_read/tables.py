"""
Tables the async db_read requests may read.

This list is the allowlist itself: a table that is not here cannot be read, whatever the
request says, so it is meant to be reviewed by hand whenever it changes. The first list
takes the tables the code reads most often, counted over the places in the code that read
the database, leaving out users (it holds DNs and mail addresses); filesTable_ARCH and
CONFIG were added for the coverage they bring. Secrets and proxykey are never to be listed.

The job and task status logs are not here: GET /v1/job/get_status_history and
GET /v1/task/get_status_history read them.
"""

from dataclasses import dataclass

from pandaserver.config import panda_config


@dataclass(frozen=True)
class AllowedTable:
    # name a request uses, matched without regard to case
    name: str
    # attribute of panda_config holding the schema the table is in
    schema_attribute: str

    @property
    def owner(self) -> str:
        owner: str = getattr(panda_config, self.schema_attribute)
        return owner


_TABLES = (
    AllowedTable("JEDI_Datasets", "schemaJEDI"),
    AllowedTable("JEDI_Tasks", "schemaJEDI"),
    AllowedTable("jobsActive4", "schemaPANDA"),
    AllowedTable("JEDI_Dataset_Contents", "schemaJEDI"),
    AllowedTable("filesTable4", "schemaPANDA"),
    AllowedTable("jobsDefined4", "schemaPANDA"),
    AllowedTable("JEDI_AUX_Status_MinTaskID", "schemaJEDI"),
    AllowedTable("JEDI_Events", "schemaJEDI"),
    AllowedTable("jobsArchived4", "schemaPANDA"),
    AllowedTable("SCHEDCONFIG_JSON", "schemaPANDA"),
    AllowedTable("jobsArchived", "schemaPANDAARCH"),
    AllowedTable("T_TASK", "schemaDEFT"),
    AllowedTable("JEDI_Work_Queue", "schemaJEDI"),
    AllowedTable("JOBS_SHARE_STATS", "schemaPANDA"),
    AllowedTable("SiteData", "schemaMETA"),
    AllowedTable("data_carousel_requests", "schemaJEDI"),
    AllowedTable("jobParamsTable", "schemaPANDA"),
    AllowedTable("Datasets", "schemaPANDA"),
    AllowedTable("data_carousel_relations", "schemaJEDI"),
    AllowedTable("filesTable_ARCH", "schemaPANDAARCH"),
    AllowedTable("CONFIG", "schemaPANDA"),
)

ALLOWED_TABLES: dict[str, AllowedTable] = {table.name.lower(): table for table in _TABLES}


def get_allowed_table(name: str) -> AllowedTable | None:
    """
    Get a table of the allowlist.

    Args:
        name(str): the name a request gives, in any case

    Returns:
        AllowedTable or None: the table, or None when it is not allowed
    """
    return ALLOWED_TABLES.get(name.lower())
