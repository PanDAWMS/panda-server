# Description: Unit tests for dispatching to a worker bound to one container image. No database is used
import unittest
from typing import Any

from pandaserver.taskbuffer.db_proxy_mods.job_complex_module import JobComplexModule

IMAGE = "/cvmfs/singularity.opensciencegrid.org/eicweb/eic_xl:26.07.3-stable"


def where_clause(**kwargs: Any) -> tuple[str, dict[str, Any]]:
    params: dict[str, Any] = dict(
        site_name="SITE",
        mem=0,
        disk_space=0,
        background=False,
        resource_type=None,
        prod_source_label="managed",
        computing_element=None,
        is_gu=False,
        job_type=None,
        prod_user_id=None,
        task_id=None,
        average_memory_limit=None,
        remaining_time=None,
    )
    params.update(kwargs)
    # the method reads nothing from the instance
    return JobComplexModule.construct_where_clause(None, **params)


class TestContainerImageWhereClause(unittest.TestCase):
    def test_bound_worker_gets_only_its_image(self) -> None:
        sql, var_map = where_clause(container_image=IMAGE)
        self.assertIn("AND container_name=:containerName ", sql)
        self.assertEqual(var_map[":containerName"], IMAGE)

    def test_unbound_worker_is_unchanged(self) -> None:
        for value in (None, ""):
            sql, var_map = where_clause(container_image=value)
            self.assertNotIn("container_name", sql)
            self.assertNotIn(":containerName", var_map)
        self.assertEqual(where_clause(container_image=None), where_clause())

    def test_combines_with_the_other_filters(self) -> None:
        sql, var_map = where_clause(container_image=IMAGE, task_id=123, excluded_task_ids=[7])
        self.assertIn("AND jediTaskID=:taskID ", sql)
        self.assertIn("AND container_name=:containerName ", sql)
        self.assertIn("jediTaskID NOT IN (", sql)
        self.assertEqual(var_map[":taskID"], 123)


if __name__ == "__main__":
    unittest.main()
