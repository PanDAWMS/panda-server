# Description: Unit tests for how a FileSpec gets the PandaID of its job. No database is used
import pickle
import unittest
from typing import Any

from pandaserver.taskbuffer import JobUtils
from pandaserver.taskbuffer.FileSpec import FileSpec
from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.PickleJobSpec import PickleJobSpec


def make_file(lfn: str, panda_id: int | None = None) -> FileSpec:
    # a file as read from filesTable4, with the PandaID column already filled
    file_spec = FileSpec()
    row: list[Any] = [None] * len(FileSpec._attributes)
    row[FileSpec._attributes.index("row_ID")] = 1
    row[FileSpec._attributes.index("PandaID")] = panda_id
    row[FileSpec._attributes.index("lfn")] = lfn
    row[FileSpec._attributes.index("type")] = "input"
    file_spec.pack(row)
    return file_spec


def make_job(panda_id: int | None, n_files: int = 2) -> JobSpec:
    job = JobSpec()
    job.PandaID = panda_id
    job.jobName = "test"
    for i in range(n_files):
        job.addFile(make_file(f"file{i}"))
    job.resetChangedList()
    for file_spec in job.Files:
        file_spec.resetChangedList()
    return job


def contains_job(obj: Any) -> bool:
    if isinstance(obj, JobSpec):
        return True
    if isinstance(obj, (list, tuple)):
        return any(contains_job(x) for x in obj)
    if isinstance(obj, dict):
        return any(contains_job(x) for x in obj.values())
    return False


class TestAddFile(unittest.TestCase):
    def test_file_takes_job_panda_id_as_unchanged(self) -> None:
        job = make_job(123, n_files=0)
        file_spec = make_file("a", panda_id=999)
        job.addFile(file_spec)
        self.assertEqual(file_spec.PandaID, 123)
        self.assertNotIn("PandaID", file_spec._changedAttrs)
        self.assertNotIn("PandaID", file_spec.bindUpdateChangesExpression())
        self.assertEqual(file_spec.valuesMap(onlyChanged=True), {})

    def test_add_forgets_a_pending_panda_id_change(self) -> None:
        file_spec = make_file("a", panda_id=1)
        file_spec.PandaID = 2
        self.assertIn("PandaID", file_spec._changedAttrs)
        make_job(123, n_files=0).addFile(file_spec)
        self.assertEqual(file_spec.PandaID, 123)
        self.assertNotIn("PandaID", file_spec._changedAttrs)

    def test_job_without_panda_id(self) -> None:
        job = make_job(None, n_files=0)
        file_spec = make_file("a", panda_id=999)
        job.addFile(file_spec)
        self.assertEqual(file_spec.PandaID, "NULL")
        self.assertIsNone(object.__getattribute__(file_spec, "PandaID"))
        self.assertIsNone(file_spec.valuesMap()[":PandaID"])

    def test_file_holds_no_job(self) -> None:
        job = make_job(123)
        file_spec = job.Files[0]
        self.assertFalse(hasattr(file_spec, "_owner"))
        self.assertFalse(contains_job(file_spec.__getstate__()))
        restored = pickle.loads(pickle.dumps(file_spec))
        self.assertEqual(restored.PandaID, 123)


class TestPandaIDChange(unittest.TestCase):
    def test_new_panda_id_reaches_every_file(self) -> None:
        job = make_job(None)
        job.PandaID = 456
        for file_spec in job.Files:
            self.assertEqual(file_spec.PandaID, 456)
            self.assertEqual(file_spec._changedAttrs, {"PandaID": 456})
            self.assertEqual(file_spec.bindUpdateChangesExpression(), "PandaID=:PandaID ")
            self.assertEqual(file_spec.valuesMap(onlyChanged=True), {":PandaID": 456})
            self.assertEqual(file_spec.valuesMap(useSeq=True)[":PandaID"], 456)
        self.assertEqual(job._changedAttrs["PandaID"], 456)

    def test_same_panda_id_is_not_a_change(self) -> None:
        job = make_job(456)
        job.PandaID = 456
        for file_spec in job.Files:
            self.assertEqual(file_spec._changedAttrs, {})

    def test_reset_changed_list(self) -> None:
        job = make_job(None)
        job.PandaID = 456
        for file_spec in job.Files:
            file_spec.resetChangedList()
            self.assertEqual(file_spec.valuesMap(onlyChanged=True), {})
            self.assertEqual(file_spec.PandaID, 456)

    def test_reset_to_none(self) -> None:
        job = make_job(456)
        job.PandaID = None
        for file_spec in job.Files:
            self.assertEqual(file_spec.PandaID, "NULL")
            self.assertEqual(file_spec.valuesMap(onlyChanged=True), {":PandaID": None})

    def test_files_replaced_after_copy_keep_original(self) -> None:
        # the pattern of makeFakeCoJumbo, the ES retry and JEDI's clonePandaJob
        import copy

        job = make_job(456)
        new_job = copy.copy(job)
        new_job.Files = []
        new_job.addFile(make_file("b"))
        new_job.PandaID = 789
        self.assertEqual([f.PandaID for f in job.Files], [456, 456])
        self.assertEqual([f.PandaID for f in new_job.Files], [789])


class TestSerialization(unittest.TestCase):
    def test_job_pickle_round_trip(self) -> None:
        job = make_job(456)
        restored = pickle.loads(pickle.dumps(job))
        self.assertEqual([f.PandaID for f in restored.Files], [456, 456])
        # the restored files follow the restored job, not the original one
        restored.PandaID = 789
        self.assertEqual([f.PandaID for f in restored.Files], [789, 789])
        self.assertEqual([f.PandaID for f in job.Files], [456, 456])

    def test_pickle_job_spec_keeps_panda_id_change(self) -> None:
        job = make_job(None)
        job.PandaID = 456
        p_job = PickleJobSpec()
        p_job.update(job)
        restored = pickle.loads(pickle.dumps(p_job))
        for file_spec in restored.Files:
            self.assertEqual(file_spec.PandaID, 456)
            # PickleFileSpec.update also records its own _reserveChangedState there, which
            # valuesMap ignores
            self.assertEqual(file_spec.valuesMap(onlyChanged=True), {":PandaID": 456})

    def test_json_round_trip(self) -> None:
        job = make_job(456)
        state = job.dump_to_json_serializable()
        for file_state in state[-1]:
            self.assertEqual(len(file_state), len(FileSpec._attributes) + 1)
            self.assertIsNone(file_state[-1])
        restored = JobUtils.load_jobs_json(JobUtils.dump_jobs_json([job]))[0]
        self.assertEqual([f.PandaID for f in restored.Files], [456, 456])
        self.assertEqual([f.lfn for f in restored.Files], ["file0", "file1"])
        for file_spec in restored.Files:
            self.assertEqual(file_spec._changedAttrs, {})

    def test_panda_client_state(self) -> None:
        # panda-client sends the columns and a trailing None where the owner used to be
        file_state: list[Any] = [None] * len(FileSpec._attributes) + [None]
        file_state[FileSpec._attributes.index("lfn")] = "a"
        file_spec = FileSpec()
        file_spec.__setstate__(file_state)
        self.assertEqual(file_spec.lfn, "a")
        self.assertEqual(file_spec.PandaID, "NULL")
        job = make_job(456, n_files=0)
        job.addFile(file_spec)
        self.assertEqual(file_spec.PandaID, 456)

    def test_old_state_with_owner_job(self) -> None:
        # an older version put the owner JobSpec in the last entry; it is not kept
        owner = make_job(456, n_files=0)
        file_state: list[Any] = [None] * len(FileSpec._attributes) + [owner]
        file_state[FileSpec._attributes.index("PandaID")] = 456
        file_spec = FileSpec()
        file_spec.__setstate__(file_state)
        self.assertEqual(file_spec.PandaID, 456)
        self.assertFalse(contains_job(file_spec.__getstate__()))
        owner.PandaID = 789
        self.assertEqual(file_spec.PandaID, 456)


if __name__ == "__main__":
    unittest.main()
