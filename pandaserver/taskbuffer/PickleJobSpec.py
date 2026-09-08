# Derived JobSpec to preserve changed attributes in pickle as it is impossible to change JobSpec.reserveChangedState
# consistently with all clients at the same time

from typing import Any

from pandaserver.taskbuffer.JobSpec import JobSpec
from pandaserver.taskbuffer.PickleFileSpec import PickleFileSpec


class PickleJobSpec(JobSpec):
    def __init__(self) -> None:
        JobSpec.__init__(self)
        object.__setattr__(self, "_reserveChangedState", True)

    def update(self, spec: JobSpec) -> None:
        spec._reserveChangedState = True
        self.__setstate__(spec.__getstate__())
        p_file_list = []
        for file in self.Files:
            p_file = PickleFileSpec()
            p_file.update(file)
            p_file_list.append(p_file)
        object.__setattr__(self, "Files", p_file_list)

    # see PickleFileSpec.__setstate__ for why the entries are not uniformly typed
    def __setstate__(self, state: list[Any]) -> None:
        object.__setattr__(self, "_reserveChangedState", True)
        JobSpec.__setstate__(self, state)
