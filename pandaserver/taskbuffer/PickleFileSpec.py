# Derived FileSpec to preserve changed attributes in pickle as it is impossible to change FileSpec.reserveChangedState
# consistently with all clients at the same time

from typing import Any

from pandaserver.taskbuffer.FileSpec import FileSpec


class PickleFileSpec(FileSpec):
    def __init__(self) -> None:
        FileSpec.__init__(self)
        object.__setattr__(self, "_reserveChangedState", True)

    def update(self, spec: FileSpec) -> None:
        spec._reserveChangedState = True
        self.__setstate__(spec.__getstate__())

    # the state is the column values followed by _changedAttrs and _owner, so its entries are
    # not uniformly typed. FileSpec.__getstate__ builds it and is the only thing that reads it
    def __setstate__(self, state: list[Any]) -> None:
        object.__setattr__(self, "_reserveChangedState", True)
        FileSpec.__setstate__(self, state)
