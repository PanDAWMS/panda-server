# Derived FileSpec to preserve changed attributes in pickle as it is impossible to change FileSpec.reserveChangedState
# consistently with all clients at the same time

from pandaserver.taskbuffer.FileSpec import FileSpec


class PickleFileSpec(FileSpec):
    def __init__(self):
        FileSpec.__init__(self)
        object.__setattr__(self, "_reserveChangedState", True)

    def update(self, spec):
        spec._reserveChangedState = True
        self.__setstate__(spec.__getstate__())

    def __setstate__(self, state):
        object.__setattr__(self, "_reserveChangedState", True)
        FileSpec.__setstate__(self, state)
