import math
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pandaserver.taskbuffer.TaskBuffer import TaskBuffer


# run
def main(tbuf: "TaskBuffer | None" = None, **kwargs: Any) -> None:
    print("Dummy test of PanDA daemon ---- START")

    res = math.cos(math.pi / 3)
    print(f"cos(pi/3) = {res}")

    print(f"uid={os.getuid()} , pid={os.getpid()}")

    if tbuf is not None:
        print("I got panda taskBuffer from daemon!!")
    else:
        print("I did not get panda taskBuffer from daemon...")

    print("Dummy test of PanDA daemon ---- END")


# run
if __name__ == "__main__":
    main()
