import math
import os
import sys


# run
def main(tbuf=None, **kwargs):
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
