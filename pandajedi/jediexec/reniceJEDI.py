import subprocess

from pandajedi.jediconfig import jedi_config

proc = subprocess.Popen(
    f"ps -l x -U {jedi_config.master.uname}",
    shell=True,
    stdout=subprocess.PIPE,
)
stdoutList = str(proc.communicate()[0]).split("\n")
for line in stdoutList:
    try:
        items = line.split()
        if len(items) < 6:
            continue
        pid = items[3]
        nice = int(items[7])
        if "JediMaster.py" in line and nice > 0:
            reniceProc = subprocess.Popen(
                f"renice 0 {pid}",
                shell=True,
                stdout=subprocess.PIPE,
            )
    except Exception:
        pass
