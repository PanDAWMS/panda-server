import json
import os
import sys

from pandacommon.liveconfigparser.LiveConfigParser import (
    LiveConfigParser,
    expand_values,
)

# get ConfigParser
tmp_conf = LiveConfigParser()

# read
tmp_conf.read("panda_jedi.cfg")


# dummy section class
class _SectionClass:
    pass


# ---------------------------------------------------------------------------
# The config file drives which sections exist, and expand_values() installs each
# section's keys as attributes, so a type checker sees none of them. Every section
# the code actually reads gets a subclass below declaring the keys it uses, and the
# loop at the bottom instantiates that subclass, so these declarations describe the
# object that really exists rather than a fiction layered over _SectionClass.
#
# The types are what the call sites do with the value. expand_values() derives them
# from the cfg text -- "True"/"False" become bool, a run of digits becomes int,
# "None" becomes None, anything else stays a str -- so a value written in an
# unexpected form can still contradict a declaration here.
#
# Keys that the code reads behind a hasattr() guard are declared too. The guard is
# not redundant: the section may genuinely lack the key at runtime.
# ---------------------------------------------------------------------------
class _Confeeder(_SectionClass):
    checkInterval: int
    loopCycle: int
    nWorkers: int
    procConfig: str


class _Daemon(_SectionClass):
    config: str
    enable: bool


class _Db(_SectionClass):
    dbhost: str
    dbname: str
    dbpasswd: str
    dbuser: str
    nWorkers: int
    schemaJEDI: str


class _Ddm(_SectionClass):
    endpoints_json_path: str
    modConfig: str
    user_scope_in_lowercase: bool
    voWithScope: str


class _JobBroker(_SectionClass):
    modConfig: str


class _JobGen(_SectionClass):
    inactive_poll_probability: float
    lockInterval: int
    lockProcess: str
    loopCycle: int
    nWorkers: int
    procConfig: str
    touchSandbox: bool
    typicalNumFile: str


class _JobThrottle(_SectionClass):
    modConfig: str


class _Master(_SectionClass):
    gname: str
    loggername: str
    uname: str


class _Mq(_SectionClass):
    configFile: str


class _MsgProcessor(_SectionClass):
    configFile: str


class _PostProcessor(_SectionClass):
    modConfig: str
    nTasks: int
    nWorkers: int
    procConfig: str


class _TaskBroker(_SectionClass):
    loopCycle: int
    modConfig: str
    nWorkers: int
    procConfig: str


class _TaskGen(_SectionClass):
    modConfig: str


class _TaskRefine(_SectionClass):
    loopCycle: int
    modConfig: str
    nWorkers: int
    procConfig: str


class _TaskSetup(_SectionClass):
    modConfig: str


class _TCommando(_SectionClass):
    loopCycle: int
    procConfig: str


class _WatchDog(_SectionClass):
    loopCycle: int
    modConfig: str
    procConfig: str
    timeoutForPending: int
    timeoutForPendingVoLabel: str
    waitForAchieved: int
    waitForExhausted: int
    waitForLocked: int
    waitForPending: int
    waitForPicked: int
    waitForThrottled: int


_SECTION_CLASSES: dict[str, type[_SectionClass]] = {
    "confeeder": _Confeeder,
    "daemon": _Daemon,
    "db": _Db,
    "ddm": _Ddm,
    "jobbroker": _JobBroker,
    "jobgen": _JobGen,
    "jobthrottle": _JobThrottle,
    "master": _Master,
    "mq": _Mq,
    "msgprocessor": _MsgProcessor,
    "postprocessor": _PostProcessor,
    "taskbroker": _TaskBroker,
    "taskgen": _TaskGen,
    "taskrefine": _TaskRefine,
    "tasksetup": _TaskSetup,
    "tcommando": _TCommando,
    "watchdog": _WatchDog,
}

# The sections themselves, installed into this module's dict by the loop below.
# Declared without values, so nothing is created at import time.
confeeder: _Confeeder
daemon: _Daemon
db: _Db
ddm: _Ddm
jobbroker: _JobBroker
jobgen: _JobGen
jobthrottle: _JobThrottle
master: _Master
mq: _Mq
msgprocessor: _MsgProcessor
postprocessor: _PostProcessor
taskbroker: _TaskBroker
taskgen: _TaskGen
taskrefine: _TaskRefine
tasksetup: _TaskSetup
tcommando: _TCommando
watchdog: _WatchDog


# load configmap
config_map_data = {}
if "PANDA_HOME" in os.environ:
    config_map_name = "panda_jedi_config.json"
    config_map_path = os.path.join(os.environ["PANDA_HOME"], "etc/config_json", config_map_name)
    if os.path.exists(config_map_path):
        with open(config_map_path) as f:
            config_map_data = json.load(f)

# loop over all sections
for tmp_section in tmp_conf.sections():
    # read section
    tmp_dict = getattr(tmp_conf, tmp_section)
    # load configmap
    if tmp_section in config_map_data:
        tmp_dict.update(config_map_data[tmp_section])
    # make section class. a section the code reads gets the subclass declared above,
    # which carries only annotations, so this behaves exactly as _SectionClass did
    tmp_self = _SECTION_CLASSES.get(tmp_section, _SectionClass)()
    # update module dict
    sys.modules[__name__].__dict__[tmp_section] = tmp_self
    # expand all values
    expand_values(tmp_self, tmp_dict)
