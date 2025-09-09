import atexit
import logging

from pandacommon.pandalogger.PandaLogger import PandaLogger

from pandajedi.jediconfig import jedi_config
from pandaserver.taskbuffer import OraDBProxy

logger = PandaLogger().getLogger(__name__.split(".")[-1])
OraDBProxy._logger = logger


# add handlers of filtered logger
tmpLoggerFiltered = PandaLogger().getLogger(__name__.split(".")[-1] + "Filtered")
for tmpHdr in tmpLoggerFiltered.handlers:
    tmpHdr.setLevel(logging.INFO)
    logger.addHandler(tmpHdr)
    tmpLoggerFiltered.removeHandler(tmpHdr)


# get mb proxies used in DBProxy methods
def get_mb_proxy_dict():
    if hasattr(jedi_config, "mq") and hasattr(jedi_config.mq, "configFile") and jedi_config.mq.configFile:
        # delay import to open logger file inside python daemon
        from pandajedi.jediorder.JediMsgProcessor import MsgProcAgent

        in_q_list = []
        out_q_list = ["jedi_jobtaskstatus", "jedi_contents_feeder", "jedi_job_generator"]
        mq_agent = MsgProcAgent(config_file=jedi_config.mq.configFile)
        mb_proxy_dict = mq_agent.start_passive_mode(in_q_list=in_q_list, out_q_list=out_q_list)
        # stop with atexit
        atexit.register(mq_agent.stop_passive_mode)
        # return
        return mb_proxy_dict


# main class
class DBProxy(OraDBProxy.DBProxy):
    # constructor
    def __init__(self, useOtherError=False):
        OraDBProxy.DBProxy.__init__(self, useOtherError)

        # set JEDI attributes
        self.set_jedi_attributes(jedi_config, get_mb_proxy_dict)

    # connect to DB (just for INTR)
    def connect(
        self,
        dbhost=jedi_config.db.dbhost,
        dbpasswd=jedi_config.db.dbpasswd,
        dbuser=jedi_config.db.dbuser,
        dbname=jedi_config.db.dbname,
        dbtimeout=None,
        reconnect=False,
    ):
        return OraDBProxy.DBProxy.connect(self, dbhost=dbhost, dbpasswd=dbpasswd, dbuser=dbuser, dbname=dbname, dbtimeout=dbtimeout, reconnect=reconnect)
