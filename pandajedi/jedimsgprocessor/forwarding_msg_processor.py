from pandacommon.pandalogger import logger_utils

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

base_logger = logger_utils.setup_logger(__name__.split(".")[-1])


# forwarding message processing plugin
class ForwardingMsgProcPlugin(BaseMsgProcPlugin):
    """
    Simply forward the message from one queue to another
    """

    def process(self, msg_obj):
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name="process")
        # start
        # tmp_log.info('start')
        # tmp_log.debug('sub_id={0} ; msg_id={1}'.format(msg_obj.sub_id, msg_obj.msg_id))
        # run
        try:
            msg = msg_obj.data
            tmp_log.debug(f"forward message {msg}")
        except Exception as e:
            err_str = f"failed to run, skipped. {e.__class__.__name__} : {e}"
            tmp_log.error(err_str)
            raise
        # done
        # tmp_log.info('done')
        return msg
