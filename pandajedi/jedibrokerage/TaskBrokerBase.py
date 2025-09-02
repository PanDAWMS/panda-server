from pandajedi.jedicore import Interaction


# base class for task brokerge
class TaskBrokerBase(object):
    def __init__(self, taskBufferIF, ddmIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.refresh()

    def refresh(self):
        self.siteMapper = self.taskBufferIF.get_site_mapper()


Interaction.installSC(TaskBrokerBase)
