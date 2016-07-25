import re

from taskbuffer.TaskBuffer import taskBuffer
from config import panda_config
from pandalogger.PandaLogger import PandaLogger
_logger = PandaLogger().getLogger('TaskBuffer')

class Node(object):

    def __init__(self):
        self.children = []

    def add_child(self, node):
        self.children.append(node)

    def get_leaves(self, leaves=[]):

        # If the node has no leaves, return the node in a list
        if not self.children:
            leaves.append(self)
            return leaves

        # Recursively get to the bottom
        for child in self.children:
            child.get_leaves(leaves)

        return leaves


class Share(Node):
    """
    Implement the share node
    """
    def __str__(self, level=0):
        """
        Print the tree structure
        """
        ret = "{0} name: {1}, value: {2}\n".format('\t' * level, self.name, self.value)
        for child in self.children:
            ret += child.__str__(level + 1)
        return ret

    def __repr__(self):
        return self.__str__()

    def __mul__(self, other):
        """
        If I multiply a share object by a number, multiply the value field
        """
        self.value *= other
        return self.value

    def __rmul__(self, other):
        return self.__mul__

    def __imul__(self, other):
        return self.__mul__

    def __init__(self, name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype):
        Node.__init__(self)
        self.name = name
        self.value = value
        self.parent = parent
        self.prodsourcelabel = prodsourcelabel
        self.workinggroup = workinggroup
        self.campaign = campaign
        self.processingtype = processingtype

    def normalize(self, multiplier=100, divider=100):
        """
        Will run down the branch and normalize values beneath
        """
        self.value *= (multiplier * 1.0 / divider)
        if not self.children:
            return

        divider = 0
        for child in self.children:
            divider += child.value

        multiplier = self.value

        for child in self.children:
            child.normalize(multiplier=multiplier, divider=divider)

        return


class GlobalShares:
    """
    Class to manage the tree of shares
    """

    def __init__(self):

        # Initialize DB connection
        taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
        self.__task_buffer = taskBuffer

        # Root dummy node
        self.tree = Share('root', 100, None, None, None, None, None)

        # Get top level shares from DB
        shares_top_level = self.__task_buffer.getShares(parents=None)

        # Load branches
        for (name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype) in shares_top_level:
            share = Share(name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype)
            self.tree.children.append(self.__load_branch(share))

        # Normalize the values in the database
        self.tree.normalize()

        # get the leave shares (the ones not having more children)
        self.leave_shares = self.tree.get_leaves()


    def __load_branch(self, share):
        """
        Recursively load a branch
        """
        node = Share(share.name, share.value, share.parent, share.prodsourcelabel,
                     share.workinggroup, share.campaign, share.processingtype)

        children = self.__task_buffer.getShares(parents=share.name)
        if not children:
            return node

        for (name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype) in children:
            child = Share(name, value, parent, prodsourcelabel, workinggroup, campaign, processingtype)
            node.children.append(self.__load_branch(child))

        return node

    def compare_share_task(self, share, task):
        """
        Logic to compare the relevant fields of share and task
        """

        if share.prodsourcelabel is not None and re.match(share.prodsourcelabel, task.prodSourceLabel) is None:
            return False

        if share.workinggroup is not None and re.match(share.workinggroup, task.workingGroup) is None:
            return False

        if share.campaign is not None and re.match(share.campaign, task.campaign) is None:
            return False

        if share.processingtype is not None and re.match(share.processingtype, task.processingtype) is None:
            return False

        return True

    def get_share_for_task(self, task):
        """
        Return the share based on a task specification
        """

        selected_share_name = 'Undefined'

        for share in self.leave_shares:
            if self.compare_share_task(share, task):
                selected_share_name = share.name
                break

        if selected_share_name=='Undefined':
            _logger.warning("No share matching jediTaskId={0} (prodSourceLabel={1} workingGroup={2} campaign={3} )".
                                  format(task.jediTaskID, task.prodSourceLabel, task.workingGroup, task.campaign))

        return selected_share_name

    def is_valid_share(self, share_name):
        """
        Checks whether the share is a valid leave share
        """
        for share in self.leave_shares:
            if share_name == share.name:
                # Share found
                return True

        # Share not found
        return False

# Singleton
GlobalShares = GlobalShares()

if __name__ == "__main__":
    """
    Functional testing of the shares tree
    """
    global_shares = GlobalShares

    # print the global share structure
    print(global_shares.tree)

    # print the normalized leaves, which will be the actual applied shares
    print(global_shares.leave_shares)

    # check a couple of shares if they are valid leave names
    share_name = 'wrong_share'
    print ("Share {0} is valid: {1}".format(share_name, global_shares.is_valid_share(share_name)))
    share_name = 'MC16Pile'
    print ("Share {0} is valid: {1}".format(share_name, global_shares.is_valid_share(share_name)))

    # create a fake tasks with relevant fields and retrieve its share
    from pandajedi.jedicore.JediTaskSpec import JediTaskSpec
    task_spec = JediTaskSpec()

    # Analysis task
    task_spec.prodSourceLabel = 'user'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'dummy_wg'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Analysis')".format(global_shares.get_share_for_task(task_spec)))

    # Production task without any matching leave
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'dummy_wg'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Undefined')".format(global_shares.get_share_for_task(task_spec)))

    # Test task
    task_spec.prodSourceLabel = 'test123'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'dummy_wg'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Test')".format(global_shares.get_share_for_task(task_spec)))

    # Derivations task without any matching leave
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'GP_PHYS'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Undefined')".format(global_shares.get_share_for_task(task_spec)))

    # Reprocessing task without any matching leave
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'AP_REPR'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Undefined')".format(global_shares.get_share_for_task(task_spec)))

    # Group production task
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'GP_LOL'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Group production')".format(global_shares.get_share_for_task(task_spec)))

    # Upgrade task
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'AP_UPG'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Upgrade')".format(global_shares.get_share_for_task(task_spec)))

    # HLT Reprocessing
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'AP_THLT'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'HLT Reprocessing')".format(global_shares.get_share_for_task(task_spec)))

    # Validation
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'AP_VALI'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Validation')".format(global_shares.get_share_for_task(task_spec)))

    # Event Index
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'dummy_campaign'
    task_spec.workingGroup = 'proj-evind'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Event Index')".format(global_shares.get_share_for_task(task_spec)))

    # MC Derivations
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'mc.*'
    task_spec.workingGroup = 'GP_PHYS'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'MC Derivations')".format(global_shares.get_share_for_task(task_spec)))

    # Data Derivations
    task_spec.prodSourceLabel = 'managed'
    task_spec.campaign = 'data.*'
    task_spec.workingGroup = 'GP_PHYS'
    task_spec.processingType = 'dummy_type'
    print("Share for task is {0}(should be 'Data Derivations')".format(global_shares.get_share_for_task(task_spec)))