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

    def __init__(self, name, value, parent, prodsourcelabel, workinggroup, campaign):
        Node.__init__(self)
        self.name = name
        self.value = value
        self.parent = parent
        self.prodsourcelabel = prodsourcelabel
        self.workinggroup = workinggroup
        self.campaign = campaign

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

    def __init__(self, task_buffer):

        self.__task_buffer = task_buffer
        # Root dummy node
        self.tree = Share('root', 100, None, None, None, None)

        # Get top level shares from DB
        shares_top_level = self.__task_buffer.getShares(parents=None)

        # Load branches
        for (name, value, parent, prodsourcelabel, workinggroup, campaign) in shares_top_level:
            share = Share(name, value, parent, prodsourcelabel, workinggroup, campaign)
            self.tree.children.append(self.__load_branch(share))

        # Normalize the values in the database
        self.tree.normalize()

    def __load_branch(self, share):
        """
        Recursively load a branch
        """
        node = Share(share.name, share.value, share.parent, share.prodsourcelabel, share.workinggroup, share.campaign)

        children = self.__task_buffer.getShares(parents=share.name)
        if not children:
            return node

        for (name, value, parent, prodsourcelabel, workinggroup, campaign) in children:
            child = Share(name, value, parent, prodsourcelabel, workinggroup, campaign)
            node.children.append(self.__load_branch(child))

        return node

    def compare_share_task(self, share, task):
        """
        Logic to compare the relevant fields of share and task
        """

        if share.prodsourcelabel is not None and share.prodsourcelabel != task.prodSourceLabel:
            return False

        if share.workinggroup is not None and share.workinggroup != task.workingGroup:
            return False

        if share.campaign is not None and share.campaign != task.campaign:
            return False

        return True

    def get_share_for_task(self, task):
        """
        Return the share based on a task specification
        """

        selected_share_name = None

        # get the leave shares (the ones not having more children)
        leave_shares = self.tree.get_leaves()

        for share in leave_shares:
            if self.compare_share_task(share, task):
                selected_share_name = share.name
                break

        if selected_share_name is None:
            self.__logger.warning("No share matching jediTaskId={0} (prodSourceLabel={1} workingGroup={2} campaign={3}".
                    format(task.prodSourceLabel, task.workingGroup, task.campaign))

        return selected_share_name

if __name__ == "__main__":
    """
    Functional testing of the shares tree
    """

    from taskbuffer.TaskBuffer import taskBuffer
    from config import panda_config

    taskBuffer.init(panda_config.dbhost, panda_config.dbpasswd, nDBConnection=1)
    global_shares = GlobalShares(taskBuffer)

    # print the global share structure
    print(global_shares.tree)

    # print the normalized leaves, which will be the actual applied shares
    print(global_shares.tree.get_leaves())