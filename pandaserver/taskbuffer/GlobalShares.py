from

class SelfRepresented(object):

    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append("{key}='{value}'".format(key=key, value=self.__dict__[key]))
        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()


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


class Share(Node, SelfRepresented):
    """
    Implement the share node
    """

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
        self.value *= (multiplier*1.0/divider)
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

        # Root dummy node
        self.tree = Share(0, 'root', 100, None, '', '', '', '')

        # Get top level shares from DB
        shares_top_level = self.task_buffer.getShares(parent=None)

        # Load branches
        for share in shares_top_level:
            self.tree.children.append(self.load_branch(share))

        # Normalize the values in the database
        self.tree.normalize()
        return self.tree

    def load_branch(self, share):
        """
        Recursively load a branch
        """
        node = Share(share.name, share.value, share.parent, share.criteria, share.variables)

        children = self.task_buffer.getShares(parent=share.name)
        if not children:
            return node

        for child in children:
            node.children.append(self.load_branch(child))

        return node