# Definitions
EXECUTING = "executing"
QUEUED = "queued"
PLEDGED = "pledged"


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

    _attributes = (
        "name",
        "value",
        "parent",
        "prodsourcelabel",
        "workinggroup",
        "campaign",
        "processingtype",
        "transpath",
        "vo",
        "rtype",
        "queue_id",
        "throttled",
    )

    def __str__(self, level=0):
        """
        Print the tree structure
        """
        ret = "{0} name: {1}, value: {2}\n".format("\t" * level, self.name, self.value)
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

    def __init__(
        self,
        name,
        value,
        parent,
        prodsourcelabel,
        workinggroup,
        campaign,
        processingtype,
        transpath,
        rtype,
        vo,
        queue_id,
        throttled,
    ):
        # Create default attributes
        for attr in self._attributes:
            setattr(self, attr, None)

        Node.__init__(self)
        self.name = name
        self.value = value
        self.parent = parent
        self.prodsourcelabel = prodsourcelabel
        self.workinggroup = workinggroup
        self.campaign = campaign
        self.processingtype = processingtype
        self.transpath = transpath
        self.rtype = rtype
        self.vo = vo
        self.queue_id = queue_id
        self.throttled = throttled

    def pretty_print_hs_distribution(self, hs_distribution, level=0):
        try:
            executing = hs_distribution[self.name][EXECUTING] / 1000.0
        except Exception:
            executing = 0

        try:
            target = hs_distribution[self.name][PLEDGED] / 1000.0
        except Exception:
            target = 0

        try:
            queued = hs_distribution[self.name][QUEUED] / 1000.0
        except Exception:
            queued = 0

        ret = "{0} name: {1}, values: {2:.1f}k|{3:.1f}k|{4:.1f}k\n".format("\t" * level, self.name, executing, target, queued)
        for child in self.children:
            ret += child.pretty_print_hs_distribution(hs_distribution, level + 1)
        return ret

    def normalize(self, multiplier=100, divider=100):
        """
        Will run down the branch and normalize values beneath
        """
        self.value *= multiplier * 1.0 / divider
        if not self.children:
            return

        divider = 0
        for child in self.children:
            divider += child.value

        multiplier = self.value

        for child in self.children:
            child.normalize(multiplier=multiplier, divider=divider)

        return

    def sort_branch_by_current_hs_distribution(self, hs_distribution):
        """
        Runs down the branch in order of under-pledging. It returns a list of sorted leave shares
        """
        sorted_shares = []

        # If the node has no leaves, return the node in a list
        if not self.children:
            sorted_shares = [self]
            return sorted_shares

        # If the node has leaves, sort the children
        children_sorted = []
        for child1 in self.children:
            loop_index = 0
            insert_index = len(children_sorted)  # insert at the end, if not deemed otherwise

            # Calculate under-pledging
            try:
                child1_under_pledge = hs_distribution[child1.name][EXECUTING] * 1.0 / hs_distribution[child1.name][PLEDGED]
            except ZeroDivisionError:
                child1_under_pledge = 10**6  # Initialize to a large default number

            for child2 in children_sorted:
                try:
                    # Calculate under-pledging
                    child2_under_pledge = hs_distribution[child2.name][EXECUTING] * 1.0 / hs_distribution[child2.name][PLEDGED]
                except ZeroDivisionError:
                    child2_under_pledge = 10**6  # Initialize to a large default number
                except KeyError:
                    continue  # Does not exist

                if child1_under_pledge < child2_under_pledge:
                    insert_index = loop_index
                    break

                loop_index += 1

            # Insert the child into the list
            children_sorted.insert(insert_index, child1)

        # Go recursively and sort the grand* children
        for child in children_sorted:
            sorted_shares.extend(child.sort_branch_by_current_hs_distribution(hs_distribution))

        return sorted_shares

    def aggregate_hs_distribution(self, hs_distribution):
        """
        We have the current HS distribution values for the leaves, but want to propagate it updwards to the parents.
        We will traverse the tree from top to bottom and bring up the aggregated values.
        """
        executing, queued, pledged = 0, 0, 0

        # If the node has no children, it's a leave and should have an entry in the hs_distribution
        if not self.children:
            try:
                executing = hs_distribution[self.name][EXECUTING]
                queued = hs_distribution[self.name][QUEUED]
                pledged = hs_distribution[self.name][PLEDGED]
            except KeyError:
                pass

            return executing, queued, pledged

        # If the node has children, sum up the values of the children
        executing = 0
        queued = 0
        pledged = 0

        for child in self.children:
            (
                executing_child,
                queued_child,
                pledged_child,
            ) = child.aggregate_hs_distribution(hs_distribution)
            executing += executing_child
            queued += queued_child
            pledged += pledged_child

        # Add the aggregated value to the map
        hs_distribution[self.name] = {
            EXECUTING: executing,
            QUEUED: queued,
            PLEDGED: pledged,
        }

        # Return the aggregated values
        return executing, queued, pledged

    # return column names
    def column_names(cls):
        ret = ""
        for attr in cls._attributes:
            if ret != "":
                ret += ","
            ret += attr
        return ret

    column_names = classmethod(column_names)
