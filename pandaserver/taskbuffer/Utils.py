from typing import Generator


def create_shards(input_list: list, size: int) -> Generator:
    """
    Partitions input into shards of a given size for bulk operations.
    @author: Miguel Branco in DQ2 Site Services code

    Args:
        input_list (list): list to be partitioned
        size (int): size of the shards

    Returns:
        list: list of shards

    """
    shard, i = [], 0
    for element in input_list:
        shard.append(element)
        i += 1
        if i == size:
            yield shard
            shard, i = [], 0

    if i > 0:
        yield shard
