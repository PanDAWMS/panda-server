"""
Utilities for scout jobs
"""


def get_num_scout_chunks_for_dataset(remaining_chunks: int, remaining_datasets: int) -> int:
    """
    Get the number of scout chunks to allocate to a master input dataset, so that the scout chunks
    of a task are spread over its master input datasets instead of being consumed by the first ones.
    When there are as many master input datasets as scout chunks or more, the first datasets get one
    chunk each and the rest get none. Otherwise, the chunks are distributed almost evenly. The caller
    subtracts the chunks actually consumed, so chunks left unused by a small dataset go to the following ones.

    :param remaining_chunks: The number of scout chunks not yet consumed by the preceding datasets.
    :param remaining_datasets: The number of master input datasets still to be fed, including this one.

    :return: The number of scout chunks for the dataset.
    """
    if remaining_chunks <= 0:
        return 0
    if remaining_datasets <= 1:
        return remaining_chunks
    # ceiling division
    return -(-remaining_chunks // remaining_datasets)
