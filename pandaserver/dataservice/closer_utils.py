"""
Dataset utilities used by closer.
"""

import re

def is_top_level_dataset(dataset_name: str) -> bool:
    """
    Check if top dataset

    Args:
        dataset_name (str): Dataset name.

    Returns:
        bool: True if top dataset, False otherwise.
    """
    return re.sub("_sub\d+$", "", dataset_name) == dataset_name

def is_not_sub_dataset(dataset_name: str) -> bool:
    """
    Check if the dataset name does not end with '_sub' followed by one or more digits.

    Args:
        dataset_name (str): The name of the dataset.

    Returns:
        bool: True if the dataset name does not end with '_sub' followed by one or more digits, False otherwise.
    """
    return re.search("_sub\d+$", dataset_name) is None

def is_tid_dataset(destination_data_block: str) -> bool:
    """
    Check if the destination data block ends with '_tid' followed by one or more digits.

    Args:
        destination_data_block (str): The destination data block.

    Returns:
        bool: True if the destination data block ends with '_tid' followed by one or more digits, False otherwise.
    """
    return re.search("_tid[\d_]+$", destination_data_block) is not None

def is_hc_test_dataset(destination_data_block: str) -> bool:
    """
    Check if the destination data block starts with 'hc_test.'.

    Args:
        destination_data_block (str): The destination data block.

    Returns:
        bool: True if the destination data block starts with 'hc_test.', False otherwise.
    """
    return re.search("^hc_test\.", destination_data_block) is not None

def is_user_gangarbt_dataset(destination_data_block: str) -> bool:
    """
    Check if the destination data block starts with 'user.gangarbt.'.

    Args:
        destination_data_block (str): The destination data block.

    Returns:
        bool: True if the destination data block starts with 'user.gangarbt.', False otherwise.
    """
    return re.search("^user\.gangarbt\.", destination_data_block) is not None

def is_not_lib_dataset(destination_data_block: str) -> bool:
    """
    Check if the destination data block does not end with '.lib'.

    Args:
        destination_data_block (str): The destination data block.

    Returns:
        bool: True if the destination data block does not end with '.lib', False otherwise.
    """
    return re.search("\.lib$", destination_data_block) is None
