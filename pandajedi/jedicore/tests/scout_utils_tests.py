# Description: Unit tests for the scout utility functions
import unittest

from pandajedi.jedicore.ScoutUtils import get_num_scout_chunks_for_dataset


def distribute(total_chunks: int, consumable_chunks: list[int]) -> list[int]:
    """
    Simulate ContentsFeeder feeding master datasets in order. consumable_chunks[i] is the max
    number of chunks dataset i can fill, and the return value is the chunks it actually consumes.
    """
    remaining = total_chunks
    consumed_list = []
    for i_ds, consumable in enumerate(consumable_chunks):
        alloc = get_num_scout_chunks_for_dataset(remaining, len(consumable_chunks) - i_ds)
        consumed = min(alloc, consumable)
        consumed_list.append(consumed)
        remaining = max(remaining - consumed, 0)
    return consumed_list


class TestGetNumScoutChunksForDataset(unittest.TestCase):
    def test_single_dataset(self) -> None:
        self.assertEqual(distribute(10, [100]), [10])

    def test_few_datasets(self) -> None:
        self.assertEqual(distribute(10, [100, 100, 100]), [4, 3, 3])

    def test_as_many_datasets_as_chunks(self) -> None:
        self.assertEqual(distribute(10, [100] * 10), [1] * 10)

    def test_more_datasets_than_chunks(self) -> None:
        self.assertEqual(distribute(10, [100] * 15), [1] * 10 + [0] * 5)

    def test_leftover_goes_to_following_datasets(self) -> None:
        self.assertEqual(distribute(10, [1, 100, 100]), [1, 5, 4])

    def test_no_budget(self) -> None:
        self.assertEqual(get_num_scout_chunks_for_dataset(0, 3), 0)
        self.assertEqual(get_num_scout_chunks_for_dataset(-2, 3), 0)

    def test_last_dataset_gets_all(self) -> None:
        self.assertEqual(get_num_scout_chunks_for_dataset(7, 1), 7)
        self.assertEqual(get_num_scout_chunks_for_dataset(7, 0), 7)


if __name__ == "__main__":
    unittest.main()
