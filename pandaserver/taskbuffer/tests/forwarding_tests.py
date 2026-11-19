import pickle
import unittest
from typing import Any
from unittest import mock

from pandaserver.taskbuffer.TaskBuffer import TaskBuffer


def make_task_buffer(proxy: Any) -> tuple[TaskBuffer, mock.MagicMock]:
    """A TaskBuffer whose pool hands out the given proxy, without connecting to a database, and the pool"""
    pool = mock.MagicMock()
    pool.get.return_value.__enter__.return_value = proxy
    task_buffer = TaskBuffer.__new__(TaskBuffer)
    task_buffer.proxyPool = pool
    return task_buffer, pool


class TestForwarding(unittest.TestCase):
    def test_forwards_a_dbproxy_method_through_the_pool(self) -> None:
        proxy = mock.MagicMock()
        proxy.getTaskParamsWithID_JEDI.return_value = "{}"
        task_buffer, pool = make_task_buffer(proxy)
        self.assertEqual(task_buffer.getTaskParamsWithID_JEDI(42), "{}")
        proxy.getTaskParamsWithID_JEDI.assert_called_once_with(42)
        # the proxy goes back to the pool after the call
        pool.get.return_value.__exit__.assert_called_once()

    def test_keyword_arguments_are_passed_on(self) -> None:
        proxy = mock.MagicMock()
        task_buffer, _ = make_task_buffer(proxy)
        task_buffer.getTaskParamsWithID_JEDI(42, use_commit=False)
        proxy.getTaskParamsWithID_JEDI.assert_called_once_with(42, use_commit=False)

    def test_the_proxy_goes_back_when_the_call_raises(self) -> None:
        proxy = mock.MagicMock()
        proxy.getTaskParamsWithID_JEDI.side_effect = RuntimeError("broken")
        task_buffer, pool = make_task_buffer(proxy)
        with self.assertRaises(RuntimeError):
            task_buffer.getTaskParamsWithID_JEDI(42)
        pool.get.return_value.__exit__.assert_called_once()

    def test_a_taskbuffer_method_is_not_forwarded(self) -> None:
        # TaskBuffer.querySQL is its own method; it calls DBProxy.querySQLS
        proxy = mock.MagicMock()
        proxy.querySQLS.return_value = (True, [(1,)])
        task_buffer, _ = make_task_buffer(proxy)
        self.assertEqual(task_buffer.querySQL("SELECT 1 FROM dual", {}), [(1,)])
        proxy.querySQL.assert_not_called()

    def test_the_forwarded_function_carries_the_name(self) -> None:
        task_buffer, _ = make_task_buffer(mock.MagicMock())
        self.assertEqual(task_buffer.getTaskParamsWithID_JEDI.__name__, "getTaskParamsWithID_JEDI")


class TestNotForwarded(unittest.TestCase):
    def test_private_names(self) -> None:
        task_buffer, _ = make_task_buffer(mock.MagicMock())
        for name in ("_commit", "_rollback", "__setstate__"):
            self.assertFalse(hasattr(task_buffer, name), name)

    def test_names_dbproxy_lacks(self) -> None:
        # by getattr: written as an attribute, the misspelt name is already refused by mypy
        task_buffer, _ = make_task_buffer(mock.MagicMock())
        self.assertFalse(hasattr(task_buffer, "no_such_method"))
        with self.assertRaises(AttributeError):
            getattr(task_buffer, "no_such_method")

    def test_dbproxy_attributes_that_are_not_methods(self) -> None:
        # conn and cur are set on a DBProxy instance, not methods of its class
        task_buffer, _ = make_task_buffer(mock.MagicMock())
        self.assertFalse(hasattr(task_buffer, "conn"))
        self.assertFalse(hasattr(task_buffer, "cur"))

    def test_proxy_pool_before_it_is_set_is_an_attribute_error(self) -> None:
        task_buffer = TaskBuffer.__new__(TaskBuffer)
        with self.assertRaises(AttributeError):
            getattr(task_buffer, "proxyPool")

    def test_hasattr_and_getattr_as_the_pipe_receivers_use_them(self) -> None:
        # JEDI's CommandReceiveInterface and the server's TaskBufferInterface find methods by name
        task_buffer, _ = make_task_buffer(mock.MagicMock())
        self.assertTrue(hasattr(task_buffer, "getTaskParamsWithID_JEDI"))
        self.assertTrue(callable(getattr(task_buffer, "getTaskParamsWithID_JEDI")))

    def test_pickling_does_not_reach_the_forwarding(self) -> None:
        # pickle looks names such as __reduce_ex__ and __setstate__ up; none may be forwarded
        task_buffer = TaskBuffer.__new__(TaskBuffer)
        task_buffer.nDBConnection = 3
        copied = pickle.loads(pickle.dumps(task_buffer))
        self.assertEqual(copied.nDBConnection, 3)


if __name__ == "__main__":
    unittest.main()
