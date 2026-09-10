"""
Type of a spec column whose unset value reads back as the "NULL" sentinel.

JobSpec, FileSpec and DatasetSpec install their columns as None and override
__getattribute__ to substitute the string "NULL" for a column that is still None. A read
therefore yields the column type or that sentinel, while a write takes the column type or
None -- two types for one attribute, which only a descriptor can express.

Declaring a column as SpecColumn[int] instead of `int | str` gives a type checker both
halves:

    job.PandaID            # int | str
    job.PandaID = 5        # ok
    job.PandaID = None     # ok, and reads back as "NULL"
    job.PandaID = "5"      # error, which `int | str` could not catch

The declarations carry no value, so nothing here runs beyond building the generic alias
when the spec module is imported; the columns are still installed by __init__.

The "NULL" sentinel is legacy debt. When it goes, every SpecColumn[T] becomes T | None and
this module is deleted.
"""

from typing import Generic, Literal, TypeVar

_T = TypeVar("_T")

# the sentinel __getattribute__ substitutes for a column that is still None
Null = Literal["NULL"]


class SpecColumn(Generic[_T]):
    """A spec column: read as _T or the "NULL" sentinel, written as _T, None or that
    sentinel -- writing it back is how a column is copied from one spec to another, and
    valuesMap() turns it into a NULL bind value just as it does for None."""

    def __get__(self, obj: object, objtype: "type | None" = None) -> "_T | Null":
        # never reached: the descriptor is only ever a type. The columns themselves are
        # installed on the instance by the spec's __init__.
        raise NotImplementedError

    def __set__(self, obj: object, value: "_T | Null | None") -> None:
        raise NotImplementedError
