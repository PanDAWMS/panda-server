__author__ = "retmas"

from collections.abc import Callable
from typing import Any

from snakemake.parser import Rule, RuleKeywordState
from snakemake.workflow import Workflow


# These three are installed on snakemake's Workflow by inject() below, so self is that
# workflow and the decorator they answer takes snakemake's RuleInfo -- neither of which
# carries a type of its own
def scatter(self: Any, value: Any) -> Callable[[Any], Any]:
    def decorate(rule_info: Any) -> Any:
        rule_info.scatter = value
        return rule_info

    return decorate


class Scatter(RuleKeywordState):
    @property
    def keyword(self) -> str:
        return "scatter"


def loop(self: Any, value: Any) -> Callable[[Any], Any]:
    def decorate(rule_info: Any) -> Any:
        rule_info.loop = value
        return rule_info

    return decorate


class Loop(RuleKeywordState):
    @property
    def keyword(self) -> str:
        return "loop"


def condition(self: Any, value: Any) -> Callable[[Any], Any]:
    def decorate(rule_info: Any) -> Any:
        rule_info.condition = value
        return rule_info

    return decorate


class Condition(RuleKeywordState):
    @property
    def keyword(self) -> str:
        return "when"


_rule_properties = dict(scatter=Scatter, loop=Loop, when=Condition)


def inject() -> None:
    Rule.subautomata.update(**_rule_properties)
    setattr(Workflow, "scatter", scatter)
    setattr(Workflow, "loop", loop)
    setattr(Workflow, "when", condition)
    rule = getattr(Workflow, "rule")

    def rule_new(self: Any, name: str | None = None, lineno: int | None = None, snakefile: str | None = None, checkpoint: bool = False) -> Callable[[Any], Any]:
        decorate = rule(self, name, lineno, snakefile, checkpoint)

        def decorate_new(rule_info: Any) -> Any:
            rule_obj = self.get_rule(name)
            if getattr(rule_info, "scatter", None):
                rule_obj.scatter = rule_info.scatter
            if getattr(rule_info, "loop", None):
                rule_obj.loop = rule_info.loop
            if getattr(rule_info, "condition", None):
                rule_obj.condition = rule_info.condition
            return decorate(rule_info)

        return decorate_new

    setattr(Workflow, "rule", rule_new)
