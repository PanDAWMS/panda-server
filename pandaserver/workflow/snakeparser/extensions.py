__author__ = "retmas"

from snakemake.parser import Rule, RuleKeywordState
from snakemake.workflow import Workflow


def scatter(self, value):
    def decorate(rule_info):
        rule_info.scatter = value
        return rule_info

    return decorate


class Scatter(RuleKeywordState):
    @property
    def keyword(self):
        return "scatter"


def loop(self, value):
    def decorate(rule_info):
        rule_info.loop = value
        return rule_info

    return decorate


class Loop(RuleKeywordState):
    @property
    def keyword(self):
        return "loop"


def condition(self, value):
    def decorate(rule_info):
        rule_info.condition = value
        return rule_info

    return decorate


class Condition(RuleKeywordState):
    @property
    def keyword(self):
        return "when"


_rule_properties = dict(scatter=Scatter, loop=Loop, when=Condition)


def inject():
    Rule.subautomata.update(**_rule_properties)
    setattr(Workflow, "scatter", scatter)
    setattr(Workflow, "loop", loop)
    setattr(Workflow, "when", condition)
    rule = getattr(Workflow, "rule")

    def rule_new(self, name=None, lineno=None, snakefile=None, checkpoint=False):
        decorate = rule(self, name, lineno, snakefile, checkpoint)

        def decorate_new(rule_info):
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
