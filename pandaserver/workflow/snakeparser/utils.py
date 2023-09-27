__author__ = "retmas"

from snakemake.workflow import Rule, RuleProxy


class ParamRule(object):
    def __init__(self, name: str, source_rule: Rule = None):
        self._name = name
        self._source_rule = source_rule

    def __repr__(self):
        return f"param.{self.name}"

    @property
    def name(self) -> str:
        return self._name

    @property
    def rule(self) -> Rule:
        return self._source_rule


def param_of(name, source: RuleProxy = None) -> ParamRule:
    rule = source.rule if source is not None else None
    return ParamRule(name, rule)


def param_exp(template):
    return lambda wildcards: template
