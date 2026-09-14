__author__ = "retmas"

from .parser import Parser

# Parser is imported above to be re-exported: workflow_parser and workflow_processor take
# it from this package rather than from .parser, so naming it here says that the import is
# the package's interface and not a leftover.
__all__ = ["Parser"]
