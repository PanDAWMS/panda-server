"""
Offline check of how analysis brokerage orders site candidates before the maxNumSites cut.

The ordering decides which sites survive truncation, so a site missing from the front cannot be
chosen however good its weight. It used to be dead code: the membership test compared
SiteCandidate objects against a collection of site-name strings, so the data-holding sites were
never moved to the front. Fixing that changes placement for every analysis task, workflow or not,
which is why the non-workflow behaviour is pinned here too.

Run from the repository root:  python3 pandajedi/jedibrokerage/tests/site_ordering_test.py
"""

import os
import random
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from pandajedi.jedibrokerage.workflow_affinity import (  # noqa: E402
    DEFAULT_WORKFLOW_AFFINITY_FACTOR,
    build_anchor_weights,
    order_site_candidates,
    workflow_affinity_weight,
)


class SiteCandidate:
    """Just enough of the real one: the ordering only ever reads these two names"""

    def __init__(self, site_name, unified_name):
        self.siteName = site_name
        self.unifiedName = unified_name


def make_candidate(name):
    """A candidate as the broker builds it: pseudo name first, unified name second"""
    return SiteCandidate(name, name.split("/")[0])


def names(candidates):
    return [c.siteName for c in candidates]


def check(label, condition, detail=""):
    print(f"  {'PASS' if condition else 'FAIL'}  {label}{'  ' + str(detail) if detail and not condition else ''}")
    return condition


def main():
    failures = 0

    print("\n=== data-holding sites lead the list ===")
    # This is what never happened before: the comparison was object-against-string.
    with_data = make_candidate("SITE_A")
    without_data = make_candidate("SITE_B")
    ordered = order_site_candidates({1.0: [without_data], 2.0: [with_data]}, {"SITE_A"}, [])
    failures += not check("the data-holding site is first", names(ordered)[0] == "SITE_A", names(ordered))
    # ...even when it weighs less, because the cut is what it is being rescued from
    ordered = order_site_candidates({5.0: [without_data], 0.1: [with_data]}, {"SITE_A"}, [])
    failures += not check("...even when it weighs less than one without data", names(ordered)[0] == "SITE_A", names(ordered))

    print("\n=== the membership test names the pseudo site ===")
    # SiteCandidate(pseudo, unified): scanSiteList, and so site_list_with_data, carries the pseudo
    # name. Comparing the unified name would make the whole thing a silent no-op again.
    # A promoted candidate goes ahead of the already-used ones, so one used site makes the
    # partition observable without depending on the shuffle.
    pseudo = SiteCandidate("SITE_C/pseudo", "SITE_C")
    marker = make_candidate("USED")
    ordered = order_site_candidates({1.0: [pseudo]}, {"SITE_C/pseudo"}, [marker])
    failures += not check("matched by pseudo name", names(ordered) == ["SITE_C/pseudo", "USED"], names(ordered))
    ordered = order_site_candidates({1.0: [pseudo]}, {"SITE_C"}, [marker])
    failures += not check("not matched by unified name", names(ordered) == ["USED", "SITE_C/pseudo"], names(ordered))

    print("\n=== weight still orders within each group ===")
    low, mid, high = make_candidate("LOW"), make_candidate("MID"), make_candidate("HIGH")
    ordered = order_site_candidates({1.0: [low], 2.0: [mid], 3.0: [high]}, set(), [])
    failures += not check("highest weight first", names(ordered) == ["HIGH", "MID", "LOW"], names(ordered))

    print("\n=== sites already used by the task keep their place ===")
    used = make_candidate("USED")
    ordered = order_site_candidates({1.0: [make_candidate("PLAIN")]}, set(), [used])
    failures += not check("ahead of sites with no data", names(ordered) == ["USED", "PLAIN"], names(ordered))
    ordered = order_site_candidates({1.0: [with_data]}, {"SITE_A"}, [used])
    failures += not check("behind sites with data", names(ordered) == ["SITE_A", "USED"], names(ordered))

    print("\n=== equal weights are still randomised ===")
    # The common case is one locality pass, where every survivor is in site_list_with_data. If the
    # shuffle happened after the partition rather than before, every candidate would land in the
    # data-holding group in a fixed order and ties would stop varying at the cut.
    everything = [f"SITE_{i:02d}" for i in range(12)]
    orders = set()
    for _ in range(200):
        candidates = [make_candidate(n) for n in everything]
        ordered = order_site_candidates({1.0: list(candidates)}, set(everything), [])
        orders.add(tuple(names(ordered)))
    failures += not check("ties vary across runs when all sites hold data", len(orders) > 1, f"{len(orders)} distinct orders")
    failures += not check("...and nothing is lost or duplicated", all(sorted(o) == sorted(everything) for o in orders))

    print("\n=== nothing is dropped ===")
    candidates = [make_candidate(f"S{i}") for i in range(6)]
    ordered = order_site_candidates({1.0: candidates[:3], 2.0: candidates[3:5]}, {"S0", "S3"}, [candidates[5]])
    failures += not check("every candidate comes back exactly once", sorted(names(ordered)) == sorted(names(candidates)), names(ordered))

    print("\n=== an empty input is not an error ===")
    failures += not check("no weights, no used sites", order_site_candidates({}, set(), []) == [])
    failures += not check("no weights but used sites", names(order_site_candidates({}, set(), [used])) == ["USED"])

    print("\n=== the caller's list is not mutated ===")
    # The broker reuses weightMap and candidateSpecList afterwards for logging.
    weight_map = {1.0: [make_candidate("X"), make_candidate("Y")]}
    used_list = [used]
    order_site_candidates(weight_map, {"X"}, used_list)
    failures += not check("the weight map still holds its candidates", len(weight_map[1.0]) == 2, len(weight_map[1.0]))
    failures += not check("the used list is unchanged", len(used_list) == 1, len(used_list))

    print("\n=== workflow anchor sites also survive the cut ===")
    # Keyed by unified name, unlike site_list_with_data, so both have to be honoured.
    anchored = SiteCandidate("SITE_W/pseudo", "SITE_W")
    plain = make_candidate("SITE_X")
    ordered = order_site_candidates({2.0: [plain], 1.0: [anchored]}, set(), [], {"SITE_W"})
    failures += not check("an anchor site leads despite a lower weight", names(ordered)[0] == "SITE_W/pseudo", names(ordered))
    ordered = order_site_candidates({2.0: [plain], 1.0: [anchored]}, set(), [], None)
    failures += not check("without an anchor the weight decides", names(ordered)[0] == "SITE_X", names(ordered))
    ordered = order_site_candidates({2.0: [plain], 1.0: [anchored]}, set(), [], set())
    failures += not check("an empty anchor changes nothing", names(ordered)[0] == "SITE_X", names(ordered))

    print("\n=== the affinity weight is a bounded preference ===")
    weights = {"BEST": 10.0, "MIDDLE": 5.0, "NONE": 0.0}
    best = workflow_affinity_weight("BEST", weights)
    middle = workflow_affinity_weight("MIDDLE", weights)
    failures += not check("the strongest anchor earns the full factor", best == DEFAULT_WORKFLOW_AFFINITY_FACTOR, best)
    failures += not check("a weaker anchor earns less", 1 < middle < best, middle)
    # never below 1: a site that is not an anchor must not be penalised, only not helped
    failures += not check("an unknown site is unchanged", workflow_affinity_weight("ELSEWHERE", weights) == 1.0)
    failures += not check("a zero-weight site is unchanged", workflow_affinity_weight("NONE", weights) == 1.0)
    failures += not check("no anchor at all is unchanged", workflow_affinity_weight("ANY", None) == 1.0)
    failures += not check("an empty anchor is unchanged", workflow_affinity_weight("ANY", {}) == 1.0)
    failures += not check("all-zero weights are unchanged", workflow_affinity_weight("A", {"A": 0.0}) == 1.0)
    failures += not check("the factor is honoured", workflow_affinity_weight("BEST", weights, factor=3.0) == 3.0)

    print("\n=== the anchor weighs sites against each other ===")
    # The five-step case: s2's input is at A, s3's and s4's are both at B. Transfer is unavoidable,
    # but B is the cheaper anchor for the step that has a free choice. A flat set cannot say this.
    anchor = build_anchor_weights(input_sites=["SITE_A", "SITE_B", "SITE_B"])
    failures += not check("the site more of the workflow points at wins", anchor["SITE_B"] > anchor["SITE_A"], anchor)
    failures += not check("...and so earns the larger boost", workflow_affinity_weight("SITE_B", anchor) > workflow_affinity_weight("SITE_A", anchor))

    print("\n=== firmer knowledge outweighs prediction ===")
    anchor = build_anchor_weights(pinned_sites=["PINNED"], input_sites=["GUESS", "GUESS", "GUESS"])
    failures += not check("a pinned sibling beats three predictions", anchor["PINNED"] > anchor["GUESS"], anchor)
    anchor = build_anchor_weights(output_sites=["RAN"], input_sites=["GUESS"])
    failures += not check("output already written beats a prediction", anchor["RAN"] > anchor["GUESS"], anchor)
    anchor = build_anchor_weights(output_sites=["RAN"], input_sites=["GUESS", "GUESS", "GUESS"])
    failures += not check("but enough predictions still outweigh one finished step", anchor["GUESS"] > anchor["RAN"], anchor)

    print("\n=== a site some step cannot use is no anchor ===")
    anchor = build_anchor_weights(input_sites=["GOOD", "BANNED", "BANNED"], vetoed_sites={"BANNED"})
    failures += not check("the vetoed site is absent, not merely lighter", "BANNED" not in anchor, anchor)
    failures += not check("the rest survives", anchor.get("GOOD") == 1.0, anchor)
    failures += not check("nothing at all yields an empty anchor", build_anchor_weights() == {})

    print(f"\n{'ALL CHECKS PASSED' if not failures else f'{failures} CHECK(S) FAILED'}")
    return 1 if failures else 0


if __name__ == "__main__":
    random.seed()
    sys.exit(main())
