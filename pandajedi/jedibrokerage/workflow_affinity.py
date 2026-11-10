"""
Pure site-selection helpers used by analysis brokerage.

Deliberately free of heavy imports. AtlasBrokerUtils and AtlasAnalJobBroker reach the grid, the
database and the site mapper at import time, so nothing in them can be exercised without a live
instance. The decisions here are arithmetic over lists and maps, which is exactly the part worth
testing on its own, so they live where a test can import them.

Candidates are duck-typed: anything with a ``siteName`` attribute will do.
"""

from __future__ import annotations

import random
from typing import Any, Collection, Iterable, Protocol, TypeVar


class SiteCandidateLike(Protocol):
    """What the ordering needs of a candidate: the two names it is known by

    Structural rather than the concrete SiteCandidate, because nothing here touches the rest of
    that class and a caller testing the ordering should not have to build one.
    """

    # read-only members, so a class declaring either name more narrowly still matches
    @property
    def siteName(self) -> str: ...

    @property
    def unifiedName(self) -> str | None: ...


# generic over the candidate, so a caller gets back the concrete type it passed in
CandidateT = TypeVar("CandidateT", bound=SiteCandidateLike)

# How much a site may be favoured for holding the rest of its workflow. The anchor weight of a
# site is scaled into [1, this] before it multiplies the site's brokerage weight, so the strongest
# anchor is worth this many times an unrelated site and the weakest is worth nothing extra. A
# multiplier rather than an addition, because the weights it joins span orders of magnitude.
DEFAULT_WORKFLOW_AFFINITY_FACTOR = 5.0

# Where the workflow manager records the anchor on the workflow, and brokerage reads it from. One
# name shared by producer and consumer so the two cannot drift apart silently.
WORKFLOW_ANCHOR_PARAM = "brokerage_anchor"


def order_site_candidates(
    weight_map: dict[float, list[CandidateT]],
    site_list_with_data: Collection[str],
    used_site_candidates: list[CandidateT],
    preferred_sites: Collection[str] | None = None,
) -> list[CandidateT]:
    """
    Order site candidates highest weight first, with the sites already holding data in front

    The order decides who survives the maxNumSites cut that follows, so a site missing from the
    front of this list cannot be chosen however good its weight. Sites the task has already run at
    are kept ahead of the rest but behind the data-holding ones, which is where the caller's
    partly-filled list puts them.

    Args:
        weight_map: final brokerage weight to the candidates carrying it
        site_list_with_data: names of the sites that survived the data-locality pass, as
            scanSiteList carries them, i.e. pseudo names
        used_site_candidates: candidates the caller has already set aside, being the sites this
            task has run at before
        preferred_sites: unified names of sites holding the rest of this task's workflow, which
            are promoted alongside the data-holding ones so that co-location survives the cut

    Returns:
        list[SiteCandidate]: the candidates in the order they should be offered
    """
    preferred_sites = preferred_sites or set()
    promoted: list[CandidateT] = []
    others = list(used_site_candidates)
    for weight_value in sorted(weight_map.keys(), reverse=True):
        candidates = list(weight_map[weight_value])
        # Shuffle before partitioning, not after. site_list_with_data is the whole surviving list
        # of the data-locality pass, so when only that pass runs -- the common case -- every
        # candidate holds data and the partition below takes all of them. Shuffling what is left
        # over would then leave equal-weight sites in a fixed order at the cut, where they have
        # always been randomised.
        random.shuffle(candidates)
        for site_candidate in candidates:
            # site_list_with_data holds site names while these are SiteCandidate objects, so the
            # membership test has to name the attribute. siteName is the pseudo name, which is
            # what scanSiteList carries; the workflow anchor is keyed by unified name instead.
            if site_candidate.siteName in site_list_with_data or getattr(site_candidate, "unifiedName", None) in preferred_sites:
                promoted.append(site_candidate)
            else:
                others.append(site_candidate)
    return promoted + others


def workflow_affinity_weight(unified_site_name: str, anchor_weights: dict[str, float] | None, factor: float = DEFAULT_WORKFLOW_AFFINITY_FACTOR) -> float:
    """
    How much to favour a site for already holding part of this task's workflow

    Scaled against the strongest anchor rather than used raw, so that the boost means the same
    thing whatever arithmetic produced the anchor: the site with most of the workflow behind it is
    worth ``factor`` times an unrelated site, and a site with none of it is worth exactly 1, i.e.
    unchanged. Returning 1 rather than 0 for the unknown case is what keeps this a preference --
    a site that is not an anchor is never penalised, only not helped.

    Args:
        unified_site_name: the site being weighed, by unified name
        anchor_weights: unified site name to anchor weight, as the workflow recorded it
        factor: the multiplier the strongest anchor earns

    Returns:
        float: a multiplier in [1, factor]
    """
    if not anchor_weights:
        return 1.0
    strongest = max(anchor_weights.values())
    if strongest <= 0:
        return 1.0
    share = anchor_weights.get(unified_site_name, 0) / strongest
    if share <= 0:
        return 1.0
    return 1.0 + (factor - 1.0) * share


def build_anchor_weights(
    pinned_sites: Iterable[str] = (),
    output_sites: Iterable[str] = (),
    input_sites: Iterable[str] = (),
    vetoed_sites: Collection[str] = (),
) -> dict[str, float]:
    """
    Combine what is known about a workflow's placement into one weighted preference map

    A map rather than a set, because the sites have to be comparable: in a workflow whose steps
    take input from A once and from B twice, both are anchors but B is the cheaper one to join,
    and a set cannot say so. Each source contributes per step, so a site named by several steps
    outweighs one named by a single step.

    The sources are ranked by how firm they are. A site a submitter pinned a step to is a fact
    that will not move; a site a finished step left its output at is a fact that may be joined by
    others; a site holding an input of a step not yet placed is a prediction. Repetition within a
    source still matters, so three predictions outweigh one.

    Args:
        pinned_sites: unified sites that a sibling step is pinned to, one entry per step
        output_sites: unified sites where a sibling step's output already sits, one entry per step
        input_sites: unified sites holding the input of a sibling step not yet placed, one entry
            per step per site
        vetoed_sites: unified sites some step of the workflow cannot use, which are worthless as
            an anchor for the others however much data sits there

    Returns:
        dict[str, float]: unified site name to anchor weight, vetoed sites absent
    """
    weights: dict[str, float] = {}
    for sites, per_step_weight in ((pinned_sites, 4.0), (output_sites, 2.0), (input_sites, 1.0)):
        for site_name in sites:
            if not site_name or site_name in vetoed_sites:
                continue
            weights[site_name] = weights.get(site_name, 0.0) + per_step_weight
    return weights


def summarize_anchor(anchor: dict[str, Any] | None, max_sites: int = 5) -> str:
    """One line naming the strongest anchor sites, for the brokerage log"""
    weights = (anchor or {}).get("weights") or {}
    if not weights:
        return "no workflow anchor"
    ranked = sorted(weights.items(), key=lambda item: item[1], reverse=True)[:max_sites]
    return ", ".join(f"{site}={weight:g}" for site, weight in ranked)
