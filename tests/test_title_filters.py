"""Job-title qualification rules used to keep or drop people after a waterfall search.

The rules are tested in detail against bigcommerce_pipeline.py (the simplest
template). The same rules are copied into other template pipelines, so a
final test checks every copy reaches the same verdicts; if one drifts, the
failure names the file.
"""
from __future__ import annotations

import pytest

from helpers import load_script

COPIES = [
    "woocommerce_pipeline",
    "contentful_pipeline",
    "tier_a_ae_pipeline",
    "tier_a_so_pipeline",
    "tier_a_tef_pipeline",
    "marketplace_pipeline",
]

MARKETING = ["VP Marketing", "Head of SEO", "Director, Brand & Communications",
             "Senior Manager, E-commerce", "Chief Product Officer", "Head of PR"]
FOUNDER = ["Co-Founder & CEO", "Owner", "Managing Director", "President", "Principal"]
DISQUALIFIED = ["Marketing Coordinator", "Executive Assistant to the CEO",
                "Associate Brand Manager", "Junior SEO Specialist",
                "Part-time Content Writer", "Chief of Staff to the President"]
UNRELATED = ["Software Engineer", "Head of Finance", "Operations Lead", "Procurement Lead"]
ALL_TITLES = MARKETING + FOUNDER + DISQUALIFIED + UNRELATED
NAMES = ["Ann Able", "  Mary Jo  van Example ", "Morgana", "   "]


@pytest.fixture(scope="module")
def bc():
    return load_script("bigcommerce_pipeline")


@pytest.mark.parametrize("title", MARKETING)
def test_marketing_titles_qualify(bc, title):
    assert bc.is_valid_marketing_title(title)
    assert bc.is_valid_title(title)


@pytest.mark.parametrize("title", FOUNDER)
def test_founder_titles_qualify(bc, title):
    assert bc.is_valid_founder_title(title)
    assert bc.is_valid_title(title)


@pytest.mark.parametrize("title", DISQUALIFIED)
def test_support_and_junior_titles_are_disqualified(bc, title):
    assert bc.is_disqualified(title)
    assert not bc.is_valid_title(title)


@pytest.mark.parametrize("title", UNRELATED)
def test_unrelated_titles_do_not_qualify(bc, title):
    # "Procurement Lead" checks keywords match whole words: "pr" must not fire inside it.
    assert not bc.is_valid_title(title)


def test_split_name(bc):
    assert [bc.split_name(n) for n in NAMES] == [
        ("Ann", "Able"), ("Mary", "Jo van Example"), ("Morgana", ""), ("", ""),
    ]


@pytest.mark.parametrize("module", COPIES)
def test_template_copies_reach_identical_verdicts(bc, module):
    copy = load_script(module)

    def verdicts(m):
        return ([(m.is_disqualified(t), m.is_valid_marketing_title(t),
                  m.is_valid_founder_title(t)) for t in ALL_TITLES],
                [m.split_name(n) for n in NAMES])

    assert verdicts(copy) == verdicts(bc)
