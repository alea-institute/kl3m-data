"""
Regression tests for kl3m_data.parsers.generic_html.

The eCFR fixture is the eCFR "enhanced" renderer HTML for 21 CFR 178.3610 as of 2024-09-17
(https://www.ecfr.gov/api/renderer/v1/content/enhanced/2024-09-17/title-21?section=178.3610),
the example document of the kl3m-data-ecfr March 2025 regression, where the stored text read
"provisions: ( a ) Hydrogenated ..." instead of "provisions:\n\n(a) Hydrogenated ...".
"""

# imports
import re
from pathlib import Path

# packages
import pytest

# project
from kl3m_data.parsers import generic_html

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "ecfr_2024-09-17_21_178.3610.html"
SPACED_DESIGNATOR = re.compile(r"\( [A-Za-z0-9]{1,4} \)")

NESTED_ECFR_HTML = """<div class="section" id="1.1">
<h4>§ 1.1 Example.</h4>
<div id="p-1.1(a)"><p class="indent-1" data-title="1.1(a)"><span class="paragraph-hierarchy"><span class="paren">(</span>a<span class="paren">)</span></span> <em class="paragraph-heading">General.</em>  First paragraph.</p>
<div id="p-1.1(a)(1)"><p class="indent-2" data-title="1.1(a)(1)"><span class="paragraph-hierarchy"><span class="paren">(</span>1<span class="paren">)</span></span> Nested paragraph.</p>
<div id="p-1.1(a)(1)(iv)"><p class="indent-3" data-title="1.1(a)(1)(iv)"><span class="paragraph-hierarchy"><span class="paren">(</span>iv<span class="paren">)</span></span> Deeper paragraph.</p></div>
</div></div>
<div id="p-1.1(b)"><p class="indent-1" data-title="1.1(b)"><span class="paragraph-hierarchy"><span class="paren">(</span>b<span class="paren">)</span></span> Second paragraph.</p></div>
</div>"""


def _parse_text(html: str) -> str:
    documents = generic_html.parse(
        html.encode("utf-8"), source="test", identifier="test"
    )
    assert len(documents) == 1
    return documents[0].representations["text/markdown"].content


@pytest.fixture(name="ecfr_text")
def fixture_ecfr_text() -> str:
    return _parse_text(FIXTURE_PATH.read_text(encoding="utf-8"))


def test_ecfr_paragraph_designators_unspaced(ecfr_text: str):
    assert SPACED_DESIGNATOR.search(ecfr_text) is None
    assert "(a) Hydrogenated" in ecfr_text
    assert "(b) The polyolefin film" in ecfr_text


def test_ecfr_paragraph_breaks_kept(ecfr_text: str):
    assert "provisions:\n\n(a) Hydrogenated" in ecfr_text
    assert "\n\n(b) The polyolefin film" in ecfr_text
    assert (
        "§ 178.3610 α-Methylstyrene-vinyltoluene resins, hydrogenated.\n\nHydrogenated"
        in ecfr_text
    )


def test_ecfr_content_complete(ecfr_text: str):
    # heading, both paragraphs, and the source citation survive; no link URLs leak in
    for fragment in (
        "Editorial Note",
        "§ 178.3610",
        "does not exceed 0.002 inch",
        "42 FR 14609",
    ):
        assert fragment in ecfr_text
    assert "](" not in ecfr_text


def test_nested_ecfr_paragraphs():
    text = _parse_text(NESTED_ECFR_HTML)
    assert SPACED_DESIGNATOR.search(text) is None
    for designator in ("(a) ", "(1) Nested", "(iv) Deeper", "(b) Second"):
        assert f"\n\n{designator}" in text


def test_content_length_ignores_whitespace_and_link_destinations():
    assert generic_html.content_length("( a ) Text") == generic_html.content_length(
        "(a) Text"
    )
    assert generic_html.content_length(
        "see [§ 1.1(a)](/on/x/section-1.1#p-1.1(a)(1)) ok"
    ) == (generic_html.content_length("see § 1.1(a) ok"))
    assert generic_html.content_length(None) == 0


def test_select_candidate_prefers_regex_over_spaced_lxml():
    candidates = {
        "regex": "provisions:\n\n(a) Hydrogenated resins.\n\n(b) Film.",
        "auto": None,
        "lxml": "provisions: ( a ) Hydrogenated resins.\n( b ) Film.",
    }
    # the lxml text has more whitespace-separated words but the same content
    assert len(candidates["lxml"].split()) > len(candidates["regex"].split())
    assert generic_html.select_candidate(candidates) == "regex"


def test_select_candidate_falls_back_when_content_missing():
    candidates = {
        "regex": "short",
        "auto": "",
        "lxml": "much longer recovered content here",
    }
    assert generic_html.select_candidate(candidates) == "lxml"
    assert (
        generic_html.select_candidate({"regex": None, "auto": "", "lxml": None}) is None
    )
