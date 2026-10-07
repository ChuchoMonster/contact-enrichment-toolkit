"""PipelineRunner (blitz_core.py): the 3-step company -> people -> email flow.

Uses an in-memory fake client so the orchestration logic (paging, caps,
skipping, cancellation, progress events) is tested without HTTP.
"""
from __future__ import annotations

from blitz_core import PipelineRunner


def li(slug: str) -> str:
    return f"https://www.linkedin.com/company/{slug}"


class FakeClient:
    def __init__(self, pages=None, people=None, emails=None):
        self.pages = pages or []          # list of (results, next_cursor)
        self.people = people or {}        # company linkedin url -> list of raw people
        self.emails = emails or {}        # person linkedin url -> email
        self.search_calls = []
        self.waterfall_calls = []
        self.email_calls = []

    def search_companies(self, payload, cursor=None):
        self.search_calls.append(cursor)
        idx = len(self.search_calls) - 1
        return self.pages[idx] if idx < len(self.pages) else ([], None)

    def waterfall_icp(self, url, cascade, max_results=1):
        self.waterfall_calls.append((url, cascade, max_results))
        return self.people.get(url, [])

    def enrich_email(self, person_url):
        self.email_calls.append(person_url)
        return self.emails.get(person_url)


def company(slug, linkedin=True):
    co = {"name": slug.title(), "website": f"{slug}.example.com", "industry": "Software"}
    if linkedin:
        co["linkedin_url"] = li(slug)
    return co


def person(slug, name):
    return {"full_name": name, "job_title": "CEO",
            "person_linkedin_url": f"https://www.linkedin.com/in/{slug}"}


def run(runner, include=("CEO",), exclude=("Intern",)):
    return runner.run({"company": {}}, list(include), list(exclude))


def test_full_run_joins_company_person_and_email():
    client = FakeClient(
        pages=[([company("acme"), company("globex")], None)],
        people={li("acme"): [person("ann", "Ann Able")], li("globex"): [person("bo", "Bo Baker")]},
        emails={"https://www.linkedin.com/in/ann": "ann@acme.example.com"},
    )
    events = []
    results = run(PipelineRunner(client, on_progress=events.append))

    assert [r["full_name"] for r in results] == ["Ann Able", "Bo Baker"]
    assert results[0]["email"] == "ann@acme.example.com"
    assert results[0]["company_name"] == "Acme"
    assert results[0]["company_linkedin_url"] == li("acme")
    assert results[1]["email"] == ""
    assert events[-1] == {"type": "complete", "total_companies": 2,
                          "total_people": 2, "total_emails": 1}


def test_cascade_is_built_from_requested_titles():
    client = FakeClient(pages=[([company("acme")], None)],
                        people={li("acme"): [person("ann", "Ann Able")]})
    run(PipelineRunner(client), include=["VP Sales"], exclude=["Intern"])

    url, cascade, max_results = client.waterfall_calls[0]
    assert cascade == [{"include_title": ["VP Sales"], "exclude_title": ["Intern"],
                        "location": ["WORLD"], "include_headline_search": False}]
    assert max_results == 1


def test_company_search_follows_cursors_until_exhausted():
    client = FakeClient(pages=[([company("a")], "c2"), ([company("b")], "c3"), ([company("c")], None)])
    companies = PipelineRunner(client)._search_companies({})

    assert [c["company_name"] for c in companies] == ["A", "B", "C"]
    assert client.search_calls == [None, "c2", "c3"]
    assert "_raw" in companies[0]


def test_company_search_stops_at_twice_max_leads():
    # 2x oversampling: enough companies to expect max_leads verified emails.
    pages = [([company(f"co{p}{i}") for i in range(2)], f"c{p + 1}") for p in range(10)]
    client = FakeClient(pages=pages)

    companies = PipelineRunner(client, max_leads=3)._search_companies({})

    assert len(companies) == 6
    assert len(client.search_calls) == 3


def test_companies_without_linkedin_url_are_skipped():
    client = FakeClient(pages=[([company("nolink", linkedin=False), company("acme")], None)],
                        people={li("acme"): [person("ann", "Ann Able")]})
    results = run(PipelineRunner(client))

    assert [c[0] for c in client.waterfall_calls] == [li("acme")]
    assert len(results) == 1


def test_enrichment_stops_once_max_leads_verified():
    companies = [company(f"co{i}") for i in range(5)]
    people = {li(f"co{i}"): [person(f"p{i}", f"Person {i}")] for i in range(5)}
    emails = {f"https://www.linkedin.com/in/p{i}": f"p{i}@example.com" for i in range(5)}
    client = FakeClient(pages=[(companies, None)], people=people, emails=emails)

    results = run(PipelineRunner(client, max_leads=2))

    assert len([r for r in results if r["email"]]) == 2
    assert len(client.email_calls) == 2


def test_no_companies_emits_error_and_returns_empty():
    events = []
    assert run(PipelineRunner(FakeClient(), on_progress=events.append)) == []
    assert events[-1] == {"type": "error", "message": "No companies found matching your criteria."}


def test_no_people_emits_error_and_skips_enrichment():
    client = FakeClient(pages=[([company("acme")], None)])
    events = []

    assert run(PipelineRunner(client, on_progress=events.append)) == []
    assert events[-1]["type"] == "error"
    assert client.email_calls == []


def test_cancel_stops_people_search():
    client = FakeClient(pages=[([company("a"), company("b"), company("c")], None)],
                        people={li(s): [person(s, s)] for s in "abc"})
    runner = PipelineRunner(client)

    def on_progress(event):
        if event["type"] == "person":
            runner.cancel()

    runner.on_progress = on_progress
    people = runner._find_people(runner._search_companies({}),
                                 [{"include_title": ["CEO"]}])

    assert list(people) == [li("a")]
