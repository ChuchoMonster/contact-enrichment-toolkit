"""Response field normalisation in blitz_core.py.

The API returns the same fact under different keys depending on endpoint;
these functions collapse them into one shape.
"""
from __future__ import annotations

from blitz_core import extract_company_fields, extract_company_linkedin, extract_person_fields


def test_company_linkedin_skips_keys_that_are_not_linkedin_urls():
    company = {
        "linkedin_url": "",
        "company_linkedin_url": "https://acme.example.com",
        "linkedin": "https://www.linkedin.com/company/acme",
    }
    assert extract_company_linkedin(company) == "https://www.linkedin.com/company/acme"


def test_company_linkedin_empty_when_absent():
    assert extract_company_linkedin({"website": "https://acme.example.com"}) == ""


def test_company_fields_read_alternate_keys_and_join_industry_list():
    fields = extract_company_fields({
        "company_name": "Acme Widgets",
        "domain": "acme.example.com",
        "industries": ["Software", "Retail"],
        "employees_on_linkedin": 42,
        "city": "Springfield",
        "country": "US",
        "url": "https://www.linkedin.com/company/acme-widgets",
    })
    assert fields == {
        "company_name": "Acme Widgets",
        "website": "acme.example.com",
        "industry": "Software, Retail",
        "employee_count": "42",
        "hq_city": "Springfield",
        "hq_country": "US",
        "company_linkedin_url": "https://www.linkedin.com/company/acme-widgets",
    }


def test_company_fields_fall_back_to_nested_hq_object():
    fields = extract_company_fields({
        "name": "Globex",
        "hq": {"city": "Shelbyville", "country_name": "Canada", "country": "CA"},
    })
    assert fields["hq_city"] == "Shelbyville"
    assert fields["hq_country"] == "Canada"


def test_company_fields_use_location_only_when_city_missing():
    assert extract_company_fields({"location": "Ogdenville"})["hq_city"] == "Ogdenville"
    assert extract_company_fields({"city": "Capital City", "location": "Elsewhere"})["hq_city"] \
        == "Capital City"


def test_company_fields_missing_everything_yields_blank_strings():
    fields = extract_company_fields({})
    assert set(fields.values()) == {""}


def test_person_fields_split_full_name_and_pick_aliases():
    person = extract_person_fields({
        "full_name": "  Jordan Avery Example ",
        "title": "Head of Growth",
        "linkedin_profile_url": "https://www.linkedin.com/in/jordan-example",
    })
    assert person == {
        "full_name": "Jordan Avery Example",
        "first_name": "Jordan",
        "last_name": "Avery Example",
        "title": "Head of Growth",
        "person_linkedin_url": "https://www.linkedin.com/in/jordan-example",
    }


def test_person_fields_prefer_explicit_names_and_primary_url_key():
    person = extract_person_fields({
        "full_name": "Sam Q. Sample",
        "first_name": "Samantha",
        "last_name": "Sample",
        "job_title": "CMO",
        "linkedin_headline": "ignored",
        "person_linkedin_url": "https://www.linkedin.com/in/primary",
        "linkedin_url": "https://www.linkedin.com/in/secondary",
    })
    assert (person["first_name"], person["last_name"]) == ("Samantha", "Sample")
    assert person["title"] == "CMO"
    assert person["person_linkedin_url"] == "https://www.linkedin.com/in/primary"


def test_person_fields_single_word_name_has_no_last_name():
    person = extract_person_fields({"full_name": "Morgana", "linkedin_headline": "Founder"})
    assert (person["first_name"], person["last_name"], person["title"]) == ("Morgana", "", "Founder")
