"""FastAPI application for Blitz Contact Finder."""
from __future__ import annotations

import csv
import io
import json
import logging
import os
import subprocess
import threading

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from blitz_core import BlitzAPIClient, PipelineRunner
from web.database import count_jobs_for_domain, create_job, init_db, update_job

load_dotenv()

log = logging.getLogger("blitz_web")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(message)s")

app = FastAPI(title="Blitz Contact Finder")

static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

GWS_CLI = os.environ.get("GWS_CLI_PATH", os.path.expanduser("~/.npm-global/bin/gws"))
# Results are sent from whichever Google account the gws CLI is authenticated as.
BCC_EMAIL = os.environ.get("RESULTS_BCC_EMAIL", "")        # optional internal copy
SUPPORT_EMAIL = os.environ.get("SUPPORT_EMAIL", "")        # shown when a domain hits its limit
BOOKING_URL = os.environ.get("BOOKING_URL", "")            # optional meeting link in results email
MAX_LEADS = 1000
DOMAIN_JOB_LIMIT = 3


@app.on_event("startup")
async def startup():
    init_db()


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(static_dir, "index.html"), encoding="utf-8") as f:
        return HTMLResponse(f.read())


@app.get("/thank-you", response_class=HTMLResponse)
async def thank_you():
    with open(os.path.join(static_dir, "thank-you.html"), encoding="utf-8") as f:
        return HTMLResponse(f.read())


# ---------------------------------------------------------------------------
# Job submission
# ---------------------------------------------------------------------------

@app.post("/api/jobs")
async def create_job_route(request: Request):
    body = await request.json()

    # Validate contact info
    errors = []
    first_name = (body.get("first_name") or "").strip()
    last_name = (body.get("last_name") or "").strip()
    company_name = (body.get("company_name") or "").strip()
    email = (body.get("email") or "").strip()
    if not first_name:
        errors.append("First name is required.")
    if not last_name:
        errors.append("Last name is required.")
    if not company_name:
        errors.append("Company name is required.")
    if not email or "@" not in email:
        errors.append("A valid business email is required.")

    # Validate search criteria
    industries = body.get("industry_include", [])
    if not industries:
        errors.append("Select at least one industry.")
    employee_ranges = body.get("employee_range", [])
    if not employee_ranges:
        errors.append("Select at least one company size range.")
    country = body.get("country_code", "")
    if not country:
        errors.append("Select a headquarters country.")
    include_titles = body.get("include_title", [])
    if not include_titles:
        errors.append("Enter at least one job title.")
    if errors:
        return JSONResponse({"errors": errors}, status_code=422)

    # Domain-based abuse limit
    domain = email.split("@", 1)[1].lower() if "@" in email else ""
    if domain and count_jobs_for_domain(domain) >= DOMAIN_JOB_LIMIT:
        return JSONResponse(
            {"errors": [f"Submission limit reached for {domain}. "
                        + (f"Email {SUPPORT_EMAIL} if you need more." if SUPPORT_EMAIL else "")]},
            status_code=429,
        )

    # Build company search payload
    company_payload: dict = {
        "company": {
            "industry": {"include": industries},
            "employee_range": employee_ranges,
            "hq": {"country_code": [country]},
        },
        "max_results": 50,
    }

    # Optional fields
    if body.get("industry_exclude"):
        company_payload["company"]["industry"]["exclude"] = body["industry_exclude"]
    if body.get("city_include"):
        company_payload["company"].setdefault("hq", {})["city"] = {"include": body["city_include"]}
    if body.get("city_exclude"):
        hq = company_payload["company"].setdefault("hq", {})
        city = hq.setdefault("city", {})
        city["exclude"] = body["city_exclude"]
    if body.get("type_include"):
        company_payload["company"]["type"] = {"include": body["type_include"]}
    if body.get("type_exclude"):
        company_payload["company"].setdefault("type", {})["exclude"] = body["type_exclude"]
    if body.get("keywords_include"):
        company_payload["company"]["keywords"] = {"include": body["keywords_include"]}
    if body.get("keywords_exclude"):
        company_payload["company"].setdefault("keywords", {})["exclude"] = body["keywords_exclude"]
    if body.get("founded_year_min") or body.get("founded_year_max"):
        fy = {}
        if body.get("founded_year_min"):
            fy["min"] = int(body["founded_year_min"])
        if body.get("founded_year_max"):
            fy["max"] = int(body["founded_year_max"])
        company_payload["company"]["founded_year"] = fy
    if body.get("min_linkedin_followers"):
        company_payload["company"]["min_linkedin_followers"] = int(body["min_linkedin_followers"])

    exclude_titles = body.get("exclude_title", [])

    # Save job
    contact_info = {
        "first_name": first_name, "last_name": last_name,
        "company_name": company_name, "email": email,
    }
    config = {**body, "_company_payload": company_payload, "_contact": contact_info}
    job_id = create_job(config)

    _notify_slack(
        f":rocket: New lead request from {first_name} {last_name} "
        f"({email}) at {company_name} — job `{job_id}`"
    )

    # Run pipeline in background
    thread = threading.Thread(
        target=_run_pipeline,
        args=(job_id, company_payload, include_titles, exclude_titles, contact_info),
        daemon=True,
    )
    thread.start()

    return JSONResponse({"ok": True, "job_id": job_id})


def _run_pipeline(job_id: str, company_payload: dict,
                  include_titles: list, exclude_titles: list,
                  contact_info: dict):
    try:
        api_key = os.getenv("BLITZ_API_KEY")
        client = BlitzAPIClient(api_key)
        runner = PipelineRunner(client, max_leads=MAX_LEADS)
        results = runner.run(company_payload, include_titles, exclude_titles)

        update_job(job_id, "complete", results)
        log.info(f"Job {job_id}: pipeline complete — {len(results)} results")

        emails_found = sum(1 for r in results if r.get("email"))
        if results:
            _email_results(contact_info, results, job_id)
            _notify_slack(
                f":white_check_mark: Job `{job_id}` complete — "
                f"{emails_found} verified emails sent to {contact_info['email']}"
            )
        else:
            log.warning(f"Job {job_id}: no results to email")
            _notify_slack(
                f":warning: Job `{job_id}` finished with 0 results "
                f"({contact_info['email']})"
            )

    except Exception as e:
        log.exception(f"Pipeline error for job {job_id}")
        update_job(job_id, "error")
        _notify_slack(
            f":x: Job `{job_id}` failed for {contact_info['email']}: {e}"
        )


def _notify_slack(text: str):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        return
    try:
        requests.post(url, json={"text": text}, timeout=5)
    except Exception:
        log.exception("Slack notification failed")


def _build_csv(results: list[dict]) -> str:
    columns = ["company_name", "website", "industry", "employee_count",
               "full_name", "first_name", "last_name", "title",
               "person_linkedin_url", "company_linkedin_url", "email"]
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns, extrasaction="ignore")
    writer.writeheader()
    for row in results:
        if row.get("email"):
            writer.writerow(row)
    return output.getvalue()


def _email_results(contact_info: dict, results: list[dict], job_id: str):
    import email.encoders
    import email.mime.base
    import email.mime.multipart
    import email.mime.text
    import tempfile

    csv_content = _build_csv(results)
    to_email = contact_info["email"]
    first_name = contact_info["first_name"]
    total = len(results)
    emails_found = sum(1 for r in results if r.get("email"))

    subject = f"Your ColdStart Results — {emails_found} verified emails"
    booking_html = (
        f'              <span style="color:#8b9cf7;">&#8594;</span>&nbsp; Book 20 minutes: '
        f'<a href="{BOOKING_URL}" style="color:#8b9cf7; text-decoration:none; font-weight:500;">Book a call</a><br>\n'
        if BOOKING_URL else ""
    )
    booking_text = f"-> Book 20 minutes: {BOOKING_URL}\n" if BOOKING_URL else ""

    html_body = f"""\
<!DOCTYPE html>
<html lang="en" xmlns="http://www.w3.org/1999/xhtml" xmlns:v="urn:schemas-microsoft-com:vml" xmlns:o="urn:schemas-microsoft-com:office:office">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="X-UA-Compatible" content="IE=edge">
  <title>Your Leads Are Ready</title>
  <!--[if mso]>
  <noscript>
    <xml>
      <o:OfficeDocumentSettings>
        <o:PixelsPerInch>96</o:PixelsPerInch>
      </o:OfficeDocumentSettings>
    </xml>
  </noscript>
  <![endif]-->
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&display=swap');
    body, table, td, a {{ -webkit-text-size-adjust: 100%; -ms-text-size-adjust: 100%; }}
    table, td {{ mso-table-lspace: 0pt; mso-table-rspace: 0pt; }}
    img {{ -ms-interpolation-mode: bicubic; border: 0; height: auto; line-height: 100%; outline: none; text-decoration: none; }}
    body {{ margin: 0; padding: 0; width: 100% !important; background-color: #050508; }}
  </style>
</head>
<body style="margin:0; padding:0; background-color:#050508; font-family:'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;">
  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color:#050508;">
    <tr>
      <td align="center" style="padding: 40px 20px;">
        <table role="presentation" width="560" cellpadding="0" cellspacing="0" border="0" style="max-width:560px; width:100%;">
          <tr>
            <td style="padding-bottom: 32px;">
              <span style="font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:18px; font-weight:600; color:#e8e8ec; letter-spacing:-0.01em;">Cold<span style="color:#8b9cf7;">Start</span></span>
            </td>
          </tr>
          <tr>
            <td style="padding-bottom: 24px;">
              <table role="presentation" cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td style="background-color: rgba(139,156,247,0.1); border: 1px solid rgba(139,156,247,0.25); border-radius: 100px; padding: 5px 14px;">
                    <table role="presentation" cellpadding="0" cellspacing="0" border="0">
                      <tr>
                        <td style="width:6px; height:6px; border-radius:50%; background-color:#8b9cf7;" width="6" height="6"></td>
                        <td style="padding-left:8px; font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:11px; font-weight:500; letter-spacing:0.12em; text-transform:uppercase; color:#8b9cf7;">Leads attached</td>
                      </tr>
                    </table>
                  </td>
                </tr>
              </table>
            </td>
          </tr>
          <tr>
            <td style="font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:15px; font-weight:300; line-height:1.75; color:rgba(232,232,236,0.85); padding-bottom:8px;">
              Hey {first_name},
            </td>
          </tr>
          <tr>
            <td style="font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:15px; font-weight:300; line-height:1.75; color:rgba(232,232,236,0.85); padding-bottom:24px;">
              Your CSV is attached: <strong style="color:#e8e8ec; font-weight:500;">{emails_found} verified contacts</strong> matched to your criteria, ready to use.
            </td>
          </tr>
          <tr>
            <td style="font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:15px; font-weight:300; line-height:1.75; color:rgba(232,232,236,0.85); padding-bottom:24px;">
              Getting those leads was the easy part. What happens next&#8202;—&#8202;enriching the data, personalizing outreach, following up, routing responses, updating your CRM&#8202;—&#8202;that's where hours disappear.
            </td>
          </tr>
          <tr>
            <td style="font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:15px; font-weight:300; line-height:1.75; color:rgba(232,232,236,0.85); padding-bottom:24px;">
              That's where ColdStart can help.
            </td>
          </tr>
          <tr>
            <td style="font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:15px; font-weight:300; line-height:1.75; color:rgba(232,232,236,0.85); padding-bottom:32px;">
              We automate GTM processes so your team can focus on the highest value tasks while reducing operational bottlenecks.
            </td>
          </tr>
          <tr>
            <td style="padding-bottom: 24px;">
              <div style="height:1px; background: linear-gradient(90deg, rgba(139,156,247,0.35), rgba(139,156,247,0.05) 70%, transparent);"></div>
            </td>
          </tr>
          <tr>
            <td style="font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:14px; font-weight:400; line-height:2.2; color:rgba(232,232,236,0.55); padding-bottom:28px;">
              <span style="color:#8b9cf7;">&#8594;</span>&nbsp; See how we work: <a href="https://www.coldstartb2b.com" style="color:#8b9cf7; text-decoration:none; font-weight:500;">coldstartb2b.com</a><br>
{booking_html}              <span style="color:#8b9cf7;">&#8594;</span>&nbsp; Or just hit reply — I read every one.
            </td>
          </tr>
          <tr>
            <td style="font-family:'Inter',-apple-system,BlinkMacSystemFont,sans-serif; font-size:14px; font-weight:400; line-height:1.7; color:rgba(232,232,236,0.55);">
              <span style="color:#e8e8ec; font-weight:500;">John</span><br>
              Founder, ColdStart<br>
              <a href="https://www.coldstartb2b.com" style="color:#8b9cf7; text-decoration:none;">coldstartb2b.com</a>
            </td>
          </tr>
          <tr><td style="height: 40px;"></td></tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""

    # Build MIME message with CSV attachment
    msg = email.mime.multipart.MIMEMultipart("alternative")
    msg["To"] = to_email
    msg["Subject"] = subject

    # Plain text fallback
    plain_text = (
        f"Hey {first_name},\n\n"
        f"Your CSV is attached: {emails_found} verified contacts matched to your criteria, ready to use.\n\n"
        f"Getting those leads was the easy part. What happens next — enriching the data, "
        f"personalizing outreach, following up, routing responses, updating your CRM — that's where hours disappear.\n\n"
        f"That's where ColdStart can help.\n\n"
        f"We automate GTM processes so your team can focus on the highest value tasks while "
        f"reducing operational bottlenecks.\n\n"
        f"-> See how we work: coldstartb2b.com\n"
        f"{booking_text}"
        f"-> Or just hit reply — I read every one.\n\n"
        f"John\nFounder, ColdStart\ncoldstartb2b.com"
    )

    # Use mixed type for attachment support
    msg_mixed = email.mime.multipart.MIMEMultipart("mixed")
    msg_mixed["To"] = to_email
    if BCC_EMAIL:
        msg_mixed["Bcc"] = BCC_EMAIL
    msg_mixed["Subject"] = subject

    msg_alt = email.mime.multipart.MIMEMultipart("alternative")
    msg_alt.attach(email.mime.text.MIMEText(plain_text, "plain"))
    msg_alt.attach(email.mime.text.MIMEText(html_body, "html"))
    msg_mixed.attach(msg_alt)

    csv_attachment = email.mime.base.MIMEBase("text", "csv")
    csv_attachment.set_payload(csv_content.encode("utf-8"))
    email.encoders.encode_base64(csv_attachment)
    csv_attachment.add_header("Content-Disposition", "attachment",
                              filename=f"coldstart_leads_{job_id}.csv")
    msg_mixed.attach(csv_attachment)

    # Write the full RFC822 message to a temp file and pass via --upload
    # (avoids argv length limit when CSV attachment is large).
    # gws requires the upload path to be under cwd, so write into cwd.
    eml_path = None
    try:
        upload_dir = os.getcwd()
        with tempfile.NamedTemporaryFile(
            mode="wb", suffix=".eml", delete=False, dir=upload_dir,
        ) as tmp:
            tmp.write(msg_mixed.as_bytes())
            eml_path = tmp.name

        rel_path = os.path.relpath(eml_path, upload_dir)
        cmd = [
            GWS_CLI, "gmail", "users", "messages", "send",
            "--params", json.dumps({"userId": "me"}),
            "--upload", rel_path,
            "--upload-content-type", "message/rfc822",
        ]
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=120, cwd=upload_dir,
        )
        if result.returncode == 0:
            log.info(f"Job {job_id}: CSV emailed to {to_email}")
        else:
            log.error(f"Job {job_id}: gws send failed (rc={result.returncode}): "
                      f"stdout={result.stdout[:500]} stderr={result.stderr[:500]}")
    except Exception as e:
        log.exception(f"Job {job_id}: email send error")
    finally:
        if eml_path:
            try:
                os.unlink(eml_path)
            except Exception:
                pass
