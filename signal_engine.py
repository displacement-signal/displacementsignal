"""
=============================================================
  DISPLACEMENT SIGNAL ENGINE v2
  Data sources:
    1. LinkedIn job postings (RSS)
    2. Indeed job postings (RSS fallback)
    3. TechCrunch AI section (RSS)
    4. ProductHunt AI category (RSS)
    5. Hacker News "Who is Hiring" (monthly thread)
    6. Y Combinator batch announcements (RSS)
    7. Google News — "AI replaces" signals (RSS)
    8. Google News — "AI layoffs" signals (RSS)
=============================================================
"""

import os
import json
import anthropic
import requests
from datetime import date, timedelta
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

CLAUDE_API_KEY   = os.environ.get("CLAUDE_API_KEY", "")
SENDGRID_API_KEY = os.environ.get("SENDGRID_API_KEY", "")
REPORT_FROM      = os.environ.get("REPORT_FROM_EMAIL", "ai.forecast@outlook.com")
ADZUNA_APP_ID    = os.environ.get("ADZUNA_APP_ID", "")
ADZUNA_APP_KEY   = os.environ.get("ADZUNA_APP_KEY", "")

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ─────────────────────────────────────────────
# ROLES TO MONITOR
# ─────────────────────────────────────────────

MONITORED_ROLES = [
    {"role": "copywriter",           "category": "Content & Writing"},
    {"role": "content writer",       "category": "Content & Writing"},
    {"role": "technical writer",     "category": "Content & Writing"},
    {"role": "SEO specialist",       "category": "Content & Writing"},
    {"role": "data entry",           "category": "Data & Admin"},
    {"role": "data analyst",         "category": "Data & Analysis"},
    {"role": "business analyst",     "category": "Data & Analysis"},
    {"role": "financial analyst",    "category": "Finance"},
    {"role": "paralegal",            "category": "Legal"},
    {"role": "legal researcher",     "category": "Legal"},
    {"role": "compliance analyst",   "category": "Legal & Compliance"},
    {"role": "customer service",     "category": "Customer Service"},
    {"role": "customer support",     "category": "Customer Service"},
    {"role": "call center",          "category": "Customer Service"},
    {"role": "graphic designer",     "category": "Creative"},
    {"role": "illustrator",          "category": "Creative"},
    {"role": "video editor",         "category": "Creative"},
    {"role": "QA engineer",          "category": "Software"},
    {"role": "junior developer",     "category": "Software"},
    {"role": "bookkeeper",           "category": "Finance"},
    {"role": "accounts payable",     "category": "Finance"},
    {"role": "medical coder",        "category": "Healthcare Admin"},
    {"role": "medical transcriber",  "category": "Healthcare Admin"},
    {"role": "insurance adjuster",   "category": "Insurance"},
    {"role": "loan processor",       "category": "Finance"},
    {"role": "claims processor",     "category": "Insurance"},
    {"role": "radiologist",          "category": "Healthcare"},
    {"role": "translator",           "category": "Language"},
    {"role": "transcriptionist",     "category": "Admin"},
    {"role": "recruiter",            "category": "HR"},
]

# ─────────────────────────────────────────────
# HELPER: safe RSS fetch
# ─────────────────────────────────────────────

def fetch_rss(url, source_name, max_items=20):
    """
    Fetches and parses an RSS feed.
    Returns a list of dicts with title, description, link, date.
    Returns empty list on any failure.
    """
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code != 200:
            print(f"    {source_name}: status {response.status_code}")
            return []

        soup  = BeautifulSoup(response.text, "xml")
        items = soup.find_all("item")[:max_items]

        results = []
        for item in items:
            title       = item.find("title")
            description = item.find("description") or item.find("summary")
            link        = item.find("link")
            pub_date    = item.find("pubDate") or item.find("published")

            results.append({
                "source":      source_name,
                "title":       title.text.strip()       if title       else "",
                "description": description.text.strip() if description else "",
                "link":        link.text.strip()         if link        else "",
                "date":        pub_date.text.strip()     if pub_date    else "",
            })

        return results

    except Exception as e:
        print(f"    {source_name} error: {e}")
        return []


# ─────────────────────────────────────────────
# STREAM 1: JOB POSTINGS
# Tries LinkedIn first, falls back to Indeed,
# then Adzuna (free job API)
# ─────────────────────────────────────────────

def fetch_job_postings(role):
    """
    Tries multiple job sources for a given role.
    Returns count and sample titles.
    """
    query = role.replace(" ", "+")

    # Source A: Adzuna API (authenticated)
    try:
        url = (
            f"https://api.adzuna.com/v1/api/jobs/us/search/1"
            f"?app_id={ADZUNA_APP_ID}"
            f"&app_key={ADZUNA_APP_KEY}"
            f"&what={query}"
            f"&results_per_page=10"
            f"&content-type=application/json"
        )
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            data   = response.json()
            count  = data.get("count", 0)
            items  = data.get("results", [])
            titles = [i.get("title", "") for i in items[:5]]
            if count > 0:
                return {"role": role, "count": count, "titles": titles, "source": "Adzuna"}
        else:
            print(f"    Adzuna API: status {response.status_code}")
    except Exception as e:
        print(f"    Adzuna error: {e}")

    # Source B: Indeed RSS
    try:
        url      = f"https://www.indeed.com/rss?q={query}&l=United+States&sort=date"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            soup   = BeautifulSoup(response.text, "xml")
            items  = soup.find_all("item")
            count  = len(items)
            titles = [i.find("title").text for i in items[:5] if i.find("title")]
            if count > 0:
                return {"role": role, "count": count, "titles": titles, "source": "Indeed"}
    except Exception as e:
        print(f"    Indeed error: {e}")

    # Source C: SimplyHired RSS
    try:
        url      = f"https://www.simplyhired.com/search?q={query}&l=United+States&job-type=fulltime&format=rss"
        response = requests.get(url, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            soup   = BeautifulSoup(response.text, "xml")
            items  = soup.find_all("item")
            count  = len(items)
            titles = [i.find("title").text for i in items[:5] if i.find("title")]
            if count > 0:
                return {"role": role, "count": count, "titles": titles, "source": "SimplyHired"}
    except Exception as e:
        print(f"    SimplyHired error: {e}")

    return {"role": role, "count": 0, "titles": [], "source": "unavailable"}


def fetch_all_job_data():
    print("Fetching job posting data...")
    results = []
    for item in MONITORED_ROLES:
        role = item["role"]
        data = fetch_job_postings(role)
        data["category"] = item["category"]
        results.append(data)
        status = f"{data['count']} postings ({data['source']})" if data["count"] > 0 else "unavailable"
        print(f"  {role}: {status}")
    print(f"  Done — {len(results)} roles checked")
    return results


# ─────────────────────────────────────────────
# STREAM 2: AI PRODUCT RELEASES
# TechCrunch + ProductHunt
# ─────────────────────────────────────────────

def fetch_ai_releases():
    print("\nFetching AI product releases...")
    releases = []

    # TechCrunch AI
    tc = fetch_rss(
        "https://techcrunch.com/category/artificial-intelligence/feed/",
        "TechCrunch"
    )
    releases.extend(tc)
    print(f"  TechCrunch: {len(tc)} articles")

    # ProductHunt AI
    ph = fetch_rss(
        "https://www.producthunt.com/feed?category=artificial-intelligence",
        "ProductHunt"
    )
    releases.extend(ph)
    print(f"  ProductHunt: {len(ph)} launches")

    # VentureBeat AI
    vb = fetch_rss(
        "https://venturebeat.com/category/ai/feed/",
        "VentureBeat"
    )
    releases.extend(vb)
    print(f"  VentureBeat: {len(vb)} articles")

    # The Verge AI
    tv = fetch_rss(
        "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml",
        "The Verge"
    )
    releases.extend(tv)
    print(f"  The Verge: {len(tv)} articles")

    print(f"  Total AI releases: {len(releases)}")
    return releases


# ─────────────────────────────────────────────
# STREAM 3: HACKER NEWS "WHO IS HIRING"
# Monthly thread — strong leading indicator
# for which tech roles are actually in demand
# ─────────────────────────────────────────────

def fetch_hacker_news_hiring():
    print("\nFetching Hacker News hiring signals...")

    try:
        # Search HN for the monthly "Who is Hiring" thread
        url      = "https://hn.algolia.com/api/v1/search?query=Ask+HN+Who+is+hiring&tags=story&hitsPerPage=3"
        response = requests.get(url, headers=HEADERS, timeout=15)

        if response.status_code != 200:
            print(f"  HN search: status {response.status_code}")
            return []

        data    = response.json()
        hits    = data.get("hits", [])
        results = []

        for hit in hits[:1]:  # Just the most recent thread
            object_id = hit.get("objectID")
            title     = hit.get("title", "")

            # Get the top comments from this thread
            comments_url = f"https://hn.algolia.com/api/v1/search?tags=comment,story_{object_id}&hitsPerPage=30"
            cr           = requests.get(comments_url, headers=HEADERS, timeout=15)

            if cr.status_code == 200:
                comments = cr.json().get("hits", [])
                for c in comments[:20]:
                    text = c.get("comment_text", "")
                    if text and len(text) > 50:
                        results.append({
                            "source":      "HackerNews",
                            "title":       title,
                            "description": text[:300],
                            "link":        f"https://news.ycombinator.com/item?id={object_id}",
                            "date":        hit.get("created_at", ""),
                        })

        print(f"  Hacker News: {len(results)} hiring signals")
        return results

    except Exception as e:
        print(f"  Hacker News error: {e}")
        return []


# ─────────────────────────────────────────────
# STREAM 4: GOOGLE NEWS — DISPLACEMENT SIGNALS
# Searches for real-world AI replacement events
# ─────────────────────────────────────────────

def fetch_google_news_signals():
    print("\nFetching real-world displacement news...")
    signals = []

    queries = [
        ("AI replaces workers",     "AI+replaces+workers"),
        ("AI layoffs 2026",         "AI+layoffs+2026"),
        ("replaced by AI",          "replaced+by+AI"),
        ("AI automation jobs",      "AI+automation+jobs"),
        ("workforce AI reduction",  "workforce+AI+reduction"),
    ]

    for label, query in queries:
        url   = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        items = fetch_rss(url, f"Google News: {label}", max_items=5)
        signals.extend(items)
        print(f"  '{label}': {len(items)} articles")

    print(f"  Total displacement news: {len(signals)}")
    return signals


# ─────────────────────────────────────────────
# STREAM 5: VC FUNDING IN AI AUTOMATION
# Google News RSS for funding announcements
# ─────────────────────────────────────────────

def fetch_vc_signals():
    print("\nFetching VC funding signals...")
    signals = []

    queries = [
        ("AI automation funding",   "AI+automation+funding+million"),
        ("AI startup raises",       "AI+startup+raises+series"),
        ("enterprise AI investment","enterprise+AI+investment+2026"),
    ]

    for label, query in queries:
        url   = f"https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"
        items = fetch_rss(url, f"VC: {label}", max_items=5)
        signals.extend(items)
        print(f"  '{label}': {len(items)} articles")

    print(f"  Total VC signals: {len(signals)}")
    return signals


# ─────────────────────────────────────────────
# FORMAT ALL DATA FOR CLAUDE
# ─────────────────────────────────────────────

def format_job_data(job_results):
    lines = ["ROLE POSTING VOLUMES:"]
    for r in job_results:
        if r["count"] > 0:
            lines.append(f"  {r['role']} ({r['category']}): {r['count']} postings via {r['source']}")
        else:
            lines.append(f"  {r['role']} ({r['category']}): no data retrieved")
    return "\n".join(lines)


def format_news_items(items, label, max_items=15):
    if not items:
        return f"{label}: No data retrieved."
    lines = [f"{label} ({len(items)} items):"]
    for i in items[:max_items]:
        desc = i["description"][:150].strip() if i["description"] else ""
        lines.append(f"  [{i['source']}] {i['title']}\n    {desc}")
    return "\n".join(lines)


# ─────────────────────────────────────────────
# CLAUDE: GENERATE INTELLIGENCE REPORT
# ─────────────────────────────────────────────

def generate_intelligence_report(job_data, ai_releases, hn_signals,
                                  news_signals, vc_signals):
    print("\nGenerating intelligence report with Claude...")

    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    today  = date.today().strftime("%B %d, %Y")

    prompt = f"""
You are the lead analyst for Displacement Signal — a weekly intelligence
service that tracks AI-driven workforce displacement. Your subscribers
are knowledge workers who pay for early warning and specific guidance.

Today's date: {today}

You have five data streams:

{format_job_data(job_data)}

{format_news_items(ai_releases, "AI PRODUCT RELEASES & NEWS")}

{format_news_items(hn_signals, "HACKER NEWS HIRING SIGNALS")}

{format_news_items(news_signals, "REAL-WORLD DISPLACEMENT NEWS")}

{format_news_items(vc_signals, "VC FUNDING IN AI AUTOMATION")}

PRODUCE THE FOLLOWING REPORT:

---
## DISPLACEMENT SIGNAL — Weekly Intelligence Report
### {today}
---

## THIS WEEK'S TOP SIGNALS

The 4-5 most significant displacement signals this week.
For each:
- Role or category affected
- What the combined data shows
- Displacement probability: Low / Medium / High / Critical
- Estimated timeline to significant impact
- One specific action someone in this role should take NOW

---

## ACCELERATING ROLES
Roles where signals are strengthening. 2-3 sentences each, specific.

---

## STABLE ROLES
Roles where AI augments rather than replaces. 2-3 roles max, brief.

---

## MONEY MOVING IN
Key VC investments this week that signal where automation is heading next.
Follow the money — it predicts displacement 12-18 months out.

---

## THE BIGGER PICTURE
4-5 sentences on the macro trend. What does this week's combination
of signals tell us about the next 12 months?

---

## WHAT TO DO THIS WEEK
3 specific, actionable steps for knowledge workers.
Not generic advice — specific to what happened THIS week.

---

Rules:
- Every claim must trace to the data provided
- No vague statements — specific roles, tools, timelines
- Write for someone whose livelihood depends on this information
- Tone: authoritative, urgent where warranted, never alarmist
"""

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=3000,
        messages=[{"role": "user", "content": prompt}]
    )

    return response.content[0].text


# ─────────────────────────────────────────────
# EMAIL DELIVERY VIA SENDGRID
# ─────────────────────────────────────────────

def send_report(report_text, subscribers):
    if not SENDGRID_API_KEY:
        print("\nNo SendGrid key — skipping email delivery")
        return

    print(f"\nSending to {len(subscribers)} subscribers...")

    url     = "https://api.sendgrid.com/v3/mail/send"
    headers = {
        "Authorization": f"Bearer {SENDGRID_API_KEY}",
        "Content-Type":  "application/json"
    }

    subject = f"Displacement Signal — {date.today().strftime('%B %d, %Y')}"

    for email in subscribers:
        payload = {
            "personalizations": [{"to": [{"email": email}]}],
            "from":    {"email": REPORT_FROM, "name": "Displacement Signal"},
            "subject": subject,
            "content": [{"type": "text/plain", "value": report_text}]
        }
        r = requests.post(url, headers=headers, json=payload)
        status = "sent" if r.status_code in [200, 202] else f"failed ({r.status_code})"
        print(f"  {email}: {status}")


# ─────────────────────────────────────────────
# SUBSCRIBER LIST
# ─────────────────────────────────────────────

def load_subscribers():
    try:
        with open("subscribers.json") as f:
            return json.load(f).get("subscribers", [])
    except FileNotFoundError:
        return []


def save_report(report_text):
    today    = date.today().strftime("%Y-%m-%d")
    filename = f"report_{today}.txt"
    with open(filename, "w") as f:
        f.write(report_text)
    print(f"\nReport saved: {filename}")
    return filename


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

if __name__ == "__main__":

    today = date.today().strftime("%Y-%m-%d")
    print(f"\n{'='*60}")
    print(f"  DISPLACEMENT SIGNAL ENGINE v2 — {today}")
    print(f"{'='*60}\n")

    # Fetch all data streams
    job_data    = fetch_all_job_data()
    ai_releases = fetch_ai_releases()
    hn_signals  = fetch_hacker_news_hiring()
    news_signals = fetch_google_news_signals()
    vc_signals  = fetch_vc_signals()

    # Generate report
    report = generate_intelligence_report(
        job_data, ai_releases, hn_signals,
        news_signals, vc_signals
    )

    # Save
    save_report(report)

    # Send to subscribers
    subscribers = load_subscribers()
    if subscribers:
        send_report(report, subscribers)
    else:
        print("\n--- REPORT PREVIEW ---")
        print(report)

    print(f"\n{'='*60}")
    print(f"  ENGINE COMPLETE")
    print(f"{'='*60}")
