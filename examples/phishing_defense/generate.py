# Databricks notebook source
# MAGIC %md
# MAGIC # Example: phishing defense — synthetic data (seed + daily)
# MAGIC Inbound-email phishing detection data for the MLOps app. See `README.md` (same folder) for the
# MAGIC objects, the signal, and the app walkthrough.
# MAGIC
# MAGIC **Self-healing + idempotent.** First run (or `rebuild=true`) seeds `seed_days` days ending `through_date`;
# MAGIC every later run generates each missing day from `max(date(event_ts)) + 1` through `through_date`
# MAGIC (default: yesterday UTC, so no row is ever in the future). Each day's randomness is seeded by the date,
# MAGIC and the "world" (recipients, legitimate sender domains) is seeded by a constant, so a re-run reproduces
# MAGIC the same data.
# MAGIC
# MAGIC **Feature View rule:** the app passes the EOL timestamp column to EVERY feature, so every table carries
# MAGIC the same timestamp column, `event_ts`. Lookup tables are point-in-time SNAPSHOTS keyed by
# MAGIC (entity, `event_ts`) — an initial snapshot the day before the first email, then weekly (sender domains) /
# MAGIC monthly (recipients), plus a snapshot whenever a new campaign domain is first seen or gets blocklisted.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_77rg2n_catalog")
dbutils.widgets.text("schema", "phishing_defense_demo")
dbutils.widgets.text("through_date", "")   # YYYY-MM-DD; blank = yesterday (UTC)
dbutils.widgets.text("seed_days", "60")
dbutils.widgets.text("max_days", "120")    # safety cap on a single catch-up run
dbutils.widgets.dropdown("rebuild", "false", ["false", "true"])

import datetime as dt
import difflib

import numpy as np
import pandas as pd
from pyspark.sql import types as T

spark.conf.set("spark.sql.session.timeZone", "UTC")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
_td = dbutils.widgets.get("through_date").strip()
THROUGH = dt.date.fromisoformat(_td) if _td else dt.datetime.now(dt.timezone.utc).date() - dt.timedelta(days=1)
SEED_DAYS = int(dbutils.widgets.get("seed_days"))
MAX_DAYS = int(dbutils.widgets.get("max_days"))
REBUILD = dbutils.widgets.get("rebuild") == "true"

# COMMAND ----------

# MAGIC %md ## The world (deterministic): the organisation, its recipients, legitimate sender domains

# COMMAND ----------

WORLD_SEED = 20261124
WORLD_START = dt.date(2026, 1, 1)   # fixed anchor for registration / hire dates (independent of the seed window)

ORG_DOMAINS = ["halcyon.com", "halcyonbank.com", "halcyon-insure.com"]
ORG_WEIGHTS = [0.70, 0.20, 0.10]
BRANDS = ["microsoft", "docusign", "dropbox", "okta", "zoom", "fedex", "paypal", "adobe"]

# department: (share, base sim-click rate, spear-phishing target weight)
DEPTS = {
    "Engineering": (0.24, 0.06, 0.5), "Sales": (0.18, 0.14, 1.0), "Operations": (0.14, 0.11, 0.8),
    "Finance": (0.10, 0.10, 4.0), "HR": (0.08, 0.13, 2.5), "IT": (0.08, 0.04, 2.0),
    "Legal": (0.06, 0.08, 1.5), "Marketing": (0.09, 0.15, 0.8), "Executive": (0.03, 0.09, 5.0),
}


def build_recipients():
    rng = np.random.default_rng([WORLD_SEED, 1])
    n = 1200
    names = list(DEPTS)
    dept = rng.choice(names, n, p=[DEPTS[d][0] for d in names])
    is_vip = ((dept == "Executive") | (rng.random(n) < 0.01)).astype(int)
    priv = ((dept == "IT") & (rng.random(n) < 0.7) | (dept == "Executive") | (rng.random(n) < 0.03)).astype(int)
    base_click = np.clip([rng.beta(2, 2 / DEPTS[d][1] - 2) for d in dept], 0.0, 0.8)
    return pd.DataFrame({
        "recipient_id": [f"r{i:05d}" for i in range(1, n + 1)],
        "department": dept,
        "org_domain": rng.choice(ORG_DOMAINS, n, p=ORG_WEIGHTS),
        "is_vip": is_vip,
        "has_privileged_access": priv,
        "base_click": base_click,
        "hire_date": [WORLD_START - dt.timedelta(days=int(x)) for x in rng.integers(30, 4500, n)],
        "spear_weight": [DEPTS[d][2] * (3.0 if v else 1.0) for d, v in zip(dept, is_vip)],
    })


SYLL = ["nova", "tera", "blue", "peak", "core", "lumen", "vector", "atlas", "cedar", "harbor", "pixel", "summit",
        "orbit", "forge", "delta", "quill", "maple", "stride", "echo", "prism", "zen", "bolt", "crest", "ridge",
        "spark", "flux", "haven", "north", "river", "stone", "kite", "amber", "birch", "cobalt", "fjord", "jade"]
SUFFIX = ["", "", "hq", "labs", "soft", "cloud", "pay", "mail", "news", "works", "group", "tech", "systems",
          "supply", "logistics", "health", "media", "data"]
TLDS = [".com"] * 6 + [".io", ".net", ".co", ".org"]
# category: (share, links lambda, attachment p, urgency p, reply-to mismatch p, spf/dkim/dmarc pass p)
CATS = {
    "saas": (0.30, 3.0, 0.05, 0.15, 0.04, (0.96, 0.94, 0.90)),      # password resets / "action required"
    "vendor": (0.25, 2.0, 0.35, 0.15, 0.06, (0.93, 0.88, 0.82)),
    "newsletter": (0.20, 9.0, 0.02, 0.20, 0.25, (0.85, 0.78, 0.66)),
    "partner": (0.15, 2.0, 0.25, 0.10, 0.06, (0.94, 0.90, 0.85)),
    "finance": (0.10, 1.5, 0.30, 0.25, 0.04, (0.98, 0.96, 0.94)),
}


def build_legit_domains():
    rng = np.random.default_rng([WORLD_SEED, 2])
    names, seen = [], set()
    while len(names) < 450:
        d = rng.choice(SYLL) + rng.choice(SYLL) + rng.choice(SUFFIX) + rng.choice(TLDS)
        if d not in seen:
            seen.add(d); names.append(d)
    # Org-adjacent legitimate senders: they resemble the org's name, like a lookalike would.
    ORG_ADJACENT = ["halcyon-benefits.com", "halcyonrewards.com", "halcyon-events.com", "mail-halcyonbank.com",
                    "halcyoninsure-claims.com", "halcyon-careers.com", "halcyonbank-statements.com", "my-halcyon.com"]
    names = ORG_ADJACENT + names[: 450 - len(ORG_ADJACENT)]
    n = len(names)
    cats = list(CATS)
    cat = rng.choice(cats, n, p=[CATS[c][0] for c in cats])
    young = rng.random(n) < 0.06   # some legit senders are young too (new SaaS vendors) — blurs domain age
    registered = [WORLD_START + dt.timedelta(days=int(rng.integers(0, 240))) if y
                  else WORLD_START - dt.timedelta(days=int(rng.integers(400, 9000))) for y in young]
    pop = rng.zipf(1.6, n).clip(1, 200).astype(float)
    pop[: len(ORG_ADJACENT)] = rng.uniform(5, 25, len(ORG_ADJACENT))   # regular, not rare
    cat[: len(ORG_ADJACENT)] = rng.choice(["newsletter", "vendor", "partner", "finance"], len(ORG_ADJACENT))
    return pd.DataFrame({
        "sender_domain": names, "category": cat, "registered": registered,
        "registrar_risk": np.round(rng.beta(2, 6, n), 3),
        "has_mx": (rng.random(n) < 0.98).astype(int),
        "popularity": pop,
    })


RECIPIENTS = build_recipients()
LEGIT = build_legit_domains()
LEGIT_BY_NAME = LEGIT.set_index("sender_domain")
print(f"world: {len(RECIPIENTS)} recipients, {len(LEGIT)} legitimate sender domains")

# COMMAND ----------

# MAGIC %md ## Schemas (every table has `event_ts`)

# COMMAND ----------

EMAILS_SCHEMA = T.StructType([
    T.StructField("email_id", T.StringType()), T.StructField("recipient_id", T.StringType()),
    T.StructField("sender_domain", T.StringType()), T.StructField("event_ts", T.TimestampType()),
    T.StructField("subject_has_urgency", T.LongType()), T.StructField("num_links", T.LongType()),
    T.StructField("num_attachments", T.LongType()), T.StructField("has_html_form", T.LongType()),
    T.StructField("reply_to_mismatch", T.LongType()), T.StructField("spf_result", T.StringType()),
    T.StructField("dkim_result", T.StringType()), T.StructField("dmarc_result", T.StringType()),
    T.StructField("is_phishing", T.IntegerType()),
])
DOMAIN_SCHEMA = T.StructType([
    T.StructField("sender_domain", T.StringType()), T.StructField("event_ts", T.TimestampType()),
    T.StructField("domain_name", T.StringType()), T.StructField("domain_age_days", T.LongType()),
    T.StructField("registrar_risk_score", T.DoubleType()), T.StructField("has_mx_record", T.LongType()),
    T.StructField("tls_cert_age_days", T.LongType()), T.StructField("on_blocklist", T.LongType()),
])
RECIPIENT_SCHEMA = T.StructType([
    T.StructField("recipient_id", T.StringType()), T.StructField("event_ts", T.TimestampType()),
    T.StructField("department", T.StringType()), T.StructField("org_domain", T.StringType()),
    T.StructField("is_vip", T.LongType()), T.StructField("has_privileged_access", T.LongType()),
    T.StructField("phish_sim_click_rate", T.DoubleType()), T.StructField("tenure_days", T.LongType()),
])
REPORT_SCHEMA = T.StructType([
    T.StructField("report_id", T.StringType()), T.StructField("email_id", T.StringType()),
    T.StructField("recipient_id", T.StringType()), T.StructField("sender_domain", T.StringType()),
    T.StructField("event_ts", T.TimestampType()),
])

# COMMAND ----------

# MAGIC %md ## Snapshots

# COMMAND ----------

def ts(d: dt.date, hours: float = 0.0) -> pd.Timestamp:
    return pd.Timestamp(d) + pd.Timedelta(hours=hours)


def legit_domain_snapshot(at: pd.Timestamp) -> pd.DataFrame:
    rng = np.random.default_rng([WORLD_SEED, 3, at.toordinal()])
    age = np.array([(at.date() - r).days for r in LEGIT["registered"]])
    return pd.DataFrame({
        "sender_domain": LEGIT["sender_domain"], "event_ts": at, "domain_name": LEGIT["sender_domain"],
        "domain_age_days": age.clip(0),
        "registrar_risk_score": np.round(np.clip(LEGIT["registrar_risk"] + rng.normal(0, 0.01, len(LEGIT)), 0, 1), 3),
        "has_mx_record": LEGIT["has_mx"], "tls_cert_age_days": (age.clip(0) % 90), "on_blocklist": 0,
    })


def recipient_snapshot(at: pd.Timestamp) -> pd.DataFrame:
    rng = np.random.default_rng([WORLD_SEED, 4, at.toordinal()])
    months = (at.year - WORLD_START.year) * 12 + at.month - WORLD_START.month
    # Awareness training slowly lowers click rates (≈2%/month), with per-snapshot noise.
    click = np.clip(RECIPIENTS["base_click"] * (0.98 ** months) + rng.normal(0, 0.01, len(RECIPIENTS)), 0, 0.9)
    return pd.DataFrame({
        "recipient_id": RECIPIENTS["recipient_id"], "event_ts": at,
        "department": RECIPIENTS["department"], "org_domain": RECIPIENTS["org_domain"],
        "is_vip": RECIPIENTS["is_vip"], "has_privileged_access": RECIPIENTS["has_privileged_access"],
        "phish_sim_click_rate": np.round(click, 4),
        "tenure_days": [(at.date() - h).days for h in RECIPIENTS["hire_date"]],
    })

# COMMAND ----------

# MAGIC %md ## One day of email

# COMMAND ----------

def _mutate(label: str, rng) -> str:
    """A lookalike of `label`: homoglyph swap, typo, or a plausible prefix/suffix."""
    kind = rng.choice(["glyph", "glyph", "double", "drop", "suffix", "suffix", "prefix"])
    if kind == "glyph":
        swaps = [(a, b) for a, b in [("i", "1"), ("l", "1"), ("o", "0"), ("e", "3"), ("m", "rn"), ("a", "4"),
                                     ("c", "k"), ("y", "v")] if a in label]
        if swaps:
            a, b = swaps[rng.integers(len(swaps))]
            pos = [i for i, ch in enumerate(label) if ch == a]
            i = pos[rng.integers(len(pos))]
            return label[:i] + b + label[i + 1:]
    if kind == "double":
        i = int(rng.integers(len(label)))
        return label[:i] + label[i] + label[i:]
    if kind == "drop" and len(label) > 4:
        i = int(rng.integers(1, len(label) - 1))
        return label[:i] + label[i + 1:]
    if kind == "prefix":
        return rng.choice(["secure-", "my-", "login-", "mail-"]) + label
    return label + rng.choice(["-secure", "-login", "-it", "-support", "-hr", "-portal", "-verify", "-billing"])


def _auth(n, rng, p_spf, p_dkim, p_dmarc):
    pick = lambda p, alts: np.where(rng.random(n) < p, "pass", rng.choice(alts, n))
    return pick(p_spf, ["fail", "softfail", "none"]), pick(p_dkim, ["fail", "none"]), pick(p_dmarc, ["fail", "none"])


def generate_day(day: dt.date, taken: set):
    """Returns (emails, domain_snapshots, reports) for one day. `taken` = sender domains already in use."""
    rng = np.random.default_rng([WORLD_SEED, 5, day.toordinal()])
    day_end = ts(day, 24) - pd.Timedelta(seconds=1)
    recip_click = recipient_snapshot(ts(day)).set_index("recipient_id")["phish_sim_click_rate"]
    frames, snaps = [], []

    # ── Benign: ~330/weekday, half on weekends; senders by popularity; mostly business hours (UTC) ──
    n = int(rng.poisson(330 * (1.15 if day.weekday() < 5 else 0.5)))
    di = rng.choice(len(LEGIT), n, p=(LEGIT["popularity"] / LEGIT["popularity"].sum()).values)
    cat = LEGIT["category"].values[di]
    hours = np.where(rng.random(n) < 0.8, rng.normal(15, 3, n), rng.uniform(0, 24, n)).clip(0, 23.99)
    prm = [CATS[c] for c in cat]
    col = lambda k: np.array([p[k] for p in prm], dtype=float)
    spf, dkim, dmarc = (np.where(rng.random(n) < np.array([p[5][k] for p in prm]), "pass",
                                 rng.choice(["fail", "softfail", "none"] if k == 0 else ["fail", "none"], n))
                        for k in range(3))
    frames.append(pd.DataFrame({
        "recipient_id": rng.choice(RECIPIENTS["recipient_id"], n), "sender_domain": LEGIT["sender_domain"].values[di],
        "event_ts": [ts(day, h) for h in hours],
        "subject_has_urgency": (rng.random(n) < col(3)).astype(int),
        "num_links": rng.poisson(col(1)),
        "num_attachments": np.where(rng.random(n) < col(2), rng.integers(1, 3, n), 0),
        "has_html_form": (rng.random(n) < 0.05).astype(int),   # surveys, sign-up forms
        "reply_to_mismatch": (rng.random(n) < col(4)).astype(int),
        "spf_result": spf, "dkim_result": dkim, "dmarc_result": dmarc, "is_phishing": 0,
    }))

    # ── New legitimate senders: ~4/day, freshly registered (new vendors, event / marketing domains) ──
    for _ in range(int(rng.poisson(4))):
        domain = rng.choice(SYLL) + rng.choice(SYLL) + rng.choice(["", "app", "events", "go", "mail", "hq"]) + rng.choice(TLDS)
        while domain in taken:
            domain = domain.split(".")[0] + str(int(rng.integers(2, 99))) + "." + domain.split(".", 1)[1]
        taken.add(domain)
        c = rng.choice(["saas", "vendor", "newsletter"], p=[0.4, 0.3, 0.3])
        pr = CATS[c]
        m = int(rng.integers(5, 40)) if c == "newsletter" else int(rng.integers(1, 8))
        start = rng.uniform(0, 20)
        first_seen = ts(day, start) - pd.Timedelta(minutes=float(rng.uniform(5, 600)))
        age = max(0, int(rng.uniform(0, 45)))
        snaps.append({"sender_domain": domain, "domain_name": domain, "event_ts": first_seen, "domain_age_days": age,
                      "registrar_risk_score": round(float(rng.beta(2.5, 5)), 3), "has_mx_record": int(rng.random() < 0.9),
                      "tls_cert_age_days": min(age, int(rng.integers(0, 30))), "on_blocklist": 0})
        spf, dkim, dmarc = _auth(m, rng, pr[5][0] - 0.1, pr[5][1] - 0.15, pr[5][2] - 0.2)   # new senders often misconfigure
        frames.append(pd.DataFrame({
            "recipient_id": rng.choice(RECIPIENTS["recipient_id"], m, replace=False), "sender_domain": domain,
            "event_ts": [ts(day, h) for h in np.sort(start + rng.uniform(0, 4, m))],
            "subject_has_urgency": (rng.random(m) < pr[3]).astype(int), "num_links": rng.poisson(pr[1], m),
            "num_attachments": (rng.random(m) < pr[2]).astype(int), "has_html_form": (rng.random(m) < 0.08).astype(int),
            "reply_to_mismatch": (rng.random(m) < pr[4]).astype(int),
            "spf_result": spf, "dkim_result": dkim, "dmarc_result": dmarc, "is_phishing": 0,
        }))

    # ── Phishing campaigns: ~1.2/day (≈5% of email) ──
    for c in range(int(rng.poisson(1.2))):
        kind = rng.choice(["org_lookalike", "brand", "compromised_vendor"], p=[0.40, 0.30, 0.30])
        if kind == "compromised_vendor":
            pool = LEGIT[LEGIT["category"].isin(["vendor", "partner", "finance"])]
            domain = pool["sender_domain"].values[rng.choice(len(pool), p=(pool["popularity"] / pool["popularity"].sum()).values)]
        else:
            base = rng.choice([d.split(".")[0] for d in ORG_DOMAINS], p=ORG_WEIGHTS) if kind == "org_lookalike" else rng.choice(BRANDS)
            domain = _mutate(base, rng) + rng.choice([".com", ".co", ".net", ".info", ".xyz", ".support"])
            while domain in taken or domain in ORG_DOMAINS:
                domain = domain.split(".")[0] + str(int(rng.integers(2, 99))) + "." + domain.split(".", 1)[1]
        spray = rng.random() < (0.10 if kind == "compromised_vendor" else 0.55)
        m = int(rng.integers(8, 45)) if spray else int(rng.integers(1, 7))
        dur = rng.uniform(1, 4) if spray else rng.uniform(0.5, 6)
        start = rng.uniform(0, 24 - dur)
        if spray:
            targets = rng.choice(RECIPIENTS["recipient_id"], m, replace=False)
        else:
            w = RECIPIENTS["spear_weight"].values
            targets = rng.choice(RECIPIENTS["recipient_id"], m, replace=False, p=w / w.sum())
        if kind == "compromised_vendor":
            spf, dkim, dmarc = _auth(m, rng, 0.96, 0.93, 0.90)   # real vendor infra → auth passes
        else:
            spf, dkim, dmarc = _auth(m, rng, 0.70, 0.60, 0.45)   # many kits configure SPF/DKIM now
        frames.append(pd.DataFrame({
            "recipient_id": targets, "sender_domain": domain,
            "event_ts": [ts(day, h) for h in np.sort(start + rng.uniform(0, dur, m))],
            "subject_has_urgency": (rng.random(m) < 0.40).astype(int),
            "num_links": np.maximum(1, rng.poisson(2.5, m)) if kind != "compromised_vendor" else rng.poisson(1.5, m),
            "num_attachments": (rng.random(m) < (0.5 if kind == "compromised_vendor" else 0.25)).astype(int),
            "has_html_form": (rng.random(m) < (0.30 if kind == "brand" else 0.18 if kind == "org_lookalike" else 0.05)).astype(int),
            "reply_to_mismatch": (rng.random(m) < (0.5 if kind == "compromised_vendor" else 0.30)).astype(int),
            "spf_result": spf, "dkim_result": dkim, "dmarc_result": dmarc, "is_phishing": 1,
        }))
        if kind != "compromised_vendor":
            taken.add(domain)
            first_seen = ts(day, start) - pd.Timedelta(minutes=float(rng.uniform(5, 120)))
            aged = rng.random() < 0.45   # parked/aged domains: registered long before use to dodge age filters
            registered = first_seen - pd.Timedelta(days=float(rng.uniform(90, 2000) if aged else rng.uniform(0, 20)))
            snap = {"sender_domain": domain, "domain_name": domain,
                    "registrar_risk_score": round(float(rng.beta(2.5, 5) if aged else rng.beta(3, 4)), 3),
                    "has_mx_record": int(rng.random() < 0.6), "on_blocklist": 0}
            age = lambda at: max(0, (at - registered).days)
            snaps.append({**snap, "event_ts": first_seen, "domain_age_days": age(first_seen),
                          "tls_cert_age_days": int(rng.integers(0, min(age(first_seen), 60) + 1))})
            if rng.random() < 0.55:   # threat intel catches it a few hours in → later emails see on_blocklist=1
                listed = ts(day, start) + pd.Timedelta(hours=float(rng.exponential(5)))
                if listed <= day_end:
                    snaps.append({**snap, "event_ts": listed, "on_blocklist": 1, "domain_age_days": age(listed),
                                  "tls_cert_age_days": int(rng.integers(0, min(age(listed), 60) + 1))})

    emails = pd.concat(frames, ignore_index=True).sort_values("event_ts", kind="stable").reset_index(drop=True)
    emails.insert(0, "email_id", [f"e{day:%Y%m%d}-{i:05d}" for i in range(len(emails))])

    # ── User reports: phish reported by careful users, rare false reports; same-day only ──
    click = recip_click.reindex(emails["recipient_id"]).values
    p = np.where(emails["is_phishing"] == 1, 0.40 * (1 - click), 0.003)
    rep = emails[rng.random(len(emails)) < p].copy()
    rep["event_ts"] = rep["event_ts"] + pd.to_timedelta(2 + rng.exponential(90, len(rep)), unit="m")
    rep = rep[rep["event_ts"] <= day_end]
    reports = pd.DataFrame({"report_id": [f"p-{e}" for e in rep["email_id"]], "email_id": rep["email_id"],
                            "recipient_id": rep["recipient_id"], "sender_domain": rep["sender_domain"],
                            "event_ts": rep["event_ts"]})
    return emails, pd.DataFrame(snaps, columns=[f.name for f in DOMAIN_SCHEMA]), reports

# COMMAND ----------

# MAGIC %md ## Plan the run: seed or catch up

# COMMAND ----------

def exists(t):
    return spark.catalog.tableExists(f"{FQ}.{t}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
if REBUILD:
    for t in ["emails", "sender_domain_features", "recipient_features", "phish_reports", "emails_to_score"]:
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{t}")

seeding = not exists("emails")
if seeding:
    start = THROUGH - dt.timedelta(days=SEED_DAYS - 1)
else:
    start = spark.sql(f"SELECT max(date(event_ts)) AS d FROM {FQ}.emails").first()["d"] + dt.timedelta(days=1)
days = [start + dt.timedelta(days=i) for i in range((THROUGH - start).days + 1)][:MAX_DAYS]
print(f"{'SEED' if seeding else 'CATCH-UP'}: {len(days)} day(s) {days[0] if days else '-'} → {days[-1] if days else '-'}")

# COMMAND ----------

if days:
    taken = set(LEGIT["sender_domain"]) | set(ORG_DOMAINS)
    if not seeding:
        taken |= {r[0] for r in spark.sql(f"SELECT DISTINCT sender_domain FROM {FQ}.sender_domain_features").collect()}

    all_emails, all_snaps, all_reports = [], [], []
    dom_snaps, rec_snaps = [], []
    if seeding:   # initial snapshots the day before the first email, so every lookup resolves
        dom_snaps.append(legit_domain_snapshot(ts(start - dt.timedelta(days=1))))
        rec_snaps.append(recipient_snapshot(ts(start - dt.timedelta(days=1))))
    for d in days:
        if d.weekday() == 0:
            dom_snaps.append(legit_domain_snapshot(ts(d)))     # weekly, Mondays 00:00
        if d.day == 1:
            rec_snaps.append(recipient_snapshot(ts(d)))        # monthly, 1st 00:00
        e, s, r = generate_day(d, taken)
        all_emails.append(e); all_snaps.append(s); all_reports.append(r)

    emails = pd.concat(all_emails, ignore_index=True)
    domains = pd.concat(dom_snaps + all_snaps, ignore_index=True)
    recipients = pd.concat(rec_snaps, ignore_index=True) if rec_snaps else None
    reports = pd.concat(all_reports, ignore_index=True)

    def write(pdf, schema, table):
        if pdf is None or len(pdf) == 0:
            return
        cols = [f.name for f in schema.fields]
        pdf = pdf.copy()
        pdf["event_ts"] = pd.to_datetime(pdf["event_ts"]).dt.tz_localize("UTC")   # Spark Connect needs tz-aware
        sdf = spark.createDataFrame(pdf[cols].astype(object).where(pdf[cols].notna(), None).values.tolist(), schema)
        sdf.write.mode("append").saveAsTable(f"{FQ}.{table}")
        if seeding:   # CDF on so the app can materialize these tables online
            spark.sql(f"ALTER TABLE {FQ}.{table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
        print(f"  {table}: +{len(pdf):,} rows")

    write(emails, EMAILS_SCHEMA, "emails")
    write(domains, DOMAIN_SCHEMA, "sender_domain_features")
    write(recipients, RECIPIENT_SCHEMA, "recipient_features")
    write(reports, REPORT_SCHEMA, "phish_reports")

# COMMAND ----------

# MAGIC %md ## Batch-scoring input + on-demand function

# COMMAND ----------

# Newest day's emails as request rows (no label, no timestamp) — the batch-deployment demo input.
(spark.sql(f"""
    SELECT email_id, recipient_id, sender_domain, subject_has_urgency, num_links, num_attachments,
           has_html_form, reply_to_mismatch, spf_result, dkim_result, dmarc_result
    FROM {FQ}.emails WHERE date(event_ts) = (SELECT max(date(event_ts)) FROM {FQ}.emails)""")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.emails_to_score"))

# Lookalike score of a sender domain vs the recipient's own org domain (0..1). Homoglyphs are folded
# first (0→o, 1→l, rn→m …), and containing the org name (halcyon-secure.co) scores ≥ 0.9.
spark.sql(f"""
CREATE OR REPLACE FUNCTION {FQ}.domain_similarity(sender_domain STRING, org_domain STRING)
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT 'Lookalike score of sender_domain vs org_domain (1.0 = identical name). Phishing-defense example.'
AS $$
import difflib
if not sender_domain or not org_domain:
    return 0.0
s = sender_domain.lower().split('.')[0]
o = org_domain.lower().split('.')[0]
for a, b in (('rn', 'm'), ('vv', 'w'), ('0', 'o'), ('1', 'l'), ('3', 'e'), ('4', 'a'), ('5', 's')):
    s = s.replace(a, b)
r = difflib.SequenceMatcher(None, s, o).ratio()
if o in s or s in o:
    r = max(r, 0.9)
return float(round(r, 4))
$$""")

# COMMAND ----------

# MAGIC %md ## Summary

# COMMAND ----------

summary = spark.sql(f"""
  SELECT (SELECT count(*) FROM {FQ}.emails) AS emails,
         (SELECT round(avg(is_phishing), 4) FROM {FQ}.emails) AS phishing_rate,
         (SELECT min(event_ts) FROM {FQ}.emails) AS first_email,
         (SELECT max(event_ts) FROM {FQ}.emails) AS last_email,
         (SELECT count(*) FROM {FQ}.sender_domain_features) AS domain_snapshots,
         (SELECT count(*) FROM {FQ}.recipient_features) AS recipient_snapshots,
         (SELECT count(*) FROM {FQ}.phish_reports) AS reports,
         (SELECT count(*) FROM {FQ}.emails_to_score) AS to_score""").first().asDict()
print(summary)
dbutils.notebook.exit(str(summary))
