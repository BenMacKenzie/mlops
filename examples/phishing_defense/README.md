# Example: phishing defense (inbound email)

Score each inbound email at delivery: phishing or not. Synthetic data for the MLOps app that exercises
every feature type: spine fields, point-in-time lookups, sliding-window aggregations and an on-demand UDF.

- **Generator:** `generate.py`, a notebook with **no schedule**. It was seeded on 2026-11-24 with a one-off
  serverless run covering 2026-09-25 → 2026-11-23 (22,273 emails, 5.24% phishing). Running it again
  appends the missing days through yesterday and refreshes `emails_to_score`; `rebuild=true` starts over.
  To rerun: import it into the workspace (e.g. `~/.mlops/examples/phishing_defense_generate`) and run it
  on serverless.
- **Where:** `serverless_stable_77rg2n_catalog.phishing_defense_demo` (profile `fe-vm-serverless-stable-77rg2n`)

## Timestamps: every table has `event_ts`
The app passes the EOL's timestamp column to **every** feature, so every source table uses the same
column name, `event_ts`. Lookup tables are point-in-time **snapshots** keyed by (entity, `event_ts`);
the Feature View API joins the latest snapshot at or before each email's `event_ts`.
- `sender_domain_features`: an initial snapshot the day before the first email, then weekly (Mondays
  00:00). New domains get a snapshot when first seen, a few minutes to hours before their first email.
  A blocklisted campaign domain gets another snapshot with `on_blocklist = 1`, so only the emails
  sent after the listing see it.
- `recipient_features`: an initial snapshot, then monthly (1st, 00:00). `phish_sim_click_rate` drifts down
  over time to reflect awareness training.

## Objects

| Object | Role in the app | Columns |
|---|---|---|
| `emails` | **EOL spine.** One row per inbound email | `email_id`, `recipient_id`, `sender_domain`, `event_ts`, `subject_has_urgency`, `num_links`, `num_attachments`, `has_html_form`, `reply_to_mismatch` (LONG), `spf_result` / `dkim_result` / `dmarc_result` (STRING: pass/fail/softfail/none), label **`is_phishing`** (INT) |
| `sender_domain_features` | **Lookup**, key `sender_domain` | `event_ts`, `domain_name` (copy of the key; feeds the UDF), `domain_age_days`, `registrar_risk_score`, `has_mx_record`, `tls_cert_age_days`, `on_blocklist` |
| `recipient_features` | **Lookup**, key `recipient_id` | `event_ts`, `department` (STRING), `org_domain` (feeds the UDF), `is_vip`, `has_privileged_access`, `phish_sim_click_rate`, `tenure_days` |
| `phish_reports` | **Aggregation source**, entity `sender_domain` | `report_id`, `email_id`, `recipient_id`, `sender_domain`, `event_ts` (time the user reported the email) |
| `emails` (again) | **Aggregation source**, entity `sender_domain` | for counts / distinct recipients per sender domain |
| `domain_similarity(sender_domain STRING, org_domain STRING) → DOUBLE` | **On-demand UDF** (Python) | Lookalike score (0–1) of the sender vs the recipient's org. Folds homoglyphs (`0→o`, `1→l`, `rn→m` …); containing the org name scores ≥ 0.9 |
| `emails_to_score` | **Batch deployment input** | The newest day's emails as request fields: no label, no timestamp |

CDF is enabled on every table so the app can materialize them online. Categoricals are STRING; counts
and flags are LONG.

## The world
- **Organization:** Halcyon, with domains `halcyon.com`, `halcyonbank.com` and `halcyon-insure.com`.
  It has 1,200 recipients across 9 departments; Finance, HR, IT, Exec and VIPs attract spear-phishing.
- **Legitimate senders:** 450 established domains (SaaS, vendor, newsletter, partner, finance), weighted
  by popularity. They include 8 **org-adjacent** domains (`halcyon-benefits.com`, `mail-halcyonbank.com`,
  …) that look like lookalikes but aren't. About 4 **new legitimate senders** appear each day on young
  domains, often with sloppy SPF/DKIM/DMARC.
- **Phishing:** about 1.2 campaigns a day, roughly 5% of all email.
  - *Org lookalike* (40%) and *brand impersonation* (30%, e.g. Microsoft or DocuSign lookalikes): either
    spray (8–44 recipients within 1–4 hours) or spear (1–6 targets). 45% use **aged** domains,
    registered 90–2,000 days earlier, to dodge age filters. 55% get blocklisted a few hours in.
  - *Compromised vendor* (30%): a real, old vendor domain that passes authentication. Mostly spear
    invoice fraud aimed at Finance. These are the hard cases: only reply-to mismatch, user reports
    and unusual volume give them away.
- **User reports:** a phishing email is reported with p = 0.4 × (1 − the recipient's click rate), with a
  delay of about 90 minutes. False reports are rare (0.3%).

## Signal (dry-run check: 60 days, ~22k emails, 5.2% phishing)
Gradient-boosting CV AUC on the point-in-time-joined data: **0.964** with spine + lookup features, and
**0.980** with the windows, blocklist and UDF added. No single group gives the answer away: spine
fields alone reach 0.90, the domain lookup 0.89, the windows 0.75, the UDF 0.77, and the recipient
lookup 0.51. The recipient lookup changes who gets targeted, not the label.

## Walkthrough in the app
**Project:** catalog `serverless_stable_77rg2n_catalog`, schema `phishing_defense_demo`, model e.g. `phishing`.

**EOL:**
- SQL: `SELECT recipient_id, sender_domain, event_ts, subject_has_urgency, num_links, num_attachments, has_html_form, reply_to_mismatch, spf_result, dkim_result, dmarc_result, is_phishing FROM serverless_stable_77rg2n_catalog.phishing_defense_demo.emails`
- Label `is_phishing`, entity columns `recipient_id`, `sender_domain`, timestamp `event_ts`
- Entity columns and the timestamp are lookup / join keys; training drops them before fitting. Leave
  `email_id` out of the EOL: any other spine column becomes a model feature, and a per-row ID is pure noise
  (if you keep it, exclude it under Training Features).

**Feature entries:**
1. Lookup `sender_domain_features`, key `sender_domain`: `domain_age_days`, `registrar_risk_score`,
   `has_mx_record`, `tls_cert_age_days`, `on_blocklist` (+ `domain_name` for the UDF)
2. Lookup `recipient_features`, key `recipient_id`: `department`, `is_vip`, `has_privileged_access`,
   `phish_sim_click_rate` (+ `org_domain` for the UDF)
3. On-demand `…phishing_defense_demo.domain_similarity`: `sender_domain` ← `domain_name`,
   `org_domain` ← `org_domain`
4. Declarative over `emails`, entity `sender_domain`: `count(email_id)` and
   `approx_count_distinct(recipient_id)`, **sliding** 1d window (e.g. a 1h slide). This catches spray campaigns.
5. Declarative over `phish_reports`, entity `sender_domain`: `count(report_id)`, **sliding** 7d window.
   Captures reports filed by earlier recipients of the same campaign.

Use **sliding** (or tumbling) windows, not rolling: online materialization of a rolling window only
supports `last`.

**Batch deployment input:** table `…phishing_defense_demo.emails_to_score`, timestamp = run time.
