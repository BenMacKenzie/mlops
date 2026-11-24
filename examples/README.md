# Examples

Synthetic datasets for demoing the MLOps app end to end. Each folder holds its data-generation
notebook(s) and a README with the objects, the signal and an app walkthrough. Data lives in `serverless_stable_77rg2n_catalog` on `fe-vm-serverless-stable-77rg2n`.

| Example | Schema | Generator job | Notes |
|---|---|---|---|
| [credit_card_fraud](credit_card_fraud/README.md) | `credit_card_fraud_demo` | `mlops-demo-daily-transactions` (05:00 UTC) | The original demo. Only the daily generator is in git; the 2026-09-29 seed was created ad hoc. |
| [phishing_defense](phishing_defense/README.md) | `phishing_defense_demo` | none (run on demand) | Self-contained: the first run seeds 60 days; each rerun appends the missing days. Seeded 2026-11-24. |

**Rule for every example:** the Feature View API needs a timestamp on every feature, and the app uses
the EOL's timestamp column for all of them. So every source table, lookup snapshots included, has a
timestamp column with the same name as the spine's.
