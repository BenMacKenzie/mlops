# Databricks notebook source
# MAGIC %md
# MAGIC # Demo: daily synthetic transactions
# MAGIC Keeps `credit_card_fraud_demo.transactions` current so time-windowed features (30d sums/counts) aren't
# MAGIC empty "as of now", and publishes the newest day's transactions — request fields only, no label, no
# MAGIC timestamp — to `transactions_to_score`, the input table for the batch-inference demo.
# MAGIC
# MAGIC Self-healing + idempotent: generates every missing day from `max(date(transaction_ts)) + 1` through
# MAGIC `through_date` (default: today UTC), so a skipped run catches up and a re-run on the same day is a no-op.
# MAGIC
# MAGIC Distribution matches the original 2026-07-02 → 2026-09-29 seed data (`BASE_END`):
# MAGIC - ~55 txns/day (Poisson), timestamps at midnight (same as seed), customers/merchants drawn from existing IDs
# MAGIC - amount ~ U(0, 2000); pos_entry_mode ~ U{0..4}; security_code ~ U{000..999}
# MAGIC - is_fraud sampled from the seed data's empirical fraud rate per (amount quartile × pos_entry_mode=='4' ×
# MAGIC   merchant-risk tercile) cell, so the label keeps its real signal. Rates are fit on seed data ONLY (never on
# MAGIC   generated rows) to avoid drift.

# COMMAND ----------

dbutils.widgets.text("catalog", "serverless_stable_77rg2n_catalog")
dbutils.widgets.text("schema", "credit_card_fraud_demo")
dbutils.widgets.text("through_date", "")        # YYYY-MM-DD; blank = today (UTC)
dbutils.widgets.text("mean_per_day", "55")
dbutils.widgets.text("max_days", "120")          # safety cap on a single catch-up run

import datetime as dt
import json

import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CAT = dbutils.widgets.get("catalog")
SCH = dbutils.widgets.get("schema")
FQ = f"{CAT}.{SCH}"
MEAN = float(dbutils.widgets.get("mean_per_day"))
MAX_DAYS = int(dbutils.widgets.get("max_days"))
through = dbutils.widgets.get("through_date").strip()
THROUGH = dt.date.fromisoformat(through) if through else dt.datetime.now(dt.timezone.utc).date()
BASE_END = dt.date(2026, 9, 29)   # last day of the original seed data

# COMMAND ----------

tx = spark.table(f"{FQ}.transactions")
last_day = tx.select(F.max(F.to_date("transaction_ts"))).first()[0]
days = [last_day + dt.timedelta(days=i) for i in range(1, (THROUGH - last_day).days + 1)][:MAX_DAYS]
print(f"last day in table: {last_day}; generating {len(days)} day(s) through {THROUGH}")

if not days:
    dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "days": 0, "rows": 0, "last_day": str(last_day)}))

# COMMAND ----------

# Empirical fraud rates from SEED data only. Merchant risk = latest SCD2 version per merchant.
risk = (spark.table(f"{FQ}.merchant_features")
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("merchant_id").orderBy(F.col("transaction_ts").desc())))
        .where("rn = 1").select("merchant_id", "merchant_risk_score").toPandas())
seed = (tx.where(F.to_date("transaction_ts") <= F.lit(BASE_END))
        .select("customer_id", "merchant_id", "amount", "pos_entry_mode", "is_fraud").toPandas()
        .merge(risk, on="merchant_id", how="left"))

AMT_EDGES = [0, 500, 1000, 1500, np.inf]
RISK_EDGES = [-np.inf, 1 / 3, 2 / 3, np.inf]
def cells(df):
    return (pd.cut(df["amount"], AMT_EDGES, labels=False, include_lowest=True).astype(int).astype(str) + "|"
            + (df["pos_entry_mode"] == "4").astype(int).astype(str) + "|"
            + pd.cut(df["merchant_risk_score"].fillna(0.5), RISK_EDGES, labels=False).astype(int).astype(str))
seed["cell"] = cells(seed)
rates = seed.groupby("cell")["is_fraud"].mean().to_dict()
overall = float(seed["is_fraud"].mean())
customers = sorted(seed["customer_id"].unique())
merchants = sorted(risk["merchant_id"].unique())
print(f"seed rows {len(seed)}, overall fraud {overall:.3f}, {len(rates)} cells")

# COMMAND ----------

max_id = tx.select(F.max(F.regexp_extract("transaction_id", r"(\d+)", 1).cast("long"))).first()[0]
rng = np.random.default_rng(int(dt.datetime.now().timestamp()))
frames = []
next_id = max_id + 1
for d in days:
    n = int(rng.poisson(MEAN))
    df = pd.DataFrame({
        "transaction_id": [f"T{next_id + i:08d}" for i in range(n)],
        "customer_id": rng.choice(customers, n),
        "merchant_id": rng.choice(merchants, n),
        "transaction_ts": pd.Timestamp(d),
        "amount": np.round(rng.uniform(0, 2000, n), 2),
        "pos_entry_mode": rng.integers(0, 5, n).astype(str),
        "security_code": [f"{c:03d}" for c in rng.integers(0, 1000, n)],
    })
    next_id += n
    p = cells(df.merge(risk, on="merchant_id", how="left")).map(rates).fillna(overall).to_numpy()
    df["is_fraud"] = (rng.random(n) < p).astype("int32")
    frames.append(df)
new = pd.concat(frames, ignore_index=True)

# COMMAND ----------

# Match the target schema exactly (STRING codes, DOUBLE amount, INT label, TIMESTAMP ts).
new_sdf = (spark.createDataFrame(new)
           .select(F.col("transaction_id").cast("string"), F.col("customer_id").cast("string"),
                   F.col("merchant_id").cast("string"), F.col("transaction_ts").cast("timestamp"),
                   F.col("amount").cast("double"), F.col("pos_entry_mode").cast("string"),
                   F.col("security_code").cast("string"), F.col("is_fraud").cast("int")))
new_sdf.write.mode("append").saveAsTable(f"{FQ}.transactions")

# Batch-inference demo input: the newest day's transactions as the requesting system would send them —
# no label, no timestamp (the batch job supplies the observation time = its run time).
latest = max(days)
(new_sdf.where(F.to_date("transaction_ts") == F.lit(latest))
 .select("transaction_id", "customer_id", "merchant_id", "amount", "pos_entry_mode", "security_code")
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.transactions_to_score"))

summary = {"status": "SUCCESS", "days": len(days), "rows": len(new), "first_day": str(min(days)),
           "last_day": str(latest), "fraud_rate": round(float(new["is_fraud"].mean()), 3),
           "to_score_rows": int((new["transaction_ts"] == pd.Timestamp(latest)).sum())}
print(summary)
dbutils.notebook.exit(json.dumps(summary))
