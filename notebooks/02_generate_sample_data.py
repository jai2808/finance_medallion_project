# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Generate Sample Finance Data
# MAGIC Creates synthetic customers, branches, accounts, and transactions, and writes
# MAGIC them to the landing volume paths defined in `pipeline_config.json`.
# MAGIC Intentionally injects nulls / bad values into transactions so the Silver
# MAGIC data-quality rules and quarantine pattern have something real to catch.
# MAGIC
# MAGIC In production this notebook does not exist — real source systems (core
# MAGIC banking DB, card processor feed, branch CRM) land files here instead.

# COMMAND ----------
dbutils.widgets.text("catalog", "finance_project")
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------
import random
from datetime import datetime, timedelta
import pandas as pd

random.seed(42)
N_CUSTOMERS = 200
N_BRANCHES = 10
N_ACCOUNTS = 300
N_TRANSACTIONS = 5000

# COMMAND ----------
# MAGIC %md ### Customers

# COMMAND ----------
segments = ["Retail", "Premium", "Corporate"]
customers = pd.DataFrame({
    "cust_id": [f"C{1000+i}" for i in range(N_CUSTOMERS)],
    "cust_name": [f"Customer_{i}" for i in range(N_CUSTOMERS)],
    "dob": [(datetime(1970, 1, 1) + timedelta(days=random.randint(0, 18000))).strftime("%Y-%m-%d")
            for _ in range(N_CUSTOMERS)],
    "segment": [random.choice(segments) for _ in range(N_CUSTOMERS)],
    "email": [f"customer{i}@example.com" for i in range(N_CUSTOMERS)],
})

# COMMAND ----------
# MAGIC %md ### Branches

# COMMAND ----------
cities = ["Mumbai", "Bengaluru", "Delhi", "Chennai", "Hyderabad"]
branches = pd.DataFrame({
    "branch_id": [f"B{100+i}" for i in range(N_BRANCHES)],
    "branch_city": [random.choice(cities) for _ in range(N_BRANCHES)],
    "branch_name": [f"Branch_{i}" for i in range(N_BRANCHES)],
})

# COMMAND ----------
# MAGIC %md ### Accounts

# COMMAND ----------
acc_types = ["Savings", "Current", "FD", "Loan"]
accounts = pd.DataFrame({
    "acc_id": [f"A{5000+i}" for i in range(N_ACCOUNTS)],
    "customer_id": [random.choice(customers["cust_id"]) for _ in range(N_ACCOUNTS)],
    "branch_id": [random.choice(branches["branch_id"]) for _ in range(N_ACCOUNTS)],
    "account_type": [random.choice(acc_types) for _ in range(N_ACCOUNTS)],
    "open_date": [(datetime(2018, 1, 1) + timedelta(days=random.randint(0, 2500))).strftime("%Y-%m-%d")
                  for _ in range(N_ACCOUNTS)],
})

# COMMAND ----------
# MAGIC %md ### Transactions (with intentional dirty data)

# COMMAND ----------
txn_types = ["DEBIT", "CREDIT", "TRANSFER"]
rows = []
for i in range(N_TRANSACTIONS):
    amt = round(random.uniform(10, 50000), 2)
    if i % 500 == 0:
        amt = None                       # inject nulls -> caught by not_null rule
    if i % 700 == 0 and amt is not None:
        amt = -abs(amt)                  # inject negative amounts -> caught by range rule
    rows.append({
        "txn_id": f"T{20000+i}",
        "acc_id": random.choice(accounts["acc_id"]),
        "txn_amt": amt,
        "txn_type": random.choice(txn_types),
        "txn_date": (datetime(2024, 1, 1) + timedelta(days=random.randint(0, 600))).strftime("%Y-%m-%d"),
    })
transactions = pd.DataFrame(rows)

# COMMAND ----------
# MAGIC %md ### Write to landing volume (simulating source system drops)

# COMMAND ----------
landing = f"/Volumes/{catalog}/control/landing"

spark.createDataFrame(customers).write.mode("overwrite").option("header", True) \
    .csv(f"{landing}/customers/")

spark.createDataFrame(branches).write.mode("overwrite").option("header", True) \
    .csv(f"{landing}/branches/")

spark.createDataFrame(accounts).write.mode("overwrite").option("header", True) \
    .csv(f"{landing}/accounts/")

spark.createDataFrame(transactions).write.mode("overwrite") \
    .json(f"{landing}/transactions/")

print("✅ Sample finance data written to landing volume.")
print(f"customers={len(customers)}, branches={len(branches)}, "
      f"accounts={len(accounts)}, transactions={len(transactions)}")
