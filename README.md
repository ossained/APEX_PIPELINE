# 🏢 Introduction


Apex Wealth Management is a premier financial firm managing diverse portfolios for high net worth individuals, institutions, and retirement funds. Their investment strategies span conservative to aggressive, covering stocks, bonds, real estate, and alternative assets.

To deliver superior investment returns, Apex relies on accurate, real‑time market data. As the firm expands into new sectors, attracts new clients, and evaluates new funds, timely and reliable stock price data becomes essential for:

* Portfolio diversification
* Market opportunity identification
* Client reporting
* Regulatory compliance

This project builds a production‑ready automated data pipeline that fetches, cleans, validates, and stores global stock market data using the Twelve Data API, Python, Airflow, and PostgreSQL.

# 🎯 Project Overview


Apex needs  to run reliably, automatically, and with full auditability in an on premise environment.

This project upgrades the prototype into a fully automated ETL pipeline with:

* Scheduled runs

* Centralized logging

* Error handling

* Data validation

* Database persistence


# ❗ Problem Statement
Apex Wealth Management currently faces several operational challenges:

1. Manual runs are inconsistent and easy to miss.

2. Failures lack traceability due to missing structured logs.

3. No scheduler means delayed data and missed market windows.

4. Weak validation risks partial or corrupted loads entering reporting tables.

5. Continuous market monitoring is required to stay ahead of global trends.


## 💡 Why This Project Matters (Business Rationale)
1. Automate Market Data Collection
Airflow schedules daily pipeline runs so fresh market data lands without manual intervention.

2. Improve Auditability and Monitoring
Centralized Airflow logs provide timestamped execution history for debugging and compliance.

3. Protect Data Quality
A downstream validation task verifies row counts and expected stock symbols before marking the run successful.

4. Enhance Client Reporting
Accurate, up to date data supports transparent performance dashboards and client communication.

5. Ensure Regulatory Compliance
Structured ETL processes and audit trails support financial governance requirements.


## 🛠️ Tech Stack

| Layer           | Tools                |
|----------------|----------------------|
| API Source     | Twelve Data API      |
| Programming    | Python               |
| Data Processing| Pandas, JSON         |
| Orchestration  | Apache Airflow       |
| Database       | PostgreSQL           |
| Version Control| Git & GitHub         |




## 🔄 Project Workflow
STEP 1 — API Integration
Connect to Twelve Data using Python to fetch real‑time stock market data.

STEP 2 — Airflow Automation
Use Airflow DAGs to schedule daily runs and orchestrate ETL tasks.

STEP 3 — Logging & Error Handling
Each ETL stage logs events; tasks fail on API, schema, or database issues.

STEP 4 — Data Validation
A downstream quality‑check task confirms:

Expected stock symbols are present

Row counts match expectations

Data types and timestamps are valid
