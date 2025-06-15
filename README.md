# 💱 Automated FX Rate Tracker with Airflow & Docker

This project is an end-to-end ETL pipeline that automatically fetches, processes, stores, and analyzes foreign exchange (FX) rates using the [Frankfurter API](https://www.frankfurter.app/). It leverages **Apache Airflow** for orchestration, **Docker** for containerization, and **PostgreSQL** as the storage layer. Additionally, the pipeline includes basic **exploratory data analysis** and **volatility monitoring** using Python and Jupyter.

---

## 🚀 Project Overview

### ✅ Technologies Used

* **Apache Airflow**: Workflow orchestration
* **Docker**: Containerized development
* **PostgreSQL**: Relational database
* **pandas**: Data manipulation
* **SQLAlchemy**: Python SQL toolkit
* **Frankfurter API**: Currency data source
* **matplotlib**: Visualization

---

## 🔧 How It Works

1. **Extract**: Daily FX rates are fetched from the Frankfurter API.
2. **Transform**: The JSON response is parsed and converted to a structured pandas DataFrame.
3. **Load**: Cleaned data is inserted into the `exchange_rates` table in PostgreSQL using SQLAlchemy.
4. **Schedule**: Airflow executes the ETL DAG once per day.

---

## 📊 Sample Data Structure

Each row includes:

* `amount`: Always 1 (base value)
* `base_currency`: e.g., EUR
* `target_currency`: e.g., USD, GBP, TRY
* `rate`: Daily FX rate
* `date`: Corresponding date of the rate

---

## 📉 Analysis & Visualizations

The Jupyter notebook (`analysis_fx_rates.ipynb`) connects directly to PostgreSQL and performs several visual analyses:

### 📌 1. EUR Exchange Rate Trends

EUR vs USD, GBP, and TRY plotted over time:

![Exchange Rate Trends](outputs/output1.png)

---

### 📌 2. Daily % Change of EUR/TRY

Helps spot sudden fluctuations in EUR/TRY rates:

![Daily % Change EUR/TRY](outputs/output2.png)

---

### 📌 3. Daily % Change Comparison (EUR/USD, EUR/GBP, EUR/TRY)

Comparative view of daily volatility across currencies:

![% Daily Change Comparison](outputs/output3.png)

---

### 📌 4. EUR/TRY Volatility (7 & 14-Day Rolling Std Dev)

Tracks short-term vs long-term volatility:
![Volatility 7 & 14](outputs/output4.png)


---

## 🐳 Running with Docker

```bash
docker compose up --build
```

Access the Airflow UI: [http://localhost:8080](http://localhost:8080)

Default login:

* Username: `airflow`
* Password: `airflow`

---

## 🧪 Manual Querying (via Jupyter)

You can analyze the data independently with `SQLAlchemy`:

```python
from sqlalchemy import create_engine
import pandas as pd

db_url = "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow"
engine = create_engine(db_url)

query = "SELECT * FROM exchange_rates ORDER BY date DESC"
df = pd.read_sql(query, con=engine)
```

---

## 📄 License

This project is open-source and licensed under the MIT License. Intended for educational use.

---
