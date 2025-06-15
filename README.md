# 💱 Automated FX Rate Tracker with Airflow & Docker

This project is an end-to-end ETL pipeline that automatically fetches, processes, and stores foreign exchange (FX) rates using the [Frankfurter API](https://www.frankfurter.app/). It leverages **Apache Airflow** for orchestration, **Docker** for containerization, and **PostgreSQL** as the storage layer.

---

## 🚀 Project Overview

### ✅ Technologies Used:
- **Apache Airflow**: Workflow orchestration
- **Docker**: Containerized development
- **PostgreSQL**: Relational database
- **pandas**: Data manipulation
- **SQLAlchemy**: Python SQL toolkit
- **Frankfurter API**: Currency data source

---

## 🔧 How It Works

1. **Extract**: Daily FX rates are fetched from the Frankfurter API.
2. **Transform**: The JSON response is cleaned and converted to a structured pandas DataFrame.
3. **Load**: The data is inserted into a PostgreSQL table (`exchange_rates`) using SQLAlchemy.
4. **Schedule**: Airflow executes the ETL DAG once a day.

---

## 📊 Data Example

Each row contains:
- `amount`: Always 1 (base amount)
- `base_currency`: e.g., EUR
- `target_currency`: e.g., USD, GBP, JPY
- `rate`: The FX rate
- `date`: Date of the exchange rate

---

## 🐳 Running with Docker

```bash
docker compose up --build
````

Access Airflow UI at: [http://localhost:8080](http://localhost:8080)

Login credentials (default):

* Username: `airflow`
* Password: `airflow`

---

## 🧪 Manual Query (via Jupyter)

You can query the database outside Airflow using `SQLAlchemy`:

```python
db_url = "postgresql+psycopg2://airflow:airflow@localhost:5433/airflow"
engine = create_engine(db_url)
df = pd.read_sql("SELECT * FROM exchange_rates ORDER BY date DESC LIMIT 5", con=engine)
```

---

## 📌 Notes

* The DAG is configured with `catchup=False`, meaning it only runs from the current date onward.
* To backfill historical data, you can modify the DAG's `start_date` and set `catchup=True`, or implement a loop in the `extract()` function to fetch a date range.

---

## 🛠️ Future Improvements

* Add data quality checks (e.g., missing currencies, extreme values)
* Create dashboards (e.g., with Superset or Streamlit)
* Schedule alerts if rates exceed thresholds
* Add test coverage and CI/CD pipeline

---

## 📄 License

This project is for educational purposes and is MIT-licensed.
