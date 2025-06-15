import requests
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import date, timedelta

## Based on main.ipynb

## 30 days
def extract():
    end_date = date.today()
    start_date = end_date - timedelta(days=30)
    url = f"https://api.frankfurter.app/{start_date.strftime('%Y-%m-%d')}..{end_date.strftime('%Y-%m-%d')}?from=EUR"
    response = requests.get(url)
    data = response.json()
    return data

def transform(data):
    records = []

    for date_str, rate_dict in data["rates"].items():
        for target_currency, rate in rate_dict.items():
            records.append({
                "amount": data["amount"],
                "base_currency": data["base"],
                "date": date_str,
                "target_currency": target_currency,
                "rate": rate
            })

    df_final = pd.DataFrame(records)
    return df_final

def load(df_final):
    db_name = "airflow"
    db_user = "airflow"
    db_password = "airflow"
    db_host = "postgres"
    db_port = 5432

    db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(db_url)

    with engine.begin() as conn:
        for _, row in df_final.iterrows():
            query = text("""
                INSERT INTO exchange_rates (amount, base_currency, date, target_currency, rate)
                VALUES (:amount, :base_currency, :date, :target_currency, :rate)
                ON CONFLICT (base_currency, target_currency, date) DO NOTHING;
            """)
            conn.execute(query, row.to_dict())

