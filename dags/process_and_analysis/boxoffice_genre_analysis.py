import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable  # Airflow의 환경변수 불러오기 위함
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from google.cloud import storage, bigquery

load_dotenv()

BUCKET_NAME = Variable.get("BUCKET_NAME")
DAILY_REGION_BOXOFFICE_FOLDER = Variable.get("DAILY_REGION_BOXOFFICE_FOLDER")
BOXOFFICE_API_KEY = Variable.get("BOXOFFICE_API_KEY")
sa_key_dict = Variable.get("GOOGLE_APPLICATION_CREDENTIALS", deserialize_json=True)

with open("/tmp/gcp-sa-key.json", "w") as f:
    json.dump(sa_key_dict, f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp-sa-key.json"


def get_date():
    """
    어제 날짜일자를 구하는 함수입니다.
    """
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")

def upload_genre_analysis_to_bigquery():
    """
    merged_boxoffice 테이블에서 어제 날짜의 장르 데이터를 분리하고,
    BigQuery 테이블에 append 업로드합니다.
    """
    bq = bigquery.Client()
    yesterday = get_date()
    
    query = f"""
        SELECT boxoffice_date, movieNm, genreNm, salesAmt, audiCnt, openDt
        FROM `movie_boxoffice.merged_daily_boxoffice`
        WHERE DATE(date) >= DATE("{yesterday}")
    """
    df = bq.query(query).to_dataframe()
    
    df = df.dropna(subset=["genreNm"])
    df["genreNm"] = df["genreNm"].astype(str)
    
    df = df.assign(genre=df["genreNm"].str.split(","))
    df = df.explode("genre")
    df["genre"] = df["genre"].str.strip()
    
    genre_df = df[["boxoffice_date", "movieNm", "genre", "salesAmt", "audiCnt", "openDt"]]
    
    bq.load_table_from_dataframe(
        genre_df,
        "movie_boxoffice.analysis_genre",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    ).result()
    
# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="boxoffice_genre_analysis",
    schedule_interval="50 18 * * *",  # 매일 18:50 실행
    catchup=False,
    default_args=default_args,
) as dag:

    upload_genre_analysis_to_bigquery = PythonOperator(
        task_id="upload_genre_analysis_to_bigquery",
        python_callable=upload_genre_analysis_to_bigquery,
    )
    
    upload_genre_analysis_to_bigquery
