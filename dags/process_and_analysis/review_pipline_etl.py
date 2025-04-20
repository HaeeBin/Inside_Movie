import json
import os, io
import pandas as pd
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG

from airflow.models import Variable
from dotenv import load_dotenv
from google.cloud import storage, bigquery

load_dotenv()

BUCKET_NAME = Variable.get("BUCKET_NAME")
MOVIE_REVIEW_FOLDER = Variable.get("MOVIE_REVIEW_FOLDER")
sa_key_dict = Variable.get("GOOGLE_APPLICATION_CREDENTIALS", deserialize_json=True)

with open("/tmp/gcp-sa-key.json", "w") as f:
    json.dump(sa_key_dict, f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp-sa-key.json"


def get_date():
    """
    어제 날짜일자를 구하는 함수입니다.
    """
    return (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")


def upload_merged_reviews_to_bigquery():
    """
    어제 날찌의 리뷰 파일만 GCS에서 읽어와 플랫폼별로 전처리를 수행하고
    처음에 만들어놓은 merged_reviews라는 BigQuery 테이블에 데이터를 추가하는 함수입니다.
    """
    platforms = ["watcha", "megabox", "cgv", "cine"]

    all_df = []
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    target_date = get_date()

    for platform in platforms:
        blobs = bucket.list_blobs(prefix=f"{MOVIE_REVIEW_FOLDER}/{platform}_reviews/")

        for blob in blobs:
            if not blob.name.endswith(f"{target_date}_{platform}_reviews.csv"):
                continue
            
            csv = blob.download_as_text(encoding='utf-8-sig')
            df = pd.read_csv(io.StringIO(csv))
            df.columns = df.columns.str.lower()
            df.rename(columns={"name":"id", "review_date":"date", "rating":"star"}, inplace=True)
            df["platform"] = platform
            
            filename = blob.name.split("/")[-1]
            movieNm = filename.replace(f"_{target_date}_{platform}_reviews.csv", "")
            movieNm = (
                movieNm.replace("%20", " ")
                       .replace("%5B", "[")
                       .replace("%5D", "]")
                       .replace("%2C", ",")
                       .replace("%26", "&")
            )
            df["movieNm"] = movieNm
            
            all_df.append(df[["movieNm", "id", "context", "star", "date", "platform"]])
    
    if all_df:
        combined = pd.concat(all_df, ignore_index=True).drop_duplicates()
        bq = bigquery.Client()
        bq.load_table_from_dataframe(
            combined,
            "movie_dashboard.review_data",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        ).result()
        
def upload_cleaned_reviews_to_bigquery():
    """
    review_data 테이블에서 분석에 적합한 리뷰만 필터링 후
    cleaned_reviews 테이블에 append 업로드하는 함수입니다.
    """
    bq = bigquery.Client()
    df = bq.query("SELECT * FROM `movie_dashboard.review_data`").to_dataframe()
    
    df["context"] = df["context"].astype(str).str.strip()
    # null, 빈 문자열, 10글자 미만 걸러내기 => 의미없을 가능성
    df = df[df["context"].notna() & (df["context"] != "") & (df["context"].str.len() >= 10)]
    df["star"] = pd.to_numeric(df["star"], errors="coerce")
    df = df[(df["star"].isna()) | ((df["star"] >= 0) & (df["star"] <= 5))]
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    
    df.drop_duplicates(subset=["movieNm", "id", "context", "date"], inplace=True)
    
    bq.load_table_from_dataframe(
        df,
        "movie_reviews.cleaned_reviews",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    ).result()

# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="review_pipeline_etl",
    schedule_interval="0 2 * * *",  # 매일 02:00 실행
    catchup=False,
    default_args=default_args,
) as dag:

    upload_merged_reviews_to_bigquery = PythonOperator(
        task_id="upload_merged_reviews_to_bigquery",
        python_callable=upload_merged_reviews_to_bigquery
    )

    upload_cleaned_reviews_to_bigquery = PythonOperator(
        task_id="upload_cleaned_reviews_to_bigquery",
        python_callable=upload_cleaned_reviews_to_bigquery
    )

    upload_merged_reviews_to_bigquery >> upload_cleaned_reviews_to_bigquery
