import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from google.cloud import bigquery
from tqdm import tqdm
from transformers import pipeline

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
    return (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")


def upload_sentiment_result_to_bq():
    """
    cleaned_reviews 테이블에서 어제 이후 리뷰에 대해 huggingface 모델로 감정 분석을 수행하고
    review_sentiment 테이블에 업로드합니다.
    """
    bq = bigquery.Client()
    yesterday = get_date()
    query = f"""
        SELECT movieNm, id, context, date
        FROM `movie_reviews.cleaned_reviews`
        WHERE DATE(date) >= DATE("{yesterday}")
    """
    df = bq.query(query).to_dataframe()

    df = df[["movieNm", "context", "platform", "date"]].dropna()
    df["context"] = df["context"].astype(str)

    # 감성분석 모델
    model_name = "sangrimlee/bert-base-multilingual-cased-nsmc"

    sentiment_model = pipeline(
        "sentiment-analysis", model=model_name, framework="pt", device=1  # pytorch
    )

    # tqdm 적용
    tqdm.pandas()

    # 긴 문장 자르기
    df["short_text"] = df["context"].str.slice(0, 200)

    # 긍정, 부정 분류
    df["sentiment"] = df["short_text"].progress_apply(
        lambda x: sentiment_model(x)[0]["label"]
    )

    result = df[["movieNm", "context", "sentiment", "platform", "date"]]

    bq.load_table_from_dataframe(
        result,
        "movie_reviews.review_sentiment",
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_APPEND", autodetect=True
        ),
    ).result()


# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="review_sentiment_analysis",
    schedule_interval="0 4 * * *",  # 매일 04:00 실행
    catchup=False,
    default_args=default_args,
) as dag:

    upload_sentiment_result_to_bq = PythonOperator(
        task_id="upload_sentiment_result_to_bq",
        python_callable=upload_sentiment_result_to_bq,
    )

    upload_sentiment_result_to_bq
