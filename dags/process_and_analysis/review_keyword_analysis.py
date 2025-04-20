import json
import os, io, ast
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery, storage

from airflow.models import Variable
from dotenv import load_dotenv

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

def upload_movie_keywords_to_bigquery():
    """
    cleaned_reviews 테이블에서 어제 이후의 리뷰를 대상으로 워드클라우드를 만들기 위한 전처리 후
    movie_wordcloud 테이블에 업로드하는 함수입니다.
    """
    bq = bigquery.Client()
    yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    query = f"""
        SELECT movieNm, context
        FROM `movie_reviews.cleaned_reviews`
        WHERE DATE(date) >= DATE("{yesterday}")
    """
    df = bq.query(query).to_dataframe()
    
    df["clean"] = df["context"].str.replace(r"[^\uac00-\ud7a3\s]", "", regex=True).str.lower()
    df = df.dropna(subset=["clean"])
    df = df.assign(word=df["clean"].str.split()).explode("word")
    
    stopwords = set(["영화", "정말", "너무", "근데", "그리고"])
    df = df[df["word"].notna() & ~df["word"].isin(stopwords) & (df["word"].str.len() >= 2)]
    
    result = df.groupby(["movieNm", "word"]).size().reset_index(name="count")
    result.rename(columns={"word": "keyword"}, inplace=True)
    
    bq.load_table_from_dataframe(
        df,
        "movie_reviews.movie_word_frequencies",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    ).result()
    
def upload_megabox_keywords_to_bigquery():
    """
    megabox 리뷰의 추천 키워드(recommend_tags)를 분석하여
    megabox_keywords 테이블에 업로드하는 함수입니다.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    target_date = get_date()
    
    blobs = bucket.list_blobs(prefix=f"{MOVIE_REVIEW_FOLDER}/megabox_reviews/")
    all_rows = []
    
    for blob in blobs:
        if not blob.name.endswith(f"{target_date}_megabox_reviews.csv"):
            continue
        df = pd.read_csv(io.StringIO(blob.download_as_text("utf-8-sig")))
        df["movieNm"] = blob.name.split("/")[-1].split("_")[0].replace("%20", " ")
        df["recommend_tags"] = df["recommend_tags"].apply(lambda x: ast.literal_eval(x) if pd.notnull(x) else [])
        df = df.explode("recommend_tags").dropna()
        df.rename(columns={"recommend_tags": "keyword"}, inplace=True)
        all_rows.append(df[["movieNm", "keyword"]])
    
    if all_rows:
        result = pd.concat(all_rows).groupby(["movieNm", "keyword"]).size().reset_index(name="count")
        bigquery.Client().load_table_from_dataframe(
            result, 
            "movie_reviews.megabox_keywords", 
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        ).result()
        
# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="review_keyword_analysis",
    schedule_interval="0 3 * * *",  # 매일 03:00 실행
    catchup=False,
    default_args=default_args,
) as dag:

    upload_movie_keywords_to_bigquery = PythonOperator(
        task_id="upload_movie_keywords_to_bigquery",
        python_callable=upload_movie_keywords_to_bigquery
    )

    upload_megabox_keywords_to_bigquery = PythonOperator(
        task_id="upload_megabox_keywords_to_bigquery",
        python_callable=upload_megabox_keywords_to_bigquery
    )

    upload_movie_keywords_to_bigquery >> upload_megabox_keywords_to_bigquery