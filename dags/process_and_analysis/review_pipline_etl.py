import json
import os
from datetime import datetime, timedelta

from airflow.models import Variable
from dotenv import load_dotenv
from google.cloud import storage

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

    # all_df = []
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    target_date = get_date()

    for platform in platforms:
        blobs = bucket.list_blobs(prefix=f"{MOVIE_REVIEW_FOLDER}/{platform}_reviews/")

        for blob in blobs:
            if not blob.name.endswith(f"{target_date}_{platform}_reviews.csv"):
                continue
