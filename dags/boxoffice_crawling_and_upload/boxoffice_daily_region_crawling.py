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
    return (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")


def load_region_codes_from_gcs(**kwargs):
    """
    gcs에 저장된 region.csv를 가져와 dataframe으로 변환하는 함수입니다.
    """
    REGION_CODES_FOLDER = os.getenv("REGION_CODES_FOLDER")

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"{REGION_CODES_FOLDER}/region_codes.csv")

    csv_data = blob.download_as_text()
    df = pd.read_csv(pd.io.StringIO(csv_data))

    kwargs["ti"].xcom_push(
        key="load_region_codes_from_gcs", value=df.to_dict("records")
    )


def request_region_api(**kwargs):
    """
    key, 날짜 등의 필수 정보들과 지역 코드를 입력해 특정 지역의 일별 박스오피스 데이터를 가져오는 함수입니다.
    """
    target_date = get_date()

    region_codes = kwargs["ti"].xcom_pull(
        task_ids="load_region_codes_from_gcs", key="load_region_codes_from_gcs"
    )

    all_data = []
    for region in region_codes:
        region_code = str(region["fullCd"])
        region_name = str(region["korNm"])

        base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
        params = {
            "key": BOXOFFICE_API_KEY,
            "targetDt": target_date,  # 조회하고자 하는 날짜
            "wideAreaCd": region_code,  # 지역코드 (공통코드 조회 서비스에서 10자리 숫자로 조회된 지역코드)
            # "multiMovieYn" : "N",             # 다양성 영화 : Y, 상업 영화 : N (default: 전체)
            # "repNationCd" : "K",              # 한국 영화 : K, 외국 영화 : F (default: 전체)
        }

        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            data = response.json()
            all_data.append(
                {"region_code": region_code, "region_name": region_name, "data": data}
            )
        else:
            print(f"API 요청 실패: {response.status_code} : {region_name}")

    kwargs["ti"].xcom_push(key="daily_region_box_office_data", value=all_data)


def parse_daily_region_boxoffice_data(**kwargs):
    """
    지역별 박스오피스 데이터를 모아놓은 데이터를 필요한 정보만 남기고 dataframe으로 변환.
    """
    all_data = kwargs["ti"].xcom_pull(
        task_ids="request_region_api", key="daily_region_box_office_data"
    )

    all_df = []

    for d in all_data:
        region_code = d["region_code"]
        region_name = d["region_name"]
        data = d["data"]

        box_office_list = data.get("boxOfficeResult", {}).get("dailyBoxOfficeList", [])

        if box_office_list:
            df = pd.DataFrame(box_office_list)

            df["region_code"] = region_code
            df["region_name"] = region_name

            df = df[
                [
                    "region_code",
                    "region_name",
                    "rank",
                    "rankInten",
                    "rankOldAndNew",
                    "movieCd",
                    "movieNm",
                    "salesAmt",
                    "salesAcc",
                    "audiCnt",
                    "audiAcc",
                    "scrnCnt",
                    "showCnt",
                ]
            ]

            all_df.append(df)

    if all_df:
        result = pd.concat(
            all_df, ignore_index=True
        )  # 지역별로 데이터 수집한거 하나의 dataframe으로 합치는 과정
        kwargs["ti"].xcom_push(
            key="parse_region_boxoffice_data", value=result.to_json()
        )


def upload_to_gcs(**kwargs):
    """
    DataFrame을 csv로 변환하여 Google Cloud Storage에 업로드하는 함수입니다.
    """
    target_date = get_date()

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # gcs 파일 경로 설정
    gcs_file_path = (
        f"{DAILY_REGION_BOXOFFICE_FOLDER}/daily_region_box_office_{target_date}.csv"
    )
    blob = bucket.blob(gcs_file_path)

    # parse_daily_region_boxoffice_data 메소드에서 데이터 가져오기
    data = kwargs["ti"].xcom_pull(
        task_ids="parse_daily_region_boxoffice_data", key="parse_region_boxoffice_data"
    )

    # dataframe을 csv 변환 후 gcs 업로드
    if data:
        df = pd.read_json(data)
        csv_data = df.to_csv(index=False, encoding="utf-8-sig")
        blob.upload_from_string(csv_data, content_type="text/csv")

        print(f"{target_date} region file upload success")
        
def upload_to_bigquery(**kwargs):
    """
    DataFrame을 전처리 후 BigQuery에 업로드하는 함수입니다.
    """
    target_date = get_date()
    
    data = kwargs["ti"].xcom_pull(
        task_ids="parse_daily_region_boxoffice_data", key="parse_region_boxoffice_data"
    )
    
    df = pd.read_json(data)
    df["boxoffice_date"] = pd.to_datetime(target_date, format="%Y%m%d")
        
    # 컬럼 타입 변환
    cast_columns = {
        "rank": "Int64",
        "rankInten": "Int64",
        "salesAmt": "Int64",
        "salesAcc": "Int64",
        "audiCnt": "Int64",
        "audiAcc": "Int64",
        "scrnCnt": "Int64",
        "showCnt": "Int64",
        "prdtYear": "Int64",
        "showTm": "Int64",
    }
        
    for col, dtype in cast_columns.items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    # BigQuery 업로드
    bq = bigquery.Client()
    bq.load_table_from_dataframe(
        df,
        "movie_boxoffice.merged_daily_region_boxoffice",
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    ).result()
    
    
# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="daily_region_box_office_crawling",
    schedule_interval="30 18 * * *",  # 매일 18:30 실행
    catchup=False,
    default_args=default_args,
) as dag:

    load_region_codes_from_gcs = PythonOperator(
        task_id="load_region_codes_from_gcs",
        python_callable=load_region_codes_from_gcs,
    )

    request_region_api = PythonOperator(
        task_id="request_region_api",
        python_callable=request_region_api,
        provide_context=True,
    )

    parse_daily_region_boxoffice_data = PythonOperator(
        task_id="parse_daily_region_boxoffice_data",
        python_callable=parse_daily_region_boxoffice_data,
        provide_context=True,
    )

    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs", python_callable=upload_to_gcs, provide_context=True
    )
    
    upload_to_bigquery = PythonOperator(
        task_id="upload_to_bigquery",
        python_callable=upload_to_bigquery,
        provide_context=True
    )

    (
        load_region_codes_from_gcs
        >> request_region_api
        >> parse_daily_region_boxoffice_data
        >> upload_to_gcs
        >> upload_to_bigquery
    )
