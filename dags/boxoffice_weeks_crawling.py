from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable  # Airflow의 환경변수 불러오기 위함
from airflow.operators.python import PythonOperator
from google.cloud import storage

BUCKET_NAME = Variable.get("BUCKET_NAME")
DAILY_BOXOFFICE_FOLDER = Variable.get("DAILY_BOXOFFICE_FOLDER")
BOXOFFICE_API_KEY = Variable.get("BOXOFFICE_API_KEY")


def get_date():
    """ """
    return (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")


def request_url(**kwargs):
    """
    발급받은 key와 조회하고자 하는 날짜 등 정보를 입력하여 요청해 일별 박스오피스 데이터를 가져오는 함수입니다.
    """
    target_date = get_date()

    base_url = "http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    params = {
        "key": BOXOFFICE_API_KEY,
        "targetDt": target_date,  # 조회하고자 하는 날짜
        # "multiMovieYn" : "N",             # 다양성 영화 : Y, 상업 영화 : N (default: 전체)
        # "repNationCd" : "K",              # 한국 영화 : K, 외국 영화 : F (default: 전체)
        # "wideAreaCd" : "0105000000"       # 지역코드 (공통코드 조회 서비스에서 10자리 숫자로 조회된 지역코드)
    }

    response = requests.get(base_url, params=params)

    if response.status_code == 200:
        kwargs["ti"].xcom_push(key="daily_box_office_data", value=response.json())
    else:
        raise Exception(f"API 요청 실패: {response.status_code}")


def parse_dataframe(data):
    """
    가져온 박스오피스 데이터 중 필요한 정보들만 모아서 Dataframe으로 변환하는 함수입니다.
    """
    box_office_list = data["boxOfficeResult"]["dailyBoxOfficeList"]

    df = pd.DataFrame(box_office_list)

    df = df[
        [
            "rank",
            "rankInten",
            "rankOldAndNew",
            "movieCd",
            "movieNm",
            "openDt",
            "salesAmt",
            "salesShare",
            "salesInten",
            "salesChange",
            "salesAcc",
            "audiCnt",
            "audiInten",
            "audiChange",
            "audiAcc",
            "scrnCnt",
            "showCnt",
        ]
    ]

    return df


def upload_to_gcs(**kwargs):
    """
    DataFrame을 csv로 변환하여 Google Cloud Storage에 업로드하는 함수입니다.
    """
    target_date = get_date()

    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # gcs 파일 경로 설정
    gcs_file_path = f"{DAILY_BOXOFFICE_FOLDER}/daily_box_office_{target_date}.csv"
    blob = bucket.blob(gcs_file_path)

    # 데이터 받아서 parse_dataframe(data)함수로 변환
    ti = kwargs["ti"]
    data = ti.xcom_pull(task_ids="request_url", key="daily_box_office_data")

    df = parse_dataframe(data)

    # dataframe을 csv 변환 후 gcs 업로드
    csv_data = df.to_csv(index=False, encoding="utf-8-sig")
    blob.upload_from_string(csv_data, content_type="text/csv")

    print(f"daily boxOffice 업로드 완료. 날짜 : {target_date}")


# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 20),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="daily_box_office_crawling",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
) as dag:

    request_url = PythonOperator(
        task_id="request_url", python_callable=request_url, provide_context=True
    )

    upload_to_gcs = PythonOperator(
        task_id="upload_to_gcs", python_callable=upload_to_gcs, provide_context=True
    )

    request_url >> upload_to_gcs
