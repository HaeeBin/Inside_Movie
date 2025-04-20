import os  # airflow task에 넣을게 아니기 때문에 .env에 환경변수 설정

import pandas as pd
import requests
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv("BUCKET_NAME")
REGION_CODES_FOLDER = os.getenv("REGION_CODES_FOLDER")
BOXOFFICE_API_KEY = os.getenv("BOXOFFICE_API_KEY")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS


def request_region_api():
    """
    API로 지역코드를 조회하는 함수입니다.
    """
    base_url = (
        "http://www.kobis.or.kr/kobisopenapi/webservice/rest/code/searchCodeList.json"
    )
    params = {
        "key": BOXOFFICE_API_KEY,
        "comCode": "0105000000",  # 지역코드 조회 할 수 있응 상위 코드
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("지역코드 조회 실패")
        return None


def parse_region_code_data(data):
    """
    가져온 지역코드를 Dataframe으로 변환하는 함수입니다.
    """
    df = pd.DataFrame(data)

    return df


def upload_to_gcs(df):
    """
    dataframe을 csv로 바꿔서 gcs에 적재하는 함수입니다.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    gcs_file_path = f"{REGION_CODES_FOLDER}/region_codes.csv"
    blob = bucket.blob(gcs_file_path)

    csv_data = df.to_csv(index=False, encoding="utf-8-sig")
    blob.upload_from_string(csv_data, content_type="text/csv")

    print("지역코드 gcs 업로드 완료")


if __name__ == "__main__":
    region_data = request_region_api()
    df = parse_region_code_data(region_data)
    upload_to_gcs(df)
