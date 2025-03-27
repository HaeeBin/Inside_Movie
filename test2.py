import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime, timedelta
import os   # airflow task에 넣을게 아니기 때문에 .env에 환경변수 설정
from dotenv import load_dotenv
import io, ast

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv('BUCKET_NAME')
DAILY_REGION_BOXOFFICE_FOLDER = 'daily_regions_boxoffice'
BOXOFFICE_API_KEY = os.getenv('BOXOFFICE_API_KEY')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

def get_date():
    '''
    API로 어제 날짜의 데이터까지만 수집 가능하므로 어제 날짜일자를 구하는 함수입니다.
    '''
    return (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")

def load_region_codes_from_gcs():
    '''
    gcs에 저장된 region.csv를 가져와 dataframe으로 변환하는 함수입니다.
    '''
    REGION_CODES_FOLDER = os.getenv('REGION_CODES_FOLDER')
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(f'{REGION_CODES_FOLDER}/region_codes.csv')
    
    csv_data = blob.download_as_text()
    df = pd.read_csv(io.StringIO(csv_data))
    
    # 문자열을 딕셔너리로 변환
    dict = df["codes"].apply(ast.literal_eval).tolist()
    
    return dict
    
def request_region_api(region_codes, target_date):
    '''
    발급받은 key와 조회하고자 하는 날짜 등 정보를 입력하여 요청해 일별 박스오피스 데이터를 가져오는 함수입니다.
    '''
    all_data = []
    
    for region in region_codes:
        region_code = str(region["fullCd"])
        region_name = str(region["korNm"])
        
        base_url = 'http://www.kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json'
        params = {
            "key" : BOXOFFICE_API_KEY,
            "targetDt" : target_date,           # 조회하고자 하는 날짜
            "wideAreaCd" : region_code,         # 지역코드 (공통코드 조회 서비스에서 10자리 숫자로 조회된 지역코드)
            # "multiMovieYn" : "N",             # 다양성 영화 : Y, 상업 영화 : N (default: 전체)
            # "repNationCd" : "K",              # 한국 영화 : K, 외국 영화 : F (default: 전체)
        }
    
        response = requests.get(base_url, params=params)
    
        if response.status_code == 200:
            data = response.json()
            all_data.append({
                "region_code" : region_code,
                "region_name" : region_name,
                "data" : data
            })
        else:
            print(f"API 요청 실패: {response.status_code} : {region_name}")
    
    return all_data

def parse_daily_region_boxoffice_data(all_data):
    '''
    가져온 박스오피스 데이터 중 필요한 정보들만 모아서 Dataframe으로 변환하는 함수입니다.
    '''     
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
            
            df = df[[
                "region_code", "region_name",
                "rank", "rankInten", "rankOldAndNew",
                "movieCd", "movieNm", "salesAmt", "salesAcc",
                "audiCnt", "audiAcc", "scrnCnt", "showCnt"
            ]]
            
            all_df.append(df)
    
    if all_df:
        result = pd.concat(all_df, ignore_index=True)   # 지역별로 데이터 수집한거 하나의 dataframe으로 합치는 과정
        return result.to_json()

def upload_to_gcs(data, target_date):
    '''
    DataFrame을 csv로 변환하여 Google Cloud Storage에 업로드하는 함수입니다.
    '''
    #target_date = get_date()
    
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # gcs 파일 경로 설정
    gcs_file_path = f"{DAILY_REGION_BOXOFFICE_FOLDER}/daily_region_box_office_{target_date}.csv"
    blob = bucket.blob(gcs_file_path)
    
    # 데이터 받아서 parse_dataframe(data)함수로 변환
    if data:
        df = pd.read_json(io.StringIO(data))
        csv_data = df.to_csv(index=False, encoding="utf-8-sig")
        blob.upload_from_string(csv_data, content_type="text/csv")
        
        print(f"{target_date} region file upload success")

if __name__=='__main__':
    
    region_codes = load_region_codes_from_gcs()
    
    for i in range(20250201, 20250229):
        all_data = request_region_api(region_codes, i)
        data = parse_daily_region_boxoffice_data(all_data)
        upload_to_gcs(data, i)