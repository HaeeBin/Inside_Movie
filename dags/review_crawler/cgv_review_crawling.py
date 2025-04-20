import io
import json
import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

load_dotenv()

BUCKET_NAME = Variable.get("BUCKET_NAME")
MOVIE_REVIEW_FOLDER = Variable.get("MOVIE_REVIEW_FOLDER")
DAILY_BOXOFFICE_FOLDER = Variable.get("DAILY_BOXOFFICE_FOLDER")
DAILY_REGION_BOXOFFICE_FOLDER = Variable.get("DAILY_REGION_BOXOFFICE_FOLDER")
sa_key_dict = Variable.get("GOOGLE_APPLICATION_CREDENTIALS", deserialize_json=True)

with open("/tmp/gcp-sa-key.json", "w") as f:
    json.dump(sa_key_dict, f)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/tmp/gcp-sa-key.json"


def get_date():
    """
    어제 날짜일자를 구하는 함수입니다.
    """
    return (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")


def get_unique_movie_list_from_gcs(**kwargs):
    """
    어제 날짜의 박스오피스 순위에 있는 영화들의 csv파일을 가져옵니다.
    그 csv파일에서 개봉일과 영화제목만 추출해서 저장하여 반환하는 함수입니다.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    target_date = get_date()  # 어제 날짜 담긴 변수
    movie_set = set()  # 중복 없이 영화 리스트 담기 위함

    def process_boxoffice(folder):
        blobs = bucket.list_blobs(prefix=folder)
        for blob in blobs:
            if blob.name.endswith(f"{target_date}.csv"):  # 어제 날짜의 boxoffice파일
                csv_data = blob.download_as_text(encoding="utf-8-sig")
                df = pd.read_csv(io.StringIO(csv_data))

                if "movieNm" in df.columns and "openDt" in df.columns:
                    for _, row in df.iterrows():
                        movieNm = str(row["movieNm"]).strip()
                        openDt = str(row["openDt"]).strip()
                        movie_set.add((movieNm, openDt))

    process_boxoffice(DAILY_BOXOFFICE_FOLDER)  # 일별 박스오피스 조회
    process_boxoffice(DAILY_REGION_BOXOFFICE_FOLDER)  # 지역별 박스오피스 조회

    kwargs["ti"].xcom_push(key="movie_list", value=movie_set)


def get_latest_review_datetime(movieNm):
    """
    gcs에 이미 리뷰 수집된 csv 파일이 있다면 리뷰 작성 날짜를 추출해
    제일 최신의 리뷰 날짜를 구하는 함수입니다.

    수집한 리뷰를 또 수집하는 작업을 하지 않기 위해 수행하는 함수입니다.
        - 리뷰가 최신 순으로 수집되기 때문에 날짜 비교해서 수집 작업을 빠르게 끝낼 수 있습니다.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    prefix = f"{MOVIE_REVIEW_FOLDER}/cgv_reviews/{movieNm}_"
    blobs = bucket.list_blobs(prefix=prefix)

    latest_datetime = None

    for blob in blobs:
        if not blob.name.endswith(".csv"):
            continue

        csv_data = blob.download_as_text(encoding="utf-8-sig")
        df = pd.read_csv(io.StringIO(csv_data))

        if "review_date" not in df.columns or df.empty:
            df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
            latest = df["review_date"].max()

            if pd.notnull(latest) and (
                latest_datetime is None or latest > latest_datetime
            ):
                latest_datetime = latest

    return latest_datetime


def get_cgv_review_url(movieNm, openDt):
    """
    영화를 검색하여 영화 상세 페이지 url을 반환하는 함수입니다.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    url = f"http://www.cgv.co.kr/search/?query={movieNm}"
    driver.get(url)

    try:
        movie_url = driver.find_element(By.ID, "searchMovieResult").find_element(
            By.CLASS_NAME, "img_wrap"
        )
        return movie_url.get_attribute("href")
    except Exception as e:
        print(f"해당 영화 없음. : {e}")
        return None
    finally:
        driver.quit()


def scraping_cgv_reviews(**kwargs):
    """
    영화 상세페이지에 들어가서 영화 리뷰 탭을 들어갑니다.
    그 후 리뷰를 크롤링해서 dataframe으로 저장하여 gcs에 업로드하는 함수입니다.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    movie_list = kwargs["ti"].xcom_pull(
        task_ids="get_unique_movie_list_from_gcs", key="movie_list"
    )

    for movieNm, openDt in movie_list:
        movie_url = get_cgv_review_url(movieNm, openDt)  # 영화 상세 페이지 url을 가져옴

        # 해당 영화 상세페이지가 없을 경우 pass
        if movie_url is None:
            continue

        latest_datetime = get_latest_review_datetime(movieNm)
        driver.get(movie_url)
        cgv_reviews = []

        try:
            # 영화 평점/리뷰로 이동할 수 있는 탭
            review_tab = driver.find_element(By.ID, "tabComent").find_element(
                By.XPATH, "a"
            )
            driver.get(review_tab.get_attribute("href"))
        except Exception as e:
            logging.info(f"리뷰 탭이 없음. {e}")
            continue

        # 현재 페이지 1 페이지
        current_page = 1

        while True:
            review_list = driver.find_element(By.ID, "movie_point_list_container")
            review_items = review_list.find_elements(
                By.XPATH, "./li"
            )  # //li : 모든 li (자식, 손자 태그 포함), ./li : 자식 li

            if not review_items:  # 리뷰가 없을 때
                break

            stop_crawling = False

            for i in range(len(review_items)):
                try:
                    review_list = driver.find_element(
                        By.ID, "movie_point_list_container"
                    )
                    review_items = driver.find_elements(By.XPATH, "./li")
                    review = review_items[i]

                    id = review.find_element(By.XPATH, "//ul/li[1]").text.strip()
                    date = review.find_element(
                        By.XPATH, ".//ul/li[2]/span[1]"
                    ).text.strip()
                    context = review.find_element(By.XPATH, ".//div[3]/p").text.strip()

                    review_dt = pd.to_datetime(date, errors="coerce")

                    # 이미 수집한 가장 최신 리뷰 시간보다 같거나 이전이면 크롤링 중단. 유효한 날찌인지 확인
                    if (
                        latest_datetime
                        and pd.notnull(review_dt)
                        and review_dt <= latest_datetime
                    ):
                        stop_crawling = True
                        break

                    cgv_reviews.append({"id": id, "context": context, "date": date})
                except Exception as e:
                    logging.info(f"리뷰 수집안됨. {e}")
                    continue

            if stop_crawling:
                break

            # 페이지 넘기기
            try:
                next_page = current_page + 1
                paging = driver.find_element(By.ID, "paging_point")
                next_page_button = paging.find_element(
                    By.XPATH, f'.//a[@href="#{next_page}"]'
                )
                next_page_button.click()  # 다음 페이지 이동
                time.sleep(0.5)
                current_page += 1
            except Exception:
                try:
                    # 다음 10 페이지 버튼 누르기 (ex. 10페이지일 경우 11페이지로 넘어가기 위해서는 다음 10페이지 버튼을 눌러야 넘어감)
                    next_10_button = driver.find_element(
                        By.CLASS_NAME, "btn-paging.next"
                    )
                    next_10_button.click()
                    time.sleep(0.5)
                    current_page += 1
                except Exception as e:
                    logging.info(f"리뷰 수집 끝 : {e}")
                    break

        if cgv_reviews:
            df = pd.DataFrame(cgv_reviews)
            upload_to_gcs(df, movieNm)
        else:
            logging.info(f"{movieNm}의 리뷰는 없음")

    driver.quit()


def upload_to_gcs(df, movieNm):
    """
    DataFrame을 csv로 변환하여 Google Cloud Storage에 업로드하는 함수입니다.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    today = datetime.today().strftime("%Y%m%d")

    # gcs 파일 경로 설정
    gcs_file_path = (
        f"{MOVIE_REVIEW_FOLDER}/cgv_reviews/{movieNm}_{today}_cgv_reviews.csv"
    )
    blob = bucket.blob(gcs_file_path)

    # 기존 데이터가 있는 경우 다운로드하여 병합
    if blob.exists():
        csv_data = blob.download_as_text(encoding="utf-8-sig")
        existing_df = pd.read_csv(io.StringIO(csv_data))
        combined_df = pd.concat([existing_df, df], ignore_index=True)

        # 중복 제거
        combined_df.drop_duplicates(subset=["id", "context", "date"], inplace=True)
    else:
        combined_df = df

    # dataframe을 csv 변환 후 gcs 업로드
    csv_data = combined_df.to_csv(index=False, encoding="utf-8-sig")
    blob.upload_from_string(csv_data, content_type="text/csv")

    logging.info(f"cgv reviews 업로드 완료. 날짜 : {movieNm}")


# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="cgv_review_crawling",
    schedule_interval="30 18 * * *",  # 매일 18:30 실행
    catchup=False,
    default_args=default_args,
) as dag:

    get_unique_movie_list_from_gcs = PythonOperator(
        task_id="get_unique_movie_list_from_gcs",
        python_callable=get_unique_movie_list_from_gcs,
        provide_context=True,
    )

    scraping_cgv_reviews = PythonOperator(
        task_id="scraping_cgv_reviews",
        python_callable=scraping_cgv_reviews,
        provide_context=True,
    )

    get_unique_movie_list_from_gcs >> scraping_cgv_reviews
