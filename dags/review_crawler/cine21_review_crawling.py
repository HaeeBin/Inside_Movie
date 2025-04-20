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
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.cloud import storage
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

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
    API로 어제 날짜의 데이터까지만 수집 가능하므로 어제 날짜일자를 구하는 함수입니다.
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

    prefix = f"{MOVIE_REVIEW_FOLDER}/cine_reviews/{movieNm}_"
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


def get_cine_review_url(movieNm):
    """
    영화를 검색하여 영화 상세 페이지 url을 반환하는 함수입니다.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    url = f"http://www.cine21.com/search/?q={movieNm}"
    driver.get(url)

    try:
        movie_url = driver.find_element(By.CLASS_NAME, "mov_list").find_element(
            By.TAG_NAME, "a"
        )
        return movie_url.get_attribute("href")
    except Exception as e:
        logging.info(f"해당 영화 상세페이지 없음 : {e}")
        return None
    finally:
        driver.quit()


def scraping_cine_review(**kwargs):
    """
    영화 상세페이지 url을 받아서 상세페이지에 들어간 다음 평론가 리뷰를 수집하고
    네트즌 리뷰를 수집하는 함수입니다.
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

    for movieNm, _ in movie_list:
        movie_url = get_cine_review_url(movieNm)  # 영화 상세 페이지 url을 가져옴
        latest_datetime = get_latest_review_datetime(movieNm)

        # 해당 영화 상세페이지가 없을 경우 pass
        if movie_url is None:
            continue

        driver.get(movie_url)
        driver.implicitly_wait(10)
        cine_reviews = []

        try:
            # 평론가 리뷰 수집
            result_html = driver.page_source
            soup = BeautifulSoup(result_html, "html.parser")

            expert_rating = soup.find("ul", "expert_rating")
            expert_reviews = expert_rating.find_all("li")

            for review in expert_reviews:
                star = (
                    review.select_one("div > div.star_area > span")
                    if review.select_one("div > div.star_area > span")
                    else None
                )
                name = review.select_one("div > div.comment_area > a > span")
                context = (
                    review.select_one("div > div.comment_area > span")
                    if review.select_one("div > div.comment_area > span")
                    else None
                )

                if name:
                    cine_reviews.append(
                        {
                            "who": "expert",
                            "name": name,
                            "context": context,
                            "star": star,
                            "review_date": None,
                        }
                    )
        except Exception as e:
            logging.info(f"평론가 리뷰 없음 : {e}")

        try:
            # 네티즌 리뷰 수집
            netizen_review_area = driver.find_element(By.ID, "netizen_review_area")
            driver.execute_script(
                "arguments[0].scrollIntoView(true);", netizen_review_area
            )  # 네티즌 리뷰 있는 공간으로 스크롤
            time.sleep(1)

            pagination = netizen_review_area.find_element(By.CLASS_NAME, "pagination")
            page_buttons = pagination.find_elements(By.CSS_SELECTOR, ".page > a")
            last_page = int(page_buttons[-1].text.strip())  # 마지막 페이지

            for page in range(1, last_page + 1):
                driver.execute_script(
                    "$('#netizen_review_area').nzreview('list', arguments[0]);", page
                )

                netizen_review_area = driver.find_element(By.ID, "netizen_review_area")
                driver.execute_script(
                    "arguments[0].scrollIntoView(true);", netizen_review_area
                )

                # 페이지 로딩 대기
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "reply_box"))
                )
                time.sleep(1)  # 추가 대기 (렌더링 안정화)

                netizen_review_list = netizen_review_area.find_element(
                    By.CLASS_NAME, "reply_box"
                )  # 리뷰 목록들
                review_items = netizen_review_list.find_elements(By.XPATH, "./li")

                try:
                    stop_crawling = False

                    for review in review_items:
                        name = review.find_element(By.CLASS_NAME, "id").text.strip()
                        date = (
                            review.find_element(By.CLASS_NAME, "date").text.split()
                            if review.find_elements(By.CLASS_NAME, "date")
                            else None
                        )
                        star = (
                            review.find_element(By.XPATH, "./div[3]/span").text.strip()
                            if review.find_elements(By.XPATH, "./div[3]/span")
                            else None
                        )
                        try:
                            context = review.find_element(
                                By.CSS_SELECTOR, "div.comment.ellipsis_3"
                            ).text.strip()
                        except Exception as e:
                            try:
                                context = review.find_element(
                                    By.CSS_SELECTOR, "div.comment"
                                ).text.strip()
                                logging.info(f"{e}. 다른 요소에 context있음")
                            except Exception as e:
                                print(f"Error {e} : Netizen context crawling")
                                context = None

                        if date:
                            review_dt = pd.to_datetime(date, errors="coerce")

                            # 이미 수집한 가장 최신 리뷰 시간보다 같거나 이전이면 크롤링 중단. 유효한 날짜인지 확인
                            if (
                                latest_datetime
                                and pd.notnull(review_dt)
                                and review_dt <= latest_datetime
                            ):
                                stop_crawling = True
                                break

                        if name:
                            cine_reviews.append(
                                {
                                    "who": "netizen",
                                    "name": name,
                                    "context": context,
                                    "star": star,
                                    "review_date": date[0],
                                }
                            )
                except Exception as e:
                    logging.info(f"Error : {e}")
                    continue

                if stop_crawling:
                    break
        except Exception as e:
            logging.info(f"네티즌 리뷰 없음 : {e}")
        finally:
            driver.quit()

            if cine_reviews:
                df = pd.DataFrame(cine_reviews)
                upload_to_gcs(df, movieNm)
            else:
                logging.info(f"{movieNm} 리뷰 없어서 csv 생성 못함")


def upload_to_gcs(df, movieNm):
    """
    DataFrame을 csv로 변환하여 Google Cloud Storage에 업로드하는 함수입니다.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    today = datetime.today().strftime("%Y%m%d")

    # gcs 파일 경로 설정
    gcs_file_path = (
        f"{MOVIE_REVIEW_FOLDER}/cine_reviews/{movieNm}_{today}_cine_reviews.csv"
    )
    blob = bucket.blob(gcs_file_path)

    # 기존 데이터가 있는 경우 다운로드하여 병합
    if blob.exists():
        csv_data = blob.download_as_text(encoding="utf-8-sig")
        existing_df = pd.read_csv(io.StringIO(csv_data))
        combined_df = pd.concat([existing_df, df], ignore_index=True)

        # 중복 제거
        combined_df.drop_duplicates(
            subset=["id", "context", "review_date"], inplace=True
        )
    else:
        combined_df = df

    # dataframe을 csv 변환 후 gcs 업로드
    csv_data = combined_df.to_csv(index=False, encoding="utf-8-sig")
    blob.upload_from_string(csv_data, content_type="text/csv")

    logging.info(f"cine21 reviews 업로드 완료. 날짜 : {movieNm}")


# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="cine_review_crawling",
    schedule_interval="0 19 * * *",  # 매일 19:00 실행
    catchup=False,
    default_args=default_args,
) as dag:

    get_unique_movie_list_from_gcs = PythonOperator(
        task_id="get_unique_movie_list_from_gcs",
        python_callable=get_unique_movie_list_from_gcs,
        provide_context=True,
    )

    scraping_cine_review = PythonOperator(
        task_id="scraping_cine_review",
        python_callable=scraping_cine_review,
        provide_context=True,
    )

    get_unique_movie_list_from_gcs >> scraping_cine_review
