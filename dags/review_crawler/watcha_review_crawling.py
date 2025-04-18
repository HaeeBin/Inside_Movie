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
from selenium.webdriver.common.action_chains import ActionChains
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


def get_watcha_review_url(movieNm, openDt):
    """
    영화 검색을 하여 영화 상세 페이지 url을 반환하는 함수입니다.
    검색하여 나온 영화 리스트 중 맨 처음 영화의 상세 페이지로 하면 다른 영화의 상세페이지인 경우가 있어서 개봉일을 비교하여
    정확한 url을 반환하도록 합니다.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    try:
        url = f"https://pedia.watcha.com/ko-KR/search?query={movieNm}"
        driver.get(url)

        # 검색 결과 대기
        driver.implicitly_wait(10)
        clicked = False
        movies = driver.find_elements(By.CSS_SELECTOR, "li.zK9dEEA5")
        for movie in movies:
            title_tag = movie.find_element(By.TAG_NAME, "a")
            title = title_tag.get_attribute("title")
            open_year = (
                movie.find_element(
                    By.CSS_SELECTOR, "a > div.bVUVO8nJ > div.qz_zPMlN.RiDHrQhO"
                )
                .text.split("・")[0]
                .strip()
            )

            if (movieNm == title) and (open_year == openDt):
                title_tag.click()
                clicked = True
                break

        if not clicked:
            a_tags = driver.find_elements(By.CSS_SELECTOR, "li.zK9dEEA5 a")
            for a in a_tags:
                try:
                    if len(a.find_elements(By.TAG_NAME, "img")) > 0:
                        ActionChains(driver).move_to_element(a).perform()
                        a.click()
                        break
                except Exception as e:
                    logging.info(f"Error : {e}")
                    continue

            time.sleep(1)

        try:
            locator = driver.find_element(
                By.CSS_SELECTOR, "div.oBazLiES section:nth-child(2) header a"
            )
            ActionChains(driver).move_to_element(locator).perform()
            time.sleep(0.5)
            return locator.get_attribute("href")
        except Exception as e:
            logging.info(f"Error : {e}")
            return None

    except Exception as e:
        logging.info(f"Error: {e}")
        return None
    finally:
        driver.quit()


def scroll_to_bottom(driver, max_attempts=100, wait_time=1.5):
    """
    페이지 스크롤해서 더 이상 새로운 리뷰가 로드되지 않을 때까지 스크롤하는 함수입니다.
    """
    prev_height = driver.execute_script("return document.body.scrollHeight")
    attempts = 0

    while attempts < max_attempts:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(wait_time)

        new_height = driver.execute_script("return document.body.scrollHeight")

        if new_height == prev_height:
            logging.info("더 이상 로딩된 콘텐츠 없음")
            break

        prev_height = new_height
        attempts += 1

    if attempts >= max_attempts:
        logging.info("최대 스크롤 횟수 초과")


def scraping_watcha_reviews(**kwargs):
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
        # 개봉년도
        open_year = openDt.split("-")[0]

        # 해당 영화 상세페이지가 없을 경우 pass
        movie_review_url = get_watcha_review_url(movieNm, open_year)
        if not movie_review_url:
            logging.info(f"'{movieNm}'에 대한 리뷰 링크 없음")
            continue

        watcha_reviews = []

        try:
            driver.get(movie_review_url)
            scroll_to_bottom(driver)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "div.GvYMMD2G.sCUeLl5F")
                )
            )
            reviews = driver.find_elements(By.CSS_SELECTOR, "div.GvYMMD2G.sCUeLl5F")

            for review in reviews:
                try:
                    name = review.find_element(
                        By.CSS_SELECTOR,
                        "div.GqJea6zF > div.EeIUbbQr.qfFKTvc9 > a > div.TfbnQJp1.qfFKTvc9",
                    ).text.strip()
                    try:
                        star = review.find_element(
                            By.CSS_SELECTOR,
                            "div.GqJea6zF > div.H7gLZE2m.KYbG4TeN > span",
                        ).text.strip()
                    except Exception as e:
                        print(f"{e}")
                        star = "None"

                    # 스포일러가 있을 경우 리뷰가 나오지 않고 리뷰 보기 버튼을 눌러야 리뷰가 나옴.
                    try:
                        context_button = review.find_element(
                            By.CSS_SELECTOR,
                            "div.tCqzOhhS.Jgl_VtOt._4_jash0F > a > div > span > button",
                        )
                        context_button.click()
                    except Exception as e:
                        print(f"{e}")

                    context = review.find_element(
                        By.CSS_SELECTOR,
                        "div.tCqzOhhS.Jgl_VtOt._4_jash0F > a > div > div",
                    ).text.strip()

                    if name:
                        watcha_reviews.append(
                            {"id": name, "star": star, "context": context}
                        )
                except Exception as e:
                    print(f"리뷰 수집 실패: {e}")

            if watcha_reviews:
                df = pd.DataFrame(watcha_reviews)
                upload_to_gcs(df, movieNm)
            else:
                print(f"{movieNm} 리뷰 없음")

        finally:
            driver.quit()


def upload_to_gcs(df, movieNm):
    """
    DataFrame을 csv로 변환하여 Google Cloud Storage에 업로드하는 함수입니다.
    """
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # gcs 파일 경로 설정
    gcs_file_path = f"{MOVIE_REVIEW_FOLDER}/watcha_reviews/{movieNm}_watcha_reviews.csv"
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

    logging.info(f"watcha reviews 업로드 완료. 날짜 : {movieNm}")


# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 30),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="watcha_review_crawling",
    schedule_interval="0 22 * * *",  # 매일 22:00 실행
    catchup=False,
    default_args=default_args,
) as dag:

    get_unique_movie_list_from_gcs = PythonOperator(
        task_id="get_unique_movie_list_from_gcs",
        python_callable=get_unique_movie_list_from_gcs,
        provide_context=True,
    )

    scraping_watcha_reviews = PythonOperator(
        task_id="scraping_watcha_reviews",
        python_callable=scraping_watcha_reviews,
        provide_context=True,
    )

    get_unique_movie_list_from_gcs >> scraping_watcha_reviews
