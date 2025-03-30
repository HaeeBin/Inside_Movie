import io
import logging
import os
import random
import re
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

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = Variable.get("BUCKET_NAME")
MOVIE_REVIEW_FOLDER = Variable.get("MOVIE_REVIEW_FOLDER")
DAILY_BOXOFFICE_FOLDER = Variable.get("DAILY_BOXOFFICE_FOLDER")
DAILY_REGION_BOXOFFICE_FOLDER = Variable.get("DAILY_REGION_BOXOFFICE_FOLDER")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS


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

    gcs_file_path = (
        f"{MOVIE_REVIEW_FOLDER}/megabox_reviews/{movieNm}_megabox_reviews.csv"
    )
    blob = bucket.blob(gcs_file_path)

    # 리뷰 수집된 파일이 없으면 모든 리뷰 수집하기 위해 None return
    if not blob.exists():
        return None

    csv_data = blob.download_as_text(encoding="utf-8-sig")
    df = pd.read_csv(io.StringIO(csv_data))

    if "review_date" not in df.columns or df.empty:
        return None

    # 가장 최근 값 구하기
    df["review_date"] = pd.to_datetime(df["review_date"], errors="coerce")
    return df["review_date"].max()


def convert_datetime(text):
    """
    메가박스 리뷰는 50분전, 하루전, 3일전 등의 시간으로 나타나있는 경우가 많기 때문에 날짜 형식을 맞춰서 값을 저장하기 위해
    날짜 형식을 바꾸는 함수입니다.
    """
    now = datetime.now()

    if re.match(r"\d{4}\.\d{2}\.\d{2}", text):
        return text

    elif "분전" in text:
        minutes = int(re.search(r"(\d+)", text).group(1))
        converted = now - timedelta(minutes=minutes)

    elif "시간전" in text:
        hours = int(re.search(r"(\d+)", text).group(1))
        converted = now - timedelta(hours=hours)

    elif "일전" in text:
        days = int(re.search(r"(\d+)", text).group(1))
        converted = now - timedelta(days=days)

    elif "방금" in text:
        converted = now

    else:
        return None

    return converted.strftime("%Y.%m.%d")


def scraping_megabox_reviews(**kwargs):
    """
    영화 검색 후 박스오피스탭에 있는 영화 리스트에서 영화 버튼을 클릭하여 상세페이지로 들어가
    리뷰를 수집하는 함수입니다.
    """
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(options=options)
    movie_list = kwargs["ti"].xcom_pull(
        task_ids="get_unique_movie_list_from_gcs", key="movie_list"
    )

    for movieNm, _ in movie_list:
        url = f"https://www.megabox.co.kr/movie?searchText={movieNm}"

        driver.get(url)
        driver.implicitly_wait(10)

        megabox_reviews = []
        latest_datetime = get_latest_review_datetime(movieNm)

        try:
            # 영화 리스트에서 영화 상세페이지 들어갈 수 있는 버튼 찾기
            movie_button = driver.find_element(By.CSS_SELECTOR, "a.wrap.movieBtn")

            # 클릭
            ActionChains(driver).move_to_element(movie_button).click().perform()
            time.sleep(1)

            review_tab = driver.find_element(
                By.CSS_SELECTOR, 'a[title="실관람평 탭으로 이동"]'
            )
            driver.execute_script("arguments[0].acrollIntoView(true);", review_tab)
            review_tab.click()
            driver.implicitly_wait(10)

            while True:
                try:
                    # 리뷰 리스트 로딩 대기
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located(
                            (
                                By.CSS_SELECTOR,
                                "div.movie-idv-story ul > li.type01.oneContentTag",
                            )
                        )
                    )

                    time.sleep(0.5)

                    review_items = driver.find_elements(
                        By.CSS_SELECTOR,
                        "div.movie-idv-story ul > li.type01.oneContentTag",
                    )
                    if not review_items:
                        print("더 이상 리뷰 없음. 종료.")
                        break

                    stop_crawling = False

                    for review in review_items:
                        try:
                            id = review.find_element(
                                By.CLASS_NAME, "user-id"
                            ).text.strip()
                            context = (
                                review.find_element(
                                    By.CLASS_NAME, "story-txt"
                                ).text.strip()
                                if review.find_element(By.CLASS_NAME, "story-txt")
                                else None
                            )
                            rating = (
                                review.find_element(
                                    By.CSS_SELECTOR, "div.story-point > span"
                                ).text.strip()
                                if review.find_element(
                                    By.CSS_SELECTOR, "div.story-point > span"
                                )
                                else None
                            )
                            review_date_defore = (
                                review.find_element(
                                    By.CSS_SELECTOR, "div.story-date > div > span"
                                ).text.strip()
                                if review.find_element(
                                    By.CSS_SELECTOR, "div.story-date > div > span"
                                )
                                else None
                            )
                            em_tags = (
                                review.find_element(
                                    By.CLASS_NAME, "story-recommend"
                                ).find_elements(By.TAG_NAME, "em")
                                if review.find_element(
                                    By.CLASS_NAME, "story-recommend"
                                ).find_elements(By.TAG_NAME, "em")
                                else None
                            )

                            # 리뷰 날짜 형식 바꾸기
                            review_date = convert_datetime(review_date_defore)

                            review_dt = pd.to_datetime(review_date, errors="coerce")

                            # 이미 수집한 가장 최신 리뷰 시간보다 같거나 이전이면 크롤링 중단. 유효한 날찌인지 확인
                            if (
                                latest_datetime
                                and pd.notnull(review_dt)
                                and review_dt <= latest_datetime
                            ):
                                stop_crawling = True
                                break

                            # 추천 태그
                            recommend_tags = []

                            if em_tags:
                                for em in em_tags:
                                    text = em.text.strip()

                                    # 숫자 포함된 것 제외 (ex. +2, +3 등), ~외 제외(ex. 연출 외, 스토리 외 등)
                                    if "+" in text:
                                        continue
                                    if text.endswith("외") or "외" in text:
                                        continue
                                    if text:
                                        recommend_tags.append(text)

                            if id:
                                megabox_reviews.append(
                                    {
                                        "id": id,
                                        "context": context,
                                        "star": int(rating) / 2,
                                        "review_date": review_date,
                                        "recommend_tags": recommend_tags,
                                    }
                                )
                        except Exception as e:
                            logging.info(f"리뷰 수집 실패: {e}")
                            continue

                    if stop_crawling:
                        break

                    # 페이지 넘길 수 있으면 페이지 넘겨서 리뷰 수집하기
                    try:
                        # 현재 페이지 확인
                        current_page = int(
                            driver.find_element(
                                By.CSS_SELECTOR, "nav.pagination strong.active"
                            ).text
                        )
                        # 다음 페이지들 확인
                        next_buttons = driver.find_elements(
                            By.CSS_SELECTOR, "nav.pagination a[pagenum]"
                        )

                        has_next = False
                        for btn in next_buttons:
                            if btn.get_attribute("pagenum") == str(current_page + 1):
                                btn.click()

                                # 페이지 넘기기 후
                                time.sleep(random.uniform(2, 5))
                                has_next = True
                                break

                        # 다음 페이지 없으면 다음 10개 페이지 이동
                        if not has_next:
                            try:
                                next_10_button = driver.find_element(
                                    By.CSS_SELECTOR, "a.control.next"
                                )
                                next_10_button.click()
                                time.sleep(random.uniform(2, 5))
                            except Exception as e:
                                logging.info(f"모든 페이지 끝. {e}")
                                break
                    except Exception as e:
                        logging.info(f"page Error : {e}")
                        break
                except Exception as e:
                    logging.info(f"리뷰가 없음. {e}")
        except Exception as e:
            logging.info(f"영화를 찾을 수 없음. {e}")
        finally:
            driver.quit()

            if megabox_reviews:
                df = pd.DataFrame(megabox_reviews)
                upload_to_gcs(df, movieNm)


def upload_to_gcs(df, movieNm):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    # gcs 파일 경로 설정
    gcs_file_path = (
        f"{MOVIE_REVIEW_FOLDER}/megabox_reviews/{movieNm}_megabox_reviews.csv"
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

    csv_data = combined_df.to_csv(index=False, encoding="utf-8-sig")
    blob.upload_from_string(csv_data, content_type="text/csv")

    print(f"megabox reviews 업로드 완료. 날짜 : {movieNm}")


# dag 설정
default_args = {
    "start_date": datetime(2025, 3, 31),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}
with DAG(
    dag_id="megabox_review_crawling",
    schedule_interval=timedelta(days=1),
    catchup=False,
    default_args=default_args,
) as dag:

    get_unique_movie_list_from_gcs = PythonOperator(
        task_id="get_unique_movie_list_from_gcs",
        python_callable=get_unique_movie_list_from_gcs,
        provide_context=True,
    )

    scraping_megabox_reviews = PythonOperator(
        task_id="scraping_megabox_reviews",
        python_callable=scraping_megabox_reviews,
        provide_context=True,
    )

    get_unique_movie_list_from_gcs >> scraping_megabox_reviews
