from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options

import pandas as pd
import time
from google.cloud import storage
import os, io
from dotenv import load_dotenv

load_dotenv()

GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
BUCKET_NAME = os.getenv('BUCKET_NAME')
MOVIE_REVIEW_FOLDER = 'movie_reviews'
DAILY_BOXOFFICE_FOLDER = 'daily_boxoffice'
DAILY_REGION_BOXOFFICE_FOLDER = 'daily_regions_boxoffice'
BOXOFFICE_API_KEY = os.getenv('BOXOFFICE_API_KEY')

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

def get_unique_movie_list_from_gcs():
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # 중복 없이 영화 리스트 담기 위함
    movie_set = set()
    
    def process_boxoffice(folder):
        blobs = bucket.list_blobs(prefix=folder)
        for blob in blobs:
            if blob.name.endswith(".csv"):  # csv파일에 담긴 boxoffice파일
                csv_data = blob.download_as_text(encoding='utf-8-sig')
                df = pd.read_csv(io.StringIO(csv_data))
                
                if 'movieNm' in df.columns and 'openDt' in df.columns:
                    for _,row in df.iterrows():
                        movieNm = str(row["movieNm"]).strip()
                        openDt = str(row["openDt"]).strip()
                        movie_set.add((movieNm, openDt))
    
    process_boxoffice(DAILY_BOXOFFICE_FOLDER)   # 일별 박스오피스 조회
    process_boxoffice(DAILY_REGION_BOXOFFICE_FOLDER)    # 지역별 박스오피스 조회    
    
    return movie_set
                         
def crawling_cgv_review(movie_info_set):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    for movieNm, openDt in movie_info_set:
        cine_reviews = []
        
        driver = webdriver.Chrome(options=options)

        # 사이트 접속
        # url = 'http://www.cine21.com/'
        # driver.get(url)

        # # 검색 결과 대기
        # driver.implicitly_wait(10)
        
        try:
            # 영화 제목 입력 후 엔터
            # search_input = driver.find_element(By.CSS_SELECTOR,'#search_q.input_search')
            # search_input.clear()
            # search_input.send_keys(movieNm)
            # search_input.send_keys(Keys.RETURN)

            # # 검색 결과 대기
            # driver.implicitly_wait(10)
            # time.sleep(1)

            # # 영화 상세 url
            # movie_url = driver.find_element(By.CLASS_NAME, "mov_list").find_element(By.TAG_NAME, "a")
            url = f'http://www.cine21.com/search/?q={movieNm}'
            driver.get(url)
            movie_url = driver.find_element(By.CLASS_NAME, 'mov_list').find_element(By.TAG_NAME, "a")
            driver.get(movie_url.get_attribute("href"))
            driver.implicitly_wait(10)
            
            # 평론가 리뷰 
            results_html = driver.page_source

            soup = BeautifulSoup(results_html, 'html.parser')

            expert_rating = soup.find('ul', 'expert_rating')
            expert_reviews = expert_rating.find_all('li')

            for review in expert_reviews:
                star = review.select_one("div > div.star_area > span") if review.select_one("div > div.star_area > span") else None
                name = review.select_one("div > div.comment_area > a > span")
                context = review.select_one("div > div.comment_area > span") if review.select_one("div > div.comment_area > span") else None
                
                if name:
                    cine_reviews.append({
                        "who" : "expert",
                        "name" : name,
                        "context" : context,
                        "star" : star,
                        "date" : None
                    })
        except:
            pass    # 평론가 리뷰 없을 때 pass
        
        try:    
            # 네티즌 리뷰있는 곳으로 스크롤
            netizen_review_area = driver.find_element(By.ID, "netizen_review_area")
            driver.execute_script("arguments[0].scrollIntoView(true);", netizen_review_area)
            time.sleep(1)
            
            pagination = netizen_review_area.find_element(By.CLASS_NAME, "pagination")      # page 
            page_buttons = pagination.find_elements(By.CSS_SELECTOR, ".page > a")
            last_page = int(page_buttons[-1].text.strip())  # 마지막 페이지
            
            for page in range(1, last_page + 1):
                driver.execute_script("$('#netizen_review_area').nzreview('list', arguments[0]);", page)
                # driver.implicitly_wait(10)
                
                netizen_review_area = driver.find_element(By.ID, "netizen_review_area")
                driver.execute_script("arguments[0].scrollIntoView(true);", netizen_review_area)
                # driver.implicitly_wait(10)
            
                # 페이지 로딩 대기
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CLASS_NAME, "reply_box"))
                )
                time.sleep(1)  # 추가 대기 (렌더링 안정화)
                
                netizen_review_list = netizen_review_area.find_element(By.CLASS_NAME, 'reply_box')  # 리뷰 목록들
                review_items = netizen_review_list.find_elements(By.XPATH, "./li")

                try:
                    for review in review_items:
                        name = review.find_element(By.CLASS_NAME, "id").text.strip()
                        date = review.find_element(By.CLASS_NAME, "date").text.split() if review.find_elements(By.CLASS_NAME, "date") else None
                        star = review.find_element(By.XPATH, "./div[3]/span").text.strip() if review.find_elements(By.XPATH, "./div[3]/span") else None
                        
                        try:
                            context = review.find_element(By.CSS_SELECTOR, "div.comment.ellipsis_3").text.strip()
                        except:
                            try:
                                context = review.find_element(By.CSS_SELECTOR, "div.comment").text.strip()
                            except Exception as e:
                                print(f"Error {e} : Netizen context crawling")
                                context = None
                        
                        if name:
                            cine_reviews.append({
                                "who" : "netizen",
                                "name" : name,
                                "context" : context,
                                "star" : star,
                                "date" : date[0]
                            })
                        print(name, context, star, date[0])
                except Exception as e:
                    print(f"Error {e}")
                    continue
        except Exception as e:
            print("Netizen review 없음")
            print(f"Error {e}")
        finally:
            driver.quit()
            
            if cine_reviews:
                df = pd.DataFrame(cine_reviews)
                upload_to_gcs(df, movieNm)
            else:
                print(f"{movieNm} 리뷰 없음")

def upload_to_gcs(df, movieNm):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # gcs 파일 경로 설정
    gcs_file_path = f"{MOVIE_REVIEW_FOLDER}/cine_reviews/{movieNm}_cine_reviews.csv"
    blob = bucket.blob(gcs_file_path)
    
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_data, content_type="text/csv")    
    
    print(f"cgv reviews 업로드 완료. 날짜 : {movieNm}")
    
    
if __name__=='__main__':
    
    movie_info_set = get_unique_movie_list_from_gcs()   # (movieNm, openDt)의 튜플
    crawling_cgv_review(movie_info_set)