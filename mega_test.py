from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
from datetime import datetime, timedelta
import time
from google.cloud import storage
import os, io, logging, re
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

def convert_datetime(text):
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

def scraping_megabox_reviews(movie_info_set):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    for movieNm, openDt in movie_info_set:
        
        if movieNm in ('500일의 썸머', '검은 수녀들', '노스페라투', '대가족', '명탐정 코난: 14번째 표적', '무파사: 라이온 킹', '백수아파트',
                       '서브스턴스', '소방관', '써니데이', '엘리: 몬스터 패밀리', '캡틴 아메리카: 브레이브 뉴 월드', '패딩턴: 페루에 가다!',
                       '하얼빈', '해리포터와 죽음의 성물2', '히어'):
            continue
        # 개봉년도
        open_year = openDt.split("-")[0]
        url = f'https://www.megabox.co.kr/movie?searchText={movieNm}'
        driver = webdriver.Chrome()

        driver.get(url)
        driver.implicitly_wait(10)
        
        megabox_reviews = []
        
        try:
            # 영화 버튼 찾기
            movie_button = driver.find_element(By.CSS_SELECTOR, "a.wrap.movieBtn")

            # 클릭
            ActionChains(driver).move_to_element(movie_button).click().perform()
            time.sleep(1)
            
            review_tab = driver.find_element(By.CSS_SELECTOR, 'a[title="실관람평 탭으로 이동"]')
            driver.execute_script("arguments[0].scrollIntoView(true);", review_tab)
            review_tab.click()
            driver.implicitly_wait(10)

            while True:
                try:
                    # 리뷰 리스트 로딩 대기
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, 'div.movie-idv-story ul > li.type01.oneContentTag'))
                    )
                    
                    time.sleep(0.5)
                    
                    review_items = driver.find_elements(By.CSS_SELECTOR, 'div.movie-idv-story ul > li.type01.oneContentTag')
                    if not review_items:
                        print("더 이상 리뷰 없음. 종료.")
                        break
                    
                    for review in review_items:
                        try:  
                            id = review.find_element(By.CLASS_NAME, 'user-id').text.strip()
                            context = review.find_element(By.CLASS_NAME, 'story-txt').text.strip()
                            rating = review.find_element(By.CSS_SELECTOR, 'div.story-point > span').text.strip()
                            review_date_defore = review.find_element(By.CSS_SELECTOR, 'div.story-date > div > span').text.strip()
                            print(review_date_defore)
                            review_date = convert_datetime(review_date_defore)

                            recommend_tags = []
                            
                            em_tags = review.find_element(By.CLASS_NAME, 'story-recommend').find_elements(By.TAG_NAME, 'em')
                            for em in em_tags:
                                text = em.text.strip()
                                
                                # 숫자 포함된것 제외 (+2, +3 등), ~외 제외(연출 외, 스토리 외 등)
                                if "+" in text:
                                    continue
                                if text.endswith("외") or "외" in text:
                                    continue
                                if text:
                                    recommend_tags.append(text)

                            megabox_reviews.append({
                                "id" : id,
                                "context" : context,
                                "star" : int(rating) / 2,
                                "review_date" : review_date,
                                "recommend_tags" : recommend_tags
                            })
                        except Exception as e:
                            print(f"리뷰 수집 실패: {e}")
                    
                    # 페이지
                    try:
                        # 현재 페이지 확인
                        current_page = int(driver.find_element(By.CSS_SELECTOR, "nav.pagination strong.active").text)
                        next_buttons = driver.find_elements(By.CSS_SELECTOR, "nav.pagination a[pagenum]")
                    
                        has_next = False
                        for btn in next_buttons:
                            if btn.get_attribute("pagenum") == str(current_page + 1):
                                btn.click()
                                has_next = True
                                break
                        
                        # 다음 페이지 없으면 다음 10개 페이지 이동
                        if not has_next:
                            try:
                                next_10_button = driver.find_element(By.CSS_SELECTOR, "a.control.next")
                                next_10_button.click()
                            except:
                                print("모든 페이지 끝")
                                break
                        
                    except Exception as e:
                        print(f"page Error : {e}")
                        break
                
                except Exception as e:
                    print(f"전체 Error : {e}")    
                    
        except Exception as e:
            print(e)
        
        finally:
            driver.quit()
            
            if megabox_reviews:
                df = pd.DataFrame(megabox_reviews)
                upload_to_gcs(df, movieNm)



def upload_to_gcs(df, movieNm):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # gcs 파일 경로 설정
    gcs_file_path = f"{MOVIE_REVIEW_FOLDER}/megabox_reviews/{movieNm}_megabox_reviews.csv"
    blob = bucket.blob(gcs_file_path)
    
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_data, content_type="text/csv")    
    
    print(f"megabox reviews 업로드 완료. 날짜 : {movieNm}")
    
if __name__=='__main__':
    
    movie_info_set = get_unique_movie_list_from_gcs()   # (movieNm, openDt)의 튜플
    scraping_megabox_reviews(movie_info_set)