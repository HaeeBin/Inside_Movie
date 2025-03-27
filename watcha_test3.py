from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
from google.cloud import storage
import os, io, logging
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

def get_watcha_review_url(movieNm, openDt):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)

    try:
        url = f'https://pedia.watcha.com/ko-KR/search?query={movieNm}'
        driver.get(url)

        # 검색 결과 대기
        driver.implicitly_wait(10)
        clicked = False
        movies = driver.find_elements(By.CSS_SELECTOR, "li.zK9dEEA5")
        for movie in movies:
            title_tag = movie.find_element(By.TAG_NAME, "a")
            title = title_tag.get_attribute("title")
            open_year = movie.find_element(By.CSS_SELECTOR, "a > div.bVUVO8nJ > div.qz_zPMlN.RiDHrQhO").text.split("・")[0].strip()
            
            if (movieNm == title) and (open_year == openDt):
                title_tag.click()
                clicked = True
                break
        
        if clicked == False:
            a_tags = driver.find_elements(By.CSS_SELECTOR, "li.zK9dEEA5 a")
            for a in a_tags:
                try:
                    if len(a.find_elements(By.TAG_NAME, "img")) > 0:
                        ActionChains(driver).move_to_element(a).perform()
                        a.click()
                        break
                except Exception as e:
                    print(f"Error : {e}")
                    continue
            
            time.sleep(1)
        
        try:
            locator = driver.find_element(By.CSS_SELECTOR, "div.oBazLiES section:nth-child(2) header a")
            ActionChains(driver).move_to_element(locator).perform()
            time.sleep(0.5)
            review_link = locator.get_attribute("href")
            print(f"https://pedia.watcha.com{review_link}")
            return review_link
        except Exception as e:
            print(f"Error : {e}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()    
            
def scroll_to_bottom(driver, max_attempts=100, wait_time=1.5):
    '''
    페이지 스크롤해서 더 이상 새로운 리뷰가 로드되지 않을 때까지 스크롤하는 함수입니다.
    '''
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

def scraping_watcha_reviews(movie_info_set):
    options = Options()
    options.add_argument("--headless")
    options.add_argument("window-size=1200x600")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    
    for movieNm, openDt in movie_info_set:
        # 개봉년도
        open_year = openDt.split("-")[0]
        movie_review_url = get_watcha_review_url(movieNm, open_year)
        if not movie_review_url:
            print(f"'{movieNm}'에 대한 리뷰 링크 없음")
            continue

        driver = webdriver.Chrome(options=options)
        
        watcha_reviews = []
        
        try:
            driver.get(movie_review_url)
            scroll_to_bottom(driver)   
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.GvYMMD2G.sCUeLl5F"))
            )
            reviews = driver.find_elements(By.CSS_SELECTOR, 'div.GvYMMD2G.sCUeLl5F')

            for review in reviews:
                try:
                        name = review.find_element(By.CSS_SELECTOR, "div.GqJea6zF > div.EeIUbbQr.qfFKTvc9 > a > div.TfbnQJp1.qfFKTvc9").text.strip()
                        try:
                            star = review.find_element(By.CSS_SELECTOR, 'div.GqJea6zF > div.H7gLZE2m.KYbG4TeN > span').text.strip()
                        except Exception as e:
                            star = "None"
                        
                        # 스포일러가 있을 경우 리뷰가 나오지 않고 리뷰 보기 버튼을 눌러야 리뷰가 나옴.
                        try:
                            context_button = review.find_element(By.CSS_SELECTOR, 'div.tCqzOhhS.Jgl_VtOt._4_jash0F > a > div > span > button')
                            context_button.click()
                        except:
                            pass
                        
                        context = review.find_element(By.CSS_SELECTOR, 'div.tCqzOhhS.Jgl_VtOt._4_jash0F > a > div > div').text.strip()
                            
                        
                        print(name, star, context)
                        watcha_reviews.append({
                            "id" : name,
                            "star" : star,
                            "context" : context
                        })
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
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # gcs 파일 경로 설정
    gcs_file_path = f"{MOVIE_REVIEW_FOLDER}/watcha_reviews/{movieNm}_watcha_reviews.csv"
    blob = bucket.blob(gcs_file_path)
    
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_data, content_type="text/csv")    
    
    print(f"watcha reviews 업로드 완료. 날짜 : {movieNm}")
    
if __name__=='__main__':
    
    movie_info_set = get_unique_movie_list_from_gcs()   # (movieNm, openDt)의 튜플
    scraping_watcha_reviews(movie_info_set)