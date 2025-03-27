from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import pandas as pd
import time
from google.cloud import storage
from datetime import datetime, timedelta
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

    for movieNm, openDt in movie_info_set:
        if movieNm in ('모아나 2', '대가족', '괜찮아 괜찮아 괜찮아!', '엘리: 몬스터 패밀리', '폭락', '9월 5일: 위험한 특종', '노스페라투', '데드데드 데몬즈 디디디디 디스트럭션: 파트1', 
                       '브루탈리스트', '시빌 워: 분열의 시대', '페라리','보고타: 마지막 기회의 땅', '검은 수녀들', '그 시절, 우리가 좋아했던 소녀','극장판 쿠로코의 농구 라스트 게임',
                       '러브레터', '하얼빈', '수퍼 소닉3', '데드데드 데몬즈 디디디디 디스트럭션: 파트2', '영화 이상한 과자 가게 전천당',
                       '더 폴: 디렉터스 컷', '백수아파트', '브로큰', '써니데이', '엘리: 몬스터 패밀리', '캡틴 아메리카: 브레이브 뉴 월드', '컴플리트 언노운', '퇴마록'):
            continue
        cgv_reviews = []
        
        driver = webdriver.Chrome()

        # 사이트 접속
        url = 'https://www.cgv.co.kr/'
        driver.get(url)

        # 검색 결과 대기
        driver.implicitly_wait(10)
        
        try:
            # 영화 제목 입력 후 엔터
            search_input = driver.find_element(By.ID,'header_keyword')
            search_input.clear()
            search_input.send_keys(movieNm)
            search_input.send_keys(Keys.RETURN)

            # 검색 결과 대기
            driver.implicitly_wait(10)
            time.sleep(1)

            # 영화 상세 url
            movie_url = driver.find_element(By.ID, "searchMovieResult").find_element(By.CLASS_NAME, 'img_wrap')
            driver.get(movie_url.get_attribute("href"))

            # 영화 평점/리뷰로 이동할 수 있는 탭
            review_tab = driver.find_element(By.ID, "tabComent").find_element(By.XPATH, "a")
            driver.get(review_tab.get_attribute("href"))
            
            # 현재 페이지 1 페이지
            current_page = 1
            paging = driver.find_element(By.ID, "paging_point")
            
            while True:
                
                review_list = driver.find_element(By.ID, "movie_point_list_container")
                review_items = review_list.find_elements(By.XPATH, "./li")  # //li : 모든 li (자식, 손자 태그 포함), ./li : 자식 li
                
                if not review_items:
                    break

                # for review in review_items:
                for i in range(len(review_items)):
                    try:
                        review_list = driver.find_element(By.ID, "movie_point_list_container")
                        review_items = review_list.find_elements(By.XPATH, "./li")
                        review = review_items[i]
                        
                        id = review.find_element(By.XPATH, ".//ul/li[1]").text.strip()
                        date = review.find_element(By.XPATH, ".//ul/li[2]/span[1]").text.strip()
                        context = review.find_element(By.XPATH, ".//div[3]/p").text.strip()
                        print(id, date, context)
                        
                        cgv_reviews.append({
                            "id" : id,
                            "context" : context,
                            "date" : date
                        })
                    except Exception as e:
                        continue
                    
                try:    # 다음 페이지 있을 때 
                    next_page = current_page + 1
                    paging = driver.find_element(By.ID, "paging_point")
                    next_page_button = paging.find_element(By.XPATH, f'.//a[@href="#{next_page}"]')
                    next_page_button.click()
                    time.sleep(1)
                    current_page += 1
                    
                except:
                    try:   
                        next_10_button = driver.find_element(By.CLASS_NAME, "btn-paging.next")
                        next_10_button.click()
                        time.sleep(1)
                        current_page += 1
                        
                    except:
                        break
        except Exception as e:
            print("cgv에서 상영한 영화가 아니거나 리뷰 수집을 못함")
        finally:
            driver.quit()
            
            if cgv_reviews:
                df = pd.DataFrame(cgv_reviews)
                upload_to_gcs(df, movieNm)
            else:
                print(f"{movieNm} 리뷰 없음")

def upload_to_gcs(df, movieNm):
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    
    # gcs 파일 경로 설정
    gcs_file_path = f"{MOVIE_REVIEW_FOLDER}/cgv_reviews/{movieNm}_cgv_reviews.csv"
    blob = bucket.blob(gcs_file_path)
    
    csv_data = df.to_csv(index=False, encoding='utf-8-sig')
    blob.upload_from_string(csv_data, content_type="text/csv")    
    
    print(f"cgv reviews 업로드 완료. 날짜 : {movieNm}")
    
    
if __name__=='__main__':
    
    movie_info_set = get_unique_movie_list_from_gcs()   # (movieNm, openDt)의 튜플
    crawling_cgv_review(movie_info_set)
    
    
    
