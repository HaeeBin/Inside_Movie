# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.common.keys import Keys
from playwright.sync_api import sync_playwright
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

def get_watcha_review_url(movieNm):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto(f'https://pedia.watcha.com/ko-KR/search?query={movieNm}')
        page.wait_for_timeout(1000)
        
        a_tags = page.locator("li.zK9dEEA5 >> a")
        for i in range(a_tags.count()):
            a = a_tags.nth(i)
            if a.locator("img").count() > 0:
                a.click()
                break
            
        page.wait_for_timeout(1000)
        # a = page.locator(".JiOg_6zF >> a").first
        # review_link = a.get_attribute("href")
        # a = page.locator("div.oBazLiES section:nth-child(2) header a").first
        # review_link = a.get_attribute("href")
        
        locator = page.locator("div.oBazLiES section:nth-child(2) header a")

        if locator.count() == 0:
            print(f"[❌] '{movieNm}'에 대한 리뷰 링크 없음")
            browser.close()
            return None

        try:
            locator.first.scroll_into_view_if_needed()
            locator.first.wait_for(timeout=10000)
            review_link = locator.first.get_attribute("href")
        
        
            browser.close()
            print(f'https://pedia.watcha.com{review_link}')
            return f'https://pedia.watcha.com{review_link}'
        except Exception as e:
            print(f"Error : {e}")
            
def scroll_to_bottom(page, pause_time=1.5, max_scrolls=50):
    previous_height = 0
    for _ in range(max_scrolls):
        current_height = page.evaluate("() => document.documentElement.scrollHeight")
        if previous_height == current_height:
            break  # 더 이상 스크롤 안 됨 (끝 도달)
        
        page.evaluate("() => window.scrollTo(0, document.documentElement.scrollHeight)")
        time.sleep(pause_time)
        previous_height = current_height

def scraping_watcha_reviews(movie_info_set):
    
    for movieNm, openDt in movie_info_set:
        try:
            movie_review_url = get_watcha_review_url(movieNm)
        except:
            print("영화 페이지 없음")
        
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            page.goto(movie_review_url)
            time.sleep(2)
            
            prev_count = 0
            repeat = 0
            max_repeat = 3
            
            # while repeat < max_repeat:
            #     page.mouse.wheel(0,3000)
            #     time.sleep(1.5)
            #     current_reviews = page.locator("div.divGvYMMD2G.sCUeLl5F").count()
            #     if current_reviews == prev_count:
            #         repeat += 1
            #     else:
            #         repeat = 0
            #     prev_count = current_reviews
            #     time.sleep(1.5)
            scroll_to_bottom(page)   
            page.wait_for_selector("div.divGvYMMD2G.sCUeLl5F", timeout=10000) 
                            
            watcha_reviews = []
            review_elements = page.locator("div.divGvYMMD2G.sCUeLl5F")
            count = review_elements.count()
            
            for i in range(count):
                try:
                    name = review_elements.nth(i).locator("div.TfbnQJp1.qfFKTvc9").inner_text()
                    star = review_elements.nth(i).locator("div.H7gLZE2m.KYbG4TeN > span").inner_text()
                    context = review_elements.nth(i).locator("div.tCqzOhhS.Jgl_VtOt._4_jash0F > a > div > div").inner_text()
                    
                    print(name, star, context)
                    watcha_reviews.append({
                        "id" : name,
                        "star" : star,
                        "context" : context
                    })
                except:
                    print("리뷰 수집x")
            browser.close()
            if watcha_reviews:
                df = pd.DataFrame(watcha_reviews)
                upload_to_gcs(df, movieNm)
            else:
                print(f"{movieNm} 리뷰 없음")
                
                    
        # driver = webdriver.Chrome()
        
        # try:
        #     url = f'https://pedia.watcha.com/ko-KR/search?query={movieNm}'
        #     driver.get(url)

        #     # 검색 결과 대기
        #     driver.implicitly_wait(10)

        #     # 영화 리뷰 페이지로 이동
        #     a = driver.find_element(By.CLASS_NAME, "JiOg_6zF").find_element(By.TAG_NAME, 'a')
        #     driver.get(a.get_attribute("href"))
        #     driver.implicitly_wait(2)
            
        #     # 현재 페이지 1 페이지
        #     current_page = 1
        #     paging = driver.find_element(By.ID, "paging_point")
            
        #     while True:
                
        #         review_list = driver.find_element(By.ID, "movie_point_list_container")
        #         review_items = review_list.find_elements(By.XPATH, "./li")  # //li : 모든 li (자식, 손자 태그 포함), ./li : 자식 li
                
        #         if not review_items:
        #             break

        #         # for review in review_items:
        #         for i in range(len(review_items)):
        #             try:
        #                 review_list = driver.find_element(By.ID, "movie_point_list_container")
        #                 review_items = review_list.find_elements(By.XPATH, "./li")
        #                 review = review_items[i]
                        
        #                 id = review.find_element(By.XPATH, ".//ul/li[1]").text.strip()
        #                 date = review.find_element(By.XPATH, ".//ul/li[2]/span[1]").text.strip()
        #                 context = review.find_element(By.XPATH, ".//div[3]/p").text.strip()
        #                 print(id, date, context)
                        
        #                 watcha_reviews.append({
        #                     "id" : id,
        #                     "context" : context,
        #                     "date" : date
        #                 })
        #             except Exception as e:
        #                 continue
                    
        #         try:    # 다음 페이지 있을 때 
        #             next_page = current_page + 1
        #             paging = driver.find_element(By.ID, "paging_point")
        #             next_page_button = paging.find_element(By.XPATH, f'.//a[@href="#{next_page}"]')
        #             next_page_button.click()
        #             time.sleep(1)
        #             current_page += 1
                    
        #             # 시간상 3000개 리뷰만 추출
        #             if current_page > 500:
        #                 break
                    
        #         except:
        #             try:   
        #                 next_10_button = driver.find_element(By.CLASS_NAME, "btn-paging.next")
        #                 next_10_button.click()
        #                 time.sleep(1)
        #                 current_page += 1
                        
        #             except:
        #                 break
        # except Exception as e:
        #     print("cgv에서 상영한 영화가 아니거나 리뷰 수집을 못함")
        # finally:
        #     driver.quit()
            
        #     if watcha_reviews:
        #         df = pd.DataFrame(watcha_reviews)
        #         upload_to_gcs(df, movieNm)
        #     else:
        #         print(f"{movieNm} 리뷰 없음")

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