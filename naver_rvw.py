import pandas as pd
import numpy
from selenium import webdriver
import re
import time
import csv
import datetime as dt
from tqdm import tqdm
import requests

# 네이버



dt = dt.datetime.now()
options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-gpu')
options.add_argument(
    '--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
driver = webdriver.Chrome(executable_path='/usr/bin/chromedriver',options=options)

naver_brand ={'나이키': 1269, '아디다스': 20667, '탠디': 2014407, '뉴발란스': 2010946, 'XENIA': 2013840, '소다': 20759, '고세': 2010694, '엘칸토': 20544, '에스콰이아': 2013181
    , '미소페': 2011877, '푸마': 20126, '스케쳐스': 2012606, '닥스': 201217, '크록스': 2029750, '반스': 20923, '리복': 201056, '락포트': 2011294, '무크': 20135173, '발렌티노': 2012004
    , '아식스': 20654, '휠라': 207, '컨버스': 2014153, '세라': 2012439, '프로스펙스': 2014723, '핏플랍': 20157267, '구찌': 2010716, '골든구스': 20178717, '제옥스': 2030347, '프라다': 2014696
    , '어그': 2013109, '베어파우': 2012270, '토즈': 2014445, '발렌시아가': 20920, '미즈노': 20946, '라코스테': 201149, '언더아머': 2032926, '알렉산더맥퀸': 20202509, '금강제화': 20228910
    , '타미힐피거': 20240, 'BABARA': 2011944, '르까프': 2011489, '살바토레페라가모': 2014623, '포멜카멜레': 20237949, '닥터마틴': 201216, '페이유에': 20202618, '토리버치': 2032809, '수페르가': 20154988
    , '잭앤질': 2020408, '슈펜': 20208870, '스코노': 20725, '마이클코어스': 201392, '게스': 201326, '프레드페리': 2030259, '메종마르지엘라': 20214884, '스퍼': 20110112, '월드컵': 20475, '버버리': 2012034
    , '오니츠카타이커': 2031786, '코치': 2014223, 'ASH': 20176654, '미우미우': 2011893, '탐스': 2014455, '케즈': 2030900, '쎄리스': 2012442, '카파': 20327, '아떼바네사브루노': 20198715
    , '아키클래식': 20159296, '커먼프로젝트': 20200285, '캠퍼': 20199402, '브룩스': 2020895, '엑셀시오르': 20228443, '스티유': 20176471, '레페토': 20202451, '데상트': 20136246, '배롭': 20209297
    , '질바이질스튜어트': 20149383, '르꼬끄스포르티브': 2017271, '폴로': 20133, '디올': 2014298, '디스커버리익스페디션': 2029907, '호카네오네': 20197164, '에르메스': 2013143, '스닉솔': 20225402
    , '스프리스': 2012671, '샤네르꼼데가르송': 2012401, '슬레진저': 20202616, '슈마커': 20202670, '슈콤마보니': 20248853, 'MLB': 2032574, '오즈웨어': 20539, '부테로': 20185786, '블랙마틴싯봉': 20202521
    , '찰스앤키스': 20178089, '폴로키즈': 20206141, '벤시몽': 20132, '럽썸': 2029269, '다이나핏': 20222473, '나무하나': 20244494, '내셔널지오그래픽': 20138687}

url_list = []
for brand_name_list,brand_num_list in naver_brand.items():
    naver_info_and_rvw = []
    for page in range(15):
        url = 'https://search.shopping.naver.com/search/all?brand='+str(brand_num_list)+'&origQuery=%EC%8B%A0%EB%B0%9C&pagingIndex=' + str(page) + '&pagingSize=80&productSet=model&query=%EC%8B%A0%EB%B0%9C&sort=review&timestamp=&viewType=list'
        driver.get(url)
        time.sleep(3)
        prod_url_list = driver.find_elements_by_xpath(
            '//*[@id="__next"]/div/div[2]/div[2]/div[3]/div[1]/ul/div/div/li/div/div[2]/div[1]/a')
        for prod_url_attr in prod_url_list:
            base_url = prod_url_attr.get_attribute('href')
            url_list.append(base_url)
            for prod_url in url_list:
                driver.get(prod_url)  # get = 이동시키는 역할
                time.sleep(3)
                driver.implicitly_wait(10)
                prod_name = driver.find_element_by_css_selector(
                    '#container > div.summary_area > div.summary_info._itemSection > div > div.h_area > h2')
                prod_name_text = prod_name.text
                brand = driver.find_element_by_css_selector(
                    '#container > div.summary_area > div.summary_info._itemSection > div > div.goods_info > div > span:nth-child(2) > em')
                brand_text = brand.text


                # 네이버 대표 이미지 가져와서 현재 디렉토리에 저장하는 코드(디렉토리 설정해주세요.)
                prod_main_img = driver.find_element_by_css_selector('#viewImage')
                img_url = prod_main_img.get_attribute('src')
                r = requests.get(img_url)
                file = open("naver_img_{}.jpg".format(str(prod_name_text)), "wb")
                file.write(r.content)
                file.close()


                all_review_counts = driver.find_element_by_css_selector('#snb > ul > li.mall_review > a > em')
                end_page = int(all_review_counts.text) / 20
                try:
                    for page in range(1, int(end_page)):
                        driver.execute_script(f"shop.detail.ReviewHandler.page({page+1}, '_review_paging'); return false;")
                        time.sleep(5)
                except:
                    pass
                    prod_infos = driver.find_elements_by_css_selector(
                        '#_review_list > li > div > div.avg_area > span > span:nth-child(4)')
                    review_dates = driver.find_elements_by_css_selector(
                        '#_review_list > li > div > div.avg_area > span > span:nth-child(3)')
                    reviews = driver.find_elements_by_css_selector('#_review_list > li > div > div.atc')
                    for review_date, prod_info, review in zip(review_dates, prod_infos, reviews):
                        review_date_text = review_date.text
                        prod_info_text = prod_info.text
                        review_text = review.text
                        naver_info_and_rvw.append([brand_text, prod_name_text, review_date_text, prod_info_text, review_text])

            refilename = f'/root/reviews/naver_{brand_name_list}.csv'
            f = open(refilename, 'w', encoding='utf-8', newline='')
            csvWriter = csv.writer(f)
            csvWriter.writerow(['brand', 'prod_name', 'review_info','review_date', 'reviews'])
            for w in naver_info_and_rvw:
                csvWriter.writerow(w)
            f.close()
