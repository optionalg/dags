# crawling
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import re
import time
import csv
import pandas as pd
import numpy as np
import datetime as dt

def get_shoes_review():

    # 크롬 드라이버 옵션
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
    # 드라이버 위치도 맞출 것
    driver = webdriver.Chrome('드라이버 위치',options=options)

    # prod_id 불러오기 - 자신의 환경에 맞는 위치로 설정할것
    #danawa_prod_id_path = r'C:\어딘가\파일위치\danawa_아디다스_id.csv')
    prod_dataframe = pd.read_csv(danawa_prod_id_path)
    prod_ids = prod_dataframe['danawa_id']

    for prod_id in prod_ids:
        img_url_list = []
        page = 0
        while True:
            page = page + 1
            url = 'http://prod.danawa.com/info/dpg/ajax/companyProductReview.ajax.php?t=0.10499996477784657&prodCode='+str(prod_id)+'&cate1Code=1824&page='+str(page)+'&limit=100&score=0&sortType=&usefullScore=Y&innerKeyword=&subjectWord=0&subjectWordString=&subjectSimilarWordString=&_=1600608005961'
            driver.get(url)
            time.sleep(3)
            danawa_img_list = driver.find_elements_by_class_name('center > img')
            for img_src in danawa_img_list:
                danawa_img_url = img_src.get_attribute('src')
                img_url_list.append(danawa_img_url)
                
        n = 0
        for url in img_url_list:
            n = n + 1
            r = requests.get(url)
            file = open(r'/이미지/저장할위치/danawa_{0}_{1}.jpg'.format(prod_id, n), 'wb')
            file.write(r.content)
            file.close()
            

    driver.close()