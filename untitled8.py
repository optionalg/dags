import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import re
from bs4 import BeautifulSoup
from selenium.webdriver.common.keys import Keys
import time
import csv
import pandas as pd
import numpy as np

options = webdriver.ChromeOptions()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-gpu')
options.add_argument('--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
driver = webdriver.Chrome(r'D:\big11\tools\chromedriver.exe',options=options)

prod_info = []
url = 'https://store.musinsa.com/app/items/lists/005011/?category=&d_cat_cd=005011&u_cat_cd=&brand=&sort=pop&sub_sort=&display_cnt=100&page=2&page_kind=category&list_kind=small&free_dlv=&ex_soldout=&sale_goods=&exclusive_yn=&price=&color=&a_cat_cd=&size=&tag=&popup=&brand_favorite_yn=&goods_favorite_yn=&blf_yn=&campaign_yn=&bwith_yn=&price1=&price2=&brand_favorite=&goods_favorite=&chk_exclusive=&chk_sale=&chk_soldout='

driver.get(url)
time.sleep(3)
prod_brand = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.item_title > a')
prod_id_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.list_info > a')
prod_name_list = driver.find_elements_by_css_selector('#searchList > li > div.li_inner > div.article_info > p.list_info > a')

for i in prod_id_list:
    raw_prod_id = i.get_attribute("title")
    print(raw_prod_id)

    
# for q,w,e in zip(prod_id_list,prod_name_list,prod_brand):
#     raw_prod_id = q.get_attribute("title")
#     prod_name = w.text
#     prod_brand = e.text
#     prod_id = raw_prod_id.split('/')[6]
    
#     url = f'https://store.musinsa.com/app/product/detail/{prod_id}/0'
#     driver.get(url)
#     time.sleep(3)
#     # 제품코드, 브랜드 class_name 으로 찾기.
#     id_and_brand = driver.find_element_by_class_name('product_article_contents')
#     size = driver.find_element_by_class_name('option1')
#     # Name 과 brand 가 '/' 로 붙어있어서 split.
#     id_and_brand_text = id_and_brand.text
#     prod_brand = id_and_brand_text.split('/')[0] #이거는 브랜드 필요하면 쓰세요.
#     name_id = id_and_brand_text.split('/')[1]
#     gender = driver.find_element_by_class_name('txt_gender')
#     # Size text 변환후 공백 제거.
#     gender_text = gender.text
#     size_text = size.text
#     size_text_split = size_text.split()
#     # ['230','240','250','260','270'] 이렇게 리스트 형식이어서 join 으로 합치는 정규 표현식.
#     join_size_text = '-'.join(size_text_split) # 이거는 사이즈 필요하면 쓰세요.

#     prod_info.append([category, prod_brand, prod_id, prod_name, name_id, gender_text ,join_size_text])
    
# driver.close()