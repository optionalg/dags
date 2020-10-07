import pandas as pd
from selenium import webdriver
import re


options = webdriver.ChromeOptions()
options.add_argument('headless')
options.add_argument('window-size=1920x1080')
options.add_argument("disable-gpu")
options.add_argument(f'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
driver = webdriver.Chrome(r'C:\Users\Home\PycharmProjects\pythonProject1\chromedriver.exe',options=options)

mosinsa_prod_id_list = pd.read_csv('경로')[prod_id]
# prod_id 를 반복해서 넣으면 됩니다.

def Name_ID(mosinsa_prod_id):
    url = f'https://store.musinsa.com/app/product/detail/{mosinsa_prod_id}/0'
    driver.get(url)
    # 제품코드, 브랜드 class_name 으로 찾기.
    Name_ID_and_brand = driver.find_element_by_class_name('product_article_contents')
    Size = driver.find_element_by_class_name('option1')
    # Name 과 brand 가 '/' 로 붙어있어서 split.
    Name_ID_and_brand_text = Name_ID_and_brand.text
    prod_brand = Name_ID_and_brand_text.split('/')[0] #이거는 브랜드 필요하면 쓰세요.
    Name_id = Name_ID_and_brand_text.split('/')[1]
    # Size text 변환후 공백 제거.
    Size_text = Size.text
    Size_text_split = Size_text.split()
    # ['230','240','250','260','270'] 이렇게 리스트 형식이어서 join 으로 합치는 정규 표현식.
    join_size_text = '-'.join(Size_text_split) # 이거는 사이즈 필요하면 쓰세요.
    driver.close()
    return Name_id
# mosinsa id 반복해서 넣는 코드
for mosinsa_prod_id in mosinsa_prod_id_list:
    Name_ID(mosinsa_prod_id)