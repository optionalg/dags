import pymysql
import numpy as np
import pandas as pd
import glob, os

from sqlalchemy import create_engine

pymysql.install_as_MySQLdb()
import MySQLdb

musinpath = f'/root/reviews/musinsa_*_id.csv'
musin_file_list = glob.glob(os.path.join(musinpath))

musin_df_list = []
for file in musin_file_list:
    tmp_df = pd.read_csv(file, index_col=0, thousands=',')
    tmp_df['category'] = file.split('_')[1]
    musin_df_list.append(tmp_df)

musinsa_df = pd.concat(musin_df_list, axis=0, ignore_index=True)

musinsa_df.rename(columns={
    'prod_name': 'shono'
    , 'gender': 'shosex'
}
    , inplace=True
)

musinsa_df.drop(musinsa_df[musinsa_df['shosex'] == '남 여 아동'].index, axis=0, inplace=True)
musinsa_df.drop(musinsa_df[musinsa_df['shosex'] == '아동'].index, axis=0, inplace=True)
musinsa_df.drop(musinsa_df[musinsa_df['shosex'] == '라이프'].index, axis=0, inplace=True)
musinsa_df.drop(musinsa_df[musinsa_df['shosex'] == '여 아동'].index, axis=0, inplace=True)

musinsa_df['shosex'].replace('남 여', "남녀공용", inplace=True)
musinsa_df['shosex'].replace('남', "남성용", inplace=True)
musinsa_df['shosex'].replace('여', "여성용", inplace=True)

musinsa_df['minsize'] = None
musinsa_df['maxsize'] = None
musinsa_df['sizeunit'] = None

for i in musinsa_df.index:
    try:
        musinsa_df['minsize'][i] = int(musinsa_df['size'].str.split('-')[i][0])
        musinsa_df['maxsize'][i] = int(musinsa_df['size'].str.split('-')[i][-1])
        musinsa_df['sizeunit'][i] = \
            int(musinsa_df['size'].str.split('-')[i][1]) - int(musinsa_df['size'].str.split('-')[i][0])
    except:
        pass
del musinsa_df['size']

danapath = f'/root/reviews/danawa_*_id.csv'
dana_file_list = glob.glob(os.path.join(danapath))

dana_df_list = []
for file in dana_file_list:
    tmp_df = pd.read_csv(file, index_col=0, thousands=',')
    dana_df_list.append(tmp_df)

danawa_df = pd.concat(dana_df_list, axis=0, ignore_index=True)
del danawa_df['prod_info']

merged_df = pd.merge(danawa_df, musinsa_df, on=['brand', 'modelname', 'shono', 'shosex', 'price', 'category'],
                     how='outer')

engine = create_engine("mysql+mysqldb://footfootbig:" + "footbigmaria!" + "@35.185.210.97/footfoot", encoding='utf-8')
conn = engine.connect()
merged_df.to_sql(name='shoes', con=engine, if_exists='append', index=False)

conn.close()