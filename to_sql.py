import pymysql
from sqlalchemy import create_engine
pymysql.install_as_MySQLdb()
import MySQLdb

import numpy as np
import pandas as pdd

danawa = pd.read_csv('f'/root/reviews/danawa_{b_name}_id.csv', index_col=0, thousands=',')
del danawa['prod_info']

engine = create_engine("mysql+mysqldb://footfootbig:"+"footbigmaria!"+"@35.185.210.97/footfoot", encoding='utf-8')
conn = engine.connect()

sandals.to_sql(name='shoes', con=engine, if_exists='append', index=False)