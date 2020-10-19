import pandas as pd
import pymysql


# DB Connection
conn = pymysql.connect(
      host='35.185.210.97'
    , port=3306
    , user='footfootbig'
    , password='footbigmaria!'
    , database='footfoot'
)

query_even = """
    SELECT danawa_id
      FROM(
        SELECT danawa_id
             , @rownum:=@rownum+1 as rnum
          FROM shoes
             , (select @rownum:=0) as TMP
         where danawa_id is not null) as a
    where a.rnum%2=0;
 """
query_odd = """
    SELECT danawa_id
      FROM(
        SELECT danawa_id
             , @rownum:=@rownum+1 as rnum
          FROM shoes
             , (select @rownum:=0) as TMP
         where danawa_id is not null) as a
    where a.rnum%2=1;
"""

# Get a DataFrame
query_result_even = pd.read_sql(query_even, conn)
query_result_odd = pd.read_sql(query_odd, conn)

# Close connection
conn.close()