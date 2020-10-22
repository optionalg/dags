import pymysql

conn = pymysql.connect(host='35.185.210.97', user='footfootbig', password='footbigmaria!',
                       db='footfoot', charset='utf8')

try:
    with conn.cursor() as curs:
        sql = """
            insert ignore into shoes(brand, shono, modelname, category, shosex, price_m
            , price_d, minsize, maxsize, sizeunit, heelsize, musinsa_id, danawa_id)
            (select m.brand, m.shono, m.modelname, m.category, m.shosex, m.price_m
            , d.price_d, m.minsize, m.maxsize, m.sizeunit, d.heelsize, m.musinsa_id, d.danawa_id
              from musinsa_shoes as m
            left outer join danawa_shoes as d
            on m.shono=d.shono and m.brand=d.brand
            UNION
            select d.brand, d.shono, d.modelname, d.category, d.shosex, m.price_m, d.price_d
            , m.minsize, m.maxsize, m.sizeunit, d.heelsize, m.musinsa_id, d.danawa_id
              from musinsa_shoes as m
            right outer join danawa_shoes as d
                on m.shono=d.shono and m.brand=d.brand);
        """
        curs.execute(sql)

    conn.commit()

finally:
    conn.close()

