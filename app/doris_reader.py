# -*- coding: utf-8 -*-

import pymysql, json
from datetime import date
from pymysql import Error
from util.config_util import Config

conf = Config()


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, date):
        return obj.isoformat()  # 将日期转换为ISO格式的字符串
    raise TypeError("Type %s not serializable" % type(obj))


def main(sql):
    conn = pymysql.connect(
        host=conf.doris['host'],
        port=conf.doris['port'],
        user=conf.doris['user'],
        password=conf.doris['password']
    )

    try:
        cur = conn.cursor()
        cur.execute(sql)
        fetch_data = cur.fetchall()
        desc_tbl = cur.description
        data = []
        for row in fetch_data:
            tmp_dict = {}
            for index, desc_col in enumerate(desc_tbl):
                tmp_dict[desc_col[0]] = row[index]
            data.append(tmp_dict)
        # json_result = json.dumps(data, ensure_ascii=False, default=json_serial)
        print(data)
        # return data
    except Error as e:
        print(f"数据库连接或查询发生错误： {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main("select * from demo.sample_tbl limit 10")
