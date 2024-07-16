# -*- coding: utf-8 -*-
from flask import Flask, request, jsonify
# from sqlalchemy import create_engine
import json
from datetime import date
# from util.config_process import config_object
from app.database import db_engine
from sqlalchemy import text

# doris_config = config_object.doris
# # pool_pre_ping连接前预检测，判断连接是否正常
# # pool_recycle连接设置超时时间3600s，创建连接时设置超时时间，获取连接时判断是否超时
# db_engine = create_engine(f"mysql://{doris_config['user']}:{doris_config['password']}@{doris_config['host']}:"
#                           f"{doris_config['port']}/demo", pool_size=10, pool_pre_ping=True, pool_recycle=3600)

app = Flask(__name__)


# app.config['SQLALCHEMY_DATABASE_URL'] = "mysql://root:@10.6.129.242:8030/demo"


@app.route("/hello")
def hello():
    return "Hello, welcome to my world!"


@app.route("/api/data", methods=['POST', 'GET'])
async def handle_post_request():
    data = request.get_json()
    print(data.get("result") or "default value")
    return data.get("result") or "default value"


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, date):
        return obj.isoformat()  # 将日期转换为ISO格式的字符串
    raise TypeError("Type %s not serializable" % type(obj))


# @app.get("/query/doris")
def query(sql):
    conn = db_engine.connect()
    query_data = conn.execute(text(sql))
    # query_data = db_engine.engine.execute("select * from sample_tbl")
    columns = list(query_data.keys())
    print(columns)
    json_results = []
    for row in query_data:
        tmp = {columns[i]: row[i] for i in range(len(columns))}
        json_results.append(tmp)
    result = json.dumps(json_results, ensure_ascii=True, default=json_serial)
    conn.close()
    print(result)
    return json_results


if __name__ == "__main__":
    app.run(debug=True, port=5098)
    # sql = "select * from sample_tbl where (age <= 30) or (city = 'beijing')"
    # query(sql)
