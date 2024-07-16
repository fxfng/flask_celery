# -*- coding: utf-8 -*-

from celery import shared_task
import requests
from app.database import db_engine
from datetime import date
import json
from sqlalchemy import text


@shared_task(queue="q1")
def async_query(sql_sentence, callback_rul):
    result = sync_query(sql_sentence)
    requests.post(callback_rul, json={'result': result})


def sync_query(sql_sentence):
    return data_query(sql_sentence)


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, date):
        return obj.isoformat()  # 将日期转换为ISO格式的字符串
    raise TypeError("Type %s not serializable" % type(obj))


def data_query(sql_sentence):
    conn = db_engine.connect()
    query_data = conn.execute(text(sql_sentence))
    column_names = list(query_data.keys())
    json_results = []
    for row in query_data:
        tmp = {column_names[i]: row[i] for i in range(len(column_names))}
        json_results.append(tmp)
    result = json.dumps(json_results, ensure_ascii=True, default=json_serial)
    conn.close()
    return result
