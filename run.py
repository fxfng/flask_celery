# -*- coding: utf-8 -*-

from flask import Flask, jsonify, request
import os
import sys

# 在PYTHONPATH中设置project的根目录位置
# 以便gunicorn可以找到项目目录，从而可以正常导入module方法

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from app.celery import make_celery
from tasks.doris_query import async_query, sync_query
from celery.result import AsyncResult
from util.construct_query_sql import parse

app = Flask(__name__)
celery_app = make_celery(app)


@app.route("/")
def index():
    return "Hello world!"


@app.route('/request', methods=['GET', 'POST'])
def handle_requests():
    req_json = request.json
    data = req_json.get('data')
    callback_url = req_json.get('callback_url')
    result = async_query.apply_async((data, callback_url), queue="q1")
    response = {'status': 'Processing started', "result_id": result.id}
    return jsonify(response)


# @app.post("/add")
# def start_add():
#     a = request.form.get("a", type=int)
#     b = request.form.get("b", type=int)
#     result = add_together.apply_async((a, b))
#     return {"result_id": result.id}


@app.get("/result/<task_id>")
def task_result(task_id: str):
    result = AsyncResult(task_id)
    return {
        "ready": result.ready(),
        "successful": result.successful(),
        "value": result.result if result.ready() else None,
    }


@app.post("/api/post")
def show_request():
    return request.json


@app.post("/api/async/query/doris")
def handle_async_request():
    req_json = request.json
    sql_sentence = parse(req_json)
    callback_url = req_json.get('callback_url')
    result = async_query.apply_async((sql_sentence, callback_url), queue="q1")
    return jsonify({'success': 'true', 'code': 0, 'msg': u'操作成功', "result_id": result.id, 'error': []})


@app.post("/api/sync/query/doris")
def handle_sync_request():
    sql_sentence = parse(request.json)
    result = sync_query(sql_sentence)
    return jsonify({'success': 'true', 'code': 0, 'msg': u'操作成功', 'data': result})


if __name__ == "__main__":
    app.run(debug=True, port=6098)
