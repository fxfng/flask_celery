# -*- coding: utf-8 -*-

# from flask import Flask
# from .celery import make_celery
#
#
# def create_app():
#     app = Flask(__name__)
#     app.config.update(
#         CELERY_BROKER_URL="redis://:123456@10.6.129.242:6379/0",
#         RESULT_BACKEND="redis://:123456@10.6.129.242:6379/0",
#         task_ignore_result=True
#     )
#
#     celery_app = make_celery(app)
#
#     return app, celery_app
