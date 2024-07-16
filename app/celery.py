# -*- coding: utf-8 -*-

from celery import Celery
from flask import Flask
# from . import config
from util.config_process import config_object


def make_celery(app: Flask):
    celery_app = Celery(
        app.import_name
    )

    # class CeleryConfig:
    #     redis = config_object.redis
    #     broker_url = f"redis://:{redis['password']}@{redis['host']}:{redis['port']}/0"
    #     result_backend = f"redis://:{redis['password']}@{redis['host']}:{redis['port']}/1"
    #     enable_utc = False
    #     timezone = "Asia/Shanghai"

    # celery_app.config_from_object(CeleryConfig)
    # celery_app.config_from_object(config)
    celery_app.conf.update(config_object.celery)
    celery_app.set_default()

    class ContextTask(celery_app.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app.Task = ContextTask
    return celery_app
