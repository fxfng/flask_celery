# -*- coding: utf-8 -*-

from celery import shared_task
import requests
import time


@shared_task(queue="q1")
def async_query(data, callback_rul):
    time.sleep(5)
    result = f"Processed data: {data}"
    requests.post(callback_rul, json={'result': result})


@shared_task(queue="q1", ignore_result=False)
def add_together(a: int, b: int):
    return a + b
