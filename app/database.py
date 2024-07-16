# -*- coding: utf-8 -*-

from sqlalchemy import create_engine
from util.config_process import config_object

doris_config = config_object.doris
# pool_pre_ping连接前预检测，判断连接是否正常
# pool_recycle连接设置超时时间3600s，创建连接时设置超时时间，获取连接时判断是否超时
db_engine = create_engine(f"mysql+pymysql://{doris_config['user']}:{doris_config['password']}@"
                          f"{doris_config['host']}:{doris_config['port']}/demo",
                          pool_size=10, pool_pre_ping=True, pool_recycle=3600)
#  pool_size=10, pool_pre_ping=True, pool_recycle=3600
