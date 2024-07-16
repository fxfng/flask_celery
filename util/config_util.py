# -*- coding: utf-8 -*-

import sys
from ruamel.yaml import YAML, YAMLError


class Config:
    def __init__(self, configs_path='../config/test_configs.yaml') -> None:
        self.doris = None
        self.redis = None
        self.celery = None
        self.is_production = None
        self.yaml = YAML()
        self.configs_path = configs_path
        self.reload()

    def _load_config(self) -> dict:
        """定义如何加载配置文件"""
        try:
            with open(self.configs_path, "r", encoding='utf-8') as fp:
                configs = self.yaml.load(fp)
            return configs
        except YAMLError as e:
            sys.exit(f"The config file is illegal as a YAML: {e}")
        except FileNotFoundError:
            sys.exit(f"The config does not exist")

    def reload(self) -> None:
        """将配置文件里的参数，赋予单独的变量，方便后面程序调用"""
        configs = self._load_config()
        # 配置文件中的参数，手动提取出来
        self.is_production = configs['is_production']
        self.doris = configs['doris']
        self.redis = configs['redis']
        self.celery = configs['celery']
