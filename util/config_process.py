# -*- coding: utf-8 -*-
import os.path

from .config_util import Config

config_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__))) + "/config/test_configs.yaml"
config_object = Config(config_path)
