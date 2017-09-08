import os

from anode import Log

APP_CONF = dict(
    line.strip().split("=") for line in
    open(os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__))) + "/application.properties")
    if not line.startswith("#") and not line.startswith("\n"))
APP_CONF_NAME = APP_CONF["APP_NAME"]
APP_CONF_NAME_ESCAPED = APP_CONF["APP_NAME_ESCAPED"]
APP_CONF_VERSION = APP_CONF["APP_VERSION"]
APP_CONF_VERSION_NUMERIC = int(APP_CONF["APP_VERSION_NUMERIC"])

MODEL_CONF = dict(
    line.strip().split("=") for line in
    open(os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__))) + "/avro/model.properties")
    if not line.startswith("#") and not line.startswith("\n"))
APP_CONF_MODEL_VERSION = MODEL_CONF["MODEL_VERSION"]
