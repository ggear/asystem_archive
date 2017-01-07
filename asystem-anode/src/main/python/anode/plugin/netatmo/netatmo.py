# -*- coding: utf-8 -*-

from __future__ import print_function

import calendar
import json
import logging
import os
import time
from decimal import Decimal

import treq

from anode.plugin.plugin import Plugin

HTTP_TIMEOUT = 10


class Netatmo(Plugin):
    def _poll(self):
        if not self.disabled:
            if self.token_access is None:
                self.http_post("https://api.netatmo.com/oauth2/token", {'grant_type': 'password',
                                                                        'username': self.netatmo_username,
                                                                        'password': self.netatmo_password,
                                                                        'client_id': self.netatmo_client_id,
                                                                        'client_secret': self.netatmo_client_secret,
                                                                        'scope': 'read_station'}, self.cache_tokens)
            elif self.token_expiry <= calendar.timegm(time.gmtime()):
                self.http_post("https://api.netatmo.com/oauth2/token", {'grant_type': 'refresh_token',
                                                                        'refresh_token': self.token_refresh,
                                                                        'client_id': os.environ['NETATMO_CLIENT_ID'],
                                                                        'client_secret': os.environ['NETATMO_CLIENT_SECRET']}, self.cache_tokens)
            else:
                self.http_post("https://api.netatmo.com/api/devicelist", {'access_token': self.token_access}, self.push_devicelist)

    # noinspection PyShadowingNames
    def http_post(self, url, data, callback):
        connection_pool = self.config["pool"] if "pool" in self.config else None
        treq.post(url, data, timeout=HTTP_TIMEOUT, pool=connection_pool).addCallbacks(
            lambda response, url=url, callback=callback: self.http_response(response, url, callback),
            errback=lambda error, url=url: logging.getLogger().error(
                "Error processing HTTP GET [{}] with [{}]".format(url, error.getErrorMessage()))
            if logging.getLogger().isEnabledFor(logging.ERROR) else None)

    @staticmethod
    def http_response(response, url, callback):
        if response.code == 200:
            treq.text_content(response).addCallbacks(callback)
        else:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.getLogger().error("Error processing HTTP response [{}] with [{}]".format(url, response.code))

    def cache_tokens(self, text_content):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            time_start = time.time()
        dict_content = json.loads(text_content)
        self.token_access = dict_content["access_token"]
        self.token_refresh = dict_content["refresh_token"]
        self.token_expiry = calendar.timegm(time.gmtime()) + dict_content["expires_in"] - 10 * self.config["poll_seconds"]
        if logging.getLogger().isEnabledFor(logging.INFO):
            logging.getLogger().info("Plugin [netatmo] access tokens cached, refresh [{}]"
                                     .format(time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(self.token_expiry))))
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("Plugin [{}] cache_tokens on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))
        self.poll()

    # noinspection PyBroadException
    def push_devicelist(self, text_content):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            time_start = time.time()
        # noinspection PyBroadException
        try:
            dict_content = json.loads(text_content, parse_float=Decimal)
            bin_timestamp = calendar.timegm(time.gmtime())
            for module in dict_content["body"]["devices"]:
                module_name = ".indoor." + module["module_name"].lower()
                data_timestamp = module["dashboard_data"]["time_utc"]
                self.datum_push(
                    "temperature" + module_name,
                    "current", "point",
                    self.datum_value(module, ["dashboard_data", "Temperature"], factor=10),
                    u"°C",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    self.config["poll_seconds"],
                    "second"
                )
                self.datum_push(
                    "temperature" + module_name,
                    "current", "high",
                    self.datum_value(module, ["dashboard_data", "max_temp"], factor=10),
                    u"°C",
                    10,
                    module["dashboard_data"]["date_max_temp"],
                    bin_timestamp,
                    1,
                    "day"
                )
                self.datum_push(
                    "temperature" + module_name,
                    "current", "low",
                    self.datum_value(module, ["dashboard_data", "min_temp"], factor=10),
                    u"°C",
                    10,
                    module["dashboard_data"]["date_min_temp"],
                    bin_timestamp,
                    1,
                    "day"
                )
                self.datum_push(
                    "humidity" + module_name,
                    "current", "point",
                    self.datum_value(module, ["dashboard_data", "Humidity"]),
                    "%",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    self.config["poll_seconds"],
                    "second",
                    data_bound_upper=100,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "pressure" + module_name,
                    "current", "point",
                    self.datum_value(module, ["dashboard_data", "Pressure"]),
                    "mbar",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    self.config["poll_seconds"],
                    "second",
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "pressureabsolute" + module_name,
                    "current", "point",
                    self.datum_value(module, ["dashboard_data", "AbsolutePressure"]),
                    "mbar",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    self.config["poll_seconds"],
                    "second",
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "carbondioxide" + module_name,
                    "current", "point",
                    self.datum_value(module, ["dashboard_data", "CO2"]),
                    "ppm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    self.config["poll_seconds"],
                    "second",
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                self.datum_push(
                    "noise" + module_name,
                    "current", "point",
                    self.datum_value(module, ["dashboard_data", "Noise"]),
                    "dB",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    self.config["poll_seconds"],
                    "second",
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
            for module in dict_content["body"]["modules"]:
                module_name = (".indoor." if module["type"] == "NAModule4" else ".outdoor.") + module["module_name"].lower()
                data_timestamp = module["dashboard_data"]["time_utc"]
                self.datum_push(
                    "temperature" + module_name,
                    "current", "point",
                    self.datum_value(module, ["dashboard_data", "Temperature"], factor=10),
                    u"°C",
                    10,
                    data_timestamp,
                    bin_timestamp,
                    self.config["poll_seconds"],
                    "second"
                )
                self.datum_push(
                    "temperature" + module_name,
                    "current", "high",
                    self.datum_value(module, ["dashboard_data", "max_temp"], factor=10),
                    u"°C",
                    10,
                    module["dashboard_data"]["date_max_temp"],
                    bin_timestamp,
                    1,
                    "day"
                )
                self.datum_push(
                    "temperature" + module_name,
                    "current", "low",
                    self.datum_value(module, ["dashboard_data", "min_temp"], factor=10),
                    u"°C",
                    10,
                    module["dashboard_data"]["date_min_temp"],
                    bin_timestamp,
                    1,
                    "day"
                )
                self.datum_push(
                    "humidity" + module_name,
                    "current", "point",
                    self.datum_value(module, ["dashboard_data", "Humidity"]),
                    "%",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    self.config["poll_seconds"],
                    "second",
                    data_bound_upper=100,
                    data_bound_lower=0,
                    data_derived_max=True,
                    data_derived_min=True
                )
                if module["type"] == "NAModule4":
                    self.datum_push(
                        "carbondioxide" + module_name,
                        "current", "point",
                        self.datum_value(module, ["dashboard_data", "CO2"]),
                        "ppm",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        self.config["poll_seconds"],
                        "second",
                        data_bound_lower=0,
                        data_derived_max=True,
                        data_derived_min=True
                    )
            self.datum_pop()
        except Exception:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.exception(
                    "Unexpected error processing response [{}]".format(text_content))
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("Plugin [{}] push_devicelist on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))

    def __init__(self, parent, name, config):
        super(Netatmo, self).__init__(parent, name, config)
        self.disabled = False
        self.token_access = None
        self.token_refresh = None
        self.token_expiry = None
        try:
            self.netatmo_username = os.environ['NETATMO_USERNAME']
            self.netatmo_password = os.environ['NETATMO_PASSWORD']
            self.netatmo_client_id = os.environ['NETATMO_CLIENT_ID']
            self.netatmo_client_secret = os.environ['NETATMO_CLIENT_SECRET']
        except KeyError, key_error:
            self.disabled = True
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.getLogger().error("Error getting Netatmo connection key [{}] from environment, disabling plugin".format(key_error))
