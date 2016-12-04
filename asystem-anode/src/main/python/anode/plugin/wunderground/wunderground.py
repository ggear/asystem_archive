# -*- coding: utf-8 -*-

from __future__ import print_function

import calendar
import datetime
import json
import logging
import time
from decimal import Decimal

import dateutil.parser
import treq
from anode.plugin.plugin import Plugin

HTTP_TIMEOUT = 10


class Wunderground(Plugin):
    def poll(self):
        self.http_get("http://api.wunderground.com/api/8539276b98b4973b/forecast10day/q/zmw:00000.6.94615.json", self.push_forecast)

    # noinspection PyShadowingNames
    def http_get(self, url, callback):
        connection_pool = self.config["pool"] if "pool" in self.config else None
        treq.get(url, timeout=HTTP_TIMEOUT, pool=connection_pool).addCallbacks(
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

    def push_forecast(self, text_content):
        # noinspection PyBroadException
        try:
            dict_content = json.loads(text_content, parse_float=Decimal)
            bin_timestamp = calendar.timegm(time.gmtime())
            data_timestamp = int(time.mktime(dateutil.parser.parse(dict_content["forecast"]["txt_forecast"]["date"]).timetuple()))
            day_index_start = 0
            day_index_finish = 3
            while day_index_start < 10 and datetime.datetime.today().day != \
                    dict_content["forecast"]["simpleforecast"]["forecastday"][day_index_start]["date"]["day"]:
                day_index_start += 1
            if (day_index_start + day_index_finish) < 10:
                for forecast_index in range(day_index_start, day_index_start + day_index_finish):
                    self.datum_push(
                        "conditions.outdoor.forecast",
                        "forecast", "enumeration",
                        0,
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "conditions"]).lower(),
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "day"
                    )
                    self.datum_push(
                        "temperature.outdoor.forecast",
                        "forecast", "high",
                        None if self.datum_value(dict_content,
                                                 ["forecast", "simpleforecast", "forecastday", forecast_index, "high", "celsius"]) is None else int(
                            self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "high", "celsius"])),
                        u"°C",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "day"
                    )
                    self.datum_push(
                        "temperature.outdoor.forecast",
                        "forecast", "low",
                        None if self.datum_value(dict_content,
                                                 ["forecast", "simpleforecast", "forecastday", forecast_index, "low", "celsius"]) is None else int(
                            self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "low", "celsius"])),
                        u"°C",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "day"
                    )
                    self.datum_push(
                        "rain.outdoor.forecast",
                        "forecast", "integral",
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_allday", "mm"]),
                        "mm",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "day"
                    )
                    self.datum_push(
                        "rain.outdoor.forecast",
                        "forecast", "integral",
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_day", "mm"]),
                        "mm",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "daytime"
                    )
                    self.datum_push(
                        "rain.outdoor.forecast",
                        "forecast", "integral",
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_night", "mm"]),
                        "mm",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "nighttime"
                    )
                    self.datum_push(
                        "wind.outdoor.forecast",
                        "forecast", "high",
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "maxwind", "kph"]),
                        "km/h",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "day"
                    )
                    self.datum_push(
                        "wind.outdoor.forecast",
                        "forecast", "average",
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avewind", "kph"]),
                        "km/h",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "day"
                    )
                    self.datum_push(
                        "humidity.outdoor.forecast",
                        "forecast", "average",
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avehumidity"]),
                        "%",
                        1,
                        data_timestamp,
                        bin_timestamp,
                        forecast_index - day_index_start + 1,
                        "day"
                    )
            self.datum_pop()
        except Exception:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.exception(
                    "Unexpected error processing response [{}]".format(text_content))
