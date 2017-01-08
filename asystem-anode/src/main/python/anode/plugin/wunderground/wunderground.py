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
    def _poll(self):
        self.http_get("http://api.wunderground.com/api/8539276b98b4973b/forecast10day/q/zmw:00000.6.94615.json", self.push_forecast)

    # noinspection PyShadowingNames
    def http_get(self, url, callback):
        connection_pool = self.config["pool"] if "pool" in self.config else None
        treq.get(url, timeout=HTTP_TIMEOUT, pool=connection_pool).addCallbacks(
            lambda response, url=url, callback=callback: self.http_response(response, url, callback),
            errback=lambda error, url=url: logging.getLogger().error(
                "state\t\tError processing HTTP GET [{}] with [{}]".format(url, error.getErrorMessage()))
            if logging.getLogger().isEnabledFor(logging.ERROR) else None)

    @staticmethod
    def http_response(response, url, callback):
        if response.code == 200:
            treq.text_content(response).addCallbacks(callback)
        else:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.getLogger().error("state\t\tError processing HTTP response [{}] with [{}]".format(url, response.code))

    def push_forecast(self, text_content):
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            time_start = time.time()
        # noinspection PyBroadException
        try:
            dict_content = json.loads(text_content, parse_float=Decimal)
            bin_timestamp = calendar.timegm(time.gmtime())
            data_timestamp = int(calendar.timegm(dateutil.parser.parse(dict_content["forecast"]["txt_forecast"]["date"]).timetuple()))
            day_index_start = 0
            day_index_finish = 3
            while day_index_start <= 10 and datetime.datetime.today().strftime('%A') != \
                    dict_content["forecast"]["simpleforecast"]["forecastday"][day_index_start]["date"]["weekday"]:
                day_index_start += 1
            for forecast_index in range(day_index_start, day_index_start + day_index_finish):
                self.datum_push(
                    "conditions.hills.forecast",
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
                    "temperature.hills.forecast",
                    "forecast", "point",
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
                    "temperature.hills.forecast",
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
                    "temperature.hills.forecast",
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
                    "rain.hills.forecast",
                    "forecast", "integral",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_allday", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain.hills.forecast",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_allday", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain.hills.forecast",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_allday", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain.hills.forecast",
                    "forecast", "integral",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_day", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "lighthours",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain.hills.forecast",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_day", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "lighthours",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain.hills.forecast",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_day", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "lighthours",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain.hills.forecast",
                    "forecast", "integral",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_night", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "darkhours",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain.hills.forecast",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_night", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "darkhours",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain.hills.forecast",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_night", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "darkhours",
                    data_bound_lower=0
                )
                self.datum_push(
                    "wind.hills.forecast",
                    "forecast", "mean",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avewind", "kph"]),
                    "km/h",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "wind.hills.forecast",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "maxwind", "kph"]),
                    "km/h",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "wind.hills.forecast",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avewind", "kph"]),
                    "km/h",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "humidity.hills.forecast",
                    "forecast", "mean",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avehumidity"]),
                    "%",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_upper=100,
                    data_bound_lower=0
                )
                self.datum_push(
                    "humidity.hills.forecast",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avehumidity"]),
                    "%",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_upper=100,
                    data_bound_lower=0
                )
                self.datum_push(
                    "humidity.hills.forecast",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avehumidity"]),
                    "%",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_upper=100,
                    data_bound_lower=0
                )
            self.datum_pop()
        except Exception:
            if logging.getLogger().isEnabledFor(logging.ERROR):
                logging.exception(
                    "state\t\tUnexpected error processing response [{}]".format(text_content))
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.getLogger().debug("perf\t\tPlugin [{}] push_forecast on-thread [{}] ms".format(self.name, str(int((time.time() - time_start) * 1000))))
