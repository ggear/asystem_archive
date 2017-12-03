from __future__ import print_function

import calendar
import json
import logging
from decimal import Decimal

import datetime
import dateutil.parser
import treq

import anode
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
            errback=lambda error, url=url: anode.Log(logging.ERROR).log("Plugin", "error",
                                                                        lambda: "[{}] error processing HTTP GET [{}] with [{}]".format(
                                                                            self.name, url, error.getErrorMessage())))

    def http_response(self, response, url, callback):
        if response.code == 200:
            treq.text_content(response).addCallbacks(callback)
        else:
            anode.Log(logging.ERROR).log("Plugin", "error",
                                         lambda: "[{}] error processing HTTP response [{}] with [{}]".format(self.name, url, response.code))

    def push_forecast(self, content):
        log_timer = anode.Log(logging.DEBUG).start()
        # noinspection PyBroadException
        try:
            dict_content = json.loads(content, parse_float=Decimal)
            bin_timestamp = self.get_time()
            data_timestamp = int(calendar.timegm(dateutil.parser.parse(dict_content["forecast"]["txt_forecast"]["date"]).timetuple()))
            day_index_start = 0
            day_index_finish = 3
            while day_index_start <= 10 and datetime.datetime.today().strftime('%A') != \
                    dict_content["forecast"]["simpleforecast"]["forecastday"][day_index_start]["date"]["weekday"]:
                day_index_start += 1
            for forecast_index in range(day_index_start, day_index_start + day_index_finish):
                self.datum_push(
                    "conditions__forecast__glen_Dforrest",
                    "forecast", "enumeration",
                    0,
                    "__",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_string=self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "conditions"])
                    .lower().encode("ascii", "ignore"),
                )
                self.datum_push(
                    "temperature__forecast__glen_Dforrest",
                    "forecast", "point",
                    None if self.datum_value(dict_content,
                                             ["forecast", "simpleforecast", "forecastday", forecast_index, "high",
                                              "celsius"]) is None else int(
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "high", "celsius"])),
                    "_PC2_PB0C",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day"
                )
                self.datum_push(
                    "temperature__forecast__glen_Dforrest",
                    "forecast", "high",
                    None if self.datum_value(dict_content,
                                             ["forecast", "simpleforecast", "forecastday", forecast_index, "high",
                                              "celsius"]) is None else int(
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "high", "celsius"])),
                    "_PC2_PB0C",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day"
                )
                self.datum_push(
                    "temperature__forecast__glen_Dforrest",
                    "forecast", "low",
                    None if self.datum_value(dict_content,
                                             ["forecast", "simpleforecast", "forecastday", forecast_index, "low",
                                              "celsius"]) is None else int(
                        self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "low", "celsius"])),
                    "_PC2_PB0C",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day"
                )
                self.datum_push(
                    "rain__forecast__glen_Dforrest",
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
                    "rain__forecast__glen_Dforrest",
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
                    "rain__forecast__glen_Dforrest",
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
                    "rain__forecast__glen_Dforrest",
                    "forecast", "integral",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_day", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day_Dtime",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain__forecast__glen_Dforrest",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_day", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day_Dtime",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain__forecast__glen_Dforrest",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_day", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day_Dtime",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain__forecast__glen_Dforrest",
                    "forecast", "integral",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_night", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "night_Dtime",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain__forecast__glen_Dforrest",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_night", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "night_Dtime",
                    data_bound_lower=0
                )
                self.datum_push(
                    "rain__forecast__glen_Dforrest",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "qpf_night", "mm"]),
                    "mm",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "night_Dtime",
                    data_bound_lower=0
                )
                self.datum_push(
                    "wind__forecast__glen_Dforrest",
                    "forecast", "mean",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avewind", "kph"]),
                    "km_P2Fh",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "wind__forecast__glen_Dforrest",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "maxwind", "kph"]),
                    "km_P2Fh",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "wind__forecast__glen_Dforrest",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avewind", "kph"]),
                    "km_P2Fh",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_lower=0
                )
                self.datum_push(
                    "humidity__forecast__glen_Dforrest",
                    "forecast", "mean",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avehumidity"]),
                    "_P25",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_upper=100,
                    data_bound_lower=0
                )
                self.datum_push(
                    "humidity__forecast__glen_Dforrest",
                    "forecast", "high",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avehumidity"]),
                    "_P25",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_upper=100,
                    data_bound_lower=0
                )
                self.datum_push(
                    "humidity__forecast__glen_Dforrest",
                    "forecast", "low",
                    self.datum_value(dict_content, ["forecast", "simpleforecast", "forecastday", forecast_index, "avehumidity"]),
                    "_P25",
                    1,
                    data_timestamp,
                    bin_timestamp,
                    forecast_index - day_index_start + 1,
                    "day",
                    data_bound_upper=100,
                    data_bound_lower=0
                )
            self.publish()
        except Exception as exception:
            anode.Log(logging.ERROR).log("Plugin", "error", lambda: "[{}] error [{}] processing response:\n{}"
                                         .format(self.name, exception, content), exception)
        log_timer.log("Plugin", "timer", lambda: "[{}]".format(self.name), context=self.push_forecast)
