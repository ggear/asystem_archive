# -*- coding: utf-8 -*-

from __future__ import print_function

import json
import os.path
import sys

import treq
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase

from anode.anode import main


# noinspection PyUnresolvedReferences
class ANodeTest(TestCase):
    # noinspection PyPep8Naming, PyAttributeOutsideInit
    def setUp(self):
        self.ticks = 0
        self.clock = Clock()
        self.patch(treq, "get", lambda url, timeout=0, pool=None: MockResponse(url))
        self.patch(treq, "post", lambda url, data, timeout=0, pool=None: MockResponse(url))
        self.patch(treq, "text_content", lambda response: MockResponseContent(response))
        print("")

    def clock_tick(self, period, periods):
        for tickTock in range(0, period * periods, period):
            self.clock.advance(period)
            self.ticks += 1

    def clock_tock(self, anode):
        for i in range(self.ticks):
            for source in HTTP_POSTS:
                anode.push_datums({"sources": [source]}, HTTP_POSTS[source])

    def assert_anode(self, callback):
        anode = main(self.clock, callback)
        self.clock_tock(anode)
        self.assertTrue(anode is not None)
        metrics = 0
        metrics_anode = 0
        for metric in json.loads(anode.web_rest.onGet(MockRequest("/rest/?metrics=anode"))):
            metrics_anode += 1
            if metric["data_metric"].endswith("metrics"):
                metrics += metric["data_value"]
        self.assertEquals(0, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?scope=some_nonexistant_scope")))))
        self.assertEquals(0, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=some.nonexistant.metric")))))
        self.assertEquals(0, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=some.nonexistant.metric&metrics=some.other.nonexistant.metric")))))
        self.assertEquals(0, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power&types=some_nonexistant_type")))))
        self.assertEquals(0, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power&types=point&bins=some_nonexistant_bin")))))
        self.assertEquals(0, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?scope=publish")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?limit=1")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?limit=some_nonnumeric_limit&limit=1")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter&bins=1second")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter&types=point")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter&types=point&bins=1second")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter&metrics=&types=point&bins=1second")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter&metrics=some.nonexistant.metric&metrics=&types=point&bins=1second")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter&metrics=&types=point&types=some_nonexistant_type&bins=1second")))))
        self.assertEquals(1, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter&metrics=&types=point&bins=1second&bins=some_nonexistant_bin")))))
        self.assertEquals(2, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?limit=2")))))
        self.assertEquals(2, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter&metrics=power.production.grid&metrics=&types=point&bins=1second")))))
        self.assertEquals(3, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power.production.inverter")))))
        self.assertEquals(12, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?bins=1second")))))
        self.assertEquals(30, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?metrics=power")))))
        self.assertEquals(metrics - metrics_anode, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?scope=history")))))
        self.assertEquals(metrics, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?limit=" + str(metrics))))))
        self.assertEquals(metrics, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?limit=" + str(metrics * 2))))))
        self.assertEquals(metrics, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?limit=some_nonnumeric_limit")))))
        self.assertEquals(metrics, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?scope=last")))))
        self.assertEquals(metrics, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?something=else")))))
        self.assertEquals(metrics, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/?")))))
        self.assertEquals(metrics, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull/")))))
        self.assertEquals(metrics, len(json.loads(anode.web_rest.onGet(MockRequest(
            "/pull")))))

    def test_main_default(self):
        self.patch(sys, "argv", ["anode", "--config=" + FILE_CONFIG])
        self.assert_anode(lambda: self.clock_tick(60 * 60, 2))

    def test_main_verbose_short(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG, "-v"])
        self.assert_anode(lambda: self.clock_tick(60 * 60, 2))

    def test_main_verbose_long(self):
        self.patch(sys, "argv", ["anode", "--config=" + FILE_CONFIG, "--verbose"])
        self.assert_anode(lambda: self.clock_tick(60 * 60, 2))

    def test_main_quiet_short(self):
        self.patch(sys, "argv", ["anode", "-c" + FILE_CONFIG, "-q"])
        self.assert_anode(lambda: self.clock_tick(60 * 60, 2))

    def test_main_quiet_long(self):
        self.patch(sys, "argv", ["anode", "--config=" + FILE_CONFIG, "--quiet"])
        self.assert_anode(lambda: self.clock_tick(60 * 60, 2))


class MockRequest:
    def __init__(self, uri):
        self.uri = uri


class MockResponse:
    def __init__(self, url):
        self.url = url
        self.code = 200 if url in HTTP_GETS else 404

    # noinspection PyPep8Naming,PyUnusedLocal
    def addCallbacks(self, callback, errback=None):
        callback(self)


# noinspection PyPep8
class MockResponseContent:
    def __init__(self, response):
        self.response = response
        self.content = HTTP_GETS[response.url] if response.code == 200 else HTTP_GETS["http_404"]

    # noinspection PyPep8Naming,PyUnusedLocal
    def addCallbacks(self, callback, errback=None):
        callback(self.content)


FILE_CONFIG = os.path.dirname(__file__) + "/../../config/anode.yaml"

HTTP_POSTS = {
    "davis": """{
  "packet": {
    "interval": 2,
    "outHumidity": 44.0,
    "maxSolarRad": 442.6033626669229,
    "consBatteryVoltage": 4.77,
    "monthRain": 0.69,
    "sunrise": 1480429500,
    "heatindex": 80.97340257190011,
    "forecastRule": 187,
    "inDewpoint": 58.625631032841284,
    "inHumidity": 36.0,
    "windSpeed10": 3.0,
    "yearRain": 2.21,
    "inTemp": 89.1,
    "sunaz": 319.890232,
    "sunalt": 77.8923,
    "barometer": 29.856,
    "windchill": 80.9,
    "dewpoint": 56.9159769749953,
    "humidex": 86.79197224086536,
    "forecastIcon": 3,
    "usUnits": 1,
    "appTemp": 81.39028316915696,
    "dateTime": 1480408557,
    "windDir": 167.0,
    "outTemp": 80.9,
    "windSpeed": 3.0,
    "sunset": 1480380060,
    "windGust": 3.0,
    "windGustDir": 167.0,
    "cloudbase": 6080.914323864705
  },
  "record": {
    "windGust": 6.0,
    "altimeter": 29.87586136398276,
    "outHumidity": 43.0,
    "rxCheckPercent": 99.08333333333333,
    "maxSolarRad": 873.0151949589583,
    "consBatteryVoltage": 4.76,
    "windchill": 77.6,
    "windDir": 157.5,
    "lowOutTemp": 76.4,
    "outTemp": 77.6,
    "rainRate": 0.0,
    "forecastRule": 45,
    "inTemp": 74.0,
    "windrun": 27.5,
    "windGustDir": 157.5,
    "barometer": 29.897,
    "txBatteryStatus": 0,
    "dewpoint": 53.310024292660074,
    "rain": 0.0,
    "interval": 30,
    "pressure": 29.210324857457508,
    "ET": 0.0,
    "usUnits": 1,
    "humidex": 81.51717530780654,
    "appTemp": 77.50172316501988,
    "inDewpoint": 52.47528959973715,
    "heatindex": 77.6,
    "dateTime": 1480469400,
    "inHumidity": 47.0,
    "windSpeed": 2.0,
    "highOutTemp": 78.0,
    "cloudbase": 6150.449024395436
  }
}"""
}

# noinspection PyPep8
HTTP_GETS = {
    "http_404": u"""<html><body>HTTP 404</body></html>""",
    "https://api.netatmo.com/oauth2/token": u"""{
  "access_token": "57aec38265d1c4fb068b45b3|7059f974a1ba7ad2ea7f63ab5c51cadc",
  "refresh_token": "57aec38265d1c4fb068b45b3|fa9aa9ad4e8aa4fdb66cae9bb06245aa",
  "scope": [
    "read_station"
  ],
  "expires_in": 10800,
  "expire_in": 10800
}""",
    "https://api.netatmo.com/api/devicelist": u"""{
  "body": {
    "modules": [
      {
        "_id": "03:00:00:02:ed:62",
        "date_setup": {
          "sec": 1471073412,
          "usec": 796000
        },
        "main_device": "70:ee:50:19:43:7c",
        "module_name": "Living",
        "type": "NAModule4",
        "firmware": 44,
        "last_message": 1479982102,
        "last_seen": 1479982083,
        "rf_status": 65,
        "battery_vp": 5655,
        "dashboard_data": {
          "time_utc": 1479982083,
          "Temperature": 30.4,
          "Humidity": 30,
          "CO2": 797,
          "date_max_temp": 1479972649,
          "date_min_temp": 1479937120,
          "min_temp": 17.7,
          "max_temp": 32.6
        },
        "data_type": [
          "Temperature",
          "CO2",
          "Humidity"
        ]
      },
      {
        "_id": "03:00:00:03:00:f4",
        "date_setup": {
          "sec": 1471077028,
          "usec": 514000
        },
        "main_device": "70:ee:50:19:43:7c",
        "module_name": "Parents",
        "type": "NAModule4",
        "firmware": 44,
        "last_message": 1479982102,
        "last_seen": 1479982102,
        "rf_status": 67,
        "battery_vp": 5758,
        "dashboard_data": {
          "time_utc": 1479982051,
          "Temperature": 31.5,
          "Humidity": 29,
          "CO2": 683,
          "date_max_temp": 1479975386,
          "date_min_temp": 1479938319,
          "min_temp": 18.5,
          "max_temp": 32.8
        },
        "data_type": [
          "Temperature",
          "CO2",
          "Humidity"
        ]
      },
      {
        "_id": "03:00:00:02:f6:1e",
        "date_setup": {
          "sec": 1471077094,
          "usec": 820000
        },
        "main_device": "70:ee:50:19:43:7c",
        "module_name": "Edwin",
        "type": "NAModule4",
        "firmware": 44,
        "last_message": 1479982102,
        "last_seen": 1479982102,
        "rf_status": 69,
        "battery_vp": 5713,
        "dashboard_data": {
          "time_utc": 1479982051,
          "Temperature": 31.6,
          "Humidity": 29,
          "CO2": 674,
          "date_max_temp": 1479974207,
          "date_min_temp": 1479940780,
          "min_temp": 20.9,
          "max_temp": 33.4
        },
        "data_type": [
          "Temperature",
          "CO2",
          "Humidity"
        ]
      },
      {
        "_id": "02:00:00:17:d9:c4",
        "date_setup": {
          "sec": 1471071530,
          "usec": 661000
        },
        "main_device": "70:ee:50:19:43:7c",
        "module_name": "Deck",
        "type": "NAModule1",
        "firmware": 44,
        "last_message": 1479982102,
        "last_seen": 1479982096,
        "rf_status": 62,
        "battery_vp": 5898,
        "dashboard_data": {
          "time_utc": 1479982044,
          "Temperature": 28.9,
          "Humidity": 35,
          "date_max_temp": 1479970253,
          "date_min_temp": 1479937133,
          "min_temp": 16.4,
          "max_temp": 33.7
        },
        "data_type": [
          "Temperature",
          "Humidity"
        ]
      }
    ],
    "devices": [
      {
        "_id": "70:ee:50:19:43:7c",
        "access_code": "aWPHphtBDS",
        "cipher_id": "enc:16:8garYr+U5KzkpL9ELA6RKJI+3XhzfXscCQyuryvItEGzpZA28vsyEm86x\/D3TLUZ",
        "co2_calibrating": false,
        "date_setup": {
          "sec": 1471071528,
          "usec": 854000
        },
        "firmware": 124,
        "invitation_disable": false,
        "last_status_store": 1479982107,
        "last_upgrade": 1471071533,
        "module_name": "Graham",
        "modules": [
          "03:00:00:02:ed:62",
          "03:00:00:03:00:f4",
          "03:00:00:02:f6:1e",
          "02:00:00:17:d9:c4"
        ],
        "place": {
          "altitude": 193,
          "city": "Glen Forrest",
          "country": "AU",
          "improveLocProposed": true,
          "location": [
            116.10645,
            -31.9154
          ],
          "timezone": "Australia\/Perth"
        },
        "station_name": "Hardey Road",
        "type": "NAMain",
        "wifi_status": 50,
        "dashboard_data": {
          "AbsolutePressure": 994.5,
          "time_utc": 1479982090,
          "Noise": 52,
          "Temperature": 30.4,
          "Humidity": 30,
          "Pressure": 1017.5,
          "CO2": 752,
          "date_max_temp": 1479973907,
          "date_min_temp": 1479938359,
          "min_temp": 16.9,
          "max_temp": 33
        },
        "data_type": [
          "Temperature",
          "CO2",
          "Humidity",
          "Noise",
          "Pressure"
        ]
      }
    ]
  },
  "status": "ok",
  "time_exec": 0.011563062667847,
  "time_server": 1479982389
}""",
    "http://10.0.1.203/solar_api/v1/GetPowerFlowRealtimeData.fcgi": u"""{
	"Head" : {
		"RequestArguments" : {},
		"Status" : {
			"Code" : 0,
			"Reason" : "",
			"UserMessage" : ""
		},
		"Timestamp" : "2016-11-28T15:44:34+08:00"
	},
	"Body" : {
		"Data" : {
			"Site" : {
				"Mode" : "meter",
				"P_Grid" : -3422.139924,
				"P_Load" : -268.860076,
				"P_Akku" : null,
				"P_PV" : 3939,
				"rel_SelfConsumption" : 7.284207,
				"rel_Autonomy" : 100,
				"E_Day" : 4755,
				"E_Year" : 3060668,
				"E_Total" : 3060676,
				"Meter_Location" : "grid"
			},
			"Inverters" : {
				"1" : {
					"DT" : 99,
					"P" : 3691,
					"E_Day" : 4755,
					"E_Year" : 3060668,
					"E_Total" : 3060676
				}
			}
		}
	}
}""",
    "http://10.0.1.203/solar_api/v1/GetMeterRealtimeData.cgi?Scope=System": u"""{
	"Head" : {
		"RequestArguments" : {
			"DeviceClass" : "Meter",
			"Scope" : "System"
		},
		"Status" : {
			"Code" : 0,
			"Reason" : "",
			"UserMessage" : ""
		},
		"Timestamp" : "2016-11-28T14:20:33+08:00"
	},
	"Body" : {
		"Data" : {
			"0" : {
				"Details" : {
					"Serial" : "16030749",
					"Model" : "Smart Meter 63A",
					"Manufacturer" : "Fronius"
				},
				"TimeStamp" : 1480314032,
				"Enable" : 1,
				"Visible" : 1,
				"PowerReal_P_Sum" : -4493.4099,
				"Meter_Location_Current" : 0,
				"PowerReal_P_Phase_1" : -1341.44997,
				"PowerReal_P_Phase_2" : -1593.679964,
				"PowerReal_P_Phase_3" : -1558.279965,
				"PowerReactive_Q_Sum" : -259.869994,
				"PowerReactive_Q_Phase_1" : -101.859998,
				"PowerReactive_Q_Phase_2" : -51.609999,
				"PowerReactive_Q_Phase_3" : -106.399998,
				"Current_AC_Phase_1" : 5.391,
				"Current_AC_Phase_2" : 6.401,
				"Current_AC_Phase_3" : 6.239,
				"Voltage_AC_Phase_1" : 251.400012,
				"Voltage_AC_Phase_2" : 249.400012,
				"Voltage_AC_Phase_3" : 250.600012,
				"Voltage_AC_PhaseToPhase_12" : 433.700021,
				"Voltage_AC_PhaseToPhase_23" : 433.000021,
				"Voltage_AC_PhaseToPhase_31" : 434.700021,
				"Frequency_Phase_Average" : 50,
				"PowerApparent_S_Sum" : 4499.999899,
				"PowerApparent_S_Phase_1" : 1355.297529,
				"PowerApparent_S_Phase_2" : 1596.409552,
				"PowerApparent_S_Phase_3" : 1563.493549,
				"PowerFactor_Sum" : -0.99,
				"PowerFactor_Phase_1" : -0.99,
				"PowerFactor_Phase_2" : -0.99,
				"PowerFactor_Phase_3" : -0.99,
				"EnergyReal_WAC_Sum_Produced" : 2403382,
				"EnergyReal_WAC_Sum_Consumed" : 702095,
				"EnergyReactive_VArAC_Sum_Produced" : 4864600,
				"EnergyReactive_VArAC_Sum_Consumed" : 414180,
				"EnergyReal_WAC_Plus_Absolute" : 702095,
				"EnergyReal_WAC_Minus_Absolute" : 2403382
			}
		}
	}
}""",
    "http://api.wunderground.com/api/8539276b98b4973b/forecast10day/q/zmw:00000.6.94615.json": """{
  "response": {
  "version":"0.1",
  "termsofService":"http://www.wunderground.com/weather/api/d/terms.html",
  "features": {
  "forecast10day": 1
  }
	}
		,
	"forecast":{
		"txt_forecast": {
		"date":"7:54 AM AWST",
		"forecastday": [
		{
		"period":0,
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"title":"Monday",
		"fcttext":"Sunny. High 84F. Winds WSW at 10 to 20 mph.",
		"fcttext_metric":"Abundant sunshine. High 29C. Winds WSW at 15 to 30 km/h.",
		"pop":"0"
		}
		,
		{
		"period":1,
		"icon":"nt_clear",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_clear.gif",
		"title":"Monday Night",
		"fcttext":"A clear sky. Low 54F. Winds SSW at 10 to 15 mph.",
		"fcttext_metric":"Clear skies. Low 12C. Winds SSW at 15 to 25 km/h.",
		"pop":"10"
		}
		,
		{
		"period":2,
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"title":"Tuesday",
		"fcttext":"Mainly sunny. High around 85F. Winds S at 10 to 20 mph.",
		"fcttext_metric":"Mainly sunny. High around 30C. Winds S at 15 to 25 km/h.",
		"pop":"0"
		}
		,
		{
		"period":3,
		"icon":"nt_clear",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_clear.gif",
		"title":"Tuesday Night",
		"fcttext":"Clear. Low 56F. Winds SE at 10 to 15 mph.",
		"fcttext_metric":"Clear. Low 13C. Winds SE at 15 to 25 km/h.",
		"pop":"0"
		}
		,
		{
		"period":4,
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"title":"Wednesday",
		"fcttext":"Sunny along with a few clouds. High 79F. Winds SW at 15 to 25 mph.",
		"fcttext_metric":"Except for a few afternoon clouds, mainly sunny. High 26C. Winds SW at 15 to 30 km/h.",
		"pop":"0"
		}
		,
		{
		"period":5,
		"icon":"nt_partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_partlycloudy.gif",
		"title":"Wednesday Night",
		"fcttext":"A few clouds. Low 51F. Winds S at 10 to 20 mph.",
		"fcttext_metric":"A few clouds. Low near 10C. Winds S at 15 to 30 km/h.",
		"pop":"10"
		}
		,
		{
		"period":6,
		"icon":"partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/partlycloudy.gif",
		"title":"Thursday",
		"fcttext":"Partly cloudy skies. High 76F. Winds SSW at 10 to 20 mph.",
		"fcttext_metric":"Partly cloudy skies. High 24C. Winds SSW at 15 to 30 km/h.",
		"pop":"0"
		}
		,
		{
		"period":7,
		"icon":"nt_clear",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_clear.gif",
		"title":"Thursday Night",
		"fcttext":"Clear skies. Low 51F. Winds SSE at 10 to 15 mph.",
		"fcttext_metric":"A mostly clear sky. Low around 10C. Winds SSE at 15 to 25 km/h.",
		"pop":"0"
		}
		,
		{
		"period":8,
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"title":"Friday",
		"fcttext":"Plentiful sunshine. High 83F. E winds shifting to SW at 10 to 20 mph.",
		"fcttext_metric":"Generally sunny. High 29C. E winds shifting to SW at 15 to 30 km/h.",
		"pop":"0"
		}
		,
		{
		"period":9,
		"icon":"nt_partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_partlycloudy.gif",
		"title":"Friday Night",
		"fcttext":"Partly cloudy skies. Low 49F. Winds SSW at 10 to 15 mph.",
		"fcttext_metric":"Partly cloudy. Low 9C. Winds SSW at 15 to 25 km/h.",
		"pop":"10"
		}
		,
		{
		"period":10,
		"icon":"partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/partlycloudy.gif",
		"title":"Saturday",
		"fcttext":"Partly cloudy. High 76F. Winds SW at 15 to 25 mph.",
		"fcttext_metric":"Sunshine and clouds mixed. High near 25C. Winds SW at 25 to 40 km/h.",
		"pop":"0"
		}
		,
		{
		"period":11,
		"icon":"nt_mostlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_mostlycloudy.gif",
		"title":"Saturday Night",
		"fcttext":"Mostly cloudy skies. Low 52F. Winds SW at 10 to 15 mph.",
		"fcttext_metric":"Mostly cloudy skies. Low 11C. Winds SW at 15 to 25 km/h.",
		"pop":"20"
		}
		,
		{
		"period":12,
		"icon":"partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/partlycloudy.gif",
		"title":"Sunday",
		"fcttext":"Intervals of clouds and sunshine. High 76F. Winds SSW at 15 to 25 mph.",
		"fcttext_metric":"Sunshine and clouds mixed. High 24C. Winds SSW at 15 to 30 km/h.",
		"pop":"10"
		}
		,
		{
		"period":13,
		"icon":"nt_clear",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_clear.gif",
		"title":"Sunday Night",
		"fcttext":"Clear skies. Low 51F. Winds SSE at 10 to 20 mph.",
		"fcttext_metric":"Clear skies. Low near 10C. Winds SSE at 15 to 25 km/h.",
		"pop":"0"
		}
		,
		{
		"period":14,
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"title":"Monday",
		"fcttext":"Sunny. High 81F. ESE winds shifting to SW at 10 to 20 mph.",
		"fcttext_metric":"Sunny. High 27C. ESE winds shifting to SW at 15 to 25 km/h.",
		"pop":"0"
		}
		,
		{
		"period":15,
		"icon":"nt_clear",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_clear.gif",
		"title":"Monday Night",
		"fcttext":"Clear skies. Low 56F. Winds SE at 10 to 15 mph.",
		"fcttext_metric":"Clear. Low 13C. Winds SE at 15 to 25 km/h.",
		"pop":"0"
		}
		,
		{
		"period":16,
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"title":"Tuesday",
		"fcttext":"Sunny. High 89F. E winds shifting to SSW at 10 to 20 mph.",
		"fcttext_metric":"Sunny skies. High 32C. E winds shifting to SSW at 15 to 30 km/h.",
		"pop":"0"
		}
		,
		{
		"period":17,
		"icon":"nt_clear",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_clear.gif",
		"title":"Tuesday Night",
		"fcttext":"Clear skies. Low near 60F. Winds SE at 10 to 20 mph.",
		"fcttext_metric":"Clear. Low around 15C. Winds SE at 15 to 30 km/h.",
		"pop":"0"
		}
		,
		{
		"period":18,
		"icon":"partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/partlycloudy.gif",
		"title":"Wednesday",
		"fcttext":"Partly cloudy. High 89F. ENE winds shifting to SW at 10 to 20 mph.",
		"fcttext_metric":"Intervals of clouds and sunshine. High 32C. ENE winds shifting to SW at 15 to 30 km/h.",
		"pop":"0"
		}
		,
		{
		"period":19,
		"icon":"nt_partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/nt_partlycloudy.gif",
		"title":"Wednesday Night",
		"fcttext":"A few clouds. Low 61F. Winds SE at 5 to 10 mph.",
		"fcttext_metric":"A few clouds. Low 16C. Winds SE at 10 to 15 km/h.",
		"pop":"0"
		}
		]
		},
		"simpleforecast": {
		"forecastday": [
		{"date":{
	"epoch":"1480330800",
	"pretty":"7:00 PM AWST on November 28, 2016",
	"day":28,
	"month":11,
	"year":2016,
	"yday":332,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"November",
	"monthname_short":"Nov",
	"weekday_short":"Mon",
	"weekday":"Monday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":1,
		"high": {
		"fahrenheit":"84",
		"celsius":"29"
		},
		"low": {
		"fahrenheit":"54",
		"celsius":"12"
		},
		"conditions":"Clear",
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 20,
		"kph": 32,
		"dir": "WSW",
		"degrees": 239
		},
		"avewind": {
		"mph": 15,
		"kph": 24,
		"dir": "WSW",
		"degrees": 239
		},
		"avehumidity": 35,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1480417200",
	"pretty":"7:00 PM AWST on November 29, 2016",
	"day":29,
	"month":11,
	"year":2016,
	"yday":333,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"November",
	"monthname_short":"Nov",
	"weekday_short":"Tue",
	"weekday":"Tuesday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":2,
		"high": {
		"fahrenheit":"85",
		"celsius":"29"
		},
		"low": {
		"fahrenheit":"56",
		"celsius":"13"
		},
		"conditions":"Clear",
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 20,
		"kph": 32,
		"dir": "S",
		"degrees": 187
		},
		"avewind": {
		"mph": 13,
		"kph": 21,
		"dir": "S",
		"degrees": 187
		},
		"avehumidity": 39,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1480503600",
	"pretty":"7:00 PM AWST on November 30, 2016",
	"day":30,
	"month":11,
	"year":2016,
	"yday":334,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"November",
	"monthname_short":"Nov",
	"weekday_short":"Wed",
	"weekday":"Wednesday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":3,
		"high": {
		"fahrenheit":"79",
		"celsius":"26"
		},
		"low": {
		"fahrenheit":"51",
		"celsius":"11"
		},
		"conditions":"Clear",
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 25,
		"kph": 40,
		"dir": "SW",
		"degrees": 221
		},
		"avewind": {
		"mph": 17,
		"kph": 27,
		"dir": "SW",
		"degrees": 221
		},
		"avehumidity": 42,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1480590000",
	"pretty":"7:00 PM AWST on December 01, 2016",
	"day":1,
	"month":12,
	"year":2016,
	"yday":335,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"December",
	"monthname_short":"Dec",
	"weekday_short":"Thu",
	"weekday":"Thursday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":4,
		"high": {
		"fahrenheit":"76",
		"celsius":"24"
		},
		"low": {
		"fahrenheit":"51",
		"celsius":"11"
		},
		"conditions":"Partly Cloudy",
		"icon":"partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/partlycloudy.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 20,
		"kph": 32,
		"dir": "SSW",
		"degrees": 211
		},
		"avewind": {
		"mph": 16,
		"kph": 26,
		"dir": "SSW",
		"degrees": 211
		},
		"avehumidity": 42,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1480676400",
	"pretty":"7:00 PM AWST on December 02, 2016",
	"day":2,
	"month":12,
	"year":2016,
	"yday":336,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"December",
	"monthname_short":"Dec",
	"weekday_short":"Fri",
	"weekday":"Friday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":5,
		"high": {
		"fahrenheit":"83",
		"celsius":"28"
		},
		"low": {
		"fahrenheit":"49",
		"celsius":"9"
		},
		"conditions":"Clear",
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 20,
		"kph": 32,
		"dir": "S",
		"degrees": 191
		},
		"avewind": {
		"mph": 16,
		"kph": 26,
		"dir": "S",
		"degrees": 191
		},
		"avehumidity": 34,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1480762800",
	"pretty":"7:00 PM AWST on December 03, 2016",
	"day":3,
	"month":12,
	"year":2016,
	"yday":337,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"December",
	"monthname_short":"Dec",
	"weekday_short":"Sat",
	"weekday":"Saturday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":6,
		"high": {
		"fahrenheit":"76",
		"celsius":"24"
		},
		"low": {
		"fahrenheit":"52",
		"celsius":"11"
		},
		"conditions":"Partly Cloudy",
		"icon":"partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/partlycloudy.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 25,
		"kph": 40,
		"dir": "SW",
		"degrees": 232
		},
		"avewind": {
		"mph": 17,
		"kph": 27,
		"dir": "SW",
		"degrees": 232
		},
		"avehumidity": 44,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1480849200",
	"pretty":"7:00 PM AWST on December 04, 2016",
	"day":4,
	"month":12,
	"year":2016,
	"yday":338,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"December",
	"monthname_short":"Dec",
	"weekday_short":"Sun",
	"weekday":"Sunday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":7,
		"high": {
		"fahrenheit":"76",
		"celsius":"24"
		},
		"low": {
		"fahrenheit":"51",
		"celsius":"11"
		},
		"conditions":"Partly Cloudy",
		"icon":"partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/partlycloudy.gif",
		"skyicon":"",
		"pop":10,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 25,
		"kph": 40,
		"dir": "SSW",
		"degrees": 212
		},
		"avewind": {
		"mph": 17,
		"kph": 27,
		"dir": "SSW",
		"degrees": 212
		},
		"avehumidity": 42,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1480935600",
	"pretty":"7:00 PM AWST on December 05, 2016",
	"day":5,
	"month":12,
	"year":2016,
	"yday":339,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"December",
	"monthname_short":"Dec",
	"weekday_short":"Mon",
	"weekday":"Monday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":8,
		"high": {
		"fahrenheit":"81",
		"celsius":"27"
		},
		"low": {
		"fahrenheit":"56",
		"celsius":"13"
		},
		"conditions":"Clear",
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 20,
		"kph": 32,
		"dir": "S",
		"degrees": 171
		},
		"avewind": {
		"mph": 14,
		"kph": 23,
		"dir": "S",
		"degrees": 171
		},
		"avehumidity": 34,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1481022000",
	"pretty":"7:00 PM AWST on December 06, 2016",
	"day":6,
	"month":12,
	"year":2016,
	"yday":340,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"December",
	"monthname_short":"Dec",
	"weekday_short":"Tue",
	"weekday":"Tuesday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":9,
		"high": {
		"fahrenheit":"89",
		"celsius":"32"
		},
		"low": {
		"fahrenheit":"60",
		"celsius":"16"
		},
		"conditions":"Clear",
		"icon":"clear",
		"icon_url":"http://icons.wxug.com/i/c/k/clear.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 20,
		"kph": 32,
		"dir": "SSE",
		"degrees": 148
		},
		"avewind": {
		"mph": 13,
		"kph": 21,
		"dir": "SSE",
		"degrees": 148
		},
		"avehumidity": 27,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		,
		{"date":{
	"epoch":"1481108400",
	"pretty":"7:00 PM AWST on December 07, 2016",
	"day":7,
	"month":12,
	"year":2016,
	"yday":341,
	"hour":19,
	"low":"00",
	"sec":0,
	"isdst":"0",
	"monthname":"December",
	"monthname_short":"Dec",
	"weekday_short":"Wed",
	"weekday":"Wednesday",
	"ampm":"PM",
	"tz_short":"AWST",
	"tz_long":"Australia/Perth"
},
		"period":10,
		"high": {
		"fahrenheit":"89",
		"celsius":"32"
		},
		"low": {
		"fahrenheit":"61",
		"celsius":"16"
		},
		"conditions":"Partly Cloudy",
		"icon":"partlycloudy",
		"icon_url":"http://icons.wxug.com/i/c/k/partlycloudy.gif",
		"skyicon":"",
		"pop":0,
		"qpf_allday": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_day": {
		"in": 0.00,
		"mm": 0
		},
		"qpf_night": {
		"in": 0.00,
		"mm": 0
		},
		"snow_allday": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_day": {
		"in": 0.0,
		"cm": 0.0
		},
		"snow_night": {
		"in": 0.0,
		"cm": 0.0
		},
		"maxwind": {
		"mph": 20,
		"kph": 32,
		"dir": "ESE",
		"degrees": 120
		},
		"avewind": {
		"mph": 14,
		"kph": 23,
		"dir": "ESE",
		"degrees": 120
		},
		"avehumidity": 28,
		"maxhumidity": 0,
		"minhumidity": 0
		}
		]
		}
	}
}
"""
}
