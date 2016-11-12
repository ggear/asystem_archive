# -*- coding: utf-8 -*-

from __future__ import print_function

import sys

import treq
from anode.anode import main
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase


# noinspection PyUnresolvedReferences
class ANodeTest(TestCase):
    # noinspection PyPep8Naming, PyAttributeOutsideInit
    def setUp(self):
        self.clock = Clock()
        self.patch(treq, "get", lambda url, timeout=0, pool=None: MockResponse(url))
        self.patch(treq, "text_content", lambda response: MockResponseContent(response))
        print("")

    def tick_tock(self, period, periods):
        for tickTock in range(0, period * periods, period):
            self.clock.advance(period)

    def test_main_default(self):
        self.patch(sys, "argv", ["anode"])
        anode = main(self.clock, lambda: self.tick_tock(1, 1))
        self.assertTrue(anode is not None)

    def test_main_verbose_short(self):
        self.patch(sys, "argv", ["anode", "-v"])
        anode = main(self.clock, lambda: self.tick_tock(1, 1))
        self.assertTrue(anode is not None)

    def test_main_verbose_long(self):
        self.patch(sys, "argv", ["anode", "--verbose"])
        anode = main(self.clock, lambda: self.tick_tock(1, 1))
        self.assertTrue(anode is not None)

    def test_main_quiet_short(self):
        self.patch(sys, "argv", ["anode", "-q"])
        anode = main(self.clock, lambda: self.tick_tock(1, 1))
        self.assertTrue(anode is not None)

    def test_main_quiet_long(self):
        self.patch(sys, "argv", ["anode", "--quiet"])
        anode = main(self.clock, lambda: self.tick_tock(1, 1))
        self.assertTrue(anode is not None)


class MockResponse:
    def __init__(self, url):
        self.url = url
        self.code = 200 if url in HTTP_RESPONSES else 404

    # noinspection PyPep8Naming,PyUnusedLocal
    def addCallbacks(self, callback, errback=None):
        callback(self)


# noinspection PyPep8
class MockResponseContent:
    def __init__(self, response):
        self.response = response
        self.content = HTTP_RESPONSES[response.url] if response.code == 200 else HTTP_RESPONSES["http_404"]

    # noinspection PyPep8Naming,PyUnusedLocal
    def addCallbacks(self, callback, errback=None):
        callback(self.content)


HTTP_RESPONSES = {
    "http_404": u"""<html><body>HTTP 404</body></html>""",
    "http://10.0.1.203/solar_api/v1/GetPowerFlowRealtimeData.fcgi": u"""{
  "Head" : {
    "RequestArguments" : {},
    "Status" : {
      "Code" : 0,
      "Reason" : "",
      "UserMessage" : ""
    },
    "Timestamp" : "2016-11-08T14:41:29+08:00"
  },
  "Body" : {
    "Data" : {
      "Site" : {
        "Mode" : "meter",
        "P_Grid" : -4186.23,
        "P_Load" : -208.77,
        "P_Akku" : null,
        "P_PV" : 4690,
        "E_Day" : 32512,
        "E_Year" : 2364995.5,
        "E_Total" : 2365003.5,
        "Meter_Location" : "grid"
      },
      "Inverters" : {
        "1" : {
          "DT" : 99,
          "P" : 4395
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
    "Timestamp" : "2016-11-08T14:41:31+08:00"
  },
  "Body" : {
    "Data" : {
      "0" : {
        "Details" : {
          "Serial" : "16030749",
          "Model" : "Fronius Smart Meter 63A",
          "Manufacturer" : "Fronius"
        },
        "TimeStamp" : 1478587290,
        "Enable" : 1,
        "Visible" : 1,
        "PowerReal_P_Sum" : -4160,
        "Meter_Location_Current" : 0,
        "PowerReal_P_Phase_1" : -1317.9,
        "PowerReal_P_Phase_2" : -1462.74,
        "PowerReal_P_Phase_3" : -1379.36,
        "PowerReactive_Q_Sum" : -277.14,
        "PowerReactive_Q_Phase_1" : -153.52,
        "PowerReactive_Q_Phase_2" : -34.9,
        "PowerReactive_Q_Phase_3" : -88.72,
        "Current_AC_Phase_1" : 5.273,
        "Current_AC_Phase_2" : 5.761,
        "Current_AC_Phase_3" : 5.496,
        "Voltage_AC_Phase_1" : 253.7,
        "Voltage_AC_Phase_2" : 254.9,
        "Voltage_AC_Phase_3" : 252.5,
        "Voltage_AC_PhaseToPhase_12" : 440.5,
        "Voltage_AC_PhaseToPhase_23" : 439.4,
        "Voltage_AC_PhaseToPhase_31" : 438.4,
        "Frequency_Phase_Average" : 50,
        "PowerApparent_S_Sum" : 4169,
        "PowerFactor_Sum" : -0.99,
        "PowerFactor_Phase_1" : -0.99,
        "PowerFactor_Phase_2" : -0.99,
        "PowerFactor_Phase_3" : -0.99,
        "EnergyReal_WAC_Sum_Produced" : 1848359,
        "EnergyReal_WAC_Sum_Consumed" : 609060,
        "EnergyReactive_VArAC_Sum_Produced" : 3977540,
        "EnergyReactive_VArAC_Sum_Consumed" : 339150,
        "EnergyReal_WAC_Plus_Absolute" : 609060,
        "EnergyReal_WAC_Minus_Absolute" : 1848359
      }
    }
  }
}"""
}
