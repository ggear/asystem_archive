from __future__ import print_function

import treq
from treq import text_content
from treq.api import get
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.trial.unittest import TestCase
from twisted.web.client import HTTPConnectionPool

TEST_URL = "http://10.0.1.203/solar_api/v1/GetPowerFlowRealtimeData.fcgi"
TEST_URL_CODE = 200
# noinspection PyPep8
TEST_URL_TEXT = """{
  "Head" : {
    "RequestArguments" : {},
    "Status" : {
      "Code" : 0,
      "Reason" : "",
      "UserMessage" : ""
    },
    "Timestamp" : "2016-11-05T13:05:01+08:00"
  },
  "Body" : {
    "Data" : {
      "Site" : {
        "Mode" : "meter",
        "P_Grid" : -4630.06,
        "P_Load" : -410.94,
        "P_Akku" : null,
        "P_PV" : 5340,
        "E_Day" : 23547,
        "E_Year" : 2245403.2,
        "E_Total" : 2245411.2,
        "Meter_Location" : "grid"
      },
      "Inverters" : {
        "1" : {
          "DT" : 99,
          "P" : 5041
        }
      }
    }
  }
}"""


class ANodeTestHttp(TestCase):
    def test_http(self):
        get(TEST_URL, pool=HTTPConnectionPool(reactor, False)).addCallback(self.process_response)
        reactor.run()

    @inlineCallbacks
    def process_response(self, response):
        response_code = response.code
        response_text = yield text_content(response)
        print('Code: {}, Length: {}'.format(response_code, len(response_text), response_text))
        reactor.stop()

    def test_http_mock(self):
        self.patch(treq, 'get', lambda url: MockResponse(url))
        self.patch(treq, 'text_content', lambda response: response.text)
        treq.get(TEST_URL).addCallback(lambda response: print(response))


# noinspection PyPep8Naming
class MockResponse:
    def __init__(self, url):
        self.url = url
        self.code = TEST_URL_CODE
        self.text = TEST_URL_TEXT

    def __repr__(self):
        return 'Code: {}, Length: {}'.format(self.code, len(self.text))

    def addCallback(self, callback):
        callback(self)
