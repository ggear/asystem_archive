import sys

from mock import patch
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase

from anode.anode import main


class ANodeTest(TestCase):
    def setUp(self):
        print 'setUp'

    def tearDown(self):
        print 'tearDown'

    def test_main_default(self):
        with patch.object(sys, 'argv', ["anode"]):
            main(Clock())

    def test_main_quiet_short(self):
        with patch.object(sys, 'argv', ["anode", "-q"]):
            main(Clock())

    def test_main_quiet_long(self):
        with patch.object(sys, 'argv', ["anode", "--quiet"]):
            main(Clock())

