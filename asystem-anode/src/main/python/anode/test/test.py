import sys

from anode.anode import main
from mock import patch
from twisted.internet.task import Clock
from twisted.trial.unittest import TestCase


# noinspection PyUnresolvedReferences
class ANodeTest(TestCase):
    # noinspection PyPep8Naming
    # noinspection PyAttributeOutsideInit
    def setUp(self):
        self.clock = Clock()

    def tick_tock(self, period, periods):
        for tickTock in range(0, period * periods, period):
            self.clock.advance(period)

    def test_main_default(self):
        with patch.object(sys, 'argv', ["anode"]):
            main(self.clock, lambda: self.tick_tock(1, 3))

    def test_main_quiet_short(self):
        with patch.object(sys, 'argv', ["anode", "-q"]):
            main(self.clock, lambda: self.tick_tock(1, 1))

    def test_main_quiet_long(self):
        with patch.object(sys, 'argv', ["anode", "--quiet"]):
            main(self.clock, lambda: self.tick_tock(1, 1))
