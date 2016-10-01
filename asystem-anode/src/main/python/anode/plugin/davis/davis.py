from anode.plugin.plugin import Plugin


class Davis(Plugin):
    def __init__(self, config):
        super(self.__class__, self).__init__(config)

    def loop(self):
        super(self.__class__, self).loop
