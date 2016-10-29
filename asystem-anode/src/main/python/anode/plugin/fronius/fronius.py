from anode.plugin.plugin import Plugin


class Fronius(Plugin):
    def loop(self):
        super(self.__class__, self).loop()
