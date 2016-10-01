from anode.plugin.plugin import Plugin


class Fronius(Plugin):
    def __init__(self, config):
        self.config = config
        if not self.config['quiet']:
            print __name__ + ":init"

    def loop(self):
        if not self.config['quiet']:
            print __name__ + ":loop"
