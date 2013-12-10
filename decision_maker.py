#! /usr/bin/env python

import component
import config

class DecisionMaker(object):
    def jizz(self):
        return 'jizz'

if __name__ == '__main__':
    component.build_rpc_server_from_component(DecisionMaker()).serve_forever()

