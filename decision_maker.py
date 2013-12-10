#! /usr/bin/env python

import framework
import config

class DecisionMaker(object):
    def jizz(self):
        return 'jizz'

if __name__ == '__main__':
    framework.build_rpc_server_from_framework(DecisionMaker()).serve_forever()

