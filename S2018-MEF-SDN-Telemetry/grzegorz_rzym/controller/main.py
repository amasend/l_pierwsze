#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

from ryu.cmd import manager


def main():
    sys.argv.append('--ofp-listen-host')
    sys.argv.append('192.168.30.2')
    sys.argv.append('--ofp-tcp-listen-port')
    sys.argv.append('6633')
    sys.argv.append('controller_main')
    # sys.argv.append('--verbose')
    # sys.argv.append('--enable-debugger')
    manager.main()

if __name__ == '__main__':
    main()