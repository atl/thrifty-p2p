#!/usr/bin/env python
# encoding: utf-8
"""
storetest.py

Created by Adam T. Lindsay on 2009-05-18.

The MIT License

Copyright (c) 2009 Adam T. Lindsay.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""

import sys
sys.path.append('gen-py')

from locator.ttypes import Location
from storeserver import remote_call, DEFAULTPORT, parser
from location import ping_until_found, str2loc, NodeNotFound

usage = '''
  python %prog

Looks for a storage node at PEER, either as specified, or 
auto-discovered on the localhost starting from the default 
port. Sends a bunch of keys for resolution.'''

parser.set_usage(usage)
parser.remove_option('--port')

KEYS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'

if __name__ == '__main__':
    (options, args) = parser.parse_args()
    if options.peer:
        loc = str2loc(options.peer)
    else:
        try:
            loc = ping_until_found(Location('localhost', DEFAULTPORT))
        except NodeNotFound:
            print 'No peer autodiscovered.'
            sys.exit()
    for key in KEYS:
        value = remote_call(loc, 'get', key)
        if value:
            print value
        else:
            print "None received from expected %s" % remote_call(loc, 'get_node', key)
