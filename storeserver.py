#!/usr/bin/env python
# encoding: utf-8
"""
storeserver.py

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
from collections import defaultdict

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from locator.ttypes import Location
#from locator import Locator
import location
from diststore import Store

DEFAULTPORT = 9900

def remote_call(destination, method, *args):
    transport = TSocket.TSocket(destination.address, destination.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = Store.Client(protocol)
    try:
        transport.open()
    except Thrift.TException, tx:
        raise location.NodeNotFound(destination)
    out = getattr(client, method)(*args)
    transport.close()
    return out

class StoreHandler(location.LocatorHandler):
    def __init__(self, peer=None, port=9900):
        location.LocatorHandler.__init__(self, peer, port)
        self.store = defaultdict(str)
    
    def get(self, key):
        """
        Parameters:
         - key
        """
        dest = self.get_node(key)
        if location.loc2str(dest) == self.here:
            if key in self.store:
                print 'found %s' % key
            return self.store[key]
        else:
            try:
                return remote_call(dest, 'get', key)
            except location.NodeNotFound, tx:
                self.remove(tx.location, map(location.str2loc, self.ring.nodes))
                return ''
        
    def put(self, key, value):
        """
        Parameters:
         - key
         - value
        """
        dest = self.get_node(key)
        if location.loc2str(dest) == self.here:
            print 'received %s' % key
            self.store[key] = value
            return
        else:
            try:
                return remote_call(dest, 'put', key, value)
            except location.NodeNotFound, tx:
                self.remove(tx.location, map(location.str2loc, self.ring.nodes))
                return None
    
    def debug(self):
        a = "self.location: %r\n" % self.location
        a += "self.ring.nodes:\n%r\n" % self.ring.nodes
        a += "self.store:\n%r\n" % self.store
        print a
    

#
def main(inputargs):
    handler = StoreHandler(**inputargs)
    processor = Store.Processor(handler)
    transport = TSocket.TServerSocket(handler.port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    
    handler.local_join()
    
    print 'Starting the server at %s...' % (handler.here)
    try:
        server.serve()
    finally:
        handler.cleanup()
    print 'done.'

if __name__ == '__main__':
    inputargs = {}
    try:
        inputargs['port'] = int(sys.argv[-1])
        inputargs['peer'] = location.str2loc(sys.argv[-2])
    except:
        pass
    if 'port' not in inputargs:
        loc = location.ping_until_not_found(Location('localhost', DEFAULTPORT), 25)
        inputargs['port'] = loc.port
    if 'peer' not in inputargs and inputargs['port'] != DEFAULTPORT:
        try:
            loc = location.ping_until_found(Location('localhost', DEFAULTPORT))
            inputargs['peer'] = loc
        except location.NodeNotFound:
            print 'No peer autodiscovered.'
    main(inputargs)

