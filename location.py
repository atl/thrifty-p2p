#!/usr/bin/env python
# encoding: utf-8
"""
location.py

Created by Adam T. Lindsay on 2009-05-16.

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
import socket 
from collections import defaultdict
from math import sqrt
from time import sleep
from optparse import OptionParser, make_option
from functools import partial


from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

from locator.ttypes import *
from locator import Locator, Base
from hash_ring import HashRing

DEFAULTPORT = 9900
WAITPERIOD = 0.01
SERVICENAME = "Locator"

usage = '''
  python %prog [options]
  
Initiates or joins a simple peer-to-peer network.
Absent a PEER (which is the peer initially contacted for
joining the network), it attempts to autodiscover a
network running on localhost, and either joins that or
initiates its own network.

Example usage, in different terminal windows:
  python %prog
  python %prog -h localhost:9900 --port 9902
... etc. ...'''

option_list = [
    make_option("-h", "--host", dest="peer",
                  help="Use PEER as an initial peer",
                  default=''),
    make_option("-p", "--port", type="int",
                  help="Use PORT as the server port [default=9900]",
                  default=0),
    make_option("--help", action="help",
                  help="show this help message and exit"),
]

parser = OptionParser(usage=usage,
                        option_list=option_list, 
                        add_help_option=False,
                        conflict_handler='resolve')

class NodeNotFound(Thrift.TException):
    def __init__(self, location, message=None):
        self.location = location
        self.message = message
    

def loc2str(location):
    "Give the canonical string representation"
    return "%s:%d" % (location.address, location.port)

def str2loc(location):
    comp = location.rsplit(':', 1)
    return Location(comp[0], int(comp[1]))

def generic_remote_call(clientclass, method, destination, *args):
    transport = TSocket.TSocket(destination.address, destination.port)
    transport = TTransport.TBufferedTransport(transport)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = clientclass(protocol)
    try:
        transport.open()
    except Thrift.TException, tx:
        raise NodeNotFound(destination)
    out = getattr(client, method)(*args)
    transport.close()
    return out

remote_call = partial(generic_remote_call, Locator.Client)
ping = partial(generic_remote_call, Base.Client, 'ping')

def select_peers(in_set):
    lst = sorted(in_set)
    return lst

def find_matching_service(location, service, maximum=10):
    loc = Location(location.address, location.port)
    for a in range(maximum):
        try:
            if service == remote_call('service_type', loc):
                return loc
        except NodeNotFound:
            pass
        loc.port += 1
    print 'No peer autodiscovered.'
    return None

def ping_until_found(location, maximum=10):
    loc = Location(location.address, location.port)
    for a in range(maximum):
        try:
            ping(loc)
            return loc
        except NodeNotFound:
            loc.port += 1
    raise NodeNotFound(loc)

def ping_until_not_found(location, maximum=10):
    loc = Location(location.address, location.port)
    for a in range(maximum):
        try:
            ping(loc)
            loc.port += 1
        except NodeNotFound:
            return loc
    raise NodeNotFound(loc)

def ping_until_return(location, maximum=10):
    loc = Location(location.address, location.port)
    wait = WAITPERIOD
    for a in range(maximum):
        try:
            ping(loc)
            return
        except NodeNotFound:
            sleep(wait)
            wait *= 2
            print wait
    raise NodeNotFound(loc)

class BaseHandler(Base.Iface):
    @classmethod
    def service_type(cls):
        return 'Base'
    
    @classmethod
    def service_types(cls):
        services = list()
        for base in cls.__bases__:
            try:
                services.extend(base.service_types())
            except:
                pass
        services.append(cls.service_type())
        return services
    
    def ping(self):
        print 'ping()'
    

class LocatorHandler(BaseHandler, Locator.Iface):
    def __init__(self, peer=None, port=9900):
        self.address = socket.gethostbyname(socket.gethostname())
        self.port = port
        self.peer = peer
        self.ring = HashRing()
        try:
            ping(self.location)
            print 'Uh-oh. Our location responded to a ping!'
            raise socket.error(43, 'Address already in use')
        except NodeNotFound:
            pass
    
    @property
    def here(self):
        "Give the canonical string representation"
        return loc2str(self)
    
    @property
    def location(self):
        "Give the canonical Location"
        return Location(address=self.address, port=self.port)
    
    @classmethod
    def service_type(cls):
        return SERVICENAME
    
    def join(self, location):
        """
        Parameters:
         - location
        """
        self.add(location, [self.location])
        ping_until_return(location)
        items = self.ring.nodes.difference([loc2str(location)])
    
    def remove(self, location, authorities):
        """
        Parameters:
         - location
         - authorities
        """
        key = loc2str(location)
        self.ring.remove(loc2str(location))
        authorities.append(self.location)
        destinations = select_peers(self.ring.nodes.difference(map(loc2str,authorities)))
        for destination in destinations:
            try:
                remote_call('remove', str2loc(destination), location, authorities)
                break
            except NodeNotFound, tx:
                # enter all nodes as authorities to avoid race conditions
                # lazy invalidation
                self.remove(tx.location, map(str2loc, self.ring.nodes))
        print "removed %s:%d" % (location.address, location.port)
    
    def add(self, location, authorities):
        """
        Parameters:
         - location
         - authorities
        """
        key = loc2str(location)
        authorities.append(self.location)
        destinations = select_peers(self.ring.nodes.difference(map(loc2str,authorities)))
        for destination in destinations:
            try:
                remote_call('add', str2loc(destination), location, authorities)
                break
            except NodeNotFound, tx:
                # enter all nodes as authorities to avoid race conditions
                # lazy invalidation
                self.remove(tx.location, map(str2loc, self.ring.nodes))
        self.ring.append(loc2str(location))
        print "added %s:%d" % (location.address, location.port)
    
    def get_all(self):
        return map(str2loc, self.ring.nodes)
    
    def get_node(self, key):
        if self.ring.nodes:
            return str2loc(self.ring.get_node(key))
        else:
            return Location('',0)
    
    def debug(self):
        a = "self.location: %r\n" % self.location
        a += "self.ring.nodes:\n%r\n" % self.ring.nodes
        print a
    
    def cleanup(self):
        self.ring.remove(self.here)
        for node in select_peers(self.ring.nodes):
            try:
                remote_call('remove', str2loc(node), self.location, [self.location])
                break
            except NodeNotFound, tx:
                pass
    
    def local_join(self):
        self.ring.append(self.here)
        if self.peer:
            self.ring.extend(map(loc2str, remote_call('get_all', self.peer)))
            remote_call('join', self.peer, self.location)
            print 'Joining the network...'
        else:
            print 'Initiating the network...'
        
    

def main(inputargs):
    handler = LocatorHandler(**inputargs)
    processor = Locator.Processor(handler)
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
    (options, args) = parser.parse_args()
    if not options.port:
        loc = ping_until_not_found(Location('localhost', DEFAULTPORT), 40)
        options.port = loc.port
    if options.peer:
        options.peer = str2loc(options.peer)
    else:
        options.peer = find_matching_service(Location('localhost', DEFAULTPORT), SERVICENAME)
    main(options.__dict__)


