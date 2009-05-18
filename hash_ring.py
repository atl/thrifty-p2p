# -*- coding: utf-8 -*-
"""
    hash_ring
    ~~~~~~~~~~~~~~
    Implements consistent hashing that can be used when
    the number of server nodes can increase or decrease (like in memcached).

    Consistent hashing is a scheme that provides a hash table functionality
    in a way that the adding or removing of one slot
    does not significantly change the mapping of keys to slots.

    More information about consistent hashing can be read in these articles:

        "Web Caching with Consistent Hashing":
            http://www8.org/w8-papers/2a-webserver/caching/paper2.html

        "Consistent hashing and random trees:
        Distributed caching protocols for relieving hot spots on the World Wide Web (1997)":
            http://citeseerx.ist.psu.edu/legacymapper?did=38148


    Example of usage::

        memcache_servers = ['192.168.0.246:11212',
                            '192.168.0.247:11212',
                            '192.168.0.249:11212']

        ring = HashRing(memcache_servers)
        server = ring.get_node('my_key')

    :copyright: 2008 by Amir Salihefendic.
    :license: BSD
"""

# Pretty radically changed by atl on 2009.04.28:
# added append/extend/remove methods (bugfix 2009.05.18)
# made nodes a set to accommodate that more easily/naturally
#
# __getindex__() to support dict-like access::
#   ring.get_node('a') == ring['a']
#
# changed hash values around so that::
#   ring.gen_key('greenwood') == int(md5.new('greenwood').hexdigest()[0:8], base=16)
# ... generating the key is the same as the most significant quarter of the md5 hash
#
# changed the logic so that passing in a hex digest::
#   ring.gen_key('a') == ring.gen_key(md5.new('a').hexdigest())
# ...does not hash again
# This last one may make things unsuitable for other people.

import md5
import math
from bisect import bisect

HEXDIGITS = 'abcdef0123456789'

class HashRing(object):

    def __init__(self, nodes=[], weights=None):
        """`nodes` is a list of objects that have a proper __str__ representation.
        `weights` is dictionary that sets weights to the nodes.  The default
        weight is that all nodes are equal.
        """
        self.ring = dict()
        self._sorted_keys = []

        self.nodes = set(nodes)

        if not weights:
            weights = {}
        self.weights = weights

        self._generate_circle()

    def _generate_circle(self):
        """Generates the circle.
        """
        total_weight = 0
        for node in self.nodes:
            total_weight += self.weights.get(node, 1)

        for node in self.nodes:
            weight = 1

            if node in self.weights:
                weight = self.weights.get(node)

            factor = math.floor((30*len(self.nodes)*weight) / total_weight);

            for j in xrange(0, int(factor)):
                b_key = self._hash_digest( '%s-%s' % (node, j) )

                for i in xrange(0, 4):
                    key = self._hash_val(b_key, i*4)
                    self.ring[key] = node
                    self._sorted_keys.append(key)

        self._sorted_keys.sort()
    
    def append(self, item):
        self.nodes.add(item)
        self._generate_circle()
        
    def extend(self, items):
        self.nodes.update(items)
        self._generate_circle()

    def remove(self, item):
        self.nodes.discard(item)
        self._sorted_keys = []
        self.ring = dict()
        self._generate_circle()
    
    def __getitem__(self, item):
        if isinstance(item, slice): 
            raise TypeError("Does not accept slices, only single keys.")
        return self.get_node(item)
    
    def get_node(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned.

        If the hash ring is empty, `None` is returned.
        """
        pos = self.get_node_pos(string_key)
        if pos is None:
            return None
        return self.ring[ self._sorted_keys[pos] ]

    def get_node_pos(self, string_key):
        """Given a string key a corresponding node in the hash ring is returned
        along with it's position in the ring.

        If the hash ring is empty, (`None`, `None`) is returned.
        """
        if not self.ring:
            return None

        key = self.gen_key(string_key)

        nodes = self._sorted_keys
        pos = bisect(nodes, key)

        if pos == len(nodes):
            return 0
        else:
            return pos

    def iterate_nodes(self, string_key, distinct=True):
        """Given a string key it returns the nodes as a generator that can hold the key.

        The generator iterates one time through the ring
        starting at the correct position.

        if `distinct` is set, then the nodes returned will be unique,
        i.e. no virtual copies will be returned.
        """
        if not self.ring:
            yield None, None

        returned_values = set()
        def distinct_filter(value):
            if str(value) not in returned_values:
                returned_values.add(str(value))
                return value

        pos = self.get_node_pos(string_key)
        for key in self._sorted_keys[pos:]:
            val = distinct_filter(self.ring[key])
            if val:
                yield val

        for i, key in enumerate(self._sorted_keys):
            if i < pos:
                val = distinct_filter(self.ring[key])
                if val:
                    yield val

    def gen_key(self, key):
        """Given a string key it returns a long value,
        this long value represents a place on the hash ring.

        md5 is currently used because it mixes well.
        """
        if len(key) == 32 and all([f.lower() in HEXDIGITS for f in key]):
            return int(key[0:8], base=16)
        b_key = self._hash_digest(key)
        return self._hash_val(b_key, 0)

    def _hash_val(self, b_key, offset):
        return (( b_key[0] << (24 + offset))
                |(b_key[1] << (16 + offset))
                |(b_key[2] << ( 8 + offset))
                |(b_key[3] << ( 0 + offset)))

    def _hash_digest(self, key):
        m = md5.new()
        m.update(key)
        return map(ord, m.digest())
