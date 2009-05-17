= README =

== thrifty-p2p ==

A very simple p2p python implementation using the Thrift protocol and its basic RPC capabilities. Not expected to be generically usable by any project: thrifty-p2p implements a flat network, because of the size of the other messages and limited number of nodes anticipated by my own applications.

=== Basic usage ===

{{{python location.py [[peer_node] port_num]}}}

Initiates and/or joins a simple peer-to-peer network.\\
Default port_num is 9900.\\
Absent a peer_node (which is the peer initially contacted for
joining the network), it initiates a network.

Example usage, in different terminal windows:
{{{
python location.py
  
python location.py localhost:9900 9901
  
python location.py localhost:9901 9902
}}}
  
... etc. ...

=== Dependencies & Requirements ===

The basic server uses consistent hashing via [[http://pypi.python.org/pypi/hash_ring/|hash_ring.py]], developed by [[http://amix.dk/blog/viewEntry/19367|Amir Salihefendic]]. For better and for worse, I modified the code to allow for easier node set manipulations and different node resolution behavior so that it's more trivially related to md5 hashes.

The library requires the [[http://incubator.apache.org/thrift/|Apache Thrift]] [[http://pypi.python.org/pypi/thrift/1.0|Python libraries]] to be installed, but does not currently technically require the Thrift compiler or any other language libraries.

=== Design priorities ===
I'm using Thrift as much for its simple RPC underpinnings as the message format. I wanted to consistently distribute some processing and bother as little as possible with node lookup. This is the simplest thing that I came up with that sort-of works: a flat peer-to-peer network with each node keeping up with the state of all the nodes as well as it can. This necessarily limits the ring's ability to scale beyond dozens of nodes.