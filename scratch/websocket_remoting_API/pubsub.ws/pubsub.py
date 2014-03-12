# pubsub.py
# kousu, Feb 2014
# 2-clause BSD
"""
Implements a publish-subscribe (PubSub) broker over WebSockets.

Each subscribable "topic" is just a URL. Messages are "published" by sending websocket messages,
and are relayed, unfiltered, to every other websocket open to the same endpoint. The server can publish messages by calling

If you need filtering or anything else fancier, subclass this.

Since we're using WebSockets in Python, we're using Autobahn on Twisted, so make sure those are installed

"""

import sys

from twisted.internet import reactor

import autobahn.twisted.websocket
from autobahn.twisted.websocket import WebSocketServerFactory, WebSocketServerProtocol
from autobahn.twisted.resource import WebSocketResource


class PubSubProtocol(WebSocketServerProtocol):
    """
    Handle talking to a single web
    The way this works is by cooperating with PubSubBroker, which is meant to be this class's Twisted Factory
    Everytime a connection opens or gets a message, this class notifies its Factory, which represents the particular endpoint (aka URL (aka topic, in the pubsub abstraction))
    and in turn its factory has a queue of messages to send to instances of this class
    
    You can override any of these; return False from onOpen or onConnect to cancel the connection
    """
    _listening = False #flag of whether we're in our parent factory's list of listeners (this could be done better...)
    
    def onConnect(self, request):
        pass #TODO(kousu): stash request.headers here for great win
#note-to-self:
#request.extensions  request.host        request.params      request.peer        request.version     
#request.headers     request.origin      request.path        request.protocols  
#
    
    def onOpen(self):
        #TODO(kousu): ask the parent factory if it's okay if we join before we join, here? like, self.factory.subscribe(), which has the option of returning false?
        self.factory._listeners.append(self)
    
    def onMessage(self, payload, isBinary):
    	# pass the payload up to the PubSubBroker
        self.factory.send(payload, isBinary)
    
    def onClose(self, wasClean, code, reason):
        print("WebSocket connection closed: {}".format(reason))
        try:
          self.factory._listeners.remove(self)
        except ValueError:
          warnings.warn("%s unable to remove itself from parent %s during close." % (self, self.factory))


class PubSubBroker(WebSocketServerFactory):
    """
    a publish-subscribe message broker,
    implemented as a Twisted factory for websockets
    """
    protocol = PubSubProtocol
    _brokers = {}         #for debugging: instead of trying to trace python object IDs, use a short, unique,
    _unused_broker_id = 0 #and predictably incrementing ID. This code needs to die in the stable version.
    
    def __init__(self, *args, **kwargs):
        WebSocketServerFactory.__init__(self, *args, **kwargs) #can't use super(); twisted and autobahn do not derive their classes from `object`
        self._listeners = []
    
        PubSubBroker._brokers[PubSubBroker._unused_broker_id] = self #debug only!
        PubSubBroker._unused_broker_id += 1
    
    @staticmethod
    def id(broker):
        "find broker in the list, or None if unknown"
        for k, brk in PubSubBroker._brokers.iteritems():
            if brk is broker: return k    
        
    def send(self, payload, isBinary):
        "enqueue a message to send to all listeners -- but don't actually send it yet"
        "payload must be a str or a unicode"
        print "Broker id %s relaying" % PubSubBroker.id(self), "`"+payload+"`", "to", len(self._listeners), "clients:", str.join(", ", [lsnr.peer for lsnr in self._listeners])
        for lsnr in self._listeners: #iterate over all PubSubProtocols that have attached to this "topic"
            reactor.callLater(0, lsnr.sendMessage, payload, isBinary)  #XXX is it safe to use reactor here??


class PubSubResource(WebSocketResource):
    """
    convenience class so you can say
      site.putChild("path", PubSubResource())
    """
    def __init__(self, url):
        WebSocketResource.__init__(self, PubSubBroker(url, debug = False, debugCodePaths = False))

if __name__ == "__main__":
    # usage: pubsub.py 8080 /path/to/bind/to [/path2/to/bind/to /and/path3 /and/of/course/path4]
    # TODO(kousu): make a proper usage(); support -v (turns on logging) and make port into an optional -p flag
    # run with arguments, this file brings up an arbitrary number
    # (one for each path given) of plain pubsub m
    
    
    port = int(sys.argv[1])
    paths = sys.argv[2:]
    assert all(p.startswith('/') for p in paths), "URLs to bring up must be absolute paths"
    
    from twisted.python import log
    log.startLogging(sys.stdout)
    
    from twisted.web.resource import Resource
    from twisted.web.server import Site
    from twisted.web.static import File
    
    root = File(".")
    reactor.listenTCP(port, Site(root)) 
    # this is handy: Resources can be created after the Site is 'running'
    
    url = "ws://localhost:"+str(port)
    _resource_memos = {}
    for p in paths:
        components = p.split("/")[1:] #we asserted above that paths are absolute, so the first entry will be ""
        # dynamically construct the desired URL inductively
        r = root
        p = "/" #XXX be mindful of the overwriting
        for c in components[:-1]: #skip the last one; it needs to be bound to the PubSubResource
            p += c + "/"
            if p not in _resource_memos:        # if the Resouce for this path exists (say you bring up /chat/1 and /chat/2)
                _resource_memos[p] = Resource() # simply .putChild()ing will destroy it _and_ the pubsub endpoints along with it;
            r.putChild(c, _resource_memos[p])   # so we memoize
            r = _resource_memos[p]
        r.putChild(components[-1], PubSubResource(url))
		# it is a wart of Autobahn that
		# PubSubBroker needs to know its URL
    
    print len(paths), "PubSub brokers listening on:"
    for e in paths:
		print(url+e) #not correct! it would be good to be able to query Twisted itself for what paths it knows
    
    reactor.run()   #Twisted event loop
