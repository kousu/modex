#!/usr/bin/python2
# the main server code
# make sure to read src/backend/README.md if this confuses you.

import sys
from os.path import dirname, abspath, join as pathjoin

#'working directory': not the system working directory, but the directory this program is in (so that we can be run from anywhere and find the correct assets/ folder et al.)
PROJECT_ROOT = dirname(dirname(dirname(abspath(__file__)))) #currently, the project root is two levels up from the directory the server is

from twisted.internet import reactor
from twisted.python import log
from twisted.web.server import Site
from twisted.web.static import File

import autobahn.twisted.wamp

# autobahn changed their API between (which happens to be within the last month of so as of this commit)
# they added asyncio (which is py3.4-native) as an alternate option to twisted (which is py2-native),
# and hence moved moved classes from autobahn.* to autobahn.twisted.*
# evidence:
#  https://raw.github.com/tavendo/AutobahnPython/v0.7.0/examples/twisted/websocket/echo/server.py
#   ~~ the change happened here, now  ~~
#  https://raw.github.com/tavendo/AutobahnPython/v0.6.5/examples/websocket/echo/server.py
#  https://raw.github.com/tavendo/AutobahnPython/v0.6.4/examples/websocket/echo/server.py
#  https://raw.github.com/tavendo/AutobahnPython/v0.5.14/examples/websocket/echo/server.py
#  https://raw.github.com/tavendo/AutobahnPython/e1dae070e67a9361f14beba775c66961e06d43ff/demo/echo/echo_server.py

from autobahn.twisted.websocket import WebSocketServerFactory, WampWebSocketServerFactory, WebSocketServerProtocol, WampWebSocketServerProtocol

#from autobahn.wamp.websocket import WampWebSocketServerProtocol, WampWebSocketServerFactory
from autobahn.twisted.resource import * #WebSocketResource, HTTPChannelHixie76Aware

#TODO: import gdal and take vector layers to load as arguments
#TODO: Autobahn as of 0.7.4 actually supports py3, even though Twisted doesn't; this might be worth investigating.

import json

#data = {'Incandescent': [-_ for _ in range(10)],
#                'CFL': [_ for _ in range(10)],
#                'Halogen': [_*2 for _ in range(10)],
#                'LED': [_*0.5 for _ in range(10)],
#        }

# we are trying to set up a producer-consumer system, and twisted has this built in:
# https://twistedmatrix.com/documents/12.2.0/core/howto/producers.html
# ah, simpler: reactor.callLater

# TODO(kousu): set up WAMP and use it to push messages instead of using a 'raw' websocket

import random
ALPHA = .5
BETA = 10
class Model(object):
    """
    a stub 'model' that gives values from a beta distribu
    this model is an iterator and is time-stepped by calling next() on it
    """
    def __init__(self, alpha, beta):
        self._alpha = alpha
        self._beta = beta
    def __iter__(self): return self
    def next(self):
        return random.betavariate(self._alpha, self._beta)

import csv

from twisted.internet.defer import inlineCallbacks
from autobahn.twisted.util import sleep
from autobahn.twisted.wamp import ApplicationSession, ApplicationSessionFactory

import load_sim # this collects the data
sim = load_sim.sim

class ModelServer(ApplicationSession):
   """
   An application component that publishes events with no payload
   and with complex payloads every second.
   """
   def __init__(self):
      ApplicationSession.__init__(self)
      self._model = Model(ALPHA, BETA) #NB must be before join since .join() causes onJoin()... though.. uh.. @inlineCallbacks is weird
      self._running = False #NB: models don't know about real time, but ModelServer does

   def onConnect(self):
      self.join("realm1") # only one "realm" can be joined at a time
   
   def start(self):
      self._running = True
   
   def stop(self):
      self._running = False
   
   @inlineCallbacks
   def onJoin(self, details):

      self.register(self.start, "start")
      self.register(self.stop, "stop")
      
      counter = 0
      while True:
         self.publish('heartbeat')
         
         if self._running:
           state = next(self._model) #NB: model is an iterator
           self.publish('data', state) # ... this seems.. poor.. in the long run. The models internally need to be able to register data streams on themselves,
                                       # and then we need 

         yield sleep(2)

    
#TODO(kousu): move this out to scratch/ for reference on how to host a web socket server using AutobahnPython
class CtlProtocol(WebSocketServerProtocol):
   def onConnect(self, request):
      print("Client connecting: {}".format(request.peer))

   def onOpen(self):
      print("WebSocket connection open.")
      
   def onMessage(self, payload, isBinary):
      if isBinary:
         print("Binary message received: {} bytes".format(len(payload)))
      else:
         print("Text message received: |{}|".format(payload.decode('utf8')))

      #self.sendMessage(json.dumps(data), isBinary)

   def onClose(self, wasClean, code, reason):
      print("WebSocket connection closed: {}".format(reason))
   


class JsonDataServer(WebSocketServerProtocol):
   def onConnect(self, request):
      print("Client connecting: {}".format(request.peer))

   def onOpen(self):
      print("WebSocket connection open.")
      
      #I want to speak dynamically: so, as data comes in, push it to the client
      # but I don't see how to do this?? what thread am I running on???
      # ah!
      

      # this could probably be cleaner
      def feed():  #a coroutine, meant to be pumped by the twisted event loop
        for i in xrange(sim.years):
          J = sim.step()
          print "pushing", J, "to the client"
          self.speak(J)
          yield
      g = feed()
      
      def loop():  #wrap the coroutine in a callback that causes a loop setTimeout()-style playing nice with Twisted's loop (there's probably a cleaner way to do this, but shh)
        try:
          next(g)
          reactor.callLater(.3, loop)
        except StopIteration:
          pass
      loop()  #kick it off

   def onMessage(self, payload, isBinary):
      if isBinary:
         print("Binary message received: {} bytes".format(len(payload)))
      else:
         print("Text message received: |{}|".format(payload.decode('utf8')))

      #self.sendMessage(json.dumps(data), isBinary)

   def onClose(self, wasClean, code, reason):
      print("WebSocket connection closed: {}".format(reason))
   
   def speak(self, o):
     "send object o across the wire to the listener"
     "the idea is that in our setup, the clients mostly listen to us"
     if not isinstance(o, dict): #coerce 
       o = o.__dict__ #security risk?
     self.sendMessage(json.dumps(o), False)
      

   ## create a WAMP router factory
   ##
from autobahn.wamp.router import RouterFactory
from autobahn.twisted.wamp import RouterSessionFactory

   
     
## create a WAMP router session factory
##


if __name__ == '__main__':

   if len(sys.argv) > 1 and sys.argv[1] == 'debug':
      log.startLogging(sys.stdout)
      debug = True
   else:
      debug = False
   
   if debug:
     print "Starting server in", PROJECT_ROOT

   data_endpoint = WebSocketServerFactory("ws://localhost:8080",
                                    debug = debug,
                                    debugCodePaths = True)
   data_endpoint.protocol = JsonDataServer
   #data_endpoint.setProtocolOptions(allowHixie76 = True) # needed if Hixie76 is to be supported   
   
   ctl_endpoint = WebSocketServerFactory("ws://localhost:8080",
                                    debug = debug,
                                    debugCodePaths = True)
   ctl_endpoint.protocol = CtlProtocol
   

   from autobahn.wamp.router import RouterFactory
   router_factory = RouterFactory()

     
   ## create a WAMP router session factory
   ##
   from autobahn.twisted.wamp import RouterSessionFactory
   session_factory = RouterSessionFactory(router_factory)
   
   ## [ ... ... ]
   session_factory.add(ModelServer())
   
   ## create a WAMP-over-WebSocket transport server factory
   ##
   from autobahn.twisted.websocket import WampWebSocketServerFactory
   wamp_factory = WampWebSocketServerFactory(session_factory, "ws://localhost:8080", debug = True)
   wamp_factory.setProtocolOptions(failByDrop = False)
   
   
   webroot = pathjoin(PROJECT_ROOT,"src","frontend")
   assets = pathjoin(PROJECT_ROOT,"assets")
   if debug:
     print "putting", webroot,"at root"
     print "putting", assets,"at assets"
   
   
   ## we serve static files (most of the frontend html, js, and css) under "/" ..
   ## except for some which are under assets/
   ## and we have our WebSocket server under "/ws"
   root = File(webroot)
   assets = File(assets)
   data_resource = WebSocketResource(data_endpoint)
   ctl_resource = WebSocketResource(ctl_endpoint)
   wamp = WebSocketResource(wamp_factory)
   
   root.putChild("assets", assets)  #TODO: do we prefer to have each entry in assets/ sit at the root (ie http://simulation.tld/data/ instead of http://simulation.tld/assets/data/)   
   root.putChild("ws", data_resource)    #this puts the websocket at /ws. You cannot put both the site and the websocket at the same endpoint; whichever comes last wins, in Twisted
   if debug:
     root.putChild("scratch", File(pathjoin(PROJECT_ROOT,"scratch")))
   root.putChild("wamp", wamp) #okay, this is not behaving itself; crud
   root.putChild("ctl", ctl_resource) #this whole file is so not pythonic. Where's the D.R.Y. at, yo? --kousu
   
   #. <- /
   #./scratch <- /scratch
   #websocket <- /ws #random demo socket
   #websocket <- /ctl
   #websocket <- /wamp 
   #RESTful 
   
   ## both under one Twisted Web Site
   site = Site(root)
   #site.protocol = HTTPChannelHixie76Aware #  needed if Hixie76 is to be supported
   reactor.listenTCP(8080, site)

   print "Now open http://127.0.0.1:8080 in your browser"
   reactor.run()

