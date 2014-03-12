wamp.ws is overengineered. There is no reason to shove that much state on top of stateless HTTP.
AutobahnPython has a solid WebSocket implementation,
but the rest of API is questionable.

These folders give proposals for replacements that can be done to simplfify.

Implementation will be done on AutobahnPython's WebSocket support.
(note: there's [other options](../../wiki/Platforms-and-APIs.md#networking))

Messaging Patterns
==================

WAMP (aka Autobahn aka crossbar.io aka Tavendo aka Tobias) sells itself as providing these features:

* PubSub
* RPC

The confounding thing is [why these two very different patterns happen in the same library](http://wamp.ws/faq/#why_rpc_and_pubsub).

* RPC provides call-response messaging, so that the client (the browser) can query the server for info
RPC is very close to the simple send()/.onmessage() pairing.
Proposal at [rpc.ws](rpc.ws).
* PubSub lets you attach handlers to a stream of events. Proposal at [pubsub.ws](pubsub.ws/)
* Now, in addition to these, there is a third pattern not covered by either of these, which is [state.ws](state.ws/).

In addition to not providing all the messaging patterns under-the-sun, Autobahn also reimplements basic things its built on: [reliable messaging](https://github.com/tavendo/WAMP/blob/master/spec/basic.md#realms-sessions-and-transports) (ie TCP), [authentication](https://github.com/tavendo/WAMP/blob/master/spec/basic.md#realms-sessions-and-transports) (already in HTTP, several times over), [RPC](https://github.com/tavendo/WAMP/blob/master/spec/basic.md#remote-procedure-calls) (this is the same as REST, aka HTTP) and has [all this other new cruft](https://github.com/tavendo/WAMP/blob/master/spec/advanced.md#subscriber-meta-events)

Instead, this folder has several ideas, to be fleshed out over the coming months, that do away with the need for anything but plain WebSockets.

Multiplexing
============

One major stumbling block that Autobahn does solve is multiplexing multiple data streams over a single websocket.

loadtest.html demonstrates that we can probably get away with just opening distinct websockets,
and that standard HTTP rules--meaning no more than [6](citation goes here) (in ye olden days [two](FIXME))
can be active per user per site at a time--do not apply. 

Still, there should be some benefit to being able. I expect latency should be reduced if a single TCP stream is used instead of N. But, instead of building a huge protocol, 
I propose to simply construct a websocket multiplexer, offering exactly the same API as native WebSockets.

The goal would be to have the multiplexer be totally transparent. In other words, these codes should be 100% API-interchangable. Adding or removing the <code>M.</code> should give the same functionality, but one should be(? todo: actually write this and load-test it) faster:
```
w1 = WebSocket("ws://example.com/ws1")
w2 = WebSocket("ws://example.com/ws2")
w2.send("data!")
```

```
M = MultiWebSocket("ws://example.com/multiplexer")
w1 = M.WebSocket("ws://example.com/ws1")
w2 = M.WebSocket("ws://example.com/ws2")

w2.send("data!")   //calls something like M._websocket.send({url: "ws://example.com/ws2", payload: "data!"})
```


This will require backend support and a small protocol to handle the wrapping and unwrapping. In Twisted, we can probably exploit [Resource.getChild()](https://twistedmatrix.com/documents/current/api/twisted.web.resource.Resource.html#getChild) to automagically translate requests for websockets to their handlers; ((what to do if that doesn't work is undefined (we could open a relay?)))


A MultiWebSocket should probably only be able to request intra-server connections--it shouldn't even be allowed to connect to ports other than the one that ```multiplexer/``` is running on (but see Relay below)--but deciding where we break that is a bit tricky if we allow servers to have more than one hostname. Further, it might be a good idea that ws://example.com/path/to/game/instances/multiplexer is **only** able to 'plex to websockets even further down that URL tree (so ws://example.com/path/to/game/instances/abc is okay but ws://example.com/path/to/users is not) (or maybe this should be a subclass of MultiWebSocketProtocol: Protocol > WebSocketServerProtocol > MultiWebSocketServerProtocol > JailedMultiWebSocketServerProtocol)


(
[SockJS](https://github.com/sockjs/websocket-multiplex) has an implementation of this; they've done it reasonably cleanly on top of Twisted+their own Twisted-WebSocket implementation, and indeed have done just what I thought they'd have to: designed a small protocol ([it consists](https://github.com/sockjs/websocket-multiplex/blob/master/multiplex_client.js) of wrapping "data" in "sub,channel" to subscribe, "msg,channel,data" to send data, and "uns,channel" for detaching) and written client-side and server-side implementations of it. On the backend the multiplexer simply redirects messages transparently to WebSocketProtocols
```
multiplex.addFactory("channel1", Factory.forProtocol(EchoProtocol))
multiplex.addFactory("channel2", Factory.forProtocol(ChatProtocol))
```
and even has provisions (via subclassing) for dynamic channel (aka topic aka endpoint) creation.

which is much cleaner than in Autobahn where to run multiple streams over a single channel you need to change how you write your code:
```
#....
# suppose we have jumped through the hoops neessary to construct an ApplicationSession object; call it multiplex

# you /could/ do this, but this gives no way to react, because ~you aren't a websocket~
multiplex.subscribe(echo_handler, "channel1") 
multiplex.subscribe(chat_handler, "channel2") 

# ...

# to respond, you need to
# a) shove unrelated code (echo_handler and chat_handler) into the same class, or abuse python's dynamicness
# b) hard-code the channel names (because you must know where to publish to; that isn't hidden in your class)
class Multiplex2(ApplicationSession):
  #...
  def __init__(self):
     self.subscribe(self.echo_handler, "channel1")
     self.subscribe(self.chat_handler, "channel2")
  def echo_handler(self, *data):
     self.publish("channel1", data)
#....
```

SockJS-Multiplex has no security, and clients can cause servers to (un)subscribe (from)to channels and vice-versa. That might be a feature.
Its rationale is [in a enthusiastic but concise blog post](https://www.rabbitmq.com/blog/2012/02/23/how-to-compose-apps-using-websockets/), wherein it sounds like all of their concerns are my concerns: resuability, composability, simplicity. **The one thing we need to check out before diving into SockJS is whether we can take their server-side multiplexer and drop it on top of Autobahn; if they've really done it cleanly then there should be no trouble doing that --- actually, SockJS-Multiplex isn't implemented in twisted yet; we can maybe snitch their protocol though and implement it our way)

One nit though: in my design, I want the multiplexing to be totally transparent: every channel (aka topic) should be a ws:// URL and there should be no channels open that you couldn't open directly. SockJS-multiplex hangs arbitrary-name channels off their pubsub protocol. --it might be tricky determining which URL corresponds to which node, but I think we can do it, and then we have a SINGLE namespace instead of hiding.

Oo, and another problem: they've conflated pubsub with multiplexing. But those are orthogonal: pubsub is when you have a channel that lots of people can post to and all hear the same messages from at the same time, like an IRC room or a BB. Multiplexing is when you use one channel as many channels. You can have a server hosting only 1 pubsub topic with 1000 listeners, or you can have a single server hosting 1000 topics with only 1 client. Having both at the same time is just bonus.

Yes, I am suffering from N.I.H. syndrome. That doesn't make my reasons invalid :P
)

JSON
====

Similarly, a drop-in WebSocket-compatible JSONWebSocket which autowraps and unwraps all messages to/from JSON would be useful, and probably not that long.


Security
========

We need to think about implementing authentication (we can use cookies, so long as they are the magic non-pinnable session cookies; if we do it right then authors can plug in any of the authentication modules twisted can do) and we need to implement flood-control (XXX does crossbar.io do this?)

What exactly this means is unclear. Maybe instead of bringing up a WebSocketResource we bring up a GuardingWebSocketResource?


Relay Websocket
================

If we're going to write multiplexing websockets, we might as well scratch out a websocket relay, for NAT-punching and the like. I don't expect such a relay to be a /good/ idea; just an /idea/.


Relative WebSocket
==================

An apparently oversight (or purposeful lack?) is that WebSockets do not understand relative links.
This is too bad, because if we use URLs to denote simulation instances, e.g. http://example.com/games/Af354BGq/
it means that we can't write code like
<code>
$(function() {
  pieces = WebSocket("pieces/")
  pieces.onmessage = function(.....)
  //...
  chat = WebSocket("chat/")
  chat.onmessage = function(.....)
}
</code>

Instead, we would have to either:

 make a single global WebSocket endpoint and have it distinguish what
game ID ("Af354BGq") you're talking to by cookies/sessions or by you sending <code>{'session': 'Af354BGq'}</code>
<code>
$(function() {
  room = {}
  room.id = parse window.location to extract the ID
  
