
Testing WebSockify:

In one terminal, set up the proxy:
$ websockify/run 8080 localhost:5555

In another, set up the producer:
$ nc -l 5555
# wait here


In your browser, set up the consumer:
0) make sure you're on a *non https page*, or else you will need to do extra work
1) open up the js console
2) w = new WebSocket("ws://localhost:8080", "base64");
   w.onmessage = function(e) {
      console.log(atob(e.data))
   }


Type lines into your waiting producer and they should pop out in the browser.

The "base64" argument is important. WebSockify insists on using subprotocol "binary" or subprotocol "base64". atob() is there to invert the base64.
Now, using "binary" causes a nuisance: you get [Blob](https://developer.mozilla.org/en-US/docs/Web/API/Blob) objects, which are very new. And apparently to actually get data out of them you need to do "new Uint8Array(e.data)" (tip from https://github.com/phoboslab/jsmpeg/blob/master/jsmpg.js)