# implement the client side of http://www.postgresql.org/docs/current/static/protocol-flow.html#AEN98939
# the reason I'm doing this is because I want to use http://www.postgresql.org/docs/current/static/protocol-replication.html *over a websocket*, and, personally, it's easier to prototype with python

"""
scrap notes:

- the protocol is a mixture of binary and string data
- struct.(un)pack is useful for us for the binary data, but the string data is C-strings, which struct.pack does not handle (it handles fixed sized strings and pascal strings). This means we need to do scanning to parse these parts.
- the possible messages at http://www.postgresql.org/docs/current/static/protocol-message-formats.html; this prototype focuses on the client protocol, meaning only ones marked "B"(ackend) have parse() written and only ones marked "F"(rontend) have payload() written

"""

import socket


import select
import sys

import traceback


from messages import *
from replication_messages import *


class StopCoroutine(StopIteration): pass



class PgClient:
    "layer the postgres message-based protocol on top of stream-based TCP"
    #TODO: support unix domain sockets too
    
    def __init__(self, user, database=None, options = {}, server = ("localhost", 5432)):
        #self.sock = socket.socket()
        #self.sock.connect(server)
        
        self.key = None   #the postgres cryptokey ("BackendKeyData (B)" The frontend must save these values if it wishes to be able to issue CancelRequest messages later.)
        
        self.parameters = {} #the state of the server as reported by ParameterStatus
        
        self._running = True

        self.sock = socket.create_connection(server)
        self.send(StartupMessage(user, database, **options))
        
        
    def send(self, message):
        assert message.SOURCE.frontend, "%s is not meant to be sent by the client" % message
        
        print("<--", message)
        
        self.sock.send(message.render())
    
    def recv(self):
        # block
        # there's a fiddly thing: *if* we are the server then the first message (for historical reasons) has a missing typecode and the recv(1) and will break everything
        # solution: this is currently only implementing the client protocol, so whatever
        typecode = self.sock.recv(1)
        if typecode == b"":
            raise SocketClosed
        # XXX this can got bad because the socket might die at *any one* of these calls
        # and the socket module is a thin layer on the traditional socket API
        # which means: return codes, not exceptions
        
        k = self.sock.recv(4)
        if k == b"":
            raise SocketClosed
        k, = pgunpack("I", k)
        payload = self.sock.recv(k-4) #-4 because k counts itself, but we've already recv()'d it
        if payload == b"":
            raise SocketClosed
        
        #print(typecode)
        message = MESSAGES.get(typecode)
        if message is None:
            message = UnknownMessage(typecode, payload)
        else:
            message = message.parse(payload)
            assert message is not None, "You forgot to return the message in parse()!"
        assert message.SOURCE.backend, "%s is not meant to be received by the client" % message #XXX maybe this shouldn't be an assert because it means there's a DoS here.
        
        print("-->", message)
        
        # stash the postgres cryptokey (which is sent in plaintext?!)        if isinstance(message, KeyData):
        if isinstance(message, BackendKeyData):
            self.key = message.key
        elif isinstance(message, ParameterStatus): #flatten the ParameterStatus messages into something nicer
            self.parameters[message.key] = message.value
        
        return message
    
    def messages(self):
        while True:
            try:
                yield self.recv()
            except Exception as e:
                print("it bork:", e) #DEBUG
                traceback.print_exc()
                raise
    
    def process_co(self):
        "process() rewritten as a coroutine"
        "instead of setting ._running=False, use .throw(StopCoroutine)"
        
        
        self.send(StartupMessage(user, database, **options))
        
        
    
    def process(self):
        "this function is for running on a background thread"
        
        self.send(StartupMessage(user, database, **options))
        # dealing with this protocol is complicated because it's not a simple request-response protocol. A single request can result in multiple responses, which sometimes come in a particular order and sometimes do not. We're facing a state machine, and that makes it difficult to write cleanly (i.e. functionally).
        # e.g. a single response to a query is at least three messages:
        # 1) a RowDescription, giving the fieldnames and order (in csv, the first header row)
        # 2) a series of DataRows
        # 3) a CommandComplete
        
        # It's *also* complicated because there's layered protocols, though they aren't called that
        # for example, the "walsender" mode is layered on SQL messages ("Q"ueries and "D"/"C" responses); like, IDENTIFY_SYSTEM returns a SQL table, even though it's metadata that isn't actually SQL.
        # Maybe we should actually implement some lines here, like "while Reading A Query Response: m = self.recv(); ..." and..stream out the result to a queue of some sort? a queue labelled by the table name or ..something? oh, but even more complicated for us, the protocol does not embed table names in the responses.
        # the protocol has "ReadyForQuery" to help us, at least.
        
        while self._running:
            # this would be cleaner if I could say "select [pg, sys.stdin]"
            Sr, _, Se = select.select([self.sock], [], [self.sock], 1) #1s timeout means ~1s between the main thread terminating and this thread noticing and following suit
            #TODO: figure out some way to have select() wake up on a signal instead of use a timeout
            # e.g. we make a FIFO or a TCP socket to ourselves?
            # but it seems sort of strange to use TCP just to talk from one thread to another?
            #..but maybe it's not. That's what multiprocessing would do. 
            if Sr:
                try:
                    m = self.recv()
                except Exception as e:
                    #...?
                    print("it bork:", e)
                    traceback.print_exc()
                    break #???
                #...?
                    
                    #...the best thing to do might actually be to spin off a thread...
            elif Se:
                print("Socket fell over") #XXX todo better message
                break
                
        self.sock.close()
        self.sock = None
        
class SocketClosed(Exception): pass #XXX this is dumb. fix it better. please.


class PgReplicationClient:
    def __init__(self, user, database=None, server = ("localhost", 5432)):
        self._pg = PgClient(user, database, {"replication": "on"}, server)
        for m in self._pg.messages(): #chew through the startup headers (_pg is designed to cache anything it cares about in this set)
            if isinstance(m, ReadyForQuery):
                break
        self._pg.send(Replication_IdentifySystem())
        header_message = self._pg.recv() #this should give a RowDescription
        data_message = self._pg.recv() #this should give a DataRow to go with
        column_ptrs = {f[0]: i for i, f in enumerate(header_message.fields)} #TODO: move this inside of RowDescription
        
        # extract the timeline ID (tli) from the header
        tli = data_message.fields[column_ptrs["timeline"]]
        xlogpos = data_message.fields[column_ptrs["xlogpos"]]
        print("We are replicating timeline", tli, "from position", xlogpos)
        complete_message = self._pg.recv()
        ready_message = self._pg.recv()
        assert isinstance(complete_message, CommandComplete) and isinstance(ready_message, ReadyForQuery), "Protocol got out of sync!"
        
        # and use it to start replication!
        self.send(Replication_Start(tli, xlogpos))
        
        self._running = True
    
    def send(self, message):
        print("<[[[", message)
        if isinstance(message, Replication_Message): #XXX this should be handled by polymorphism!
            message = CopyData(message.render())
        print("REPLICATION: SENDING", message.render())
        self._pg.send(message)
    
    def recv(self):
        message = self._pg.recv()
        if isinstance(message, CopyData):
            # parse out the copydatas
            typecode, payload = message.data[0:1], message.data[1:]
            
            # now, we turn message into a different sort of message
            message = REPLICATION_MESSAGES.get(typecode)
            if message is None:
                message = UnknownMessage(typecode, payload)
            else:
                message = message.parse(payload)
        print("]]]>", message)
        return message   #but the lower level messages are still useful sometimes?? hm
    
    def messages(self):
        "generator"
        while True:
            try:
                yield self.recv()
            except SocketClosed:
                break
            except Exception as e:
                print("it bork:", e) #DEBUG
                traceback.print_exc()
                pass
                
    def process(self):
        for m in self.messages():
            print(m)
            self._pg._running = self._running
            if isinstance(m, Replication_Keepalive):
                if m.ping:
                    self.send(Replication_HotStandbyFeedbackMessage(1)) #why 1? why not! "the spirit of Y_0"
            pass
            


# one way: view it as a series of layered protocols, build managers for each protocol (some of which are backed (via composition, not inheritence) by the others; afterall, we already use TCP this way which uses IP this way); this is awkward because it pushes us towards using threads

# another way: abuse 'yield' so that we can write statemachines (which is what a protocol, especially one as gunky as the Postgres one, is ) as coroutines
#    t

def client_protocol():
    "coroutine implementing the entire postgres client protocol"
    "use next() to receive messages; users can use .send() to send Messages; the coroutine is written such that it only yields at an appropriate point to wait for messages" 
    # todo: write a wrapper which works somewhat like socketpair(): it returns both the instantiated generator and an fd you can select() on
    # we sniff the messages as they pass through us and possibly dump into subprotocol coroutines as needed
    #sock = 
    #send(Startup11
    pass

def server_protocol():
    "..."

def test():
    
    import threading
    import IPython
    pg = PgReplicationClient("kousu", "postgres")
    # now, I would like to wait here for 
    #...register a promise?
    #
    
    pg.thread = threading.Thread(target=pg.process)
    pg.daemon = True
    pg.thread.start()
    IPython.embed()
    
    pg._running = False #signal the client to shut itself down
    
                
                
if __name__ == '__main__':
    test()