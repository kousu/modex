# implement the client side of http://www.postgresql.org/docs/current/static/protocol-flow.html#AEN98939
# the reason I'm doing this is because I want to use http://www.postgresql.org/docs/current/static/protocol-replication.html *over a websocket*, and, personally, it's easier to prototype with python

import socket
import struct

from enum import Enum

from collections import OrderedDict
import select
import sys

# possible messages at http://www.postgresql.org/docs/current/static/protocol-message-formats.html
# only ones marked "B" are ones we might need to parse


def pgpack(fmt, *args):
    "postgres-pack: stuff some data into a single structure"
    "this is here to enforce that everything in Postgres is in network byte order"
    assert fmt[0] not in ["@", "=", "<", ">", "!"]
    return struct.pack("!"+fmt, *args) # ! means 'network order' per https://docs.python.org/3.4/library/struct.html

def pgunpack(fmt, buffer):
    assert fmt[0] not in ["@", "=", "<", ">", "!"]
    return struct.unpack("!"+fmt, buffer) # ! means 'network order' per https://docs.python.org/3.4/library/struct.html
    

def pg_marshal_dict(D):
    "marshal an ordered dictionary"
    assert type(D) == OrderedDict
    str.join("", ["%s\0%s" % (k,D[k]) for k in D]) #XXX this should be a bytes object...
   
   
class ProtocolVersion:
    "Misuse a class to define protocol version declaratively"
    Major = 3
    Minor = 0

class MessageSource:
    "in postgres each message type, and a small number are bidirectional"
    def __init__(self, backend=False, frontend=False):
        self.backend, self.frontend = backend, frontend
#aliases to make writing this easier
S = MessageSource
S.F = MessageSource(frontend=True)
S.B = MessageSource(backend=True)
S.FB = MessageSource(backend=True, frontend=True)
   
   
# NB: 
# messaging functions...
class Message(object):
    # class-wide definitions
    TYPECODE = None #the message code, a single byte
    SOURCE = None   #which MessageSource this type of Message is allowed (copied straight from the protocol docs)
    
    # overridables
    def render(self):
        "serialize this message to a bytes"
        "some message types may need to override this"
        payload = self.payload()
        k = 4 + len(payload) #4 is the len of k itself
        return self.TYPECODE + pgpack("I", k) + payload
        
    def payload(self):
        "generate the actual content of the message"
        raise NotImplementedError()
        
    @classmethod
    def parse(cls, msg):
        "parse a message"
        "subclasses should override this but super() call up to it to enforce type constraints"
        if not type(msg) is bytes: raise TypeError()
        if not msg[0] == cls.TYPECODE: raise ValueError()
    
    def __str__(self):
        return "<%s>" % (type(self).__name__,)

class UnknownMessage(Message):
    SOURCE = S.FB
    def __init__(self, typecode, payload):
        self.TYPECODE = typecode
        self.payload = payload
    def __str__(self):
        return "<%s('%s'): %s>" % (type(self).__name__, self.TYPECODE, self.payload)

# this is probably pretty simple to do fully declaratively, by definining a type. the only complicated parts are that some of the spec has like, lists

# parsing is like this: you read the type byte, then you read the length word (4 bytes), then you read the number of words given (possibly crashing there)

class StartupMessage(Message):
    TYPECODE = b"" #"For historical reasons, the very first message sent by the client (the startup message) has no initial message-type byte." <http://www.postgresql.org/docs/current/static/protocol-overview.html>
    SOURCE = S.F
    
    
    _TERMINATOR = b"\x00"
    
    def __init__(self, user, database=None, **options):
        self.options = options
        self.options['user'] = user
        if database is not None:
            self.options['database'] = database
        # the docs are unclear; it says something about there being extra arguments you can pass here...
        
    def payload(self):
        VERSION = ProtocolVersion.Major << 16 | ProtocolVersion.Minor
        #TODO: pull the null-termination thing out to a different function (maybe even to pgpack?)
        return pgpack("I", VERSION) + str.join("", ("%s\0%s\0" % e for e in self.options.items())).encode("ascii") + self._TERMINATOR

    def __str__(self):
        return "<%s(): %r>" % (type(self).__name__, self.options)



def invert_enum(enum_cls):
    enum_cls.__reverse_members__ = OrderedDict([(k.value, k) for k in enum_cls.__members__.values()])


class AuthRequest(Message):
    "The docs define several Auth messages, but they all have the same typecode and instead differ in the values in the payload, so I follow that here; but it *is* somewhat complicated by that not all values in the structure exist in all responses..."
    TYPECODE = b"R"
    SOURCE = S.B
    
    class AuthTypes(Enum):
        Ok = 0
        KerberosV5 = 2
        CleartextPassword = 3
        MD5Password = 5
        SCMCredential  = 6
        GSSAPI = 7
        SSPI = 9
        Continue = 8
    invert_enum(AuthTypes)
    
    def __init__(self, subtype, authdata=None):
        self.subtype = subtype
        self.authdata = authdata
    @classmethod
    def parse(cls, payload):
        typeable, = pgunpack("I", payload[:4])
        subtype = cls.AuthTypes.__reverse_members__[typeable]
        authdata = payload[4:] #for md5 and gssapi and sspi, this extra data will appear magically; otherwise it should be empty
        if subtype not in [cls.AuthTypes.Continue, cls.AuthTypes.MD5Password]: assert not authdata
        return cls(subtype, authdata)
        
    def __str__(self):
        return "<%s::%s(%s)>" % (type(self).__name__, self.subtype, "%r"%self.authdata if self.authdata else "")


# now all the subtypes that an auth request can be...
#class AuthOk(AuthRequest):
#    ID = 0

class KeyData(Message):
    "'Identifies the message as cancellation key data.'"
    "'The frontend must save these values if it wishes"
    " to be able to issue CancelRequest messages later.'"
    TYPECODE = b"K"
    SOURCE = S.B
    def __init__(self, key):
        self.key = key
    @classmethod
    def parse(cls, payload):
        return cls(payload)
        
    def __str__(self):
        return "<%s(%r)>" % (type(self).__name__, self.key)


class ParameterStatus(Message):
    "a parameter status is simply a key value pair wherein the server reports what its private parts look like"
    TYPECODE = b"S"
    SOURCE = S.B
    
    def __init__(self, key, value):
        self.key = key
        self.value = value
    
    @classmethod
    def parse(cls, payload):
           key, value, _ = [s.decode("ascii") for s in payload.split(b"\x00")]
           
           return cls(key, value)
    def __str__(self):
        return "<%s(%r: %r)>" % (type(self).__name__, self.key, self.value)


class ReadyForQuery(Message):

    "'ReadyForQuery is sent whenever the backend is ready for a new query cycle.'"
    TYPECODE = b"Z"
    SOURCE = S.B
    
    class TransactionStates(Enum):
        Idle = b"I"
        Transacting = b"T"
        Error = b"E"
        
    invert_enum(TransactionStates)
    
    def __init__(self, state):
        self.state = state
    @classmethod
    def parse(cls, payload):
        assert len(payload) == 1
        
        return cls(cls.TransactionStates.__reverse_members__[payload])
        
    def __str__(self):
        return "<%s::%s>" % (type(self).__name__, self.state)


#-----------------------

MESSAGES = dict((m.TYPECODE, m) for m in locals().values() if type(m) is type and issubclass(m, Message))

class PgClient:
    "layer the postgres message-based protocol on top of stream-based TCP"
    #TODO: support unix domain sockets too
    
    def __init__(self, user, database=None, options = {}, server = ("localhost", 5432)):
        #self.sock = socket.socket()
        #self.sock.connect(server)
        self.sock = socket.create_connection(server)
        self.send(StartupMessage(user, database, **options))
        
    def send(self, message):
        assert message.SOURCE.frontend, "%s is not meant to be sent by the client" % message
        #print(message)
        print("<--", message)
        self.sock.send(message.render())
    
    def recv(self):
        # block
        # there's a fiddly thing: *if* we are the server then the first message has a missing typecode for recv(1) will break everything
        # but this file is currently only implementing the client protocol so whatever
        typecode = self.sock.recv(1)
        k, = pgunpack("I", self.sock.recv(4))
        payload = self.sock.recv(k-4) #-4 because k counts itself, but we've already recv()'d it
        
        #print(typecode)
        message = MESSAGES.get(typecode)
        if message is None:
            message = UnknownMessage(typecode, payload)
        else:
            message = message.parse(payload)
        assert message.SOURCE.backend, "%s is not meant to be received by the client" % message #XXX maybe this shouldn't be an assert because it means there's a DoS here.
        
        
        print("-->", message)
        
        return message

def test():
    pg = PgClient("postgres")
    def client_fiber():
        while True:
            
            yield
            
    def REPL_fiber(): #...?
        while True:
            yield
    
    while True:
        Sr, _, _ = select.select([sys.stdin, pg.sock], [], []) #...so, a timeout of 0 is..bad
        if Sr:
            Sr = Sr[0]
            if Sr is sys.stdin:
                l = input()
                print("you typed ", l , "don't you feel special?")
            elif Sr is pg.sock:
                m = pg.recv()
                

if __name__ == '__main__':
    test()