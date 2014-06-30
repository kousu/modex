# implement the client side of http://www.postgresql.org/docs/current/static/protocol-flow.html#AEN98939
# the reason I'm doing this is because I want to use http://www.postgresql.org/docs/current/static/protocol-replication.html *over a websocket*, and, personally, it's easier to prototype with python

"""
scrap notes:

- the protocol is a mixture of binary and string data
- struct.(un)pack is useful for us for the binary data, but the string data is C-strings, which struct.pack does not handle (it handles fixed sized strings and pascal strings). This means we need to do scanning to parse these parts.

"""

import socket
import struct

from enum import Enum

from collections import OrderedDict
import select
import sys

# possible messages at http://www.postgresql.org/docs/current/static/protocol-message-formats.html
# only ones marked "B" are ones we might need to parse



def pgstruct_commands(fmt):
    "scan and parse a postgres struct format string, which is the same as a regular format string except that it cannot have its endianness specified (because postgres uses network (big) endianness) and 's' means nul-terminated non-fixed-size C strings"
    "also, for now, we make the assumption that strings mean ascii strings, though supposedly there's a way to configure this"
    
    assert fmt[0] not in ["@", "=", "<", ">", "!"] #*disallow* user choice of endianness
    p = 0 #current head
    # allowed chars in a format string: spaces (ignored), the list of single char commands, and decimal digits but only *immediately* preceeding their single char
    while p < len(fmt):
        # ignore spaces 
        if fmt[p].isspace():
            p+=1
            continue
        
        # eat numeric digits 
        e = p
        while ord("0") < ord(fmt[e]) < ord("9"):
            e += 1
        l = int(fmt[p:e]) if e > p else None
        # and the format char 
        e += 1
        # actually extract the command sequence
        cmd = fmt[p:e]
        
        # advance the pointer
        p = e


        if cmd[-1] == "s":
            assert len(cmd) == 1, "non-fixed-width strings *must not* have a length associated"
        assert cmd[-1] not in ["p", "n", "N", "P", "q", "Q"], "%s only available in native struct format, which postgres, using network format, does not use" % cmd[-1]
        
        yield cmd        #assert c in ["x","c","b","B","?","h","H","i","I","l","L","f","d","s","p", "n","N", "P", "q","Q"], "bad char in struct format"
              
#print(list(pgstruct_commands("x x h y hy h x   3xx xx x")))


def pgpack(fmt, *args):
    "postgres-pack: stuff some data into a single structure"
    "this is here to enforce that everything in Postgres is in network byte order"
    
    # in order to change the meaning of 's' we need to do at least a bit of parse ourselves
    # I think(?) the least slow (but still stuck in python) way to do this is by doing each individually in a generator then join() the results
    
    #TODO: this would be faster if we were clever about finding chunks of string vs non-string blocks and encoded the non-string blocks all at once, since that's done by the _struct.so module
    
    def fm(cmd, a):
        if cmd == "s":
            assert isinstance(a, str)
            return a.encode("ascii") + b"\x00" #nul-terminate the string and pass it back
        
        # otherwise fall back on normal
        return struct.pack("!" + cmd, a)
    
    return bytes.join(b"", (fm(c, a) for c,a in zip(pgstruct_commands(fmt), args)))
    
    return struct.pack("!"+fmt, *args) # ! means 'network order' per https://docs.python.org/3.4/library/struct.html

#print(pgpack("sh", "i am a merry model", 888))

def pgunpack(fmt, buffer):
    r, o = pgunpack_from(fmt, buffer)
    assert o == len(buffer), "pgunpack requires an exact-length buffer"
    return r

def pgunpack_from(fmt, buffer, offset=0):
    "the return value of this is *two*-valued: the usual tuple of results from struct.unpack *plus* the *remainder of buffer*, because due to C-strings there is no way to know before calling this how much of buffer will get eaten"
    "the struct module does it differently; since all its fields have their width specified in the format string, it can provide a (functional-style) routine 'calcsize' and force you to use it, even with pascal strings *which have a size byte embedded*"
    "but we only find out our field widths during parsing"
    
    # this cannot be done functionally, because the width of each buffer is variable, due to C-strings
    
    def parse(): #a generator to do the parsing for us
        nonlocal offset
        # ..hm. okkkkay. this is complicated.
        # buffer commands that *are not 's'* and unpack them all at once
        
        
        def buffered_commands(fmt):
            # yields ONLY either 's' or a valid format string of fixed width entries
            cmds = []
            
            for cmd in pgstruct_commands(fmt):
                if cmd == "s":
                    if cmds:
                        yield str.join(" ", cmds); cmds = []
                    yield "s"
                else:
                    cmds.append(cmd)
            
            if cmds:
                yield str.join(" ", cmds); cmds = []
        
        
        for cmd in buffered_commands(fmt):
            if cmd == "s":
                e = buffer.find(b"\x00", offset)
                yield buffer[offset:e]
                offset = e + 1
                
            else:
                cmd = "!" + cmd # ! means 'network order' per https://docs.python.org/3.4/library/struct.html
                w = struct.calcsize(cmd)
                for t in struct.unpack_from(cmd, buffer, offset):
                    yield t
                offset += w
                
    return tuple(parse()), offset
    

#print(pgunpack_from("sih", pgpack("sih", "i am a merry model", -919191, 888)+b" la lal al alalla I am the walrus"))

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

class BackendKeyData(Message):
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


class CommandComplete(Message):
    "Identifies the message as a command-completed response."
    "actually has several sub-classes, which we implement here (in line with the protocol) as a .tag field and the CommandComplete.Tag tree of classes"
    TYPECODE = b"C"
    SOURCE = S.B
    
    class Tag:
        pass
    
    class Insert(Tag):
        def __init__(self, object_id, rows):
            self.object_id, self.rows = object_id, rows
            
    class Delete(Tag):
        def __init__(self, object_id, rows):
            self.object_id, self.rows = object_id, rows
    
    
    
    def __init__(self, tag):
        self.tag = tag #TODO: structure this; the tags themselves    
    
    @staticmethod
    def parsetag(tag):
    
        #I D U S M F C
        h = tag[0] #I cheat: I parse the tag by looking at the first byte
        if h == b"I":
            assert tag[:6] == b"INSERT"
            # ..???
        elif h == b"D":
            assert tag[:6] == b"DELETE"
        elif h == b"U":
            assert tag[:6] == b"UPDATE"
        elif h == b"S":
            assert tag[:6] == b"SELECT"
        elif h == b"M":
            assert tag[:4] == b"MOVE"
        elif h == b"F":
            assert tag[:5] == b"FETCH"
        elif h == b"C":
            assert tag[:4] == b"COPY"
            #...?!??!?!
        
        
    
    @classmethod
    def parse(cls, payload):
        return cls(cls.parsetag(payload))

class EmptyQueryResponse(Message):
    TYPECODE = b"I"
    SOURCE = S.B
    
    @classmethod
    def parse(cls, payload):
        assert not payload, "EmptyQueryResponse should have no payload"
        return cls()
    

class Query(Message):
    TYPECODE = b"Q"
    SOURCE = S.F
    
    def __init__(self, string):
        self.string = string
        
    def payload(self):
        return self.string.encode("ascii") + b"\x00"
    
    def __str__(self):
        return "<%s>: '%s'" % (type(self).__name__, self.string)


class RowDescription(Message):

    TYPECODE = b"T"
    SOURCE = S.B
    
    
    def __init__(self, fields): #a DataRow is a list of fields, ordered but unlabelled
        self.fields = fields
       
    @classmethod 
    def parse(cls, payload):
        (n,), payload = pgunpack("H", payload[:2]), payload[2:]
        # parse each field
        fields = []
        offset = 0
        for i in range(n):
            # order: name, table ID, column ID, field type, field size, field type modifier, text/binary flag (0=Text, 1=Binary)
            f, offset = pgunpack_from("s i h i h i h", payload, offset)
            
            fields.append(f)
        return cls(fields)
    
    def __str__(self):
        return "<%s>: '%s'" % (type(self).__name__, self.fields)
        
class DataRow(Message):
    TYPECODE = b"D"
    SOURCE = S.B
    
    
    def __init__(self, data): #a DataRow is a list of fields, ordered but unlabelled
        self.fields = data
    
    @classmethod
    def parse(cls, payload):
        (n,), payload = pgunpack("H", payload[:2]), payload[2:]
        
        # parse each field
        fields = []
        for i in range(n):
            (l,), payload = pgunpack("i", payload[:4]), payload[4:]
            if l == -1:
                fields.append(None) # a SQL NULL value
                payload
            else:
                f, payload = payload[:l], payload[l:]
                # ...darn, I need a pseudo-global; I need to be able to look at PgClient.parameters["client_encoding"]. But that
                f = f.decode("ascii") #XXX hardcoded sketchiness!
                fields.append(f)
                
        assert not payload, "We didn't eat up the entire payload while parsing a DataRow; was the number of fields wrong?"
            
        return cls(fields)
        
    def __str__(self):
        return "<%s> %s" % (type(self).__name__, self.fields)

    

### replication protocol messages 
### all of these are passed as queries ("Q") messages
### http://www.postgresql.org/docs/current/static/protocol-replication.html

# XXX put these in a submodule
class Replication_IdentifySystem(Query):
    def __init__(self):
        super().__init__("IDENTIFY_SYSTEM")

#-----------------------

MESSAGES = dict((m.TYPECODE, m) for m in locals().values() if type(m) is type and issubclass(m, Message))

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
            assert message is not None, "You forgot to return the message in parse()!"
        assert message.SOURCE.backend, "%s is not meant to be received by the client" % message #XXX maybe this shouldn't be an assert because it means there's a DoS here.
        
        print("-->", message)
        
        # stash the postgres cryptokey (which is sent in plaintext?!)        if isinstance(message, KeyData):
        if isinstance(message, BackendKeyData):
            self.key = message.key
        elif isinstance(message, ParameterStatus): #flatten the ParameterStatus messages into something nicer
            self.parameters[message.key] = message.value
        
        return message
    
    def process(self):
        "; this function mostly exists to be run on a background thread"
        
        # dealing with this protocol is complicated because it's not a simple request-response protocol. A single request can result in multiple responses, which sometimes come in a particular order and sometimes do not. We're facing a state machine, and that makes it difficult to write cleanly (i.e. functionally).
        # e.g. a single response to a query is at least three messages:
        # 1) a RowDescription, giving the fieldnames and order (in csv, the first header row)
        # 2) a series of DataRows
        # 3) a CommandComplete
        
        # It's *also* complicated because there's layered protocols, though they aren't called that
        # for example, the "walsender" mode is layered on SQL messages ("Q"ueries and "D"/"C" responses); like, IDENTIFY_SYSTEM returns a SQL table, even though it's metadata that isn't actually SQL.
        # Maybe we should actually implement some lines here, like "while Reading A Query Response: m = self.recv(); ..." and..stream out the result to a queue of some sort? a queue labelled by the table name or ..something? oh, but even more complicated for us, the protocol does not embed table names in the responses.
        # the protocol has "ReadyForQuery" to help us, at least
        
        while self._running:
            # this would be cleaner if I could say "select [pg, sys.stdin]"
            Sr, _, _ = select.select([self.sock], [], [], 1) #1s timeout means ~1s between the main thread terminating and this thread noticing and following suit
            if Sr:
                m = self.recv()
                #...?
                    
                    #...the best thing to do might actually be to spin off a thread...
                
        self.sock.close()
        self.sock = None

import threading
import IPython

def test():
    pg = PgClient("kousu", "postgres", {"replication": "on"}) #this line is picky!! I'll fix it up!!
    pg.send(Replication_IdentifySystem())
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