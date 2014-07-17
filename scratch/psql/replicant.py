# implement the client side of http://www.postgresql.org/docs/current/static/protocol-flow.html#AEN98939
# the reason I'm doing this is because I want to use http://www.postgresql.org/docs/current/static/protocol-replication.html *over a websocket*, and, personally, it's easier to prototype with python

"""
scrap notes:

- the protocol is a mixture of binary and string data
- struct.(un)pack is useful for us for the binary data, but the string data is C-strings, which struct.pack does not handle (it handles fixed sized strings and pascal strings). This means we need to do scanning to parse these parts.
- the possible messages at http://www.postgresql.org/docs/current/static/protocol-message-formats.html; this prototype focuses on the client protocol, meaning only ones marked "B"(ackend) have parse() written and only ones marked "F"(rontend) have payload() written

"""

import socket
import struct

from enum import Enum
from itertools import chain

from collections import OrderedDict
import select
import sys
import time

import traceback

from warnings import warn




# for timestamping
import datetime
from calendar import timegm
datetime.timegm = timegm #"I can’t for the life of me understand why the function timegm is part of the calendar module."
del timegm               #http://ruslanspivak.com/2011/07/20/how-to-convert-python-utc-datetime-object-to-unix-timestamp/

def pgtime(t=None):
    "Make timekeeping more consistent by converting from system time to postgres time"
    "system time (for us) is whatever type is returned by time.time(), and defaults to the current time."
    " usually, this is standard unix time: a floating point in seconds (1e6 microseconds!) since 1970-01-01"
    "(but if not, the difference *should* be transparent to us, because we use the datetime module)"
    " a postgres time is integer microseconds since 2000-01-01"
    if t is None: t = time.time()
    
    epoch = datetime.datetime(2000, 1, 1) #the postgres epoch
    now = datetime.datetime.fromtimestamp(t)
    dt = now - epoch #find the time since the epoch
    v = int(dt.total_seconds()*1e6) #convert to integer microseconds
    assert 0 <= v < 1<<64, "pgtime must be a 64-bit unsigned integer"
    return v
    
    
def pgstruct_commands(fmt):
    "scan and parse a postgres struct format string, which is the same as a regular format string except that it cannot have its endianness specified (because postgres uses network (big) endianness) and 's' means nul-terminated non-fixed-size C strings"
    "also, for now, we make the assumption that strings mean ascii strings, though supposedly there's a way to configure this"
    "note: we *also* support Int64s and for this we again repurpose 'q' which is in the struct spec (but not in the *standard* struct spec only in 'native mode'"
    if len(fmt) == 0:
        return
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
        
        assert cmd[-1] not in ["p", "n", "N", "P"], "%s only available in native struct format, which postgres, using network format, does not use" % cmd[-1]
        # "q", "Q"]
        
        
        #assert cmd in ["x","c","b","B","?","h","H","i","I","l","L","f","d","s","p", "n","N", "P", "q","Q"], "bad char in struct format"
        yield cmd
        
              
#print(list(pgstruct_commands("x x h y hy h x   3xx xx x")))


def pgpack(fmt, *args):
    "postgres-pack: stuff some data into a single structure"
    "this is here to enforce that everything in Postgres is in network byte order"
    "XXX this currently FORCES ASCII. Be warned."
    
    # in order to change the meaning of 's' we need to do at least a bit of parse ourselves
    # I think(?) the least slow (but still stuck in python) way to do this is by doing each individually in a generator then join() the results
    
    #TODO: this would be faster if we were clever about finding chunks of string vs non-string blocks and encoded the non-string blocks all at once, since that's done by the _struct.so module
    
    def fm(cmd, a):
        if cmd == "s":
            assert isinstance(a, str)
            return a.encode("ascii") + b"\x00" #nul-terminate the string and pass it back
        elif cmd.lower() == "q":
            if cmd == "q":
                warn("signed Int64s are untested")
            cmd = {"q": "i", "Q": "I"}[cmd]
            # we do Int64s by reducing them to two Int32s and concatenanting
            # we should be able to ignore because the bitmask will handle that ...
            #...hm. how do negatives affect the results?
            
            return fm(cmd, a>>32) + fm(cmd, 0xFFFFFFFF & a) #network order means most significant bytes first! we concate
        
        # otherwise fall back on normal
        return struct.pack("!" + cmd, a) # ! means 'network order' per https://docs.python.org/3.4/library/struct.html
    
    return bytes.join(b"", (fm(c, a) for c,a in zip(pgstruct_commands(fmt), args)))
    

#print(pgpack("sh", "i am a merry model", 888))

def pgunpack(fmt, buffer):
    r, o = pgunpack_from(fmt, buffer)
    assert o == len(buffer), "pgunpack requires an exact-length buffer"
    return r

def pgunpack_from(fmt, buffer, offset=0):
    "the return value of this is *two*-valued: the usual tuple of results from struct.unpack *plus* the *remainder of buffer*, because due to C-strings there is no way to know before calling this how much of buffer will get eaten"
    "the struct module does it differently; since all its fields have their width specified in the format string, it can provide a (functional-style) routine 'calcsize' and force you to use it, even with pascal strings *which have a size byte embedded*"
    "but we only find out our field widths during parsing"
    "XXX this currently ASSUMES ASCII. Be warned."
    
    # this cannot be done functionally, because the width of each buffer is variable, due to C-strings
    
    def parse(): #a generator to do the parsing for us
        nonlocal offset
        # ..hm. okkkkay. this is complicated.
        # buffer commands that *are not 's'* and unpack them all at once
        
        
        def buffered_commands(fmt):
            # yields ONLY either 's' or a valid format string of fixed width entries
            cmds = []
            
            for cmd in pgstruct_commands(fmt):
                if cmd in ["s", "q", "Q"]:
                    if cmds:
                        yield str.join(" ", cmds); cmds = []
                    yield cmd
                else:
                    cmds.append(cmd)
            
            if cmds:
                yield str.join(" ", cmds); cmds = []
        
        
        for cmd in buffered_commands(fmt):
            if cmd == "s":
                e = buffer.find(b"\x00", offset)
                yield buffer[offset:e].decode("ascii")
                offset = e + 1
            elif cmd.lower() == "q": #Int64 support
                cmd = {"q": "i", "Q": "I"}[cmd]
                M, m = struct.unpack_from("!" + cmd + cmd, buffer, offset) 
                yield M << 32 | m #do the reverse of the concatenation in pgpack(): extract each 4 byte block and combine them (with our old friends bitshift and or)
                offset += 8 #8 bytes in a 64 bit number
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
#MessageSource enum-like aliases, for brevity
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
        # 'chain' and the two starmaps are from http://stackoverflow.com/questions/406121/flattening-a-shallow-list-in-python
        return pgpack("I", VERSION) + pgpack("ss"*len(self.options), *chain(*self.options.items())) + self._TERMINATOR

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
       key, value = pgunpack("ss", payload)
                  
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

class ErrorResponse(Message):
    TYPECODE = b"E"
    SOURCE = S.B
    "The message body consists of one or more identified fields, followed by a zero byte as a terminator. Fields can appear in any order. For each field there is the following:"
    def __init__(self, fields):
        "fields is a dictionary mapping single characters to strings. The chars are message types (currently, it is not worth the effort to make proper classes for all these)"
        self.fields = fields
    
    @classmethod
    def parse(cls, payload):
        p = 0
        fields = {}
        while p < len(payload):
            (code, content), p = pgunpack_from("c s", payload, p)
            if code == b"\x00":
                break
            code = code.decode("ascii")
            print(code, content, p)
            fields[code] = content
        assert p == len(payload)
        return cls(fields)
    
    def __str__(self):
        return "<%s> %s" % (type(self).__name__, self.fields)
        
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
        def __init__(self, rows):
            self.rows = rows
    class Update(Tag):
        def __init__(self, rows):
            self.rows = rows
    class Select(Tag):
        def __init__(self, rows):
            self.rows = rows
    class Move(Tag):
        def __init__(self, rows):
            self.rows = rows
    
    class Fetch(Tag):
        def __init__(self, rows):
            self.rows = rows
    
    class Copy(Tag):
        def __init__(self, rows):
            self.rows = rows
    
    def __init__(self, tag):
        self.tag = tag #TODO: structure this; the tags themselves    
    
    @staticmethod
    def parsetag(tag):
    
        #I D U S M F C
        h = tag[0:1] #I cheat: I parse the tag by looking at the first byte; note, with *bytes*, indexing is different from slicing, which is why we use 0:1
        w = "it is an error to add strings and numbers in python"
        if h == b"I":
            w = 6
            assert tag[:w] == b"INSERT"
            return CommandComplete.Insert(tag[w:]) #TODO: parse this better!!
        elif h == b"D":
            w = 6
            assert tag[:w] == b"DELETE"
            return CommandComplete.Delete(tag[w:]) #TODO: parse this better!!
        elif h == b"U":
            w = 6
            assert tag[:w] == b"UPDATE"
            return CommandComplete.Update(tag[w:]) #TODO: parse this better!!
        elif h == b"S":
            w = 6
            assert tag[:w] == b"SELECT"
            return CommandComplete.Select(tag[w:]) #TODO: parse this better!!
        elif h == b"M":
            w = 4
            assert tag[:w] == b"MOVE"
            return CommandComplete.Move(tag[w:]) #TODO: parse this better!!
        elif h == b"F":
            w = 5
            assert tag[:w] == b"FETCH"
            return CommandComplete.Fetch(tag[w:]) #TODO: parse this better!!
        elif h == b"C":
            w = 4
            assert tag[:w] == b"COPY"
            return CommandComplete.Copy(tag[w:]) #TODO: parse this better!!
        else:
            raise ValueError("Unknown tag", tag)
    
    @classmethod
    def parse(cls, payload):
        return cls(cls.parsetag(payload))
    def __str__(self):
        return "<%s.%s>" % (type(self).__name__, type(self.tag).__name__)

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
        return pgpack("s", self.string)
        
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


class CopyData(Message):
    "Data that forms part of a COPY data stream. Messages sent from the backend will always correspond to single data rows, but messages sent by frontends might divide the data stream arbitrarily."
    TYPECODE = b"d"
    SOURCE = S.FB
    
    def __init__(self, data):
        self.data = data
    
    def payload(self):
        return self.data
        
    @classmethod
    def parse(cls, payload):
        return cls(payload)
    def __str__(self):
        return "<%s> %s" % (type(self).__name__, self.data)




class CopyBothResponse(Message):
    "This message is used only for Streaming Replication, but it happens at the outer layer protocol, so it must be grouped up here"
    TYPECODE = b"W"
    SOURCE = S.B
    
    def __init__(self, format, columnformats):
        if format == FormatMode.Text:
            assert all(f == FormatMode.Text for f in columnformats) #All must be zero if the overall copy format is textual.
        self.format = format
        self.columns = columnformats
    
    @classmethod
    def parse(cls, payload):
        (format, n), p = pgunpack_from("b h", payload)
        columns, p = pgunpack_from("h"*n, payload, p)
        format = FormatMode.__reverse_members__[format]
        columns = [FormatMode.__reverse_members__[f] for f in columns] #XXX there's a DoS here! should use .get() or something. or maybe wrap the whole parsing shennanigans in a try-catch
        
        return cls(format, columns)
    
    def __str__(self):
        return "<%s> %s | %s" % (type(self).__name__, self.format, self.columns)


MESSAGES = dict((m.TYPECODE, m) for m in locals().values() if type(m) is type and issubclass(m, Message))

### replication protocol messages 
### all of these are passed as queries ("Q") messages
### http://www.postgresql.org/docs/current/static/protocol-replication.html

# XXX put these in a submodule
# 

class Replication_IdentifySystem(Query):
    def __init__(self):
        super().__init__("IDENTIFY_SYSTEM")

class Replication_Start(Query):
    def __init__(self, timeline, logpos):
        print(logpos)
        assert len(logpos.split("/")) == 2
        super().__init__("START_REPLICATION %s TIMELINE %s" % (logpos, timeline))

# so, a question:
#  who owns who (as usual):
 # should Repliaction_Message create a hidden CopyData and call its render()? 
 # should Replication_Message be a *subclass* of CopyData? (but this can't work, because the inner .TYPECODE would stomp the outer one)
 # 

#

class Replication_Message(Message):
    "in the replication subprotocol, messages from the client are embedded in Queries and messages from the server, in CopyDatas (except for the initial response to IDENTIFY_SYSTEM, which is a RowDescription/DataRow pair), and they do not come with a message length field"
    " we handle this wrapping and unwrapping below, in the event loop; the classes only know how to render themselves at the replication layer "
    def render(self):
        assert self.TYPECODE is not None, "You need to define TYPECODE in class %s" % type(self)
        return self.TYPECODE + self.payload()
    

class Replication_XLogData(Replication_Message):
    TYPECODE = b"w"
    SOURCE = S.B
    # this message is only ever found *inside* of a CopyData
    # but it's not really, itself, a CopyData message
    
    def __init__(self, start, end, clock, records):
        self.start, self.end = start, end
        self.clock = clock
        self.records = records
    
    @classmethod
    def parse(cls, payload):
        (start, end, clock), p = pgunpack_from("L L L", payload)
        records = payload[p:] #XXX the records should be parsed!!
        return cls(start, end, clock, records)
    
    def __str__(self):
        return "<%s> [%s..%s] @ %s" % (type(self).__name__, self.start, self.end, self.clock)




class Replication_Keepalive(Replication_Message):
    " end is 'The current end of WAL on the server'"
    " clock is the server's system clock when it sent the message server; if you specify this it must be in postgres format."
    " ping is a boolean where 1 means the server will disconnect you if you don't reply with a Replication_StandbyStatusUpdate or a Replication_HotStandbyFeedbackMessage soon"
    TYPECODE = b"k"
    SOURCE = S.B
    # this message is only ever found *inside* of a CopyData
    # but it's not really, itself, a CopyData message
    
    def __init__(self, end, clock=None, ping=True):
        self.end = end
        if clock is None:
            clock = pgtime()
        self.clock = clock
        self.ping = ping
    
    @classmethod
    def parse(cls, payload):
        end, clock, ping = pgunpack("Q Q ?", payload)
        return cls(end, clock, ping)
    
    def __str__(self):
        return "<%s> [..%s] @ %s; %s" % (type(self).__name__, self.end, self.clock, "[PING]" if self.ping else "")

class Replication_StatusUpdate(NotImplementedError):
    TYPECODE = b"r"
    SOURCE = S.F
    pass

class Replication_HotStandbyFeedbackMessage(Replication_Message):
    TYPECODE = b"h"
    SOURCE = S.F
    
    def __init__(self, xmin, clock=None, epoch=0):
        "clock is microseconds since midnight 2000-01-01. it defaults to the current time" 
        "epoch seems badly undocumented e.g. <http://www.postgresql.org/docs/current/static/app-pgresetxlog.html> 'the transaction ID epoch is not actually stored anywhere in the database except in the field that is set by pg_resetxlog,'?? so it defaults to 0"
        "if xmin is 0 it is considered notice that this ping is going to 'turn off' whatever that means"
        self.xmin = xmin
        if clock is None:
            clock = pgtime()
        self.clock = clock
            
        self.epoch = epoch
        
    def payload(self):
        # if clock isn't set we set it here, at render-time instead of at init time, to reduce the lag latency implicit in the timestamp as much as possible 
        #XXX this time.time() call is almost certainly wrong because the postgres epoch is year 2000
        return pgpack("Q I I", self.clock, self.xmin, self.epoch)

class FormatMode(Enum):
    Text = 0
    Binary = 1
invert_enum(FormatMode)


# oh my gosh
# there is a protocol identical in structure to the basic protocol that in walsender mode is layered on top of CopyData

REPLICATION_MESSAGES = dict((m.TYPECODE, m) for m in locals().values() if type(m) is type and issubclass(m, Replication_Message))

#-----------------------

class StopCoroutine(StopIteration): pass


#IDEA:

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
        "; this function mostly exists to be run on a background thread"
        
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