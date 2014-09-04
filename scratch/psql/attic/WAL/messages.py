# messages.py
"""
Class definitions for each sort of message in the postgres major protocol.
This is is not finished, but eventually should contain everything in
  http://www.postgresql.org/docs/current/static/protocol-message-formats.html
See replication_messages.py for the messages that form the replication subprotocol.

"""


from enum import Enum
from itertools import chain

from util import *


   
class ProtocolVersion:
    "Declare implemented protocol version by slightly misusing a class"
    # TODO: should this be in this file? it seems like it belongs somewhere more general, like in __init__.py or something
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




class FormatMode(Enum):
    Text = 0
    Binary = 1
invert_enum(FormatMode)



class Message(object):
    """
    abstract base class for messages in the postgres client-server protocol
    
    Every subclass must define
    TYPECODE - a 1-length bytes object containing the message type as spec'd in the message formats document 
    SOURCE - a MessageSource object, so the code can enforce that clients are clients and servers are servers.
    """
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
