# replication_messages.py
"""
postgres replication protocol messages
 as defined in http://www.postgresql.org/docs/current/static/protocol-replication.html
 called 'walsender' (for "Write-Ahead-Log Sender") mode by the postgres daemon.
 
 
Replication is a subprotocol, layered on outer layer messages:
 messages from the client are embedded in Queries, and
 messages from the server, in CopyDatas (except that the initial response to IDENTIFY_SYSTEM is a RowDescription/DataRow pair, which PgReplicationClient records but does not expose as a type),
 Note that all the other messages in the protocol are in principle still valid to receive while in 
 walsender mode; in particular, ErrorResponse is a very possible response.
"""

from messages import Message, Query, CopyData, MessageSource, S
from util import *

class Replication_IdentifySystem(Query):
    def __init__(self):
        super().__init__("IDENTIFY_SYSTEM")

class Replication_Start(Query):
    def __init__(self, timeline, logpos):
        print(logpos)
        assert len(logpos.split("/")) == 2
        super().__init__("START_REPLICATION %s TIMELINE %s" % (logpos, timeline))


class Replication_Message(Message):
    """
    abstract base class for messages in the replication subprotocol
    
    TODO:
    an open question: who owns who?
    should Repliaction_Message create a hidden CopyData and call its render()? 
    should Replication_Message be a *subclass* of CopyData? (but this can't work, because the inner .TYPECODE would stomp the outer one)
    or should there be an entirely separate tree of message types, parallel to but distinct from the messages.Message hierarchy?
    
    I've settled on for now on inheriting from Message but essentially having a parallel tree (which is not a big deal in python, whose notion of inheritence is pretty optional).
    Replication_Messages have TYPECODEs and SOURCEs as with Messages, and these get checked and used,
    but their payload() methods are expected to only construct the payload on the level of the replication protocol
    and the wrapping in Querys and unwrapping of CopyDatas handled silently in the client event loop.
    """
    
    def render(self):
        "unlike main protocol messages, none of the replication protocol messages have a length field, so we need to redefine render"
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
        return "<%s> [%s..%s] @ %s \n---------------------\n[%s]" % (type(self).__name__, self.start, self.end, self.clock, self.records)




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



REPLICATION_MESSAGES = dict((m.TYPECODE, m) for m in locals().values() if type(m) is type and issubclass(m, Replication_Message))
