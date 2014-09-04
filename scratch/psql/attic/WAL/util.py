# util.py
"""
postgres protocol utilities

"""

from collections import OrderedDict

from warnings import warn


# ----------------------------------------------
# --- timestamping

import time, datetime
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
    
    
# ------------------------------------------------------
# --- pgstruct: enhancements to the normal struct module
#               as needed by postgres 

import struct

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


# ----------------------------------------
# --- misc

def invert_enum(enum_cls):
    "*in-place* add a field to an enum which maps its values to its keys"
    enum_cls.__reverse_members__ = OrderedDict([(k.value, k) for k in enum_cls.__members__.values()])
