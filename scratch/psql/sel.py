
import sys
import select

def chomp(s):
    "a la perl"
    return s.splitlines()[0]

def input_nonblock():
    "non-blocking input() call via select(); returns None if no input"
    "with this, you can write a REPL with concurrent background tasks in a single thread"
    "does not allow you to provide a prompt, because ; if yo uwant a prompt, explicitly write it to stdout before looping on this"
    "returns: either the input line (without a trailing newline), or None if there was nothing to read"
    Sr, Sw, Sx = select.select([sys.stdin], [], [], 0.001)
    if Sr:
        return input()
        #return chomp(sys.stdin.readline())

#TODO: figure out if its possible to give input_nonblock a prompt and have it pair exactly one prompt with exactly one input line 
# I guess otherewise I would have to have like...
# I could have a REPL coroutine..somehow?
# 
#..hm. how do i do this without threads?

def REPL():
    def input(*prompt):
        if prompt:
            print(prompt[0], end="")
        
        while True:
            l = input_nonblock()
            if l: return l
            yield
    while True:
        l = input(">>> ")
        #... hm
        # I want to have my code written as if synchronously: "when this is done do this with it"
        # with twisted's reactor, say, I would
        for o in l: yield
        E.value

# how can i make coroutine code look like normal procedural code?

i = 0
sys.stdout.write(">>> "); sys.stdout.flush()
while True:
    if i % 111111 == 0:
        pass #print(i)
    l = input_nonblock()
    if l:
        print(l)
        sys.stdout.write(">>> "); sys.stdout.flush()
    i+=1