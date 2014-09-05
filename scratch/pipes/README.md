Experimenting with constructing pipelines with subprocess.py

The one big win that shell has over python is that doing "./spool | filter | reduce" is really easy and really efficient: as efficient as the individual programs
But the subprocess module is finicky about doing this sort of thing.

[the docs](https://docs.python.org/2/library/subprocess.html#replacing-shell-pipeline) claim you can build pipelines like this:
```
output=`dmesg | grep hda`
# becomes
p1 = Popen(["dmesg"], stdout=PIPE)
p2 = Popen(["grep", "hda"], stdin=p1.stdout, stdout=PIPE)
p1.stdout.close()  # Allow p1 to receive a SIGPIPE if p2 exits.
output = p2.communicate()[0]
```

But that is misleading: `communicate()` buffers the input between the two processes.

`spool.py` + `s2.py` demonstrates that to get things running in real time, the thing is for the producer
(`spool.py` or `dmesg` or whatever is on the left side of the pipe) to call `flush()` to get things moving.
Yet, running `spool.py` on a terminal gets output immediately.
Why is this? The pipe constructed by python by default has a buffer size of io.DEFAULT_BUFFER_SIZE(==8192).
 Frustratingly, setting the 'bufsize' argument to 0 does *not* automatically give unbuffered .stdout, but it does set .stdout to be what would otherwise be .stdout.raw
 There was [a patch](http://bugs.python.org/issue11459) to 3.1 and 3.2 that should have fixed it
 But you need also the writer to not be doing its own buffering (just because your side of a pipe is unbuffered doesn't mean the other side is)
e.g. [see](http://chase-seibert.github.io/blog/2012/11/16/python-subprocess-asynchronous-read-stdout.html): """you need to make sure that the subprocess you are invoking is not doing its own buffering. It took me a bit to figure out that mysql does do that, which is what the --unbuffered flag is there to disable.""" and [this](http://stackoverflow.com/questions/107705/python-output-buffering)


* [Related technical links](http://bugs.python.org/issue19929) (hidden in a bug report)
* http://objectmix.com/python/383415-working-around-buffering-issues-when-writing-pipes.html

By experiment, the buffer size on my computer seems to be somewhere around 10000000>>4 bytes.
Link above suggests it should be 65536 bytes.

[some systems](http://www.gnu.org/software/libc/manual/html_node/Controlling-Buffering.html) has a way to control buffering on an already open file descriptor.


