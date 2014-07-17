#!/usr/bin/python2
"""emulate this bash script, in a cross-platform and working-directory-agnostic way:
```bash
 src/backend/server.py &
 firefox http://localhost:8080/
```
"""

# TODO: silence messages from the webrowser (they are very distracting)

import sys
from os.path import dirname, abspath, join as pathjoin


import os
import threading, webbrowser, time
import subprocess

#'working directory': not the system working directory, but the directory this program is in (so that we can be run from anywhere and$
PROJECT_ROOT = dirname(abspath(__file__)) #run.py should always sit in the project root

def browse():
  time.sleep(2) #give time for the server to spin up
  #webbrowser.open('http://127.0.0.1:8080')  #the URL here is hardcoded and needs to match what server.py spins up on

if __name__ == '__main__':
     
  PYTHON = sys.executable # simply use the same python that called the run.py script. 
  
  browser = threading.Thread(target=browse) 
  browser.start()  #open a browser to the webapp's page (nonblocking)
                   # this is glitchy: depending on the mixture of close buttons and ctrl-cs sent
  
  # even though the script we're emulating runs the server in the background
  # it is cleaner to run everything else in the background and run the server on the mainthread
  # because call() hooks SIGINT (or your local system's favourite shutdown signal)
  # and politely but firmly kills the server no matter how run.py ends
  subprocess.call([PYTHON, pathjoin(PROJECT_ROOT, "src", "backend", "server.py"), "--debug", "--clean"])
  # TODO(kousu): only browser.start() if the server comes up (requires some kind of pegging; ugh)
  # TODO(kousu): figure out why the browser opens twice (sometimes) if you press ctrl-c or the server fails
