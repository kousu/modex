#!/usr/bin/env python3

import subprocess
import time

import select

with open("spool.err","wb",buffering=0) as log:
	p1 = subprocess.Popen(["python", "-u", "./spool.py"], bufsize=0, stdout=subprocess.PIPE, stderr=log)
	import IPython; IPython.embed()
