#!/usr/bin/env python3

import time, sys

def fib():
	a,b =1,1
	while True:
		yield a
		a,b = b, a+b

if __name__ == '__main__':
	for i,f in enumerate(fib()):
		print("[%d] %d" % (i, f))
		
		#sys.stdout.flush()
		print("spooled #%d" % i, file=sys.stderr, flush=True)
		time.sleep(2)
		
