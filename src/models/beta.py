"""
a dead-simple "scientific model" which represents one extreme end of the model spectrum.
This kind of braindead code is very useful in the right context.

The question this code raises is: how do we connect to the data and parameters in it?
And how do we connect to them in a way that generalizes across connecting to models 
written in R, Java (especially Repast Simphony), or with complicated underlying libraries like PyABM?
"""
import random
import time

#Model parameters:
#if the model was run under Model Explorer we would expect to
# be able to tweak ALPHA and BETA as the model was running
ALPHA = .5 #these are globals on purpose: braindead models are often written quick and dirty like this. 
BETA = 10  #We must account for this behaviour; forcing total model rewrites will make us unattractive to users.

def model(): #generator object
    while True:
        yield random.betavariate(ALPHA, BETA)

if __name__ == '__main__':
    for output in model():
        print output
        time.sleep(.2)
