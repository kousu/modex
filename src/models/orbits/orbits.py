#!/usr/bin/python
"""
orbits.py

A simple 2d physics simulation.
You can view this as an agent-based model if you take a "Planet" to be an "Agent".

This is meant to exercise
- the breadth of application of our modex framework
- approaches to data logging
- approaches to visualization (interactive and non-interactive)
- threading and/or event queues

And to highlight some of the common pitfalls of time-stepped simulation
- the choice between having agents do computations of having the universe do computations
- inaccuracies due to discretizing continuous processes
- the use of double-buffering to avoid (and especially to order of agent evaluation imposing artificial bias)
- handling conflict states

TODO:
- [ ] orbits
- [ ] elastic collisions ("handle conflicts") 
- [ ] handle conflicts more generally, with message-passing OOP: make each "agent" send a request of where they want to go instead of having them choose themselves

"""

import random
from copy import deepcopy

def vector_add(*vs): #quick hack; if this needs anything more than this we should invest in finding a physics library
    return [sum(s) for s in zip(*vs)]

def vector_sub(L, R):
    return [(l - r) for l,r in zip(L,R)]

def vector_mul(c, v):
    return tuple([c*e for e in v])

def dist(A,B):
    "euclidean distance"
    return sum(o**2 for o in vector_sub(A,B))**0.5

class Planet(object):
    """
    """
    def __init__(self, mass=None, radius=0):
        """
        If mass is not given, defaults to a N(100, 9)
        If radius is 0, this represents a point particle
        Position and velocity are initialized at random
        """
        
        if mass is None:
            mass = random.normalvariate(100, 3)
        self.mass = mass
        self.radius = radius
        self.position = (random.normalvariate(50, 2), random.normalvariate(50, 2))
        print(self.position)
        self.velocity = (random.normalvariate(50, 2), random.normalvariate(50, 2))
        

class Universe(object):
    G = 6.67e-11
    def __init__(self, num_planets):
        self.planets = [Planet() for i in range(num_planets)]
    def __iter__(self):
        return self
    def __next__(self):
        _previous = deepcopy(self) #double buffer the universe
        # compute the force on each planet
        forces = [vector_add(*[self.gravity(p, o) for o in _previous.planets if o is not p]) for p in self.planets] #notice that we loop over the previous state in the inner loop!
        for i in range(len(self.planets)):
            # apply the total force
            #self.planets[i].velocity = forces[i]
            pass
        
    @staticmethod
    def gravity(m, M):
        "newton's law of gravity"
        "gives the force that M exerts on m, as a tuple (x,y)"
        if m.position == M.position:
            return (0,0) #HACK
        d = (dist(m.position, M.position))
        magnitude = Universe.G * (m.mass * M.mass) / d**2
        # direction vector...
        direction = vector_sub(m.position, M.position)
        return vector_mul(magnitude/d, direction)
        
        

class UniversePlotter(object):
    "a 2d physics renderer built on matplotlib"
    def __init__(self, universe):
        import matplotlib.pyplot as plt
        import matplotlib.patches
        self.universe = universe
        self.plt = plt
        self.plt.patches = matplotlib.patches
        
    def __next__(self):
        circles = [self.plt.patches.Circle(p.position, p.radius, fill=True) for p in self.universe.planets]
        
        self.universe.planets

if __name__ == '__main__':
    U = Universe(2)
    d = UniversePlotter(U)
    for u in U:
        next(d)
        print("planets are at")
        for p in U.planets:
            print(p.position)
        print()