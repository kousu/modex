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
- [ ] find a good Vector class and use that

"""

import random
from copy import deepcopy

from itertools import *

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
        self.velocity = (random.normalvariate(0.05, .2), random.normalvariate(50, 2))
        self.velocity = (0,0)
        

class Universe(object):
    G = 6.67e-11
    G = 6.67e-2 #increasing the force of gravity makes things happen faster
    def __init__(self, num_planets):
        self.planets = [Planet() for i in range(num_planets)]
    def __iter__(self):
        return self
    def __next__(self):
        _previous = deepcopy(self) #double buffer the universe
        # compute the force on each planet
        # the force on each planet is the sum of the force of gravity on it from every other planet
        forces = [vector_add(*[self.gravity(p, o) for o in _previous.planets]) for p in self.planets] #notice that we loop over the previous state in the inner loop!
        print("forces", forces)
        print("velocities", [p.velocity for p in self.planets])
        print("positions", [p.position for p in self.planets])
        for i in range(len(self.planets)):
            # apply the velocity
            self.planets[i].position = vector_add(_previous.planets[i].position, _previous.planets[i].velocity)
            
            # apply the forces
            self.planets[i].velocity = vector_add(_previous.planets[i].velocity, self.force(forces[i], _previous.planets[i]))
            
            print()
    
    @staticmethod
    def force(F, m):
        "newton's second law"
        "apply force F to Planet m, functionally"
        
        # F = ma!
        return vector_mul(1/m.mass, F)
         
    
    @staticmethod
    def gravity(m, M):
        "newton's law of gravity, functionally"
        "gives the force that M exerts on m, as a tuple (x,y)"
        if m.position == M.position: #avoid a conflict case
            return (0,0)             #with a hack
            
        d = (dist(m.position, M.position))
        
        # 
        magnitude = -Universe.G * (m.mass * M.mass) / d**2
        if abs(magnitude) > 100:
            return (0,0)
        # direction vector...
        direction = vector_sub(m.position, M.position)
        #put them together
        return vector_mul(magnitude/d, direction)
        
        
        

class UniversePlotter(object):
    "a 2d physics renderer built on matplotlib"
    # it is frustrating that matplotlib forces IoC
    # For something this simple I would prefer to call figure.canvas.draw() myself
    def __init__(self, universe):
        import matplotlib.pyplot as plt
        import matplotlib.patches
        import matplotlib.animation
        import math
        import numpy as np
        self.universe = universe
        self.plt = plt
        
        self.plot = self.plt.scatter([0]*len(self.universe.planets),
                                     [0]*len(self.universe.planets),
                                     s=[p.radius if p.radius else 333 for p in self.universe.planets],
                                     cmap=plt.cm.terrain,
                                     c=np.linspace(0, 1, len(self.universe.planets)))
        # TODO: plot an arrow showing the force
        self.ani = matplotlib.animation.FuncAnimation(self.plot.figure, self.__next__, frames=range(300))
        
    def __next__(self, *args):
        xy = self.plot.get_offsets()
        next(self.universe) #why is this in here, you ask? because matplotlib forces IoC;
                            #unless you do all your processing on a thread (literally:
                            #  a python, GIL-strangled, threading module thread) it
                            # is impossible to simply use matplotlib like the widgets it is.
        #print("planets are at") #DEBUG
        #for p in U.planets:
        #    print(p.position)
            
        for i, p in enumerate(self.universe.planets):
            xy[i, ] = p.position
        
        self.plot.axes.set_xlim((-1e3, 1e3))
        self.plot.axes.set_ylim((-1e3, 1e5))
        return
        # update the plot limits to keep the planets on screen
        # but only do it as needed, to keep some continuitiy
        x_min, x_max = self.plot.axes.get_xlim()
        if not all(x_min < p.position[0] < x_max for p in self.universe.planets):
            self.plot.axes.set_xlim(min(xy[:,0]) - 10, max(xy[:,0]) + 10)
        y_min, y_max = self.plot.axes.get_ylim()
        if not all(y_min < p.position[1] < y_max for p in self.universe.planets):
            self.plot.axes.set_ylim(min(xy[:,1]) - 10, max(xy[:,1]) + 10)
        #self.fig.canvas.draw()
        
        
    def show(self):
        self.plt.show()

if __name__ == '__main__':
    U = Universe(2)
    d = UniversePlotter(U)
    d.show()