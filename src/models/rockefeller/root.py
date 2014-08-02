

from threading import *
from mutex import *
from queue import *

import itertools

def flatten(L):
    "a one-level list flatten()"
    return list(itertools.chain(*L))

"""
agents: (ie objects which have a running thread)
-Employers
-Employees



data:
- skill levels, money, jobs, etc
"""

def rule(f):
    "a decorator to mark a method as intended for overriding"
    # TODO: somehow record the named version, so that we can automatically chart case-by-case 
    return f


class Employee:
    """
    A person (aka a job-seeker or an employee, depending on their state)
    has value
    
    .skill - the employee's skill level; this is a proportion, so that 0 is totally unskilled, -0.5 is "one half destructively skilled", and 1 is . Reasonable values should be 0.1 to 0.3.
    .job   - the employee's current job; set/unset by Job.[un]employ()
    """
    
    SEEKING_THRESHOLD = 1
    
    def __init__(self, skill):
        self.job = None
        
        assert 0<=skill<=1
        self.skill = skill
    
    @rule
    def evaluate_job(self, job):
        "a person's evaluation of a job determines how" 
        return 1
    
    def income(self):
        return float("nan")
        #raise NotImplementedError("TODO")
    
    
    @rule
    def seekingness(self):
        "a continuous number "
        " --> [0,1]"
        "This base implementation just says that one seeks if unemployed."
        "subclass and override to test out different rules"
        
        if self.job is None: return 1.0
        else: return 0.0
    
    def seeking(self):
        " --> bool"
        if self.seekingness() == 1: return True
        else: return False
        
        return self.seekingness() >= Employee.SEEKING_THRESHOLD
        
    def main(self):
        while True:
            world.log("employee", {"income": self.income()})
            
            # adjust skill points; the job's quality is used as a proportion increase
            if self.job:
                self.skill *= (1 + self.job.quality)
            
            if self.seeking():
                jobs = [world.
                open_jobs = [j for j in jobs if not j.employee]
                for j in open_jobs:
                    j.apply(self)
            

class Job:
    """
    - .employer - the parent object of this job: the employer. Never None
    - .employee - the current holder of this job; may be None
    - .name     - a string, naming the job. Purely descriptive; setting to None or "" won't hurt anything.
    - .quality  - a rating of the intrinsic value of the job (imagine this like the product of the job's difficulty, meaningfulness, 
    - .paybase  - the base pay grade, in dollars, that this job pays 
    """
    def __init__(self, parent, name, quality, paybase):
        self.employer = parent
        self.employee = None
        
        self.name = name
        assert 0<=quality<=1, "quality should be a floating point number in [0,1]" #TODO: raise ValueError
        self.quality = quality
        self.paybase = paybase
        
        self.applicants = []
    
    def employ(self, employee):
        assert isinstance(employee, Employee)
        self.employee, employee.job = employee, self
        
        self.applicants = [] #toss the old applicants; WARNING: we need to guarantee that this line happens *every timestep* (or every hiring cycle at least)
    
    
    def unemploy(self):
        assert self.employee is not None, "Tried to unemploy an already empty job"
        assert self.employee.job is self, "Tried to unemploy someone else's job"
        self.employee, self.employee.job = None, None
    
    
    def apply(self, employee):
        assert self.employee is None, "%s tried to apply to a filled job" % (employee,)
        self.applicants += [employee] 
    
    
    @rule
    def pay(self):
        if self.employee is None: return 0
        else:
            return self.paybase * (1 + self.employee.skill)


class Employer:
    """
    
    TODO:
    
    """
    def __init__(self, name):
        self.name = name
        self.jobs = []
        #
    
    @rule
    def profit(self):
        "computes the company's current profit, as a function of employees and such"
        "subclass and override to experiment with different models"
        return self.
        
    
    @rule
    def evaluate_seeker(self, job, employee):
        "return a rating of employee for job"
        " --> [0,1]"
    
    
    def main(self):
        "the logic an employer goes through"
        "run this on a thread"
        #TODO: work out concurrency
        while True:
            t = world.time() #blocks until
            world.log("employer", {"employer": id(self), "profits": self.profit()})
                                     # WARNING: ^ this is not universally unique
            # TODO: create jobs
            
            # hire and fire
            for j in self.jobs
            
            # fire
            for

class World:
    def __init__(self):
        timestep = Condition() #signalled when the timestep is updated. timestep is updated when all threads signal



#------------------

# an inheritence
# ...this would be better with prototype inheritence, since it's not clear the boundary between which is a class and which is an object.
# that is, it would be nice to be able to declare template jobs (e.g. Handyman, Call Center, Programmer, Child Care Worker) 
#  Mixins are also good for solving this!

# some specific jobs
class Landscaping(Job):
    def __init__(self, 


class Banker(Job):
    

def main():
    while