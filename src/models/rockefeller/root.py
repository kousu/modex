

#from threading import *
#from mutex import *
#from queue import *

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
        if self.job is not None: return self.job.pay()
        else: return 0
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
        return self.seekingness() >= Employee.SEEKING_THRESHOLD
        
    def step(self):
        world.log("employee", {"income": self.income()})
        
        # adjust skill points; the job's quality is used as a proportion increase
        if self.job:
            self.skill *= (1 + self.job.quality)
        
        if self.seeking():
            jobs = flatten([E.jobs for E in world.employers])
            open_jobs = [j for j in jobs if not j.employee]
            print(jobs)
            for j in open_jobs:
                world.log("applications", {"job": j, "seeker": self})
                j.apply(self)

    @property
    def name(self):
        return str(id(self)) #TODO: use a name generator to make the model more friendly
    
    def __str__(self):
        return "<Employee: %s>" % (self.name,)

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
        
        print("Employing %s in %s" % (employee, self))
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
            
    def __str__(self):
        return "<Job: %s>" % (self.name,)


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
        return -1 #TODO
        
    
    @rule
    def evaluate_seeker(self, job, employee):
        "return a rating of employee for job"
        " --> [0,1]"
        return employee.skill
    
    def step(self):
        "the logic an employer goes through"
        "run this on a thread"
        #TODO: work out concurrency
        
        t = world.time() #blocks until
        world.log("employer", {"employer": self.name, "profits": self.profit()})
                                 # WARNING: ^ this is not universally unique
        # TODO: create jobs
        
        # hire
        for j in self.jobs:
            applicants = [e for e in j.applicants if e.job is None] #applicants might get a job in between applying and the end of the timestep, so we need to check for that
            if not applicants: continue #if the above filtered out everyone, skip hiring for this job
            applicants.sort(key=lambda e: self.evaluate_seeker(j, e))
            j.employ(applicants[-1]) #choose the highest (#XXX corner case seekers with the same evaluation should be decided on..randomly?)
        
        
        # fire
        # for ... ??
        

    def __str__(self):
        return "<Employer: %s>" % (self.name,)

class World:
    def __init__(self):
        #timestep = Condition() #signalled when the timestep is updated. timestep is updated when all threads signal
        self.employers = []
        self.employees = []
        self._time = 0
        
    def step(self):
        for e in self.employees:
            e.step()
        for E in self.employers:
            E.step()
        self._time += 1
            
    def time(self):
        return self._time
        
    def log(self, *args):
        print("LOG", args)

world = World() #singleton (AWKWARD)

#------------------

# an inheritence
# ...this would be better with prototype inheritence, since it's not clear the boundary between which is a class and which is an object.
# that is, it would be nice to be able to declare template jobs (e.g. Handyman, Call Center, Programmer, Child Care Worker) 
#  Mixins are also good for solving this!

# some specific jobs
#class Landscaping(Job):
#    def __init__(self):
#        Job.__init__(self, 


class Banker(Job):
    pass


def main():
    while True:
        world.step()
    
    
def test():

    # Painfully define, by hand, all the simulation objects
    
    E1 = Employer("BigCoCorp")
    E2 = Employer("Organic Farm Fresh1")
    world.employers.append(E1)
    world.employers.append(E2)
    
    e1 = Employee(0.1)
    e2 = Employee(0.3)
    e3 = Employee(0.8)
    world.employees.append(e1)
    world.employees.append(e2)
    world.employees.append(e3)
    
    
    j1 = Job(E1, "Landscaping", .5, 10000)
    j2 = Job(E1, "Designer", .8, 10000)
    j3 = Job(E1, "Burgers", .01, 10000)
    j4 = Job(E1, "Cashier", .10, 10000)
    
    E1.jobs.append(j1)
    E1.jobs.append(j2)
    E2.jobs.append(j3)
    E2.jobs.append(j4)
    
    # run the model
    main()
    
    
if __name__ == '__main__':
    test()