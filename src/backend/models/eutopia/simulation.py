from intervention import PriceIntervention, NewActivityIntervention
from eutopia import *
import json

class Scenario:
    def __init__(self, interventions):
        self.interventions = interventions
        
class Simulation:
    def __init__(self):
        self.scenarios = {}
        self.simpack = None # this will be eutopia.py, the simpack.
        self.current_interventions = None
        self.activities = []
        self.stepper = None
        self.years = 0
            
    def internal_step(self):
        """
        This iterates through all the interventions
        in the current scenario and runs the simpack
        specific stepping.
        """
        
        global time #time has to be global for Terry's simpack to work.
        for intervention in self.current_interventions:
            if time >= intervention.time:
                intervention.apply(self.simpack, time)
        time += 1
        self.simpack.step()
        
    def set_scenario(self, scenario_id):
        self.current_interventions = self.scenarios[scenario_id]
        
    def create_stepper(self, end):
        """
        Returns a generator defined with a start year and end
        year of the simulation.
        
        """     
        def internal_stepper():
            for year in xrange(1, end):
                self.internal_step()
                current_activity = self.simpack.get_activity_count()
                yield current_activity
        
        self.stepper = internal_stepper()
        self.years = end
            
            
    def step(self):
        """
        step() is called externally and it returns
        a datum of current activity if a stepper was 
        created.
        
        """
        try:
            return next(self.stepper)
        
        except TypeError:
            print "Internal stepper not created."
            
        except StopIteration:
            print "Simulation range exceeded."
        
        
            


def read_interventions(interventions):
    """
    Reads a json file with the format:
    [ {'name':, 'type':, 'data':[], 'simulation':,}
    ...
    ]
    and returns a simulation object, which in turn contains
    a dict of scenario objects, which each contain Intervention objects
    """
    simulation = Simulation()
    
    for intervention in interventions:
        scenario = intervention["scenario"]
        data = intervention["data"]
        
        if intervention["type"] == "priceIntervention":
            intervention_obj = PriceIntervention(*data)
        
        elif intervention["type"] == "newActivityIntervention":
            intervention_obj = NewActivityIntervention(*data)
                    
        try:
            simulation.scenarios[scenario].append(intervention_obj)
            
        except KeyError, scenario_id:
            simulation.scenarios[scenario_id.message] = [intervention_obj]
    
    return simulation
    

time = 0 # has to be global as per simpack format

        
if __name__ == "__main__":
    with open("interventions.json", "r") as input_json:
        interventions_json = json.load(input_json)
    
    sim = read_interventions(interventions_json)
    eutopia = Eutopia()
    sim.simpack = eutopia
    sim.set_scenario(1) # indexed by scenario 1
    sim.create_stepper(100) # creates a generator from simulation start and end dates.
    
    for i in xrange(1, 100):
        print sim.step()
        # this data will be sent whenever the web socket sends data.
        
    
        
        
    
