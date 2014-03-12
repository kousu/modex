import sys
from os.path import dirname, abspath, join as pathjoin
import json
from models.eutopia.intervention import PriceIntervention, NewActivityIntervention
from models.eutopia.eutopia import Eutopia
from models.eutopia.simulation import read_interventions

PROJECT_ROOT = dirname(dirname(dirname(abspath(__file__))))
simdir = pathjoin(PROJECT_ROOT, "src", "backend", "models", "eutopia")
test_data = pathjoin(simdir, "interventions.json")

with open(test_data, "r") as input_json:
   interventions_json = json.load(input_json)

time = 0
sim = read_interventions(interventions_json)
eutopia = Eutopia()
sim.simpack = eutopia
sim.set_scenario(1) # indexed by scenario 1
sim.create_stepper(100) # creates a generator from simulation start and end dates.


