#!/usr/bin/env python2
# this program requires python2 because GDAL requires python2

# system libs
import os
from itertools import izip

# local libs
from pygdal import *
from util import *

# eutopia files
import activity
import intervention

HERE = os.path.abspath(os.path.dirname(__file__))


#######################
## Exports

# from ourselves
__all__ = ['Farm', 'FarmFamily', 'Eutopia'] #XXX should we maybe only export "Eutopia" and just ask users to access Farms via Eutopia?
__all__ += ['create_demo_model']            #for testing (only(?))

# from intervention.py
from intervention import *    # put into local namespace for reexport
                              # users of Eutopia need to be able to create Interventions that Eutopia understands,
                              # so they need to have these symbols available.
__all__ += ['PriceIntervention', 'NewActivityIntervention'] 

# from activity.py
from activity import Activity  #ditto #XXX this is probably not super well designed.
__all__ += ['Activity']


#######################
## eutopia

MAP_SHAPEFILE = os.path.join(HERE, "Elora_esque.shp.zip") #not in the repo due to copyright; ask a team member
# TODO: test with passing a folder instead

AGRICULTURE_CODES = { #hardcoded out of the ARI dataset
   #non-agriculture features are commented out
   'C': 'CORN SYSTEM',
   #'B': 'BUILT UP',
   'G': 'GRAZING SYSTEM',
   'PC': 'PEACHES-CHERRIES',
   'H': 'HAY SYSTEM',
   'M': 'MIXED SYSTEM',
   'MG': 'GRAIN SYSTEM',
   'KM': 'MARKET GARDENS/TRUCK FARMS',
   'KN': 'NURSERY',
   'P': 'CONTINUOUS ROW CROP',
   #'A1': 'IDLE AGRICULTURAL LAND (5-10 YEARS)',
   #'A2': 'IDLE AGRICULTURAL LAND (OVER 10 YEARS)',
   #'W': 'WATER',
   #'X': 'SWAMP, MARSH OR BOG',
   #'R': 'RECREATION',
   #'Z': 'WOODLAND',
   #'E1': 'EXTRACTION PITS AND QUARRIES',
   #'HG': 'PASTURE SYSTEM',
   #'ZR': 'REFORESTATION'
}

class Farm(Feature):
    def __init__(self, feature):
        Feature.__init__(self, feature)
        #self.land_type = land_type #hmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmmm
        self.county = "BestCountyInTheWorldIsMyCountyAndNotYours"

        self.last_activity = None
        self._lat = None
        self._long = None

    #backwards compat
    @property
    def id(self): return self.fields.GetFID()

    @property
    @memoize
    def lat(self):
        return self.geometry.Centroid().GetY() 

    @property
    @memoize #fun fact: memoizing this function saves 700 times the calls
             #          --nearly 3 orders of magnitude-- as of this commit.
    def long(self):
        return self.geometry.Centroid().GetX()

    @property
    def area(self): return self.geometry.Area()

class FarmFamily:
    def __init__(self, eutopia):
        self.eutopia = eutopia
        self.farms = []
        self.bank_balance = 1000000.00
        self.equipment = []
        self.preferences = {'money': 1.0, 
                            'follow_society':0.1,  # how important is following what everyone else is doing
                            'follow_local':0.2,    # how important is doing what my neighbours are doing
                            }

    def add_farm(self, farm):
        self.farms.append(farm)
        farm.family = self

    def make_planting_decision(self, activities, farm):
        all_activities = dict(self.eutopia.latest_activity_count)
        total_activities = float(sum(all_activities.values()))
        if total_activities > 0:
            for k,v in all_activities.items():
                all_activities[k]/=total_activities

        if self.preferences.get('follow_local', 0) != 0:
            local_activities = self.eutopia.get_activity_count(farm.neighbours)
            total_activities = float(sum(local_activities.values()))
            if total_activities > 0:
                for k,v in local_activities.items():
                    local_activities[k]/=total_activities


        


        best = None
        for activity in activities:
            total = 0
            for pref, weight in self.preferences.items():
                if pref=='follow_society':
                    total += all_activities.get(activity.name,0) * weight
                elif pref=='follow_local':
                    if weight != 0:
                        total += local_activities.get(activity.name,0) * weight
                else:
                    total += activity.get_product(pref, farm) * weight
                # TODO: improve choice algorithm
                #    - maybe by allowing different sensitivities to risk
                #      on different income dimensions

            
            if best is None or total > best_total:
                best = activity
                best_total = total

        return best


    def step(self):
        for farm in self.farms:
            # changed to self.eutopia to make it work with the sim version that is passed to Family21
            activity = self.make_planting_decision(self.eutopia.activities.activities, farm)

            money = activity.get_product('money', farm)
            self.bank_balance += money

            farm.last_activity = activity



class Eutopia:
    """
    The Eutopic World
    The main simulation class
    There is an API here for controlling and querying the model state
    """
    def __init__(self, log = None):
        self.log = log

        try:
            shapefile = Shapefile(MAP_SHAPEFILE)
        except IOError:       #py2.7
            #except FileNotFoundError: #py3k
            raise RuntimeError("No shapefile `%s` found; you may need to download it from a team member (privately)" % MAP_SHAPEFILE)

        self.map = shapefile[0] #cheating: assume the only layer we care about is this one
        assert self.map.GetGeomType() == wkbPolygon, "Farm boundaries layer is not a Polygon; it is a" + str.join(" or ", invertOGRConstants(self.map.GetGeomType()))
        #assert isinstance(self.map, PolygonLayer), "Farm boundaries layer is not a Polygon; it is a " + str(type(self.map))

        #########################
        # modelling begins here
        self.time = 0
        self.activities = activity.Activities()
        self.interventions = []

        #XXX should we write this as literally constructing a new Layer?
        # for now, a List is alright, but it's worth thinking about doing that and about what pygdal requires to support doing that
        self.farms = [Farm(f) for f in self.map if f.MAP_CODE in AGRICULTURE_CODES.keys()]
        print("Constructed", len(self.farms), "farms", "out of", len(self.map), "features")

        self.families = []
        # for now, every Family goes with one single Farm on it
        for farm in self.farms:
            family = FarmFamily(self)
            family.add_farm(farm)
            self.families.append(family)

        for farm in self.farms:
            farm.neighbours = self.get_local_farms(farm.lat, farm.long, count=10)

    def dumpsMap(self):
        "convert the map data to a GeoJSON string"
        "meant to be used in a ModelExplorer endpoint"
        return self.map.dumps()
    
    def dumpMap(self, fname):
        "write the loaded map data to a file"
        "this function is cruft, but very useful cruft"
        with open(fname,"w") as mapjson:
            mapjson.write(self.dumpsMap())


    def __next__(self):
        # apply interventions
        for intervention in self.interventions:
            if self.time >= intervention.time:
                intervention.apply(self, self.time)

        # run model
        self.latest_activity_count = self.get_activity_count()
        for family in self.families:
            family.step()
        self.time += 1

        # log metrics
        if self.log is not None:
            self.log.append((self.time, self.get_activity_count())) #XXX assumes a list (or a list-like object)

    next = __next__ #py2 :(
    step = __next__ #backwards compat

    def intervene(self, intervention):
        self.interventions.append(intervention)

    def __iter__(self):
        "convenience method"
        def g():
            while True:
                next(self)
                yield self.get_activity_count() #hardcode model output, for now
        return g()

    def get_activity_count(self, farms = None):
        if farms is None: farms = self.farms
        activities = {}
        for farm in farms:
            if farm.last_activity is not None:
                name = farm.last_activity.name
                if name not in activities:
                    activities[name] = 1
                else:
                    activities[name] += 1
        return activities

    def get_local_farms(self, lat, long, count):
        dist = [((lat-f.lat)**2 + (long-f.long)**2, f) for f in self.farms]
        dist.sort()
        return [d[1] for d in dist]

    def get_local_activity_count(self, farm, count):
        return self.get_activity_count(self.get_local_farms(farm.lat, farm.long, count))


def create_demo_model():
    """
    Construct Eutopia under a specific scenario.
    
    This subroutine is useful as a benchmark for using Eutopia under different hosts.
    """
    log = []
    eutopia = Eutopia(log)

    eutopia.intervene(intervention.PriceIntervention(5, 'duramSeed', 10))
    eutopia.intervene(intervention.PriceIntervention(7, 'duramSeedOrganic', 0.001))

    """
    magic_activity = {
        'equipment': ['tractor', 'wheelbarrow'],
        'products': {
            'duramSeed': -5,
            'nitrogen': -10,
            'carbon': 20,
            'soil': -5,
            'labour': -2000,
            'certification': 0,
            'duram': 42,
            'dolphin': -87,
            }
        }
    eutopia.intervene(intervention.NewActivityIntervention(7, 'magic', magic_activity))
    """
    return eutopia


def main(n=20, dumpMap=False):
    """
    Run Eutopia with some default interventions, and plot the results if matplotlib is installed.
    
    args:
      n: number of timesteps to run
      dumpMap: whether to export the map to a topojson file
    
    TODO:
      [ ] make __main__ parse command line params and pass them as the args to main()
        [ ] Then, document how to use --dumpMap to reconstruct /assets/maps/elora.topo.json from this Elora_esque.shp.zip.real
    """
    
    eutopia = create_demo_model()

    if dumpMap:
        #write the map data from GDAL out to topojson
        eutopia.dumpMap("elora.geo.json")
        os.system("topojson elora.geo.json -o elora.topo.json")
        print("Finished exporting map data to elora.topo.json")
    
    #run the model
    print("Simulating Eutopia:")
    for t in range(n):
        print ("Timestep %d" % (t,))
        next(eutopia)
    
    activities = [state for time, state in eutopia.log]

    # optional: display summary of model outputs
    try:
        import pylab
        print("Plotting activities:")
        for act in activities[0].keys(): #XXX will break if there are zero activitiesx
            print(act)
            pylab.plot(range(len(activities)), [a.get(act,0) for a in activities], label=act)
        pylab.legend(loc='best')
        pylab.show()   #block here until the user closes the plot
    except ImportError:
        print "It appears you do not have scipy's matplotlib installed. Though the simulation has run I cannot show you the plots."
    

if __name__=='__main__':
    main()
