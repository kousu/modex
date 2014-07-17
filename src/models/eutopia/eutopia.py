#!/usr/bin/env python2
# this program requires python2 because GDAL requires python2

# system libs
import os
from warnings import warn

import random
import json

# local libs
from simulationlog import *
import pygdal
from util import *

# eutopia files
import activity
import intervention

HERE = os.path.abspath(os.path.dirname(__file__))


#######################
## Model Overview
#
#
# state:
#  Farms - a set of georeferenced polygons containing (id, soil_type, county, activity, {some other stuff that we ignore})
#        MAP_CODE is the type of farming done on that land (which might not strictly be farming. Activities like lying fallow or running a jungle gym are possible) 
#  FarmFamily - an agent which has (bank_account, preferences)
#  
#  
# interventions:
# Upon the substrate of model state, Eutopia gives the user the power to
# manipulate things like tax rates, new farming (or otherwise) activities, new equipment
# We call these "interventions" and they operate by
# (TODO: this part hasn't actually been properly spec'd yet; there are arguments to have)
# 
# logged state:
#  - every Farm's (activity, soil status, ...?)
#  - every FarmFamily (bank account, farms <-- requires a separate table...)
#  - the current value of aggregate_measures (TODO: doing this cleanly requires cleaning up Activity first and dealing with the Normal()s in there which are in the wrong place)
#  - activity counts
#
# future plans:
# Farms will grow SOIL_TYPE
# A Farmer class, which is born into a FarmFamily, moves, perhaps inherits farms from their parents, dies
#
#
#
#
#######################

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

SOIL_TYPES = [ #more than you ever wanted to know at http://sis.agr.gc.ca/cansis/taxa/cssc3/index.html
    "CLAY",
    "PEAT",
    "LOAM",
    "CHRERNOZEM", #aka "black earth" e.g. Holland Landing
    "SAND"
]

class Farm: #(pygdal.Feature): #inheritence commented out until we determine if it's a good idea or not
                               #the trouble comes down to that I want to have a copy constructor:
                               # I want to say Farm(map.getsomefeature())
                               # but a native ogr.Feature needs to be ogr.Feature(ogr.FeatureDefn(...)) which is all sorts of pain
                               # For now, we clone only the given feature's geometry, which is all we are using at the moment
    def __init__(self, feature):
        #pygdal.Feature.__init__(self, feature)
        self._geometry = feature.GetGeometryRef() #instead of trying to muck with inheritence, just use get a pointer to the geometry and ignore the columns
        
        self.soil_type = random.choice(SOIL_TYPES) #TODO: pull from a real dataset
        self.county = "BestCountyInTheWorldIsMyCountyAndNotYours"
        self._activity = self._last_activity = None
        # you might use _last_activity to make_planting_decision?
        
        (self.long, self.lat) #..uh oh. for some reason we need to access (and memoize) these here, or else gdal segfaults
        #print(self.lat, self.long)
    
    def get_activity(self):
        return self._activity;
    
    def set_activity(self, value):
        self._last_activity, self._activity = self._activity, value;
    
    activity = property(get_activity, set_activity)
    
    @property
    @memoize
    def lat(self):
        return self._geometry.Centroid().GetY() 

    @property
    @memoize #fun fact: memoizing this function saves 700 times the calls
             #          --nearly 3 orders of magnitude-- as of this commit.
    def long(self):
        return self._geometry.Centroid().GetX()

    @property
    def area(self): return self._geometry.Area()
    
    def ExportToJSON(self):
        # hand-rolled export function
        # Until we figure out a consistent and efficient way to handle time-varying shapefiles,
        # this will do to only export the properties which are actual geofeature properties (as distinct from the helpers that make the model easier to write)
        
        return json.dumps(
        {"type": "Feature",
         "geometry": json.loads(self.geometry.ExportToJSON()), #XXX ridiculously inefficient
         "properties":
            {"activity": self.activity,
             "soil_type": self.soil_type,
             "county": self.county}})

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
            farm.activity = self.make_planting_decision(self.eutopia.activities.activities, farm)
            
            self.bank_balance += farm.activity.get_product('money', farm)


#notational hack
#TODO: move somewhere less foolish
sqlalchemy.JSON = sqlalchemy.String
JSON = String

class Eutopia:
    """
    The Eutopic World
    The main simulation class
    There is an API here for controlling and querying the model state
    """
    def _init_log(self, log):
        """
        helper to contan the verbose declarative schema statements
        TODO: hack it with metaclasses and some schema-walking magic
           so that the schema can be declared at the class level
           but the actual log (and runID) only made at runtime
           the Tables by themselves shouldn't need to know
         the trick would be maybe.. clone the Table objects and swap out
            the magic hidden columns with `default=` hooks.
            but let all SimulationTables share the other Columns
        Besides cleanliness, my motivation for this would be speed:
          actually creating the tables seems to be very very slow
          so if we can do it in a way that runs can share objects,
          that would be best
        """
        self.log = TimestepLog(log)
        
        # define the logging SQL schema 
        equipment = Table(self.log, "equipment", Column("id", Integer, primary_key=True), Column("name", String))
        TimestepTable(self.log, "farms",
                                Column("id", Integer, primary_key=True),
                                Column("activity", String), Column("county", String),
                                Column("geometry", JSON)) #TODO: don't store geojson to the database; this is massively inefficient; instead, figure out a way to use geodata natively but not too clumsily; perhaps instead of storing a geometry, store a feature ID and include in the model docs a pointer to the shapefile that contains the geometry. The indirection is painful, but geojson is sooooo inefficient; plus, I promised my mother I would never pack entire objects into a whole database column ever again.
        TimestepTable(self.log, "farmers",
                                Column("id", Integer, primary_key=True),
                                Column("bank_account", Float))
        TimestepTable(self.log, "farmers_equipment", #list of each farmer's farms (this could also be done by an 'owner' column on farms, but that doesn't ORM-map as cleanly)
                                Column("farmer_id", Integer, ForeignKey("farmers.id")),
                                Column("equipment_id", Integer, ForeignKey("equipment.id")))
        TimestepTable(self.log, "farmers_farms", #list of each farmer's farms (this could also be done by an 'owner' column on farms, but that doesn't ORM-map as cleanly)
                                Column("farmer_id", Integer, ForeignKey("farmers.id")),
                                Column("farm_id", Integer, ForeignKey("farms.id")))
        TimestepTable(self.log, "farmers_preferences",
                                Column("farmer_id", Integer, ForeignKey("farmers.id")),
                                Column("money", Float), #these are hardcoded for now; but they come from a list which might change and, in SQL, that means this table should be a dictionary ((farmer, id) => preference) with a separate table to store the names to go with the ids
                                Column("follow_local", Float),
                                Column("follow_society", Float))
        
        # tables for "bigger" state
        TimestepTable(self.log, "aggregate_measures",
                                *[Column(a, Float) for a in activity.aggregate_measures])
        
        # (version 1 is in some ways "ugly sql" but it makes very pretty tables and is actually easier to work with! )
        # activity_counts Version 1: flopping activities across columns
        # activity_counts Version 2: a more normalized sql form, where we essentially embed a dictionary into a table
        # ALSO this table can be derived from the farms table, e.g. by a
        # related but separate EutopiaStatistics program; whether to use 
        # only use one table, the other, or to write both is a question of
        # a) speed b) space c) what the model writer is interested in looking at
        # For now, we err on the side of logging too much than too little.
        TimestepTable(self.log, "activity_counts", #notice how this table has no per-step primary key:
                                                   #in this particular table, each timestep *has only one row*
                                *[Column(a, Integer) for a in activity.activities])
        
        self.log.create_tables()
        
        try:
            equipment.insert().values([{"id": id, "name": name} for name, id in activity.equipments.items()]).execute()
        except sqlalchemy.exc.IntegrityError as e:
            warn("We probably already inserted the static `equipment` table")
            warn(str(e))
            pass
        
    
    def __init__(self, log = None):
        """
        log: the SQLAlchemy connection string to log into;
        default is to log into a temporary memory database.
        """
        if not log: log = "sqlite://"
        self._init_log(log)
                                                
        #TODO: for categorical data, like 'county' and 'soil_type', switch to using an integer id to save space
        #  even better, support the (now built in!) enum type: https://pypi.python.org/pypi/enum34 in general, with a spare table to give the mapped strings and a proper Foreign Key constraint
        # (does SQLAlchemy already do this? it should!)
        # TODO: apply SQLAlchemy's ORM because this is getting out of hand.
        #  The trick will be to apply the ORM yet use TimestepTables.
        
        try:
            shapefile = pygdal.Shapefile(MAP_SHAPEFILE)
        except IOError:       #py2.7
            #except FileNotFoundError: #py3k
            raise RuntimeError("No shapefile `%s` found; you may need to download it from a team member (privately)" % MAP_SHAPEFILE)

        self.map = shapefile[0] #cheating: assume the only layer we care about is this one
        assert self.map.GetGeomType() == pygdal.wkbPolygon, "Farm boundaries layer is not a Polygon; it is a" + str.join(" or ", pygdal.invertOGRConstants(self.map.GetGeomType()))
        #assert isinstance(self.map, pygdal.PolygonLayer), "Farm boundaries layer is not a Polygon; it is a " + str(type(self.map))

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
        # BUG: order of agent execution matters here!
        # we should be using doublebuffering
        # and then handling the fiddly corner cases like what happens
        #  when two agents make incompatible decisions, like both buying the same farm, etc
        self.latest_activity_count = self.get_activity_count()
        for family in self.families:
            family.step()
        self.time += 1
        
        self._log() #TODO: should logging happen *before* we compute?
        
    def _log(self):
        # log system state
        # ...
        
        # log metrics
        self.log("activity_counts", **self.get_activity_count()) #XXX see version 1 vs version 2 below; this is version 1
        
        # finally, update the log's time
        self.log.step(self.time)
    
    @property
    def time(self):
        #TODO: handle null logging; in that case, use a backing ._time like a normal person
        return self.log.time
    
    next = __next__ #backwards compatibility with python2
    step = __next__ #backwards compat with ourselves

    def intervene(self, intervention):
        self.interventions.append(intervention)

    def __iter__(self):
        "convenience method"
        while True:
            next(self)
            yield self.get_activity_count() #hardcode model output, for now #XXX this is senseless now that we have a database in place

    def get_activity_count(self, farms = None):
        "return a dictionary containing the current value of each economic activitiy"
        if farms is None: farms = self.farms
        # this is a 'bucketize' operation
        activities = {}
        for farm in farms:
            if farm.activity is None: continue
            activities[farm.activity.name] = activities.get(farm.activity.name, 0) + 1
        return activities

    def get_local_farms(self, lat, long, count):
        "collect the 'count' closest farms to coordinates (lat, long)"
        dist = [((lat-f.lat)**2 + (long-f.long)**2, f) for f in self.farms]
        dist.sort()
        return [d[1] for d in dist][:count]

    def get_local_activity_count(self, farm, count):
        return self.get_activity_count(self.get_local_farms(farm.lat, farm.long, count))


def create_demo_model(db=None):
    """
    Construct Eutopia under a specific scenario.
    
    This subroutine is useful as a benchmark for using Eutopia under different hosts.
    """
    eutopia = Eutopia(db)
    
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


def main(n=50, db=None, plot=True):
    """
    Run Eutopia with some default interventions, and plot the results if matplotlib is installed.
    
    args:
      n: number of timesteps to run
      dumpMap: whether to export the map to a topojson file
    
    TODO:
      [ ] make __main__ parse command line params and pass them as the args to main()
        [ ] Then, document how to use --dumpMap to reconstruct /assets/maps/elora.topo.json from this Elora_esque.shp.zip.real
    """
    
    eutopia = create_demo_model(db)

    #if dumpMap:
    #    #write the map data from GDAL out to topojson
    #    # TODO: move this to the `scripts/` folder
    #    eutopia.dumpMap("elora.geo.json")
    #    os.system("topojson elora.geo.json -o elora.topo.json")
    #    print("Finished exporting map data to elora.topo.json")
    
    #run the model
    print("Simulating Eutopia:")
    for t in range(n):
        print ("Timestep %d" % (t,))
        next(eutopia)
    
    # display activity count results
    # as a side effect, order the results into individual timeseries by name
    print("Farm activities over time:")
    #print(list(eutopia.log['activities'].all()))
    #print(eutopia.log['activities'].columns)
    #import IPython; IPython.embed()
    # Now, reading the data is awkward because we use raw Python
    # If we installed Pandas (which is not unreasonable, given that we care about stats and dataset manipulation)
    # the cruft would get hidden (and probably run faster too, since Pandas has been tuned)
    timeseries = {}
    activity_counts = eutopia.log.activity_counts
    for act in activity.activities.keys():

        # (version 1 is in some ways "ugly sql" but it makes very pretty tables and is actually easier to work with! )
        # Version 1: flopping activities across columns
        timeseries[act] = [r[0] for r in select([activity_counts.c[act]])
                                         .order_by(activity_counts.c.time)
                                         .where(activity_counts.c.run_id == eutopia.log.run_id)
                                         .execute().fetchall()]
        print(act, ":", timeseries[act])
        
    if not plot: return
    
    # optional: display summary of model outputs
    # automatically kicks in if matplotlib is installed
    try:
        import pylab
        print("Plotting activities with matplotlib:")
        # split the table into individual columns
        # TODO: use pandas; in R I would say table[, act] and I want to do the same here.
        for act in timeseries:
            pylab.plot(range(len(timeseries[act])), timeseries[act], label=act)
            pylab.xlabel("time")
            pylab.ylabel("activity")
        
        pylab.legend(loc='best')
        pylab.show()   #block here until the user closes the plot    
    except ImportError:
        print "It appears you do not have scipy's matplotlib installed. Though the simulation has run I cannot show you the plots."
    except RuntimeError, e: #this crashes on the off chance you're not running X; not a big deal, but notable, so a less-scary warning to the user
        print e.message, "=> unable to show plots."
        
    print("good bye!")

if __name__=='__main__':
    db = None
    import sys
    if len(sys.argv) > 1:
        db = "sqlite:///" + sys.argv[1] #construct
    main(db=db)
