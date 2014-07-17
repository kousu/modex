# activity.py
"""
This defines the sorts of economic actions that a farmer can perform.
Currently, all activities are planting choices performed upon a particular Farm polygon.

Each activity has a name, requires some equipment, and has some costs and products.
Currently, costs are rolled in as negative products.
Some of the "products" are actually "measures": the effect of the activity on
some global variable, like GDP or carbon. (Activity.get_product handles the two cases).

TODO:
* [ ] think about fixed costs and non-linear (economies of scale)
* [ ] include equipment
* [ ] Write a Product class; make sure it's hashable so it can be used as dictionary keys.

"""

# the 1:1 mapping between equipment and equipment IDs 
# copied into the database at init
# pluralized to distinguish it from all the other variables named equipment
equipments = ['tractor', 'plow']  
equipments = dict((name, id) for id, name in enumerate(equipments)) #invert the mapping, so that we can log by looking up the ID
# TODO: maybe use https://pypi.python.org/pypi/bidict/0.1.1 ? 

# quick-hack hard-coding of the sorts of activities in the system
# TODO: rewrite more flexibly and cleaner. Perhaps with subclasses (ruby-style)?
activities = {
    'durumWheatConventional': {
        'equipment': ['tractor'],
        'products': {
            'duramSeed': -5,
            'nitrogen': -10,
            'carbon': 20,
            'soil': -5,
            'labour': -2000,
            'certification': 0,
            'duram': 40,
            'dolphin': -87,
            }
        },
    'durumWheatGreen': {
        'equipment': ['tractor', 'plow'],
        'products': {
            'duramSeedOrganic': -4,
            'nitrogen': 0,
            'carbon': 5,
            'soil': -2,
            'labour': -2500,
            'certification': -500,
            'duramOrganic': 40,
            'dolphin': -17,
            }
        },
    }    
   
import random   
class Normal:
    """
    A Gaussian probability distribution.
    This class is like the whole gaussian family
    An _instance_ on the other hand, is a particular distribution
    
    Normal(m, s) doesn't mean "generate a Normal(m, s)" it means
    "generate a callable which generates Normal(m, s)'s"

    TODO: CLEANUP: pull out to a util module or find the same in scipy
    """
    def __init__(self, mean, sd):
        self.mean = mean
        self.sd = sd
    
    def __call__(self):
        return random.gauss(self.mean, self.sd)
    value = __call__ #backwards compat
    
    def __add__(self, location):
        return Normal(self.mean + location, self.sd)            
    def __mul__(self, scale):
        return Normal(self.mean*scale, self.sd*scale)

    def __iadd__(self, location):
        self.mean += location
    def __imul__(self, scale):
        self.mean *= scale
        self.scale *= scale
        
def _Normal(a,b):
    """
    The above written as a closure
    problem: this does not support the operator overloading sugar.
    But the only difference is in where you need to bracket; if D = Normal(m,s) then either
    l + D()*s 
    vs
    (l + D*s)()
    More incisively though, the sugar allows you to alter the distribution over time,
    which might be useful. Hmmmm.
    """
    def c():
        return random.gauss(a,b)
    return c

# These correspond to are metrics that someone interested, in the human world,
# like an economist, ecologist, or politician, might measure
# The idea is that we can plot these as linegraphs over time,
# to see what happens as players tweak Interventions.
#
# The aggregate-ness of this is that the "money" metric is the sum of 
# the economic value of the products wheat seed, wheat, labour, certification, etc.,
# and the environment metric is the sum of the carbon and dolphin footprints.
# 
# The name of this dict is a bit misleading because the inner
# entries are *probability distributions*:
#  these represent the (randomized) weight that an agent gives to 
# randomized to represent that in valuing an Activity, each Farmer values
# different inputs differently, even compared to themselves.
# TODO: move this randomness into the Farmer class, because it's misleading here.
aggregate_measures = {
    'money': {
        'duramSeed': Normal(50,10),
        'duramSeedOrganic': Normal(100,10),
        'labour': Normal(5,1),
        'certification': Normal(1,0),
        'duram': Normal(300,10),
        'duramOrganic': Normal(350,10),
        },
    'environment': {
        'carbon': Normal(1,0),
        'dolphin': Normal(10,100),
        },
    }    

    
    
class Activity:
    def __init__(self, name, equipment, products, aggregate_measures):
        self.name = name
        self.equipment = equipment
        self.products = products
        self.aggregate_measures = aggregate_measures
    
    def get_product(self, product, farm):
        """
        the amount this Activity produces the given product when performed by the given farm.
        Product can either be something in the big list of possible products ('duramSeed', 'nitrogen', 'dolphin'), or
             it can be one of the aggregate measures (for clarity: currently only 'money' or 'environment')
        """
        if product in self.products:
            # i. if the product is an actual product, return its value
            total = self.products[product]
            
        elif product in self.aggregate_measures:
            # ii. but if the product is an aggregate measure,
            #     add up the effect of all products on all elements
            # __weighting the different products differently__

            #TODO: rewrite using set().intersect() for clarity
            # relevant_products = set(self.products.keys()).intersect(set(self.aggregate_measures[product].keys()))
            # or maybe just a list comprehension:
            # relevant_products = [r for r in self.products.keys() if r in self.aggregate_measures[product].keys()]
            #
            # then we can use sum() instead of a distracting loop
            # relevant_products = dict((r, self.aggregate_measures[r]) for r in relevant_products])) #map into product => weight
            # sum(weight()*self.products[r] for r, weight in relevant_products.items()) #compute a weighted sum: the amount this activity produces each product times a random amount that product is valued            
            total = 0
            for item, distribution in self.aggregate_measures[product].items():
                if item in self.products:
                    weight = distribution()
                    total += weight*self.products[item]
            return total
        else:
            raise KeyError('Could not find product "%s"' % (product,))

        # scaled by the given farm's size
        return total*farm.area

class Activities:
    def __init__(self):
        self.aggregates = dict(aggregate_measures)
        
        self.activities = []
        for name, data in activities.items():
            self.activities.append(Activity(name, aggregate_measures=self.aggregates, **data))
    
if __name__=='__main__':
    activities = Activities()
