# TODO: think about fixed costs and non-linear (economies of scale)
# TODO: include equipment
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
            'labour': -2000,
            'certification': -500,
            'duramOrganic': 40,
            'dolphin': -17,
            }
        },
    }    
   
import random   
class Normal:
    def __init__(self, mean, sd):
        self.mean = mean
        self.sd = sd
    def value(self):
        return random.gauss(self.mean, self.sd)
    def __mul__(self, scale):
        return Normal(self.mean*scale, self.sd*scale)
        
        

aggregate_measures = {
    'money': {
        'duramSeed': Normal(50,10),
        'duramSeedOrganic': Normal(55,10),
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
    
    def get_product(self, key, farm):
        if key in self.products.keys():
            return self.products[key]*farm.area
        elif key in self.aggregate_measures.keys():
            total = 0
            for item, distribution in self.aggregate_measures[key].items():
                if item in self.products.keys():
                    weight = distribution.value()
                    total += weight*self.products[item]*farm.area
            return total
    
        raise Exception('Could not find product "%s"'%key)

class Activities:
    def __init__(self):
        self.aggregates = dict(aggregate_measures)
        
        self.activities = []
        for name, data in activities.items():
            self.activities.append(Activity(name, aggregate_measures=self.aggregates, **data))
    
if __name__=='__main__':
    activities = Activities()
