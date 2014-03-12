import map
import activity
import intervention

class Farm:
    def __init__(self, id, county, lat, long, area, land_type):
        self.id = id
        self.land_type = land_type
        self.county = county
        self.lat = lat
        self.long = long
        self.area = area
        
        self.last_activity = None
        

class Family:
    def __init__(self, eutopia):
        self.eutopia = eutopia
        self.farms = []
        self.bank_balance = 1000000.00
        self.equipment = []
        self.preferences = {'money': 1}
        
    def add_farm(self, farm):
        self.farms.append(farm)
        farm.family = self
        
    def make_planting_decision(self, activities, farm):    
        best = None
        for activity in activities:
            total = 0
            for pref, weight in self.preferences.items():
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
    def __init__(self):
        self.map = map.Map('guatemala.json')
        
        self.activities = activity.Activities()
        
        self.time = 0
        
        self.farms = []
        for farm_data in self.map.farms:
            farm = Farm(*farm_data)
            self.farms.append(farm)
            
        self.families = []
        for farm in self.farms:
            family = Family(self)
            family.add_farm(farm)
            self.families.append(family)
            
    def step(self):
        for family in self.families:
            family.step()
        self.time += 1
            
    def get_activity_count(self):
        activities = {}
        for farm in self.farms:
            if farm.last_activity is not None:
                name = farm.last_activity.name
                if name not in activities:
                    activities[name] = 1
                else:
                    activities[name] += 1
        return activities            
                
        
if __name__=='__main__':

    interventions = []
    eutopia = Eutopia()
    
    interventions.append(intervention.PriceIntervention(5, 'duramSeed', 10))

    interventions.append(intervention.PriceIntervention(7, 'duramSeedOrganic', 0.001))

    magic_activity = {
        'equipment': ['tractor', 'wheelbarrow'],
        'products': {
            'duramSeed': -5,
            'nitrogen': -10,
            'carbon': 20,
            'soil': -5,
            'labour': -2000,
            'certification': 0,
            'duram': 1000000,
            'dolphin': -87,
            }
        }
    interventions.append(intervention.NewActivityIntervention(7, 'magic', magic_activity))    
    
    
    time = 0
    def step():
        global time
        for intervention in interventions:
            if time >= intervention.time:
                intervention.apply(eutopia, time)
        time += 1
    
        eutopia.step()
    
    
    activities = []
    for i in range(10):
        step()
        activities.append(eutopia.get_activity_count())
        
    print activities    
    
    #import pylab
    #pylab.plot(range(10), [a.get('durumWheatConventional',0) for a in activities])
    #pylab.plot(range(10), [a.get('durumWheatGreen',0) for a in activities])
    #pylab.plot(range(10), [a.get('magic',0) for a in activities])
    #pylab.show()

        
