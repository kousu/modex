import json
import numpy as np
import shelve
import sys
from os.path import dirname, abspath, join as pathjoin
PROJECT_ROOT = dirname(dirname(dirname(dirname(abspath(__file__)))))

def point_in_poly(x, y, poly):
    n = len(poly)
    inside = False

    p1x,p1y = poly[0]
    for i in range(n+1):
        p2x,p2y = poly[i % n]
        if y > min(p1y,p2y):
            if y <= max(p1y,p2y):
                if x <= max(p1x,p2x):
                    if p1y != p2y:
                        xints = (y-p1y)*(p2x-p1x)/(p2y-p1y)+p1x
                    if p1x == p2x or x <= xints:
                        inside = not inside
        p1x,p1y = p2x,p2y

    return inside
    
def find_area(array):
    a = 0
    ox,oy = array[0]
    for x,y in array[1:]:
        a += (x*oy-y*ox)
        ox,oy = x,y
    return a/2    


class Map:
    def __init__(self, filename):
        db = shelve.open('mapcache')
        if filename not in db:
            #this is being changed to an absolute filename for compability with the server
            filename = pathjoin(PROJECT_ROOT, "models", "eutopia", filename)
            
            self.topology = json.load(open(filename)) # changed from filename to abs_map_location
            self.arcs = self.topology['arcs']
            self.farms = self.get_farm_list()
            db[filename] = self
        else:
            map = db[filename]
            self.topology = map.topology
            self.arcs = map.arcs
            self.farms = map.farms
        db.close()
            
    def get_points(self, arcs):
        points = []
        
        if not isinstance(arcs[0][0], int):
            arcs = arcs[0]
        for arc in arcs:
            for a in arc:
                if a<0:
                    a = ~a
                if a>=len(self.arcs):
                    print a, len(self.arcs)
                    continue
                pts = self.arcs[a]
                x, y = pts[0]
                #points.append(pts[0])
                for p in pts:
                    dx, dy = p
                    x += dx
                    y += dy
                    points.append([x,y]) 
                    
        return np.array(points, dtype='float') 
    
    def transform_to_lat_long(self, x, y):
        scale = self.topology['transform']['scale']
        translate = self.topology['transform']['translate']
        
        tx = x * scale[0]+translate[0]
        ty = y * scale[1]+translate[1]
        
        return tx, ty

    def get_counties(self):
        counties = {}
        
        g1 = self.topology['objects']['guatemala1']['geometries']
        for i, g in enumerate(g1):
            outline = self.get_points(g['arcs'])
            counties[i] = outline
        return counties
        
    def get_farm_list(self):
        counties = self.get_counties()
        
        farms = []
        
        g2 = self.topology['objects']['guatemala2']['geometries']
        for i, g in enumerate(g2):
            farm_outline = self.get_points(g['arcs'])
            
            # find centre
            middle = np.average(farm_outline, axis=0)
            
            for key, c_outline in counties.items():
                if point_in_poly(middle[0], middle[1], c_outline):
                    county = key
                    break
            else:
                print 'could not find county for farm',i
                continue
                
            # TODO: fill in data in a non-random way    
            rng = np.random.RandomState(seed=i)
            type = rng.choice(['dry', 'soggy', 'flooded', 'cold', 'rocky'])
            
            # TODO: why is area negative?
            area = abs(find_area(farm_outline))
            tx, ty = self.transform_to_lat_long(*middle)    
            farm = (i, county, tx, ty, area, type)    
            farms.append(farm)
        return farms    
        
    
        
if __name__=='__main__':    
    map = Map('guatemala.json')
    
    for farm in map.farms:
        print farm
        
    """    
    #print map.topology['transform']['scale']
    
    g0 = map.topology['objects']['guatemala0']['geometries'][0]
    main_map = map.get_points(g0['arcs'])
    
    g1 = map.topology['objects']['guatemala1']['geometries']
    
    middles = []
    for g in g1:
        outline = map.get_points(g['arcs'])
        middle = np.average(outline, axis=0)
        middles.append(middle)
        print point_in_poly(middle[0], middle[1], main_map)
    middles = np.array(middles)
    
    import pylab
    pylab.plot(main_map[:,0], main_map[:,1])
    pylab.plot(middles[:,0], middles[:,1], linewidth=0, markersize=10, marker='o', markerfacecolor='k')
    
    pylab.show()
    """
