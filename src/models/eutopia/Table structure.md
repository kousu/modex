# Eutopia Table structure

Logging model state seems like a natural problem for an ORM to solve, and SQLAlchemy provides an ORM, ... however
an ORM does not record time or runIDs (instead it just overwrites what was there previously).
This is rough work designing the table structure I would "naturally" expect, based on what is in the model,
and that I will use manually, for a first draft,
and that will guide attempts to get SQLAlchemy or Django to munge our objects into a DB more cleverly.
Actually, SQLAlchemy seems like it is extraordinarily flexible, and the appropriate amount of metaclass magic will probably get us there.
( .. PyABM must have facilities for this ? )

**run**
```
runID | parameter1 | parameter2 | .... |
```

**Farm**: the GIS table of farm features
```
runID | time | id | owner | land_type | activity | geometry
```
id is the GIS Feature ID (it is standard to call it FID)
ownder is the id of the owning FarmFamily.s
activity is the current (I do not like this name; it is threaded throughout eutopia, and I would like to change it).
  -->> the eutopia code uses last_activity, but I would rather stored the current activity and make last_activity a property which queries the database
  
geometry is the actual polygon/line/point data, which is either serialized to geojson (ugly!) or is stored in a non-column by a special purpose GIS database (Shapefile, Postgis).
a constraint: each farm polygon can only have one owner
 ---> the eutopia code is the other way around: FarmFamily contains a pointer to its owned Farms; maybe that's just an artificat of mapping objects to relations... how do standard ORMs do this?
((  so we could also do a "**Farm_Owners**" table with
```
runID | time | FarmFamily_id | Farm_id
```
pro: this is the way that an ORM would do it, probably, which future-proofs the design.
con: because of the single-owner constraint the owner can be stored in the farm.
))

**FarmFamily**
```
runID | time | id | bankAccount | 
```
The farmers have "equipment" and "preferences" as well, but I'm not going to write those in just yet; I think they require yet more subsidiary tables which will just be a distraction at this point.

**activity** contains the log of how many farms
TODO: this is *not* model state; it is already an aggregation of lower level data, with the aggregation being done.
```
runID | time | id
```

**activities** are the set of farming operations that a farmer can choose to engage in. Each has some inputs and utpouts ((see `activity.py` for more))
id | name
1  | durumWheatConventional
2  | durumWheatGreen


**equipment** contains the farm equipment available for use by the farmers 
```
id | name
1  | tractor
2  | plow
```

**products** contains the possible outputs of each 'activity'
```
id | name          | description
1  | duramSeed     | duram wheat seed
2  | nitrogen      | 
3  | carbon        |
4  | soil          |
5  | labour        |
6  | certification |
7  | duram         | duram wheat, grown conventionally
8  | duramOrganic  | duram wheat, grown organically (and labelled as such)
9  | dolphin       | dolphins in the ocean
```

**activities_equipment** lists what farm equipment each activity needs.
```
activity_id | equipment_id
1  | 1
2  | 1 
2  | 2
```

**activities_products** contains the units of products made by doing one timestep of each activity. Negative values are _costs_.
```
activity_id | product_id | amount; primary key (activity_id, product_id)
1           |     1      |  -5 
1           |     2      |  -10
1           |     3      |  20
1           |     4      |  -5 
1           |     5      |  -2000 
1           |     6      |  0
1           |     7      |  40 
1           |     8      |  0
1           |     9      |  -87
2           |     1      |  -4
2           |     2      |  0
2           |     3      |  5
2           |     4      |  -2 
2           |     5      |  -2500 
2           |     6      |  -500
2           |     7      |  0 
2           |     8      |  40
2           |     9      |  -17
```
(nb: the 0 valued rows should deleted in this table)


(missing data is a likely occurence here; these are only samples from a larger space that we will never see all of)


(nb: this table could be reduced in size by splitting it so that each value in the "activity" column becomes a new table ("activity_watering", "activity_corn"); 
using SQLAlchemy it is even possible to do that dynamically and using dataset (on top) it is not even difficult to do. Whether )

Kirsten is right: handling of time is a difficult special case that we should pay attention to.
The ideal would be for model writers to just write "log these data" and then we internally append (runID, time) track time...
Like, the user defines one schema and then it is automatically augmented to be a larger schema.
(nb: the (runID, time) tuples could be compressed away by adding a layer of indirection: another table, 
logpoints
```
runID | time | logid
```
and then making the other tables start with 'logid' (instead of 'runid | time') (and with logid being a UUID for good measure)

(runID, time) is always one of the primary keys!


Implementation Plan
--------------------

first draft: just has the 'activities' table record the activities, same as the non-databased version


Questions
---------------

Something not directly related; some of my instincts tell me that scientific analysis prefers to work with totally denormalized data.
I could write a REST API to automatically denormalize the entire database (perhaps, first query, then denormalize)... but the number of downsides to this is huge. We need to see more use cases to clear this up.