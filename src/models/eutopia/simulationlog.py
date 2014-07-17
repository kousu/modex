"""
second draft of model logging, using composition instead of inheritence, and dropping the dependency on dataset

the reason using dataset directly is awkward is because it is hard-coded to use its own Table.

the reason this is complicated because we have several cooperating classes kicking around: the database and the tables in that database.

TODO: use python's logging class instead of debugprints
"""


import functools
def funcdebug(f):
    # decorator which traces function calls
    def ff(*args, **kwargs):
        print("DEBUG: %s(%s, %s)" % (f.__name__, args, kwargs))
        return f(*args, **kwargs)
    ff = functools.wraps(f)(ff)
    return ff


#import dataset
#import dataset.persistence.database

import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import *

import uuid #use uuids instead of autoincrementing ids; this requires more storage, but has the advantage that our runs are absolutely uniquely identifiable.

class Table(sqlalchemy.Table):
    """
    override the default sqlalchemy Table to change the constructor
    to be consistent with the other Tables
    """
    def __new__(cls, parent, name, *schema, **kwargs):
        
        self =  sqlalchemy.Table.__new__(cls, name, parent._metadata, *schema, **kwargs) #NB: cannot use super() here because we're in __new__ which is ~~magic~~ 
        self.parent = parent
        
        setattr(self.parent, name, self) #for convenience
        
        return self

class SimulationTable(Table):
    def __new__(cls, parent, name, *schema, **kwargs):
        """
        For some reason I haven't read enough of the code to grok yet,
        the superclass is written in terms of __new__,  instead of __init__
        and it does all sorts of shennigans in there that are hard to work around.
        So, we just pretend that __new__ *is* __init__, as much as possible
        """
        # prefix the table by "run_id". prefix becase, while SQL in theory
        # works on sets and is ignorant of order, but sqlalchemy isn't, and csv isn't.
        assert isinstance(parent, SimulationLog), "SimulationTable only works with SimulationLogs."
        schema = (Column("run_id", Integer, primary_key=True, default=parent.run_id),) + schema #the default here is a constant and *private*
        
        return Table.__new__(cls, parent, name, *schema, **kwargs)
        

class TimestepTable(SimulationTable):
    def __new__(cls, parent, name, *schema, **kwargs):
        # prefix the table by 'time'; note that the time is pulled, via closure, from the parent ModelLog object
        #assert isinstance(parent, TimestepLog), "TimestepTable only works with TimestepLogs."
        assert hasattr(parent, 'time'), "TimestepTable only works with TimestepLogs." #looser, duck-typed precondition
        schema = (Column("time", Integer, primary_key=True, default=lambda: parent.time),) + schema
        return SimulationTable.__new__(cls, parent, name, *schema, **kwargs)

class SimulationLog(object):
    """
    A coordinator class for managing logs from simulations.
    Each instance is for one simulation run, and handles constructing a random run ID.
    
    usage: construct this and then construct a series of ModelTables (or their subclasses!)
      in cooperation with this class      giving this as their first argument
      This is just like making a regular set of sqlalchemy.Tables
      with the caveat that, since ModelTable defines a primary key (at least one, more if you use the subclasses)
      you must also define and use at least one primary key if you want to log more than one piece of data per run
      
      a SimulationLog may share tables with preexisting databases, other sqlalchemy.Engines,
      or even other SimulationLogs.
      the speciality is that each simulation log represents a unique run,
      and rows inserted under it will have their run_id automatically set to the run_id of that SimulationLog
      
     the idea is that you make new TimestepTables to log *new* data
     so the use cases are optimized for that
     
    like so: ...
    tables involved in your database need not necessarily be timestep tables: it is alright to do ._metadata.reflect()
    
    as a convenience, tables are attached as member variables under their name (so your tables need to be named according to python naming rules, which luckily largely overlap with sql naming rules)
    but the proper way to access them is log[name]
     
     TODO: support minimal querying, for completeness
     e.g. a .read() method which does a full select()
     its awkward that the only way to get things out is to grab the internal member .database
     
     TODO: support syntactic sugar for generating tables; something like log['newtablename'](Column(), Column(), ...)
    """
    def __init__(self, connection_string):
        self.database = sqlalchemy.create_engine(connection_string)
        self._metadata = sqlalchemy.MetaData() #create a new metadata for each , so that the column default trick is isolated per-
        self._metadata.bind = self.database
        
        self.run_id = uuid.uuid4().int & 0xFFFFFFFF #generate a new unique id, then clip it to 32bits because SQL can't handle bigints
        
    
    def keys(self):
        return self._metadata.tables.keys()
    
    def __getitem__(self, name):
        # rely on the fact that any table adds *itself* to the metadata object
        if name not in self._metadata.tables:
            raise KeyError("Unknown SimulationLog table '%s'" % (name,))
        return self._metadata.tables[name]
    
    def __call__(self, table, **row):
        "syntactic sugar for logging into one of the tables" 
        #TODO: support insert many: if row is a single
        #print("log(%s, %s)" % (table, row)) #DEBUG
        self.database.execute(self[table].insert(row))
    
    def create_tables(self):
        """
        create tables if they are missing from the db
        
        XXX: if tables with the same name exist in the db but have a different schema, bad things will happen
        """
        with self.database.begin(): #use a transaction, so that this really is either create_all or create_none
            self._metadata.create_all(self.database, checkfirst=True) #TODO: look into if SQLAlchemy already uses a transaction

class TimestepLog(SimulationLog):
    "a SimulationLog which adds a time column; cooperates with TimestepTable"
    def __init__(self, *args, **kwargs):
        self.time = 0
        super(TimestepLog, self).__init__(*args, **kwargs)
        
    def step(self, value=None):
        if value is not None:
            self.time = value
        else:
            self.time += 1
    


if __name__ == '__main__':
    # tests!
    import random
    log = TimestepLog("sqlite://")
    TimestepTable(log, "myawesometable", Column("farmer", sqlalchemy.String, primary_key=True), Column("riches", sqlalchemy.Integer))
    # hmmmmm
    # should we maybe *first* read the schema (MetaData.reflect()) 
    # and then if a user makes a TimestepTable, allow it but only if its schema matches what is in the db?
    # this seems.. hard.
    # maybe sqlalchemy defined .__eq__ on schema elements...
    for t in range(20):
        print("Timestep", t)
        for farmer in ["frank", "alysha", "barack"]:
            log('myawesometable', farmer=farmer, riches=random.randint(0, 222))
        #inform the logger that we are going to a new timestep
        log.step()
        
    print(log.myawesometable.columns.keys())
    for row in log.database.execute(log.myawesometable.select()):
        print(row)

"""
the sqlalchemy way (let's try to clone that as much as possible)
would be
schema = MetaData()
farmers = Table("farmers", schema, Column("run_id",  primary_key=True), Column("time", primary_key=True), Column("id", INTEGER, primary_key=True), Column("Column("bankaccount", Integer)
farmer_farms = Table("farmer_farms", schema, Column("run_id",  primary_key=True), Column("time", primary_key=True), Column("farm_id"), Column("farmer_id"))

"""
