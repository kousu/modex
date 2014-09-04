"""
second draft of model logging, using composition instead of inheritence, and dropping the dependency on dataset

the reason using dataset directly is awkward is because it is hard-coded to use its own Table.

the reason this is complicated because we have several cooperating classes kicking around: the database and the tables in that database.

TODO: use python's logging class instead of debugprints
TODO: maybe instead of passing the SimulationLog to each SimulationTable at init, we take a page from SQLAlchemy and use a .bind member
      reason being that it feels awkward, sometimes, to have to choose your run_id and set up your timesteps just to create logging table
TODO: create_tables() is an awkward bit of procedural mucking up my nice. Maybe we can do them on first log() call??
TODO: allow optional disabling of autocommit; postgres is reasonably fast at taking commits but sqlite is achingly slow. Presumably keeping things in memory and then flushing (say, once per 10 simulation steps) will be a lot easier.
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



def FKC_target_table(F):
    """
    extract the name of the target table of ForeignKeyConstraint F"
    
    I consider it a bug that despite the precondition that a
    ForeignKeyConstraint's targets all share a table, there is no way to
    extract that table and I'm not sure the FKC even enforces it.
    """
    r = {f._column_tokens[1] for f in F.elements}
    assert len(r) == 1, "All referenced columns should be on the same table" #--> which using a set comprehension will detect
    return r.pop()
        

class Table(sqlalchemy.Table):
    """
    override the default sqlalchemy Table to record a pointer to the parent SimulationLog
    """
    def __new__(cls, parent, name, *schema, **kwargs):
        
        self =  sqlalchemy.Table.__new__(cls, name, parent._metadata, *schema, **kwargs) #NB: cannot use super() here because we're in __new__ which is ~~magic~~ 
        self.parent = parent
        
        setattr(self.parent, name, self) #for convenience
        
        return self

class SimulationTable(Table):
    """
    a specialized
    """
    def __new__(cls, parent, name, *schema, **kwargs):
        """
        constructor
        
        parent is a SimulationLog object
        
        For some reason I haven't read enough of the code to grok yet,
        the superclass is written in terms of __new__,  instead of __init__
        and it does all sorts of shennigans in there that are hard to work around.
        So, we just pretend that __new__ *is* __init__, as much as possible
        """
        # prefix the table by "run_id". prefix becase, while SQL in theory
        # works on sets and is ignorant of order, but sqlalchemy isn't, and csv isn't.
        assert isinstance(parent, SimulationLog), "SimulationTable only works with SimulationLogs."
        
        if any(isinstance(c, Column) and c.name == "run_id" for c in schema):
            raise ValueError("run_id is a reserved column name in SimulationTables")
        
        schema = (Column("run_id", Integer, ForeignKey("runs.id"), primary_key=True, default=parent.run_id),) + schema #the default here is a constant and *private*
        # TODO: use ForeignKey(parent.runs_table.c.id) instead of a string, for stronger typing win
        
        # small bug: 
        #  - postgres (which has very strong integrity guarantees) balks where sqlite does not at our use of foreign keys
        #   namely: 
        
        # maybe the proper sol'n is to have an external runs table, and give *every* table a foreign key into that
        
        # CREATE TABLE runs (
        #   id INTEGER NOT NULL,
        #   description
        #   date DATETIME NOT NULL,
        #   PRIMARY KEY (id)
        # );
        
        # TimestepTable has the same bug, but we'd have to have a "timestep" table, which looks like
        # CREATE TABLE timesteps (
        #   run_id INTEGER NOT NULL,
        #   time INTEGER NOT NULL,
        #   PRIMARY KEY (run_id, time),
        #   FOREIGN KEY(run_id) REFERENCES runs (id)
        # );
        # ^ if I make run_id *not* part of the primary key, what can happen?
        #  then ... then time needs to be unique, which is wrong (we could only have one time sequence ever)
        #
        # how would I do this? TimestepTable creates a SimulationTable called timesteps like SimulationTable(parent, "timesteps", Column("run_id", Integer, ForeignKey("runs.id")))
        # but TimestepTable cannot do this within itself--or needs to be careful, anyhow--because there needs to be only one of these per db ( at the SQLAlchemy layer, one per MetaData instance)
        # 
        
        # SQLAlchemy draws a line in the sand at making compound ForeignKeys. Each ForeignKey points at one and only one column (but you can have multiple of them *so long as* (and here's the sticky thing for me) you have a column available to point them at...
        
        # if we don't have single master tables for runs and timesteps, but instead define the sets of runs and available timesteps implicitly, then it's ambiguous which table 
        
        # i learned all this by looking at SQLAlchemy's generated SQL via a sqlite .dump
        
        # recap: the crash happens when a foreign key is defined on a SimulationTable, because while you can define arbitrary sets of columns on a single table as PRIMARY KEY as 
        #        as soon as you add any FOREIGN KEY you must replicate the *entire* remote PRIMARY KEY
        
        # child tables need to reference runs
        #  but they *also* need
        # The foreign key rule of thumb: if ANY primary key is referenced in a foreign key, then ALL primary keys of that table must be
        
        # therefore SimulationTables must all foreign key their run_id columns to runs
        #     and   TimestepTables   must foreign key to both runs.id  and to (timesteps.run_id, timesteps.time)
        #   so the trick is that the run_id column on a timestep table needs to grow an extra ForeignKey
        # which is a bit tricky because it is SimulationTable.__new__ which actually creates the Column("run_id"[, ...]) object
        
        
        # darn darn darn
        #  I have managed to abuse (not cleanly: using a global to flag a second ForeignKey; but it generates the SQL I was expecting and I can probably reverse engineer whatever magic SQLAlchemy is doing differently)
        #  SQLAlchemy insists that every foreign key points at only one column
        #   however postgres--I guess because it processes schemas line by line--insists (indirectly) that if you have multiple foreign columns to a single table that they all be written together in a single FOREIGN KEY
        # compare the code below: the first gives 
        # > ERROR:  there is no unique constraint matching given keys for referenced table "timesteps"
        # the second works
        # however I can't figure out how cause SQLAlchemy to write this SQL.
        # ...the docs mentioned something about using ForeignKeyConstraint if you need more control...
        
        # dammit
        # I could write this up and ask postgres-general but it will probably take longer to do that than to just work around it
        
        #CREATE TABLE aggregate_measures (
        #	run_id INTEGER NOT NULL, 
        #	time INTEGER NOT NULL, 
        #	environment FLOAT, 
        #	money FLOAT, 
        #	PRIMARY KEY (run_id, time), 
        #	FOREIGN KEY(run_id) REFERENCES runs (id), 
        #	FOREIGN KEY(run_id) REFERENCES timesteps (run_id), 
        #	FOREIGN KEY(time) REFERENCES timesteps (time)
        #);

        #CREATE TABLE aggregate_measures (
        #	run_id INTEGER NOT NULL, 
        #	time INTEGER NOT NULL, 
        #	environment FLOAT, 
        #	money FLOAT, 
        #	PRIMARY KEY (run_id, time), 
        #	FOREIGN KEY(run_id) REFERENCES runs (id), 
        #	FOREIGN KEY(run_id, time) REFERENCES timesteps (run_id, time)
        #);
        
        # ^ the fix for this is to use ForeignKeyConstraint directly, which is what SQLAlchemy rewrites ForeignKey objects into anyway
        
        # so we have a bigger problem:
        # if a *client* uses ForeignKey OR a ForeignKeyConstraint AND its target table is a SimulationTable
        # THEN we are obligated to write a new a ForeignKeyConstraint which adds run_id before we call up to Table.__new__()
        # and if the table is a TimestepTable then we have to add run_id and time
        # Quickfix: since SQLAlchemy rewrites ForeignKeys into ForeignKeyConstraints *anyway*, we might as well do the same. Scan the schema for foreignkeys and foreignkeyconstraints, group by target table, and write new ForeignKeyConstraints -- with the addition that IF the target is a SimulationTable we add "run_id" -> "${target}.run_id"
        
        
        table = Table.__new__(cls, parent, name, *schema, **kwargs)
        
        # Any foreign key to a SimulationTable needs to have run_id appended to it,
        #  since the full remote primary key 
        # (and postgres, at least, enforces this, though sqlite does not)
        # There's a small rub:
        #  this code can only catch such situations when it is called from a source SimulationTable
        #  
        # ASSUMPTION: the only time a foreign of a time column comes from being a TimestepTable (though, in principle, python lets you root around in the guts so that this isn't true)
        #  therefore it is ILLEGAL to run a simulation and then after the fact make a construct like this:# ^XXX: this was written for TimestepTable; you should fix it up
        # CREATE TABLE sim_analysis ( run_id int, average_score float, farmer_id int, FOREIGN KEY (farmer_id) references farmers(id));
        # if you need to be able to make constructs like this, you need to do database normalization: split the time step parts of the farmers table like so:
        # SimulationTable("farmers", ...)
        # TimestepTable("farmer_histories", ...)
        # then you can FK to farmers without having to FK to the time column as well.
        # "time" -> "${target}.time"
        # and if you're a TimestepTable then you are obligated to do this or that
        
        
        # The least invasive way I've found to do this is to, *after* construction,
        # to rewrite any of the table's foreign key constraints--that
        # are computed from but stored separately from the columns' FKs--
        # which refer to a SimulationTable.
        
        # SQLAlchemy indeed has much alchemy behind it--a lot of moving parts
        # tumble over whenever you do almost anything. Even just 
        # So, this solution is not as elegant and probably not even as stable as it could be.
        # I need advice from the SQLAlchemy people on how to do this properly.
        # I'll clean up the code, post it to github, and ask. But for now, hacks.
        
        # BUG: ForeignKey.target_fullname yet the SQLAlchemy docs call this the "referenced" column 
        # BUG: printing a ForeignKeyConstraint only shows the source columns and the table!
        
        def update_FK(F):
            "append extra columns"
            # I dislike how explicit this is, but there doesn't seem to be any safe way
            # to mutate most SQLAlchemy objects without nasty unforeseen side-effects down the line
            # and there doesn't seem to be "clone but change this" methods akin to how immutable strings work
            # maybe we could do **F.__dict__
            target = FKC_target_table(F)
            new_columns = F.columns #[f.name for f in F.columns]
            new_columns.insert(1, "run_id")
            new_targets = [f.target_fullname for f in F.elements]
            new_targets.insert(11, target + "." + "run_id")
            
            # XXX this is flakkkkey
            #  it is important that the 0th element of columns is actually a Column
            # because of this line in sql.compiler:
            #          remote_table = list(constraint._elements.values())[0].column.table
            # I don't know why that happens here
            # since ForeignKey(["a","c"], ["t.d","t.e"]) is supposed to be a totally supported use
            
            return ForeignKeyConstraint(new_columns, new_targets,
                                        name=F.name, onupdate=F.onupdate, ondelete=F.ondelete,
                                        deferrable=F.deferrable, initially=F.initially, use_alter=F.use_alter,
                                        link_to_name = F.link_to_name, match=F.match,
                                        **dict(F.dialect_kwargs)
                                        )
                                    # TODO: there's a confusing crash here  which occurs if you create the tables in the wrong order, so that parent.tables cannot look up the foreign key table
                                    # SQLAlchemy goes to great lengths to give helpful exceptions and we should do that same

        
        table.constraints = {(update_FK(F) if
                 (isinstance(F, ForeignKeyConstraint) and isinstance(parent._metadata.tables[FKC_target_table(F)], SimulationTable)) 
                               else
                       F)
                   for F in set(table.constraints)} #the extra set() is to clone table.constraints, since simply constructing ForeignKeyConstraint and telling it what table its attached to (which you do when you give it columns as Column objects instead of as strings) makes it edit table.contraints and crash the set comprehension
        return table
        
        
class TimestepTable(SimulationTable):
    def __new__(cls, parent, name, *schema, **kwargs):
        # prefix the table by 'time'; note that the time is pulled, via closure, from the parent ModelLog object
        #assert isinstance(parent, TimestepLog), "TimestepTable only works with TimestepLogs."
        assert hasattr(parent, 'time'), "TimestepTable only works with TimestepLogs." #looser, duck-typed precondition
        
        if any(isinstance(c, Column) and c.name == "time" for c in schema):
            raise ValueError("time is a reserved column name in TimestepTables")
        
        schema = (Column("time", Integer, ForeignKey("timesteps.time"), primary_key=True, default=lambda: parent.time),
                 ) + schema
                 
        
        table = SimulationTable.__new__(cls, parent, name, *schema, **kwargs)
        
        #XXX copy-pasted from SimulationTable.__new__
        # TODO: figure out how to factor
        
        def update_FK(F):
            "append extra columns"
            # I dislike how explicit this is, but there doesn't seem to be any safe way
            # to mutate most SQLAlchemy objects without nasty unforeseen side-effects down the line
            # and there doesn't seem to be "clone but change this" methods akin to how immutable strings work
            # maybe we could do **F.__dict__
            target = FKC_target_table(F)
            new_columns = F.columns #[f.name for f in F.columns]
            new_columns.insert(1, "time") #<-- 1! not 0! so that it comes *after* run_id which we assume was added by SimulationTable.__new__
            new_targets = [f.target_fullname for f in F.elements]
            new_targets.insert(1, target + "." + "time")
            return ForeignKeyConstraint(new_columns, new_targets,
                                        name=F.name, onupdate=F.onupdate, ondelete=F.ondelete,
                                        deferrable=F.deferrable, initially=F.initially, use_alter=F.use_alter,
                                        link_to_name = F.link_to_name, match=F.match,
                                        **dict(F.dialect_kwargs)
                                        )
                                    # TODO: there's a confusing crash here  which occurs if you create the tables in the wrong order, so that parent.tables cannot look up the foreign key table
                                    # SQLAlchemy goes to great lengths to give helpful exceptions and we should do that same

        
        table.constraints = {(update_FK(F) if
                 (isinstance(F, ForeignKeyConstraint) and isinstance(parent._metadata.tables[FKC_target_table(F)], TimestepTable)) 
                               else
                       F)
                   for F in set(table.constraints)} #the extra set() is to clone table.constraints, since simply constructing ForeignKeyConstraint and telling it what table its attached to (which you do when you give it columns as Column objects instead of as strings) makes it edit table.contraints and crash the set comprehension
        
        
        
        from sqlalchemy.sql.ddl import CreateTable
        #print(CreateTable(table))
        if name == 'farmers_equipment':
        #    import IPython; IPython.embed()
            pass
        return table
         
        

class SimulationLog(object):
    """
    A logger for recording data from simulations to SQL.
    Essentially, this encapsulates the view of one complete database that one run of a simulation has.
    This framework code provides that, for one simulation run, and handles constructing a random run ID.
    
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
    def __init__(self, connection_string = None):
        if connection_string is None: connection_string = "sqlite://"
        self.database = sqlalchemy.create_engine(connection_string)
        self._metadata = sqlalchemy.MetaData() #create a new metadata for each , so that the column default trick is isolated per-
        self._metadata.bind = self.database
        
        self.run_id = uuid.uuid4().int & 0x7FFFFFFF #generate a new unique id, then clip it to 31bits because SQL can't handle bigints (interestingly, sqlite's int type can handle 32 bit (ie unsigned int), but postgres's cannot
        # we could store run_id as text but I feel like premature optimization is the name of the day here
        
        # construct a central table that SimulationTables can ForeignKey their id columns to.
        self.runs_table = Table(self, "runs", Column("id", sqlalchemy.Integer, primary_key=True))
    
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
        
        # at init time, we also need to actually install the run id that all SimulationTables will be ForeignKeying to.
        self.database.execute(self.runs_table.insert({"id": self.run_id}))

class TimestepLog(SimulationLog):
    "a SimulationLog which adds a time field to all logged data by the cooperation of TimestepTables"
    def __init__(self, *args, **kwargs):
        super(TimestepLog, self).__init__(*args, **kwargs)
        
        # construct a central table that TimestepTables can ForeignKey their time columns to.
        self.timesteps_table = SimulationTable(self, "timesteps", Column("time", sqlalchemy.Integer, primary_key = True))
        
        self.time = 0
        
        # hmm. it violates the FKs to
        # but it is 
        
    def step(self, value=None):
    
        if value is not None:
            self.time = value
        else:
            self.time += 1
            
        # record the existence of the new timestep
        self.database.execute(self.timesteps_table.insert({"time": self.time}))
    
    def create_tables(self):
        """
        extend create_tables so that the initial timestep gets recorded 
        because it violates the autogenerated FKs to log data before the
        timestep table has recorded that that timestep exists.
        """
        super(TimestepLog, self).create_tables()
        self.database.execute(self.timesteps_table.insert({"time": self.time}))
        


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
