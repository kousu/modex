#!/usr/bin/python2
# the main server code
# make sure to read src/backend/README.md if this confuses you.

import IPython #DEBUG
import traceback #DEBUG

import sys, os, warnings
from os.path import dirname, abspath, join as pathjoin

import json
import tempfile

from twisted.internet import reactor
from twisted.internet import task
from twisted.web import *
from twisted.web.resource import *
import twisted.web.http as http #TODO: fix the import namings

from twisted.python import log
from twisted.web.server import Site
from twisted.web.static import File
from twisted.internet.defer import inlineCallbacks

from autobahn.twisted.websocket import WebSocketServerFactory, WebSocketServerProtocol
from autobahn.twisted.resource import WebSocketResource

import dataset
import sqlalchemy     # so that we can catch and wrap
import sqlalchemy.exc # SQL errors into HTTP errors.

#'working directory': not the system working directory, but the directory this program is in (so that we can be run from anywhere and find the correct assets/ folder et al.)
PROJECT_ROOT = dirname(dirname(dirname(abspath(__file__)))) #currently, the project root is two levels up from the directory the server is
SQLITE_DB = os.path.join(PROJECT_ROOT, "eutopia.sqlite")


#########################
## Models

# though our demo model (eutopia) is run via python import
# in the Real System(TM) models will be dynamically
# loaded somehow, so pretend you don't see this line:

import eutopia   #this is actually sitting in ../models/, but is symlinked into ./
                 #to avoid having to write sketchy __import__ calls. In the long run
                 #simulation.py should be abstracting--somehow--over the models
                 #(with magical __import__s or wrapping SQL or otherwise), so
                 #this server won't be touching them directly at all.

# NB:
# we are trying to set up a producer-consumer system, and twisted has this built in:
# https://twistedmatrix.com/documents/12.2.0/core/howto/producers.html
# ah, simpler: reactor.callLater
# useful: <https://twistedmatrix.com/documents/current/core/howto/time.html>

######################
## Twisted Components


# Brute force SQL dumping
# There's four classes that should be here:
#  [1               /databases whose getChild() returns instances of
#  [2 the class for /databases/db, whose getChild() returns an instance of the
#  [3     class for /databases/db/table, whose render exports entire tables, and whose getChild() returns instances of the
#  [4     class for /databases/db/table/col1,col2,col3?  whose render exports slices of tables
# Right now SqlDumperResource == [2] and SqlDumperTableResource == [3]
# Instead of [4], we could make [3] take optional HTTP query arguments.

class SqlDumperResource(Resource): #XXX name
    # an HTTP resource which interprets requests given to it as requests
    # to dump entire database tables to csv
    # BE CAREFUL WITH THIS; it has a very good chance of exposing private data
    # This is just a kludge until our jsDataset is functional.
    # TODO: move the connect() call inside of this class?
    #       It is pretty standard in deployments (take any PHP site ever)
    #       that you can just reaccess a resource to get the backend db connect to try again;
    #       but we want the connection to pool, if possible..
    #  SQLAlchemy pools by default < http://docs.sqlalchemy.org/en/latest/core/pooling.html >, but does dataset?
    #    I want to precreate an engine but not precreate connections...
    #   
    def __init__(self, db):
        # db should be a dataset.Database, e.g. as constructed by dataset.connect()
        self.db = db
        Resource.__init__(self)
    
    def getChild(self, path, request):
        # what is the 'path' we get??hi)
        # is request.path == path??
        table = path
        
        try:
            self.db.metadata.reflect(self.db.engine, extend_existing=True)  #kick dataset into reconnecting and updating its cached schema;
                                                      #awkwardness reported as a bug at https://github.com/pudo/dataset/issues/88
            
            if unicode(table) in self.db.tables: #XXX the cast here is dodgy;
                                                 #does dataset guarantee names in unicode?
                                                 # This line needs to change when we port to python3.
                return SqlDumperTableResource(self.db[table])
        except sqlalchemy.exc.OperationalError as e:
            # Why does twisted.web.error.Error exist if ErrorPage exists
            #  and yet "raise twisted.web.error.Error()" doesn't render one? 
            # XXX under the mysql driver, the interesting error message is e.orig.args[1]; but we cannot rely on that always being the case
            # so we use e.message which includes the exception's class and whatever other args were passed to the exception
            return ErrorPage(http.SERVICE_UNAVAILABLE, "Unable to connect to database.", e.message) # XXX is including e.message here a security leak?
            
        #return Resource.getChild(self, path, request) #this causes a 404
        return NoResource("Database table '%s' not found." % (table,)) #or would we rather be explicit about giving the 404 ourselves?

#TODO: write another class (or maybe some special cases inside of the below)
#      such that we can request a subset of columns like /tables/analysisresults/runID,minHappiness

def dataset_freezes(result, **kw):
    # serialize a dataset result set to a string.
    # 
    # dataset has serialization built in which is very convenient,
    # however, it insists on having a real filesystem file to dump to;
    # We can deal with the file issue using tmpfiles, but it's redundant CPU cycles that could be better spent.
    # bug reported at https://github.com/pudo/dataset/issues/79
    #
    # TODO: submit this as a patch to dataset
    # actually dataset.freeze has other problems:
    #   - it takes "indent" as a keyword arg, but that is only relevant to the json serializer
    #   - Decimals are printed in full 16-decidigit glory, even when they don't need to be (e.g. "1.0000000000");
    #     an artifact of the SQL db underneath, which it would be worthwhile to excise at the serializer.
    
    assert "filename" not in kw, "freezes() returns strings, not files"
    assert "prefix" not in kw, "freezes() returns strings, not files"
    
    with tempfile.NamedTemporaryFile() as dump:
        dataset.freeze(result, filename=dump.name,
                       prefix=dirname(dump.name)   #dataset.freeze demands we also tell it what folder we're exporting to
                                                   #probably because of the templating shennanigans that freeze() supports;
                                                   # awkward... perhaps we do not want to use freeze(), but rather its subroutines.
                       , **kw
                       )
        
        # if we made it safely all the way here, output the dump
        dump.seek(0)
        return dump.read()
# attach this where it belongs (I know, the Ruby people are laughing at this syntax gobble)
dataset.freezes = dataset_freezes
del dataset_freezes

class SqlDumperTableResource(Resource): #XXX this name is the worst
    # TODO: explore exposing and enforcing the natural permissions that the database backend(s) already carries ta
    # ..or maybe when you init this class you pass what database and table it dumps...?
    def __init__(self, table):
        # table should be a dataset.Table object
        self.table = table
        Resource.__init__(self)
        
    def render_GET(self, request):
            request.setHeader("Content-Type", "text/csv")
            try:
                # ---actually this dump is going to lag the WHOLE SERVER
                # because, as written, the dump is done on the server thread
                # TODO: look into using Deferreds here
                #       or twisted.web.server.NOT_DONE_YET (eg http://ferretfarmer.net/2013/09/06/tutorial-real-time-chat-with-django-twisted-and-websockets-part-3/) (which might be the same thing..)
                
                return dataset.freezes(self.table.all())
            except sqlalchemy.exc.OperationalError as e:
                # XXX is including e.message here a security leak?
                return ErrorPage(http.SERVICE_UNAVAILABLE, "Unable to connect to database.", e.message).render(request) 


class CtlProtocol(WebSocketServerProtocol):
   def onConnect(self, request):
      print("Client connecting: {}".format(request.peer))

   def onOpen(self):
      print("WebSocket connection open.")
      ng
   def onMessage(self, payload, isBinary):
      if isBinary:
        print("This is probably bad. Binary message received: {} bytes.".format(len(payload)))
      else:
        print("Text message received: |{}|".format(payload.decode('utf8')))
        try:
          payload = json.loads(payload)
        except ValueError, e:
          payload = {}
 
        message = payload.get('message', 'no message :(')

        if message == 'play':
          print "play"
          try:
            poke_model.start(0.5)
          except AssertionError: # Happens if we tried to start an already started LoopingCall
            pass  

        elif message == 'pause':
          print "pause"
          poke_model.stop()

        elif message == 'setInterventions':
          # delete all interventions in the intervention list
          del model.interventions[:]
          # clean up the log 
          del model.log[:]
          # add all the new interventions
          for intervention in payload['content']['interventions']:
            # FIXME change the names of the arguments to PriceIntervention to match those of the frontend, or vice versa. 
            print intervention
            print 'setted the interventions!'
            product = intervention['activity']
            time = intervention['year']
            scale = intervention['tax_value']
            model.intervene(PriceIntervention(time, product, scale))



   def onClose(self, wasClean, code, reason):
      print("WebSocket connection closed: {}".format(reason))

class ModelDataServer(WebSocketServerProtocol):
   """
   a connection serving timeseries data from Eutopia.
   self.factory.model is Eutopia and is shared amongst
   all "Viewer" ModelDataServers.
   """
   def onOpen(self):
      self.time = self.factory.model.time #stash the time we started watching the model at
      model = self.factory.model
      def push():
          "this function essentially polls for new data"
          "using select() or events would be better"  
          if self.time < len(list(model.log['activities'].distinct("time"))): #XXX this should extract the time range from model.log;
              # but we happen to know that the model starts at 0 always and always increments and always *records* its increments
              #  so this will work;
              # ALSO we should NOT be using "len(list(", we should be asking SQL to do the count for us (.count())...
              #  also, an issue: the db is not normalized, so the timesets might be inconsistent across the tables... 
              # this is crying out for a thin wrapper which makes the database look like something indexable by time
              #TODO: if we are much behind where model is, do we want to batch all the updates at once?
              #  - i like it the way it is now for debugging as we get settled into the new stream, since the model and the data are running at different rates and will therefore get out of sync
              J = model.log['activities'].find_one(time=self.time)
              # the format we were pushing special-cased time: (time, {activity: value, activity: value, ...})
              # for now, backcompat it by selectively keeping columns..
              activities = [act for act in model.log['activities'].columns if act not in ["runID", "time"]]
              J = [J['time'], {k: J[k] for k in activities}]
              print "pushing", J, "to", self.peer #debug
              self.sendMessage(json.dumps(J))
              self.time+=1
      
      self.push = task.LoopingCall(push)
      self.push.start(0.06)
   
   def onClose(self, *args):
      self.push.stop()


#######################
## Main

if __name__ == '__main__':
   #TODO: reindent
   import argparse
   argv = argparse.ArgumentParser(description="Model Explorer Backend Server")
   argv.add_argument("--clean", help="Erase the model database before starting", action='store_true')
   argv.add_argument("--debug", help="Turn on verbose output", action='store_true')
   argv = argv.parse_args()   
   
   if argv.debug:
       log.startLogging(sys.stdout)
       print "Starting server in", PROJECT_ROOT
   
   if argv.clean:
       if os.path.exists(SQLITE_DB):
           os.unlink(SQLITE_DB)
       if argv.debug:
           print("Removing old %s" % (SQLITE_DB,))

   if argv.debug:
       print("Saving eutopia database at %s" % (SQLITE_DB,))
   model = eutopia.Eutopia("sqlite:///%s" % (SQLITE_DB,))
   
   poke_model = task.LoopingCall(lambda: next(model))
   poke_model.start(.5) # intervals measured in seconds (comment out to wait for the user to start the 'game' via the HTML5 UI via the WebSocket)
   
   data_endpoint = WebSocketServerFactory()
   data_endpoint.protocol = ModelDataServer
   data_endpoint.model = model
   
   ctl_endpoint = WebSocketServerFactory()
   ctl_endpoint.protocol = CtlProtocol
      
   webroot = pathjoin(PROJECT_ROOT,"src","frontend")
   assets = pathjoin(PROJECT_ROOT,"assets")
   if argv.debug:
     print "putting", webroot,"at root"
     print "putting", assets,"at assets"
   
   ## we serve static files (most of the frontend html, js, and css) under "/" ..
   ## except for some which are under assets/
   ## and we have our WebSocket server under "/ws"
   root = File(webroot)
   assets = File(assets)
   
   table_data_resource = SqlDumperResource(model.log) # a kludge just to get going; this Resource is parallel to data_resource and basically makes it pointless.
   root.putChild("tables", table_data_resource)
      
   data_resource = WebSocketResource(data_endpoint)
   ctl_resource = WebSocketResource(ctl_endpoint)
   
   root.putChild("assets", assets)  #TODO: do we prefer to have each entry in assets/ sit at the root (ie http://simulation.tld/data/ instead of http://simulation.tld/assets/data/)   
   root.putChild("ws", data_resource)    #this puts the websocket at /ws. You cannot put both the site and the websocket at the same endpoint; whichever comes last wins, in Twisted
   if argv.debug:
     root.putChild("scratch", File(pathjoin(PROJECT_ROOT,"scratch")))
   root.putChild("ctl", ctl_resource) #this whole file is so not pythonic. Where's the D.R.Y. at, yo? --kousu
   
   #. <- /
   #./scratch <- /scratch
   #websocket <- /ws #random demo socket
   #websocket <- /ctl
   #RESTful ??
   
   ## both under one Twisted Web Site
   site = Site(root)
   reactor.listenTCP(8080, site)

   print "Now open http://127.0.0.1:8080 in your browser"
   reactor.run()

