/* Table: an in-memory in-javascript database system modelling multisets instead of tables or documents (though all three translate easily to the others) and heavily supporting dataflow programming (lately cum 'functional reactive', though you could extend this with non-pure-function operators).
 *
 * The system provides subtypes (Where, and, or, not, columns, distinct, sum, average, min, max, groupBy, ...).
 * with each type maintaining a live cache of its current state, as computed from whatever it is based on (necessarily, then, the supported operations are limited to whatever can be efficiently computed on a stream; limited to roughly what SQL provides, in fact).
 
 * The main concept here is the caching, which is a lot like postgres's materialized view or Elm's or PD's per-item state memory; but unlike postgres's implementation, this does edge-triggered processing: it reacts to changes pushed from a source instead of having to poll a source to get a complete new copy.
 *  Since JS is pointers-everywhere, for the types which compute new sets (Where, and, or, not) are storage-cheap: each element only actually exists once; the storage requirement for each type is only sizeof(js_ptr)*cache.length. The other types do not have this guarantee (in particular, Columns has to create new objects)
 *
 *
 * Dependent types listen to their parent's "insert" and "delete" events; when a parent's cache is updated it fires an event, which causes a chain reaction of dependents to check if they should update their cache, and fire their events.
 *
 */
 
 
/* TODO

1) break into multiple files
figure out how to do unit testing properly
 and uh, do we need to do a build system or somtehine? prolly...
  purpose of a js build system: 1) minification 2) to put everything into one file for easy including
2) make Tables into _Tables and make Tables a clone of Tables with its insert and delete removed
 --> which forces us to use Table.prototoype.<f>.call(this, <args>) instead, but its worth it to make the dependent types "immutable"


3) finish NotComplex

note down that the whole pending-queue thing
also note down that the pending-queue types are probably(?) less efficient than simply using where() effectively


figure out how to dispose


refactor and/or because they are sooooo similar because they implement parallel but opposite operations

 *
 * Garbage Collection:
 *  - it is important that dependees hold strong references to their parents and that parents hold (at most) weak references to their children
 *  e.g. if you say
 *   var C = new Table([...]);
 *   var large = C.Where(function(e) { e.width > 9 });
 *   var v = large.columns(["name", "type"]).distinct();
 *  then you have must have a chain of strong references
 *    v (a Distinct) -> Columns -> Where -> Table 
 *   which is good and right because v ultimately depends on all of those 
 *  but if you
 *    delete v; (or otherwise lose its reference)
 *  you do not care about either v or the anonymous Columns instance either, and expect them to be garbage collected; however, because they would have registered event listeners with their parents, their parents are holding a pointer to something that eventually leads to them; these pointers must therefore be weak references or else we will leak memory.

 * TODO: some sort of SortedArray type which has, like, indexOf ops that aren't linear scans (which are slow, on the datasets I'm planning to shove into this) merge() and filter() ops to make
 * TODO: a multiset class, which counts instances ((the reason I've gone to lengths to avoid that is so that all I depend on is regular arrays) 
 * TODO: a Database class which is a collection of tables so that we
         needn't specify what table to replicate
 * TODO: clean up terminology; decide between parents,children and dependees,dependents
 * TODO: look into writing asynchronously, to avoid bogging down the browser with long computations
 */ 


// TIP: if you get "too much recursion" on inserts and deletes, check if you dependent types are calling accidentally calling this.insert instead of self.insert in their event handlers! PourOver.Events runs event handlers *with this set to the object the event happened on*
// TIP: 
 
/* Vision for partial server-side querying
 *  if our query ops are compatible with SQL (or at least, if some of them are)
 *  then perhaps we could do something like just quoting the syntax we use to do querying in js and sending that to the server
 *  or do the AST-object idea where we have a parallel tree of RTable objects (R for "Remote")
 *
 * the best would be to have a situation where we can switch between RTable and Table objects just by renaming, and mix and match at will,
 *  easy enough that we can do performance testing--perhaps even hotspot performance testing--to figure out which queries should be server side and which should be client side
 */


// how do I extend the array type?
// do I want to extend the array type?

// Table has an API similar to but only overlapping with--not inheriting--the Array API
// hence, we wrap an array
// Notably, *this type is not actually a set* but rather a multiset since it allows multiple copies of a single item
// (Use Case: cloning SQL db tables; in practice, all SQL rows should have a unique ID, which automatically, but since SQL allows this not to be true I need to support it not being true)


// TODO: support updates; right now, the API forces you to perform updates as a delete foll
// TODO: are there gnarly edge cases where a delete,insert != insert,delete ? perhaps made gnarlier by the identity issues that is() represents?
// TODO: write a o_cmp which can be used by Array.sort() to order objects; by default, sort() misbehaves on objects
// TODO: wrap this all in one big namespace / module / make it work with nodejs / etc
// TODO: implement all the PourOver filters as functions that construct predicates and then return a Where
// TODO: implement view disposal

//var _ = require('../../assets/libs/underscore.js'); //??
//var PourOver = require('../../assets/libs/pourover.js'); 

//module.exports = Table

/* is essentially defines what we consider to be an equal object
 * we could also use underscore's _.isEqual() which does recursive value comparison
 * or == which has some nasty corner cases that === avoids
 */
 // TODO: test identity issues. What happens if we insert three "3"s then remove two of them? What if we do an equivalent test but with objects? what if the objects are literally the same object, and what if they are clones? I use === which performs python's "is" operator but only on two objects; on two primitive types (ints, floats) it does value comparison, and strings seem to exist in the middle: "a" === "a" is true but new String("a") === new String("a") is false ((probably because js does string interning, like python used to do)
// , which might be desirable but also might make problems with double
// ...hm. since my use case plans to eat deletions as json like {-: obj}, obj will *not* be identity-equal with the target
// one solution: switch to _.isEqual(); pro: does what I need; con: slow; potentially unbounded runtime; con: 
// another: design the API--like PourOver--to demand .remove() is given objects from itself (so that we can depend on identity-equals). Then, at the layer that speaks to the WebSocket, do the conversion. This way, the _.isEqual() cruft only has to happen at one layer, and the whole dataflow chain from then on can just use ===
// is-ness matters for: deletes and Distinct
function is(a,b) {
    //return a === b;
    return _.isEqual(a,b); //rational for putting this in here: yes, it slows things down, but it allows .delete() to behave
    // maybe we should write it that .delete() is special cased to use isEqual
}




/* class Table
 *
 * A Table implements a MultiSet of objects. It is intentionally very similar to a SQL table.
 * 
 * API:
 *  - .on("insert", new_row)  -- fired AFTER insertion
 *  - .on("delete", old_row)  -- fired AFTER deletion 
 *  - FUTURE: .on("update", old_row, new_row) -- fired AFTER updating
 *  - .on("rerender")         -- fired AFTER inserts, updates, deletes
 *  - FUTURE: .at, .toArray(), etc(??)
 *  - CURRENT: use ._cache to access the current state
 *  - Operators:
 *      S.and(Q), S.or(Q)
 *        or And(S, Q, ...), Or(S, Q, ...)
 *      S.where(pred)  - filter the set by a predicate
 *      S.distinct()   - choose only unique items, as compared by underscore's _.isEqqual()
 *      S.map(f)       - convert elements to a new element via a map function
 *      S.select(fields) - slice out new objects from 
 *      S.scalar(field)  - choose only the given field
 *
 *      FUTURE: S.reduce((prev, insert) -> next, (prev, ) - compact the table into a single value
              using this reduce is a bit unusual compared to the classic reduce, because you "prev" value needs to be as much data as you need to handle both inserts and deletes; for 'sum' prev is simply the value of the sum (since a delete can be applied with subtraction), but for more complica
 *      FUTURE: S.sum()         - only works on tables of numbers; will throw an exception if it hits
 
 *  - FUTURE: Join(S, Q, fields)
 * 
 * roughly three categories: filters (and, or, not, where, distinct), maps (map, select, scalar), reduces (sum, avg, 
 *
 * TODO: pick a more descriptive and accurate name
 * TODO: implement .length
 * TODO: implement iterators or element access or something (rather than telling clients to just use ._cache); or, subclass Array and use its built in indexers 
 */
function Table(seed) {
  if(seed === null) { seed = new Array(); }
  this._cache = seed.slice(0); //shallow copy the seed array
}

// core Table API
_.extend(Table.prototype, {
insert: function(e) {
  this._cache.push(e);
  //TODO: keep sorted
  
  this.trigger("insert", e);
  this.trigger("rerender");
},

delete: function(e) {
  // find an element equal to e; note: there might be more than one!
  i = this.findIndex(e);
  //i = this._cache.indexOf(e);
  //i = this._cache.findIndex(function(g) { return _.isEqual(g, e); })
  //TODO: once the caches are kept sorted, use a binary search instead
  
  if(i >= 0) {  //silly bug: "if(i)" is false if i==0, but i==0 is a valid result from indexOf
    this._cache.splice(i, 1);
    this.trigger("delete", e);
    this.trigger("rerender");
  }
},


/* helper method which many of the things share
 *  NB: .findIndex is a gecko extension, which we've imported by a polyfill in libs
 *  this
 */
findIndex: function(e) {
  return this._cache.findIndex(function(g) { return is(g,e); });
}

})

//operators
_.extend(Table.prototype, {
// filtering operators
//NB: these are intentionally restricted to binary operators,
// even though some (And/Or/etc) allow, because it's easier to write
// and because there's an ugly assymetry to A.and(B,C,D) which is not
// in A.and(B) and not in new And(A,B,C,D)
and: function(B) { return new And(this, B); },
or: function(B) { return new Or(this, B); },
not: function(B) { return new Not(this, B); },
where: function(pred) { return new Where(this, pred); },
distinct: function() { return new Distinct(this); },

// map operators
map: function(m) { return new Map(this, m); },
select: function(fields) { return new Select(this, fields); },
scalar: function(field) { return new Scalar(this, field); },

// reduce operators
count: function() { return new Count(this); },
sum: function() { return new Sum(this); },
mean: function() { return new Mean(this); },
// variance: is hard to stream; i think, not impossible, but definitely difficult
// cumsum: cumulative sum is useful but tricky ((also it is not quite a reduce and not quite a map)); it's obvious what to do on insert(), but what do you do with a delete()? do you findIndex() and then shift everything in cache above there down? do you assume sets are unsorted?

// is this going to be an inner or an outer join??
join: function(that, fields) { return new Join(this, that, fields); }
})

_.extend(Table.prototype, PourOver.Events); // TODO: use Backbone.Events instead, since that's the original source
// alternately, roll these ideas into PourOver, though that will be difficult without breaking PourOver, or at least its zen.




/* A Where is a filtered slice of a Table
 *  and is itself considered a Table
 * Like a SQL View, a Where
 *  but a Where is more like a Materialized View (c.f. Postgres; also this hack in MySQL: http://www.fromdual.com/mysql-materialized-views)
  // a Where S = Where(P, pred) by definition is supposed to maintain the invariant that
  // S = { e for e in P if pred(e) }
  // which equivalently means
  //  \forall `e`: `e in P` and `pred(e)` => `e in S`
*/
function Where(parent, pred) {
  var self = this;
  
  //XXX this would be more elegant if it was a subclass of Table,
  // but I don't know how to do js inheritance properly
  // also, it's not clear if a Where should allow direct .insert() and .delete()s
  // TODO: implement .Where(), at least
  
  
  Table.call(this, parent._cache.filter(pred)) //call the super constructor
  
  //  my goal is to avoid having to redefine the methods like insert() and delete() which should come for free
  // in particular, I am *not* rewriting the conviencence methods .and(), .or(), etc; I'm putting them in one place, at the top
  // I also want to run some but maybe not all of the initialization code from the
  
  
  self._pred = pred;
  
  // whoops; .on() runs its callback in the scope of parent.
  // bah. dynamic scopppinggggg!!!
  parent.on("insert", function(e) {
    if(self._pred(e)) { //careful: this is 'parent' in here, not 'self'
      Table.prototype.insert.call(self, e);
    }
  });
  
  parent.on("delete", function(e) {
  
  // if parent.delete(e) actually completes (thus triggering this)
  //   then we know `e in P`.
  // if we also have `pred(e)`
  //   then, by the invariant,
  // we imply
  //    e *is* in self
  
    if(self._pred(e)) {
      Table.prototype.delete.call(self, e);
    }
  });
}
_.extend(Where.prototype, Table.prototype);
_.extend(Where.prototype, { insert: null, delete: null }) //disable insert and delete
 //TODO: better to write this as "disable"



function And() { //<-- varargs
  var self = this;
  
  // The arguments to And are a list of sources to monitor and take the intersection of 
  var Sources = _(arguments).toArray();
  var Sink = self;  // instead of 'self' we use 'Sink' for readability via parallel with 'Sources'.
  
  // Notation: for a multiset M
  //  M[e]
  // is the number of copies of 'e' that exist in M
  // len(M) is the usual length of the multiset--counting each replication of each e
  //  
  // Note that we have this property (for a correct implementation)
  // Sink[e] = min(Source[0][e], Source[1][e], ...)
  //   (( e.g. {a,b,g,g} AND {b, c, g, g} AND {a, c, g} = {g} since g appears only once everywhere  <-- TODO: better example ))
  
  var cache = []
  
  // pending queues: each Source has an associated "pending" multiset of items that are as yet undecided. 
  // Some explanation: 
  //  this is a very highly stateful implementation of a very functionally pure operation
  //  hence, we need to map the functional definition to a state invariant
  // Our invariant, for And, is:
  //  the number of copies of each item e in P[i] is equal to the *difference* between the number of e in Source[i] and the number of e in Sink
  // By the 'min' property of And, we have P[i] = Source[i][e] - Sink[e] = Source[i][e] - min(Source[j][e] | j in 0...) 
  //  so P[i] >= 0 ((since it is defined as a number in a set less the minimum of the numbers in that set))
  // For our actual implementation, this means:
  //  i) we only need to be concerned with storing positive numbers of elements (i.e., regular arrays)
  // ii) if we were ever to go to a negative number of e, that is precisely when we need to instead adjust Sink to maintain the invariant (i.e. self.delete(e))
  // and for good measure, I should mention that self.insert(e) occurs when there is a full house of 'e's across all Sources; so we actually have another invariant:
  //iii) it is never the case that all P[i]s contain an e (((actually, for readability, i'll allow this briefly and then have a scanner do an adjustment)) 
  var P = _(Sources).map(function(e) { return [] })
  
  // our initial cache and Pending queues are constructed simultaneously:
  //  we scan our Sources and make matching sets (like in Go Fish, sort of), creating our cache (aka the multiset "Sink")
  //  and whatever is left over is precisely the difference between Sink and Source (aka the pending queues)
  
  // recursively scan the Sources
  // ummmm do we have to make Source[0] special?
  // step 1) we need shallow copies of all the sources, because we're going to enforce our invariant by chewing up the current state of the sources
  // step 2) scan
  // i) pick an item to look for looking down the scan list until you find
  //   do this by... looping up sources until you find one. popping the  (S[0], S[1], ..., S[m])
  // ii) loop: find its index in all the other sources, if you can
  // iii) pop the given locations ((in js this is the awkward .splice function)) and then
  //      if all other sources provided an index, put one copy into Sink
  //      else, put each copy into their respective P
  // stop when all S are exhausted
  
  var items = common_items(_(Sources).pluck("_cache")); //XXX pluck()ing _cache is awkwardness imposed by Tables (aka Multisets? aka CacheSets aka DataflowMultisets aka I haven't a good name) not being instances of arrays
  
  iterForEach(items, function(value) {
    var found = value.found;
    var item = value.item;
    
    var passed = _.all(found);
    
    if(passed) {
      // if item is in every Source, the And passes it into itself
      // insert one copy into ourselves
      cache.push(item);
    } else {
      // otherweise, item is "incomplete" so we move one copy of it 
      // to each pending queue it came from
      _(_.zip(found, P)).forEach(function(f_p) {
        var f = f_p[0];
        var p = f_p[1];
        
        if(f) {  // only push values that were found, of course
          p.push(item);
        }
      })
    }
  })
  
  
  
  // XXX test how this library handles 'undefined' a 'null'--especially multiple copies of 'undefined' and 'null', which are actually situations that might come up if you use 'delete'
  // XXX make sure to handle the case where user just calls "new And()" which is legal and should be the empty multiset
  // XXX what if S[0] is shorter than the others and runs out of items first? we need to not hardcode S[0] in!! -- for And this is alright but we want the same basic idea for Or and it won't be alright there)
  
  // Compute (A and B) as [e for e in A if e in B]
  //TODO: exploit sorting to make this faster
  // as written, this is an O(n^2) step
  // XXX it's also *wrong*: since A and B are supposed to be multisets, the proper result on A={g,g,g} B={g,g} is {g,g} but it will end up being {g,g,g} because for each of A's entries it will just see if it can find any copy of g in B
  //Table.call(this, A._cache.filter(function(a) {
  //  return B._cache.indexOf(a) != -1;
  //}));
  
  
  self._Sources = Sources; //DEBUG
  self._P = P; //DEBUG
  
  
  // and Or is similar to all this, except instead of counting the difference Source - Sink
  // it should count the opposite: Sink - Source
  //  because for Or Sink[e] = max(Source[i][e] | i ...), so Sink[e] >= Source[i][e] 
  // it adjusts (with insert, instead of with delete) when a source breaches the max
  // and otherwise
  
  // Not is also similar:
  // Not can be written "A and (not B)" so everything for not is the same as for and except inserts and deletes are reversed for B(?)
  
  Table.call(this, cache);
  
  // now, an incoming item can only come in if it is BOTH in A and in B
  //  that means we cannot add(self, e) until we have heard A.on("insert", e) and B.on("insert", e)
  // so, we maintain a limbo state, of items we're waiting to hear from A and from B about
  // this is totally symmetric about A,B (because AND is commutative), so without loss of generality, suppose A.on("insert",e) happens, and suppose there is nothing else but e (since each item is essentially in a slice unto itself)
  // then there are four possible next events, as far as this object is concerned:
  //   A.on("insert", e)  --> in this case, e is a duplicate; since we *allow* duplicates, we just act as if e is a new unique item (i.e. this case isn't really a distinct case at all)
  //   A.on("delete", e)  --> remove e from the limbo queue
  //   B.on("insert", e)  --> remove e from the limbo queue and put it into cache
  //   B.on("delete", e)  --> we haven't heard about e yet; ignore
  //   -> if, before we hear about e from
  
  // now, for each Source, we need to set event listeners
  //  we need to use a closure here to bind our ID for each Source (i.e. its index in S) against who it is, so that we know which P[i] to update
  
  //I dislike the syntax I have to use for "zip"; in python "for a,b in zip(A,B)" is super readable. _(_.zip(A,B)).forEach(function(a_b) { var a = a_b[0]; var b = a_b[1]; ... } ) is a lot less 
  
  _(_.zip(Sources, P)).forEach(function(s_p) {
    var s = s_p[0];
    var p = s_p[1];
    
    // attach event listeners to s
    s.on("insert", function(e) {
      // remember: 'this' changes inside of 'on()'s
      // insert e to p, then tell Sink to scan its pending queues again
      p.push(e);
      
      // on insert: we need to find scan the pending queues to see if
      // 
      //  ---can we use common_items here??
      // if we use common_items we'll end up in a problem
      // because though it will tel us what items are common, it won't tell us
      // we could
      
      // as a premature optimization, we tell the scanner what item it's looking for apriori, so it can skip the outer scanning loop
      // TODO: as another premature optimization, if we zipped the index of (s,p) = (S[i],P[i]),
      //  instead of this brutish approach, we could only scan the non-i P-queues and break as soon as we don't turn up a finding
      // actually, the alg below is wrong, because we *don't* pop unless we pop all
      // okay, moving the 'if(passed)' made it correct, but now the map() is unused and would be better written as forEach()
      
      // XXX copied from above; this needs a refactor!!
      // now, find item in sources
      
      var locations = indexOfMany_Is(P, e)
    
      var passed = _.all(locations, function(p) { return p != -1 });
    
      if(passed) {
        // now we pop all those items off the pending queue
        _(_.zip(locations, P)).forEach(function(l_p) {
          var l = l_p[0];
          var p = l_p[1]; //warning! shadows the previous 'p' 
          
          if(l != -1) {
            p.removeAt(l);
          }
        });
        
        // and move them into self        
        self.insert(e);
      }
    })
    
    
    s.on("delete", function(e) {
      // remember: 'this' changes inside of 'on()'s
      // try to remove e from p; if we can't, 
      // then tell Sink it needs to delete e ((see class header for why))
      var i = -1;
      if((i = p.indexOf_Is(e)) != -1) {
        p.removeAt(i)
      } else {
        self.delete(e);
        
        // in order to maintain the invariant, whcih is
        // \forall i: P[i][e] == Source[i][e] - Sink[e],
        // if we subtract 1 from sink
        // then we must add 1 to each P[i]
        // ..do we need to even do this to the current P[i]?? that would seem.. odd. 
        // no. no we don't. because additionally we just deleted from Source[i]
        // XXX this is a kludge we should just give up on zip() and just use for loops!!
        P.forEach(function(_p) {
          if(_p === p) { return } //skip the current P
          // wait.. this is.. wrong...
          // it's only correct *if the associated source actually contains e, and the right number of them*
          // ..actually, the only ways we can end up in this branch are all if Source[k] contains e
          /// BECAUSE if Source[k] doesn't contain e, then Source[k]
          // is it possible for eg P[0] = {g}, P[1] = {}, P[2] = {}, then you S[1].delete(g), which triggers P[0] = {g,g}, P[1] = {}, P[2] = {g} but where S[2] never contained g at all?
          // okay, how would I get this situation?
          // S[0] = {g,h}, S[1] = {h}, S[2] = {h} --> AND = {h}
          // // RIGHT:  in order for S[1].delete(g) to actually go through yet P[1] to not contain g, it must be true that g got gobbled up previously, i.e. every source contained a g
          // so funnily enough this weird step is totally correct. 
          _p.push(e);
        }) 
      }
    })
  })
}
_.extend(And.prototype, Table.prototype);



function Or(A, B) {
  var self = this;
  
  // The arguments to And are a list of sources to monitor and take the intersection of 
  var Sources = _(arguments).toArray(); //XXX copypasta
  var Sink = self;  // instead of 'self' we use 'Sink' for readability via parallel with 'Sources'.
  
  // Or is reversed from And:
  // instead of P[i] = Source[i] - Sink
  // it's       P[i] = Sink - Source[i]
  // so, when P is positive it means there are more 'e's in Sink than in Source
  // and P is disallowed from being negative: Source[i] <= Sink because Sink = Max(Source[i] for i in ...)
  // because???????????
  
  var cache = [] //XXX copypasta
  var P = _(Sources).map(function(e) { return [] }) //XXX copypasta
  
  var items = common_items(_(Sources).pluck("_cache")); //XXX copypasta
  
  iterForEach(items, function(value) {
    var found = value.found;
    var item = value.item;
    
    var passed = _.any(found); //Note the key difference from And(): we use any() here instead of all()
    
    
    //TODO: this could be cleaned up if instead of doing cache.push() before Table.call() and self.insert() after, we just used self.insert(); the only awkwardness is that that calls self.trigger("insert") before we're fully constructed --except js is single-threaded so it shouldn't be possible for any events to exist.
    
    if(passed) {
      // if item is in any Source, the Or includes it
      cache.push(item);
      
      // to maintain the invariant
      // every pending quefound item needs to end up grow
      // push() above is Sink++ 
      // so source P[i] ++ too
      // since p counts how many below the max s is
      //   delete
      _(_.zip(found, P)).forEach(function(f_p) {
        var f = f_p[0];
        var p = f_p[1];
        
        if(!f) { p.push(item) }
      })
      // 
    } // no else, because if none are found there's nothing else to do
    // If this seems asymmetrical from And(), remember that we've factored part of the operation--the part that noms identical items so that they don't cause redundant inserts, into common_items()
    // or look at it this way: P[i] counts the number of deletes on each source
    // and at init there are none
    //..wait..
    // uhhh
  })
  
  
  
  Table.call(this, cache);
  
  _(_.zip(Sources, P)).forEach(function(s_p) {
    var s = s_p[0];
    var p = s_p[1];
    
    
    s.on("insert", function(e) {
      // when you get an insert, you either
      //  -> push to self
      // or 
      //  -> push to a pending thing(???)
        
      //1) check if e exists in our pending queue. if so, eat it from there silently
      //2) else, this is a *new* insert
        
      var i = -1;
      if((i = p.indexOf_Is(e)) != -1) {
        p.removeAt(i);
      } else {
        self.insert(e);
        
        P.forEach(function(_p) {
          if(_p === p) { return } //skip the current 
          _p.push(e);
        }) 
      }
    })
    
    // Ppi] is the difference between the number in each
    
    s.on("delete", function(e) {
      // push to p... 
      p.push(e);
      
      // now, find out if pushing e caused a pending set to complete
      // 
      // if so, we have one full delete:
      // i.e. every s has lost one e, so the maximum number of es should be reduced by 1
      // 
      var locations = indexOfMany_Is(P, e); 
      var passed = _.all(locations.map(function(l) { return l != -1 } ))
      if(passed) {
        //XXX copypasted
        
        // now we pop all those items off the pending queue
        _(_.zip(locations, P)).forEach(function(l_p) {
          var l = l_p[0];
          var p = l_p[1]; //warning! shadows the previous 'p' 
          
          if(l != -1) {
            p.removeAt(l);
          }
        });
        
        self.delete(e);
      }
    })
  })
  
}
_.extend(Or.prototype, Table.prototype);





function NotSimple(S, Z) {
  /* Implementation of the "Not" operator (Not(S,Z) := S \ Z)
   *  using a single pending queue
   *
   */
  var self = this;
  
  // the external invariant this class should maintain is
  //   self[e] = S[e] - Z[e] and self[e] > 0 or 
  //   self[e] = 0
  //  we have these state changes:
  // S.insert(e) or Z.delete(e) cause (S[e] - Z[e])++
  // S.delete(e) or Z.insert(e) cause (S[e] - Z[e])--
  // but to minimize data and time we don't actually want to keep
  //   entire copies of S[e] or Z[e] and require scans to work with them
  // instead we actually maintain the internal invariant
  //   S[e] - Z[e] = self[e] - P[e]   and
  //   self[e] >= 0
  // So we map the external state changes to this:
  // S.insert(e) or Z.delete(e) cause P[e]-- unless P[e]=0, in which case self[e]++
  // S.delete(e) or Z.insert(e) cause S[e]-- unless S[e]=0, in which case P[e]++
  //  observe: These transitions guarnatee self[e], P[e] >= 0
  // TODO: more explicit proof of correctness
  
  var cache = []
  var P = [] // pending queue, which is not really a queue at all
  
  
  self._P = P;
  var items = common_items([S._cache, Z._cache]);
  iterForEach(items, function(value) {
    var found = value.found;
    var item = value.item;
    
    if(_.all(found)) {    
      // common items are *ignored* because they cancel each other out, with Not()
    } else if(found[0]) {
      // only found in S, so in S\Z
      cache.push(item);
    } else if(found[1]) {
      // only in Z, so not in S\Z but we need to record that there is an unbalanced item
      P.push(item);
    }
  })
  
  // step 2) scan
  // i) pick an item to look for looking down the scan list until you find
  //   do this by... looping up sources until you find one. popping the  (S[0], S[1], ..., S[m])
  // ii) loop: find its index in all the other sources, if you can
  // iii) pop the given locations ((in js this is the awkward .splice function)) and then
  //      if all other sources provided an index, put one copy into Sink
  //      else, put each copy into their respective P
  // stop when all S are exhausted
  
  
  // In python I would write this as a chain of generators:
  //  items(), which makes itemsetes tuples as long as the number of  ((and internally is chewing its input lists up, since that is simpler and maybe even more efficient than the functional style of marking certain items as )) [ aside: the great power of python's generators (besides their officially sanctioned misuse as coroutines) is that you can hide stateful code inside something that produces pure-functional values ]
  //  then some code that for each
  // then a function which takes a function to do on each item
  
  Table.call(this, cache);
  
  function Not_push(e) { //WARNING: DO NOT USE `this` HERE
    var i=-1;
    if((i = P.indexOf_Is(e)) != -1) {
      P.removeAt(i);
    } else {
      self.insert(e);
    }
  }
  
  // TODO: factor these? since it's copypasted?
  function Not_pop(e) { //WARNING: DO NOT USE `this` HERE
    var i=-1;
    if((i = self._cache.indexOf_Is(e)) != -1) {
      self.delete(e); //XXX this redoes the scan we just did!!
      // XXX maybe 'delete' should return 'true' if it succeeded?
    } else {
      P.push(e);
    }
  }
  
  S.on("insert", Not_push)
  Z.on("delete", Not_push)
  S.on("delete", Not_pop)
  Z.on("insert", Not_pop)
  
}
_.extend(NotSimple.prototype, Table.prototype);

function NotComplicated(S, Z) {
  /* Implementation of the "Not" operator (Not(S,Z) := S \ Z)
   *  using a a pending queue for each source
   *  this code is longer than NotSimple, but points the way
   *  towards a factorization of all classes that use pending queues
   *
   */
  var self = this;
  
  // The idea here is roughly the same,
  // except we would like to have P[S] and P[Z] queues which in some sense "legitimately" track the state
  // so, a {insert,delete} on S only affects P[S], and same for Z
  // this is actually *more code* but it..feels..more correct.
  
  throw "NotImplemented"; 
  
  
  Table.call(this, cache);

}
_.extend(NotComplicated.prototype, Table.prototype);

var Not = NotSimple



/* kludge: find e across all sets in S
 */
function indexOfMany_Is(S, e) {
  return _(S).map(function(s) { 
    return s.indexOf_Is(e);
  })
}


/* kludge to use is() (i.e., currently _.isEqual()) in searching for elements
 *
 */
Array.prototype.indexOf_Is = function(e) {
  return this.findIndex(function(g) { return is(g, e) });
}

/* kludge; there's probably a more js-onic way to handle arrays ? */
Array.prototype.removeAt = function(i) {
  var e = this[i];
  this.splice(i, 1);
  return e;
}



function iterForEach(items, f) {
  /* helper that hides the ugly edge cases of iterators
   
   */
  // XXX this should be a method on the iterator type... except there is no iterator type
  // I want to say while(!v.done) except I don't have a v until I call next once,
  //  but I can't call next() and start processing blindly because v might be done on the first step!
  // python handles this common edge case fluidly by making "for e in g()" understand how to break at the first step
  // I have to hand-roll the same for js, it seems? 
  
  while(true) { // aaah why isn't there like a ... do-maybe()? How 
  // the https://developer.mozilla.org/en-US/docs/Web/JavaScript/Guide/The_Iterator_protocol is thin on the syntax for actually pumping an iterator...
    var v = items.next();
    if(v.done) { break; }
    
    f(v.value);
  }
}


function common_items(S) {
 /* an iterator which returns tuples of the items as found in
  *  the operation is sort of like zip, except it zips down to common items, with equality defined by is() above
  *
  */
 // S is an array of sets (sets passed as arrays)
 // the arrays will be chewed up, with each step yielding  (in implementation-dependent order! don't rely on it!) 
 //  an item and a list of booleans indicating whether that item was found in each set
 // {item: ..., found: [.., .., ..., ]}
 //TODO: figure out how to call functions in js while setting the arguments array; 
 // ie. I really want to do def common_items(*S) but I don't know how to make that not awkward in js
 
  // clone the input, since this algorithm destructively edits it
  var S = _(S).map(function(S) { return _(S).clone(); })
  // XXX...is _(S).clone() enough here? I think clone() is only a one-level-deep clone, right? so I need to explicitly say the map() to get two-levels-deep...

  return {
    next: function() {


      //find some item to split up on
      var available_set = _(S).find(function(s) { //NB: at this point, item is actually an array containing items (or undefined)
        return s.length > 0;
      })


      if(available_set !== undefined) {

        var item = available_set[0]; //pick out the item to use. DO NOT USE pop() HERE.

        //TODO: there's some obvious minor optimizations we could make, like not scanning back over Sources that we know are already empty and not scanning the chosen source for its 0th element
        // TODO: maybe sorting sources by their lengths is a good idea, since that guarantees that S[0] runs out last--or simultaneously last

        // try to locate one copy of item in each source 
        var locations = indexOfMany_Is(S, item)

        // squish down the locations to simple booleans
        // returning the locations is meaningless, externally, since the locations change as we nom up S and bear little relation to the initially passed
        var found = _.map(locations, function(p) { return p != -1 });
        
        // now we pop all those items
        // XXX using zip() is almost as verbose as just using an explicit for loop
        _(_.zip(locations, S)).forEach(function(l_s) {
          var l = l_s[0]; //this is ugly because I'm trying to write python in javascript
          var s = l_s[1]; // this could probably be done more js-esque.
          
          if(l != -1) {
            s.removeAt(l);
          }
        });



        return {value: {item: item, found: found}, done: false};
      } else {
        // no more items to split up!
        console.assert(_.all(_(S).map(function(s) { return s.length == 0 })), "when we're done initializing AND, S, the cloned Sources, should be empty")
        delete S;

        return {value: null, done: true};
      }
    }
  }

}




function Distinct(parent) {

  var self = this;
  Table.call(this, [])
  
  // TODO: speed this up by sorting
  // TODO: handle deleting (to do it correctly we need to track the number of occurences of each element); perhaps if we had a usable groupBy...
  function insert(e) {
    if(self.findIndex(e) < 0) {
      Table.prototype.insert.call(self, e);
    }
  }
  parent._cache.forEach(insert); //XXX this is an O(n^2) line, currently!! On a moderately sized set it causes multiple unresponsive script warnings before finishing
  // ergh..
  // is there a way maybe to do this asynchronously?
  
  parent.on("insert", insert);
}
_.extend(Distinct.prototype, Table.prototype);




function Map(parent, m) {
  
  var self = this;
  Table.call(this, parent._cache.map(m));
  
  parent.on("insert", function(e) {
    e = m(e);
    self.insert(e);  //warning! 'this' is not 'self' within these event handlers!!
  });
  
  parent.on("delete", function(e) { // as in "Where", we have an invariant that implies that if we actually see a delete we know necessarily we will perform a delete
    e = m(e);
    self.delete(e);
  });
}
_.extend(Map.prototype, Table.prototype);


/* Select: select only the given fields from the objects
 *  if you use this on scalars you will have a bad time.
 *  
 */ 
function Select(parent, fields) { //TODO: fixit so that you ; or just wrap everything in new-safety code and tell people not to use new with any of the operator-types.
  function select(o, fields) {
    //TODO: special case: if fields is not an array, wrap it as a 1-element array
    // or maybe, if fields is *not* an array, operate as pluck() (eschewing creating a container object at all)
    r = {}
    fields.forEach(function(f) {
      r[f] = o[f];
    })
    return r;
  }

  Map.call(this, parent, function(e) {
    return select(e, fields);
  })
}
_.extend(Select.prototype, Map.prototype);

/* Scalar: like select() but extracts a single field and does not wrap it in an object;
 *  equivalent to underscore's pluck(); should we alias it to match?
 */
function Scalar(parent, field) {
  Map.call(this, parent, function(e) {
    return e[field];
  })
}
_.extend(Scalar.prototype, Map.prototype);


// TODO: factor all these into a Reduce type

function Count(parent) {
  var self = this;
  
  self.value = +parent._cache.length;
  parent.on("insert", function(e) { self.value += 1})
  parent.on("delete", function(e) { self.value -= 1})
}
_.extend(Count.prototype, PourOver.Events);

function Sum(parent) {
  var self=this;
  self.value = parent._cache.reduce(function(prev, e) { return (+prev) + (+e) }); /* the +(.) is js for "typecast to numeric"; things that are not already numeric come out NaN, which is a reasonable thing */
  
  parent.on("insert", function(e) { self.value += e })
  parent.on("delete", function(e) { self.value -= e })
}
_.extend(Sum.prototype, PourOver.Events);


function Mean(parent) {
  var self=this;
  
  var sum = parent.sum()
  var count = parent.count()
  
  function sync(e) { self.value = (+sum.value) / (+count.value) }
  sync()
  
  parent.on("rerender", sync)
}
_.extend(Mean.prototype, PourOver.Events);



function test_NOT() {


var A = new Table(["g", "g", "h", 2]);
var B = new Table([3, "h", "g", 9]);

var N = new Not(A,B);

console.assert(_.isEqual(_.clone(N._cache).sort(),  [2, "g"]), "")


//whitebox test:
// removing from A should

A.delete("g");
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2]), "")

A.delete("g"); //not a no-op, but no "observable" difference in N
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2]), "")
A.delete("g"); //should be a no-op

A.insert("g");
A.insert("g");
A.insert("g");
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2,"g","g"]), "")

B.insert("g");
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2,"g"]), "")


A.insert("g");
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2,"g","g"]), "")
B.insert("g");
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2,"g"]), "")

// inserting a bunch to B should shadow further inserts to A
B.insert("g");
B.insert("g");
B.insert("g");
B.insert("g");
B.insert("g");
B.insert("g"); // at this point, we should be at -5

console.assert(_.isEqual(_.clone(N._cache).sort(),  [2]), "")
A.insert("g"); //-4
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2]), "")
A.insert("g"); //-3
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2]), "")
A.insert("g"); //-2
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2]), "")
A.insert("g"); //-1
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2]), "")
A.insert("g"); //0
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2]), "")
A.insert("g"); //1
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2,"g"]), "")
A.insert("g"); //2
console.assert(_.isEqual(_.clone(N._cache).sort(),  [2,"g","g"]), "")


}


function test_AND() {

// I've intermixed blackbox, whitebox, and state-based tests here
// They should really be separated by category (and esp. the work put in to make generating known states easy to then run a battery of tests again, instead of copypasting)
// and into different functions
// The nodejs/dat people know how to do testing for js

var A = new Table(["g", "g", "h", 2]);
var B = new Table([3, "h", "g", 9]);
var AB = new And(A, B);

window.A = A;  //DEBUG
window.B = B;
window.AB = AB;

console.assert(_.isEqual(_.clone(AB._cache).sort(),  ["g", "h"]), "Initializing AND should get the right results and be able to handle multiple copies properly")

B.insert("g"); //should cauase AB to contain

console.assert(_.isEqual(_.clone(AB._cache).sort(),  ["g", "g", "h"]), "Inserting a dupe should be allowed and the pending queues should react appropriately--since these are multisets")

B.delete("g"); //should cauase AB to contain
console.assert(_.isEqual(_.clone(AB._cache).sort(),  ["g", "h"]), "Undoing the insert should make AND indistinguishable from its previous state")


// Second round of tests

// empty should be allowed!!
var AB = new And();
console.assert(_.isEqual(_.clone(AB._cache).sort(),  []), "An empty And should be allowed and should produce the empty set")


// Third round of tests
// wherein A and B do not overlap

var A = new Table(["a", "b", "g", 2]);
var B = new Table([3, "c", "h", 9]);
var AB = new And(A, B);


window.A = A;  //DEBUG
window.B = B;
window.AB = AB;

console.assert(_.isEqual(_.clone(AB._cache).sort(),  []), "Initializing AND should get the right results and be able to handle multiple copies properly")

B.insert("g"); //should cauase AB to contain

console.assert(_.isEqual(_.clone(AB._cache).sort(),  ["g"]), "Inserting should react properly")

B.delete("g"); //should cauase AB to contain
console.assert(_.isEqual(_.clone(AB._cache).sort(),  []), "Undoing the insert should make AND indistinguishable from its previous state")


B.delete("h")
console.assert(_.isEqual(_.clone(AB._cache).sort(),  []), "eating something in B when there is no overlap should cause no reaction")
console.assert(_.isEqual(_.clone(AB._P[0]).sort(),  [2, "a", "b", "g"]), "eating something in B should only cause the pending queue for B to change")
console.assert(_.isEqual(_.clone(AB._P[1]).sort(),  [3, 9, "c"]), "eating something in B when there is no overlap should only cause the pending queue for B to change")


B.insert("h")
console.assert(_.isEqual(_.clone(AB._cache).sort(),  []), "eating something in B when there is no overlap should cause no reaction")
console.assert(_.isEqual(_.clone(AB._P[0]).sort(),  [2, "a", "b", "g"]), "eating something in B when there is no overlap should cause no reaction")
console.assert(_.isEqual(_.clone(AB._P[1]).sort(),  [3, 9, "c", "h"]), "eating something in B when there is no overlap should cause no reaction")


//Fourth round of tests: is deleting
var A = new Table(["h"]);
var B = new Table(["h", "g"]);
var C = new Table(["h"]);
var ABC = new And(A, B, C);


console.assert(_.isEqual(_.clone(ABC._cache).sort(),  ["h"]), "")
console.assert(_.isEqual(_.clone(ABC._P[0]).sort(),  []), "")
console.assert(_.isEqual(_.clone(ABC._P[1]).sort(),  ["g"]), "")
console.assert(_.isEqual(_.clone(ABC._P[2]).sort(),  []), "")

C.delete("g") //should have no effect
console.assert(_.isEqual(_.clone(ABC._cache).sort(),  ["h"]), "")
console.assert(_.isEqual(_.clone(ABC._P[0]).sort(),  []), "")
console.assert(_.isEqual(_.clone(ABC._P[1]).sort(),  ["g"]), "")
console.assert(_.isEqual(_.clone(ABC._P[2]).sort(),  []), "")

B.delete("g") //shoudl only affect the pending queue
console.assert(_.isEqual(_.clone(ABC._cache).sort(),  ["h"]), "")
console.assert(_.isEqual(_.clone(ABC._P[0]).sort(),  []), "")
console.assert(_.isEqual(_.clone(ABC._P[1]).sort(),  []), "")
console.assert(_.isEqual(_.clone(ABC._P[2]).sort(),  []), "")

B.insert("g") //should reset..
console.assert(_.isEqual(_.clone(ABC._cache).sort(),  ["h"]), "")
console.assert(_.isEqual(_.clone(ABC._P[0]).sort(),  []), "")
console.assert(_.isEqual(_.clone(ABC._P[1]).sort(),  ["g"]), "")
console.assert(_.isEqual(_.clone(ABC._P[2]).sort(),  []), "")

A.delete("h") //should shift the now-missing h from the AND onto the pending queues that *aren't* A's
console.assert(_.isEqual(_.clone(ABC._cache).sort(),  []), "")
console.assert(_.isEqual(_.clone(ABC._P[0]).sort(),  []), "")
console.assert(_.isEqual(_.clone(ABC._P[1]).sort(),  ["g","h"]), "")
console.assert(_.isEqual(_.clone(ABC._P[2]).sort(),  ["h"]), "")

window.A = A;  //DEBUG
window.B = B;
window.C = C;
window.AB = AB;

}

function test_OR() {


var A = new Table(["g", "g", "h", 2]);
var B = new Table([3, "h", "g", 9]);
var AB = new Or(A, B);

console.assert(_.isEqual(_.clone(AB._cache).sort(),  [2,3,9,"g", "g", "h"]), "Initializing OR should get the right results and be able to handle multiple copies properly")

B.insert("g"); //should cauase AB to contain
console.assert(_.isEqual(_.clone(AB._cache).sort(),  [2,3,9,"g", "g", "h"]), "Inserting a shadowed element should result in no difference")


B.insert("g"); //should cauase AB to contain
console.assert(_.isEqual(_.clone(AB._cache).sort(),  [2,3,9,"g", "g", "g", "h"]), "..but the next insert of the same should spill over")


var A = new Table(["g", "g", "h", 2]);
var B = new Table([3, "h", "g", 9]);
var AB = new Or(A, B);

A.insert("g"); //should cauase AB to contain
console.assert(_.isEqual(_.clone(AB._cache).sort(),  [2,3,9,"g", "g", "g", "h"]), "Conversely, inserting to A is *not* shadowed by B so it should immediately grow")


B.insert("g"); //should cauase AB to contain
B.insert("g"); //should cauase AB to contain
console.assert(_.isEqual(_.clone(AB._cache).sort(),  [2,3,9,"g", "g", "g", "h"]), "making inserts to B shadowed twice over")

B.insert("g"); //should cauase AB to contain
console.assert(_.isEqual(_.clone(AB._cache).sort(),  [2,3,9,"g", "g", "g", "g", "h"]), "but thrice does cause an insert")


}


function test_Table() {


var monsters = [{name: "sphinx", mythology: "greek", eyes: 2, sex: "f", hobbies: ["riddles","sitting","being a wonder"]},
                {name: "hydra", mythology: "greek", eyes: 18, sex: "m", hobbies: ["coiling","terrorizing","growing"]},
                {name: "huldra", mythology: "norse", eyes: 2, sex: "f", hobbies: ["luring","terrorizing"]},
                {name: "cyclops", mythology: "greek", eyes: 1, sex: "m", hobbies: ["staring","terrorizing","sitting"]},
                {name: "fenrir", mythology: "norse", eyes: 2, sex: "m", hobbies: ["growing","god-killing","sitting"]},
                {name: "medusa",  mythology: "greek", eyes: 2, sex: "f", hobbies: ["coiling","staring"]}];
var P = new Table(monsters);

function setstr(s) {
  //return Scalar(s, "name")._cache
  return _(s._cache).pluck("name")
}

console.info("Table TESTS");
window.P = P; //DEBUG

var norse_monsters = P.where(function(m) { return m.mythology == "norse" });
var sitting_monsters = P.where(function(m) { return _(m.hobbies).contains("sitting") });
window.norse_monsters = norse_monsters; //DEBUG
window.sitting_monsters = sitting_monsters; 

var names = P.scalar("name");


console.log("norse_monsters = ", setstr(norse_monsters));
console.log("sitting_monsters = ", setstr(sitting_monsters));

console.log("scalar[names] = ", JSON.stringify(names._cache));

var norse_and_sitting = norse_monsters.and(sitting_monsters);
console.log("norse AND sitting = ", setstr(norse_and_sitting));

var norse_or_sitting = norse_monsters.or(sitting_monsters);
console.log("norse OR sitting = ", setstr(norse_or_sitting));
console.info("");

var norse_view = norse_monsters.select(["name", "mythology"]);


var n = {name: "ogabooga", mythology: "norse", eyes: 17, sex: "t", hobbies: ["staring","scaring","sitting","crying"]};


var seated_eyes = sitting_monsters.scalar("eyes").sum()
var sitting_count = sitting_monsters.select(["eyes", "hobbies"]).count()
var mean_eyes = P.scalar("eyes").mean()

console.log("scalar[names] = ", JSON.stringify(names._cache));

console.log("there are",seated_eyes.value,"sitting eyes");
console.log("and ",sitting_count.value,"sitting monsters");
console.log("for an average of ",mean_eyes.value,"eyes");

console.log("adding", n);
P.insert(n);


console.log("scalar[names] = ", JSON.stringify(names._cache));
console.log("norse_monsters = ", setstr(norse_monsters));
console.log("norse_view = ", JSON.stringify(norse_view._cache));
console.log("sitting_monsters = ", setstr(sitting_monsters));
console.log("norse AND sitting = ", setstr(norse_and_sitting));
console.log("norse OR sitting = ", setstr(norse_or_sitting));
console.info("");



console.log("there are",seated_eyes.value,"sitting eyes");
console.log("and ",sitting_count.value,"sitting monsters");
console.log("for an average of ",mean_eyes.value,"eyes");

console.log("adding", n);
P.insert(n);



console.log("norse_monsters = ", setstr(norse_monsters));
console.log("norse_view = ", JSON.stringify(norse_view._cache));
console.log("sitting_monsters = ", setstr(sitting_monsters));
console.log("norse AND sitting = ", setstr(norse_and_sitting));
console.log("norse OR sitting = ", setstr(norse_or_sitting));
console.info("");


console.log("there are",seated_eyes.value,"sitting eyes");
console.log("and ",sitting_count.value,"sitting monsters");
console.log("for an average of ",mean_eyes.value,"eyes");

console.log("deleting", n);
P.delete(n);

console.log("norse_monsters = ", setstr(norse_monsters));
console.log("norse_view = ", JSON.stringify(norse_view._cache));
console.log("sitting_monsters = ", setstr(sitting_monsters));
console.log("norse AND sitting = ", setstr(norse_and_sitting));
console.log("norse OR sitting = ", setstr(norse_or_sitting));
console.info("");


console.log("there are",seated_eyes.value,"sitting eyes");
console.log("and ",sitting_count.value,"sitting monsters");
console.log("for an average of ",mean_eyes.value,"eyes");

console.log("deleting", monsters[2]);
P.delete(monsters[2])

console.log("norse_monsters = ", setstr(norse_monsters));
console.log("norse_view = ", JSON.stringify(norse_view._cache));
console.log("sitting_monsters = ", setstr(sitting_monsters));
console.log("norse AND sitting = ", setstr(norse_and_sitting));
console.log("norse OR sitting = ", setstr(norse_or_sitting));	
console.info("");

console.log("there are",seated_eyes.value,"sitting eyes");
console.log("and ",sitting_count.value,"sitting monsters");
console.log("for an average of ",mean_eyes.value,"eyes");
}
//test_Table()
test_AND();
test_NOT();
test_OR();


/*************************

 sketch of how using sorted sets speeds operations up
 1) .findIndex() can be replaced by .findIndexByBinarySearch()
 2) 

*/