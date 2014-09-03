/* cacheset: an in-memory in-javascript database system based around multisets instead of around tables or documents (though any can be translated to the others) and around dataflow programming.
 *
 * The system provides subtypes (subset, and, or, not, columns, distinct, sum, average, min, max, groupBy, ...).
 * with each type maintaining a live cache of its current state, as computed from whatever it is based on (necessarily, then, the supported operations are limited to whatever can be efficiently computed on a stream; limited to roughly what SQL provides, in fact).
 
 * The main concept here is the caching, which is a lot like postgres's materialized view; but unlike postgres's implementation, this does edge-triggered processing: it reacts to changes pushed from a source instead of having to poll a source to get a complete new copy.
 *  Since JS is pointers-everywhere, for the types which compute new sets (subset, and, or, not) are storage-cheap: each element only actually exists once; the storage requirement for each type is only sizeof(js_ptr)*cache.length. The other types do not have this guarantee (in particular, Columns has to create new objects)
 *
 *
 * Dependent types listen to their parent's "insert" and "delete" events; when a parent's cache is updated it fires an event, which causes a chain reaction of dependents to check if they should update their cache, and fire their events.
 *
 *
 * Garbage Collection:
 *  - it is important that dependees hold strong references to their parents and that parents hold (at most) weak references to their children
 *  e.g. if you say
 *   var C = new CacheSet([...]);
 *   var large = C.subset(function(e) { e.width > 9 });
 *   var v = large.columns(["name", "type"]).distinct();
 *  then you have must have a chain of strong references
 *    v (a Distinct) -> Columns -> SubSet -> CacheSet 
 *   which is good and right because v ultimately depends on all of those 
 *  but if you
 *    delete v; (or otherwise lose its reference)
 *  you do not care about either v or the anonymous Columns instance either, and expect them to be garbage collected; however, because they would have registered event listeners with their parents, their parents are holding a pointer to something that eventually leads to them; these pointers must therefore be weak references or else we will leak memory.
 
 * TODO: clean up terminology; decide between parents,children and dependees,dependents
 */ 
 

// how do I extend the array type?
// do I want to extend the array type?

// CacheSet has an API similar to but only overlapping with--not inheriting--the Array API
// hence, we wrap an array
// Notably, *this type is not actually a set* but rather a multiset since it allows multiple copies of a single item
// (Use Case: cloning SQL db tables; in practice, all SQL rows should have a unique ID, which automatically, but since SQL allows this not to be true I need to support it not being true)


// TODO: support updates; right now, the API forces you to perform updates as a delete foll
// TODO: are there gnarly edge cases where a delete,insert != insert,delete ? perhaps made gnarlier by the identity issues that is() represents?

//var _ = require('../../assets/libs/underscore.js'); //??
//var PourOver = require('../../assets/libs/pourover.js'); 

//module.exports = CacheSet

/* is essentially defines what we consider to be an equal object
 * we could also use underscore's _.isEqual() which does recursive value comparison
 * or == which has some nasty corner cases that === avoids
 */
 // TODO: test identity issues. What happens if we insert three "3"s then remove two of them? What if we do an equivalent test but with objects? what if the objects are literally the same object, and what if they are clones? I use === which performs python's "is" operator but only on two objects; on two primitive types (ints, floats) it does value comparison, and strings seem to exist in the middle: "a" === "a" is true but new String("a") === new String("a") is false ((probably because js does string interning, like python used to do)
// , which might be desirable but also might make problems with double
// ...hm. since my use case plans to eat deletions as json like {-: obj}, obj will *not* be identity-equal with the target
// one solution: switch to _.isEqual(); pro: does what I need; con: slow; potentially unbounded runtime; con: 
// another: design the API--like PourOver--to demand .remove() is given objects from itself (so that we can depend on identity-equals). Then, at the layer that speaks to the WebSocket, do the conversion. This way, the _.isEqual() cruft only has to happen at one layer, and the whole dataflow chain from then on can just use ===
function is(a,b) {
    return a === b;
}

/* helper method which many of the things share
 *
 */
function findIndex(self, e) {
  return self._cache.findIndex(function(g) { return is(g,e); });
}

function add(self, e) {
  self._cache.push(e);
  //TODO: keep sorted
  
  self.trigger("insert", e);
}

function remove(self, e) { 
  // find an element equal to e; note: there might be more than one!
  //i = findIndex(self, e);
  i = self._cache.indexOf(e);
  //i = self._cache.findIndex(function(g) { return _.isEqual(g, e); })
  //TODO: once the caches are kept sorted, use a binary search instead
  
  if(i >= 0) {  //silly bug: "if(i)" is false if i==0, but i==0 is a valid result from indexOf
    self._cache.splice(i, 1);
    self.trigger("delete", e);
  }
}



/* class CacheSet
 *
 * TODO: pick a more descriptive and accurate name
 * TODO: implement .length
 * TODO: implement iterators or element access or something (rather than telling clients to just use ._cache); or, subclass Array and use its built in indexers 
 */
function CacheSet(seed) {
  // if 'new' was not used
  if(! (this instanceof CacheSet)) return new CacheSet(seed);
  
  if(seed === null) { seed = new Array(); }
  this._cache = seed.slice(0); //shallow copy the seed array
  
}
_.extend(CacheSet.prototype, PourOver.Events); // TODO: use Backbone.Events instead, since that's the original source
// alternately, roll these ideas into PourOver, though that will be difficult without breaking PourOver, or at least its zen.



CacheSet.prototype.insert = function(e) {
  add(this, e);
}

CacheSet.prototype.delete = function(e) {
  remove(this, e);
}

CacheSet.prototype.subset = function(pred) {
  return new SubSet(this, pred);
}

/* A SubSet is a filtered slice of a CacheSet
 *  and is itself considered a CacheSet
 * Like a SQL View, a SubSet
 *  but a SubSet is more like a Materialized View (c.f. Postgres; also this hack in MySQL: http://www.fromdual.com/mysql-materialized-views)
  // a SubSet S = SubSet(P, pred) by definition is supposed to maintain the invariant that
  // S = { e for e in P if pred(e) }
  // which equivalently means
  //  \forall `e`: `e in P` and `pred(e)` => `e in S`
*/
function SubSet(parent, pred) {

  // if 'new' was not used
  if(! (this instanceof SubSet)) return new SubSet(parent, pred);
  
  //XXX this would be more elegant if it was a subclass of CacheSet,
  // but I don't know how to do js inheritance properly
  // also, it's not clear if a SubSet should allow direct .insert() and .delete()s
  // TODO: implement .subset(), at least
  
  var self = this;
  
  self._pred = pred;
  self._cache = parent._cache.filter(self._pred);
  
  
  // whoops; .on() runs its callback in the scope of parent.
  // bah. dynamic scopppinggggg!!!
  parent.on("insert", function(e) {
    if(self._pred(e)) {
      add(self, e);
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
      remove(self, e);
    }
  });
}
SubSet.prototype.subset = function(pred) {
  return new SubSet(this, pred);
}
_.extend(SubSet.prototype, PourOver.Events);


// TODO: write a o_cmp which can be used by Array.sort() to order objects; by default, sort() misbehaves on objects

function And(A, B) {
  // if 'new' was not used
  if(! (this instanceof And)) return new And(A, B);
  var self = this;
  
  self._A = A;
  self._B = B;
  
  // Compute (A and B) as [e for e in A if e in B]
  //TODO: exploit sorting to make this faster
  // as written, this is an O(n^2) step
  self._cache = self._A._cache.filter(function(a) {
    return self._B._cache.indexOf(a) != -1;
  });
    
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
  
  // hmm how do I factor this nicely? clever closures, probably... maybe an array (so A_limbo == limbo[0], B_limbo==limbo[1]...; which makes generalizing to n ANDed terms easy) 
  A_limbo = [];
  B_limbo = [];
  
  self._A.on("insert", function(e) {
    // 1) check if e is in B's limbo
    if((i = B_limbo.indexOf(e)) != -1) {
      B_limbo.splice(i, 1);
      add(self, e);
    } else {
    // 2) we "haven't" seen e yet; queue it
      A_limbo.push(e); //TODO: keep sortttted 
    }
  });
  
  self._A.on("delete", function(e) {
    // three paths: if e is in limbo, eating the limbo copy has priority (XXX does this actually make sense? what if A and B both contain x initially, someone adds an x to A but not B, then deletes it from A.,. so in that case, yes, the 
    //          else, if e is in the cache, take it out, because if just one of A or B fails to have e then A AND B has to fail as well
    //    if e isn't in in us at all, ignore
    
    // 1) check if e is in A's limbo
    if((i = A_limbo.indexOf(e)) != -1) {
      A_limbo.splice(i, 1);
      
    } else {
    // 2) we "haven't" seen e yet; queue it
      A_limbo.push(e); //TODO: keep sortttted 
      remove(self, e);
    }
  });
  
  
  self._B.on("insert", function(e) {
    // 1) check if e is in B's limbo
    if((i = A_limbo.indexOf(e)) != -1) {
      A_limbo.splice(i, 1);
      add(self, e);
    } else {
    // 2) we "haven't" seen e yet; queue it
      B_limbo.push(e); //TODO: keep sortttted 
    }
  });
  
  self._B.on("delete", function(e) {
    // three paths: if e is in limbo, eating the limbo copy has priority (XXX does this actually make sense? what if A and B both contain x initially, someone adds an x to A but not B, then deletes it from A.,. so in that case, yes, the 
    //          else, if e is in the cache, take it out, because if just one of A or B fails to have e then A AND B has to fail as well
    //    if e isn't in in us at all, ignore
    
    // 1) check if e is in A's limbo
    if((i = B_limbo.indexOf(e)) != -1) {
      B_limbo.splice(i, 1);
      
    } else {
    // 2) we "haven't" seen e yet; queue it
      B_limbo.push(e); //TODO: keep sortttted 
      remove(self, e);
    }
  });
}
_.extend(And.prototype, PourOver.Events);

// I really need some sort of SortedArray type which has, like, merge() and filter() ops

function Or(A, B) {
  if(! (this instanceof Or)) return new Or(A, B);
  var self = this;
  
  self._A = A;
  self._B = B;
  
  // Compute (A or B) as (A concat B) - (A and B) ((where this set- only removes *one* copy per item))
  //TODO: exploit sorting to make this faster
  // as written, this is an O(n^2) step
  // in fact, it's *even worse* here than in And(), since not only is there the n^2 And step, then there's a tedious n^2 filtering out step
  // this pains me so much
  
  self._cache = self._A._cache.concat(self._B._cache);
  intersection = self._A._cache.filter(function(a) {
    return self._B._cache.indexOf(a) != -1;
  });
  
  //console.debug("naive unioning left", intersection, " duplicated; erasing");
  // remove exactly one copy of each intersection element from the cache    
  for(i = 0; i<intersection.length; i++) {
    e = intersection[i];
    t = self._cache.indexOf(e);
    self._cache.splice(t, 1);
  }
  
}
_.extend(Or.prototype, PourOver.Events);


//TODO: maybe rename "SubSet" "Where"
// and "Columns" "Select" to match SQL

function select(o, fields) {
  //TODO: special case: if fields is not an array, wrap it as a 1-element array
  // or maybe, if fields is *not* an array, operate as pluck() (eschewing creating a container object at all)
  r = {}
  fields.forEach(function(f) {
    r[f] = o[f];
  })
  return r;
}

function Map(parent, m) {
  
  var self = this;
  self._cache = parent._cache.map(m);
  
  parent.on("insert", function(e) {
    e = m(e);
    add(self, e);
  });
  
  parent.on("delete", function(e) { // as in "SubSet", we have an invariant that implies that if we actually see a delete we know necessarily we will perform a delete
    e = m(e);
    remove(self, e);
  });
}
_.extend(Map.prototype, PourOver.Events);


/* Columns: select only the given fields
 *NB: this is a generalized version of _.pluck; should we alias it to match?
 *  
 */ 
function Select(parent, fields) { //TODO: fixit so that you ; or just wrap everything in new-safety code and tell people not to use new with any of the operator-types.
  
  return new Map(parent, function(e) {
    return select(e, fields);
  });
}

/* Scalar: like select() but extracts a single field and does not wrap it in an object; equivalent to underscore's pluck()
 */
function Scalar(parent, field) {
  return new Map(parent, function(e) {
    return e[field];
  });
}

// TODO: implement all the PourOver filters as functions that construct predicates and then return a SubSet
// TODO: implement view disposal

var monsters = [{name: "sphinx", mythology: "greek", eyes: 2, sex: "f", hobbies: ["riddles","sitting","being a wonder"]},
                {name: "hydra", mythology: "greek", eyes: 18, sex: "m", hobbies: ["coiling","terrorizing","growing"]},
                {name: "huldra", mythology: "norse", eyes: 2, sex: "f", hobbies: ["luring","terrorizing"]},
                {name: "cyclops", mythology: "greek", eyes: 1, sex: "m", hobbies: ["staring","terrorizing","sitting"]},
                {name: "fenrir", mythology: "norse", eyes: 2, sex: "m", hobbies: ["growing","god-killing","sitting"]},
                {name: "medusa",  mythology: "greek", eyes: 2, sex: "f", hobbies: ["coiling","staring"]}];
var P = new CacheSet(monsters);

function setstr(s) {
  return Scalar(s, "name")._cache
}


console.info("CACHESET TESTS");
console.log(P);

var norse_monsters = P.subset(function(m) { return m.mythology == "norse" });
var sitting_monsters = P.subset(function(m) { return m.hobbies.indexOf("sitting") != -1 });


console.log("norse_monsters = ", setstr(norse_monsters));
console.log("sitting_monsters = ", setstr(sitting_monsters));


var names = Scalar(P, "name");
console.log("scalar[names] = ", names._cache);

var norse_and_sitting = new And(norse_monsters, sitting_monsters);
console.log("norse AND sitting = ", setstr(norse_and_sitting));

var norse_or_sitting = new Or(norse_monsters, sitting_monsters);
console.log("norse OR sitting = ", setstr(norse_or_sitting));
console.info("");

var norse_view = Select(norse_monsters, ["name", "mythology"]);


var n = {name: "ogabooga", mythology: "norse", eyes: 17, sex: "t", hobbies: ["staring","scaring","sitting","crying"]};


console.log("adding", n);
P.insert(n);
console.log("norse_monsters = ", setstr(norse_monsters));
console.log("norse_view = ", JSON.stringify(norse_view._cache));
console.log("sitting_monsters = ", setstr(sitting_monsters));
console.log("norse AND sitting = ", setstr(norse_and_sitting));
console.log("norse OR sitting = ", setstr(norse_or_sitting));
console.info("");

console.log("adding", n);
P.insert(n);
console.log("norse_monsters = ", setstr(norse_monsters));
console.log("norse_view = ", JSON.stringify(norse_view._cache));
console.log("sitting_monsters = ", setstr(sitting_monsters));
console.log("norse AND sitting = ", setstr(norse_and_sitting));
console.log("norse OR sitting = ", setstr(norse_or_sitting));
console.info("");

console.log("deleting", n);
P.delete(n);

console.log("norse_monsters = ", setstr(norse_monsters));
console.log("norse_view = ", JSON.stringify(norse_view._cache));
console.log("sitting_monsters = ", setstr(sitting_monsters));
console.log("norse AND sitting = ", setstr(norse_and_sitting));
console.log("norse OR sitting = ", setstr(norse_or_sitting));
console.info("");

console.log("deleting", monsters[2]);
P.delete(monsters[2])

console.log("norse_monsters = ", setstr(norse_monsters));
console.log("norse_view = ", JSON.stringify(norse_view._cache));
console.log("sitting_monsters = ", setstr(sitting_monsters));
console.log("norse AND sitting = ", setstr(norse_and_sitting));
console.log("norse OR sitting = ", setstr(norse_or_sitting));	
console.info("");

// all the norse monsters also sit.



/*************************

 sketch of how using sorted sets speeds operations up
 1) .findIndex() can be replaced by .findIndexByBinarySearch()
 2) 

*/