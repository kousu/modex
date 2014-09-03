/* cacheset: a set type based that supports watching state transitions, plus subtypes (subset, and, or, not, distinct, sum, average, ...) supporting dataflow programming.
 *
 * Each type maintains its own cache of the current
 *  Since JS is pointers-everywhere, this is automatically cheap: each element only actually exists once; the storage requirement for each type is only sizeof(js_ptr)*type.length
 
 * The main concept here is the caching, which is a lot like postgres's materialized view; but unlike postgres's implementation, this does edge-triggered processing: it reacts to changes pushed from a source instead of having to poll a source to get a complete new copy.
 *
 *
 *
 */ 
 

// how do I extend the array type?
// do I want to extend the array type?

// CacheSet has an API similar to but only overlapping with--not inheriting--the Array API
// hence, we wrap an array
// Notably, *this type is not actually a set* since it allows multiple copies of a single item
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
  //TODO: once the caches are kept sorted, use a binary search instead
  
  if(i) {
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
  if(! (this instanceof CacheSet)) return new CacheSet();
  
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
  if(! (this instanceof SubSet)) return new SubSet();
  
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


// TODO: implement all the PourOver filters as functions that construct predicates and then return a SubSet
// TODO: implement view disposal

var monsters = [{name: "sphinx", mythology: "greek", eyes: 2, sex: "f", hobbies: ["riddles","sitting","being a wonder"]},
                {name: "hydra", mythology: "greek", eyes: 18, sex: "m", hobbies: ["coiling","terrorizing","growing"]},
                {name: "huldra", mythology: "norse", eyes: 2, sex: "f", hobbies: ["luring","terrorizing"]},
                {name: "cyclops", mythology: "greek", eyes: 1, sex: "m", hobbies: ["staring","terrorizing"]},
                {name: "fenrir", mythology: "norse", eyes: 2, sex: "m", hobbies: ["growing","god-killing"]},
                {name: "medusa",  mythology: "greek", eyes: 2, sex: "f", hobbies: ["coiling","staring"]}];
var P = new CacheSet(monsters);


console.log(P);

var norse_monsters = P.subset(function(m) { return m.mythology == "norse" });

console.log(norse_monsters);







/*************************

 sketch of how using sorted sets speeds operations up
 1) .findIndex() can be replaced by .findIndexByBinarySearch()
 2) 

*/