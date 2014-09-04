/* Table: an in-memory in-javascript database system based around multisets instead of around tables or documents (though any can be translated to the others) and around dataflow programming.
 *
 * The system provides subtypes (Where, and, or, not, columns, distinct, sum, average, min, max, groupBy, ...).
 * with each type maintaining a live cache of its current state, as computed from whatever it is based on (necessarily, then, the supported operations are limited to whatever can be efficiently computed on a stream; limited to roughly what SQL provides, in fact).
 
 * The main concept here is the caching, which is a lot like postgres's materialized view; but unlike postgres's implementation, this does edge-triggered processing: it reacts to changes pushed from a source instead of having to poll a source to get a complete new copy.
 *  Since JS is pointers-everywhere, for the types which compute new sets (Where, and, or, not) are storage-cheap: each element only actually exists once; the storage requirement for each type is only sizeof(js_ptr)*cache.length. The other types do not have this guarantee (in particular, Columns has to create new objects)
 *
 *
 * Dependent types listen to their parent's "insert" and "delete" events; when a parent's cache is updated it fires an event, which causes a chain reaction of dependents to check if they should update their cache, and fire their events.
 *
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
 
 * TODO: clean up terminology; decide between parents,children and dependees,dependents
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
and: function(B) { return new And(this, B); },
or: function(B) { return new Or(this, B); },
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


function And(A, B) {
  var self = this;
  
  self._A = A;
  self._B = B;
  
  // Compute (A and B) as [e for e in A if e in B]
  //TODO: exploit sorting to make this faster
  // as written, this is an O(n^2) step
  Table.call(this, A._cache.filter(function(a) {
    return B._cache.indexOf(a) != -1;
  }));
    
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
      self.insert(e);
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
      self.delete(e);
    }
  });
  
  
  self._B.on("insert", function(e) {
    // 1) check if e is in B's limbo
    if((i = A_limbo.indexOf(e)) != -1) {
      A_limbo.splice(i, 1);
      self.insert(e);
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
      self.delete(e);
    }
  });
}
_.extend(And.prototype, Table.prototype);

// I really need some sort of SortedArray type which has, like, merge() and filter() ops

function Or(A, B) {
  var self = this;
  
  Table.call(this, A._cache.concat(B._cache));
  
  self._A = A;
  self._B = B;
  
  // Compute (A or B) as (A concat B) - (A and B) ((where this set- only removes *one* copy per item))
  //TODO: exploit sorting to make this faster
  // as written, this is an O(n^2) step
  // in fact, it's *even worse* here than in And(), since not only is there the n^2 And step, then there's a tedious n^2 filtering out step
  // this pains me so much
  
  
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
_.extend(Or.prototype, Table.prototype);



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
  
  self.value = parent._cache.length;
  parent.on("insert", function(e) { self.value += 1})
  parent.on("delete", function(e) { self.value -= 1})
}
_.extend(Count.prototype, PourOver.Events);

function Sum(parent) {
  var self=this;
  self.value = parent._cache.reduce(function(prev, e) { return prev + e });
  
  parent.on("insert", function(e) { self.value += e })
  parent.on("delete", function(e) { self.value -= e })
}
_.extend(Sum.prototype, PourOver.Events);


function Mean(parent) {
  var self=this;
  
  var sum = parent.sum()
  var count = parent.count()
  
  function sync(e) { self.value = sum.value / count.value }
  sync()
  
  parent.on("rerender", sync)
}
_.extend(Mean.prototype, PourOver.Events);


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
test_Table();


/*************************

 sketch of how using sorted sets speeds operations up
 1) .findIndex() can be replaced by .findIndexByBinarySearch()
 2) 

*/