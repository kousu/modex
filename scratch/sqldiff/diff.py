#!/usr/bin/env python3
# diff.py 
"""
sqldiff algorithm


inputs: two tables with the same columns, fully sorted
{nb: fully sorted means first sorted on the first column, then by the second to break ties, then by the third.. all the way down to the end; sql's ORDER BY clause performs this for us}
(other nb: the particular sort uses doesn't matter, so long as it is consistent from run to run)

Call the first table L and the second N

start rolling down the tables, searching for identical rows.
Because of the sort, we know that [....] 
 e.g. if the left says [a, 4, q] and the right says [a, 6, g] 
  then you know to advance the left pointer


output: whatever remains in L is the "deletes" list and what is in N is "adds" 


"""



from itertools import *


def drop(L, idx):
    "drop elements given a sorted list of indecies"
    "equivalent to L[-idx] in R or L[L[i] for i in itertools.filterfalse(lambda i: i in idx, range(len(L))] or to some similar constructions in numpy"
    "but this runs in linear time because of the sorted condition"
    "and operates on any iterable"
    #TODO: pull to a util module
    #TODO: it seems odd that this isn't in itertools; itertools has dropwhile() but that's only a fancy (yet only one half) sort of slice.
    idx = iter(idx)
    d = None
    L = iter(L)
    for i, l in enumerate(L): #..this really wants to be a do-while..; instead, encode first-state as "None" and hope that idx contains no Nones
        if d is None or i == d: #skip
            if d is None: yield l #hack around the do-while case
            try:
                d = next(idx)
                if not isinstance(d, int):
                    raise TypeError("idx must be a list of integers")
            except StopIteration:
                # when it runs out, exhaust the iterable immediately
                for l in L: #note, this depends on iter() being idempotent when called on things that are already iter()s!
                    yield l
                break
        else:
            yield l
            
def test_drop():
    L = range(59)
    LL = drop(L, [3, 44, 66])
    LL = list(LL)
    TT = list(range(59))
    TT.pop(44); TT.pop(3) #don't pop 66 because that's (purposely) not a valid index (but drop() should just ignore such things)
    assert LL == TT
    
def negativeidx(idx):
    return [-i for i in idx]

def fancyslice(L, idx): #implements indexing-by-set like R and scipy
    # TODO: support negative indexing to mean "drop it"
    return [l for i, l in enumerate(L) if i in idx]

    # a line that is an /update/ is.. marked on the update list and stepped over, I think?
    # because  you've figured out what is up with both rows and dealt with them
    # an
    # but if the rows are totally different
    #  the rows
    # ..my algorithm is going to miss some updates as written?? because how can it possibly detect if
    # like, if I claim that ""
    # instead what I have to do is iterate over all elements and find which ones are the same
    # if ALL are different, that's
    # but how does sorting come into this then?
    # i still need to like, walk
    # sorting ensures that the differences happen at the end
    #  ..h
    # [3]
    # 
    """
    FLAG = None #instructs magic-gen whether to step left, right or eat
    def magicgenerator():
        # yields the current lines to consider
        # carefully handling the cases where either gen might run out
        while True:
    
    def cmp(l, r):
        # compare two rows
        # decide if the two rows are:
        # equal
        # r is an update to l
        # we could do this with the built-in < and == ops, but to check for updatedness we also want to know *where* the elements
        assert len(l) == len(r)
        # TODO: also assert that the types of all elements match up
      
    for left, right in magicgenerator():
       """ 
# this feels sort of like a mergesort, doesn't it?


def coopy(diff):
    "translate a table diff from internal format to [coopy](http://dataprotocols.org/tabular-diff-format/) format"
    raise NotImplemented

# some visualizations I want:
#1) what is the bipartition in diffu like
#0) in applydiff(), what is the order of d? Do true updates tend to be adjacent (because if so, this changes the approach I would take)
#--> first need t

def diff(L, R):
    """
    this prototype operates on iterators of lists: the format you get by reading a csv
    XXX bug: it actually is currently hardcoded to require knowing the length of the tables in advance
    
    (perhaps with suitable abstraction into iterables, the same code could run identically whether L and R are SQLAlchemy resultssets, csv.readers, or hard-coded lists)
    
    L is "LOCAL" aka the Left input
    R is "REMOTE" aka the Right input
    """
    assert L == sorted(L)
    assert R == sorted(R)
    len_L, len_R = len(L), len(R)
    L, R = iter(L), iter(R)
    Kept_L, Kept_R = [], []
    i_L, i_R = 0, 0
    # start walking the lists
    # this algorithm is not supposed to be, youy know, elegant. i'll get to that.
    L_row = next(L)
    R_row = next(R)
    ops = -1 #DEBUG; count the number of steps the algorithm takes
    try:
        while True:
            ops+=1
            print()
            print(ops)
            print("L[%d] = %s" % (i_L, L_row))
            print("R[%d] = %s" % (i_R, R_row))
            # ending conditions are complicated 
            if L_row == R_row:
                # if terms are equal, then erase them both by stepping the index
                print("samesies!")
                L_row = next(L)
                R_row = next(R)
                i_L += 1
                i_R += 1
            else:
                # however if they are different, we need to figure out how different
                print("differences!")
                # we.. step the left if ..
                # TODO: make the common case be 'updated', and 'deleted' be the degenerate update case
                #
                if L_row < R_row:
                    print("stepping left")
                    Kept_L += [(i_L, L_row)]
                    i_L += 1
                    L_row= next(L)
                else:
                    #and the right otherwise..
                    print("stepping right")
                    Kept_R += [(i_R, R_row)]
                    i_R += 1
                    R_row= next(R)
    except StopIteration:
        # finish up any leftovers
        Kept_L += islice(L, 0, None)
        Kept_R += islice(R, 0, None)

    print(Kept_L)
    print(Kept_R)        
    #return fancyslice(L, Kept_L), fancyslice(R, Kept_R)
    return Kept_L, Kept_R


def diff(L, R):
    "prev impl has bugs; this is one with more preconditions in order to shake them out oin th wash"
    l, r = 0, 0 #left iterator; right iterator
    deletions = []
    additions = []
    while True: #this loop is essentially the merge() part of mergesort(), but with the inner operations changed
                #curious; does that mean it can be factored?
        if l == len(L):
            # patch remaining Rs onto "additions"
            print("break left") #DEBUG
            additions += R[r:]
            break
        
        if r == len(R):
            print("break right") #DEBUG
            # ran out of Rs
            # patch remaining Ls onto "deletions"
            #deletions += L[l:]
            deletions += list(range(l,len(L)))
            break
        
        #print("L[%d] = %s" % (l, L[l]), "R[%d] = %s" % (r, R[r]), "out of %s" % ((len(L),len(R)),))  #DEBUG
        if L[l] == R[r]:
            l += 1
            r += 1
        elif L[l] < R[r]:
            #deletions += [L[l]]
            deletions += [l] #for deletions, we want to know the row ids to delete
            l += 1
        else:
            additions += [R[r]]
            r += 1
        
    return deletions, additions #TODO: swap this API

def diffu(L, R):
    "diff() which returns (additions, updates, deletions)"
    # an "update" as far as we care is a row which shares any of its elements
    # it is typed as this tuple: (id, {columnid: newvalue, ...})
    #  where id is the index of the row in the L table (not in the R table!)
    # a single update takes the place of an addition plus a deletion in diff()
    # we have as a precondition that L and R are sorted from the left column onwards (equivalent to the result of calling sorted() on them)
    # so detecting a change like (a,b,c) -> (a,b,d) is easy
    # detecting (a,b,c) -> (z,b,c) is harder (impossible?)
    #
    deletions, additions = diff(L,R) 
    # now, scan the deletions and additions list for pairs that might represent updates
    # in fact, we don't care if they represent
    # (and we want to minimize what is sent over the wire, so we prefer
    #  i. larger updates; eg if we have two candidate rows and one updates 5 and one updates 2 ,  we prefer the one that only updates 2 (which is more likely the true change anyway)
    #   but this might(??) be in conflict with
    # ii. the data to be sent to be minimal; so, we prefer to choose ; 
    #   I can't immediately think of a way to do such minimization without lots of clever backtracking , so I'm going to assume that it might as well be NP hard.
    # ((but this isn't always true!! an addition that has a large number
    #
    # This is expensive (O(n^2 * p) where n is the number of rows and p is the number of columns
    # but i don't see a real alternative to brute force.
    # It's also even more expensive than that, currently, since it's verrrrrrrrrry python
    # maybe the sorting gives us a few small savings that we can exploit? like 
    
    def overlap(r1, r2):
        assert len(r1) == len(r2)
        # the overlap is the number of columns the two rows have identical
        return sum(cc in r2 for cc in r1)
        
    
    updates = []
    d = 0 #index into deletions; every update is made of a pair (delete, add); it doesn't really matter which we outer-search (in fact, except for datastructure details, the algorithm should be symmetric about switching the two) starts from a deletion; note that we use manual indexing here because we're using .pop() inside the loop
    while d < len(deletions):
        D = L[deletions[d]] #the row we are searching for something similar to
        candidates = []
        # count how many overlaps there are in each
        # TODO: write iteratively so we don't uselessly store the entire set and *then* sort
        dupes = [(a,overlap(D, A)) for a,A in enumerate(additions)]
        dupes.sort(key = lambda v: v[0])
        dupes = [d for d in dupes if d[1] > 0] #drop 0 non-overlappings
        print("diffu dupes")
        print(dupes)
        
        # our candidate is whatever has the largest overlap with D, if any row with overlap exists
        # TODO: amongst all can, try to choose one that means sending the least data
        # TODO: instead of writing this functionally, write is iteratively because we're wasting a lottt of memory (and as a side effect ,time) as written
        # (?)
        # TODO: for clarity, write it to minimize difference instead of maximize overlap,
        candidate = dupes[:1]
        
        
        if not candidate:
            print("could not update '%s'; leaving deleted." % (D,))
            d+=1
            continue
        else:
            a = candidate[0][0] #wheeee magic numbers
            # ^this step tosses away our information about the overlap
            # but it was only a count so that's no good anyhow...
            # we can clean this all up and make it spiffy once it works
            print(a)
            candidate = additions[a]
            print(candidate)
            # figure out the overlap (again) and this time record which columns
            assert len(candidate) == len(D)
            difference = [c for c in range(len(D)) if D[c] != candidate[c]]
            update = dict((c, candidate[c]) for c in difference)
            print(update)
            # compress the chosen pair (delete, add) --> (update,)
            updates.append((deletions[d], update))
            deletions.pop(d)
            additions.pop(a)
    
    return deletions, updates, additions
    
    
    
def diffu(L, R):
    "diffu implemented using bipartite matching"
    "this version finds the minimal diff (whereas before we found a diff, but not necessarily the minimal one)"
    "TODO: support different columns; this requires enforcing that L and R are not just lists, but Table objects which have a header we can look at"
    # L and R are two sets
    # first, scan L and R to find the overlap; this includes redundant information
    # second, use an optimization algorithm to choose the set of updates which is minimal (in the sense that the number of changed entries is least)
    #  this design is also nice because, once it works, it will be trivial to change the weighting to account for the serialized size of the data (e.g. it's cheaper to send {"count": 2, "count"} than {"flux": "a long string made of blue cheese"} even tho the former has more updates
    
    #   we have four sets: {deletions, additions, updates, unchanged}
    #   we wish to output: {deletions, additions, updates}
    # {deletions, additions} is the *non*overlap 
    # the overlap {unchanged, updates}
    #  --> UNCHANGED is rows which are FULL OVERLAPS
    # 
    
    # the  bipartite graph is this: the L rows are vertices and the R rows are vertices; edges are mappings "this L row corresponds to this R row" and they have weights attached, which is the size of the difference between the rows (smaller is better)
    # initially, we start with the complete bipartite graph: every vertex
    # we prefilter it and drop;
    #  
    #   we find exact matches ('unchanged') 
    #       
    
    # some edges have 0 weight (bc they are)
    # the optimization will choose them
    # (but we could probably pre-drop them; we don't care which maps to which, only that something maps to something; so if we find a zero edge)
    #
    # --> remember that our vertices
    
    #p = ncol(L) == ncol(R) #really want to say this
    p = len(L[0]) # but we're using Lists so we say this
    print(100*"p is ", p)
    
    def rowdiff(l, r):
        assert len(l) == p and len(r) == p, "every row must be the same length; we do not handle changing columns (and when we do, we will preprocess to only consider the overlapping columns"
        
        return [i for i in range(p) if l[i] != r[i]]
        #return {i: r[i] for i in range(len(r)) if l[i] != l[r]} #maybe this dictionary instead?
    
    updates = [] # a table of edges: (L, R, weight)
    
    # TODO: we can make the optimization that once we find a 0-delta *we have found an unchanged row* (it might not be the same mapping the user made; e.g. maybe they edited two rows such that one ended up the same as a later row in the set; but and then that later row wi
    # TODO: make use of the sorting;  
    
    for l, l_row in enumerate(L):
        for r, r_row in enumerate(R):
            print("considering pair", l,r)
            delta = rowdiff(l_row,r_row)
            if len(delta) not in [0, p]:
                updates.append((l, r, len(delta))) #record the edge and its weight
            elif len(delta) == p:
                # pass
                pass #?@#?#?#?#
    
    import pydot
    M = max(len(L), len(R)) #graphviz is too good; it's too picky about; but if I balance the bipartitions then make a bunch of hidden nodes, maybe it will be alright
    G = pydot.Graph("diffu() bipartition of the rows", rankdir="UB", splines=False)
    GG = pydot.Cluster("Left", label="Local Set")
    G.add_subgraph(GG)
    for o in range(M):
        N = pydot.Node("L%d" % o, color="blue")
        if o >= len(L): N.set_style("invis")
        GG.add_node(N)
        
        # enforce ordering <http://graphviz.996277.n3.nabble.com/Sorting-node-in-the-same-rank-td2082.html>
        if o < len(L)-1: GG.add_edge(pydot.Edge("L%d" % o, "L%d" % (o+1), style="invis"))
        
    GG = pydot.Cluster("Right", label="Remote Set")
    G.add_subgraph(GG)
    for o in range(M):
        N = pydot.Node("R%d" % o, color="red")
        if o >= len(R): N.set_style("invis")
        GG.add_node(N)
        # enforce ordering <http://graphviz.996277.n3.nabble.com/Sorting-node-in-the-same-rank-td2082.html>
        if o < len(R)-1: GG.add_edge(pydot.Edge("R%d" % o, "R%d" % (o+1), style="invis"))
        # at R26.. is 26<len(R) ; presumabl
    for e in updates:
        G.add_edge(pydot.Edge("L%d" % e[0], "R%d" % e[1], label=str(e[2])))
    print(G.to_string())
    with open("viz.dot", "w") as out:
        out.write(G.to_string())
    
    print("updates is")
    print(updates)
    # want to say unique(updates[, "L"]) but we're using Lists so we say..
    deletions = set(range(len(L))) - set(u[0] for u in updates)
    additions = set(range(len(R))) - set(u[1] for u in updates)
    # ^ this is wrong. for one thing, the set of rows which might be updated is a superset of the set of rows which are;
    # we don't know until
    import IPython; IPython.embed()
    print(additions)
    print(deletions)
    
    assert len(update_L) == len(update_R), "After filtering, there should be exactly one local row left to match to each remote row."
    # ^ dis aint true; why isn't it true?
    
    # we are searching for an optimal case of what is called a Matching: a set of edges (the updates) without common vertices (ie every row is touched by only one update). In our case, the weighted bipartite case there is a specific name: the [Assignment Problem](https://en.wikipedia.org/wiki/Assignment_problem)) and two algorithms. We use the newer (and faster) of the two
    # We can solve this by integer linear-programming in a relatively straightforward way:
    # ((except note that we have to solve the real-valued relaxation first, and only after coerce to integer))
    # We have a set of edges each with a cost value c_n (the size of the diff the edge represents)
    # the coefficients we solve for are a different set of weights e_n, which specify how "included" that edge is (0 = not included, 0.6 = 60% included)
    
    # our objective function is
    # min sum(e_n * c_n)
    # ...
    
    # and our constraints are defined such that each vertex has at most one edge going to it:
    # Each n corresponds to a pair (i,j) -- the edge; 
    # For each vertex i in the L set, we add a constraint across all edges touching it so that at most one of them can be left at the end
    # 0 < e_{i,a} + e_{i,b} + ... + e_{i, _last_i_vertex} <= 1
    # and similarly for each vertex j in the R set
    # 0 < e_{a,j} + e_{b,j} + ... + e_{_last_j_vertex, j} <= 1
    # ...
    
    # For us, we just have to initialize these constraints, and then call our black-box LP solver:
    # ...
    
    # now we read of the solution
    # ...
    
    # isomorphtastic! isn't math great?

def humanized_diff(L, delta):
    """
    Convert the given diff to a textual diff, with +s and -s, similar to regular diff(1)'s "-u" mode
    
    This only works on V1 deltas (i.e. (deletions, additions) where deletions is a list of indecies and additions is a list of rows)
    as produced by diff();
    In the future it will be modified to handle diffu() output, as diffu() starts working...
    """
    #    XXX this code overlaps with similar goals in nearby subroutines general
    #  (e.g. 'hunkifying' is an operation that seems like it requires constructing the merge() anyhow)
    #   ..I'm sure the relationships will become clearer as this code evolves.
    from merge import merge
    D, A = delta #
    
    U, D = drop(L, D), [L[i] for i in D] #map the local table L and the list of deletions
    # into a list of unchanged lines and the verbose list of deletions (if this was math,
    # this would be something like finding the decomposition L = U + D)
    
    def prefix(v, S):
        return [(v,s) for s in S]
    
    # mark each set with its type, so that when we merge the lines we can know what to print later
    A = prefix("+", A)
    D = prefix("-", D)
    U = prefix(" ", U)
    
    R = merge(A,D,U, key=lambda v: v[1]) #merge, sorting by the values (v[1] is the value, v[0] is the type marker)
    # TODO: scan R for large contiguous blocks of Us and omit them (well, actually, replace them with a fourh type of marker ("@", n) which contains the index of the next line), like normal diff does, instead only includding Us that are within a couple of rows of a change
    # TODO: is it maybe more efficient to not construct those Us in the first place? is there any way for us to do that?
    # ...probably not. not that AND track the indecies too
    #^ this is probably best implemented by doing as normal diff does and instead scanning for hunks of changes, and then including adjacent lines (which, by construction, will be guaranteed); record the hunks as a "list of hunks" [(start, end), ...]
    # which we can then map
    # TODO: merge nearby +s and -s into blocks, like normal diff does
    R = ["%s%s" % (r[0], r[1]) for r in R]
    return str.join("\n", R)

    
def applydiff(Table, additions, deletions):
    """
    preconditions (not enforced), : L is a sorted list of rows;
     additions is a sorted list of rows
     deletions is a list of indexes
    """
    #assert Table == sorted(Table) #DISABLING TO MAKE APPLYDIFFU() WORK
    assert additions == sorted(additions)
    assert all(type(row) is list for row in Table)
    assert all(type(id) is int for id in deletions)
    assert all(0<=id<len(Table) for id in deletions)
    
    # first, remove the deletions
    Table = [l for i, l in enumerate(Table) if i not in deletions]
    
    # add the additions via a mergesort (is mergesort in the stdlib somewhere?)
    # mergesort is: 1) recursively partition the array until you have sorted arrays, ie. single elements (or, if you're more clever, you can use something else when you get to < 10 elts since the recursion overhead outweighs brute force there, usually) 2) as you go up the stack, use merge()
    
    def merge(L, R): #TODO: take *seqs instead of l1, l2 and support an arbitrary number of list TODO: support generic iterables (iter(), StopIteration, etc) 
        # TODO: find this in the stdlib
        l = r = 0
        while l < len(L) or r < len(R):
            #print(l,r,len(L),len(R)) #DEBUG
            if l == len(L):
                # patch remaining Rs
                yield R[r]
                r += 1
                continue
            if r == len(R):
                # patch remaining Rs
                yield L[l]
                l += 1
                continue
            
            if L[l] == R[r]:
                yield L[l]
                yield R[r]
                l += 1
                r += 1
            elif L[l] < R[r]:
                yield L[l]
                l += 1
            else:
                yield R[r]
                r += 1
    
    return list(merge(Table, additions))

def applydiffu(L, diff):
    L = L[:] #quickly shallow copy just in case
    
    deletions, updates, additions = diff
    # apply updates first, since they're easy and don't change the order
    for u, U in updates:
        for c in U:
            L[u][c] = U[c]
            
    L = applydiff(L, additions, deletions)
    L.sort() #jussst for good measure, since the diff process doesn't guarantee leaving things sorted properlike
    return(L)

def read_table(fname):
    """
    load a csv , and sort it to serve the preconditions
    drops the header row!
    Inefficient!
    """    
    import csv
    return sorted(list(csv.reader(open(fname,"r")))[1:])

# TODO: rewrite

def test_empty_left():
    L = []
    R = [[-13, 'h', '55'],
         [-9, 'a', '3'],
         [1, 'a', 'q'],]
         
    deletions, additions = diff(L, R)

    print("additions")
    for a in additions:
        print(a)
    print("deletions")
    for d in deletions:
        print(d)
    
    assert applydiff(L, additions, deletions) == R

def test_empty_right():
    L = [[-13, 'h', '55'],
         [-9, 'a', '3'],
         [1, 'a', 'q'],]    
    R = []
         
    deletions, additions = diff(L, R)

    print("additions")
    for a in additions:
        print(a)
    print("deletions")
    for d in deletions:
        print(d)
    assert applydiff(L, additions, deletions) == R

# i need tests...

# one sort of test: construct a list of additions and deletions and updates
# apply them
# then run diff on the results
# and compare the result of diffs

# cases to check for:
#  - a typical case
#  - duplicate rows should be considered distinct!!
#  - what happens if one of the earlier columns is updated; is it detected as an update?
#  - what happens if a mixture of the 
#  - what happens if only one column is updated
#  - having lists of overlap [a, ...], [a, ....]
#  - an empty left (---> ejntirely adds)
#  - an empty right (---> entirely deletes)
#  -

# these next two tests are not very interesting if we just have
#  additions/deletions, but are very interesting if there's updates
# will the algorithm detect updates to early columns?

def test_later_column(): 
    L = [[1,2,3,4]]
    R = [[1,2,7,4]]
    
    deletions, additions = diff(L, R)

    print("additions")
    for a in additions:
        print(a)
    print("deletions")
    for d in deletions:
        print(d)
    
    assert applydiff(L, additions, deletions) == R

def test_early_column():
    L = [[1,2,3,4]]
    R = [[7,2,3,4]]
    
    deletions, additions = diff(L, R)

    print("additions")
    for a in additions:
        print(a)
    print("deletions")
    for d in deletions:
        print(d)
    assert applydiff(L, additions, deletions) == R


def test_typical(f1 = "activity_counts.shuf1.csv", f2="activity_counts.shuf2.csv"):
    L, R = [read_table(f) for f in (f1, f2)]
    
    delta = diff(L, R)
    deletions, additions = delta

    print("additions")
    for a in additions:
        print(a)
    print("deletions")
    for d in deletions:
        print(d)
        
    print("And the diff, in human readable format")
    print(humanized_diff(L, delta))
    assert applydiff(L, additions, deletions) == R

def test_typical_u(f1 = "activity_counts.shuf1.csv", f2="activity_counts.shuf2.csv"):
    L, R = [read_table(f) for f in (f1, f2)]
    
    delta = diffu(L, R)

    print("additions")
    for a in delta[2]:
        print(a)
    print("updates")
    for u in delta[1]:
        print(u)
    print("deletions")
    for d in delta[0]:
        print(d)
    
    print("And the diff, in human readable format")
    print(humanized_diff(L, delta))
    
    assert applydiffu(L, delta) == R


def tests():
    for name, test in list(globals().items()):
        if not name.startswith("test_"): continue
        
        print("----------------")
        print("TEST:", name)
        test()
        print("----------------")
        print()

test_early_column()

if __name__ == '__main__':
    import sys
    if sys.argv[1:]:
        test_typical(sys.argv[1], sys.argv[2])
        print("test passed")
    else:
        tests()
