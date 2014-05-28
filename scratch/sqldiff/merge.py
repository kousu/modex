#merge.py:
# generalized mergesort

from warnings import *
from functools import *


identity = lambda v: v

def which_min(L, key=identity):
    "R's which.min, ported to python"
    #as it turns out, this is very shortly written by squinting at 'min' and using it sideways:
    # instead of min()ing over values, we min() over the indecies of the values, but we provide a keyfunc which makes the sort be based on the values
    L = list(L) #exhaust iterables so that we can call len(L)
    # min() doesn't make sense on an infinite iterable anyway
    return min(range(len(L)), key=lambda i: key(L[i]))

def which_max(L, key=identity):
    return max(range(len(L)), key=lambda i: key(L[i]))


def _merge(*arrays, key=identity, reversed=False):
    "merge any number of arrays sorted low to high"
    #written as a generator, because generators are ideal for this sort of work: conditional array walking
    # TODO: take a 'reversed' flag like normal sort
    assert all(isinstance(a, list) for a in arrays), "Precondition" #maybe we can weaken this?
    assert all(sorted(a) == a for a in arrays), "Precondition" #FOR DEBUGGING ONLY; calling python's 'sorted' is cheating
    arrays = list(arrays)
    idx = [0] * len(arrays) #each array has a current index
    search = which_max if reversed else which_min
    while arrays:   #loop stops when all arrays are exhausted
        # find the minimum; the next value to yield
        k = search((A[idx[k]] for k,A in enumerate(arrays)), key=key)
        print("array %d has the minimum" % k, ":", arrays[k][idx[k]]) #DEBUG
        yield arrays[k][idx[k]]
        idx[k]+=1
        if(idx[k]) == len(arrays[k]):
            # we've exhausted this one array; drop it
            idx.pop(k)
            arrays.pop(k)

def _merge(*arrays, key=identity, reversed=False):
    "the above, generalized to take iterables instead of lists"
    
    search = which_max if reversed else which_min
    
    arrays = list(arrays) #list-ify so that we can pop dead arrays
    arrays = [iter(A) for A in arrays] #get iterators #TODO: we can do these two in one step
    # excessive code necessary(?) to handle the empty-iterable case
    # this code is partially replicated in the loop; it is ismpler there, however.
    active_arrays = [] #for filtering out; we cannot just do "for A in arrays: if A is bad arrays.pop(A)" because python disallows changing array sizes during iteration
    heads = []     #list of the current heads (in the Lisp sense) of each array
    for A in arrays:
        try:
            heads.append(next(A))
        except StopIteration: #careful: using exceptions as return values
            # iterable had 0 length; ignore it
            continue
        active_arrays.append(A)
        
    arrays = active_arrays; del active_arrays;
    while arrays:
        # find the minimum of the current heads
        k = search(heads, key=key)
        yield heads[k]
        try:
            _last = heads[k]
            heads[k] = next(arrays[k])
            if reversed: 
                # XXX: D.R.Y. this up (there should be some good way to factor things so that.. stuff.. right?
                assert key(_last) >= key(heads[k]), ("Array[%d] is unsorted: key(%d) < key(%d) when asked for descending order." % (k, _last, heads[k]))
            else:
                assert key(_last) <= key(heads[k]), ("Array[%d] is unsorted: key(%d) > key(%d) when asked for ascending order." % (k, _last, heads[k]))
        except StopIteration:
            # array is exhausted; remove it
            heads.pop(k)
            arrays.pop(k)

@wraps(_merge)
def merge(*arrays, **kwargs):
    "quick wrapper: auto-exhaust the generator" 
    return list(_merge(*arrays, **kwargs))


def test_merge_none():
    assert merge() == []

def test_merge_one_none():
    assert merge([]) == []

def test_merge_one():
    assert merge([1,2,3]) == [1,2,3]

def test_merge_many():
    raise NotImplementedError

def test_merge_iterable():
    # test out using iterables,  e.g. islice(count()...))
    raise NotImplementedError

def test_merge_bad():
    A = [1,-4,6]
    B = [1,2,8]
    try:
        assert merge(A, B) == [1,1,2,3,6,8]
    except AssertionError:
        # expecte
        # TODO: change the expected type; more likely this is a ValueError
        pass
    else:
        assert False, "This test *should* have crashed"

def test_merge_typical():
    A = [1,3,6]
    B = [1,2,8]
    assert merge(A, B) == [1,1,2,3,6,8]

def test_merge_reversed():
    A = reversed([1,3,6])
    B = reversed([1,2,8])
    assert merge(A, B, reversed=True) == [8,6,3,2,1,1]

def mergesort(L, key=identity):
    "a great way to exercise merge() is to write mergesort() in terms of it"
    if len(L) <= 1: return L
    half = len(L)//2
    return list(merge(mergesort(L[:half], key=key),
                      mergesort(L[half:], key=key),
                      key=key))


def test_mergesort_typical():
    print("test_mergesort_typical")
    import random
    L = list(range(32))
    random.shuffle(L)
    print("Input:")
    print(L)
    print("Output:")
    qq = mergesort(L)
    print(qq)
    assert qq == list(range(32))
    
try:
    from functools import reduce
except:
    warn("Can't import reduce; are you perhaps running python2?")

def test_mergesort_typical_with_key():
    print("test_mergesort_typical_with_key")
    import random
    L = list(range(32))
    random.shuffle(L)
    print("Input:")
    print(L)
    # sorts the numbers by which remainder block (for the mathies in the audience, which equivalence class) each 
    # but preserves the order otherwise (ie, this is a stable sort), which comes for free from the API of merge()
    print("Output:")
    qq = mergesort(L, key=lambda v: v%3)
    print(qq)
    tt = reduce(lambda a,b: a+b, [[v for v in L if v % 3 == c] for c in range(3)]) #these list comprehensions explicitly partition the input set into each equivalence class, then reduce() globs them back together
    assert qq == tt

if __name__ == '__main__':
    test_merge_none()
    test_merge_typical()
    test_merge_reversed()
    test_merge_one()
    test_merge_one_none()
    test_mergesort_typical()
    test_mergesort_typical_with_key()
    test_merge_bad()