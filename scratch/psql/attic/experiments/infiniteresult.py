
-- pl/python can return iterators, and postgres, like most SQLs, has the concept of fetching through cursors (basically an iterator that has the option of going backwards)
-- so returning an infinite iterator should 


DROP FUNCTION IF EXISTS fib();  --use DROP FUNCTION instead of CREATE OR REPLACE because postgres doesn't allow changing a function's API with "OR REPLACE" and I need to experiment with that here
CREATE FUNCTION fib() RETURNS SETOF numeric AS
$$
  """
    returns: a generator yielding the fibonacci sequence
             this rapidly goes into bigint territory; postgres can handle bigints with its NUMERIC type.
  """
  def gfib(): #a generator of the fibonacci numbers
    a,b = 1,1
    while True:
      a,b = b, a+b
      print("fib(): ", a)
      yield a
  
  return gfib()
$$ LANGUAGE plpython2u;


-- from http://www.postgresql.org/docs/9.3/static/plpgsql-cursors.html
DROP TABLE IF EXISTS test;
CREATE TABLE test (col text);
INSERT INTO test VALUES ('123');

DROP FUNCTION IF EXISTS reffunc();
CREATE OR REPLACE FUNCTION reffunc() RETURNS refcursor AS '
DECLARE
    c1 CURSOR FOR SELECT col FROM test;
BEGIN
    RETURN c1;
END;
' LANGUAGE plpgsql;

-- weird... you are not allowed to create cursors except inside of procedural functions
--  however you are allowed, once they are made and named ((does that mean all cursors are global vars?)) to use FETCH on them

BEGIN;
SELECT reffunc() INTO hello;
FETCH ALL IN hello;
COMMIT;


-- oh! I don't need cursors at all. RETURNS SETOF must implictly cause a cursor to be constructed, probably per-session; so it doesn't rerun 
select fib() limit 10; 
select fib() limit 3; 
select fib() limit 5; 
select fib() limit 20;
select fib() limit 7;  

-- new question: when *does* a new cursor get constructed? presumably when the function dies?

CREATE OR REPLACE FUNCTION count_to(n int) RETURNS SETOF int AS
$$
  def gcount():
    print("<counting> to %d" % n)
    for i in xrange(n):
      yield i
    print("</counting>")
  return gcount()
$$ LANGUAGE plpython2u;

-- <counting> happens here
select count_to(66) limit 7;
-- then the same is repeated
select count_to(66) limit 7;
select count_to(66) limit 10;
select count_to(66) limit 3;
select count_to(66) limit 8;
select count_to(66) limit 5;
select count_to(66) limit 17;
select count_to(66) limit 5;
select count_to(66) limit 8;
-- answer: ^ the limit 8 line only returns 4 results, then </counting> happens,
-- and this next line makes <counting> happen
select count_to(66) limit 3;

-- Interesting. So SETOF + generators behave in a logical but unusual way; *sometimes* the call reruns. This is sort of like the trickiness of referential transparency in functional programming.
-- 
select 'Does postgres distinguish different arguments as different calls?';

select count_to(10) limit 3;

select 'No, it does not. The above line keeps pumping the currently existing generator.';

-- So in general, to pump a stored procedure generator, you'd do
-- while True: pg.execute("select gen() limit 1");


select 'Another discovery: select * from gen() and select gen() behave differently: the former exhausts the generator first.';
select count_to(66) limit 3;
select 'You need to look at the server stdout to see this; try both forms and note that the former causes </counting> to come out and the latter does not, even though the client-side gets identical output both ways.';

select 'Hence, this next line first exhausts the currently existing generator (if you have left the previous line in the latter form)';
select * from count_to(9) limit 7;
select 'Then these next two do identical operations, completely (and wastefully) exhausting the generator both times';
select * from count_to(9) limit 7;
select * from count_to(9) limit 7;