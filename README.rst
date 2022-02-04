DWOPS - Datawarehouse Operator Package
======================================

**Dwops** aims to streamline insight generation on large database tables
by giving ability to make and run summary queries efficiently.
It also allows automated running of sql scripts.

The interface between database (lifeblood) and analytics environment (brain)
is often unstreamlined.
Does one inefficiently read in millions of rows just to summaries them
into a dozen numbers, does one run some sql elsewhere and copy
some intermediate csv around, or dose one write up some
unwieldy sql at the start of a python script?

**Dwops** helps by flexibly generate common sql summary query, run it,
automatically log the sql used, then expose the results as pandas dataframe,
ready for other python machineries.
Thus, end-to-end within python, the interface is smooth,
and it gives a Excel-pivot table like experience with large database tables.

.. end-of-readme-intro

Installation
------------

::

    pip install dwops


Features
--------

* `Run query with less friction using default credentials`_
* `Automate processes with run sql from file, text replacement`_
* `Programatically make and run simple sql query`_
* `Make and run common summary queries efficiently and flexibly`_
* `Automatic logging with fully reproducible sql`_


Walk Through
------------

Run query with less friction using default credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On import, the package gives the operator objects with default credentials
(need to be set up prior). 
This allows running queries from any console window
or python program with few boilerplates.

>>> from dwops import pg
>>> pg.run('select count(1) from test')
    42
>>> pg.qry('test').len()
    42

Alternatively, use the make_eng function and the operator constructors
to access database.

>>> from dwops import make_eng, Pg
>>> url = "postgresql://scott:tiger@localhost/mydatabase"
>>> pg = Pg(make_eng(url))
>>> pg.run('select count(1) from test')
    42

Automate processes with run sql from file, text replacement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The operator object's run method also allows running sql stored on a file.
One could then replace parameters via a mapping dictionary,
or simply supply the mappings to the function directly.

>>> from dwops import oc
>>> oc.run(pth = "E:/projects/my_sql_script.sql"
...     , my_run_date = '2022-01-31'
...     , my_label = '20220131'
...     , threshold = 10.5)

Above code runs the sql from the file E:/projects/my_sql_script.sql:

.. code-block:: sql

    create table monthly_extract_:my_label as
    select * from test
    where 
        date = to_date(':my_run_date','YYYY-MM-DD')
        and measurement > :threshold

Programatically make and run simple sql query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The operator object's qry method returns the query object.
Use it's clause methods to make a simple sql query,
as the query object's underlying query.
It can be run directly, but the main usage is to act as
the preprocessing step of the summary query methods.

::

    from dwops import lt
    (   
        lt.qry('test a').select('a.id', 'a.time')
        .case('amt', cond = {'amt < 1000':500,'amt < 2000':1500}, els = 'amt')
        .join('test2 b', 'a.id = b.id')
        .where("score > 0.5", "cat = 'test'")
        .print()#.run()
    )

Above code prints:

.. code-block:: sql

    select a.id,a.time
        ,case
            when amt < 1000 then 500
            when amt < 2000 then 1500
            else amt
        end as amt
    from test a
    left join test2 b
        on a.id = b.id
    where score > 0.5
        and cat = 'test'

Note no ink is saved when comparing to simply write out the sql,
the efficiency gain comes from the summary methods, which follows this step,
instead.

Make and run common summary queries efficiently and flexibly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The operator object's qry method returns the query object.
Use it's summary methods to make and run a summary query.
The summary query operates on top of the underlying query,
which is placed into a with clause, forming a pre-processing step
to the summary query.

Example:

.. code-block:: python
    :linenos:

    from dwops import lt
    lt.qry('test').where("score > 0.5") \
    .valc('time, cat',"avg(score) avgscore, round(sum(amt)/1e3,2) total") \
    .pivot('time','cat',['n','avgscore','total'])

Explanation of lines:

#. Get default sqlite operator object.
#. Make, but do not run, an underlying sub query.
#. Make and run a value counts summary query with 2 groups, custom calcs,
   with the previous step's underlying query placed inside a with clause.
#. Query result comes back to python as a standard pandas dataframe,
   call it's pivot method.

Automatic logs showing the sql that was ran on line 3:

.. code-block:: sql

    2022-01-23 01:08:13,407 [INFO] running:
    with x as (
        select * from test
        where score > 0.5
    )
    select 
        time, cat
        ,count(1) n
        ,avg(score) avgscore, round(sum(amt)/1e3,2) total
    from x
    group by time, cat
    order by n desc
    2022-01-23 01:08:13,413 [INFO] done

Results:

==========  =====  =====  ========  ========  ======  ======
cat           n           avgscore             total
----------  -----  -----  --------  --------  ------  ------
time         test  train    test     train     test   train 
==========  =====  =====  ========  ========  ======  ======
2013-01-02  816.0  847.0  0.746747  0.750452  398.34  417.31
2013-02-02  837.0  858.0  0.748214  0.743094  419.11  447.04
2013-03-02  805.0  860.0  0.756775  0.739017  394.89  422.35
==========  =====  =====  ========  ========  ======  ======

Automatic logging with fully reproducible sql
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many of the package methods are wired through the standard logging package.

In particular, the run method emits sql used as INFO level message.
The relevant logger object has standard naming and is called 'dwops.db'.
Configure the logging package or the logger at the start of application code.

Example configuration to show logs in console:

::

    import logging
    logging.basicConfig(level = logging.INFO)

Alternatively, to avoid logging info messages from other packages:

::

    import logging
    logging.basicConfig()
    logging.getLogger('dwops.db').setLevel(logging.INFO)


Example configuration to show in console and store on file, with timestamps:

::

    import logging
    logging.basicConfig(
        format = "%(asctime)s [%(levelname)s] %(message)s"
        ,handlers=[
            logging.FileHandler("E:/projects/logs.log"),
            logging.StreamHandler()
        ]
    )
    logging.getLogger('dwops.db').setLevel(logging.INFO)

Example logs:

.. code-block:: sql

    2022-01-23 01:08:13,407 [INFO] running:
    with x as (
        select * from test
        where score > 0.5
    )
    select 
        time, cat
        ,count(1) n
        ,avg(score) avgscore, round(sum(amt)/1e3,2) total
    from x
    group by time, cat
    order by n desc
    2022-01-23 01:08:13,413 [INFO] done

.. end-of-readme-usage


Documentation
-------------

* `API`_