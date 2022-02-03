DWOPS - Datawarehouse Operator Package
======================================

The **Dwops** python package aims to streamline 
the summary insight generation workflow on large database tables. 
It also allows automated running of sql scripts.

An issue when trying to get insight out of large database tables 
is the dilemma of does one read in the chunky set of data to python
then work with them, or does one use sql queries to get 
a smaller intermediate set than pass those to python?

It could be inefficient and time-consuming to read in millions of rows into ram
just to summaries them into a dozen numbers. 
Using sql to preprocess/pre-summary solves this issue
, but passing the intermediate csv around and saving the sql
somewhere for record keeping doesn't sound very robust or efficient either.

**Dwops** helps by flexibly generate common sql summary query, run it
and log the sql used behind the scene, then expose the results in python
, ready to be pumped to all the other python machineries. 
Thus, the end-to-end process is in one environment,
and it gives a Excel-pivot table like experience with large database tables.

.. end-of-readme-intro

Installation
------------

::

    pip install dwops


Features
--------

* `Run query in console frictionlessly with default credentials`_
* `Automate processes with run from sql file, text replacement`_
* `Make and run common summary queries flexibly and quickly`_
* `Set up default credentials or ask for it on use`_
* `Automatic logging with fully reproducible sql`_


Walk Through
------------

Run query in console frictionlessly with default credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The package pre-instantiates database operator objects, with your default
credential (after they've set up prior). This gives ability to run queries 
from any console window or python program with few boilerplates.

Less friction = faster insights.

.. code-block:: console

    python
    Python 3.9.6 ...
    >>> from dwops import lt
    >>> lt.run('select count(1) from test')
        42


Automate processes with run from sql file, text replacement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The run method allows run sql stored on a file
, automating parameter replacement via a mapping dictionary
, or simply supply the replacements as new parameters.

E:/projects/my_sql_script.sql:

.. code-block:: sql

    create table monthly_extract_:my_label as
    select date,count(1)
    from test
    where 
        date = to_date(':my_run_date','YYYY-MM-DD')
        and measurement > :threshold

In python application:

::

    from dwops import pg
    pg.run(pth = "E:/projects/my_sql_script.sql"
        , my_run_date = '2022-01-31'
        , my_label = '20220131'
        , threshold = 10.5)

Make and run common summary queries flexibly and quickly
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Key concept of package is that a flexible but common summary query is made
up of a simple sql query on a with clause, and a summary query works on the 
sub-query defined by the result of the with clause.

Allow building of select, case when, join, where, group by, having
, order by clauses.

Utlise data manipulation capability of database instead of passing
large raw data to python.
Summary query results as pandas dataframe for access to python toolkit.

Aim to cater for sqlite, postgre and oracle dialects.

::

    from dwops import lt
    lt.qry('test').where("score > 0.5") \
    .valc('time,cat',"avg(score) avgscore,round(sum(amt)/1e3,2) total") \
    .pivot('time','cat',['n','avgscore','total'])


Code is doing 3 things:

1. Generate a query with a where clause to be used as the base table 
   for next stage.
2. Generate and run a summary query grouping 2 columns
   , and with 2 aggregation calcs, results as pandas dataframe.
3. Call pandas dataframe's pivot method to make pivot table from 
   the database generated intermediate results.

By orchastrating the data heavy step to the database
, only minimal amount of data is passed between systems.
This allows quick summary results on large database tables
, programmatically made in one environment.

Logging messages:

.. code-block:: sql

    2022-01-23 01:08:13,407 [INFO] running:
    with x as (
        select * from test
        where score > 0.5
    )
    select 
        time,cat
        ,count(1) n
        ,avg(score) avgscore, round(sum(amt)/1e3,2) total
    from x
    group by time,cat
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

asdf

Set up default credentials or ask for it on use
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The package provides 3 database operator classes for 3 dialects: 
Lt for sqlite, Pg for postgre, Oc for oracle.

Database operator objects are initiated via sqlalchemy engine objects
, which are generated via sqlalchemy engine urls.
Format of url and details, see: 
https://docs.sqlalchemy.org/en/14/core/engines.html

The function dw.make_eng makes engine from url.
The package provides a folder within the package installation directory 
called "urls" to store default urls in txt file,
this is also supported via dw.get_url function to retrieve url 
via file name of the text file.

example usage where "my_pg_url.txt" in the url folder stores user defined url:

::

    from dw import Pg,make_eng,get_url
    pg = Pg(make_eng(get_url('my_pg_url')))


example usage where url is provided:

::

    from dw import Pg,make_eng
    url = "postgresql://scott:tiger@localhost/mydatabase"
    pg = Pg(make_eng(url))

For convenience, the package pre-instantiate 3 database operator objects: 
lt, pg, and oc.
The sqlite operator, lt, is coded to connect to the in-memory databse 
for illustration purpose.
The postgre operator, pg, uses the placeholder url txt file 
called "psql_default".
The oracle operator, oc, uses the placeholder url txt file called "oc_default".

User should consider to rewrite these implementations on 
the \_\_init\_\_.py file to cater to their own password management strategy.


.. end-of-readme-usage


Documentation
-------------

* `API`_