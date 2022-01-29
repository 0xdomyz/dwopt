DWOPS - Datawarehouse Operator Package
======================================

Features
--------

Run sql statement in python
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Support argument passing, text replacement, read sql from file.
Provide functions for running DDL/DML on database table and pandas dataframe.
Logging via the logging package.

Programmatically generate simple sql query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Allow building of select, case when, join, where, group by, having
, order by clauses.

Generate and run summary queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Utlise data manipulation capability of database instead of passing
large raw data to python.
Summary query results as pandas dataframe for access to python toolkit.

Aim to cater for sqlite, postgre and oracle dialects.

.. end-of-readme-intro


Installation
------------

::

    pip install dwops


Usage
-----

Excel pivot table - like experience on database tables
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

::

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

Setting up default and/or specific database connections
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

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

    from dwops import Pg,make_eng,get_url
    pg = Pg(make_eng(get_url('my_pg_url')))


example usage where url is provided:

::

    from dwops import Pg,make_eng
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

links
