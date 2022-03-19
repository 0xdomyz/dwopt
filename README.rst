DWOPT - Datawarehouse Operator
==============================

Getting insights out of database tables can often be unstreamlined.
Does one read in millions of rows before doing any work on Python,
or run sql elsewhere and use intermediate CSVs,
or write sql strings in a python script?

**Dwopt** is a Python package that streamlines analytics on databases
via pre-built sql templates and a flexible summary query building framework.

The goal is to have, at fingertips,
Excel-pivot-table-like and dataframe-summary-methods-like API
for common analytics on large database tables.
See the Features and the Walk Through section for examples.

.. end-of-readme-intro


Installation
------------

.. code-block:: console

    pip install dwopt


Features
--------

* `Run sql frictionlessly using saved credentials`_
* `Run sql script with text replacement`_
* `Programatically make simple sql query`_
* `Templates: Excel-pivot-table-like API`_
* `Templates: Dataframe-summary-methods-like API`_
* `Templates: DDL/DML statements, metadata queries`_
* `Standard logging with reproducible sql`_


Walk Through
------------

Run sql frictionlessly using saved credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _database operator objects: https://dwopt.readthedocs.io/en/stable/dbo.html#dwopt.dbo._Db
.. |dwopt.db| replace:: ``dwopt.db``
.. _dwopt.db: https://dwopt.readthedocs.io/en/stable/set_up.html#dwopt.db
.. |dwopt.save_url| replace:: ``dwopt.save_url``
.. _dwopt.save_url: https://dwopt.readthedocs.io/en/stable/set_up.html#dwopt.save_url

On import, the package gives ready-to-be-used `database operator objects`_
(e.g. ``pg``, ``lt``, ``oc``, one for each supported database),
with default credentials
(Saved prior by user to the system keyring using the |dwopt.save_url|_ function).

>>> from dwopt import pg
>>> pg.iris()
>>> pg.run('select count(1) from iris')
150

This way, quick analysis can be done from any Python/Console window:

>>> from dwopt import lt
>>> lt.iris()
>>> lt.qry('iris').valc('species', 'avg(petal_length)')
   species   n  avg(petal_length)
0  sicolor  50              4.260
1   setosa  50              1.462
2  rginica  50              5.552

Alternatively, use the database operator object factory function |dwopt.db|_
and the database engine url to access database.

>>> from dwopt import db
>>> d = db("postgresql://dwopt_tester:1234@localhost/dwopt_test")
>>> d.mtcars()
>>> d.run('select count(1) from mtcars')
32


Run sql script with text replacement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. |run| replace:: ``run``
.. _run: https://dwopt.readthedocs.io/en/stable/dbo.html#dwopt.dbo._Db.run

The database operator object's |run|_ method allows running sql stored on a file.
One could then replace ``:`` marked parameters via mappings supplied on runtime.

>>> from dwopt import pg, make_test_tbl
>>> _ = make_test_tbl(pg)
>>> pg.run(pth = "E:/projects/my_sql_script.sql",
...     my_run_date = '2022-03-03',
...     my_label = '20220303',
...     threshold = 5)
   count
0    137

Above runs the sql stored on ``E:/projects/my_sql_script.sql`` as below:

.. code-block:: sql

    drop table if exists monthly_extract_:my_label;
    create table monthly_extract_:my_label as
    select * from test
    where
        date = to_date(':my_run_date','YYYY-MM-DD')
        and score > :threshold;
    select count(1) from monthly_extract_:my_label;


Programatically make simple sql query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _clause methods: https://dwopt.readthedocs.io/en/stable/api.html#clause-methods
.. |qry| replace:: ``qry``
.. _qry: https://dwopt.readthedocs.io/en/stable/dbo.html#dwopt.db._Db.qry
.. _query object: https://dwopt.readthedocs.io/en/stable/qry.html#dwopt._qry._Qry
.. _summary methods: https://dwopt.readthedocs.io/en/stable/api.html#summary-methods

The database operator object's |qry|_ method returns the `query object`_.
Use it's `clause methods`_ to make a simple sql query, as it's underlying query.
The underlying query can be run directly, but the main usage is to act as
the preprocessing step of the `summary methods`_
explained in the subsequent sections.

.. code-block:: python

    from dwopt import lt
    (   
        lt.qry('test a').select('a.id', 'a.time')
        .case('amt', cond = {'amt < 1000':500,'amt < 2000':1500}, els = 'amt')
        .join('test2 b', 'a.id = b.id')
        .where("score > 0.5", "cat = 'test'")
        .print()#.run()
    )

Above prints:

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


Templates: Excel-pivot-table-like API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. |valc| replace:: ``valc``
.. _valc: https://dwopt.readthedocs.io/en/stable/qry.html#dwopt._qry._Qry.valc

A few lines of code specifying minimal information could produce a pivot-table
similiar to what could be achieved in Excel. Difference being
it is the efficient database engine doing the data processing work,
and the flexible python machineries doing the presentation work.

For example:

.. code-block:: python

    from dwopt import lt, make_test_tbl #1
    _ = make_test_tbl(lt)
    (
        lt.qry('test')
        .where('score > 0.5', 'date is not null', 'cat is not null') #2
        .valc('date, cat','avg(score) avgscore, round(sum(amt)/1e3,2) total') #3
        .pivot('date', 'cat') #4
    )

Results:

==========  =====  =====  ========  ========  ======  ======
cat           n           avgscore             total
----------  -----  -----  --------  --------  ------  ------
date         test  train    test     train     test   train 
==========  =====  =====  ========  ========  ======  ======
2013-01-02  816.0  847.0  0.746747  0.750452  398.34  417.31
2013-02-02  837.0  858.0  0.748214  0.743094  419.11  447.04
2013-03-02  805.0  860.0  0.756775  0.739017  394.89  422.35
==========  =====  =====  ========  ========  ======  ======

Explanation of lines:

.. |pivot| replace:: ``pivot``
.. _pivot: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.pivot.html

#. Set up the databse operator object and the test table.
#. Make, but do not run, an underlying query.
#. Make and run a value counts summary query (|valc|_) with 2 groups,
   custom calcs, with the previous step's underlying query placed
   inside a with clause.
#. Query result comes back to python as a pandas dataframe, call it's |pivot|_ method.

Automatic logs showing the sql that was ran on #3:

.. code-block:: sql

    2022-01-23 11:08:13,407 [INFO] running:
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
    2022-01-23 11:08:13,413 [INFO] done


Templates: Dataframe-summary-methods-like API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is possible to mimic some of the dataframe's summary methods,
but implement via sql templates.
Difference being
it is the efficient database engine doing the data processing work,
and the flexible python machineries doing the presentation work.

Use the `query object`_ and it's list of `summary methods`_
to make and run summary queries.
These methods works under the summary query building framework.

For example:

.. code-block:: python

    from dwopt import lt #1
    tbl = lt.qry('test').where("score > 0.5") #2
    tbl.top()   #show top row to understand shape of data
    tbl.head()  #as expected
    tbl.cols()  #as expected
    tbl.len()   #as expected
    tbl.mimx('time')  #min and max of the column
    tbl.dist('time', 'time, cat') #count distinct on the column or columns


Templates: DDL/DML statements, metadata queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _metadata methods: https://dwopt.readthedocs.io/en/stable/api.html#metadata-methods
.. _operation methods: https://dwopt.readthedocs.io/en/stable/api.html#operation-methods

Use the list of `operation methods`_ to make and run some
DDL/DML statements with convenient or enhanced functionalities. Example:

::

    from dwopt import lt
    lt.drop('test')
    lt.drop('test') #alter return instead of raising error if table not exist
    lt.create(
            tbl_nme = 'test'
            ,dtypes = {
                'id':'integer'
                ,'score':'real'
                ,'amt':'integer'
                ,'cat':'text'
                ,'time':'text'
                ,'constraint df_pk':
                    'primary key (id)'
            }
        )
    lt.write(df,'test')
    lt.write_nodup(df,'test',['id']) #remove duplicates before inserting

Use the list of `metadata methods`_ to make and run some useful metadata queries.
Example:

::

    from dwopt import pg
    pg.list_tables() #list all tables
    pg.table_cols('test.test') #examine columns
    pg.table_cons() #list constraints


Standard logging with reproducible sql
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many of the package's methods are wired through the standard
`logging <https://docs.python.org/3/library/logging.html#module-logging>`_
package.

.. |INFO| replace:: ``INFO``
.. _INFO: https://docs.python.org/3/howto/logging.html#when-to-use-logging

In particular, the |run|_ method emits sql used as |INFO|_ level message.
The relevant logger object has standard naming and is called ``dwopt.db``.
Configure the logging package or the logger at the start of application code
for logs.
See the `logging package documentation
<https://docs.python.org/3/howto/logging.html#logging-from-multiple-modules>`_
for details.


Example configuration to show logs in console:

.. code-block:: python

    import logging
    logging.basicConfig(level = logging.INFO)

    from dwopt import lt
    lt.list_tables()

Alternatively, to avoid logging info messages from other packages:

.. code-block:: python

    import logging
    logging.basicConfig()
    logging.getLogger('dwopt.db').setLevel(logging.INFO)


Example configuration to show in console and store on file, with timestamps:

.. code-block:: python

    import logging
    logging.basicConfig(
        format = "%(asctime)s [%(levelname)s] %(message)s"
        ,handlers=[
            logging.FileHandler("E:/projects/logs.log"),
            logging.StreamHandler()
        ]
    )
    logging.getLogger('dwopt.db').setLevel(logging.INFO)

Example logs:

.. code-block:: sql

    2022-01-23 11:08:13,407 [INFO] running:
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
    2022-01-23 11:08:13,413 [INFO] done


Development
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Testing
"""""""""""""""""""""""

Main tests and checks. Only test for sqlite:

.. code-block:: console

    tox

Quick main test:

.. code-block:: console

    pytest

.. |dwopt.make_test_tbl| replace:: ``dwopt.make_test_tbl``
.. _dwopt.make_test_tbl: https://dwopt--10.org.readthedocs.build/en/10/set_up.html#dwopt.make_test_tbl

Testing for sqlite, postgre, oracle.
Set up environment based on |dwopt.make_test_tbl|_ function notes.
Oracle test unimplemented:

.. code-block:: console

    pytest --db="pg"

.. code-block:: console

    pytest --db="pg" --db="oc"

Future
""""""""""""""""""""""""""

* Set up oracle test environment.
* Add more summary templates based on Python pandas, R tidyverse,
  and Excel pivot table functionalities.
* Add more DML/DDL, metadata templates.
* For logging package, consider using
  `loguru <https://pypi.org/project/loguru/>`_.
* For sql syntax, consider using `sqlfluff <https://docs.sqlfluff.com/en/stable/>`_
  style and toolkit.
* For templating internals, consider using
  `jinjasql <https://github.com/sripathikrishnan/jinjasql>`_ toolkit.
* For query building internals, consider using
  `sqlalchemy <https://www.sqlalchemy.org/>`_ toolkit.
* For text replacement directives, consider using
  `jinja2 <https://jinja2docs.readthedocs.io/en/stable/>`_ syntax.

.. end-of-readme-usage


Documentation
-------------

* `API <https://dwopt.readthedocs.io/en/stable/api.html>`_