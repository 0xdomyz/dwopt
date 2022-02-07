DWOPS - Datawarehouse Operator Python Package
=============================================

Have you ever felt how the interface between the database
and the analytics environment is often unstreamlined?
Does one inefficiently read in millions of rows before doing anything,
or running sql elsewhere and copy some CSVs around,
or writing up some embedded sql in the middle of a python script?

**Dwops** helps by allowing frictionless running of sql codes & scripts,
generation of simple sql query via code,
and making & running of common summary queries & DDL/DML statement
via template functions.
It also automatically logs the sql used along the way.

All together, an Excel-pivot table like experience with large database tables
could be achieved, take a look at the features & the walk through section for
some examples.

.. end-of-readme-intro

Installation
------------

.. code-block:: console

    pip install dwops


Features
--------

* `Run query with less friction using default credentials`_
* `Automate processes with run sql from file, text replacement`_
* `Programatically make and run simple sql query`_
* `Make and run common summary queries from template`_
* `Automatic logging with fully reproducible sql`_


Walk Through
------------

.. highlight:: python

.. |save_default_url| replace:: ``save_default_url``
.. _save_default_url: https://dwops.readthedocs.io/en/latest/urls.html#dwops.save_default_url

.. |make_eng| replace:: ``make_eng``
.. _make_eng: https://dwops.readthedocs.io/en/latest/urls.html#dwops.make_eng

.. |run| replace:: ``run``
.. _run: https://dwops.readthedocs.io/en/latest/db.html#dwops.db._Db.run

.. |qry| replace:: ``qry``
.. _qry: https://dwops.readthedocs.io/en/latest/db.html#dwops.db._Db.qry

.. |valc| replace:: ``valc``
.. _valc: https://dwops.readthedocs.io/en/latest/qry.html#dwops._qry._Qry.valc

.. |dataframe| replace:: ``dataframe``
.. _dataframe: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

.. |pivot| replace:: ``pivot``
.. _pivot: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.pivot.html

.. |logging| replace:: ``logging``
.. _logging: https://docs.python.org/3/library/logging.html#module-logging

.. |INFO| replace:: ``INFO``
.. _INFO: https://docs.python.org/3/howto/logging.html#when-to-use-logging

.. _operator object: https://dwops.readthedocs.io/en/latest/db.html#dwops.db._Db
.. _operator constructors: https://dwops.readthedocs.io/en/latest/db.html#dwops.db._Db
.. _query object: https://dwops.readthedocs.io/en/latest/qry.html#dwops._qry._Qry
.. _clause methods: https://dwops.readthedocs.io/en/latest/api.html
.. _summary methods: https://dwops.readthedocs.io/en/latest/api.html

Run query with less friction using default credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On import, the package gives 3 different `operator object`_
(``pg``, ``lt``, ``oc``, one for each supported database),
with default credentials (Use the |save_default_url|_ function to set up).
This allows running queries from any console window
or python program with few boilerplates.

>>> from dwops import pg
>>> pg.run('select count(1) from test')
    42
>>> pg.qry('test').len()
    42

Alternatively, use the |make_eng|_ function and the `operator constructors`_
(``Pg``, ``Lt``, ``Oc``) to access database.

>>> from dwops import make_eng, Pg
>>> url = "postgresql://scott:tiger@localhost/mydatabase"
>>> pg = Pg(make_eng(url))
>>> pg.run('select count(1) from test')
    42

Automate processes with run sql from file, text replacement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `operator object`_'s |run|_ method also allows running sql stored on a file.
One could then replace parameters via a mapping dictionary,
or simply supply the mappings to the function directly.

.. code-block:: python

    from dwops import oc
    oc.run(pth = "E:/projects/my_sql_script.sql"
        , my_run_date = '2022-01-31'
        , my_label = '20220131'
        , threshold = 10.5)

Above runs the sql stored on ``E:/projects/my_sql_script.sql`` as below:

.. code-block:: sql

    create table monthly_extract_:my_label as
    select * from test
    where 
        date = to_date(':my_run_date','YYYY-MM-DD')
        and measurement > :threshold

Programatically make and run simple sql query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `operator object`_'s |qry|_ method returns the `query object`_.
Use it's `clause methods`_ to make a simple sql query, as it's underlying query.
The underlying query can be run directly, but the main usage is to act as
the preprocessing step of the `summary methods`_ explained in next section.

.. code-block:: python

    from dwops import lt
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

Make and run common summary queries from template
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `operator object`_'s |qry|_ method returns the `query object`_.
Use it's `summary methods`_ to make and run summary queries.
These methods operate on top of the underlying query
as explained in previous section.

Example:

.. code-block:: python

    from dwops import lt #1
    lt.qry('test').where("score > 0.5") \ #2
    .valc('time, cat',"avg(score) avgscore, round(sum(amt)/1e3,2) total") \ #3
    .pivot('time','cat',['n','avgscore','total']) #4

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

Explanation of lines:

#. Get the default sqlite `operator object`_.
#. Make, but do not run, an underlying sub query.
#. Make and run a value counts summary query (|valc|_) with 2 groups,
   custom calcs, with the previous step's underlying query placed
   inside a with clause.
#. Query result comes back to python as a standard pandas |dataframe|_,
   call it's |pivot|_ method.

Automatic logs showing the sql that was ran on line 3:

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

Note the sql shows how the summary query operates on the pre-processing query,
which is placed inside a with block.


Automatic logging with fully reproducible sql
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many of the package's methods are wired through the standard |logging|_ package.

In particular, the |run|_ method emits sql used as |INFO|_ level message.
The relevant logger object has standard naming and is called ``dwops.db``.
Configure the logging package or the logger at the start of application code
for logs.
See the `logging package documentation <https://docs.python.org/3/howto/logging.html#logging-from-multiple-modules>`_
for details.


Example configuration to show logs in console:

.. code-block:: python

    import logging
    logging.basicConfig(level = logging.INFO)

    from dwops import lt
    lt.list_tables()

Alternatively, to avoid logging info messages from other packages:

.. code-block:: python

    import logging
    logging.basicConfig()
    logging.getLogger('dwops.db').setLevel(logging.INFO)


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
    logging.getLogger('dwops.db').setLevel(logging.INFO)

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

.. end-of-readme-usage

Documentation
-------------

* `API <https://dwops.readthedocs.io/en/latest/api.html>`_