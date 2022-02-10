DWOPT - Datawarehouse Operator
==============================

**Dwopt** is a Python package that attempts to streamline
the insight generation process when working with database tables.

The interface between databases and python can often be unstreamlined.
Does one read in millions of rows before doing anything,
or run sql elsewhere and copy some CSVs around,
or write up some embedded sql in the middle of a python script?

**Dwopt** allows frictionless running of sql codes/scripts,
default credentials on the system keyring,
simple query generation via code,
making & running common summary queries, DDL, DML statement
via pre-built templates. It also logs the sql used along the way.

All together, for common analytics,
an Excel-pivot table or dataframe summary function like experience
on large database tables could be effortlessly achieved,
see examples in the Features & the Walk Through section.

.. end-of-readme-intro

Installation
------------

.. code-block:: console

    pip install dwopt


Features
--------

* `Run query with less friction using default credentials`_
* `Automate processes with run sql from file, text replacement`_
* `Programatically make and run simple query`_
* `Sql template: Excel-pivot table experience`_
* `Sql template: Dataframe summary function experience`_
* `Sql template: DDL/DML statement, metadata queries`_
* `Automatic logging with fully reproducible sql`_


Walk Through
------------

.. highlight:: python

.. |save_url| replace:: ``save_url``
.. _save_url: https://dwopt.readthedocs.io/en/latest/urls.html#dwopt.save_url

.. |make_eng| replace:: ``make_eng``
.. _make_eng: https://dwopt.readthedocs.io/en/latest/urls.html#dwopt.make_eng

.. |run| replace:: ``run``
.. _run: https://dwopt.readthedocs.io/en/latest/db.html#dwopt.db._Db.run

.. |qry| replace:: ``qry``
.. _qry: https://dwopt.readthedocs.io/en/latest/db.html#dwopt.db._Db.qry

.. |valc| replace:: ``valc``
.. _valc: https://dwopt.readthedocs.io/en/latest/qry.html#dwopt._qry._Qry.valc

.. |dataframe| replace:: ``dataframe``
.. _dataframe: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html

.. |pivot| replace:: ``pivot``
.. _pivot: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.pivot.html

.. |logging| replace:: ``logging``
.. _logging: https://docs.python.org/3/library/logging.html#module-logging

.. |INFO| replace:: ``INFO``
.. _INFO: https://docs.python.org/3/howto/logging.html#when-to-use-logging

.. _operator object: https://dwopt.readthedocs.io/en/latest/db.html#dwopt.db._Db
.. _operator constructors: https://dwopt.readthedocs.io/en/latest/db.html#dwopt.db._Db
.. _query object: https://dwopt.readthedocs.io/en/latest/qry.html#dwopt._qry._Qry
.. _clause methods: https://dwopt.readthedocs.io/en/latest/api.html
.. _summary methods: https://dwopt.readthedocs.io/en/latest/api.html
.. _operation methods: https://dwopt.readthedocs.io/en/latest/api.html
.. _metadata methods: https://dwopt.readthedocs.io/en/latest/api.html

Run query with less friction using default credentials
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

On import, the package gives 3 different `operator object`_
(``pg``, ``lt``, ``oc``, one for each supported database),
with default credentials
(Use the |save_url|_ function to save to the system keyring).
These allow frictionless running of sql from any python window.

>>> from dwopt import pg
>>> pg.run('select count(1) from test')
    42
>>> pg.qry('test').len()
    42

Alternatively, use the |make_eng|_ function and the `operator constructors`_
(``Pg``, ``Lt``, ``Oc``) to access database.

>>> from dwopt import make_eng, Pg
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

    from dwopt import oc
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

In future releases, the package will likely migrate to
the `jinja <https://jinja2docs.readthedocs.io/en/stable/>`_
package's directive syntax.

Programatically make and run simple query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `operator object`_'s |qry|_ method returns the `query object`_.
Use it's `clause methods`_ to make a simple sql query, as it's underlying query.
The underlying query can be run directly, but the main usage is to act as
the preprocessing step of the `summary methods`_
explained in the following sections.

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

In future releases, the package's query construction internals will likely
be improved from text manipulation to the
`sqlalchemy <https://www.sqlalchemy.org/>`_ pakage's toolkit.

Sql template: Excel-pivot table experience
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A few lines of code specifying minimal information could produce a summary
table similiar to what could be achieved in Excel. Difference being
it is the efficient database engine doing the data processing work,
and the flexible python machineries doing the presentation work.

The `operator object`_'s |qry|_ method returns the `query object`_.
Use it's `summary methods`_ to make and run summary queries.
These methods operate on top of the underlying query.

For example:

.. code-block:: python

    from dwopt import lt #1
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

In future releases, the package's templating internals will ikely be
driven by the
`jinjasql <https://github.com/sripathikrishnan/jinjasql>`_
package.

Sql template: Dataframe summary function experience
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

It is possible to mimic what some of the dataframe summary functions
would return, but implement via running sql templates.
Difference being
it is the efficient database engine doing the data processing work,
and the flexible python machineries doing the presentation work.

The `operator object`_'s |qry|_ method returns the `query object`_.
Use it's `summary methods`_ to make and run summary queries.
These methods operate on top of the underlying query.

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

Explanation of lines:

#. Get the default sqlite `operator object`_.
#. Make, but do not run, an underlying sub query.
#. See the `summary methods`_ section for list of methods and
   their descriptions, examples, underlying sql shown in logs.

Sql template: DDL/DML statement, metadata queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The `operator object`_'s `operation methods`_ allows running of
DDL/DML statements programatically, and enhances functionalities 
where desirable. 

Also, the `operator object`_'s `metadata methods`_ makes some useful
metadata queries available.

Operation methods example:

.. code-block:: python

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

Metadata methods example:

.. code-block:: python

    from dwopt import pg
    pg.list_tables() #list all tables
    pg.table_cols('test.test') #examine columns
    pg.table_cons() #list constraints


Automatic logging with fully reproducible sql
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Many of the package's methods are wired through the standard |logging|_ package.

In particular, the |run|_ method emits sql used as |INFO|_ level message.
The relevant logger object has standard naming and is called ``dwopt.db``.
Configure the logging package or the logger at the start of application code
for logs.
See the `logging package documentation <https://docs.python.org/3/howto/logging.html#logging-from-multiple-modules>`_
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

.. end-of-readme-usage

Documentation
-------------

* `API <https://dwopt.readthedocs.io/en/latest/api.html>`_