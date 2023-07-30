DWOPT - Datawarehouse Operator
==============================

Getting summary statistics out of database tables is often an unstreamlined process.
Does one read in millions of rows before doing any work on Python,
or run sql elsewhere and use intermediate CSVs,
or write sql strings in python scripts?

The Python package **dwopt**
provides Excel-pivot-table-like and dataframe-summary-methods-like API,
driven by sql templates, under a flexible summary query building framework.

See the Features and the Walk Through section for examples.

.. end-of-readme-intro


Installation
------------

.. code-block:: console

    pip install dwopt

Install the database drivers for the database engines you want to use.

.. code-block:: console

    pip install psycopg2 # postgres
    pip install psycopg2-binary # postgres alternative in case of error
    
    pip install oracledb # oracle

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
with default credentials, which could be saved prior by user to
the system keyring using the |dwopt.save_url|_ function.

.. code-block:: python

    from dwopt import lt
    lt.iris()
    lt.run('select count(1) from iris')
       count
    0    150

This enable quick analysis from any Python/Console window:

.. code-block:: python

    from dwopt import pg
    pg.iris()
    pg.qry('iris').valc('species', 'avg(petal_length)')
       species   n  avg(petal_length)
    0  sicolor  50              4.260
    1   setosa  50              1.462
    2  rginica  50              5.552

Alternatively, use the database operator object factory function |dwopt.db|_
and the database engine url to access database.

.. code-block:: python

    from dwopt import db
    d = db("postgresql://dwopt_tester:1234@localhost/dwopt_test")
    d.mtcars()
    d.run('select count(1) n from mtcars')
        n
    0  32

.. code-block:: python

    from dwopt import db
    url = """oracle+oracledb://dwopt_test:1234@localhost:1521/?service_name=XEPDB1 
    &encoding=UTF-8&nencoding=UTF-8"""
    lib_dir = "C:/app/{user_name}/product/21c/dbhomeXE/bin"
    o = db(url, thick_mode={"lib_dir": lib_dir})
    o.run("select * from dual")
      dummy
    0     X

Supports:

* Python 3.10, 3.11.
* Windows 10: Sqlite, Postgres, Oracle.
* Linux: Sqlite, Postgres.

See `Testing`_ section for package version tested.

Run sql script with text replacement
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. |run| replace:: ``run``
.. _run: https://dwopt.readthedocs.io/en/stable/dbo.html#dwopt.dbo._Db.run

Use the database operator object's
|run|_ method to run sql script file.
One could then replace ``:`` marked parameters via mappings supplied to the method.

Colon syntax is to be depreciated. A future version will use jinja2 syntax across the board.

.. code-block:: python

    from dwopt import pg, make_test_tbl
    _ = make_test_tbl(pg)
    pg.run(pth = "E:/projects/my_sql_script.sql",
        my_run_dte = '2022-03-03',
        my_label = '20220303',
        threshold = 5)
       count
    0    137

Above runs the sql stored on ``E:/projects/my_sql_script.sql`` as below:

.. code-block:: sql

    drop table if exists monthly_extract_:my_label;

    create table monthly_extract_:my_label as
    select * from test
    where
        dte = to_date(':my_run_dte','YYYY-MM-DD')
        and score > :threshold;

    select count(1) from monthly_extract_:my_label;


Programatically make simple sql query
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _list of clause methods: https://dwopt.readthedocs.io/en/stable/api.html#clause-methods
.. |qry| replace:: ``qry``
.. _qry: https://dwopt.readthedocs.io/en/stable/dbo.html#dwopt.db._Db.qry
.. _summary query building framework: https://dwopt.readthedocs.io/en/stable/qry.html#the-summary-query-building-framework
.. _query object: https://dwopt.readthedocs.io/en/stable/qry.html#dwopt._qry._Qry

The database operator object's |qry|_ method returns the `query object`_.
Use it's `list of clause methods`_ to make a simple sql query.

This is not faster than just writing the sql,
main usage is to provide flexibility to the `summary query building framework`_.

.. code-block:: python

    from dwopt import lt
    lt.mtcars()
    sql = "select cyl from mtcars group by cyl having count(1) > 10"
    q = (
        lt.qry('mtcars a')
        .select('a.cyl, count(1) n, avg(a.mpg)')
        .case('cat', "a.cyl = 8 then 1", els=0)
        .join(f'({sql}) b', 'a.cyl = b.cyl', how='inner')
        .group_by('a.cyl')
        .having('count(1) > 10')
        .order_by('n desc')
    )
    q.run()
       cyl   n  avg(a.mpg)  cat
    0    8  14   15.100000    1
    1    4  11   26.663636    0

.. code-block:: sql

    q.print()
    select a.cyl, count(1) n, avg(a.mpg)
        ,case when a.cyl = 8 then 1 else 0 end as cat
    from mtcars a
    inner join (select cyl from mtcars group by cyl having count(1) > 10) b
        on a.cyl = b.cyl
    group by a.cyl
    having count(1) > 10
    order by n desc


Templates: Excel-pivot-table-like API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. |valc| replace:: ``valc``
.. _valc: https://dwopt.readthedocs.io/en/stable/qry.html#dwopt._qry._Qry.valc

.. |pivot| replace:: ``pivot``
.. _pivot: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.pivot.html

Use the `query object`_ and it's |valc|_ method to make and run
a value counts summary query with custom groups and calcs,
on top of arbituary sub-query, as part of the `summary query building framework`_.

Then call the result dataframe's |pivot|_ method to finalize the pivot table.

.. code-block:: python

    from dwopt import lt, make_test_tbl
    _ = make_test_tbl(lt)
    (
        lt.qry('test')
        .where('score>0.5', 'dte is not null', 'cat is not null')
        .valc('dte,cat', 'avg(score) avgscore, round(sum(amt)/1e3,2) total')
        .pivot('dte', 'cat')
    )

Result:

==========  =====  =====  ========  ========  ======  ======
cat           n           avgscore             total
----------  -----  -----  --------  --------  ------  ------
dte         test   train    test     train     test   train 
==========  =====  =====  ========  ========  ======  ======
2022-01-01  1140   1051   2.736275  2.800106  565.67  530.09
2022-02-02  1077   1100   2.759061  2.748898  536.68  544.10
2022-03-03  1037   1072   2.728527  2.743825  521.54  528.85
==========  =====  =====  ========  ========  ======  ======

The final query used can be invoked by the |valc|_ method, or logged via standard
logging.

.. code-block:: sql

    with x as (
        select * from test
        where score>0.5
            and dte is not null
            and cat is not null
    )
    select
        dte,cat
        ,count(1) n
        ,avg(score) avgscore, round(sum(amt)/1e3,2) total
    from x
    group by dte,cat
    order by n desc


Templates: Dataframe-summary-methods-like API
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _list of summary methods: https://dwopt.readthedocs.io/en/stable/api.html#summary-methods

Use the `query object`_ and it's `list of summary methods`_ to make and run
summary queries on top of arbituary sub-query,
as part of the `summary query building framework`_:

.. code-block:: python

    from dwopt import pg
    pg.iris()
    q = pg.qry('iris a').select('a.*').case('cat',
        "petal_length > 5             then '5+'",
        "petal_length between 2 and 5 then '2-5'",
        "petal_length < 2             then '-2'",
    )

    #Column names:
    q.cols()
    ['sepal_length', 'sepal_width', 'petal_length', 'petal_width', 'species', 'cat']

    #Number of distinct combination:
    q.dist(['species', 'petal_length'])
    count    48
    Name: 0, dtype: int64

    #Head:
    q.head()
       sepal_length  sepal_width  petal_length  petal_width species cat
    0           5.1          3.5           1.4          0.2  setosa  -2
    1           4.9          3.0           1.4          0.2  setosa  -2
    2           4.7          3.2           1.3          0.2  setosa  -2
    3           4.6          3.1           1.5          0.2  setosa  -2
    4           5.0          3.6           1.4          0.2  setosa  -2

    #Length:
    q.len()
    150

    #Min and max value:
    q.mimx('petal_length')
    max    6.9
    min    1.0
    Name: 0, dtype: float64

    #Top record:
    q.top()
    sepal_length       5.1
    sepal_width        3.5
    petal_length       1.4
    petal_width        0.2
    species         setosa
    cat                 -2
    Name: 0, dtype: object

    #Value count followed by pivot:
    q.valc('species, cat').pivot('species','cat','n')
    cat        -2   2-5    5+
    species
    rginica   NaN   9.0  41.0
    setosa   50.0   NaN   NaN
    sicolor   NaN  49.0   1.0

.. code-block:: sql

    #--All summary methods support output by printing or str:
    q.valc('species, cat', out=1)
    with x as (
        select a.*
            ,case
                when petal_length > 5             then '5+'
                when petal_length between 2 and 5 then '2-5'
                when petal_length < 2             then '-2'
                else NULL
            end as cat
        from iris a
    )
    select
        species, cat
        ,count(1) n
    from x
    group by species, cat
    order by n desc

Templates: DDL/DML statements, metadata queries
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. _list of metadata methods: https://dwopt.readthedocs.io/en/stable/api.html#metadata-methods
.. _list of operation methods: https://dwopt.readthedocs.io/en/stable/api.html#operation-methods

Use the `list of operation methods`_ to make and run some
DDL/DML statements with convenient or enhanced functionalities:

.. code-block:: python

    import pandas as pd
    from dwopt import lt
    tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
    tbl2 = pd.DataFrame({'col1': [1, 3], 'col2': ['a', 'c']})
    lt.drop('test')
    lt.create('test', col1='int', col2='text')
    lt.write(tbl, 'test')
    lt.write_nodup(tbl2, 'test', ['col1'], "col1 < 4")
    lt.run("select * from test")
       col1 col2
    0     1    a
    1     2    b
    2     3    c

.. code-block:: python

    lt.drop('test')
    lt.cwrite(tbl, 'test')
    lt.qry('test').run()
       col1 col2
    0     1    a
    1     2    b


Use the `list of metadata methods`_ to make and run some useful metadata queries:

.. code-block:: python

    from dwopt import pg
    pg.iris()
    pg.table_cols('public.iris')
        column_name          data_type
    0  sepal_length               real
    1   sepal_width               real
    2  petal_length               real
    3   petal_width               real
    4       species  character varying

.. code-block:: python

    from dwopt import lt
    lt.iris()
    lt.mtcars()
    lt.list_tables().iloc[:,:-1]
        type    name tbl_name  rootpage
    0  table    iris     iris         2
    1  table  mtcars   mtcars         5


Standard logging with reproducible sql
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. |INFO| replace:: ``INFO``
.. _INFO: https://docs.python.org/3/howto/logging.html#when-to-use-logging

Many of the package's methods are wired through the standard
`logging <https://docs.python.org/3/library/logging.html#module-logging>`_
package. In particular, the |run|_ method emits sql used as |INFO|_ level message.
The relevant logger object has standard naming and is called ``dwopt.dbo``.

Example configuration to show logs in console:

.. code-block:: python

    import logging
    logging.basicConfig(level = logging.INFO)

    from dwopt import lt
    lt.iris(q=1).valc('species', 'avg(petal_length)')

.. code-block:: text

    INFO:dwopt.dbo:dropping table via sqlalchemy: iris
    INFO:dwopt.dbo:done
    INFO:dwopt.dbo:creating table via sqlalchemy:
    INFO:dwopt.dbo:('sepal_length', Column('sepal_length', REAL(), table=<iris>))
    INFO:dwopt.dbo:('sepal_width', Column('sepal_width', REAL(), table=<iris>))
    INFO:dwopt.dbo:('petal_length', Column('petal_length', REAL(), table=<iris>))
    INFO:dwopt.dbo:('petal_width', Column('petal_width', REAL(), table=<iris>))
    INFO:dwopt.dbo:('species', Column('species', String(), table=<iris>))
    INFO:dwopt.dbo:done
    INFO:dwopt.dbo:running:
    INSERT INTO iris (sepal_length, sepal_width, petal_length, petal_width, species) VALUES (:sepal_length, :sepal_width, :petal_length, :petal_width, :species)
    INFO:dwopt.dbo:args len=150, e.g.
    {'sepal_length': 5.1, 'sepal_width': 3.5, 'petal_length': 1.4, 'petal_width': 0.2, 'species': 'setosa'}
    INFO:dwopt.dbo:done
    INFO:dwopt.dbo:running:
    with x as (
        select * from iris
    )
    select
        species
        ,count(1) n
        ,avg(petal_length)
    from x
    group by species
    order by n desc
    INFO:dwopt.dbo:done
       species   n  avg(petal_length)
    0  sicolor  50              4.260
    1   setosa  50              1.462
    2  rginica  50              5.552

Alternatively, to avoid logging info messages from other packages:

.. code-block:: python

    import logging
    logging.basicConfig()
    logging.getLogger('dwopt.dbo').setLevel(logging.INFO)

Example configuration to print on console and store on file with timestamps:

.. code-block:: python

    import logging
    logging.basicConfig(
        format = "%(asctime)s [%(levelname)s] %(message)s"
        ,handlers=[
            logging.FileHandler("E:/projects/logs.log"),
            logging.StreamHandler()
        ]
    )
    logging.getLogger('dwopt.dbo').setLevel(logging.INFO)

Debug logging:

.. code-block:: python

    import logging
    logging.basicConfig()
    logging.getLogger('dwopt').setLevel(logging.DEBUG)

Sqlalchemy logger can also be used to obtain even more details:

.. code-block:: python

    import logging
    logging.basicConfig()
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)


Development
---------------

Installation
^^^^^^^^^^^^^^^

Testing, documentation building package:

.. code-block:: console

    #venv on linux
    sudo apt-get install python3-venv
    python3.11 -m venv dwopt_dev
    source dwopt_dev/bin/activate
    deactivate

    #testing
    python -m pip install pytest black flake8 tox
    
    #doco and packaging
    python -m pip install sphinx sphinx_rtd_theme build twine wheel
    
    #depend
    python -m pip install -U sqlalchemy pandas keyring
    python -m pip install -U keyrings.alt
    python -m pip install -U psycopg2
    python -m pip install -U oracledb
    
    # consider
    python -m pip install -U psycopg2-binary
    python -m pip install -U cx_Oracle
    
    #package
    python -m pip install -e .

Testing
^^^^^^^^^^^^

Test:

.. code-block:: console

    python -m tox

.. |dwopt.make_test_tbl| replace:: ``dwopt.make_test_tbl``
.. _dwopt.make_test_tbl: https://dwopt.readthedocs.io/en/stable/set_up.html#dwopt.make_test_tbl

Testing for specific databases.
Set up environment based on |dwopt.make_test_tbl|_ function notes.

.. code-block:: console

    python -m pytest
    python -m pytest --db=pg
    python -m pytest --db=oc

Test code styles:

.. code-block:: console

    flake8 src/dwopt

Databases used for testings are::

    Postgres 15
    Oracle express 21c

Package versions tested are::



Documentation
^^^^^^^^^^^^^^^^^

Build document:

.. code-block:: console

    cd docs
    make html

Test examples across docs:

.. code-block:: console

    cd docs
    make doctest

Future
^^^^^^^^^

* For text replacement directives, use
  `jinja2 <https://jinja2docs.readthedocs.io/en/stable/>`_ syntax.
* Add more summary, DML/DDL, metadata templates.

.. end-of-readme-usage


Documentation
-------------

* `API <https://dwopt.readthedocs.io/en/stable/api.html>`_
