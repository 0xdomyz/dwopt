The Query Class
===============

Query objects provide access to sql query building methods and
pre-built summary queries. See below :ref:`qry-build-framework` for details.
Categories of methods:

* :ref:`qry-build-framework`
* :ref:`qry-cls-mtd`
* :ref:`qry-ops-mtd`
* :ref:`qry-sum-mtd`

.. autoclass:: dwopt._qry._Qry


.. _qry-build-framework:

The query building framework
----------------------------

Queries are flexibly built as a combination of a ``sub query``
and a ``summary query``. For example:

.. code-block:: sql

    -- Sub query
    with x as (
        select 
            a.*
            ,case when amt < 1000 then amt*1.2 else amt end as amt
        from test a
        where score > 0.5
    )
    -- Summary query
    select
        time,cat
        ,count(1) n
        ,avg(score) avgscore, round(sum(amt)/1e3,2) total
    from x
    group by time,cat
    order by n desc

The ``sub query`` is an arbituary query within a with clause
named as ``x``.
It functions as a pre-processing step of the overall query.
    
Use the query objects' ``clause methods`` to iteratively
piece together a query, or use the ``sql`` method to provide
an arbituary query. This created query will then be placed inside a
with block on invocation of any ``summary methods``.

The ``summary query`` is a parameterized pre-built summary query template. 
Call the query objects' ``summary methods`` to invoke these templates,
which completes the query and immediately runs it.

The ``summary query`` operates on top of the sub query, therefore
heavy intermediate results from the sub query are never realized
outside of the database engine.
These templates are the core of the package and achieve the bulk of
efficiency and convenience gains.


.. _qry-cls-mtd:

The clause methods
------------------

Iteratively build a query.
Calling any of these methods will return a new query object
with the intended modification to the underlying query.

Can be paired with the operation methods to build and run query.
However the main usage is to act as pre-processing step of the summary methods.
See :any:`qry-build-framework` for details.

.. autosummary::
   :nosignatures:

   dwopt._qry._Qry.select
   dwopt._qry._Qry.case
   dwopt._qry._Qry.from_
   dwopt._qry._Qry.join
   dwopt._qry._Qry.where
   dwopt._qry._Qry.group_by
   dwopt._qry._Qry.having
   dwopt._qry._Qry.order_by
   dwopt._qry._Qry.sql

.. automethod:: dwopt._qry._Qry.select
.. automethod:: dwopt._qry._Qry.case
.. automethod:: dwopt._qry._Qry.from_
.. automethod:: dwopt._qry._Qry.join
.. automethod:: dwopt._qry._Qry.where
.. automethod:: dwopt._qry._Qry.group_by
.. automethod:: dwopt._qry._Qry.having
.. automethod:: dwopt._qry._Qry.order_by
.. automethod:: dwopt._qry._Qry.sql


.. _qry-ops-mtd:

The operation methods
---------------------

Directly operate on the underlying query built by the clause methods.
These functions does not place the underlying query into a with block,
which is the behaviour of the summary methods.


.. autosummary::
   :nosignatures:

   dwopt._qry._Qry.print
   dwopt._qry._Qry.run

.. automethod:: dwopt._qry._Qry.print
.. automethod:: dwopt._qry._Qry.run


.. _qry-sum-mtd:

The summary methods
-------------------

Apply a parameterized pre-built summary query template, to the underlying
query, to build a complete summary query. Then immediately runs it.
See :any:`qry-build-framework` for details.

.. autosummary::
   :nosignatures:

   dwopt._qry._Qry.top
   dwopt._qry._Qry.cols
   dwopt._qry._Qry.head
   dwopt._qry._Qry.len
   dwopt._qry._Qry.dist
   dwopt._qry._Qry.mimx
   dwopt._qry._Qry.valc
   dwopt._qry._Qry.hash

.. automethod:: dwopt._qry._Qry.top
.. automethod:: dwopt._qry._Qry.cols
.. automethod:: dwopt._qry._Qry.head
.. automethod:: dwopt._qry._Qry.len
.. automethod:: dwopt._qry._Qry.dist
.. automethod:: dwopt._qry._Qry.mimx
.. automethod:: dwopt._qry._Qry.valc
.. automethod:: dwopt._qry._Qry.hash