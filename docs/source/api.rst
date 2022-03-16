API Reference
=============

.. toctree::
   :maxdepth: 2
   :hidden:

   set_up.rst
   db.rst
   qry.rst


.. _set-up functions:

.. rubric:: The set-up functions

.. autosummary::
   :nosignatures:

   dwopt.save_url
   dwopt.db
   dwopt.Db
   dwopt.make_eng
   dwopt.make_test_tbl


.. rubric:: Database operator object

.. autosummary::
   :nosignatures:

   dwopt.dbo._Db


.. _operation methods:

.. rubric:: Database operator object - operation methods

.. autosummary::
   :nosignatures:

   dwopt.dbo._Db.add_pkey
   dwopt.dbo._Db.create
   dwopt.dbo._Db.create_schema
   dwopt.dbo._Db.cwrite
   dwopt.dbo._Db.delete
   dwopt.dbo._Db.drop
   dwopt.dbo._Db.exist
   dwopt.dbo._Db.iris
   dwopt.dbo._Db.mtcars
   dwopt.dbo._Db.qry
   dwopt.dbo._Db.run
   dwopt.dbo._Db.update
   dwopt.dbo._Db.write
   dwopt.dbo._Db.write_nodup


.. _metadata methods:

.. rubric:: Database operator object - metadata methods

.. autosummary::
   :nosignatures:

   dwopt.dbo._Db.list_cons
   dwopt.dbo._Db.list_tables
   dwopt.dbo._Db.table_cols
   dwopt.dbo._Db.table_sizes


.. rubric:: Query object

.. autosummary::
   :nosignatures:

   dwopt._qry._Qry


.. _clause methods:

.. rubric:: Query object - clause methods

.. autosummary::
   :nosignatures:

   dwopt._qry._Qry.case
   dwopt._qry._Qry.from_
   dwopt._qry._Qry.group_by
   dwopt._qry._Qry.having
   dwopt._qry._Qry.join
   dwopt._qry._Qry.order_by
   dwopt._qry._Qry.print
   dwopt._qry._Qry.run
   dwopt._qry._Qry.select
   dwopt._qry._Qry.sql
   dwopt._qry._Qry.str
   dwopt._qry._Qry.where


.. _summary methods:

.. rubric:: Query object - summary methods

.. autosummary::
   :nosignatures:

   dwopt._qry._Qry.bin
   dwopt._qry._Qry.cols
   dwopt._qry._Qry.dist
   dwopt._qry._Qry.five
   dwopt._qry._Qry.hash
   dwopt._qry._Qry.head
   dwopt._qry._Qry.len
   dwopt._qry._Qry.mimx
   dwopt._qry._Qry.pct
   dwopt._qry._Qry.piv
   dwopt._qry._Qry.top
   dwopt._qry._Qry.valc
