API Reference
=============

.. toctree::
   :maxdepth: 2
   :hidden:

   set_up.rst
   db.rst
   qry.rst


.. rubric:: The set-up functions

.. autosummary::
   :nosignatures:

   dwopt.save_url
   dwopt.db
   dwopt.Db
   dwopt.make_eng
   dwopt.make_test_tbl


.. rubric:: Database operator object - operation methods

.. autosummary::
   :nosignatures:

   dwopt.dbo._Db.run
   dwopt.dbo._Db.create
   dwopt.dbo._Db.add_pkey
   dwopt.dbo._Db.drop
   dwopt.dbo._Db.write
   dwopt.dbo._Db.write_nodup
   dwopt.dbo._Db.qry


.. rubric:: Database operator object - metadata methods

.. autosummary::
   :nosignatures:

   dwopt.dbo._Db.list_tables
   dwopt.dbo._Db.table_cols
   dwopt.dbo._Db.table_sizes
   dwopt.dbo._Db.list_cons


.. rubric:: Query object - query operation methods

.. autosummary::
   :nosignatures:

   dwopt._qry._Qry.print
   dwopt._qry._Qry.run


.. rubric:: Query object - clause methods

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


.. rubric:: Query object - summary methods

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
