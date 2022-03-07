The Database Operator Class
===========================

Database operators provide access to DDL/DML statements, metadata queries, and
the relevant query objects. Categories of methods:

* :ref:`db-ops-mtd`
* :ref:`db-qry-mtd`
* :ref:`db-meta-mtd`


.. autoclass:: dwopt.db._Db


.. _db-ops-mtd:

The operation methods
---------------------

Include the ``run`` method which is the core of the package,
and various convenience wrapers for common DDL and DML statements:

.. autosummary::
   :nosignatures:

   dwopt.db._Db.run
   dwopt.db._Db.create
   dwopt.db._Db.add_pkey
   dwopt.db._Db.drop
   dwopt.db._Db.write
   dwopt.db._Db.write_nodup

.. automethod:: dwopt.db._Db.run
.. automethod:: dwopt.db._Db.create
.. automethod:: dwopt.db._Db.add_pkey
.. automethod:: dwopt.db._Db.drop
.. automethod:: dwopt.db._Db.write
.. automethod:: dwopt.db._Db.write_nodup


.. _db-qry-mtd:

The query method
----------------

.. automethod:: dwopt.db._Db.qry


.. _db-meta-mtd:

The metadata methods
--------------------

Various convenience wrapers for common metadata statements:

.. autosummary::
   :nosignatures:

   dwopt.db._Db.list_tables
   dwopt.db._Db.table_cols
   dwopt.db._Db.table_sizes
   dwopt.db._Db.list_cons

.. automethod:: dwopt.db._Db.list_tables
.. automethod:: dwopt.db._Db.table_cols
.. automethod:: dwopt.db._Db.table_sizes
.. automethod:: dwopt.db._Db.list_cons