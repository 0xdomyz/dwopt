The Database Operator Objects
=============================

The database operator objects are the interface to databases and give access to
the :doc:`query objects <qry>`, which hold the summary query templates.

See below :class:`class description <dwopt.db._Db>` for ways to instantiate.
Available methods are grouped into 3 catagories as below:

* :ref:`db-ops-mtd`
* :ref:`db-qry-mtd`
* :ref:`db-meta-mtd`


.. autoclass:: dwopt.db._Db


.. _db-ops-mtd:

The operation methods
---------------------

Include the :meth:`~dwopt.db._Db.run` method which is the package's most fundamental
database interface,
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

Various convenience wrapers for common metadata queries:

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