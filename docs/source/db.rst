The Database Operator Class
===========================

.. autoclass:: dwopt.db._Db


.. _db-ops-mtd:

The operation methods
---------------------

List of operation methods:

* :meth:`dwopt.db._Db.run`
* :meth:`dwopt.db._Db.create`
* :meth:`dwopt.db._Db.add_pkey`
* :meth:`dwopt.db._Db.drop`
* :meth:`dwopt.db._Db.write`
* :meth:`dwopt.db._Db.write_nodup`

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

List of metadata methods:

* :meth:`dwopt.db._Db.list_tables`
* :meth:`dwopt.db._Db.table_cols`
* :meth:`dwopt.db._Db.table_sizes`
* :meth:`dwopt.db._Db.list_cons`

.. automethod:: dwopt.db._Db.list_tables
.. automethod:: dwopt.db._Db.table_cols
.. automethod:: dwopt.db._Db.table_sizes
.. automethod:: dwopt.db._Db.list_cons