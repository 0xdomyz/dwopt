API Reference
=============

.. toctree::
   :hidden:

   urls.rst
   db.rst
   qry.rst

:doc:`The Set-up functions <urls>`

        * :func:`dwopt.save_url`
        * :func:`dwopt.make_eng`

:doc:`The Database Operator Class <db>`

    :ref:`db-ops-mtd`:
        * :meth:`dwopt.db._Db.run`
        * :meth:`dwopt.db._Db.create`
        * :meth:`dwopt.db._Db.add_pkey`
        * :meth:`dwopt.db._Db.drop`
        * :meth:`dwopt.db._Db.write`
        * :meth:`dwopt.db._Db.write_nodup`
    
    :ref:`db-qry-mtd`:
        * :meth:`dwopt.db._Db.qry`
    
    :ref:`db-meta-mtd`:
        * :meth:`dwopt.db._Db.list_tables`
        * :meth:`dwopt.db._Db.table_cols`
        * :meth:`dwopt.db._Db.table_sizes`
        * :meth:`dwopt.db._Db.list_cons`


:doc:`The Query Class <qry>`

    :ref:`qry-cls-mtd`:
        * :meth:`dwopt._qry._Qry.select`
        * :meth:`dwopt._qry._Qry.case`
        * :meth:`dwopt._qry._Qry.from_`
        * :meth:`dwopt._qry._Qry.join`
        * :meth:`dwopt._qry._Qry.where`
        * :meth:`dwopt._qry._Qry.group_by`
        * :meth:`dwopt._qry._Qry.having`
        * :meth:`dwopt._qry._Qry.order_by`
        * :meth:`dwopt._qry._Qry.sql`
    
    :ref:`qry-ops-mtd`:
        * :meth:`dwopt._qry._Qry.print`
        * :meth:`dwopt._qry._Qry.run`
    
    :ref:`qry-sum-mtd`:
        * :meth:`dwopt._qry._Qry.top`
        * :meth:`dwopt._qry._Qry.cols`
        * :meth:`dwopt._qry._Qry.head`
        * :meth:`dwopt._qry._Qry.len`
        * :meth:`dwopt._qry._Qry.dist`
        * :meth:`dwopt._qry._Qry.mimx`
        * :meth:`dwopt._qry._Qry.valc`
        * :meth:`dwopt._qry._Qry.hash`