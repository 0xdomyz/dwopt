API Reference
=============

.. toctree::
   :hidden:
   :includehidden:

   urls.rst
   db.rst
   qry.rst

**The Set-up functions**
        * :func:`dwops.save_url`
        * :func:`dwops.make_eng`

**The Database Operator Class** :class:`dwops.db._Db`

    The operation methods:
        * :meth:`dwops.db._Db.run`
        * :meth:`dwops.db._Db.create`
        * :meth:`dwops.db._Db.add_pkey`
        * :meth:`dwops.db._Db.drop`
        * :meth:`dwops.db._Db.write`
        * :meth:`dwops.db._Db.write_nodup`
    
    The query method:
        * :meth:`dwops.db._Db.qry`
    
    The metadata methods:
        * :meth:`dwops.db._Db.list_tables`
        * :meth:`dwops.db._Db.table_cols`
        * :meth:`dwops.db._Db.table_sizes`
        * :meth:`dwops.db._Db.list_cons`


**The Query Class** :class:`dwops._qry._Qry`

    The clause methods:
        * :meth:`dwops._qry._Qry.select`
        * :meth:`dwops._qry._Qry.case`
        * :meth:`dwops._qry._Qry.from_`
        * :meth:`dwops._qry._Qry.join`
        * :meth:`dwops._qry._Qry.where`
        * :meth:`dwops._qry._Qry.group_by`
        * :meth:`dwops._qry._Qry.having`
        * :meth:`dwops._qry._Qry.order_by`
        * :meth:`dwops._qry._Qry.sql`
    
    The operation methods:
        * :meth:`dwops._qry._Qry.print`
        * :meth:`dwops._qry._Qry.run`
    
    The summary methods:
        * :meth:`dwops._qry._Qry.top`
        * :meth:`dwops._qry._Qry.cols`
        * :meth:`dwops._qry._Qry.head`
        * :meth:`dwops._qry._Qry.len`
        * :meth:`dwops._qry._Qry.dist`
        * :meth:`dwops._qry._Qry.mimx`
        * :meth:`dwops._qry._Qry.valc`
        * :meth:`dwops._qry._Qry.hash`