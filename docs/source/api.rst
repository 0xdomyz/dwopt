API Reference
=============

.. toctree::
   :hidden:
   :includehidden:

   urls.rst
   db.rst
   qry.rst

**The Set-up functions**
        * :func:`dwopt.save_url`
        * :func:`dwopt.make_eng`

**The Database Operator Class** :class:`dwopt.db._Db`

    The operation methods:
        * :meth:`dwopt.db._Db.run`
        * :meth:`dwopt.db._Db.create`
        * :meth:`dwopt.db._Db.add_pkey`
        * :meth:`dwopt.db._Db.drop`
        * :meth:`dwopt.db._Db.write`
        * :meth:`dwopt.db._Db.write_nodup`
    
    The query method:
        * :meth:`dwopt.db._Db.qry`
    
    The metadata methods:
        * :meth:`dwopt.db._Db.list_tables`
        * :meth:`dwopt.db._Db.table_cols`
        * :meth:`dwopt.db._Db.table_sizes`
        * :meth:`dwopt.db._Db.list_cons`


**The Query Class** :class:`dwopt._qry._Qry`

    The clause methods:
        * :meth:`dwopt._qry._Qry.select`
        * :meth:`dwopt._qry._Qry.case`
        * :meth:`dwopt._qry._Qry.from_`
        * :meth:`dwopt._qry._Qry.join`
        * :meth:`dwopt._qry._Qry.where`
        * :meth:`dwopt._qry._Qry.group_by`
        * :meth:`dwopt._qry._Qry.having`
        * :meth:`dwopt._qry._Qry.order_by`
        * :meth:`dwopt._qry._Qry.sql`
    
    The operation methods:
        * :meth:`dwopt._qry._Qry.print`
        * :meth:`dwopt._qry._Qry.run`
    
    The summary methods:
        * :meth:`dwopt._qry._Qry.top`
        * :meth:`dwopt._qry._Qry.cols`
        * :meth:`dwopt._qry._Qry.head`
        * :meth:`dwopt._qry._Qry.len`
        * :meth:`dwopt._qry._Qry.dist`
        * :meth:`dwopt._qry._Qry.mimx`
        * :meth:`dwopt._qry._Qry.valc`
        * :meth:`dwopt._qry._Qry.hash`