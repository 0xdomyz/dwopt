#db method

def list_tables(self,owner):
    """
    List all tables on database or specified schema. 

    Parameters
    ----------
    owner : str
        Only applicable for oracle. Name of the schema.

    Returns
    -------
    pandas.DataFrame

    Notes
    -----

    Postgre sql used, `information_schema.tables
    <https://www.postgresql.org/docs/current/infoschema-tables.html>`_:

    .. code-block:: sql

        select 
            table_catalog,table_schema,table_name
            ,is_insertable_into,commit_action
        from information_schema.tables
        where table_schema
        not in ('information_schema','pg_catalog')

    Sqlite sql used, `sqlite_schema <https://www.sqlite.org/schematab.html>`_:

    .. code-block:: sql

        select * from sqlite_master
        where type ='table'
        and name NOT LIKE 'sqlite_%'

    Oracle sql used, `all_tab_columns
    <https://docs.oracle.com/en/database/oracle/oracle-database/21/
    refrn/ALL_TAB_COLUMNS.html>`_:

    .. code-block:: sql

        select/*+PARALLEL (4)*/ owner,table_name
            ,max(column_name),min(column_name)
        from all_tab_columns
        where owner = ':owner'
        group by owner,table_name

    """
    raise(Exception('Not Implemented.'))

def table_sizes(self):
    """ 
    List sizes of all tables in current schema. 

    Returns
    -------
    pandas.DataFrame

    Notes
    -----

    Oracle sql used, `user_extents
    <https://docs.oracle.com/en/database/oracle/oracle-database/21/refrn/
    USER_EXTENTS.html>`_:

    .. code-block:: sql

        select/*+PARALLEL (4)*/
            tablespace_name,segment_type,segment_name
            ,sum(bytes)/1024/1024 table_size_mb
        from user_extents
        group by tablespace_name,segment_type,segment_name

    """
    raise(Exception('Not Implemented.'))

def table_cols(self,sch_tbl_nme):
    """
    Show information of specified table's columns.

    Notes
    -----

    Postgre sql used, `information_schema.columns
    <https://www.postgresql.org/docs/current/infoschema-columns.html>`_:

    .. code-block:: sql

        select column_name, data_type
        from information_schema.columns
        where table_schema = ':schema_nme'
        and table_name = ':tbl_nme'

    Oracle sql used, `all_tab_columns
    <https://docs.oracle.com/en/database/oracle/oracle-database/21/
    refrn/ALL_TAB_COLUMNS.html>`_:

    .. code-block:: sql

        select/*+PARALLEL (4)*/ *
        from all_tab_columns
        where owner = ':schema_nme'
        and table_name = ':tbl_nme'

    Parameters
    ----------
    sch_tbl_nme : str
        Table name in format: `schema.table`.

    Returns
    -------
    pandas.DataFrame
    """
    raise(Exception('Not Implemented.'))

def list_cons():
    """
    List all constraints. 

    Returns
    -------
    pandas.DataFrame

    Notes
    -----

    Postgre sql used, `information_schema.constraint_table_usage
    <https://www.postgresql.org/docs/current/infoschema-
    constraint-table-usage.html>`_:

    .. code-block:: sql

        select * from information_schema.constraint_table_usage

    """
    raise(Exception('Not Implemented.'))

#qry methods

def head(self):
    """Fetch top 5 rows of the sub query table.

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>> 
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").where("col1 < 5").head()
            col1  col2
        0     0    10
        1     1    11
        2     2    12
        3     3    13
        4     4    14
    """
    return self.run("select * from x limit 5")

def top(self):
    """Fetch top row of the sub query table.

    Returns
    -------
    pandas.Series

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>> 
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").where("col1 < 5").head()
        col1     0
        col2    10
        Name: 0, dtype: int64
    """
    return self.run("select * from x limit 1").iloc[0,]

def cols(self):
    """Fetch column names of the sub query table.

    Returns
    -------
    Column names as list of str

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>> 
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").cols()
        ['col1', 'col2']
    """
    return self.run("select * from x where 1=2").columns.tolist()

def len(self):
    """Length of the sub query table.

    Returns
    -------
    int

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>> 
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").len()
        10
    """
    return self.run("select count(1) from x").iloc[0,0]

def dist(self,*args):
    """
    Count number of distinct occurances of data within specified columns
    , or combination of columns, of the sub query table.

    Parameters
    ----------
    *args : str or [str]
        Either column names as str, or iterator of column name str.

    Returns
    -------
    pandas.Series

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>> 
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").where("col1 < 5").dist('col1','col2',['col1','col2'])
        count(distinct col1)                   5
        count(distinct col2)                   5
        count(distinct col1 || '_' || col2)    5
        Name: 0, dtype: int64
    """
    _ = (" || '_' || ".join(_) if not isinstance(_,str) else _ 
        for _ in args)
    _ = ''.join(
        f"    ,count(distinct {j})\n" 
        if i else 
        f'    count(distinct {j})\n'
        for i,j in enumerate(_)
    )
    _ = (
        "select \n"
        f'{_}'
        'from x'
    )
    return self.run(_).iloc[0,:]

def mimx(self,col):
    """
    Summarise on max and min values of a column for a sub query table.

    Parameters
    ----------
    col : str
        Column name as str.

    Returns
    -------
    pandas.Series

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>> 
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").where("col1 < 5").mimx('col1')
        max(col1)    4
        min(col1)    0
        Name: 0, dtype: int64
    """
    _ = (
        "select \n"
        f"    max({col}),min({col})\n"
        "from x"
    )
    return self.run(_).iloc[0,:]

def valc(self,group_by,agg = None,order_by = None,n = True):
    """
    Value count of a column or combination of columns. A value count is a 
    group by query, with total number of row of each group calculated.
    Also allow custom summary calculation, and custom order by clauses 
    to be added.

    Parameters
    ----------
    group_by : str
        Group by clause as str.
    agg : str
        Custom aggeregation clause as str.
    order_by : str
        Order by clause as str.
    n : Bool
        Should the value count column be automatically created or not. Default
        to be True.

    Returns
    -------
    pandas.DataFrame

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import lt
    >>> 
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> lt.drop('test')
    >>> lt.create('test',{'col1':'int','col2':'int'})
    >>> lt.write(tbl,'test')
    >>> lt.qry("test").case("cat","col1 > 3 then 'A'",els = "'B'") \\
    ... .where("col1 < 5").valc('cat',"sum(col2) col2")
           cat  n  col2
        0   B   4    46
        1   A   1    14
    """
    group_by_cls = (','.join(group_by) if not isinstance(group_by,str)
        else group_by)
    if agg is None:
        agg_cls = ''
    elif isinstance(agg,str):
        agg_cls = f"    ,{agg}\n"
    else:
        agg_cls = ''.join(f"    ,{_}\n" for _ in agg)
    if order_by is None:
        if n:
            order_by_cls = 'n desc'
        else:
            order_by_cls = group_by_cls
    else:
        order_by_cls = order_by
    _ = (
        "select \n"
        f"    {group_by_cls}\n"
        f"{f'    ,count(1) n{chr(10)}' if n else ''}"
        f"{agg_cls}"
        "from x\n"
        f"group by {group_by_cls}\n"
        f"order by {order_by_cls}"
    )
    return self.run(_)

def hash(self,*args):
    """
    Calculate a simple configuration of oracle hash to arrive at a indicative
    has value for a number of columns or all columns of a sub query table.
    Hash value is a number or symbol that is calculated from data
    , and is sensitive to any small changes in data. It serves as method to
    detect if any data element in data is changed.

    Parameters
    ----------
    *args : str
        Column names in str. If no value is given, a cols method will be
        performed to fetch the list of all columns, from which a hash will be 
        calculated.

    Returns
    -------
    int

    Examples
    --------
    >>> import pandas as pd
    >>> from dwopt import oc
    >>> 
    >>> tbl = pd.DataFrame({'col1': range(10), 'col2': range(10,20)})
    >>> oc.drop('test')
    >>> oc.create('test',{'col1':'int','col2':'int'})
    >>> oc.write(tbl,'test')
    >>> oc.qry("test").where("col1 < 5").hash()
    """
    raise(Exception('Not Implemented.'))

def piv(self):
    pass

