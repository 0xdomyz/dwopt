class _Qry:
    """ 
    Generic query class. There are 2 main usages:

    1. Make sql query.
    2. Make and run summary query on top of the generated sql query. 
       In particular, the sql query is placed into a sub query clause
       , and the summary query operates on the intermediate result 
       that would be arrived by the query.

    These usages work in the conext of a common program pattern 
    where the summary query has preprocessing steps such as a case 
    statement or a where clause. The intermediate tables are often not usefull
    on it's own, thus avoiding explicitly materilising it gives performance
    gain. A example as below:

    .. code-block:: sql

        with x as (
            select 
                a.*
                ,case when amt < 1000 then amt*1.2 else amt end as amt
            from test a
            where score > 0.5
        )
        select
            time,cat
            ,count(1) n
            ,avg(score) avgscore, round(sum(amt)/1e3,2) total
        from x
        group by time,cat
        order by n desc

    Query classes should not be instantiated directly by user
    , the appropriate query object should be returned by the appropriate 
    database operator object's qry method. Query classes:

    * dwopt._qry.PgQry: Postgre query class.
    * dwopt._qry.LtQry: Sqlite query class.
    * dwopt._qry.OcQry: Oracle query class.

    Parameters
    ----------
    operator : dwopt.db._Db
        Database operator object to operate on generated queries.
    from_ : str
        Query table name str format.
    select: str
        Columns in str format.
    join : str
        Join clause in str format.
    where : str
        Conditions in str format.
    group_by : str
        Group by names in str format.
    having : str
        Conditions in str format.
    order_by : str
        Order by names in str format.
    sql : str
        Sql code in str format.

    Notes
    -----
    Alternative to initializing the query object by all desired clauses
    , various convenience methods are given to augment the query. 
    Use the methods.

    Examples
    --------

    Example of multiple join statements, and the underlying sql.

    .. code-block:: python

        (
            lt.qry('test x')
            .select('x.id','y.id as yid','x.score','z.score as zscore')
            .join("test y","x.id = y.id+1","x.id <= y.id+1")
            .join("test z","x.id = z.id+2","x.id >= z.id+1")
            .where('x.id < 10','z.id < 10')
            .head()
        )

    .. code-block:: sql

        with x as (
            select x.id,y.id as yid,x.score,z.score as zscore
            from test x
            left join test y
                on x.id = y.id+1
                and x.id <= y.id+1
            left join test z
                on x.id = z.id+2
                and x.id >= z.id+1
            where x.id < 10
                and z.id < 10
        )
        select * from x limit 5

    Example of group by and related clauses, and the underlying sql.

    .. code-block:: python

        (
            lt.qry('test x')
            .select('x.cat,y.cat as bcat'
                ,'sum(x.score) bscore','sum(y.score) yscore','count(1) n')
            .join("test y","x.id = y.id+1")
            .where('x.id < 1000')
            .group_by('x.cat,y.cat')
            .having('count(1) > 50','sum(y.score) > 100')
            .order_by('x.cat desc','sum(y.score) desc')
            .run()
        )

    .. code-block:: sql

        select x.cat,y.cat as bcat,sum(x.score) bscore,sum(y.score) yscore,count(1) n
        from test x
        left join test y
            on x.id = y.id+1
        where x.id < 1000
        group by x.cat,y.cat
        having count(1) > 50
            and sum(y.score) > 100
        order by x.cat desc,sum(y.score) desc

    """
    def __init__(self
            ,operator
            ,from_ = None,select = None,join = None
            ,where = None,group_by = None,having = None
            ,order_by = None,sql = None):
        self._ops = operator
        self._from_ = from_
        self._select = select
        self._join = join
        self._where = where
        self._group_by = group_by
        self._having = having
        self._order_by = order_by
        self._sql = sql

    def __copy__(self):
        return type(self)(
             self._ops
            ,self._from_,self._select,self._join
            ,self._where,self._group_by,self._having
            ,self._order_by,self._sql
        )

    def _args2str(self,args,sep):
        """
        Parse a tuple of str, or iterator of str, 
        into a combined str, fit to be used as part of query.

        Parameters
        ----------
        args : (str,) or ([str],)
            Either a tuple of elemental and/or combined str
            , or a tuple with first and only element 
            being a iterator of elemental str.
        sep : str
            Seperator used to seperate elemental str.

        Returns
        -------
        str
            The combined str.

        Examples
        --------
        >>> import dwopt
        >>> dwopt._qry._Qry._args2str(_,('a,b,c',),',')
            'a,b,c'
        >>> dwopt._qry._Qry._args2str(_,('a','b','c',),',')
            'a,b,c'
        >>> dwopt._qry._Qry._args2str(_,(['a','b','c'],),',')
            'a,b,c'
        >>> dwopt._qry._Qry._args2str(_,(('a','b','c'),),',')
            'a,b,c'
        """
        l = len(args)
        if l == 0:
            res = None
        elif l == 1:
            arg = args[0]
            if isinstance(arg,str):
                res = arg
            else:
                res = sep.join(arg)
        else:
            res = sep.join(args)
        return res

    def select(self,*args,sep = ','):
        """
        Add the select clause to query.

        Parameters
        ----------
        *args : str or [str]
            Column name str as positional arguments
            , or an iterator of str column names.
        sep : str
            Symbol used for seperating column names, default ``,``.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry('test').select("id,score,amt").print()
            select id,score,amt
            from test
        >>> lt.qry('test').select(["id","score","amt"]).print()
            select id,score,amt
            from test
        >>> lt.qry('test').select("id","score","amt").print()
            select id,score,amt
            from test
        """
        _ = self.__copy__()
        _._select = self._args2str(args,sep)
        return _

    def case(self,col,*args,cond = None,els = 'NULL'):
        """
        Add a case when statement to select clause in query.
        Calling this method multiple times would add multiple statements.

        Parameters
        ----------
        col : str
            Column name of the resulting column.
        *args : str
            Positional argument in form 'condition then treatement'.
        cond : dict
            Dictionary of condition str to treatment str mappings.
        els : str
            Treatment str for else clause, default ``NULL``.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry("test").case('col',"x>5 then 'A'").print()
            select *
                ,case when x>5 then 'A' else NULL end as col
            from test
        >>> lt.qry("test").case('col',cond = {'x>5':'A','x<2':'B'}).print()
            select *
                ,case
                    when x>5 then A
                    when x<2 then B
                    else NULL
                end as col
            from test
        >>> lt.qry("test").select('a','b') \\
        ... .case('col'
        ...     ,'x<2 then B'
        ...     ,cond = {'x>5':'A'}
        ...     ,els = 'C').print()
            select a,b
                ,case
                    when x<2 then B
                    when x>5 then A
                    else C
                end as col
            from test
        """
        _ = self.__copy__()
        if cond is not None:
            for i,j in cond.items():
                args = args + (f"{i} then {j}",)
        if len(args) == 0:
            raise Exception('too few cases')
        elif len(args) == 1 and len(args[0]) < 35:
            cls = f"\n    ,case when {args[0]} else {els} end as {col}"
        else:
            cls = self._args2str(args,'\n        when ')
            cls = (
                "\n    ,case"
                f"\n        when {cls}"
                f"\n        else {els}"
                f"\n    end as {col}"
            )
        if _._select is None:
            _._select = '*' + cls
        else:
            _._select = _._select + cls
        return _

    def from_(self,from_):
        """
        Add the from clause to query. Alternative to simply specifying table
        name as the only argument of the qry method. Use the qry method.

        Parameters
        ----------
        from_ : str
            Table name str.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry().from_("test").print()
            select * from test
        """
        _ = self.__copy__()
        _._from_ = from_
        return _

    def join(self,tbl,*args,how = 'left'):
        """
        Add a join clause to query. 
        Calling this method multiple times would add multiple clauses.

        Parameters
        ----------
        tbl : str
            Table name to join to in str format.
        *args : str
            Joining conditions in str format.
        how : str
            The join keyword in str format, for example: ``inner``, ``cross``.
            Default ``left``.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry('test x') \\
        ... .select('x.id','y.id as yid','x.score','z.score as zscore') \\
        ... .join("test y","x.id = y.id+1","x.id <= y.id+1") \\
        ... .join("test z","x.id = z.id+2","x.id >= z.id+1") \\
        ... .print()
            select x.id,y.id as yid,x.score,z.score as zscore
            from test x
            left join test y
                on x.id = y.id+1
                and x.id <= y.id+1
            left join test z
                on x.id = z.id+2
                and x.id >= z.id+1
        """
        _ = self.__copy__()
        on = self._args2str(args,'\n    and ')
        cls = (
            f'{how} join {tbl}'
            f'\n    on {on}'
        )
        if _._join is not None:
            _._join = _._join + '\n' + cls
        else:
            _._join = cls
        return _

    def where(self,*args):
        """
        Add the where clause to query.

        Parameters
        ----------
        *args : str or [str]
            Conditions in str format, or iterator of condition str.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry('test').where('x>5','x<10').print()
            select * from test
            where x>5
                and x<10
        >>> lt.qry('test').where(['x>5','x<10','y <> 5']).print()
            select * from test
            where x>5
                and x<10
                and y <> 5
        """
        _ = self.__copy__()
        _._where = self._args2str(args,'\n    and ')
        return _

    def group_by(self,*args):
        """
        Add the group by clause to query.

        Parameters
        ----------
        *args : str or [str]
            Group by columns in str format, or iterator of them.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry('test').select('a,count(1) n').group_by('a').print()
            select a,count(1) n
            from test
            group by a
        >>> lt.qry('test').select('a,b,count(1) n').group_by(['a','b']).print()
            select a,b,count(1) n
            from test
            group by a,b
        """
        _ = self.__copy__()
        _._group_by = self._args2str(args,',')
        return _

    def having(self,*args):
        """
        Add the having clause to query.

        Parameters
        ----------
        *args : str or [str]
            Conditions in str format, or iterator of them.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry('test').select('a,count(1) n').group_by('a') \\
        ... .having("count(1)>5").print()
            select a,count(1) n
            from test
            group by a
            having count(1)>5
        """
        _ = self.__copy__()
        _._having = self._args2str(args,'\n    and ')
        return _

    def order_by(self,*args):
        """
        Add the order by clause to query.

        Parameters
        ----------
        *args : str or [str]
            Order by names in str format, or iterator of them.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry('test').select('a,count(1) n').group_by('a') \\
        ... .having("count(1)>5").order_by('a','n desc').print()
            select a,count(1) n
            from test
            group by a
            having count(1)>5
            order by a,n desc
        """
        _ = self.__copy__()
        _._order_by = self._args2str(args,',')
        return _

    def sql(self,sql):
        """
        Replace entire query by specified sql. 
        This allows arbituary advanced sql to be incorporated into framework.

        Parameters
        ----------
        sql : str
            Sql code in str format.

        Returns
        -------
        dwopt._qry._Qry
            New query object with clause added.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry().sql("select * from test \\nconnect by level <= 5").print()
            select * from test
            connect by level <= 5
        """
        _ = self.__copy__()
        _._sql = sql
        return _

    def _make_cls(self,key,load,na = ''):
        """Add keyword to clause payload"""
        return f"{key}{load}" if load is not None else na

    def _make_qry(self):
        """Stitch together query components."""
        if self._sql is not None:
            self._qry = self._sql
        else:
            select = self._make_cls('select ',self._select,'select *')
            from_ = self._make_cls('from ',self._from_,'from test')
            join = self._make_cls('\n',self._join)
            where = self._make_cls('\nwhere ',self._where)
            group_by = self._make_cls('\ngroup by ',self._group_by)
            having = self._make_cls('\nhaving ',self._having)
            order_by = self._make_cls('\norder by ',self._order_by)
            self._qry = (
                select 
                + (' ' if select == 'select *' else '\n')
                + from_ 
                + join
                + where 
                + group_by 
                + having 
                + order_by
            )

    def __str__(self):
        self._make_qry()
        return self._qry

    def print(self):
        """Print the underlying query.

        Examples
        --------
        >>> from dwopt import lt
        >>> lt.qry().print()
            select * from test
        """
        self._make_qry()
        print(self)

    def run(self,sql = None,*args,**kwargs):
        """
        Run the underlying query directly, without using it to make summary
        queries.

        Parameters
        ----------
        sql :
        *args :
            Positional arguments to pass on to database operator's run method.
        **kwargs :
            Keyword arguments to pass on to database operator's run method.

        Returns
        -------
        pandas.DataFrame or None
            Returned by the database operator's run method.

        Examples
        --------
        >>> import pandas as pd
        >>> from dw import lt
        >>> 
        >>> tbl = pd.DataFrame({'col1': [1, 2], 'col2': ['a', 'b']})
        >>> lt.drop('test')
        >>> lt.create('test',{'col1':'int','col2':'text'})
        >>> lt.write(tbl,'test')
        >>> lt.qry("test").where("col2 = 'b'").run()
            col1  col2
        0     2     b
        """
        self._make_qry()
        if sql is not None:
            _ = self._qry.replace('\n','\n    ')
            _ = (
                "with x as (\n"
                f"    {_}\n"
                ")"
            )
            qry = f"{_}\n{sql}"
        else:
            qry = self._qry
        return self._ops.run(qry,*args,**kwargs)

    from dwopt._sqls.base import head
    from dwopt._sqls.base import top
    from dwopt._sqls.base import cols
    from dwopt._sqls.base import len
    from dwopt._sqls.base import dist
    from dwopt._sqls.base import mimx
    from dwopt._sqls.base import valc
    from dwopt._sqls.base import hash

class PgQry(_Qry):
    pass

class LtQry(_Qry):
    pass

class OcQry(_Qry):
    def _make_qry(self):
        super()._make_qry()
        self._qry = self._qry.replace('select','select /*+PARALLEL (4)*/')

    from dwopt._sqls.oc import head
    from dwopt._sqls.oc import top
    from dwopt._sqls.oc import hash