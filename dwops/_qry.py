class _Qry:
    """ """
    print_ = False

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

        :param args: 
        :param sep: 

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

        :param *args: 
        :param sep:  (Default value = ')
        :param ': 

        """
        _ = self.__copy__()
        _._select = self._args2str(args,sep)
        return _

    def case(self,col,*args,cond = None,els = 'NULL'):
        """

        :param col: 
        :param *args: 
        :param cond:  (Default value = None)
        :param els:  (Default value = 'NULL')

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

        :param from_: 

        """
        _ = self.__copy__()
        _._from_ = from_
        return _

    def join(self,tbl,*args,how = 'left'):
        """

        :param tbl: 
        :param *args: 
        :param how:  (Default value = 'left')

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

        :param *args: 

        """
        _ = self.__copy__()
        _._where = self._args2str(args,'\n    and ')
        return _

    def group_by(self,*args):
        """

        :param *args: 

        """
        _ = self.__copy__()
        _._group_by = self._args2str(args,',')
        return _

    def having(self,*args):
        """

        :param *args: 

        """
        _ = self.__copy__()
        _._having = self._args2str(args,'\n    and ')
        return _

    def order_by(self,*args):
        """

        :param *args: 

        """
        _ = self.__copy__()
        _._order_by = self._args2str(args,',')
        return _

    def sql(self,sql):
        """

        :param sql: 

        """
        _ = self.__copy__()
        _._sql = sql
        return _

    def _make_cls(self,key,load,na = ''):
        """

        :param key: 
        :param load: 
        :param na:  (Default value = '')

        """
        return f"{key}{load}" if load is not None else na

    def _make_qry(self):
        """ """
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
        """ """
        self._make_qry()
        print(self)

    def run(self,sql = None,*args,**kwargs):
        """

        :param sql:  (Default value = None)
        :param *args: 
        :param **kwargs: 

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

    from dw._sqls.base import head
    from dw._sqls.base import top
    from dw._sqls.base import cols
    from dw._sqls.base import len
    from dw._sqls.base import dist
    from dw._sqls.base import mimx
    from dw._sqls.base import valc

class PgQry(_Qry):
    """ """
    pass

class LtQry(_Qry):
    """ """
    pass

class OcQry(_Qry):
    """ """

    def _make_qry(self):
        """ """
        super()._make_qry()
        self._qry = self._qry.replace('select','select /*+PARALLEL (4)*/')

    from dw._sqls.oc import head
    from dw._sqls.oc import top
    from dw._sqls.oc import hash



