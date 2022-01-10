class _Qry:
    print_ = False

    def __init__(self
            ,operator
            ,from_ = None,select = None,left_join = None,where = None
            ,order = None,sql = None):
        self._ops = operator
        self._from = from_
        self._select = (select,) if isinstance(select,str) else select
        self._left_join = left_join
        self._where = where
        self._order = order
        self._sql = sql
        self._make_qry()

    def __copy__(self):
        return type(self)(
             self._ops,self._from,self._select,self._left_join,self._where
            ,self._order,self._sql
        )

    def _make_qry(self):
        if self._sql is not None:
            self._qry = self._sql
        else:
            select = (
                f"select {','.join(self._select)}" 
                if self._select is not None else 'select *'
            )
            _ = '\n' if len(select) > 60 else ' '
            from_ = (
                f'from {self._from}' if self._from is not None else 'from test'
            )
            self._qry = select + _ + from_
            if self._left_join is not None:
                for i,j in self._left_join:
                    left = (
                        f'\nleft join {i}\n'
                        f'on {j}'
                    )
                    self._qry = self._qry + left
            where = (
                f'\nwhere {self._where}'
                if self._where is not None 
                else ''
            )
            self._qry = self._qry + where
            order = (
                f'order by {self._order}' if self._order is not None else ''
            )
            _ = '\n' if len(where) == 0 or len(where)> 40 else ' '
            self._qry = self._qry + _ + order

    def select(self,*args):
        _ = self.__copy__()
        _._select = args[0] if not isinstance(args[0],str) else args
        _._make_qry()
        return _

    def from_(self,from_):
        _ = self.__copy__()
        _._from = from_
        _._make_qry()
        return _

    def left_join(self,tbl,on):
        _ = self.__copy__()
        if _._left_join is not None:
            _._left_join = _._left_join + [(tbl,on)]
        else:
            _._left_join = [(tbl,on)]
        _._make_qry()
        return _

    def where(self,where):
        _ = self.__copy__()
        _._where = where
        _._make_qry()
        return _

    def order_by(self,order):
        _ = self.__copy__()
        _._order = order
        _._make_qry()
        return _

    def sql(self,sql):
        _ = self.__copy__()
        _._sql = sql
        _._make_qry()
        return _

    def __str__(self):
        return self._qry

    def print(self):
        print(self)

    def run(self,sql = None):
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
        if self.print_:
            print(qry)
        return self._ops.run(qry)

    from dw._sqls.base import head
    from dw._sqls.base import first
    from dw._sqls.base import cols
    from dw._sqls.base import len
    from dw._sqls.base import dist
    from dw._sqls.base import mimx
    from dw._sqls.base import valc

class PgQry(_Qry):
    pass

class LtQry(_Qry):
    pass

class OcQry(_Qry):
    from dw._sqls.oc import head
    from dw._sqls.oc import first
    from dw._sqls.oc import hash



