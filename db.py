import sqlalchemy as alc
import pandas as pd
import logging as logging
from dw.fil import get_key
from dw._qry import PgQry, LtQry, OcQry
_logger = logging.getLogger(__name__)

class _Db:
    def __init__(self,key_nme):
        self.key_nme = key_nme
        self.url = get_key(self.key_nme)[0]
        self.eng = alc.create_engine(self.url)

    def con(self):
        return self.eng.connect()

    def run(self,sql):
        with self.con() as c:
            _logger.info('running:')
            _logger.info(sql)
            r = c.execute(sql)
            if r.returns_rows:
                _ = pd.DataFrame(r.all(),columns = r.keys())
                _logger.info(f'done, len: {len(_)}')
            else:
                _logger.info('done')
                _ = None
        return _

    def create(self,sch_tbl_nme,sql):
        return (
            f'create table {sch_tbl_nme} '
            f'\n{sql}'
        )

    def write(self,tbl,sch_tbl_nme):
        sch,tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        _logger.info(f'writing to {sch_tbl_nme}, len: {len(tbl)}')
        with self.con() as c:
            tbl.to_sql(tbl_nme,c,sch,if_exists = 'append',index=False)
        _logger.info(f'done')

    def write_nodup(self,tbl,sch_tbl_nme,pkey,where = None):
        cols = ','.join(pkey)
        where_cls = f'\nwhere {where}' if where else ''
        db_tbl = self.run(f"select {cols} from {sch_tbl_nme} {where_cls}")
        dedup_tbl = (
            tbl
            .merge(db_tbl,how='left',on = pkey
                ,validate='one_to_one',indicator=True)
            .loc[lambda x:x._merge == 'left_only',:]
            .drop(columns='_merge')
        )
        self.write(dedup_tbl,sch_tbl_nme)

    def drop(self,tbl_nme):
        try:
            self.run(f'drop table {tbl_nme}')
        except Exception as ex:
            return str(ex)
        else:
            return f'{tbl_nme} dropped'

    def _parse_sch_tbl_nme(self,sch_tbl_nme):
        _ = sch_tbl_nme.split('.')
        n = len(_)
        if n == 1:
            sch = None
            tbl_nme = _[0]
        elif n == 2:
            sch = _[0]
            tbl_nme = _[1]
        else:
            sch = _[0]
            tbl_nme = '.'.join(_[1:])
            tbl_nme = f'"{tbl_nme}"'
        return sch, tbl_nme

class Pg(_Db):
    def list_tables(self):
        sql = (
            "select table_catalog,table_schema,table_name"
            "\n    ,is_insertable_into,commit_action"
            "\nfrom information_schema.tables"
            "\nwhere table_schema"
            "\n   not in ('information_schema','pg_catalog')"
        )
        print(sql)
        return self.run(sql)

    def table_cols(self,sch_tbl_nme):
        sch,tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
        sql = (
            "select column_name, data_type from information_schema.columns"
            f"\nwhere table_schema = '{sch}' "
            f"\nand table_name = '{tbl_nme}'"
        )
        print(sql)
        return self.run(sql)

    def add_pkey(self,tbl_nme,pkey):
        sql = f"alter table {tbl_nme} add primary key ({pkey})"
        print(sql)
        return self.run(sql)

    def qry(self,*args,**kwargs):
        return PgQry(self,*args,**kwargs)

class Lt(_Db):
    def list_tables(self):
        sql = (
            "select * from sqlite_schema "
            "\nwhere type ='table' "
            "\nand name NOT LIKE 'sqlite_%' "
        )
        print(sql)
        return self.run(sql)

    def qry(self,*args,**kwargs):
        return LtQry(self,*args,**kwargs)

class Oc(_Db):
    def list_tables(self):
        sql = (
            "select * from all_table_columns "
            "\nwhere rownum < 5"
        )
        print(sql)
        return self.run(sql)

    def qry(self,*args,**kwargs):
        return OcQry(self,*args,**kwargs)




