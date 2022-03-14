import pandas as pd
import numpy as np
import sqlalchemy as alc
from sqlalchemy.dialects.oracle import NUMBER

# db method


def list_tables(self, owner):
    sql = (
        "select/*+PARALLEL (4)*/ owner,table_name"
        "\n    ,max(column_name),min(column_name)"
        "\nfrom all_tab_columns"
        f"\nwhere owner = '{owner.upper()}'"
        "\ngroup by owner,table_name"
    )
    return self.run(sql)


def table_sizes(self):
    sql = (
        "select/*+PARALLEL (4)*/"
        "\n    tablespace_name,segment_type,segment_name"
        "\n    ,sum(bytes)/1024/1024 table_size_mb"
        "\nfrom user_extents"
        "\ngroup by tablespace_name,segment_type,segment_name"
    )
    return self.run(sql)


def table_cols(self, sch_tbl_nme):
    sch, tbl_nme = self._parse_sch_tbl_nme(sch_tbl_nme)
    sql = (
        "select/*+PARALLEL (4)*/ *"
        "\nfrom all_tab_columns"
        f"\nwhere owner = '{sch.upper()}'"
        f"\nand table_name = '{tbl_nme.upper()}'"
    )
    return self.run(sql)


def _guess_dtype(self, dtype):
    if np.issubdtype(dtype, np.int64):
        return NUMBER
    else:
        return super(type(self), self)._guess_dtype(dtype)


# qry method


def head(self):
    return self.run("select * from x where rownum<=5")


def top(self):
    res = self.run("select * from x where rownum<=1")
    if res.empty:
        return pd.Series(index=res.columns)
    else:
        return res.iloc[
            0,
        ]


def hash(self, *args):
    if len(args) == 0:
        args = self.cols()
    _ = args[0] if len(args) == 1 and not isinstance(args[0], str) else args
    _ = " || '_' || ".join(_)
    _ = (
        "select/*+ PARALLEL(4) */ \n"
        "    ora_hash(sum(ora_hash(\n"
        f"        {_}\n"
        "    ) - 4294967296/2)) hash\n"
        "from x"
    )
    return self.run(_).iloc[0, 0]
