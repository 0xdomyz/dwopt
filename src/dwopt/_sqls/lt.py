import numpy as np
import sqlalchemy as alc

# db method


def list_tables(self):
    sql = (
        "select * from sqlite_master "
        "\nwhere type ='table' "
        "\nand name NOT LIKE 'sqlite_%' "
    )
    return self.run(sql)


def _guess_dtype(self, dtype):
    if np.issubdtype(dtype, np.float64):
        return alc.REAL
    elif np.issubdtype(dtype, np.datetime64):
        return alc.String
    else:
        return super(type(self), self)._guess_dtype(dtype)
