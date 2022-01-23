from .db import make_eng,Pg,Lt,Oc

import os
_pth = os.path.dirname(__file__)
def get_url(nme):
    fp = f'{_pth}\\urls\\{nme}.txt'
    with open(fp,"r") as f:
        return f.readline().rstrip()

lt = Lt(make_eng('sqlite://'))
pg = Pg(make_eng(get_url('psql_default')))
oc = Oc(make_eng(get_url('oc_default')))
