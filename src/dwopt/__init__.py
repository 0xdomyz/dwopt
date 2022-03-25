from .dbo import db, Db, Pg, Lt, Oc
from .set_up import save_url, make_eng, _get_url
from .testing import make_test_tbl

pg = db(_get_url("pg"))
lt = db(_get_url("lt"))
oc = db(_get_url("oc"))

d = db
l = lt
m = make_test_tbl
o = oc
p = pg
s = save_url
