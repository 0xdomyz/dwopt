from .dbo import db, Db, Pg, Lt, Oc
from .set_up import save_url, make_eng, _get_url
from .testing import make_test_tbl

pg = Pg(make_eng(_get_url("pg")))
lt = Lt(make_eng(_get_url("lt")))
oc = Oc(make_eng(_get_url("oc")))

d = db
l = lt
m = make_test_tbl
o = oc
p = pg
s = save_url
