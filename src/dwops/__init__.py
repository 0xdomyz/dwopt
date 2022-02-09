from .db import Pg, Lt, Oc
from .urls import (
    save_url
    ,make_eng
    ,_save_dummy_url_if_not_exist
    ,_get_url
)

_save_dummy_url_if_not_exist()
pg = Pg(make_eng(_get_url('pg')))
lt = Lt(make_eng(_get_url('lt')))
oc = Oc(make_eng(_get_url('oc')))