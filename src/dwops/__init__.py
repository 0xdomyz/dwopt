from .db import Pg, Lt, Oc
from .urls import (
    save_url
    ,make_eng
    ,_save_dummy_url_if_not_exist
    ,_get_url_default_lt
)

_save_dummy_url_if_not_exist()
pg = Pg(make_eng(_get_url_default_lt('pg')))
lt = Lt(make_eng(_get_url_default_lt('lt')))
oc = Oc(make_eng(_get_url_default_lt('oc')))