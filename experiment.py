# connect to oracle via sqlalchemy thick mode
######################################################
import pandas as pd
import sqlalchemy as alc

# url
url_base = "dwopt_test:1234@localhost:1521/?service_name=XEPDB1 &encoding=UTF-8&nencoding=UTF-8"

# thin mode
url = f"oracle+oracledb://{url_base}"
eng = alc.create_engine(url, echo=True)
df = pd.read_sql("select * from dual", eng)
df

# thick mode
url = f"oracle+oracledb://{url_base}"
bin_path = r"C:\app\yzdom\product\21c\dbhomeXE\bin"
eng = alc.create_engine(url, echo=True, thick_mode={"lib_dir": bin_path})
df = pd.read_sql("select * from dual", eng)
df


# new save_url func that saves thick mode paths, additional param in save_url
######################################################
def save_url(db_nme, url, method="keyring", **kwargs):
    kwargs.update({"url": url})
    new_url = json.dumps(kwargs)
    # precede as before
    return new_url


# todo: new save method, in an initialised engine, which has thick mode paths
######################################################


# new load func that load both formats str into dict
######################################################
def triage_loads(raw):
    if '"url":' in raw:
        res = json.loads(raw)
        if isinstance(res, dict):
            url = res["url"]
            res.pop("url")
            kwargs = res
        elif isinstance(res, str):
            url = res
            kwargs = {}
        else:
            raise ValueError(f"unknown loaded url format: {res}")
    else:  # backward compatibility
        url = raw
        kwargs = {}

    return url, kwargs


dict_form = save_url("oc", url=url, method="keyring", thick_mode={"lib_dir": bin_path})
dict_form
triage_loads(dict_form)

dict_form_wo_kwargs = save_url("oc", url=url, method="keyring")
dict_form_wo_kwargs
triage_loads(dict_form_wo_kwargs)

raw_url_form = url
raw_url_form
triage_loads(raw_url_form)

# related codes
#####################

# payload(dict or str) to str, then back. Via json.dumps and json.loads
import json
from typing import Any, Dict, Union

a = json.dumps(
    {"mode": "thick", "dir": "E:/asdf/asdf", "password": "asdf3qrfds://asdf+sdaf"}
)
b = json.dumps("asdf3qrfds://asdf+sdaf")


def triage_loads(payload):
    res = json.loads(payload)

    if isinstance(res, dict):
        print(res["mode"])
        print(res["dir"])
        print(res["password"])
    else:
        print(res)


triage_loads(a)
triage_loads(b)
