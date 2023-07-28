# specialize for if pyload is a dict, when most of time it is jsut a str
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


# base64 string, even if string is a dict's json.dumps
import base64

# json dumps write into a config file
# a = json.dumps(
#     {"mode": "thick", "dir": "E:/asdf/asdf", "password": "asdf3qrfds://asdf+sdaf"}
# )
# a
# from configparser import ConfigParser

# _CONFIG_PTH = "config.ini"
# cfg = ConfigParser()


def _encode(x: str) -> str:
    """
    Examples
    -------------
    ::

        _encode("asdf3qrfds+asdf2://asdf+sdaf")
    """
    if x is not None:
        return base64.b64encode(x.encode("UTF-8")).decode("UTF-8")


def _decode(x):
    """
    Examples
    -------------
    ::

        _decode("YXNkZjNxcmZkcythc2RmMjovL2FzZGYrc2RhZg==")
    """

    if x is not None:
        return base64.b64decode(x.encode("UTF-8")).decode("UTF-8")


triage_loads(_decode(_encode(a)))

# save thick mode as well
# option1: new save_url func that saves thick mode paths, additional param in save_url
# option2: save from a initialised engine, which has thick mode paths


# connect to oracle via sqlalchemy thick mode
import pandas as pd
import sqlalchemy as alc

# url
url_base = "dwopt_test:1234@localhost:1521/?service_name=XEPDB1 &encoding=UTF-8&nencoding=UTF-8"

# use just oracle
url = f"oracle://{url_base}"
eng = alc.create_engine(url, echo=True)
df = pd.read_sql("select * from dual", eng)
df

# oracledb
url = f"oracle+oracledb://{url_base}"
bin_path = r"C:\app\yzdom\product\21c\dbhomeXE\bin"
eng = alc.create_engine(url, echo=True, thick_mode={"lib_dir": bin_path})
df = pd.read_sql("select * from dual", eng)
df

# thin mode
url = f"oracle+oracledb://{url_base}"
eng = alc.create_engine(url, echo=True)
df = pd.read_sql("select * from dual", eng)
df

# config save and read dict
from configparser import ConfigParser

_CONFIG_PTH = "config.ini"
db_nme = "oc"

cfg = ConfigParser()
cfg.read(_CONFIG_PTH)
if not cfg.has_section("url"):
    cfg.add_section("url")
cfg.set("url", db_nme, url)
with open(_CONFIG_PTH, "w") as f:
    cfg.write(f)

cfg = ConfigParser()
cfg.read(_CONFIG_PTH)
if cfg.has_option("url", db_nme):
    url = _decode(cfg.get("url", db_nme))
if url is not None:
    print(url)


# save url save additional args
def save_url(db_nme, url, method="keyring"):
    pass


alc.create_engine(url, db_nme="a")
alc.create_engine(url, method="a")


def save_url(db_nme, url, method="keyring", **kwargs):
    kwargs.update({"url": url})
    new_url = json.dumps(kwargs)
    return new_url


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
    else:
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
