# specialize for if pyload is a dict, when most of time it is jsut a str
import json

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


# save thick mode as well
# option1: new save_url func that saves thick mode paths, additional param in save_url
# option2: save from a initialised engine, which has thick mode paths
