from dwopt import make_test_tbl
import pytest
from pathlib import Path
import keyring
import os

_CONFIG_PTH = Path.home() / ".dwopt"
_KEYRING_SERV_ID = (Path(__file__).parents[1] / "src" / "dwopt").resolve().as_posix()


def _clean_up_credential():
    if _CONFIG_PTH.exists():
        _CONFIG_PTH.unlink()
    for db_nme in ["pg", "lt", "oc"]:
        try:
            keyring.delete_password(_KEYRING_SERV_ID, db_nme)
        except Exception as ex:
            print(ex)
        environ_variable = f"dwopt_{db_nme}"
        if environ_variable in os.environ.keys():
            os.environ.pop(environ_variable)


@pytest.fixture()
def creds():
    pg_url = "postgresql://tiger:!@#aD123@localhost/mydatabase"
    lt_url = "sqlite:////E:/db.sqlite"
    oc_url = "oracle://tiger:!@#aD123@tnsname"
    _clean_up_credential()
    yield pg_url, lt_url, oc_url
    _clean_up_credential()


def pytest_addoption(parser):
    parser.addoption(
        "--db",
        action="append",
        default=["lt"],
        help="list of database to test",
    )


def pytest_generate_tests(metafunc):
    if "test_tbl" in metafunc.fixturenames:
        metafunc.parametrize("test_tbl", metafunc.config.getoption("db"), indirect=True)


@pytest.fixture(scope="session", autouse=True)
def test_tbl(request):
    if request.param == "pg":
        db, df = make_test_tbl("pg", "test")
    elif request.param == "lt":
        db, df = make_test_tbl("lt", "test")
        print(type(db))
    elif request.param == "oc":
        db, df = make_test_tbl("oc", "test")
    else:
        raise ValueError("invalid test command line input")
    yield db, df
    db.drop("test")


@pytest.fixture()
def test_tbl2(test_tbl):
    db, _ = test_tbl
    db.drop("test2")
    yield
    db.drop("test2")
