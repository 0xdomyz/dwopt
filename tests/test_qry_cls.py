import io
from contextlib import redirect_stdout

from pandas.testing import assert_frame_equal

_SQL = "select count(1) from test"


def test_qry_ops_run(test_tbl):
    db, df = test_tbl
    act = db.qry(sql=_SQL).run()
    exp = db.run(_SQL)
    assert_frame_equal(act, exp)


def test_qry_ops_print(test_tbl):
    db, df = test_tbl
    with redirect_stdout(io.StringIO()) as f:
        db.qry(sql=_SQL).print()
    act = f.getvalue()
    exp = _SQL
    assert act.strip() == exp.strip()


def test_qry_cls_select(test_tbl):
    db, df = test_tbl

    act = db.qry("test").select("id, score, amt").run()
    exp = db.run(
        """
select id, score, amt
from test
    """
    )
    assert_frame_equal(act, exp)

    act = db.qry("test").select("id", "score", "amt").run()
    assert_frame_equal(act, exp)

    act = db.qry("test").select(["id", "score", "amt"]).run()
    assert_frame_equal(act, exp)


def test_qry_cls_case(test_tbl):
    db, df = test_tbl
    act = (
        db.qry("test")
        .select("test.*")
        .case("col", "score >= 0.5 then 'A'", els="'B'")
        .run()
    )
    exp = db.run(
        """
select test.*,
    case when score >= 0.5 then 'A' else 'B' end as col
from test
    """
    )
    assert_frame_equal(act, exp)


def test_qry_cls_from_(test_tbl):
    db, df = test_tbl
    act = db.qry().from_("test").run()
    exp = db.run("select * from test")
    assert_frame_equal(act, exp)


def test_qry_cls_join(test_tbl):
    db, df = test_tbl
    act = (
        db.qry("test x")
        .select("x.id", "x.score", "y.score as scorey", "z.score as scorez")
        .join("test y", "x.id = y.id+1")
        .join("test z", "x.id = z.id+2")
        .where("x.id <= 100")
        .run()
    )
    exp = db.run(
        """
select 
    x.id, x.score, y.score as scorey, z.score as scorez
from test x
left join test y on x.id = y.id+1
left join test z on x.id = z.id+2
where x.id <= 100
    """
    )
    assert_frame_equal(act, exp)


def test_qry_cls_where(test_tbl):
    db, df = test_tbl
    act = db.qry("test").where("score > 0.5").run()
    exp = db.run(
        """
select * from test where score > 0.5
    """
    )
    assert_frame_equal(act, exp)


def test_qry_cls_group_by(test_tbl):
    db, df = test_tbl
    act = db.qry("test").select("cat, count(1) n").group_by("cat").run()
    exp = db.run(
        """
select cat, count(1) as n
from test
group by cat
    """
    )
    assert_frame_equal(act, exp)


def test_qry_cls_having(test_tbl):
    db, df = test_tbl
    act = (
        db.qry("test")
        .select("cat, count(1) n")
        .group_by("cat")
        .having("count(1) > 100")
        .run()
    )
    exp = db.run(
        """
select cat, count(1) as n
from test
group by cat
having count(1) > 100
    """
    )
    assert_frame_equal(act, exp)


def test_qry_cls_order_by(test_tbl):
    db, df = test_tbl
    act = db.qry("test").order_by("id desc").run()
    exp = db.run(
        """
select *
from test
order by id desc
    """
    )
    assert_frame_equal(act, exp)


def test_qry_cls_sql(test_tbl):
    db, df = test_tbl
    sql = """
select * from test
where score > 0.5
    """
    act = db.qry().sql(sql).run()
    exp = db.run(sql)
    assert_frame_equal(act, exp)
