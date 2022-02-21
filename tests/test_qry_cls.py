from mimetypes import suffix_map
from pandas.testing import assert_frame_equal, assert_series_equal, assert_index_equal
import numpy as np

def test_qry_cls_select(db_df):
    db, df = db_df

    act = db.qry('test').select('id, score, amt').run()
    exp = db.run("""
select id, score, amt
from test
    """)
    assert_frame_equal(act,exp)

    act = db.qry('test').select('id', 'score', 'amt').run()
    assert_frame_equal(act,exp)

    act = db.qry('test').select(['id', 'score', 'amt']).run()
    assert_frame_equal(act,exp)


def test_qry_cls_case(db_df):
    db, df = db_df
    act = db.qry("test").select("test.*") \
        .case('col', "score >= 0.5 then 'A'", els = "'B'").run()
    exp = db.run("""
select test.*,
    case when score >= 0.5 then 'A' else 'B' end as col
from test
    """)
    assert_frame_equal(act, exp) 


def test_qry_cls_from_(db_df):
    db, df = db_df
    act = db.qry().from_('test').run()
    exp = df
    assert_frame_equal(act, exp) 


def test_qry_cls_join(db_df):
    db, df = db_df
    act = (
        db.qry('test x')
        .select('x.id', 'x.score', 'y.score as scorey', 'z.score as scorez')
        .join("test y", "x.id = y.id+1")
        .join("test z", "x.id = z.id+2")
        .where("x.id <= 100")
        .run()
    )
    exp = db.run("""
select 
    x.id, x.score, y.score as scorey, z.score as scorez
from test x
left join test y on x.id = y.id+1
left join test z on x.id = z.id+2
where x.id <= 100
    """)
    assert_frame_equal(act, exp)


def test_qry_cls_where(db_df):
    db, df = db_df
    act = db.qry('test').where('score > 0.5').run()
    exp = db.run("""
select * from test where score > 0.5
    """)
    assert_frame_equal(act, exp)


def test_qry_cls_group_by(db_df):
    db, df = db_df
    act = db.qry('test').select('cat, count(1) n').group_by('cat').run()
    exp = db.run("""
select cat, count(1) as n
from test
group by cat
    """)
    assert_frame_equal(act, exp)


def test_qry_cls_having(db_df):
    db, df = db_df
    act = (
        db.qry('test').select('cat, count(1) n')
        .group_by('cat').having("count(1) > 100").run()
    )
    exp = db.run("""
select cat, count(1) as n
from test
group by cat
having count(1) > 100
    """)
    assert_frame_equal(act, exp)


def test_qry_cls_order_by(db_df):
    db, df = db_df
    act = db.qry('test').order_by('id desc').run()
    exp = db.run("""
select *
from test
order by id desc
    """)
    assert_frame_equal(act, exp)


def test_qry_cls_sql(db_df):
    db, df = db_df
    sql = """
select * from test
where score > 0.5
    """
    act = db.qry().sql(sql).run()
    exp = db.run(sql)
    assert_frame_equal(act, exp)

