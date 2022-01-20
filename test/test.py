import pandas as pd
from dw import pg
import logging
import random
logging.basicConfig(
    level = logging.INFO
    ,format = "%(asctime)s [%(levelname)s] %(message)s")

n = 10000
df = pd.DataFrame(
    {
        "id": range(n)
        ,"score": [random.random() for i in range(n)]
        ,"amt": [random.choice(range(1000)) for i in range(n)]
        ,"cat": [random.choice(["test", "train"]) for i in range(n)]
        ,'time': [
                random.choice(
                    [pd.Timestamp(i) for i in 
                        ["20130102","20130202","20130302"]]
                ) 
                for i in range(n)]
    }
)
df.head()

pg.drop('test')
pg.create(
        tbl_nme = 'test'
        ,dtypes = {
            'id':'integer'
            ,'score':'float8'
            ,'amt':'integer'
            ,'cat':'varchar(20)'
            ,'time':'timestamp'
            ,'constraint df_pk':
                'primary key (id)'
        }
    )
pg.write(df,'test')

pg.qry('test').where("score > 0.5") \
.valc('time,cat',"avg(score) avgscore,round(sum(amt)/1e3,2) total") \
.pivot('time','cat',['n','avgscore','total'])

pg.run('select * from test limit 2')
pg.run('select * from test where score > :score limit 2'
    ,args = {'score':'0.9'})
pg.run('select * from test where score > :score limit 2'
    ,score = 0.9)

pg.qry('test').top()

pg.qry('test').head()

pg.qry('test').len()

pg.qry('test').select("id,score,amt").top()
pg.qry('test').select(["id","score","amt"]).top()
pg.qry('test').select("id","score","amt").top()
pg.qry('test').select("\n    id","score","amt",sep = "\n    ,").head()

pg.qry('test').case('flg',"id>2 then 'big'","id<=2 then 'small'").head()
pg.qry('test').case('flg',"id=2 then 'middle'" \
    ,cond = {"id>2":"'big'","id<2":"'small'"}).head()

(
    pg.qry('test x')
    .select('x.id','y.id as yid','x.score','z.score as zscore')
    .join("test y","x.id = y.id+1","x.id <= y.id+1")
    .join("test z","x.id = z.id+2","x.id >= z.id+1")
    .where('x.id < 10','z.id < 10')
    .head()
)

(
    pg.qry('test x')
    .select('x.cat,y.cat as bcat'
        ,'sum(x.score) bscore','sum(y.score) yscore','count(1) n')
    .join("test y","x.id = y.id+1")
    .where('x.id < 1000')
    .group_by('x.cat,y.cat')
    .having('count(1) > 50','sum(y.score) > 100')
    .order_by('cat desc','yscore desc')
    .run()
)

