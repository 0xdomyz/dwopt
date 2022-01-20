## dw - database operator package

### Run sql statement
support argument passing, text replacement, read sql from file \
provide function for DDL/DML on database table and python dataframe \
primitive to query running processes, uses logging package for audit trails

### Programmatically generate simple sql query
based on text manipulation

### Generate and run summary queries
build after generated query in the previous step, results as pandas dataframe \
query generation to summary results end-to-end within python

caters for sqlite, postgre and oracle dialects

```python
from dw import pg
pg.qry('test').where("score > 0.5") \
.valc('time,cat',"avg(score) avgscore,round(sum(amt)/1e3,2) total") \
.pivot('time','cat',['n','avgscore','total'])
```
```<!-- language: none -->
2022-01-20 23:52:44,145 [INFO] running: 
with x as (
    select * from test
    where score > 0.5
)
select
    time,cat
    ,count(1) n
    ,avg(score) avgscore,round(sum(amt)/1e3,2) total
from x
group by time,cat
order by n desc
2022-01-20 23:52:44,184 [INFO] done
```

<table border=\"1\" class=\"dataframe\">
  <thead>
    <tr>
      <th></th>
      <th>n</th>
      <th></th>
      <th>avgscore</th>
      <th></th>
      <th>total</th>
    </tr>
    <tr>
      <th>cat</th>
      <th>test</th>
      <th>train</th>
      <th>test</th>
      <th>train</th>
      <th>test</th>
      <th>train</th>
    </tr>
    <tr>
      <th>time</th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
      <th></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>2013-01-02</th>
      <td>855</td>
      <td>789</td>
      <td>0.748191</td>
      <td>0.750798</td>
      <td>423.06</td>
      <td>393.58</td>
    </tr>
    <tr>
      <th>2013-02-02</th>
      <td>828</td>
      <td>829</td>
      <td>0.747892</td>
      <td>0.755623</td>
      <td>402.04</td>
      <td>415.65</td>
    </tr>
    <tr>
      <th>2013-03-02</th>
      <td>839</td>
      <td>855</td>
      <td>0.751682</td>
      <td>0.75397</td>
      <td>423.70</td>
      <td>419.70</td>
    </tr>
  </tbody>
</table>
