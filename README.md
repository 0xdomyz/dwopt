## dw - database operator

### Run sql statement in python
support argument passing, text replacement, read sql from file \
function for DDL/DML on database table and pandas dataframe \
logging via the logging package

### Programmatically generate simple sql query
based on string injection

### Generate and run summary queries
utlise data manipulation capability of database instead of passing \
large raw data to python which may not be feasible \
summary query results as pandas dataframe for access to python toolkit \

aim to cater for sqlite, postgre and oracle dialects

```python
from dw import lt
lt.qry('test').where("score > 0.5") \
.valc('time,cat',"avg(score) avgscore,round(sum(amt)/1e3,2) total") \
.pivot('time','cat',['n','avgscore','total'])
```
```<!-- language: none -->
2022-01-23 01:08:13,407 [INFO] running:
with x as (
    select * from test
    where score > 0.5
)
select 
    time,cat
    ,count(1) n
    ,avg(score) avgscore, round(sum(amt)/1e3,2) total
from x
group by time,cat
order by n desc
2022-01-23 01:08:13,413 [INFO] done
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
      <td>816.0</td>
      <td>847.0</td>
      <td>0.746747</td>
      <td>0.750452</td>
      <td>398.34</td>
      <td>417.31</td>
    </tr>
    <tr>
      <th>2013-02-02</th>
      <td>837.0</td>
      <td>858.0</td>
      <td>0.748214</td>
      <td>0.743094</td>
      <td>419.11</td>
      <td>447.04</td>
    </tr>
    <tr>
      <th>2013-03-02</th>
      <td>805.0</td>
      <td>860.0</td>
      <td>0.756775</td>
      <td>0.739017</td>
      <td>394.89</td>
      <td>422.35</td>
    </tr>
  </tbody>
</table>
