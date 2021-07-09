# pycms
> Tools to work with MRO MCS data, for LASP team members


```python
# import the class definition
from pymcs.db_tools import MCSDB
```

```python
# create a class instance
# it will automatically connect to the database and keep the connection alive:
db = MCSDB()
```

```python
db.con
```

```python
print(db.example_sql)
```

```python
# execute this quick test query to see if everything works:
db.test_call().plot()
```

```python
db.profile_columns
```

```python
db.header_columns
```

```python
db.limb_columns
```

```python
db.nadir_columns
```

```python
df = db.get_profile_cols_by_date(["pressure", "temperature", "obstime", "altitude"], "20080101")
df.head()
```

```python
df.ALTITUDE.value_counts()
```

```python
sql = """
select temperature, dust, latitude, longitude, obsdate
from mcs_profile_data
where pressure between 45 and 55
and obsdate between 20080830 and 20080920
"""
```

```python
db.query(sql)
```

```python
sql = """
select surfacetemperature
from mcs_header_data
where obsdate = 20070101
"""
```

```python
db.query(sql)
```

```python
sql = """
select obsdate, sclk, ls, localtime, nadirlatitude, nadirlongitude, surfacetemperature, surfaceterror, surfacepressure, surfacepuncertainty, DUST_463_NAD_OD, DUST_463_NAD_OD_ERR
from mcs_profile_data
where nadirlatitude between -86.994 and -86.842
and nadirlongitude between -7.887 and -5.97
and obsdate between 20070310 and 20210530
"""
```

```python
db.query(sql)
```

```python
sql = """
select surfacetemperature
from mcs_header_data
where obsdate = 20190523
"""

df = db.query(sql)
df[df < 0] = np.nan
```

```python
import hvplot.pandas
```

```python
df.SURFACETEMPERATURE.hvplot.hist(bins=50)
```

```python
df.info()
```

```python
sql = """
select temperature, dust, latitude, longitude, obsdate
from mcs_profile_data
where pressure between 45 and 55
and obsdate between 20080830 and 20080920
"""
```

```python
db.query(sql)
```
