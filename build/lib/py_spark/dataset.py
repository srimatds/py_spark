from pyspark.sql import Row
from datetime import datetime, date

##dataset with relevant schema
dataset_withschema = ([
    Row(car='Audi', date=date(2021, 1, 10), color='red', price=int(20000), sold=1000),
    Row(car='Audi', date=date(2021, 1, 20), color='red', price=int(300000), sold=500),
    Row(car='Audi', date=date(2021, 1, 3), color='green', price=int(0), sold=100),
    Row(car='BMW', date=date(2021, 1, 1), color='yellow', price=int(10000), sold=100),
    Row(car='BMW', date=date(2020, 1, 20), color='blue', price=int(20000), sold=150),
    Row(car='BMW', date=date(2020, 1, 3), color='blue', price=int(25000), sold=80),
])

##dataset without schema
dataset_withoutschema=(['Audi', "2021-1-10",'red',20000,1000,"2021-1-10 11:09:23.000"],
['Audi', "2021-1-20",'red',300000,500,"2021-1-20 13:09:53.000"],
['Audi', "2021-1-3",'green',0,100,"2021-1-3 11:03:43.000"],
['BMW', "2021-1-1",'yellow',10000,100,"2021-1-1 15:09:23.000"],
['BMW', "2020-1-20",'blue',20000,150,"2020-1-20 16:09:23.000"],
['BENZ', "2020-1-3",'black',25000,80,"2020-1-3 03:09:23.000"],
)
##column names
col_names=('car','manufactured_date','color','price','sold_num','sold_timestamp')