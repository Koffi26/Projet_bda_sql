from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from Script.Data_Creation import *


student.join(borrow,["sid"])\
        .filter(col("sid")=="S15")\
        .join(book,["bid"])\
        .select(col('sid'),col('title'))\
        .show()

spark.sql(""" select sid,title from borrow_sql
              left join book_sql on borrow_sql.bid=book_sql.bid
          where sid="S15"  """).show()

# Question 2
book.join(borrow,["bid"],how='full')\
    .filter(col('sid').isNull())\
    .select(col('title')).show()

spark.sql(""" select title from borrow_sql
              full join book_sql on borrow_sql.bid=book_sql.bid
              where sid is null """).show()

# Question 3
student.join(borrow,["sid"]).filter(col("bid")=="0002")\
       .select(col('sname'),col('sid'),col('bid'))\
       .show()

spark.sql(""" select bid,sname from student_sql
              left join borrow_sql on borrow_sql.sid=student_sql.sid
              where bid="0002" """).show()

# Question 4
student.join(borrow,["sid"])\
    .filter(col("dept")=="mecanique")\
    .join(book,["bid"]).select(col('sid'),col('title'))\
    .show()

spark.sql(""" select sname,title from student_sql
              inner join borrow_sql on borrow_sql.sid=student_sql.sid
              inner join book_sql on book_sql.bid=borrow_sql.bid
              where dept="mecanique"
              """).show()

# Question 5
student.join(borrow,["sid"],how='full')\
       .filter(col('sid').isNull())\
       .show()

spark.sql(""" select * from borrow_sql
              full join student_sql on borrow_sql.sid=student_sql.sid
              where sname is null """).show()

# Question6
Author.join(write,['aid'])\
    .groupBy(col('name'))\
    .agg(F.count(col('name')))\
    .show()

spark.sql(""" select name,count(*) as cnt from write_SQL
              left join Author_sql on write_SQL.aid=Author_sql.aid
              group by name""").show()

#Question 8

borrow.join(student,['sid'])\
    .filter(col('return_time')=="null")\
    .show()

spark.sql(""" select sname from borrow_sql
              left join student_sql on borrow_sql.sid=student_sql.sid
              where return_time ="null" """).show()

borrow.withColumn("check", F.to_date(F.col("checkout_time"), "dd-MM-yyyy"))\
    .withColumn("retour", F.to_date(F.col("return_time"), "dd-MM-yyyy"))\
    .withColumn("Duree", F.datediff(F.col("retour"), F.col("check")))\
    .withColumn("3mois+", (F.when(F.col("Duree")>=90, 1).otherwise(0)))\
    .toPandas().to_csv('Contention/retour.csv')



spark.stop()