from pyspark.sql import SparkSession

spark = SparkSession.builder\
                    .master("local")\
                    .appName("TP5")\
                    .getOrCreate()

L1=[("07890","Jean Paul Sartre"),
    ("05678","Pierre de Ronsard")]
Rdd1=spark.sparkContext.parallelize(L1)
Rdd1.collect()
Author=Rdd1.toDF(["aid","name"])
Author.show()
Author.createOrReplaceTempView("Author_sql")

L2 = [("0001", "L'existentialisme est un humanisme", "Philosophie"),
      ("0002", "Huis clos. Suivi de Les Mouches", "Philosophie"),
      ("0003", "Mignonne allons voir si la rose", "Poeme"),
      ("0004", "Les Amours", "Poeme")]
RDD2 = spark.sparkContext.parallelize(L2)
book = RDD2.toDF(["bid", "title", "category"])
book.show()
book.createOrReplaceTempView("book_SQL")

L3 = [("S15", "toto", "math"),
      ("S16", "popo", "eco"),
      ("S17", "fofo", "mecanique")]
RDD3 = spark.sparkContext.parallelize(L3)
student = RDD3.toDF(["sid", "sname", "dept"])
student.show()
student.createOrReplaceTempView("student_sql")

L4 = [("07890", "0001"),
      ("07890", "0002"),
      ("05678", "0003"),
      ("05678", "0003") ]
RDD4 = spark.sparkContext.parallelize(L4)
write = RDD4.toDF(["aid", "bid"])
write.show()
write.createOrReplaceTempView("write_SQL")

L5 = [("S15", "0003", "02-01-2020","01-02-2020"),
      ("S15", "0002", "13-06-2020","null"),
      ("S15", "0001", "13-06-2020","13-10-2020"),
      ("S16", "0002", "24-01-2020","24-01-2020"),
      ("S17", "0001", "12-04-2020","01-07-2020")]
RDD5 = spark.sparkContext.parallelize(L5)
borrow = RDD5.toDF(["sid", "bid", "checkout_time","return_time"])
borrow.show()
borrow.createOrReplaceTempView("borrow_sql")