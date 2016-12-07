hdfs dfs -put person.txt /
val lineRDD = sc.textFile("hdfs://hdp01:9000/person.txt").map(_.split(" "))
case class Person(id:Int,name:String,age:Int)
val personRDD = lineRDD.map(x => Person(x(0).toInt,x(1),x(2).toInt))
val personDF = personRDD.toDF

personDF.show
personDF.select(personDF.col("name")).show
personDF.select(col("name"),col("age")).show
personDF.select("name").show

personDF.printSchema

personDF.select(col("id"),col("name"),col("age") + 1).show
personDF.select(personDF("id"),personDF("name"),personDF("age") + 1).show

personDF.filter("age") >= 18).show
personDF.groupBy("age").count().show

personDF.registerTempTable("t_person")
sqlContext.sql("select * from t_person order by age desc limit 2").show
sqlContext.sql("desc t_person").show