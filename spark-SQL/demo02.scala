package cn.zhuchangbao.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object InferringSchema {
  def main(args: Array[String]) {

    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("SQL-1")
    //SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)
    //创建SQLContext
    val sqlContext = new SQLContext(sc)

    //从指定的地址创建RDD
    val lineRDD = sc.textFile(args(0)).map(_.split(" "))

    //创建case class
    //将RDD和case class关联
    val personRDD = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    //导入隐式转换，如果不到人无法将RDD转换成DataFrame
    //将RDD转换成DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF
    //注册表
    personDF.registerTempTable("t_person")
    //传入SQL
    val df = sqlContext.sql("select * from t_person order by age desc limit 2")
    //将结果以JSON的方式存储到指定位置
    df.write.json(args(1))
    //停止Spark Context
    sc.stop()
  }
}
//case class一定要放到外面
case class Person(id: Int, name: String, age: Int)


//提交任务
/usr/local/spark-1.5.2-bin-hadoop2.6/bin/spark-submit \
--class cn.zhuchangbao.spark.sql.InferringSchema \
--master spark://hdp01:7077 \
/root/spark-mvn-1.0-SNAPSHOT.jar \
hdfs://hdp01:9000/person.txt \
hdfs://hdp01:9000/out 