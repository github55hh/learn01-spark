package cn.itcast.spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object InferringSchema {
  def main(args: Array[String]) {

    //����SparkConf()������App����
    val conf = new SparkConf().setAppName("SQL-1")
    //SQLContextҪ����SparkContext
    val sc = new SparkContext(conf)
    //����SQLContext
    val sqlContext = new SQLContext(sc)

    //��ָ���ĵ�ַ����RDD
    val lineRDD = sc.textFile(args(0)).map(_.split(" "))

    //����case class
    //��RDD��case class����
    val personRDD = lineRDD.map(x => Person(x(0).toInt, x(1), x(2).toInt))
    //������ʽת��������������޷���RDDת����DataFrame
    //��RDDת����DataFrame
    import sqlContext.implicits._
    val personDF = personRDD.toDF
    //ע���
    personDF.registerTempTable("t_person")
    //����SQL
    val df = sqlContext.sql("select * from t_person order by age desc limit 2")
    //�������JSON�ķ�ʽ�洢��ָ��λ��
    df.write.json(args(1))
    //ֹͣSpark Context
    sc.stop()
  }
}
//case classһ��Ҫ�ŵ�����
case class Person(id: Int, name: String, age: Int)


//�ύ����
/usr/local/spark-1.5.2-bin-hadoop2.6/bin/spark-submit \
--class cn.itcast.spark.sql.InferringSchema \
--master spark://node1.itcast.cn:7077 \
/root/spark-mvn-1.0-SNAPSHOT.jar \
hdfs://node1.itcast.cn:9000/person.txt \
hdfs://node1.itcast.cn:9000/out 