package org.apche.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Tparlise {


  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
    System.setProperty("HADOOP_USER_NAME", "hadoop")

    val conf = new SparkConf().setAppName("wordcount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5, 10)
    val myArr = data.map(x => x * 10)
    myArr.foreach(println)
    val mypar: RDD[Int] = sc.parallelize(1 to 10)
    val myRDD: RDD[Any] = mypar.map(_ * 2)
  }


}
