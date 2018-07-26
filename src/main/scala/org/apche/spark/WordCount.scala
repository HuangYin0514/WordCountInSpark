package org.apche.spark

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {

  /*  System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1")
    System.setProperty("HADOOP_USER_NAME", "hadoop")
*/
    val conf = new SparkConf().setAppName("wordcount")/*.setMaster("local[1]")*/
    val sc = new SparkContext(conf)

//    val file = sc.textFile("hdfs://mini1:9000/ceshi/a.txt")
    val file = sc.textFile(args(0))
    val word = file.flatMap(_.split(" "))
    val tuple = word.map((_,1))
    val result = tuple.reduceByKey(_+_)
    val resultSorted = result.sortBy(_._2,false)//true是正序
    resultSorted.foreach(println)
//    resultSorted.saveAsTextFile("hdfs://mini1:9000/ceshi/outputwordcount")
    resultSorted.saveAsTextFile(args(1))
  }

}
