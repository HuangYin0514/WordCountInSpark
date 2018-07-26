package org.apche.javaOnSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple1;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

public class WordCountOnJava {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\工作\\大数据\\hadoop\\软件\\hadoop-2.6.1");
        System.setProperty("HADOOP_USER_NAME", "hadoop");

        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile("hdfs://mini1:9000/ceshi/a.txt");
        JavaRDD<String> words = textFile.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] split = s.split(" ");
                return Arrays.asList(split);
            }
        });
        /*
        JavaRDD<Tuple2> stringTuple2 = words.map(new Function<String, Tuple2>() {
            @Override
            public Tuple2 call(String v1) throws Exception {
                return new Tuple2(v1, 1);
            }
        });
        */
        JavaPairRDD<String, Integer> stringTuple = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> result = stringTuple.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(result.collect());
    }

}
