package com.patrick.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 本地测试程序
 */
public class WordCountCluster {
    public static void main(String[] args){
        //将spark 运行放到集群上
        //1. 修改/删除 setMaster , 自己会去寻找
        //2. 从读取本地文件 -》  读取hadoop hdfs服务器上的存储的大文件
        //3。 打包 放上spark1中
        //4. 编写 spark submit脚本，提交到集群中运行
        SparkConf conf = new SparkConf().setAppName("WordCountCluster");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //JavaRDD<String> lines = sc.textFile("/Users/patricklo/Documents/test.txt");
        JavaRDD<String> lines = sc.textFile("hdfs://spark1:9000/test.txt");
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID  = 1L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID  = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairRDD<String,Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> wordCount) throws Exception {
                System.out.println(wordCount._1+" : "+wordCount._2);
            }
        });

    }
}
