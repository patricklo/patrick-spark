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
 * 1. 本地测试程序
 */
public class WordCountLocal {
    public static void main(String[] args){
        /**第1步： 创建spark conf对象，设置spark应用的配置信息*/
        //setMaster可以设置spark应用程序要连接的spark集群的Master节点的url,
        //但如果设置为local，则代表在本地运行
        SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");

        /**第2步： 创建JavaSparkContext*/
        //在spark中，SparkContext是spark所有功能的一个入口，你无论 是Java还是scala，甚至是python编写都必须有一个sparkcontext,
        //它的主要作用，包括初始化spark应用程序所需的一些核心组件，包括调度器（DAGSchedule, TaskScheduler),还会去spark master节点上进行注册，等
        //一句放话,SparkContext,是spark应用中，可以说是最最重要的一个对象
        //但是呢，在spark中，编写不同类型的spark应用程序,使用的sparkcontext是不同的。
        //scala: 使用的就是原生的sparkcontext
        //java: JavaSparkContext
        //如果是开发spark sql: 就是SQLContext, HiveContext
        //如果是开发spark streaming程序，则是它独有的SparkContexts
        JavaSparkContext sc = new JavaSparkContext(conf);

        /**第3步： 要针对输入源(hdfs,本地文件 ，等)，创建一个初始的RDD*/
        /**输入源中的数据会经过RDD，打散，分配到RDD的每个partition中，从而形成一个初始的分布式的数据集*/
        //在这里呢，RDD中，有元素这种概念，如果是HDFS或者本地文件，创建的RDD，每一个元素其实就相当于本地文件中的一行。所以用string存储
        JavaRDD<String> lines = sc.textFile("/Users/patricklo/Documents/test.txt");

        /** 第4步： 对初始RDD进行transformation操作，也就是一些计算操作
         *
         *     4.1 flatMap ->  将一行拆分成单独单词
         */
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            private static final long serialVersionUID  = 1L;
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        /**
         * 4.2 将每个单词拆分成 (单词,1)的map,这样后面就可以groupby key, 把value sum起来  -》达到统计的作用
         *
         * JavaPairRDD的2个泛型参数分别代表的scala tuple元素中的第1和第2个值。
         */
        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID  = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        /**
         * reduceBy key, 把同一个key的value sum up起来  -》达到统计的作用
         */
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

        /**
         * 到这为止，我们通过spark的几个算子操作，已经统计出单词的个数
         * 但是，我们之前使用的flatMap,reduceByKey, mapToPair 这些都叫transformation操作
         * 在spark程序中，光有transformation操作是不够的。
         * 最后，我们通过使用一个action操作（foreach),来触发程序的执行
         * */
    }
}
