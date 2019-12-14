package com.patrick.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public class LineCount {
    public static void main(String[] args){
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("LineCount").setMaster("local");
        //创建对应开发语言的context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //创建RDD
        JavaRDD<String> lines = javaSparkContext.textFile("/Users/patricklo/Documents/test1.txt");


        JavaPairRDD<String,Integer> javaPairRDD = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String,Integer>(s,1);
            }
        });

        JavaPairRDD<String, Integer> lineWordCount = javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        lineWordCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1 + " appear "+t._2);
            }
        });

        //关闭Java context

        javaSparkContext.close();
    }
}
