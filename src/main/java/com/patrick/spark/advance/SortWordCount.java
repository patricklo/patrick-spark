package com.patrick.spark.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

public class SortWordCount {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("SortWordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("/Users/patricklo/Documents/big.txt").cache();
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String,Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });

        //进行key,value反转value,key
        JavaPairRDD<Integer,String> countWords= wordCounts.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2,t._1);
            }
        });

        JavaPairRDD<Integer,String> sortedCountWords = countWords.sortByKey(false);

        //再反转成原来的格式
        JavaPairRDD<String,Integer> sortedWordsCount= sortedCountWords.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                return new Tuple2<String, Integer>(t._2,t._1);
            }
        });

        sortedWordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });

        sc.close();
    }
}
