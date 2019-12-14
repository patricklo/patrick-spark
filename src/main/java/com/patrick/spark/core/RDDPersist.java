package com.patrick.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * RDD cache/persist 是有规则的
 * 必须在trasformation或者是textFile等创建了一个RDD之后连续调用 cache()或persist()才可以
 * 如果先创建再调用 cache(）是没有用的
 */
public class RDDPersist {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("WordCountLocal").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        //no cache
        //JavaRDD<String> lines = sc.textFile("/Users/patricklo/Documents/big.txt");
        //cached
        JavaRDD<String> lines = sc.textFile("/Users/patricklo/Documents/big.txt").cache();

        long beginTime = System.currentTimeMillis();
        long count = lines.count();
        System.out.println(count);
        long endTime = System.currentTimeMillis();
        System.out.println(endTime-beginTime);

        beginTime = System.currentTimeMillis();
        count = lines.count();
        System.out.println(count);
        endTime = System.currentTimeMillis();
        System.out.println(endTime-beginTime);


        sc.close();
    }
}
