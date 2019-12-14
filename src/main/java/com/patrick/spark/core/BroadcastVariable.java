package com.patrick.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.sources.In;

import java.util.Arrays;
import java.util.List;

public class BroadcastVariable {
    public static void main(String[] args){
        SparkConf conf =new SparkConf().setAppName("BroadcastVariable").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        final int factor =3 ;
        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD = javaSparkContext.parallelize(numbers);

        //默认情况下每一个循环都会copy一份factor值进去
        //将factor定义为BroadcastVariable后，提升性能同时降低内存使用
        final Broadcast<Integer> factorBroadcast = javaSparkContext.broadcast(factor);
        JavaRDD<Integer> multipleNumbers = numbersRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v) throws Exception {
                int factor = factorBroadcast.value();
                return v*factor;
            }
        });

        multipleNumbers.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });

    }
}
