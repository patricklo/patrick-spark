package com.patrick.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

public class C4AccumulatorVar {
    public static void main(String[] args){
        SparkConf conf =new SparkConf().setAppName("AccumulatorVar").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);

        final Accumulator<Integer> sum = javaSparkContext.accumulator(0);
        final int factor =3 ;
        List<Integer> numbers = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> numbersRDD = javaSparkContext.parallelize(numbers);

        numbersRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                sum.add(integer);
            }
        });

        System.out.println(sum);

    }
}
