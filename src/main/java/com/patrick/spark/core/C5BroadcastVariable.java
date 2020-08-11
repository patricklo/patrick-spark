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

/**
 * SPARK一个非常重要的特性就是共享变量
 *
 * 默认情况下，如果在一个算子的函数中使用到了某个外部的变量，那么这个变量的值会被 拷贝到每个task中。
 * 此时每个task只能操作自己的那份变量副本。如果多个task想要共享某个变量，那么这种方式是做不到的。
 *
 * spark为此提供了2种共享变量。
 * 1.broadcast variable 广播变量，会将使用到的变量，仅仅为每个节点拷贝一份，更大的用处是优化性能，减少网络及内存消耗
 * 2.Accumulator variable累加变量，可以让多个task共同操作一份变量，主要可以进行累加操作。
 */
public class C5BroadcastVariable {
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
