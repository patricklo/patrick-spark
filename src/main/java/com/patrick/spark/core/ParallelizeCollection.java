package com.patrick.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ParallelizeCollection {
    /**
     * 并行化创建集合RDD
     * @param args
     */
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("ParallelizeCollection").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        //要通过并行化集合的方式创建RDD，那么就调用SparkContext以及其子类的parallelize()方法
        List<Integer> numbers = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = javaSparkContext.parallelize(numbers);

        //执行算子操作

        int sum = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer num1, Integer num2) throws Exception {
                return num1 + num2;
            }
        });

        //输出累加的和
        System.out.println("##############"+sum);


        //关闭sparkcontext
        javaSparkContext.close();



    }
}
