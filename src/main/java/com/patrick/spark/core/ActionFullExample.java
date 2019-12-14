package com.patrick.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * 1. reduce
 * 2. collect
 * 3. count
 * 4. take
 * 5. saveAsTextfile
 * 6. countByKey
 * 7. foreach
 */
public class ActionFullExample {
    public static void main(String[] args) {
        //reduce();
        collect();
    }

    public static void reduce() {
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("ActionFullExample_Reduce").setMaster("local");

        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9, 10);
        //并行化集合
        JavaRDD<Integer> numberRDD = javaSparkContext.parallelize(numberList);

        //使用reduce操作对集合中的数字进行累加
        Integer result = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(result);
        javaSparkContext.close();
    }

    public static void collect() {
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("ActionFullExample_Reduce").setMaster("local");

        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9, 10);
        //并行化集合
        JavaRDD<Integer> numberRDD = javaSparkContext.parallelize(numberList);

        //先使用map对所有元素*2
        JavaRDD<Integer> mulNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v) throws Exception {
                return v*2;
            }
        });

        //不用foreach 在远程集群上遍历rdd中的元素
        //使用collect操作 将远程集群的doubleNumbers RDD数据摘取到本地
        //仅为演示用途，此方法不推荐使用，因为一旦集群上的数据量较大时,性能较差，一般都用foreach
        List<Integer> doubleNumberList = mulNumberRDD.collect();
        for(Integer number:doubleNumberList){
            System.out.println(number);
        }




        javaSparkContext.close();
    }
}
