package com.patrick.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
        //collect();
        //countByKey();
        take();
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


    public static void saveASText() {
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

        //直接将rdd中的数据，保存在文件中
        //需开启服务器上的服务
        //需要注意的是，我们这里只能保存文件夹，也就是目录
        //实际过程中，double_number.txt是被创建成一个目录
        mulNumberRDD.saveAsTextFile("hdfs://spark01:90 00/double_number.txt");
        javaSparkContext.close();
    }

    public static void countByKey(){
            //创建spark conf
            SparkConf sparkConf = new SparkConf().setAppName("countByKey").setMaster("local");

            //创建context
            JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
            //构建集合
            List<Tuple2<String, String>> scores = Arrays.asList(
                    new Tuple2<String, String>("class1", "leo"),
                    new Tuple2<String, String>("class2", "Jack"),
                    new Tuple2<String, String>("class1", "marry"),
                    new Tuple2<String, String>("class2", "tom"),
                    new Tuple2<String, String>("class2", "david")
            );
            //并行化集合
            JavaPairRDD<String, String> scoresRDD = javaSparkContext.parallelizePairs(scores);

            //针对scoresRDD 执行countByKey操作,统计每个班级的学生人数，也就是统计每个key对应的元素个数
            Map<String, Object> countResult = scoresRDD.countByKey();

            for(Map.Entry<String, Object> studentCount : countResult.entrySet()){
                System.out.println(studentCount.getKey()+":"+studentCount.getValue());
            }


            //关闭context
            javaSparkContext.close();
    }

    public static void take(){
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("ActionFullExample_Reduce").setMaster("local");

        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Integer> numberList = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 9, 10);
        //并行化集合
        JavaRDD<Integer> numberRDD = javaSparkContext.parallelize(numberList);
        //拿前3个数
        List<Integer> subNumbers = numberRDD.take(3);
        System.out.println(subNumbers);

        javaSparkContext.close();

    }
}
