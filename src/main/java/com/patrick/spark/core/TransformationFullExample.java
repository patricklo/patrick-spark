package com.patrick.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.sources.In;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 常用transformation 案例
 * 常用transformation 介绍
 * 1. map  将RDD中的每个元素传入自定义函数，获取一个新的元素，然后用新的元素组成新的RDD并返回，可进行下一步处理
 * 2. filter  对RDD的元素进行判断，如返回TRUE则保留，反之则剔除
 * 3. flatMap 与map类似，但每个元素都可以返回一个或多个新元素
 * 4. groupByKey
 * reduceByKey 对每个key对应的value进行reduce操作
 * 5. sortByKey
 * 6. join 对2个包含<Key,value>对的RDD进行join操作，每个key join上的新pair，都会传入到自定义函数进行处理
 * 7. cogroup 同join,但每个key上的iterable<value>都会传入自定义函数进行处理
 */
public class TransformationFullExample {

    /**
     * 1. map    将集合每个元素乘以2
     * 2. filter 过滤出集合中的偶数
     * 3. flatMap 将行拆分成单词
     * 4. groupByKey 将每个班级成绩进行分组
     * reduceByKey 统计每个班级的总分
     * 5. sortByKey 将学生分数进行排序
     * 6. join 打印每个学生的成绩
     * 7. cogroup 打印每个学生的成绩
     */

    public static void main(String[] args) {
        //TransformationFullExample.map();
        //filter();
        //flatMap();
        //groupByKey();
        //reduceByKey();
        //sortByKey();
        //join();
        cogroup();
    }


    /**
     *   cogroup
     */

    public static  void cogroup(){
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("joinAndCogroup").setMaster("local");

        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "Leo"),
                new Tuple2<Integer, String>(2, "L"),
                new Tuple2<Integer, String>(3, "e"),
                new Tuple2<Integer, String>(4, "lili")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 110),
                new Tuple2<Integer, Integer>(1, 60),
                new Tuple2<Integer, Integer>(2, 110),
                new Tuple2<Integer, Integer>(4, 60)
        );
        //并行化2个RDD 集合
        JavaPairRDD<Integer, String> studentRDD = javaSparkContext.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoresRDD = javaSparkContext.parallelizePairs(scoreList);
        //通过key进行join
        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores = studentRDD.cogroup(scoresRDD);
        studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
                                  @Override
                                  public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {

                                      System.out.println(t._1+":"+t._2._1+":"+t._2._2);
                                  }
                              });
                javaSparkContext.close();
    }
    /**
     * join  cogroup
     */

    public static  void join(){
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("joinAndCogroup").setMaster("local");

        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "Leo"),
                new Tuple2<Integer, String>(2, "L"),
                new Tuple2<Integer, String>(3, "e"),
                new Tuple2<Integer, String>(4, "lili")
        );
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 110),
                new Tuple2<Integer, Integer>(4, 60)
        );
        //并行化2个RDD 集合
        JavaPairRDD<Integer, String> studentRDD = javaSparkContext.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoresRDD = javaSparkContext.parallelizePairs(scoreList);
        //通过key进行join
        JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores = studentRDD.join(scoresRDD);
        studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
                System.out.println(t._1+":"+t._2._1+":"+t._2._2);
            }
        });
        javaSparkContext.close();
    }
    /**
     * 4. sortByKey 将每个班级成绩进行分组
     */
    public static void sortByKey() {
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("TransformationFullExample").setMaster("local");

        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Tuple2<Integer, String>> scores = Arrays.asList(
                new Tuple2<Integer, String>(50, "Leo"),
                new Tuple2<Integer, String>(90, "L"),
                new Tuple2<Integer, String>(60, "e"),
                new Tuple2<Integer, String>(70, "lili")
        );
        //并行化集合
        JavaPairRDD<Integer, String> scoresRDD = javaSparkContext.parallelizePairs(scores);

        JavaPairRDD<Integer, String> sortedRDD = scoresRDD.sortByKey(false);

        sortedRDD.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });
        javaSparkContext.close();
    }
    /**
     * 4. reduceByKey 将每个班级成绩进行分组
     */
    public static void reduceByKey() {
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("TransformationFullExample").setMaster("local");

        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<String, Integer>("class1", 50),
                new Tuple2<String, Integer>("class2", 90),
                new Tuple2<String, Integer>("class1", 110),
                new Tuple2<String, Integer>("class2", 50),
                new Tuple2<String, Integer>("class2", 50)
        );
        //并行化集合
        JavaPairRDD<String, Integer> scoresRDD = javaSparkContext.parallelizePairs(scores);

        //针对scoresRDD 执行reduceByKey操作
        JavaPairRDD<String, Integer> totalScoresRDD = scoresRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1+v2;
                    }
                }
        );

        totalScoresRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });
        //关闭context
        javaSparkContext.close();
    }

    /**
     * 4. groupByKey 将每个班级成绩进行分组
     */
    public static void groupByKey() {
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("TransformationFullExample").setMaster("local");

        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Tuple2<String, Integer>> scores = Arrays.asList(
                new Tuple2<String, Integer>("class1", 50),
                new Tuple2<String, Integer>("class2", 90),
                new Tuple2<String, Integer>("class1", 110),
                new Tuple2<String, Integer>("class2", 50),
                new Tuple2<String, Integer>("class2", 50)
        );
        //并行化集合
        JavaPairRDD<String, Integer> scoresRDD = javaSparkContext.parallelizePairs(scores);

        //针对scoresRDD 执行groupByKey操作
        JavaPairRDD<String, Iterable<Integer>> groupedScoresRDD = scoresRDD.groupByKey();

        //打印groupedScoresRDD
        groupedScoresRDD.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                System.out.println(t._1 + ":" + t._2);
            }
        });


        //关闭context
        javaSparkContext.close();
    }


    /**
     * 3. flatMap 将行拆分成单词
     */

    public static void flatMap() {
        //创建spark conf
        SparkConf sparkConf = new SparkConf().setAppName("TransformationFullExample").setMaster("local");
        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<String> lines = Arrays.asList("hello you", "Hello Me", "Hello aaa");
        //并行化集合，创建初始RDD
        JavaRDD<String> linesRDD = javaSparkContext.parallelize(lines);

        JavaRDD<String> flatMapRDD = linesRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        flatMapRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        //关闭context
        javaSparkContext.close();
    }

    /**
     * * 2. filter 过滤出集合中的偶数
     */
    public static void filter() {
        //创建sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("TransformationFullExample").setMaster("local");
        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        //并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = javaSparkContext.parallelize(numbers);

        //使用map鼻子，将集合中每个元素都乘以2
        //map算子，是对任何类型的RDD，都可以调用
        //在java中，map算子接收的参数是function函数
        //创建的function对象 ，一定会让你设置第二个泛型参数，即返回的新元素类型
        JavaRDD<Integer> filterNumberRDD = numberRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer integer) throws Exception {
                return integer % 2 == 0;
            }
        });
        //打印新的RDD
        filterNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        //关闭context
        javaSparkContext.close();
    }

    /**
     * 1. map    将集合每个元素乘以2
     */
    public static void map() {
        //创建sparkconf
        SparkConf sparkConf = new SparkConf().setAppName("TransformationFullExample").setMaster("local");
        //创建context
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        //构建集合
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        //并行化集合，创建初始RDD
        JavaRDD<Integer> numberRDD = javaSparkContext.parallelize(numbers);

        //使用map鼻子，将集合中每个元素都乘以2
        //map算子，是对任何类型的RDD，都可以调用
        //在java中，map算子接收的参数是function函数
        //创建的function对象 ，一定会让你设置第二个泛型参数，即返回的新元素类型
        JavaRDD<Integer> multiplyNumberRDD = numberRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * 2;
            }
        });
        //打印新的RDD
        multiplyNumberRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        //关闭context
        javaSparkContext.close();
    }


}
