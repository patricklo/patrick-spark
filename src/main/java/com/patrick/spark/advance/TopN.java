package com.patrick.spark.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class TopN {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("TopN").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines =javaSparkContext.textFile("/Users/patricklo/Documents/top.txt");
        JavaPairRDD<Integer, String> numberPairs = lines.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s), s);
            }
        });

       JavaPairRDD<Integer, String> sortedPairs = numberPairs.sortByKey(false);

        JavaRDD<Integer> sortedNumbers = sortedPairs.map(new Function<Tuple2<Integer, String>, Integer>() {
            @Override
            public Integer call(Tuple2<Integer, String> t) throws Exception {
                return t._1;
            }
        });

        List<Integer> top3 = sortedNumbers.take(3);
        System.out.println(top3);
        javaSparkContext.close();
    }
}
