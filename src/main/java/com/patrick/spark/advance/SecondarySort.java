package com.patrick.spark.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;


/**
 * 自定义的二次排序 算法
 */
public class SecondarySort {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("SecondarySort").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines =javaSparkContext.textFile("/Users/patricklo/Documents/secondarySort.txt");
        JavaPairRDD<SecondarySortKey, String> pairs = lines.mapToPair(new PairFunction<String, SecondarySortKey, String>() {
            @Override
            public Tuple2<SecondarySortKey, String> call(String s) throws Exception {
                String[] lineSplitted = s.split(" ");
                SecondarySortKey key =new SecondarySortKey(Integer.valueOf(lineSplitted[0]),Integer.valueOf(lineSplitted[1]));
                return new Tuple2<SecondarySortKey, String>(key,s);
            }
        });

        JavaPairRDD<SecondarySortKey, String> sortedPairs = pairs.sortByKey();

        JavaRDD<String> sortedLines = sortedPairs.map(new Function<Tuple2<SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });

        sortedLines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        javaSparkContext.close();

    }
}
