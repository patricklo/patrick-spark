package com.patrick.spark.core;

import com.patrick.spark.advance.SecondarySortKey;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.Arrays;

public class C7SecondSort {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("sortwc").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        final JavaRDD<String> lines = sc.textFile("/Users/patricklo/Documents/secondarySort.txt");
        JavaPairRDD<C7SecondarySortKey,String> pairs = lines.mapToPair(new PairFunction<String, C7SecondarySortKey, String>() {
            @Override
            public Tuple2<C7SecondarySortKey, String> call(String s) throws Exception {
                String[] lineSplitted = s.split(" ");
                C7SecondarySortKey c7SecondarySortKey = new C7SecondarySortKey(Integer.valueOf(lineSplitted[0]),Integer.valueOf(lineSplitted[1]));
                return new Tuple2<C7SecondarySortKey, String>(c7SecondarySortKey,s);
            }
        });

        JavaPairRDD<C7SecondarySortKey, String> sortedParis = pairs.sortByKey();

        JavaRDD<String> sortedLines = sortedParis.map(new Function<Tuple2<C7SecondarySortKey, String>, String>() {
            @Override
            public String call(Tuple2<C7SecondarySortKey, String> v1) throws Exception {
                return v1._2;
            }
        });


        sortedLines.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
        sc.close();
    }
}
