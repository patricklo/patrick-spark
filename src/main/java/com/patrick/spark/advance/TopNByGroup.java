package com.patrick.spark.advance;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class TopNByGroup {
    public static void main(String[] args){
        SparkConf sparkConf = new SparkConf().setAppName("TopN").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines =javaSparkContext.textFile("/Users/patricklo/Documents/score.txt");

        JavaPairRDD<String,Integer> pairs = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s.split(" ")[0],Integer.valueOf(s.split(" ")[1]));
            }
        });
        JavaPairRDD<String,Iterable<Integer>> groupedPairs = pairs.groupByKey();
        JavaPairRDD<String,Iterable<Integer>> top3Score = groupedPairs.mapToPair(new PairFunction<Tuple2<String, Iterable<Integer>>, String, Iterable<Integer>>() {
            @Override
            public Tuple2<String, Iterable<Integer>> call(Tuple2<String, Iterable<Integer>> t) throws Exception {
                Integer[] top3 = new Integer[3];
                String className = t._1;
                Iterator<Integer> scores = t._2.iterator();
                while(scores.hasNext()){
                    Integer score = scores.next();
                    for(int i=0;i<3;i++){
                        if(top3[i]==null){
                            top3[i] = score;
                            break;
                        }else if(score>top3[i]){
                            for(int j=2;j>i;j--){
                                top3[j] = top3[j-1];
                            }
                            top3[i] = score;
                            break;
                        }
                    }

                }
                return new Tuple2<String, Iterable<Integer>>(className, Arrays.asList(top3));
            }
        });

        top3Score.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
                              @Override
                              public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                                            System.out.println(stringIterableTuple2._1+stringIterableTuple2._2);
                              }
                          }
        );
                javaSparkContext.close();
    }
}
