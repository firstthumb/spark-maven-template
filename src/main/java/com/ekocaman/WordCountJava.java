package com.ekocaman;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class WordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCountJava").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Tuple2<String, Integer>> result = sc.textFile("src/main/resources/input.txt", 1)
                .flatMap(line -> Arrays.asList(line.split(" ")))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .collect();


        result.forEach((tuple) -> System.out.printf("%s -> %d\n", tuple._1(), tuple._2()));
    }
}
