package com.comp30770;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkMain {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Word Count").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        JavaRDD<String> lines = context.textFile("C:\\Users\\sebas\\Documents\\Web Dev\\Project\\22206116\\COMP30770-Project\\src\\main\\resources\\mobydick.txt");

        JavaPairRDD<String, Integer> wordCounts = lines.flatMap((line) -> Arrays.asList(line.split(" "))
                .iterator()).mapToPair(word -> new Tuple2<>(word, 1)).reduceByKey(Integer::sum);


        wordCounts.collect().forEach(System.out::println);
        context.close();
    }
}
