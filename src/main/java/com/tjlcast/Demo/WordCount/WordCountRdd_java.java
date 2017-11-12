package com.tjlcast.Demo.WordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by tangjialiang on 2017/11/12.
 * ~/project/spark/bin/spark-submit --master spark://10.108.219.61:7077 --class com.tjlcast.Demo.WordCount.WordCountRdd_java ./MySparkJavaScala.jar
 */
public class WordCountRdd_java {
    public static void main(String[] args) {
        SparkConf sparkconfig = new SparkConf().setMaster("spark://10.108.219.61:7077").setAppName("wordcount");

        JavaSparkContext sc = new JavaSparkContext(sparkconfig);

        // data source
        String filePath = "" ;
        int minPartitions = 3 ;
        JavaRDD<String> stringRDD = sc.textFile(filePath);

        JavaPairRDD<String, Integer> word_count = stringRDD.flatMap(line -> (Iterator<String>) Arrays.asList(line.split(" ")))
                .mapToPair(word -> new Tuple2<String, Integer>(word, 1))
                .reduceByKey((a, b) -> a + b);// (word, count)

        word_count.cache();

        Map<String, Integer> stringIntegerMap = word_count.collectAsMap();
        for(Map.Entry<String, Integer> entry : stringIntegerMap.entrySet()) {
            String key = entry.getKey() ;
            Integer value = entry.getValue();

            System.out.format("the number of \"%s\" is %d\n", key, value.toString()) ;
        }
    }
}
