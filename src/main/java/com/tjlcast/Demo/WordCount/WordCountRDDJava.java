package com.tjlcast.Demo.WordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import scala.Function1;
import scala.collection.TraversableOnce;

import java.util.Arrays;

/**
 * Created by tangjialiang on 2017/11/12.
 */
public class WordCountRDD {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("word_count_rdd") ;

        SparkContext sc = new SparkContext(sparkConf);

        // read data source
        String filePath = "" ;
        int minPartitions = 2 ;
        RDD<String> stringRDD = sc.textFile(filePath, minPartitions);

        // do something
        stringRDD.flatMap(line -> )

    }
}
