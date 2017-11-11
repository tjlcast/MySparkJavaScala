package com.tjlcast.Demo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created by tangjialiang on 2017/11/11.
 * the example of wordcount by java.
 *
 */

public class WordCount {
    public static void main(String[] args) {
        System.out.println("WordCount is starting") ;

        SparkConf sparkConf = new SparkConf() ;
        sparkConf.setMaster("local[2]") ;
        sparkConf.setAppName("test-for-spark-ui") ;
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        // data from socket
        String host = "10.108.219.61" ;
        int port = 9999 ;
        final JavaReceiverInputDStream<String> data = ssc.socketTextStream(host, port);

        // data: 知识，哪怕是知识的幻影，也会成为你的铠甲，保护你不被愚昧反噬。
        JavaPairDStream<String, Integer> counts = data.flatMap(line ->  (Iterator<String>)Arrays.asList(line.split(" ")))
                .mapToPair(s -> new Tuple2<String, Integer>(s, 1))
                .reduceByKey((x, y) ->  x + y) ;

        // show result
        counts.print();

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
