package com.tjlcast.Demo

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by tangjialiang on 2017/11/11.
  */

object WordCount_scala {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("spark://10.108.219.61:7077").setAppName("myWordCount")
        val ssc = new StreamingContext(sparkConf, Seconds(1))

        val host = "10.108.219.61"
        val port = 9999
        val dStream = ssc.socketTextStream(host, port)

        val counts = dStream.flatMap(line => line.split(" "))
            .map(word => (word, 1))
            .reduceByKey((a, b) => (a + b))

        counts.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
